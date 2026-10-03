/**
 * One long-lived Claude Code process per conversation.
 *
 * Claude Code is a session process, not a request/response call: it keeps
 * running while background shells and agents it started are alive, and when
 * one of them settles it starts a turn of its own to report on it. Running
 * one query per Telegram prompt and waiting for it to exit therefore held the
 * next prompt hostage to whatever the model had backgrounded, and closing the
 * query early killed that work and dropped the report.
 *
 * Here the process stays attached through an open streaming prompt for as
 * long as its session stays mounted:
 *   - an operator turn pushes its prompt and ends on the result that names it;
 *   - a turn the CLI starts itself (a background task report) becomes a
 *     "CLI turn": it holds the conversation busy like any turn, accepts
 *     Steer, and its answer is delivered as its own durable reply;
 *   - the process is replaced when the mounted session, folder or model
 *     changes, and closed at shutdown or when it exits on its own.
 *
 * Each process lives in a scope of its own, forked from the live sessions' scope:
 * closing it ends the prompt channel and aborts and closes the query. What the
 * process writes is read as a Stream by a fiber that runs until the query ends,
 * which it does once closed, so whatever turn the process was serving is settled
 * however it went; shutdown closes every process and waits for those fibers.
 *
 * The processes are kept by conversation in a Map rather than an RcMap or LayerMap:
 * a process is not shared by holders whose count decides its life; it lives until
 * it is replaced, closed, or exits. It is started with what the turn that needs it
 * brings (the session it resumes, the workspace's bayma endpoint, the model choice),
 * which a lookup by key is not given, and whether it is replaced depends on what it
 * has learnt since (the session its init named). Every change to the Map is
 * synchronous, so a process exiting as a turn replaces it cannot remove its successor.
 */
import { randomUUID } from "node:crypto";

import {
  query,
  type HookCallback,
  type HookInput,
  type HookJSONOutput,
  type SDKBackgroundTasksChangedMessage,
  type SDKMessage,
  type SDKResultMessage,
  type SessionStore,
} from "@anthropic-ai/claude-agent-sdk";
import { Clock, Context, Deferred, Effect, Exit, Fiber, FiberSet, Scope, Stream } from "effect";

import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  type CommandEventPolicy,
  createCommandEventPolicy,
} from "../../codex/command-event-policy.ts";
import { appendBlock, errorBlock, isVisibleCodexItem, mapItemToBlocks, type ResponseBlock } from "../../codex/event-projection.ts";
import { createTurnTimer, type TurnTimer } from "../../codex/turn-timing.ts";
import type { BaymaEndpoint, SandboxError } from "../../kube/sandboxes.ts";
import { type BaymaMcpServer, type HostBaymaScope, type NoFolderWorkspaces, noFolderBayma } from "../../mcp/bayma.ts";
import { isBlockedDbCommand } from "../../policy/db-guardrail.ts";
import { extractShellCommands } from "../../policy/embedded-shell.ts";
import { looksLikeSelfRestartCommand } from "../../policy/restart-command.ts";
import { detectWorkflowWait } from "../../policy/workflow-wait.ts";
import { effectRunnerHere } from "../../shared/effects.ts";
import { createLogger, withLogScope } from "../../shared/log.ts";
import type { SessionError } from "../../sandbox/index.ts";
import { outsideTraces } from "../../telemetry/index.ts";
import { ActiveTurns, type RunningTurn, type StopReason, stopReasonText } from "../active-turns.ts";
import type { TransportTurn, TurnParams, TurnPersistence, TurnResult } from "../index.ts";
import { CLAUDE_HARNESS } from "../names.ts";
import { buildClaudeEnv } from "./env.ts";
import { cacheReadTokensFromUsage, projectAssistantMessageToItems, projectResultMessage } from "./event-projection.ts";
import { claudeMcpServers } from "./mcp.ts";
import { getClaudeEffort, getClaudeModel } from "./model.ts";
import { buildClaudeUserMessage, instrumentPromptChannel, makePromptChannel, type PromptChannel, promptUuidsAnsweredBy } from "./prompt-channel.ts";
import {
  BAYMA_EXEC_TOOL,
  buildClaudeQueryOptions,
  ClaudeCodeError,
  type ClaudeQuery,
  type ClaudeQueryFactory,
  getErrorMessage,
} from "./runtime.ts";
import type { ClaudeSessionApi } from "./sessions.ts";
import { claudeTelemetryEnv } from "./telemetry.ts";

const LOG_SCOPE = "claude-live";
/** The scope's lines, for the helpers that log through a logger of their own (the turn timer, the command policy). */
const log = createLogger(LOG_SCOPE);

/** How long a turn waits for a steered prompt the CLI has not answered once its own prompt is answered. */
const UNANSWERED_PROMPT_GRACE = "15 seconds";

/** A background task Claude Code reports live: a shell or agent it started that is still running. */
export type BackgroundTask = SDKBackgroundTasksChangedMessage["tasks"][number];

/**
 * The turn an operator's prompt started on a live process: the prompts it waits on
 * answers to (its own and any steered into it), the response it is building, and how
 * it reports back when it ends.
 */
export interface OperatorTurn {
  readonly promptUuids: Set<string>;
  readonly blockSequence: ResponseBlock[];
  readonly pendingResponseId: string;
  readonly persistence: TurnPersistence;
  readonly turnTimer: TurnTimer;
  readonly onStarted: (() => void) | undefined;
  readonly onTransportCompleted: ((turn: TransportTurn) => void) | undefined;
  responseCompleted: boolean;
  interrupted: boolean;
  /** The first command the database guardrail denied in this turn. */
  blockedGuardrailCommand: string | null;
  /** Bounds the wait for a steered prompt's answer once the turn's own prompt is answered. */
  unansweredGrace: Fiber.Fiber<void> | undefined;
  /** Steers that arrived before the process was ready, pushed after the prompt. */
  readonly earlySteers: string[];
  readonly commandPolicy: CommandEventPolicy;
  /** How the turn ended, once it has. */
  readonly done: Deferred.Deferred<TurnResult>;
  /** Its registration as its conversation's running turn, closed as it ends. */
  readonly registration: Scope.Closeable;
  firstEventLogged?: boolean;
  firstVisibleItemLogged?: boolean;
}

/** A turn Claude Code started on its own, delivered as a reply of its own. */
export interface CliTurn {
  readonly pendingResponseId: string;
  readonly blockSequence: ResponseBlock[];
  /** Prompts steered into it. */
  readonly promptUuids: Set<string>;
  /** Its registration as its conversation's running turn, closed as it ends. */
  readonly registration: Scope.Closeable;
}

/** How a live process came to serve its session: resuming it, starting it under a reserved id, or starting a new one. */
export type ClaudeHostMode = "resume" | "reserved" | "start";

/** The live Claude Code process serving one conversation, and the turns it is running. */
export interface ClaudeHost {
  readonly threadKey: string;
  /** The model and effort it runs on, as modelKey renders them. */
  readonly key: string;
  readonly baymaUrl: string | null;
  sessionId: string | null;
  readonly channel: PromptChannel;
  /** Aborts the process; the Agent SDK is given it to stop the process with. */
  readonly controller: AbortController;
  readonly sdkQuery: ClaudeQuery;
  /** What the process lives in: closing it closes the process. */
  readonly scope: Scope.Closeable;
  readonly mode: ClaudeHostMode;
  initialized?: boolean;
  closed: boolean;
  closeReason: string | null;
  current: OperatorTurn | null;
  cliTurn: CliTurn | null;
  backgroundTasks: BackgroundTask[];
  /** Prompts of finished turns, whose late results are ignored. */
  readonly retiredUuids: Set<string>;
  readonly interruptedUuids: Set<string>;
  persistence: TurnPersistence;
  chatId: string;
  messageId: string;
  onBackgroundResponse: Effect.Effect<void>;
  notifyIdle: Effect.Effect<void>;
}

/** What makeClaudeLiveSessions is given. */
export interface ClaudeLiveSessionsOptions {
  readonly workingDirectory: string;
  readonly sessions: Pick<ClaudeSessionApi, "sessionExists">;
  readonly sessionStore?: SessionStore | null;
  readonly sessionFsBayma?: Effect.Effect<BaymaEndpoint, SessionError> | null;
  readonly folderBayma?: (scope: Pick<HostBaymaScope, "threadKey">) => Effect.Effect<BaymaMcpServer, SandboxError | NoFolderWorkspaces>;
  readonly queryFactory?: ClaudeQueryFactory | undefined;
}

/** The live Claude Code processes of one working directory, one per conversation, for as long as their scope is open. */
export interface ClaudeLiveSessions {
  readonly runTurn: (params: TurnParams) => Effect.Effect<TurnResult>;
  /** The live process serving a conversation, if any (for tests and diagnostics). */
  readonly get: (threadKey: string) => ClaudeHost | null;
  /** Ends the live process of a conversation, if it has one. */
  readonly close: (threadKey: string, reason?: string) => Effect.Effect<void>;
}

function modelKey(persistence: TurnPersistence, threadKey: string): string {
  const choice = persistence.getModelChoice?.(threadKey, CLAUDE_HARNESS) ?? null;
  return JSON.stringify({ model: getClaudeModel(process.env, choice) ?? null, effort: getClaudeEffort(process.env, choice) ?? null });
}

function describeTasks(tasks: readonly BackgroundTask[]): string {
  return tasks.map((task) => task.description || task.task_type || task.task_id).join("; ");
}

/** A folder conversation's bayma where none is given: a deployment's without folder workspaces. */
const defaultFolderBayma = ({ threadKey }: Pick<HostBaymaScope, "threadKey">) => noFolderBayma({ harness: CLAUDE_HARNESS, threadKey });

/** A promise of what starting Claude Code needs, failing as starting it fails. */
const attempt = <A>(evaluate: () => Promise<A>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.tryPromise({ try: evaluate, catch: (cause) => new ClaudeCodeError({ cause }) });

/** What starting Claude Code needs from alasio's services, failing as starting it fails. */
const needed = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.mapError(effect, (cause) => new ClaudeCodeError({ cause }));

/** Now, as a log line gives it. */
const now = Effect.map(Clock.currentTimeMillis, (millis) => new Date(millis).toISOString());

/** Ends a turn the CLI started: its prompts retired, and the conversation let go of. */
const releaseCliTurn = (host: ClaudeHost, cliTurn: CliTurn): Effect.Effect<void> =>
  Effect.suspend(() => {
    for (const uuid of cliTurn.promptUuids) {
      host.retiredUuids.add(uuid);
    }
    return Scope.close(cliTurn.registration, Exit.void);
  }).pipe(Effect.andThen(host.notifyIdle));

/**
 * The live processes of `workingDirectory`, for as long as the scope they are made in
 * is open. `workingDirectory` is where the CLI runs: a folder workspace itself, with
 * the conversation's bayma from `folderBayma()`, or a session filesystem's harness
 * directory, when `sessionFsBayma()` gives the workspace's bayma endpoint (bringing its
 * Sandbox up) and the CLI is confined to it (sessionfs.ts).
 */
export const makeClaudeLiveSessions = Effect.fnUntraced(function*({
  workingDirectory,
  sessions,
  sessionStore = null,
  sessionFsBayma = null,
  folderBayma = defaultFolderBayma,
  queryFactory = query,
}: ClaudeLiveSessionsOptions): Effect.fn.Return<ClaudeLiveSessions, never, Scope.Scope | ActiveTurns> {
  const scope = yield* Effect.scope;
  const activeTurns = yield* ActiveTurns;
  // Runs Claude Code's hooks, which the Agent SDK calls as promises, and each process's
  // consumer, which starts outside the trace of the turn that starts the process.
  const run = yield* effectRunnerHere(Context.empty());
  const hosts = new Map<string, ClaudeHost>();
  const consumers = yield* FiberSet.make<void>();
  // At shutdown the processes, whose scopes are forked from this one, are closed first;
  // then every turn they served is settled as their consumers end.
  yield* Scope.addFinalizer(scope, FiberSet.awaitEmpty(consumers));

  /** Closes the process: its prompt ends, its query is aborted and closed, and its consumer then ends. */
  const closeProcess = Effect.fnUntraced(function*(host: ClaudeHost, reason: string) {
    if (host.closed) {
      return;
    }
    host.closed = true;
    host.closeReason = reason;
    if (hosts.get(host.threadKey) === host) {
      hosts.delete(host.threadKey);
    }
    const live = host.backgroundTasks.filter((task) => !task.ambient);
    yield* Effect.logInfo(
      `closing thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)} reason=${JSON.stringify(reason)}`
        + (live.length > 0 ? ` stopping_background_tasks=${JSON.stringify(describeTasks(live))}` : ""),
    );
    yield* host.channel.end;
    host.controller.abort(reason);
    try {
      host.sdkQuery.close();
    } catch {
      // Already closed.
    }
  });

  const closeHost = (host: ClaudeHost, reason: string): Effect.Effect<void> =>
    Effect.andThen(closeProcess(host, reason), Scope.close(host.scope, Exit.void));

  const finishOperatorTurn = Effect.fnUntraced(function*(host: ClaudeHost, current: OperatorTurn) {
    const grace = current.unansweredGrace;
    current.unansweredGrace = undefined;
    if (grace) {
      yield* Fiber.interrupt(grace);
    }
    for (const uuid of current.promptUuids) {
      host.retiredUuids.add(uuid);
    }
    current.promptUuids.clear();
    yield* Scope.close(current.registration, Exit.void);
    if (current.blockedGuardrailCommand && !current.responseCompleted && !current.interrupted) {
      appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
        type: "text",
        content: buildDbGuardrailFallbackText(current.blockedGuardrailCommand),
      });
    }
    current.turnTimer("query.finished", { guardrail_blocked: Boolean(current.blockedGuardrailCommand) });
    yield* Effect.logInfo(`turn.done completed=${current.responseCompleted} interrupted=${current.interrupted}`);
    yield* Deferred.succeed(current.done, {
      blockSequence: current.blockSequence,
      sessionId: host.sessionId,
      pendingResponseId: current.pendingResponseId,
      interrupted: current.interrupted,
      responseCompleted: current.responseCompleted,
    });
    yield* host.notifyIdle;
  });

  /**
   * Finish whatever turn is open when the process goes away. An operator turn
   * that was not answered reports the cause unless we closed the process on
   * purpose (shutdown or replacement), in which case recovery owns it.
   */
  const settleOnExit = Effect.fnUntraced(function*(host: ClaudeHost, error: unknown) {
    const current = host.current;
    if (current) {
      host.current = null;
      if (!current.responseCompleted && !current.interrupted) {
        const deliberate = host.closed && host.closeReason !== "process exited";
        if (!deliberate) {
          const message = error ? getErrorMessage(error) : "Claude Code exited before answering";
          current.turnTimer("query.error", { error: message });
          yield* Effect.logError(`Claude Code ended during a turn thread=${host.threadKey}: ${message}`);
          appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, errorBlock(message));
        }
      }
      yield* finishOperatorTurn(host, current);
    }
    if (host.cliTurn) {
      const cliTurn = host.cliTurn;
      host.cliTurn = null;
      host.persistence.markPendingAsPosted(cliTurn.pendingResponseId);
      yield* releaseCliTurn(host, cliTurn);
    }
  });

  /** Interrupts the turn the process is running, closing the process if it cannot. */
  const interrupt = (host: ClaudeHost, reason: string): Effect.Effect<void> =>
    attempt(() => host.sdkQuery.interrupt()).pipe(
      Effect.catchTag("ClaudeCodeError", (error) =>
        Effect.logWarning(`interrupt failed thread=${host.threadKey}; closing the process: ${error.message}`).pipe(
          Effect.andThen(closeHost(host, reason)),
        )),
    );

  /** A turn Claude Code started on its own, typically to report a settled background task. */
  const ensureCliTurn = Effect.fnUntraced(function*(host: ClaudeHost) {
    if (host.cliTurn) {
      return host.cliTurn;
    }
    const pendingResponseId = host.persistence.createPendingResponse(host.chatId, `claude-cli-turn:${randomUUID()}`, host.sessionId);
    const stop = Effect.fnUntraced(function*(reason: StopReason) {
      const abortReason = stopReasonText(reason);
      yield* Effect.logInfo(`interrupting CLI turn thread=${host.threadKey} reason=${JSON.stringify(abortReason)}`);
      yield* interrupt(host, abortReason);
      if (host.cliTurn === cliTurn) {
        host.cliTurn = null;
        host.persistence.markPendingAsPosted(cliTurn.pendingResponseId);
        yield* releaseCliTurn(host, cliTurn);
      }
    });
    const steer = Effect.fnUntraced(function*(steerPrompt: string) {
      if (host.closed || host.cliTurn !== cliTurn) {
        return false;
      }
      const uuid = randomUUID();
      cliTurn.promptUuids.add(uuid);
      return yield* host.channel.push(buildClaudeUserMessage(steerPrompt, uuid));
    });
    const cliTurn: CliTurn = {
      pendingResponseId,
      blockSequence: [],
      promptUuids: new Set(),
      registration: yield* Scope.make(),
    };
    host.cliTurn = cliTurn;
    // The conversation is busy while it runs, so new messages get the usual Steer/Queue
    // choice; an operator's turn running already keeps the conversation its own.
    const running: RunningTurn = { cliInitiated: true, stop, steer };
    yield* activeTurns.register(host.threadKey, running, { onlyIfIdle: true }).pipe(Scope.provide(cliTurn.registration));
    yield* Effect.logInfo(`cli-turn.started thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)}`);
    return cliTurn;
  });

  const finishCliTurn = Effect.fnUntraced(function*(host: ClaudeHost, message: SDKResultMessage) {
    const cliTurn = yield* ensureCliTurn(host);
    host.cliTurn = null;
    const projected = projectResultMessage(message);
    const text = projected?.ok ? projected.text?.trim() : `Error: ${projected?.error ?? "Claude did not complete"}`;
    if (text) {
      appendBlock(cliTurn.blockSequence, host.persistence, cliTurn.pendingResponseId, {
        type: "text",
        content: text,
        phase: "final_answer",
      });
      host.persistence.markPendingResponseComplete(cliTurn.pendingResponseId);
    } else {
      host.persistence.markPendingAsPosted(cliTurn.pendingResponseId);
    }
    yield* Effect.logInfo(`cli-turn.done thread=${host.threadKey} delivered=${Boolean(text)}`);
    yield* releaseCliTurn(host, cliTurn);
    if (text) {
      yield* host.onBackgroundResponse;
    }
  });

  const handleResult = Effect.fnUntraced(function*(host: ClaudeHost, message: SDKResultMessage) {
    const current = host.current;
    const answered = current ? promptUuidsAnsweredBy(message, current.promptUuids) : [];
    if (current && answered.length > 0) {
      const projected = projectResultMessage(message);
      host.sessionId = message.session_id ?? host.sessionId;
      if (projected?.ok) {
        current.turnTimer("turn.completed");
        if (projected.text?.trim()) {
          appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
            type: "text",
            content: projected.text,
            phase: "final_answer",
          });
        }
        current.persistence.markPendingResponseComplete(current.pendingResponseId);
        current.responseCompleted = true;
        current.onTransportCompleted?.({ sessionId: host.sessionId, turnId: null });
      } else {
        current.turnTimer("turn.failed", { error: projected?.error ?? "unknown" });
        appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, errorBlock(projected?.error ?? "Claude did not complete"));
      }
      const cacheRead = cacheReadTokensFromUsage(projected?.usage);
      if (host.sessionId && cacheRead !== undefined) {
        current.persistence.updateSessionUsage(host.sessionId, { cacheReadInputTokens: cacheRead });
      }
      for (const uuid of answered) {
        current.promptUuids.delete(uuid);
        host.retiredUuids.add(uuid);
      }
      if (current.promptUuids.size === 0) {
        host.current = null;
        yield* finishOperatorTurn(host, current);
      } else {
        // A steered prompt is still unanswered; wait for it, but bounded, because the
        // CLI may have folded it into the answer it just gave.
        const previous = current.unansweredGrace;
        if (previous) {
          yield* Fiber.interrupt(previous);
        }
        current.unansweredGrace = yield* Effect.sleep(UNANSWERED_PROMPT_GRACE).pipe(
          Effect.andThen(Effect.uninterruptible(Effect.suspend(() => {
            if (host.current !== current) {
              return Effect.void;
            }
            current.unansweredGrace = undefined;
            return Effect.logInfo(`unanswered prompt grace elapsed thread=${host.threadKey} pending=${current.promptUuids.size}`).pipe(
              Effect.andThen(Effect.sync(() => {
                host.current = null;
              })),
              Effect.andThen(finishOperatorTurn(host, current)),
            );
          }))),
          Effect.forkIn(host.scope),
        );
      }
      return;
    }
    const cliUuids = host.cliTurn ? promptUuidsAnsweredBy(message, host.cliTurn.promptUuids) : [];
    const named = Array.isArray(message.user_message_uuids)
      ? message.user_message_uuids
      : typeof message.user_message_uuid === "string" ? [message.user_message_uuid] : [];
    if (named.some((uuid) => host.interruptedUuids.has(uuid))) {
      host.interruptedUuids.clear();
      yield* Effect.logInfo(`interrupted turn closed thread=${host.threadKey}`);
      return;
    }
    if (cliUuids.length > 0 || named.length === 0) {
      // Unattributed: a turn the CLI started itself. Attributed to a prompt steered into
      // such a turn: the same turn. Either way it is a reply of its own.
      yield* finishCliTurn(host, message);
      return;
    }
    if (named.some((uuid) => host.retiredUuids.has(uuid))) {
      yield* Effect.logInfo(`late result for a finished turn ignored thread=${host.threadKey}`);
      return;
    }
    // A resumed session re-runs a turn an earlier worker left interrupted and stamps
    // that turn's prompt; it is not ours and not new.
    yield* Effect.logInfo(
      `result for another turn ignored thread=${host.threadKey} uuid=${named[0] ?? "none"} resume_reason=${message.resume_reason ?? "none"}`,
    );
  });

  const handleMessage = Effect.fnUntraced(function*(host: ClaudeHost, message: SDKMessage) {
    const current = host.current;
    if (current && !current.firstEventLogged) {
      current.firstEventLogged = true;
      current.turnTimer("first_event", { event_type: `${message.type}${"subtype" in message && message.subtype ? `.${message.subtype}` : ""}` });
    }
    current?.onStarted?.();
    if (message.type === "system" && message.subtype === "init") {
      host.initialized = true;
      host.sessionId = message.session_id ?? host.sessionId;
      if (current && host.sessionId) {
        current.persistence.updatePendingSessionId(current.pendingResponseId, host.sessionId);
        current.persistence.updateActiveTurnSessionId(host.threadKey, host.sessionId);
      }
      return;
    }
    if (message.type === "system" && message.subtype === "mirror_error") {
      // The store missed a batch; startup reconciliation fills it in.
      yield* Effect.logWarning(`transcript mirror dropped a batch thread=${host.threadKey}: ${message.error ?? JSON.stringify(message).slice(0, 300)}`);
    }
    if (message.type === "system" && message.subtype === "background_tasks_changed") {
      host.backgroundTasks = Array.isArray(message.tasks) ? message.tasks : [];
      yield* Effect.logInfo(`background-tasks thread=${host.threadKey} live=${host.backgroundTasks.length}${host.backgroundTasks.length ? ` ${JSON.stringify(describeTasks(host.backgroundTasks))}` : ""}`);
      return;
    }
    if (message.type === "assistant") {
      if (!current && !host.cliTurn && host.interruptedUuids.size > 0) {
        return;
      }
      const target = current ?? (yield* ensureCliTurn(host));
      for (const item of projectAssistantMessageToItems(message)) {
        if (current && !current.firstVisibleItemLogged && isVisibleCodexItem(item)) {
          current.firstVisibleItemLogged = true;
          current.turnTimer("first_visible_item", { item_type: item.type });
        }
        mapItemToBlocks(item, {
          blockSequence: target.blockSequence,
          persistence: current?.persistence ?? host.persistence,
          pendingResponseId: target.pendingResponseId,
        });
      }
      return;
    }
    if (message.type === "result") {
      yield* handleResult(host, message);
      return;
    }
    if (message.type === "auth_status" && message.error && current) {
      current.turnTimer("event.error", { error: message.error });
      appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, errorBlock(message.error));
    }
  });

  /** Reads what the process writes until it ends, then settles what it was serving. */
  const consume = Effect.fnUntraced(function*(host: ClaudeHost) {
    const exitError = yield* Stream.fromAsyncIterable(host.sdkQuery, (cause) => new ClaudeCodeError({ cause })).pipe(
      Stream.runForEach((message) => handleMessage(host, message)),
      Effect.as(null),
      Effect.catchTag("ClaudeCodeError", (error) => Effect.succeed(error.cause)),
      // Failing to handle what it wrote ends the process too, and the turn reports why.
      Effect.catchDefect(Effect.succeed),
    );
    if (!host.closed) {
      yield* closeHost(host, "process exited");
    }
    yield* settleOnExit(host, exitError);
  });

  /**
   * Bash is disabled, so shell work arrives as code sent to bayma exec; restart
   * provenance and the database guardrail inspect the commands embedded in it.
   */
  const execHook = Effect.fnUntraced(function*(host: ClaudeHost, input: HookInput): Effect.fn.Return<HookJSONOutput> {
    if (input?.hook_event_name !== "PreToolUse") {
      return {};
    }
    const toolInput = input.tool_input;
    const code = typeof toolInput === "object" && toolInput !== null && "code" in toolInput && typeof toolInput.code === "string"
      ? toolInput.code
      : "";
    const commands = extractShellCommands(code);
    const [firstCommand] = commands;
    if (firstCommand === undefined) {
      return {};
    }
    yield* Effect.logInfo(`exec-hook seen thread=${host.threadKey} at=${yield* now} code=${JSON.stringify(code.slice(0, 120))}`);
    const command = commands.find((candidate) => isBlockedDbCommand(candidate));
    if (command) {
      if (host.current) {
        host.current.blockedGuardrailCommand = host.current.blockedGuardrailCommand ?? command;
      }
      yield* Effect.logWarning("DB guardrail denied a Claude Code Bash command");
      return {
        hookSpecificOutput: {
          hookEventName: "PreToolUse",
          permissionDecision: "deny",
          permissionDecisionReason: buildDbGuardrailSyntheticText(command),
        },
      };
    }
    const policy = host.current?.commandPolicy ?? createCommandEventPolicy({
      persistence: host.persistence,
      threadKey: host.threadKey,
      chatId: host.chatId,
      messageId: host.messageId,
      controller: { abort: () => undefined },
      log,
    });
    // One call records at most one restart event, so inspect the most telling command.
    const primary = commands.find((candidate) => looksLikeSelfRestartCommand(candidate))
      ?? commands.find((candidate) => detectWorkflowWait(candidate))
      ?? firstCommand;
    policy.inspectCommand({ command: primary, sessionId: host.sessionId });
    return {};
  });

  const startHost = Effect.fnUntraced(function*(params: TurnParams, key: string, bayma: BaymaEndpoint | null): Effect.fn.Return<ClaudeHost, ClaudeCodeError> {
    const { threadKey, resumeSession, persistence } = params;
    const claudeEnv = buildClaudeEnv();
    // A session filesystem's bayma runs in its sandbox; a folder's is the conversation's
    // own, in a host-profile Sandbox of its own.
    const mcpServers = bayma
      ? undefined
      : claudeMcpServers(yield* needed(folderBayma({ threadKey })));
    const resumeExists = resumeSession ? yield* attempt(() => sessions.sessionExists(resumeSession)) : false;
    const controller = new AbortController();
    const channel = instrumentPromptChannel(yield* makePromptChannel, threadKey);
    // Claude Code calls it only once the process runs, by when `host` is made.
    const hook: HookCallback = (input) => run.runPromise(execHook(host, input));
    const options = buildClaudeQueryOptions({
      workingDirectory,
      modelChoice: persistence.getModelChoice?.(threadKey, CLAUDE_HARNESS) ?? null,
      claudeEnv: { ...claudeEnv, ...claudeTelemetryEnv({ conversationId: threadKey }) },
      mcpServers,
      resumeSession,
      resumeExists,
      controller,
      sessionStore,
      sessionFsBayma: bayma,
      hooks: {
        PreToolUse: [{ matcher: BAYMA_EXEC_TOOL, hooks: [hook] }],
      },
    });
    // The process outlives the turn that starts it, so it starts outside that turn's
    // trace: Claude Code's traces are its own, found from a turn by its session id.
    const sdkQuery = yield* Effect.try({
      try: () => outsideTraces(() => queryFactory({ prompt: channel.prompts, options })),
      catch: (cause) => new ClaudeCodeError({ cause }),
    });
    const host: ClaudeHost = {
      threadKey,
      key,
      baymaUrl: bayma?.url ?? null,
      sessionId: resumeSession ?? null,
      channel,
      controller,
      sdkQuery,
      scope: yield* Scope.fork(scope),
      mode: resumeSession ? (resumeExists ? "resume" : "reserved") : "start",
      closed: false,
      closeReason: null,
      current: null,
      cliTurn: null,
      backgroundTasks: [],
      retiredUuids: new Set(),
      // Prompts of an interrupted turn whose closing result has not arrived; output
      // until then is that turn's tail, not a turn the CLI started itself.
      interruptedUuids: new Set(),
      persistence,
      chatId: params.chatId,
      messageId: params.messageId,
      onBackgroundResponse: Effect.void,
      notifyIdle: Effect.void,
    };
    yield* Scope.addFinalizer(host.scope, closeProcess(host, "shutdown"));
    // Read outside the turn's trace too, for as long as the process runs.
    yield* FiberSet.add(consumers, outsideTraces(() => run.runFork(consume(host))));
    yield* Effect.logInfo(`started thread=${threadKey} session=${String(host.sessionId).slice(0, 8)} mode=${host.mode}`);
    return host;
  });

  /**
   * The live process for this turn: reused when it serves the mounted session
   * with the same model, replaced otherwise. A turn with no mounted session asks
   * for a new one, so it never reuses a process.
   */
  const hostFor = Effect.fnUntraced(function*(params: TurnParams): Effect.fn.Return<ClaudeHost, ClaudeCodeError> {
    const key = modelKey(params.persistence, params.threadKey);
    // A session filesystem's Sandbox is made sure of before every turn, not only when the
    // process starts: the process outlives turns, and the Sandbox may have been suspended
    // in between. Its bayma is reached at the same address either way.
    const bayma = sessionFsBayma ? yield* needed(sessionFsBayma) : null;
    const existing = hosts.get(params.threadKey);
    if (existing && !existing.closed) {
      if (params.resumeSession && existing.sessionId === params.resumeSession && existing.key === key && existing.baymaUrl === (bayma?.url ?? null)) {
        return existing;
      }
      yield* closeHost(existing, "mounted session, model, or workspace door changed");
    }
    const host = yield* startHost(params, key, bayma);
    hosts.set(params.threadKey, host);
    return host;
  });

  const runTurn = Effect.fnUntraced(function*(params: TurnParams): Effect.fn.Return<TurnResult> {
    const { prompt, threadKey, chatId, messageId, persistence, onStarted } = params;
    const turnTimer = createTurnTimer({ harness: CLAUDE_HARNESS, threadKey, resumeSession: params.resumeSession, prompt, log });
    yield* Effect.logInfo(`Querying Claude Code (resume=${params.resumeSession})`);
    turnTimer("query.start");
    onStarted?.();
    const pendingResponseId = persistence.createPendingResponse(chatId, messageId, params.resumeSession);
    persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
    const done = yield* Deferred.make<TurnResult>();
    let host: ClaudeHost | null = null;
    /** Stops the turn, once its process runs it; one still starting is stopped as it ends. */
    const stop = Effect.fnUntraced(function*(reason: StopReason) {
      const live = host;
      if (!live || live.current !== current) {
        yield* Deferred.await(done);
        return;
      }
      const text = stopReasonText(reason);
      current.interrupted = true;
      current.blockSequence.length = 0;
      for (const uuid of current.promptUuids) {
        live.interruptedUuids.add(uuid);
      }
      current.turnTimer("query.interrupted", { reason: text });
      yield* Effect.logInfo(`Claude Code turn interrupted by operator control: ${text}`);
      yield* interrupt(live, text);
      if (live.current === current) {
        live.current = null;
        yield* finishOperatorTurn(live, current);
      }
      yield* Deferred.await(done);
    });
    const steer = Effect.fnUntraced(function*(steerPrompt: string) {
      const live = host;
      if (!live) {
        // The process is still starting; the steer follows the prompt once it is pushed.
        current.earlySteers.push(steerPrompt);
        return true;
      }
      if (live.closed || live.current !== current) {
        return false;
      }
      const uuid = randomUUID();
      current.promptUuids.add(uuid);
      return yield* live.channel.push(buildClaudeUserMessage(steerPrompt, uuid));
    });
    const current: OperatorTurn = {
      promptUuids: new Set(),
      blockSequence: [],
      pendingResponseId,
      persistence,
      turnTimer,
      onStarted,
      onTransportCompleted: params.onTransportCompleted,
      responseCompleted: false,
      interrupted: false,
      blockedGuardrailCommand: null,
      unansweredGrace: undefined,
      earlySteers: [],
      commandPolicy: createCommandEventPolicy({
        persistence,
        threadKey,
        chatId,
        messageId,
        controller: { abort: () => undefined },
        log,
      }),
      done,
      registration: yield* Scope.make(),
    };
    // Registered before the process is made sure of, so the conversation reads as busy at
    // once; the registration ends with the turn, or with this fiber if it is interrupted.
    yield* activeTurns.register(threadKey, { cliInitiated: false, stop, steer }).pipe(Scope.provide(current.registration));
    return yield* Effect.gen(function*() {
      turnTimer("env.built");
      const started = yield* hostFor(params).pipe(
        Effect.catchTag("ClaudeCodeError", (error) => {
          turnTimer("query.error", { error: error.message });
          return Effect.logError(`Error starting Claude Code: ${error.message}`).pipe(
            Effect.andThen(Effect.sync(() => appendBlock(current.blockSequence, persistence, pendingResponseId, errorBlock(error.message)))),
            Effect.as(null),
          );
        }),
      );
      if (started === null) {
        const failed: TurnResult = { blockSequence: current.blockSequence, sessionId: params.resumeSession ?? null, pendingResponseId, interrupted: false, responseCompleted: false };
        // A stop asked for while the process was starting waits for how the turn ended.
        yield* Deferred.succeed(done, failed);
        return failed;
      }
      turnTimer("query.created", { mode: started.mode });
      host = started;
      started.persistence = persistence;
      started.chatId = chatId;
      started.messageId = messageId;
      started.onBackgroundResponse = params.onBackgroundResponse ?? Effect.void;
      started.notifyIdle = params.onIdle ?? Effect.void;
      started.current = current;
      started.interruptedUuids.clear();
      const uuid = randomUUID();
      current.promptUuids.add(uuid);
      params.onPromptDispatched?.();
      yield* started.channel.push(buildClaudeUserMessage(prompt, uuid));
      for (const steerPrompt of current.earlySteers.splice(0)) {
        yield* steer(steerPrompt);
      }
      // A reused process sends no new init, so the turn learns its session here.
      if (started.initialized && started.sessionId) {
        persistence.updatePendingSessionId(pendingResponseId, started.sessionId);
        persistence.updateActiveTurnSessionId(threadKey, started.sessionId);
      }
      params.onTransportStarted?.({ sessionId: started.sessionId, turnId: null });
      return yield* Deferred.await(done);
    }).pipe(Effect.ensuring(Scope.close(current.registration, Exit.void)));
  }, withLogScope(LOG_SCOPE));

  return {
    runTurn,
    get: (threadKey) => hosts.get(threadKey) ?? null,
    close: (threadKey, reason = "closed") =>
      Effect.suspend(() => {
        const host = hosts.get(threadKey);
        return host ? closeHost(host, reason) : Effect.void;
      }).pipe(withLogScope(LOG_SCOPE)),
  };
}, withLogScope(LOG_SCOPE));
