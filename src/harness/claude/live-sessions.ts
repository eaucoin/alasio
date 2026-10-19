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
import { Clock, Deferred, Effect, Exit, Fiber, FiberSet, Schedule, Scope, Stream } from "effect";

import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  type CommandEventPolicy,
  createCommandEventPolicy,
} from "../../codex/command-event-policy.ts";
import {
  appendBlock,
  completeResponse,
  errorBlock,
  isVisibleCodexItem,
  mapItemToBlocks,
  responseProjection,
  type ResponseProjection,
  storeBlocks,
} from "../../codex/event-projection.ts";
import { createTurnTimer, type TurnTimer } from "../../codex/turn-timing.ts";
import type { BaymaEndpoint } from "../../sandbox/gateway-client.ts";
import type { ModelChoice } from "../../persistence/conversation-repository.ts";
import type { StoreError } from "../../persistence/sql.ts";
import type { TurnFenced } from "../../persistence/store.ts";
import type { TurnOutcome } from "../../persistence/turn-repository.ts";
import { type FolderBaymaOutcome, type FolderBaymaScope, type NoFolderWorkspaces, noFolderBayma } from "../../mcp/bayma.ts";
import { isBlockedDbCommand } from "../../policy/db-guardrail.ts";
import { extractShellCommands } from "../../policy/embedded-shell.ts";
import { looksLikeSelfRestartCommand } from "../../policy/restart-command.ts";
import { detectWorkflowWait } from "../../policy/workflow-wait.ts";
import { withLogScope } from "../../shared/log.ts";
import type { SessionError } from "../../sandbox/index.ts";
import { outsideTraces } from "../../telemetry/index.ts";
import { ActiveTurns, type RunningTurn, type StopReason, stopReasonText } from "../active-turns.ts";
import type { OwnTurn, OwnTurns, TransportTurn, TurnParams, TurnPersistence, TurnResult } from "../index.ts";
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

/** How long a turn waits for a steered prompt the CLI has not answered once its own prompt is answered. */
const UNANSWERED_PROMPT_GRACE = "15 seconds";

/** How often a CLI turn waiting on a fork's hold of its conversation looks again. */
const HOLD_RECHECK = "1 second";

/** The pending response of a CLI turn that has not claimed its conversation: none yet. */
const UNCLAIMED = "";

/** A background task Claude Code reports live: a shell or agent it started that is still running. */
export type BackgroundTask = SDKBackgroundTasksChangedMessage["tasks"][number];

/**
 * The turn an operator's prompt started on a live process: the prompts it waits on
 * answers to (its own and any steered into it), the response it is building, and how
 * it reports back when it ends.
 */
export interface OperatorTurn {
  readonly promptUuids: Set<string>;
  readonly projection: ResponseProjection;
  readonly persistence: TurnPersistence;
  readonly turnTimer: TurnTimer;
  readonly onTransportCompleted: ((turn: TransportTurn) => Effect.Effect<void>) | undefined;
  responseCompleted: boolean;
  interrupted: boolean;
  /** The first command the database guardrail denied in this turn. */
  blockedGuardrailCommand: string | null;
  /** Bounds the wait for a steered prompt's answer once the turn's own prompt is answered. */
  unansweredGrace: Fiber.Fiber<void> | undefined;
  /** Steers that arrived before the process was ready, pushed after the prompt. */
  readonly earlySteers: string[];
  readonly commandPolicy: CommandEventPolicy;
  /** How the turn ended, once it has, and its response stored; or how storing it failed. */
  readonly done: Deferred.Deferred<TurnResult, StoreError | TurnFenced>;
  /** Its registration as its conversation's running turn, closed as it ends. */
  readonly registration: Scope.Closeable;
  firstEventLogged?: boolean;
  firstVisibleItemLogged?: boolean;
}

/**
 * A turn Claude Code started on its own, delivered as a reply of its own. It holds its
 * conversation busy, and claims it, as any turn does; where another turn holds it as it
 * starts, it runs unclaimed, its blocks kept here, and claims the conversation once that
 * turn has let go, to deliver its reply.
 */
export interface CliTurn {
  /** Its response so far: its pending response's, once it has claimed its conversation; under UNCLAIMED, unstored, until then. */
  projection: ResponseProjection;
  /** Its claim of its conversation, and the policy of its commands under it; null until it claims it. */
  claim: { readonly own: OwnTurn; readonly commandPolicy: CommandEventPolicy } | null;
  /** How the operator's controls reach it, once it holds its conversation. */
  readonly running: RunningTurn;
  /** Prompts steered into it. */
  readonly promptUuids: Set<string>;
  /** Its registration as its conversation's running turn, closed as it ends. */
  readonly registration: Scope.Closeable;
  /**
   * Where it began while another turn held its conversation, or a report before it waited
   * to: its wait to hold the conversation and claim it, as it runs; and what it lets the
   * report after it go with, once it is let go of. Null where it held the conversation as
   * it began.
   */
  taking: { readonly fiber: Fiber.Fiber<void, StoreError | TurnFenced>; readonly delivered: Deferred.Deferred<void> } | null;
}

/** How a live process came to serve its session: resuming it, starting it under a reserved id, or starting a new one. */
export type ClaudeHostMode = "resume" | "reserved" | "start";

/** The live Claude Code process serving one conversation, and the turns it is running. */
export interface ClaudeHost {
  readonly threadKey: string;
  /** The model and effort it runs on, as modelKey renders them. */
  readonly key: string;
  readonly baymaUrl: string | null;
  /** Why it runs without its folder's bayma, which its turns' prompts are told, as its last bind said: none where it has it, or runs in a session filesystem. */
  baymaNotice: string | null;
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
  /** Done once the last of the CLI turns' replies waiting for the conversation is delivered; null while none waits. */
  reportsWaiting: Deferred.Deferred<void> | null;
  backgroundTasks: BackgroundTask[];
  /** Prompts of finished turns, whose late results are ignored. */
  readonly retiredUuids: Set<string>;
  readonly interruptedUuids: Set<string>;
  persistence: TurnPersistence;
  /** How a turn the CLI starts on its own claims the conversation: as the latest operator turn's says. */
  ownTurns: OwnTurns;
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
  readonly folderBayma?: (scope: Omit<FolderBaymaScope, "harness">) => Effect.Effect<FolderBaymaOutcome, NoFolderWorkspaces>;
  readonly queryFactory?: ClaudeQueryFactory | undefined;
}

/** The live Claude Code processes of one working directory, one per conversation, for as long as their scope is open. */
export interface ClaudeLiveSessions {
  readonly runTurn: (params: TurnParams) => Effect.Effect<TurnResult, StoreError | TurnFenced>;
  /** The live process serving a conversation, if any (for tests and diagnostics). */
  readonly get: (threadKey: string) => ClaudeHost | null;
}

function modelKey(choice: ModelChoice | null): string {
  return JSON.stringify({ model: getClaudeModel(process.env, choice) ?? null, effort: getClaudeEffort(process.env, choice) ?? null });
}

function describeTasks(tasks: readonly BackgroundTask[]): string {
  return tasks.map((task) => task.description || task.task_type || task.task_id).join("; ");
}

/** A folder conversation's bayma where none is given: a deployment's without folder workspaces. */
const defaultFolderBayma = (scope: Omit<FolderBaymaScope, "harness">) => noFolderBayma({ ...scope, harness: CLAUDE_HARNESS });

/** A promise of the Agent SDK's, failing as Claude Code failed it. */
const attempt = <A>(evaluate: () => Promise<A>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.tryPromise({ try: evaluate, catch: (cause) => new ClaudeCodeError({ cause }) });

/** What starting Claude Code needs from alasio's services, failing as starting it fails. */
const needed = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.mapError(effect, (cause) => new ClaudeCodeError({ cause }));

/** Now, as a log line gives it. */
const now = Effect.map(Clock.currentTimeMillis, (millis) => new Date(millis).toISOString());

/**
 * Ends a turn the CLI started: its prompts retired, its wait to hold the conversation given
 * up where it waits still, the conversation let go of, and the report after it let go.
 */
const releaseCliTurn = (host: ClaudeHost, cliTurn: CliTurn): Effect.Effect<void> =>
  Effect.suspend(() => {
    for (const uuid of cliTurn.promptUuids) {
      host.retiredUuids.add(uuid);
    }
    const taking = cliTurn.taking;
    return (taking ? Fiber.interrupt(taking.fiber) : Effect.void).pipe(
      Effect.andThen(Scope.close(cliTurn.registration, Exit.void)),
      Effect.andThen(taking
        ? Deferred.succeed(taking.delivered, undefined).pipe(Effect.andThen(Effect.sync(() => {
          if (host.reportsWaiting === taking.delivered) host.reportsWaiting = null;
        })))
        : Effect.void),
    );
  }).pipe(Effect.andThen(host.notifyIdle));

/**
 * Ends a CLI turn as `outcome` says, with no reply: its response, never completed, let go
 * of, and its claim ended, where it claimed its conversation. It only tidies, so a failure
 * is only said.
 */
const endCliTurn = (host: ClaudeHost, cliTurn: CliTurn, outcome: TurnOutcome): Effect.Effect<void> => {
  const claim = cliTurn.claim;
  return claim
    ? claim.own.persistence.markPendingAsPosted(cliTurn.projection.pendingResponseId).pipe(
      Effect.andThen(claim.own.end(outcome)),
      Effect.catch((error) => Effect.logWarning(`could not end a CLI turn thread=${host.threadKey}: ${error.message}`)),
    )
    : Effect.void;
};

/**
 * The live processes of `workingDirectory`, for as long as the scope they are made in
 * is open. `workingDirectory` is where the CLI runs: a folder workspace itself, with
 * the conversation's bayma from `folderBayma()`, or a session filesystem's harness
 * directory, when `sessionFsBayma()` gives the workspace's bayma endpoint (binding the
 * workspace) and the CLI is confined to it (sessionfs.ts).
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
  // Runs Claude Code's hooks, which the Agent SDK calls and awaits as promises, with what
  // the live sessions run with (alasio's logger and tracer among it).
  const runHook = Effect.runPromiseWith(yield* Effect.context<never>());
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
      appendBlock(current.projection, {
        type: "text",
        content: buildDbGuardrailFallbackText(current.blockedGuardrailCommand),
      });
    }
    yield* current.turnTimer("query.finished", { guardrail_blocked: Boolean(current.blockedGuardrailCommand) });
    yield* Effect.logInfo(`turn.done completed=${current.responseCompleted} interrupted=${current.interrupted}`);
    yield* Deferred.complete(current.done, storeBlocks(current.projection, current.persistence).pipe(Effect.as({
      blockSequence: current.projection.blockSequence,
      sessionId: host.sessionId,
      pendingResponseId: current.projection.pendingResponseId,
      interrupted: current.interrupted,
      responseCompleted: current.responseCompleted,
    })));
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
          yield* current.turnTimer("query.error", { error: message });
          yield* Effect.logError(`Claude Code ended during a turn thread=${host.threadKey}: ${message}`);
          appendBlock(current.projection, errorBlock(message));
        }
      }
      yield* finishOperatorTurn(host, current);
    }
    if (host.cliTurn) {
      const cliTurn = host.cliTurn;
      host.cliTurn = null;
      yield* stopTaking(cliTurn);
      yield* endCliTurn(host, cliTurn, "incomplete");
      yield* releaseCliTurn(host, cliTurn);
    }
  });

  /**
   * Settles the turn a write was refused for, as another turn has claimed the conversation
   * since: it is ended, as it no longer holds it, and the process reads on.
   */
  const settleFenced = Effect.fnUntraced(function*(host: ClaudeHost, fenced: TurnFenced) {
    yield* Effect.logWarning(`a write of a turn superseded was refused thread=${host.threadKey}: ${fenced.message}`);
    const current = host.current;
    if (current && current.persistence.epoch === fenced.epoch) {
      host.current = null;
      yield* finishOperatorTurn(host, current);
    }
    const cliTurn = host.cliTurn;
    if (cliTurn && cliTurn.claim?.own.persistence.epoch === fenced.epoch) {
      host.cliTurn = null;
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

  /**
   * Claims the conversation for a CLI turn that holds it busy: its pending response begun,
   * with the blocks it has so far, and only then the turn's claim, after which what it says
   * is stored as it says it, and its reply delivered. Not interrupted midway, so a claim made
   * is the turn's, which ends it; a claim whose response could not be begun, or its blocks
   * stored, is ended here.
   */
  const claimCliTurn = (host: ClaudeHost, cliTurn: CliTurn): Effect.Effect<void, StoreError | TurnFenced> =>
    Effect.uninterruptible(Effect.gen(function*() {
      const messageId = `claude-cli-turn:${randomUUID()}`;
      const own = yield* host.ownTurns.claim(messageId, host.sessionId);
      yield* Effect.gen(function*() {
        const pendingResponseId = yield* own.persistence.createPendingResponse(host.chatId, messageId, host.sessionId);
        cliTurn.projection = { ...cliTurn.projection, pendingResponseId };
        yield* storeBlocks(cliTurn.projection, own.persistence);
        cliTurn.claim = { own, commandPolicy: createCommandEventPolicy({ persistence: own.persistence, chatId: host.chatId, messageId }) };
      }).pipe(
        Effect.tapError(() =>
          own.end("failed").pipe(Effect.catch((error) => Effect.logWarning(`could not end a CLI turn's claim thread=${host.threadKey}: ${error.message}`)))
        ),
      );
    }));

  /**
   * Gives up a CLI turn's wait to hold its conversation, where it waits still, once its
   * claim, if it is making one, is made: so that what ends the turn ends that claim too.
   */
  const stopTaking = (cliTurn: CliTurn): Effect.Effect<void> => (cliTurn.taking ? Effect.asVoid(Fiber.interrupt(cliTurn.taking.fiber)) : Effect.void);

  /** Whether no hold that is no turn's (a fork's) keeps the conversation; where that cannot be told, one does, as said in alasio's log. */
  const freeOfHolds = (host: ClaudeHost): Effect.Effect<boolean> =>
    host.ownTurns.held.pipe(
      Effect.map((held) => !held),
      Effect.catch((error) => Effect.as(Effect.logWarning(`could not tell whether a fork holds thread=${host.threadKey}, so a CLI turn waits: ${error.message}`), false)),
    );

  /**
   * Holds the conversation busy for a CLI turn, as its running turn, unless another turn
   * holds it, or a hold that is no turn's does: whether it holds it. The hold is asked of
   * once the turn is registered, as a hold asks whether a turn runs once it is made, so
   * that of the two begun at once one sees the other; where one holds it, the turn's
   * registration is let go of.
   */
  const holdForCliTurn = (host: ClaudeHost, cliTurn: CliTurn, register: (scope: Scope.Scope) => Effect.Effect<boolean>): Effect.Effect<boolean> =>
    Effect.gen(function*() {
      const attempt = yield* Scope.fork(cliTurn.registration);
      if (!(yield* register(attempt))) return false;
      if (yield* freeOfHolds(host)) return true;
      yield* Scope.close(attempt, Exit.void);
      return false;
    });

  /** Holds the conversation busy for a CLI turn once neither another turn nor a hold that is no turn's does, waiting as long as either does. */
  const holdForCliTurnWhenFree = (host: ClaudeHost, cliTurn: CliTurn): Effect.Effect<void> =>
    holdForCliTurn(host, cliTurn, (attempt) => Effect.as(activeTurns.registerWhenIdle(host.threadKey, cliTurn.running).pipe(Scope.provide(attempt)), true)).pipe(
      Effect.repeat({ until: (held) => held, schedule: Schedule.spaced(HOLD_RECHECK) }),
      Effect.asVoid,
    );

  /**
   * Delivers a CLI turn's reply, once it has claimed its conversation: its response
   * completed and its claim ended, then the conversation let go of, however that went,
   * and the reply handed to delivery.
   */
  const deliverCliTurn = (host: ClaudeHost, cliTurn: CliTurn, own: OwnTurn): Effect.Effect<void, StoreError | TurnFenced> =>
    completeResponse(cliTurn.projection, own.persistence).pipe(
      Effect.andThen(own.end("completed")),
      Effect.andThen(Effect.logInfo(`cli-turn.done thread=${host.threadKey} delivered=true`)),
      Effect.onError(() => endCliTurn(host, cliTurn, "failed")),
      Effect.ensuring(releaseCliTurn(host, cliTurn)),
      Effect.andThen(host.onBackgroundResponse),
    );

  /**
   * A turn Claude Code started on its own, typically to report a settled background task.
   * It holds the conversation busy and claims it, unless another turn holds it, or a report
   * before it still waits to: then it runs unclaimed, and its reply waits behind theirs.
   */
  const ensureCliTurn = Effect.fnUntraced(function*(host: ClaudeHost) {
    if (host.cliTurn) {
      return host.cliTurn;
    }
    const stop = Effect.fnUntraced(function*(reason: StopReason) {
      if (host.cliTurn !== cliTurn) {
        return;
      }
      const abortReason = stopReasonText(reason);
      yield* Effect.logInfo(`interrupting CLI turn thread=${host.threadKey} reason=${JSON.stringify(abortReason)}`);
      yield* interrupt(host, abortReason);
      if (host.cliTurn === cliTurn) {
        host.cliTurn = null;
        yield* stopTaking(cliTurn);
        yield* endCliTurn(host, cliTurn, "interrupted");
        yield* releaseCliTurn(host, cliTurn);
      }
    }, withLogScope(LOG_SCOPE));
    const steer = Effect.fnUntraced(function*(steerPrompt: string) {
      if (host.closed || host.cliTurn !== cliTurn) {
        return false;
      }
      const uuid = randomUUID();
      cliTurn.promptUuids.add(uuid);
      return yield* host.channel.push(buildClaudeUserMessage(steerPrompt, uuid));
    }, withLogScope(LOG_SCOPE));
    const cliTurn: CliTurn = {
      projection: responseProjection(UNCLAIMED),
      claim: null,
      // The conversation is busy while it runs, so new messages get the usual Steer/Queue
      // choice; an operator's turn running already keeps the conversation its own.
      running: { cliInitiated: true, stop, steer },
      promptUuids: new Set(),
      registration: yield* Scope.make(),
      taking: null,
    };
    host.cliTurn = cliTurn;
    const held = host.reportsWaiting === null
      && (yield* holdForCliTurn(host, cliTurn, (attempt) => activeTurns.register(host.threadKey, cliTurn.running, { onlyIfIdle: true }).pipe(Scope.provide(attempt))));
    if (held) {
      yield* claimCliTurn(host, cliTurn).pipe(
        Effect.onError(() => Effect.sync(() => void (host.cliTurn = null)).pipe(Effect.andThen(releaseCliTurn(host, cliTurn)))),
      );
      yield* Effect.logInfo(`cli-turn.started thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)}`);
      return cliTurn;
    }
    // Another turn holds the conversation, as an operator's turn that has just ended holds it
    // until it has let go, or a fork's hold does, or a report before this one waits to: this
    // one waits behind that report, then holds the conversation and claims it, while it runs,
    // so a prompt that comes once the other has let go finds it busy, and its reply is
    // delivered under its claim. The process reads on meanwhile.
    const before = host.reportsWaiting;
    const delivered = yield* Deferred.make<void>();
    host.reportsWaiting = delivered;
    const fiber = yield* (before ? Deferred.await(before) : Effect.void).pipe(
      Effect.andThen(holdForCliTurnWhenFree(host, cliTurn)),
      Effect.andThen(claimCliTurn(host, cliTurn)),
      Effect.andThen(Effect.logInfo(`cli-turn.claimed thread=${host.threadKey} as the turn before it let go`)),
      Effect.forkIn(scope),
    );
    cliTurn.taking = { fiber, delivered };
    yield* Effect.logInfo(`cli-turn.started thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)} unclaimed: another turn holds the conversation, or a fork's hold does, or a report waits to`);
    return cliTurn;
  });

  const finishCliTurn = Effect.fnUntraced(function*(host: ClaudeHost, message: SDKResultMessage) {
    const cliTurn = yield* ensureCliTurn(host);
    host.cliTurn = null;
    const projected = projectResultMessage(message);
    const text = projected?.ok ? projected.text?.trim() : `Error: ${projected?.error ?? "Claude did not complete"}`;
    if (!text) {
      yield* stopTaking(cliTurn);
      yield* endCliTurn(host, cliTurn, "incomplete");
      yield* Effect.logInfo(`cli-turn.done thread=${host.threadKey} delivered=false`);
      yield* releaseCliTurn(host, cliTurn);
      return;
    }
    appendBlock(cliTurn.projection, {
      type: "text",
      content: text,
      phase: "final_answer",
    });
    const taking = cliTurn.taking;
    if (!taking) {
      if (cliTurn.claim) yield* deliverCliTurn(host, cliTurn, cliTurn.claim.own);
      return;
    }
    // It began while another turn held its conversation (ensureCliTurn): the reply is
    // delivered under its claim once that is made, whether it is being made or yet to be.
    // The process reads on meanwhile.
    yield* Fiber.join(taking.fiber).pipe(
      Effect.flatMap(() => (cliTurn.claim ? deliverCliTurn(host, cliTurn, cliTurn.claim.own) : Effect.void)),
      Effect.catch((error) =>
        Effect.logWarning(`could not deliver a CLI turn's reply thread=${host.threadKey}: ${error.message}`).pipe(
          Effect.andThen(endCliTurn(host, cliTurn, "failed")),
        )
      ),
      // However it went, the conversation is let go of, and the report after it may go.
      Effect.ensuring(releaseCliTurn(host, cliTurn)),
      Effect.forkIn(scope),
    );
  });

  const handleResult = Effect.fnUntraced(function*(host: ClaudeHost, message: SDKResultMessage) {
    const current = host.current;
    const answered = current ? promptUuidsAnsweredBy(message, current.promptUuids) : [];
    if (current && answered.length > 0) {
      const projected = projectResultMessage(message);
      host.sessionId = message.session_id ?? host.sessionId;
      if (projected?.ok) {
        yield* current.turnTimer("turn.completed");
        if (projected.text?.trim()) {
          appendBlock(current.projection, {
            type: "text",
            content: projected.text,
            phase: "final_answer",
          });
        }
        yield* completeResponse(current.projection, current.persistence);
        current.responseCompleted = true;
        if (current.onTransportCompleted) {
          yield* current.onTransportCompleted({ sessionId: host.sessionId, turnId: null });
        }
      } else {
        yield* current.turnTimer("turn.failed", { error: projected?.error ?? "unknown" });
        appendBlock(current.projection, errorBlock(projected?.error ?? "Claude did not complete"));
      }
      const cacheRead = cacheReadTokensFromUsage(projected?.usage);
      if (host.sessionId && cacheRead !== undefined) {
        yield* current.persistence.updateSessionUsage(host.sessionId, { cacheReadInputTokens: cacheRead });
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
      yield* current.turnTimer("first_event", { event_type: `${message.type}${"subtype" in message && message.subtype ? `.${message.subtype}` : ""}` });
    }
    if (message.type === "system" && message.subtype === "init") {
      host.initialized = true;
      host.sessionId = message.session_id ?? host.sessionId;
      if (current && host.sessionId) {
        yield* current.persistence.updatePendingSessionId(current.projection.pendingResponseId, host.sessionId);
        yield* current.persistence.updateActiveTurnSessionId(host.sessionId);
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
          yield* current.turnTimer("first_visible_item", { item_type: item.type });
        }
        mapItemToBlocks(item, target.projection);
      }
      // A CLI turn's blocks are stored once it has claimed its conversation.
      const writer = current ? current.persistence : host.cliTurn?.claim?.own.persistence;
      if (writer) {
        yield* storeBlocks(target.projection, writer);
      }
      return;
    }
    if (message.type === "result") {
      yield* handleResult(host, message);
      return;
    }
    if (message.type === "auth_status" && message.error && current) {
      yield* current.turnTimer("event.error", { error: message.error });
      appendBlock(current.projection, errorBlock(message.error));
      yield* storeBlocks(current.projection, current.persistence);
    }
  });

  /** Reads what the process writes until it ends, then settles what it was serving. */
  const consume = Effect.fnUntraced(function*(host: ClaudeHost) {
    const exitError = yield* Stream.fromAsyncIterable(host.sdkQuery, (cause) => new ClaudeCodeError({ cause })).pipe(
      // A write refused, as another turn has claimed the conversation since, ends the turn
      // it was for, and nothing else.
      Stream.runForEach((message) => handleMessage(host, message).pipe(Effect.catchTag("TurnFenced", (fenced) => settleFenced(host, fenced)))),
      Effect.as(null),
      Effect.catchTag("ClaudeCodeError", (error) => Effect.succeed(error.cause)),
      // Failing to handle what it wrote, alasio's store failing among it, ends the process
      // too, and the turn reports why.
      Effect.catchTag("StoreError", Effect.succeed),
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
    const policy = host.current?.commandPolicy ?? host.cliTurn?.claim?.commandPolicy ?? createCommandEventPolicy({
      persistence: host.persistence,
      chatId: host.chatId,
      messageId: host.messageId,
    });
    // One call records at most one restart event, so inspect the most telling command.
    const primary = commands.find((candidate) => looksLikeSelfRestartCommand(candidate))
      ?? commands.find((candidate) => detectWorkflowWait(candidate))
      ?? firstCommand;
    // The command runs whether or not what it sets off could be recorded.
    yield* policy.inspectCommand({ command: primary, sessionId: host.sessionId }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not record what a command sets off thread=${host.threadKey}: ${error.message}`)),
    );
    return {};
  });

  const startHost = Effect.fnUntraced(function*(params: TurnParams, key: string, bayma: BaymaEndpoint | null, folder: FolderBaymaOutcome | null): Effect.fn.Return<ClaudeHost, ClaudeCodeError> {
    const { threadKey, resumeSession, persistence } = params;
    const claudeEnv = buildClaudeEnv();
    // A session filesystem's bayma runs in its sandbox; a folder's is the conversation's
    // own, the gateway's folder instance, where it started.
    const mcpServers = folder?.server ? claudeMcpServers(folder.server) : undefined;
    const resumeExists = resumeSession ? yield* sessions.sessionExists(resumeSession) : false;
    const controller = new AbortController();
    const channel = instrumentPromptChannel(yield* makePromptChannel, threadKey);
    // Claude Code calls it only once the process runs, by when `host` is made; it runs
    // outside every trace, as the process does.
    const hook: HookCallback = (input) => runHook(execHook(host, input).pipe(outsideTraces));
    const options = buildClaudeQueryOptions({
      workingDirectory,
      modelChoice: params.modelChoice,
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
      try: () => queryFactory({ prompt: channel.prompts, options }),
      catch: (cause) => new ClaudeCodeError({ cause }),
    }).pipe(outsideTraces);
    const host: ClaudeHost = {
      threadKey,
      key,
      baymaUrl: bayma?.url ?? null,
      baymaNotice: folder?.notice ?? null,
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
      reportsWaiting: null,
      backgroundTasks: [],
      retiredUuids: new Set(),
      // Prompts of an interrupted turn whose closing result has not arrived; output
      // until then is that turn's tail, not a turn the CLI started itself.
      interruptedUuids: new Set(),
      persistence,
      ownTurns: params.ownTurns,
      chatId: params.chatId,
      messageId: params.messageId,
      onBackgroundResponse: Effect.void,
      notifyIdle: Effect.void,
    };
    yield* Scope.addFinalizer(host.scope, closeProcess(host, "shutdown"));
    // Read outside the turn's trace too, for as long as the process runs.
    yield* FiberSet.run(consumers, consume(host).pipe(outsideTraces));
    yield* Effect.logInfo(`started thread=${threadKey} session=${String(host.sessionId).slice(0, 8)} mode=${host.mode}`);
    return host;
  });

  /**
   * The live process for this turn: reused when it serves the mounted session
   * with the same model, replaced otherwise. A turn with no mounted session asks
   * for a new one, so it never reuses a process.
   */
  const hostFor = Effect.fnUntraced(function*(params: TurnParams): Effect.fn.Return<ClaudeHost, ClaudeCodeError> {
    const key = modelKey(params.modelChoice);
    // A session filesystem is bound before every turn, not only when the process starts: the
    // process outlives turns, and the gateway may have released its binding in between. Its
    // bayma is reached at the same address either way, the process's MCP client initializing a
    // session anew with the one bound since, which knows none of the one before's
    // (test/bayma-reconnect.test.ts).
    const bayma = sessionFsBayma ? yield* needed(sessionFsBayma) : null;
    const existing = hosts.get(params.threadKey);
    const reusable = existing && !existing.closed && params.resumeSession && existing.sessionId === params.resumeSession && existing.key === key && existing.baymaUrl === (bayma?.url ?? null);
    // A folder's bayma bound as the process starts, and again before each turn of a process
    // that started without it, which starts anew once it has it; one released meanwhile is
    // bound anew by the turn's first request of it, its MCP client initializing a session anew.
    const folder = bayma || (reusable && existing.baymaNotice === null)
      ? null
      : yield* needed(folderBayma({ threadKey: params.threadKey, workingDirectory, chatId: params.chatId }));
    if (reusable && (folder === null || folder.server === null)) {
      if (folder) existing.baymaNotice = folder.notice;
      return existing;
    }
    if (existing && !existing.closed) {
      yield* closeHost(existing, reusable ? "its folder's bayma started" : "mounted session, model, or workspace door changed");
    }
    const host = yield* startHost(params, key, bayma, folder);
    hosts.set(params.threadKey, host);
    return host;
  });

  const runTurn = Effect.fnUntraced(function*(params: TurnParams): Effect.fn.Return<TurnResult, StoreError | TurnFenced> {
    const { prompt, threadKey, chatId, messageId, persistence } = params;
    const turnTimer = createTurnTimer({ harness: CLAUDE_HARNESS, threadKey, resumeSession: params.resumeSession, prompt });
    yield* Effect.logInfo(`Querying Claude Code (resume=${params.resumeSession})`);
    yield* turnTimer("query.start");
    const pendingResponseId = yield* persistence.createPendingResponse(chatId, messageId, params.resumeSession);
    yield* persistence.updateActiveTurnPendingResponseId(pendingResponseId);
    const done = yield* Deferred.make<TurnResult, StoreError | TurnFenced>();
    let host: ClaudeHost | null = null;
    /** Whether the turn's prompt is pushed to its process: before then, a stop is the turn's to take up (stoppedBeforePush). */
    let pushed = false;
    /** Why the turn is stopped, where it was before its prompt was pushed; null while it is not. */
    let stoppedFor: string | null = null;
    /**
     * Stops the turn: once its prompt is pushed, by interrupting its process; before then, as
     * its process starts or its prompt's dispatch is recorded, by the turn itself, which then
     * pushes nothing and ends as interrupted.
     */
    const stop = Effect.fnUntraced(function*(reason: StopReason) {
      // However the turn ends, the stop is done once it has.
      const ended = Effect.ignore(Deferred.await(done));
      const text = stopReasonText(reason);
      if (!pushed) {
        stoppedFor ??= text;
        yield* ended;
        return;
      }
      const live = host;
      if (!live || live.current !== current) {
        yield* ended;
        return;
      }
      current.interrupted = true;
      current.projection.blockSequence.length = 0;
      for (const uuid of current.promptUuids) {
        live.interruptedUuids.add(uuid);
      }
      yield* current.turnTimer("query.interrupted", { reason: text });
      yield* Effect.logInfo(`Claude Code turn interrupted by operator control: ${text}`);
      yield* interrupt(live, text);
      if (live.current === current) {
        live.current = null;
        yield* finishOperatorTurn(live, current);
      }
      yield* ended;
    }, withLogScope(LOG_SCOPE));
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
    }, withLogScope(LOG_SCOPE));
    const current: OperatorTurn = {
      promptUuids: new Set(),
      projection: responseProjection(pendingResponseId),
      persistence,
      turnTimer,
      onTransportCompleted: params.onTransportCompleted,
      responseCompleted: false,
      interrupted: false,
      blockedGuardrailCommand: null,
      unansweredGrace: undefined,
      earlySteers: [],
      commandPolicy: createCommandEventPolicy({
        persistence,
        chatId,
        messageId,
      }),
      done,
      registration: yield* Scope.make(),
    };
    /**
     * Ends the turn stopped, for `reason`, before its prompt was pushed to `live`, its process,
     * which it leaves idle, as finishOperatorTurn ends one: the conversation let go of, the turn
     * done, and the conversation's idle hook run, for what waits on it to go on.
     */
    const stoppedBeforePush = Effect.fnUntraced(function*(live: ClaudeHost, reason: string) {
      if (live.current === current) live.current = null;
      current.interrupted = true;
      current.projection.blockSequence.length = 0;
      current.promptUuids.clear();
      yield* current.turnTimer("query.interrupted", { reason });
      yield* Effect.logInfo(`Claude Code turn stopped before its prompt was sent: ${reason}`);
      yield* Scope.close(current.registration, Exit.void);
      const interrupted: TurnResult = { blockSequence: current.projection.blockSequence, sessionId: live.sessionId ?? params.resumeSession ?? null, pendingResponseId, interrupted: true, responseCompleted: false };
      yield* Deferred.complete(done, storeBlocks(current.projection, persistence).pipe(Effect.as(interrupted)));
      yield* params.onIdle ?? Effect.void;
      return yield* Deferred.await(done);
    });
    // Registered before the process is made sure of, so the conversation reads as busy at
    // once; the registration ends with the turn, or with this fiber if it is interrupted.
    yield* activeTurns.register(threadKey, { cliInitiated: false, stop, steer }).pipe(Scope.provide(current.registration));
    return yield* Effect.gen(function*() {
      yield* turnTimer("env.built");
      const started = yield* hostFor(params).pipe(
        Effect.catchTag("ClaudeCodeError", Effect.fnUntraced(function*(error) {
          yield* turnTimer("query.error", { error: error.message });
          yield* Effect.logError(`Error starting Claude Code: ${error.message}`);
          appendBlock(current.projection, errorBlock(error.message));
          return null;
        })),
      );
      if (started !== null && stoppedFor !== null) return yield* stoppedBeforePush(started, stoppedFor);
      if (started === null) {
        const failed: TurnResult = { blockSequence: current.projection.blockSequence, sessionId: params.resumeSession ?? null, pendingResponseId, interrupted: false, responseCompleted: false };
        // A stop asked for while the process was starting waits for how the turn ended.
        yield* Deferred.complete(done, storeBlocks(current.projection, persistence).pipe(Effect.as(failed)));
        return yield* Deferred.await(done);
      }
      yield* turnTimer("query.created", { mode: started.mode });
      host = started;
      started.persistence = persistence;
      started.ownTurns = params.ownTurns;
      started.chatId = chatId;
      started.messageId = messageId;
      started.onBackgroundResponse = params.onBackgroundResponse ?? Effect.void;
      started.notifyIdle = params.onIdle ?? Effect.void;
      started.current = current;
      started.interruptedUuids.clear();
      const uuid = randomUUID();
      current.promptUuids.add(uuid);
      if (params.onPromptDispatched) {
        yield* params.onPromptDispatched;
      }
      // Stopped as its dispatch was recorded, the turn pushes nothing; past this, a stop interrupts its process.
      if (stoppedFor !== null) return yield* stoppedBeforePush(started, stoppedFor);
      pushed = true;
      // Told, where it runs without its folder's bayma, that its REPL tools are gone.
      yield* started.channel.push(buildClaudeUserMessage(started.baymaNotice ? `${started.baymaNotice}\n\n${prompt}` : prompt, uuid));
      for (const steerPrompt of current.earlySteers.splice(0)) {
        yield* steer(steerPrompt);
      }
      // A reused process sends no new init, so the turn learns its session here.
      if (started.initialized && started.sessionId) {
        yield* persistence.updatePendingSessionId(pendingResponseId, started.sessionId);
        yield* persistence.updateActiveTurnSessionId(started.sessionId);
      }
      if (params.onTransportStarted) {
        yield* params.onTransportStarted({ sessionId: started.sessionId, turnId: null });
      }
      return yield* Deferred.await(done);
    }).pipe(Effect.ensuring(Scope.close(current.registration, Exit.void)));
  }, withLogScope(LOG_SCOPE));

  return {
    runTurn,
    get: (threadKey) => hosts.get(threadKey) ?? null,
  };
}, withLogScope(LOG_SCOPE));
