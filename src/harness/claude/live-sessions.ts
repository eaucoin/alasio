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
 */
import { randomUUID } from "node:crypto";

import type {
  HookCallback,
  SDKBackgroundTasksChangedMessage,
  SDKResultMessage,
  SessionStore,
} from "@anthropic-ai/claude-agent-sdk";

import type { CommandEventPolicy } from "../../codex/command-event-policy.ts";
import type { ResponseBlock } from "../../codex/event-projection.ts";
import type { TurnTimer } from "../../codex/turn-timing.ts";
import type { BaymaEndpoint } from "../../kube/sandboxes.ts";
import type { BaymaMcpServer, HostBaymaScope } from "../../mcp/bayma.ts";
import type { ActiveQueries, ActiveQuery, TransportTurn, TurnParams, TurnPersistence, TurnResult } from "../index.ts";
import { CLAUDE_HARNESS } from "../names.ts";
import type { PromptChannel } from "./prompt-channel.ts";
import type { ClaudeSessionApi } from "./sessions.ts";
import {
  type ClaudeQuery,
  type ClaudeQueryFactory,
  appendBlock,
  buildClaudeEnv,
  buildClaudeQueryOptions,
  buildClaudeUserMessage,
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  cacheReadTokensFromUsage,
  claudeMcpServers,
  createCommandEventPolicy,
  createPromptChannel,
  createTurnTimer,
  defaultQueryFactory,
  getErrorMessage,
  instrumentPromptChannel,
  isBlockedDbCommand,
  isOperatorInterrupt,
  isVisibleCodexItem,
  mapItemToBlocks,
  projectAssistantMessageToItems,
  projectResultMessage,
  promptUuidsAnsweredBy,
  UNANSWERED_PROMPT_GRACE_MS,
} from "./runtime.ts";
import { getClaudeEffort, getClaudeModel } from "./model.ts";
import { BAYMA_EXEC_TOOL } from "./runtime.ts";
import { folderBaymaServer } from "../../mcp/bayma.ts";
import { extractShellCommands } from "../../policy/embedded-shell.ts";
import { looksLikeSelfRestartCommand } from "../../policy/restart-command.ts";
import { detectWorkflowWait } from "../../policy/workflow-wait.ts";
import { createLogger } from "../../shared/log.ts";
import { outsideTraces } from "../../telemetry/index.ts";
import { claudeTelemetryEnv } from "./telemetry.ts";

const log = createLogger("claude-live");

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
  unansweredTimer: NodeJS.Timeout | undefined;
  /** Steers that arrived before the process was ready, pushed after the prompt. */
  readonly earlySteers: string[];
  readonly commandPolicy: CommandEventPolicy;
  readonly resolve: (result: TurnResult) => void;
  readonly activeQuery: ActiveQuery;
  firstEventLogged?: boolean;
  firstVisibleItemLogged?: boolean;
}

/** A turn Claude Code started on its own, delivered as a reply of its own. */
export interface CliTurn {
  readonly pendingResponseId: string;
  readonly blockSequence: ResponseBlock[];
  /** Prompts steered into it. */
  readonly promptUuids: Set<string>;
  readonly activeQuery: ActiveQuery;
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
  readonly controller: AbortController;
  sdkQuery: ClaudeQuery | null;
  loop?: Promise<void>;
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
  activeQueries: ActiveQueries;
  chatId: string;
  messageId: string;
  onBackgroundResponse: () => void;
  notifyIdle: () => void;
}

/** What createClaudeLiveSessions is given. */
export interface ClaudeLiveSessionsOptions {
  readonly workingDirectory: string;
  readonly sessions: Pick<ClaudeSessionApi, "sessionExists">;
  readonly sessionStore?: SessionStore | null;
  readonly sessionFsBayma?: (() => Promise<BaymaEndpoint>) | null;
  readonly folderBayma?: (scope: Pick<HostBaymaScope, "threadKey">) => Promise<BaymaMcpServer>;
  readonly queryFactory?: ClaudeQueryFactory | undefined;
}

/** The live Claude Code processes of one working directory, one per conversation. */
export interface ClaudeLiveSessions {
  runTurn(params: TurnParams): Promise<TurnResult>;
  /** The live process serving a conversation, if any (for tests and diagnostics). */
  get(threadKey: string): ClaudeHost | null;
  close(threadKey: string, reason?: string): void;
  closeAll(reason?: string): Promise<void>;
}

function modelKey(persistence: TurnPersistence, threadKey: string): string {
  const choice = persistence.getModelChoice?.(threadKey, CLAUDE_HARNESS) ?? null;
  return JSON.stringify({ model: getClaudeModel(process.env, choice) ?? null, effort: getClaudeEffort(process.env, choice) ?? null });
}

function describeTasks(tasks: readonly BackgroundTask[]): string {
  return tasks.map((task) => task.description || task.task_type || task.task_id).join("; ");
}

/** A folder conversation's bayma, from the deployment's host profile. */
const defaultFolderBayma = ({ threadKey }: Pick<HostBaymaScope, "threadKey">) => folderBaymaServer({ harness: CLAUDE_HARNESS, threadKey });

/**
 * `workingDirectory` is where the CLI runs: a folder workspace itself, with the
 * conversation's bayma from `folderBayma()`, or a session filesystem's harness
 * directory, when `sessionFsBayma()` gives the workspace's bayma endpoint (bringing its
 * Sandbox up) and the CLI is confined to it (sessionfs.ts).
 */
export function createClaudeLiveSessions({
  workingDirectory,
  sessions,
  sessionStore = null,
  sessionFsBayma = null,
  folderBayma = defaultFolderBayma,
  queryFactory = defaultQueryFactory,
}: ClaudeLiveSessionsOptions): ClaudeLiveSessions {
  const hosts = new Map<string, ClaudeHost>();

  function closeHost(host: ClaudeHost, reason: string): void {
    if (host.closed) {
      return;
    }
    host.closed = true;
    host.closeReason = reason;
    if (hosts.get(host.threadKey) === host) {
      hosts.delete(host.threadKey);
    }
    const live = host.backgroundTasks.filter((task) => !task.ambient);
    log.info(
      `closing thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)} reason=${JSON.stringify(reason)}`
        + (live.length > 0 ? ` stopping_background_tasks=${JSON.stringify(describeTasks(live))}` : ""),
    );
    host.channel.end();
    host.controller.abort(reason);
    try {
      host.sdkQuery?.close?.();
    } catch {
      // Already closed.
    }
  }

  /**
   * Finish whatever turn is open when the process goes away. An operator turn
   * that was not answered reports the cause unless we closed the process on
   * purpose (shutdown or replacement), in which case recovery owns it.
   */
  function settleOnExit(host: ClaudeHost, error: unknown): void {
    const current = host.current;
    if (current) {
      host.current = null;
      if (!current.responseCompleted && !current.interrupted) {
        const deliberate = host.closed && host.closeReason !== "process exited";
        if (!deliberate) {
          const message = error ? getErrorMessage(error) : "Claude Code exited before answering";
          current.turnTimer("query.error", { error: message });
          log.error(`Claude Code ended during a turn thread=${host.threadKey}: ${message}`);
          appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
            type: "text",
            content: `Error: ${message}`,
          });
        }
      }
      finishOperatorTurn(host, current);
    }
    if (host.cliTurn) {
      const cliTurn = host.cliTurn;
      host.cliTurn = null;
      host.persistence.markPendingAsPosted?.(cliTurn.pendingResponseId);
      releaseCliTurn(host, cliTurn);
    }
  }

  function finishOperatorTurn(host: ClaudeHost, current: OperatorTurn): void {
    clearTimeout(current.unansweredTimer);
    for (const uuid of current.promptUuids) {
      host.retiredUuids.add(uuid);
    }
    current.promptUuids.clear();
    if (host.activeQueries.get(host.threadKey) === current.activeQuery) {
      host.activeQueries.delete(host.threadKey);
    }
    if (current.blockedGuardrailCommand && !current.responseCompleted && !current.interrupted) {
      appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
        type: "text",
        content: buildDbGuardrailFallbackText(current.blockedGuardrailCommand),
      });
    }
    current.turnTimer("query.finished", { guardrail_blocked: Boolean(current.blockedGuardrailCommand) });
    log.info(`turn.done completed=${current.responseCompleted} interrupted=${current.interrupted}`);
    current.resolve({
      blockSequence: current.blockSequence,
      sessionId: host.sessionId,
      pendingResponseId: current.pendingResponseId,
      interrupted: current.interrupted,
      responseCompleted: current.responseCompleted,
    });
    host.notifyIdle();
  }

  function releaseCliTurn(host: ClaudeHost, cliTurn: CliTurn): void {
    for (const uuid of cliTurn.promptUuids) {
      host.retiredUuids.add(uuid);
    }
    if (host.activeQueries.get(host.threadKey) === cliTurn.activeQuery) {
      host.activeQueries.delete(host.threadKey);
    }
    host.notifyIdle();
  }

  async function interrupt(host: ClaudeHost, reason: string): Promise<void> {
    try {
      // startHost sets the query before the host serves any turn, and only turns interrupt.
      await host.sdkQuery!.interrupt();
    } catch (error) {
      log.warn(`interrupt failed thread=${host.threadKey}; closing the process: ${getErrorMessage(error)}`);
      closeHost(host, reason);
    }
  }

  /** A turn Claude Code started on its own, typically to report a settled background task. */
  function ensureCliTurn(host: ClaudeHost): CliTurn {
    if (host.cliTurn) {
      return host.cliTurn;
    }
    const pendingResponseId = host.persistence.createPendingResponse(host.chatId, `claude-cli-turn:${randomUUID()}`, host.sessionId);
    const cliTurn: CliTurn = {
      pendingResponseId,
      blockSequence: [],
      promptUuids: new Set(),
      activeQuery: {
        cliInitiated: true,
        abort: async (abortReason) => {
          log.info(`interrupting CLI turn thread=${host.threadKey} reason=${JSON.stringify(abortReason)}`);
          await interrupt(host, abortReason);
          if (host.cliTurn === cliTurn) {
            host.cliTurn = null;
            host.persistence.markPendingAsPosted?.(cliTurn.pendingResponseId);
            releaseCliTurn(host, cliTurn);
          }
        },
        steer: async (steerPrompt) => {
          if (host.closed || host.cliTurn !== cliTurn) {
            return false;
          }
          const uuid = randomUUID();
          cliTurn.promptUuids.add(uuid);
          return host.channel.push(buildClaudeUserMessage(steerPrompt, uuid));
        },
      },
    };
    host.cliTurn = cliTurn;
    // The conversation is busy while it runs, so new messages get the usual Steer/Queue choice.
    if (!host.activeQueries.has(host.threadKey)) {
      host.activeQueries.set(host.threadKey, cliTurn.activeQuery);
    }
    log.info(`cli-turn.started thread=${host.threadKey} session=${String(host.sessionId).slice(0, 8)}`);
    return cliTurn;
  }

  function finishCliTurn(host: ClaudeHost, message: SDKResultMessage): void {
    const cliTurn = ensureCliTurn(host);
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
      host.persistence.markPendingAsPosted?.(cliTurn.pendingResponseId);
    }
    log.info(`cli-turn.done thread=${host.threadKey} delivered=${Boolean(text)}`);
    releaseCliTurn(host, cliTurn);
    if (text) {
      host.onBackgroundResponse();
    }
  }

  function handleResult(host: ClaudeHost, message: SDKResultMessage): void {
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
        appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
          type: "text",
          content: `Error: ${projected?.error ?? "Claude did not complete"}`,
        });
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
        finishOperatorTurn(host, current);
      } else {
        // A steered prompt is still unanswered; wait for it, but bounded, because the
        // CLI may have folded it into the answer it just gave.
        clearTimeout(current.unansweredTimer);
        current.unansweredTimer = setTimeout(() => {
          if (host.current === current) {
            log.info(`unanswered prompt grace elapsed thread=${host.threadKey} pending=${current.promptUuids.size}`);
            host.current = null;
            finishOperatorTurn(host, current);
          }
        }, UNANSWERED_PROMPT_GRACE_MS);
        current.unansweredTimer.unref?.();
      }
      return;
    }
    const cliUuids = host.cliTurn ? promptUuidsAnsweredBy(message, host.cliTurn.promptUuids) : [];
    const named = Array.isArray(message.user_message_uuids)
      ? message.user_message_uuids
      : typeof message.user_message_uuid === "string" ? [message.user_message_uuid] : [];
    if (named.some((uuid) => host.interruptedUuids.has(uuid))) {
      host.interruptedUuids.clear();
      log.info(`interrupted turn closed thread=${host.threadKey}`);
      return;
    }
    if (cliUuids.length > 0 || named.length === 0) {
      // Unattributed: a turn the CLI started itself. Attributed to a prompt steered into
      // such a turn: the same turn. Either way it is a reply of its own.
      finishCliTurn(host, message);
      return;
    }
    if (named.some((uuid) => host.retiredUuids.has(uuid))) {
      log.info(`late result for a finished turn ignored thread=${host.threadKey}`);
      return;
    }
    // A resumed session re-runs a turn an earlier worker left interrupted and stamps
    // that turn's prompt; it is not ours and not new.
    log.info(
      `result for another turn ignored thread=${host.threadKey} uuid=${named[0] ?? "none"} resume_reason=${message.resume_reason ?? "none"}`,
    );
  }

  async function consume(host: ClaudeHost, sdkQuery: ClaudeQuery): Promise<void> {
    let exitError: unknown = null;
    try {
      for await (const message of sdkQuery) {
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
          continue;
        }
        if (message.type === "system" && message.subtype === "mirror_error") {
          // The store missed a batch; startup reconciliation fills it in.
          log.warn(`transcript mirror dropped a batch thread=${host.threadKey}: ${message.error ?? JSON.stringify(message).slice(0, 300)}`);
        }
        if (message.type === "system" && message.subtype === "background_tasks_changed") {
          host.backgroundTasks = Array.isArray(message.tasks) ? message.tasks : [];
          log.info(`background-tasks thread=${host.threadKey} live=${host.backgroundTasks.length}${host.backgroundTasks.length ? ` ${JSON.stringify(describeTasks(host.backgroundTasks))}` : ""}`);
          continue;
        }
        if (message.type === "assistant") {
          if (!current && !host.cliTurn && host.interruptedUuids.size > 0) {
            continue;
          }
          const target = current ?? ensureCliTurn(host);
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
          continue;
        }
        if (message.type === "result") {
          handleResult(host, message);
          continue;
        }
        if (message.type === "auth_status" && message.error && current) {
          current.turnTimer("event.error", { error: message.error });
          appendBlock(current.blockSequence, current.persistence, current.pendingResponseId, {
            type: "text",
            content: `Error: ${message.error}`,
          });
        }
      }
    } catch (error) {
      exitError = error;
    }
    if (!host.closed) {
      closeHost(host, "process exited");
    }
    settleOnExit(host, exitError);
  }

  async function startHost(params: TurnParams, key: string, bayma: BaymaEndpoint | null): Promise<ClaudeHost> {
    const { threadKey, resumeSession, persistence } = params;
    const claudeEnv = buildClaudeEnv();
    // A session filesystem's bayma runs in its sandbox; a folder's is the conversation's
    // own, in a host-profile Sandbox of its own.
    const mcpServers = bayma
      ? undefined
      : claudeMcpServers(await folderBayma({ threadKey }));
    const resumeExists = resumeSession ? await sessions.sessionExists(resumeSession) : false;
    const controller = new AbortController();
    const channel = instrumentPromptChannel(createPromptChannel(), { threadKey, log });
    const host: ClaudeHost = {
      threadKey,
      key,
      baymaUrl: bayma?.url ?? null,
      sessionId: resumeSession ?? null,
      channel,
      controller,
      sdkQuery: null,
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
      activeQueries: params.activeQueries,
      chatId: params.chatId,
      messageId: params.messageId,
      onBackgroundResponse: () => undefined,
      notifyIdle: () => undefined,
    };
    // Bash is disabled, so shell work arrives as code sent to bayma exec; restart
    // provenance and the database guardrail inspect the commands embedded in it.
    const execHook: HookCallback = async (input) => {
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
      log.info(`exec-hook seen thread=${threadKey} at=${new Date().toISOString()} code=${JSON.stringify(code.slice(0, 120))}`);
      const command = commands.find((candidate) => isBlockedDbCommand(candidate));
      if (command) {
        if (host.current) {
          host.current.blockedGuardrailCommand = host.current.blockedGuardrailCommand ?? command;
        }
        log.warn("DB guardrail denied a Claude Code Bash command");
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
        threadKey,
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
    };
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
        PreToolUse: [{ matcher: BAYMA_EXEC_TOOL, hooks: [execHook] }],
      },
    });
    // The process outlives the turn that starts it, so it starts outside that turn's
    // trace: Claude Code's traces are its own, found from a turn by its session id.
    outsideTraces(() => {
      const sdkQuery = queryFactory({ prompt: channel.iterable, options });
      host.sdkQuery = sdkQuery;
      host.loop = consume(host, sdkQuery);
    });
    log.info(`started thread=${threadKey} session=${String(host.sessionId).slice(0, 8)} mode=${host.mode}`);
    return host;
  }

  /**
   * The live process for this turn: reused when it serves the mounted session
   * with the same model, replaced otherwise. A turn with no mounted session asks
   * for a new one, so it never reuses a process.
   */
  async function hostFor(params: TurnParams): Promise<ClaudeHost> {
    const key = modelKey(params.persistence, params.threadKey);
    // A session filesystem's Sandbox is made sure of before every turn, not only when the
    // process starts: the process outlives turns, and the Sandbox may have been suspended
    // in between. Its bayma is reached at the same address either way.
    const bayma = sessionFsBayma ? await sessionFsBayma() : null;
    const existing = hosts.get(params.threadKey);
    if (existing && !existing.closed) {
      if (params.resumeSession && existing.sessionId === params.resumeSession && existing.key === key && existing.baymaUrl === (bayma?.url ?? null)) {
        return existing;
      }
      closeHost(existing, "mounted session, model, or workspace door changed");
    }
    const host = await startHost(params, key, bayma);
    hosts.set(params.threadKey, host);
    return host;
  }

  async function runTurn(params: TurnParams): Promise<TurnResult> {
    const { prompt, threadKey, chatId, messageId, persistence, activeQueries, onStarted } = params;
    const turnTimer = createTurnTimer({ harness: CLAUDE_HARNESS, threadKey, resumeSession: params.resumeSession, prompt, log });
    log.info(`Querying Claude Code (resume=${params.resumeSession})`);
    turnTimer("query.start");
    onStarted?.();
    const pendingResponseId = persistence.createPendingResponse(chatId, messageId, params.resumeSession);
    persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
    const { promise: done, resolve } = Promise.withResolvers<TurnResult>();
    let host: ClaudeHost | null = null;
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
      unansweredTimer: undefined,
      earlySteers: [],
      commandPolicy: createCommandEventPolicy({
        persistence,
        threadKey,
        chatId,
        messageId,
        controller: { abort: () => undefined },
        log,
      }),
      resolve,
      activeQuery: {
        abort: async (reason) => {
          if (!host || host.current !== current) {
            await done;
            return;
          }
          current.interrupted = isOperatorInterrupt(reason);
          current.blockSequence.length = 0;
          for (const uuid of current.promptUuids) {
            host.interruptedUuids.add(uuid);
          }
          current.turnTimer("query.interrupted", { reason: getErrorMessage(reason) });
          log.info(`Claude Code turn interrupted by operator control: ${getErrorMessage(reason)}`);
          await interrupt(host, reason);
          if (host.current === current) {
            host.current = null;
            finishOperatorTurn(host, current);
          }
          await done;
        },
        steer: async (steerPrompt) => {
          if (!host) {
            // The process is still starting; the steer follows the prompt once it is pushed.
            current.earlySteers.push(steerPrompt);
            return true;
          }
          if (host.closed || host.current !== current) {
            return false;
          }
          const uuid = randomUUID();
          current.promptUuids.add(uuid);
          return host.channel.push(buildClaudeUserMessage(steerPrompt, uuid));
        },
      },
    };
    // Registered before any await so the conversation reads as busy immediately.
    activeQueries.set(threadKey, current.activeQuery);
    try {
      turnTimer("env.built");
      host = await hostFor(params);
      turnTimer("query.created", { mode: host.mode });
    } catch (error) {
      activeQueries.delete(threadKey);
      const message = getErrorMessage(error);
      turnTimer("query.error", { error: message });
      log.error(`Error starting Claude Code: ${message}`);
      appendBlock(current.blockSequence, persistence, pendingResponseId, { type: "text", content: `Error: ${message}` });
      return { blockSequence: current.blockSequence, sessionId: params.resumeSession ?? null, pendingResponseId, interrupted: false, responseCompleted: false };
    }
    host.persistence = persistence;
    host.activeQueries = activeQueries;
    host.chatId = chatId;
    host.messageId = messageId;
    host.onBackgroundResponse = params.onBackgroundResponse ?? (() => undefined);
    host.notifyIdle = params.onIdle ?? (() => undefined);
    host.current = current;
    host.interruptedUuids.clear();
    const uuid = randomUUID();
    current.promptUuids.add(uuid);
    host.channel.push(buildClaudeUserMessage(prompt, uuid));
    for (const steerPrompt of current.earlySteers.splice(0)) {
      await current.activeQuery.steer(steerPrompt);
    }
    // A reused process sends no new init, so the turn learns its session here.
    if (host.initialized && host.sessionId) {
      persistence.updatePendingSessionId(pendingResponseId, host.sessionId);
      persistence.updateActiveTurnSessionId(threadKey, host.sessionId);
    }
    params.onTransportStarted?.({ sessionId: host.sessionId, turnId: null });
    return await done;
  }

  return {
    runTurn,
    /** The live process serving a conversation, if any (for tests and diagnostics). */
    get(threadKey) {
      return hosts.get(threadKey) ?? null;
    },
    close(threadKey, reason = "closed") {
      const host = hosts.get(threadKey);
      if (host) {
        closeHost(host, reason);
      }
    },
    async closeAll(reason = "shutdown") {
      const all = [...hosts.values()];
      for (const host of all) {
        closeHost(host, reason);
      }
      await Promise.all(all.map((host) => host.loop?.catch(() => undefined)));
    },
  };
}
