import type { v2 } from "../../../.types/codex/index.js";
import { Clock, type Duration, Effect, Schema } from "effect";

import {
  ALASIO_CODEX_MODEL,
  ALASIO_CODEX_REASONING_EFFORT,
  withAlasioCodexModelConfig,
} from "../model.ts";
import type { CodexThreadConfig } from "../thread-config.ts";
import type { AppServerNotifications } from "./notification-queue.ts";
import type { AppServerParams, AppServerResult } from "./protocol.ts";
import type { AppServerGone, AppServerRequestError, AppServerRpc, AppServerScope, AppServerStartError } from "./rpc-client.ts";

const INTERRUPT_TIMEOUT = "5 seconds";
/** Threads or turns one list request asks for. */
const LIST_PAGE_SIZE = 100;
/**
 * The thread sources the session panels list: the Codex CLI's and editors',
 * which include alasio's app-server threads, and alasio's exec transport's.
 * Sub-agent threads are left out.
 */
const LISTED_SOURCE_KINDS: v2.ThreadSourceKind[] = ["cli", "vscode", "exec"];

/** The app-server's paginated lists alasio reads whole, each a page of its items at a time. */
type ListMethod = "thread/list" | "thread/turns/list" | "model/list";

/** A thread alasio loads in the app-server, under the conversation's thread key. */
export interface ThreadOptions extends AppServerScope {
  readonly threadKey: string;
  readonly config: CodexThreadConfig;
}

export interface EnsureThreadOptions extends ThreadOptions {
  readonly threadId: string;
}

export interface ForkThreadOptions extends EnsureThreadOptions {
  readonly beforeTurnId: string;
}

export interface StartTurnOptions {
  readonly threadId: string;
  readonly prompt: string;
  readonly cwd: string;
  readonly model?: string | undefined;
  readonly effort?: string | null | undefined;
  /** Run just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: Effect.Effect<void> | undefined;
}

export interface SteerTurnOptions {
  readonly threadId: string;
  readonly turnId: string | null | undefined;
  readonly prompt: string;
}

/** A call about one thread, in the scope of the app-server that holds it. */
export interface ThreadScope extends AppServerScope {
  readonly threadId: string;
}

export interface SetGoalOptions extends ThreadScope {
  readonly objective?: string | null | undefined;
  readonly status?: v2.ThreadGoalStatus | null | undefined;
  readonly tokenBudget?: number | null | undefined;
}

/** The app-server answered a thread's start or fork with no thread. */
export class ThreadIdMissing extends Schema.TaggedError<ThreadIdMissing>()("ThreadIdMissing", {
  method: Schema.String,
}) {
  override get message(): string {
    return `Codex app-server ${this.method} did not return a thread id`;
  }
}

/** A steer with no turn to steer. */
export class NoActiveTurn extends Schema.TaggedError<NoActiveTurn>()("NoActiveTurn", {}) {
  override get message(): string {
    return "Cannot steer Codex without an active turn id";
  }
}

/** A turn left running on a thread could not be interrupted before the thread's next turn. */
export class StaleTurnCleanupError extends Schema.TaggedError<StaleTurnCleanupError>()("StaleTurnCleanupError", {
  threadId: Schema.String,
  turnId: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Failed to clean up stale Codex turn ${this.turnId} for thread ${this.threadId}: ${this.cause instanceof Error ? this.cause.message : String(this.cause)}`;
  }
}

/** A thread's work in the app-server: its threads, turns, goals, and the models it offers. */
export interface AppServerThreads {
  /** Loads the thread, unless the app-server has it loaded; the id of the thread loaded. */
  readonly ensureThread: (options: EnsureThreadOptions) => Effect.Effect<string, AppServerStartError | ThreadIdMissing>;
  readonly startThread: (options: ThreadOptions) => Effect.Effect<string, AppServerStartError | ThreadIdMissing>;
  /**
   * Forks a thread before one of its turns, leaving that turn and every later one out:
   * Codex's own fork, into a new thread loaded as a resume loads one. The new thread's id.
   */
  readonly forkThread: (options: ForkThreadOptions) => Effect.Effect<string, AppServerStartError | ThreadIdMissing>;
  /** The threads whose session ran in `cwd`, most recently updated first, without their turns. */
  readonly listThreads: (scope: AppServerScope) => Effect.Effect<v2.Thread[], AppServerStartError>;
  /** A thread's turns, newest first, each with a summary of its items. */
  readonly listTurns: (scope: ThreadScope) => Effect.Effect<v2.Turn[], AppServerStartError>;
  /** The models this machine's Codex login can use, as the app-server reports them. */
  readonly listModels: (scope: AppServerScope) => Effect.Effect<v2.Model[], AppServerStartError>;
  /** Starts a turn of the prompt, interrupting one left running first; the turn's id. */
  readonly startTurn: (options: StartTurnOptions) => Effect.Effect<string, AppServerRequestError | StaleTurnCleanupError>;
  /** Takes the turn `turnId`, which the app-server runs on its own (a goal's), as the thread's. */
  readonly claimTurn: (threadId: string, turnId: string) => Effect.Effect<void>;
  /** The id of the thread's running turn, once it has one; null if none starts within `timeout`. */
  readonly waitForTurnId: (threadId: string, timeout?: Duration.Input) => Effect.Effect<string | null, AppServerGone>;
  readonly steerTurn: (options: SteerTurnOptions) => Effect.Effect<v2.TurnSteerResponse, AppServerRequestError | NoActiveTurn>;
  /** Interrupts the thread's running turn; whether it had one. `origin` says, in the log, who asked. */
  readonly interrupt: (threadId: string, origin?: string) => Effect.Effect<boolean, AppServerRequestError>;
  readonly getGoal: (scope: ThreadScope) => Effect.Effect<v2.ThreadGoalGetResponse, AppServerStartError>;
  readonly setGoal: (options: SetGoalOptions) => Effect.Effect<v2.ThreadGoalSetResponse, AppServerStartError>;
  readonly clearGoal: (scope: ThreadScope) => Effect.Effect<v2.ThreadGoalClearResponse, AppServerStartError>;
}

/** What every thread alasio starts, resumes, or forks is loaded with. */
function threadOverrides({ cwd, config }: { readonly cwd: string; readonly config: CodexThreadConfig }) {
  return {
    cwd,
    model: ALASIO_CODEX_MODEL,
    approvalPolicy: "never",
    sandbox: "danger-full-access",
    config: withAlasioCodexModelConfig(config),
  } as const;
}

/** Milliseconds since `startedAt` (Clock.currentTimeNanos), as the log shows them. */
const millisSince = (startedAt: bigint): Effect.Effect<string> =>
  Effect.map(Clock.currentTimeNanos, (now) => (Number(now - startedAt) / 1_000_000).toFixed(1));

/** A thread's work in the app-server `rpc` talks to, its turns followed by `notifications`. */
export function makeAppServerThreads(
  rpc: Pick<AppServerRpc, "start" | "request" | "whenGone">,
  notifications: AppServerNotifications,
): AppServerThreads {
  /** Every page of one of the app-server's paginated lists. */
  const listAll = Effect.fnUntraced(function*<M extends ListMethod>(method: M, params: AppServerParams<M>): Effect.fn.Return<AppServerResult<M>["data"][number][], AppServerRequestError> {
    const all: AppServerResult<M>["data"][number][] = [];
    let cursor: string | null = null;
    do {
      const page: AppServerResult<M> = yield* rpc.request(method, { ...params, limit: LIST_PAGE_SIZE, ...(cursor ? { cursor } : {}) });
      all.push(...(page?.data ?? []));
      cursor = page?.nextCursor ?? null;
    } while (cursor);
    return all;
  });

  const interrupt = Effect.fnUntraced(function*(threadId: string, origin = "unspecified"): Effect.fn.Return<boolean, AppServerRequestError> {
    const turnId = yield* notifications.currentTurnId(threadId);
    if (!turnId) {
      return false;
    }
    yield* Effect.logInfo(`interrupting app-server turn thread=${threadId.slice(0, 8)} turn=${turnId} origin=${origin}`);
    yield* notifications.forgetTurn(threadId);
    yield* rpc.request("turn/interrupt", { threadId, turnId }, INTERRUPT_TIMEOUT);
    return true;
  });

  return {
    ensureThread: Effect.fnUntraced(function*({ threadId, threadKey, cwd, env, config }) {
      yield* rpc.start({ env, cwd });
      const startedAt = yield* Clock.currentTimeNanos;
      const loaded = yield* rpc.request("thread/loaded/list", {});
      if (Array.isArray(loaded?.data) && loaded.data.includes(threadId)) {
        yield* Effect.logInfo(`thread already loaded thread=${threadId.slice(0, 8)} key=${JSON.stringify(threadKey)} ms=${yield* millisSince(startedAt)}`);
        return threadId;
      }
      const response = yield* rpc.request("thread/resume", {
        threadId,
        excludeTurns: true,
        ...threadOverrides({ cwd, config }),
      });
      const resumedId = response?.thread?.id ?? threadId;
      yield* Effect.logInfo(`thread resumed thread=${resumedId.slice(0, 8)} key=${JSON.stringify(threadKey)} turns=${response?.thread?.turns?.length ?? "?"} ms=${yield* millisSince(startedAt)}`);
      return resumedId;
    }),

    startThread: Effect.fnUntraced(function*({ threadKey, cwd, env, config }) {
      yield* rpc.start({ env, cwd });
      const startedAt = yield* Clock.currentTimeNanos;
      const response = yield* rpc.request("thread/start", threadOverrides({ cwd, config }));
      const threadId = response?.thread?.id;
      if (!threadId) {
        return yield* new ThreadIdMissing({ method: "thread/start" });
      }
      yield* Effect.logInfo(`thread started thread=${threadId.slice(0, 8)} key=${JSON.stringify(threadKey)} ms=${yield* millisSince(startedAt)}`);
      return threadId;
    }),

    forkThread: Effect.fnUntraced(function*({ threadId, beforeTurnId, threadKey, cwd, env, config }) {
      yield* rpc.start({ env, cwd });
      const startedAt = yield* Clock.currentTimeNanos;
      const response = yield* rpc.request("thread/fork", {
        threadId,
        beforeTurnId,
        excludeTurns: true,
        ...threadOverrides({ cwd, config }),
      });
      const forkedId = response?.thread?.id;
      if (!forkedId) {
        return yield* new ThreadIdMissing({ method: "thread/fork" });
      }
      yield* Effect.logInfo(`thread forked thread=${forkedId.slice(0, 8)} from=${threadId.slice(0, 8)} before_turn=${beforeTurnId} key=${JSON.stringify(threadKey)} ms=${yield* millisSince(startedAt)}`);
      return forkedId;
    }),

    listThreads: ({ env, cwd }) =>
      rpc.start({ env, cwd }).pipe(Effect.andThen(listAll("thread/list", {
        cwd,
        sortKey: "updated_at",
        sourceKinds: LISTED_SOURCE_KINDS,
      }))),

    listTurns: ({ threadId, env, cwd }) => rpc.start({ env, cwd }).pipe(Effect.andThen(listAll("thread/turns/list", { threadId }))),

    listModels: ({ env, cwd }) => rpc.start({ env, cwd }).pipe(Effect.andThen(listAll("model/list", { includeHidden: false }))),

    startTurn: Effect.fnUntraced(function*({ threadId, prompt, cwd, model = ALASIO_CODEX_MODEL, effort = ALASIO_CODEX_REASONING_EFFORT, onPromptDispatched }) {
      const previousTurnId = yield* notifications.currentTurnId(threadId);
      if (previousTurnId) {
        yield* Effect.logWarning(`interrupting leftover app-server turn before starting a new one thread=${threadId.slice(0, 8)} turn=${previousTurnId}`);
        yield* interrupt(threadId, "stale-turn-cleanup").pipe(
          Effect.mapError((cause) => new StaleTurnCleanupError({ threadId, turnId: previousTurnId, cause })),
        );
      }
      yield* notifications.beginTurn(threadId);
      const startedAt = yield* Clock.currentTimeNanos;
      if (onPromptDispatched) yield* onPromptDispatched;
      const response = yield* rpc.request("turn/start", {
        threadId,
        input: [{ type: "text", text: prompt, text_elements: [] }],
        cwd,
        model,
        ...(effort ? { effort } : {}),
        approvalPolicy: "never",
        sandboxPolicy: { type: "dangerFullAccess" },
      });
      const responseTurnId = response?.turn?.id;
      const turnId = (yield* notifications.rememberTurn(threadId, responseTurnId)) ?? responseTurnId;
      yield* notifications.discardStaleForTurn(threadId, turnId);
      const turnDetail = responseTurnId && turnId !== responseTurnId ? ` response_turn=${responseTurnId}` : "";
      yield* Effect.logInfo(`turn start accepted thread=${threadId.slice(0, 8)} turn=${turnId ?? "unknown"}${turnDetail} ms=${yield* millisSince(startedAt)}`);
      return turnId;
    }),

    claimTurn: Effect.fnUntraced(function*(threadId, turnId) {
      if (!(yield* notifications.currentTurnId(threadId)) && !(yield* notifications.turnAliases(threadId)).has(turnId)) {
        yield* notifications.beginTurn(threadId);
      }
      yield* notifications.rememberTurn(threadId, turnId);
      yield* notifications.discardStaleForTurn(threadId, turnId);
    }),

    waitForTurnId: (threadId, timeout) =>
      Effect.flatMap(rpc.whenGone, (gone) => notifications.waitForTurnId(threadId, timeout).pipe(Effect.raceFirst(gone))),

    steerTurn: Effect.fnUntraced(function*({ threadId, turnId, prompt }) {
      const activeTurnId = (yield* notifications.currentTurnId(threadId)) ?? turnId;
      if (!activeTurnId) {
        return yield* new NoActiveTurn();
      }
      return yield* rpc.request("turn/steer", {
        threadId,
        expectedTurnId: activeTurnId,
        input: [{ type: "text", text: prompt, text_elements: [] }],
      });
    }),

    interrupt,

    getGoal: ({ threadId, cwd, env }) => rpc.start({ env, cwd }).pipe(Effect.andThen(rpc.request("thread/goal/get", { threadId }))),

    setGoal: ({ threadId, cwd, env, objective, status, tokenBudget }) => {
      const params: v2.ThreadGoalSetParams = { threadId };
      if (objective !== undefined) {
        params.objective = objective;
      }
      if (status !== undefined) {
        params.status = status;
      }
      if (tokenBudget !== undefined) {
        params.tokenBudget = tokenBudget;
      }
      return rpc.start({ env, cwd }).pipe(Effect.andThen(rpc.request("thread/goal/set", params)));
    },

    clearGoal: ({ threadId, cwd, env }) => rpc.start({ env, cwd }).pipe(Effect.andThen(rpc.request("thread/goal/clear", { threadId }))),
  };
}
