import type { v2 } from "../../../.types/codex/index.js";
import { Clock, Deferred, type Duration, Effect, Fiber, Schema } from "effect";

import {
  ALASIO_CODEX_MODEL,
  ALASIO_CODEX_REASONING_EFFORT,
  withAlasioCodexModelConfig,
} from "../model.ts";
import type { CodexThreadConfig } from "../thread-config.ts";
import type { AppServerNotifications } from "./notification-queue.ts";
import type { AppServerParams, AppServerResult } from "./protocol.ts";
import { type AppServerGone, type AppServerRequestError, AppServerRequestTimeout, type AppServerRpc, type AppServerScope, type AppServerStartError } from "./rpc-client.ts";

const INTERRUPT_TIMEOUT = "5 seconds";
/** How long a turn's start stopped as it waited is waited for still, so that the turn it starts is interrupted. */
const STOPPED_START_WAIT = "30 seconds";

/** The app-server did not answer, within STOPPED_START_WAIT, the start of a turn stopped as it waited: the turn may run on there. */
class StoppedStartUnanswered extends Schema.TaggedError<StoppedStartUnanswered>()("StoppedStartUnanswered", {}) {
  override get message(): string {
    return `the app-server did not answer the turn's start within ${STOPPED_START_WAIT}`;
  }
}

/** Threads or turns one list request asks for. */
const LIST_PAGE_SIZE = 100;
/** How long the app-server is given to say whether a conversation has Codex working in it (backgroundWork). */
const BACKGROUND_WORK_TIMEOUT = "5 seconds";
/** How long a turn waits for the login's models (loginModels) before it runs on the model chosen. */
const LOGIN_MODELS_TIMEOUT = "10 seconds";
/**
 * The thread sources the session panels list: the Codex CLI's and editors',
 * which include alasio's app-server threads, and alasio's exec transport's.
 * Sub-agent threads are left out.
 */
const LISTED_SOURCE_KINDS: v2.ThreadSourceKind[] = ["cli", "vscode", "exec"];

/** The app-server's paginated lists alasio reads whole, each a page of its items at a time. */
type ListMethod = "thread/list" | "thread/turns/list" | "model/list" | "thread/backgroundTerminals/list";

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
  /**
   * Every model the login offers, hidden ones too, which a turn may run on: listed once an
   * app-server, as its first turn asks, within LOGIN_MODELS_TIMEOUT, and kept for its next.
   * None where the list could not be read in time, said in alasio's log, so the turns
   * waiting on it run on the model chosen; what did not come is not kept, and the next
   * turn asks again.
   */
  readonly loginModels: (scope: AppServerScope) => Effect.Effect<readonly v2.Model[], AppServerStartError>;
  /**
   * Refreshes this machine's Codex login now, as Codex refreshes it (account/read, its refresh
   * token asked for): its access token, and its refresh token, which works once, rotated in its
   * home's auth.json.
   */
  readonly refreshLogin: (scope: AppServerScope) => Effect.Effect<void, AppServerStartError>;
  /**
   * Whether the conversation `threadKey` has Codex working in it on its own: a turn of one
   * of its threads running in the app-server, whoever started it (a goal's, as its
   * thread/goal/updated names it), or a terminal one's commands left running in the
   * background, as the app-server running lists them (thread/backgroundTerminals/list).
   * Each thread loaded, started or forked for the conversation is asked of, not only its
   * session's, as one left by a fresh session or a fork stays loaded, its terminals running.
   * Asked only of an app-server that runs, none started to ask, and within
   * BACKGROUND_WORK_TIMEOUT, failing with AppServerRequestTimeout past it.
   */
  readonly backgroundWork: (threadKey: string) => Effect.Effect<boolean, AppServerRequestError>;
  /** Starts a turn of the prompt, interrupting one left running first; the turn's id. */
  readonly startTurn: (options: StartTurnOptions) => Effect.Effect<string, AppServerRequestError | StaleTurnCleanupError>;
  /** Takes the turn `turnId`, which the app-server runs on its own (a goal's), as the thread's. */
  readonly claimTurn: (threadId: string, turnId: string) => Effect.Effect<void>;
  /** The id of the thread's running turn, once it has one; null if none starts within `timeout`. */
  readonly waitForTurnId: (threadId: string, timeout?: Duration.Input) => Effect.Effect<string | null, AppServerGone>;
  readonly steerTurn: (options: SteerTurnOptions) => Effect.Effect<v2.TurnSteerResponse, AppServerRequestError | NoActiveTurn>;
  /** Interrupts the thread's running turn, or none but the turn `only` where given: whether there was one. `origin` says, in the log, who asked. */
  readonly interrupt: (threadId: string, origin?: string, only?: string) => Effect.Effect<boolean, AppServerRequestError>;
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

/** Runs `work` apart from what asked for it, in the app-server's time: its stop waits for it. */
export type AppServerApart = (work: Effect.Effect<void>) => Effect.Effect<Fiber.Fiber<void>>;

/**
 * A thread's work in the app-server `rpc` talks to, its turns followed by `notifications`;
 * what outlasts what asked for it run `apart`.
 */
export function makeAppServerThreads(
  rpc: Pick<AppServerRpc, "start" | "request" | "stop" | "whenGone" | "running">,
  notifications: AppServerNotifications,
  apart: AppServerApart,
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

  /** The models the login offers, as the app-server running listed them, and which app-server that is. */
  let offered: { readonly running: number; readonly models: Deferred.Deferred<readonly v2.Model[]> } | null = null;

  const interrupt = Effect.fnUntraced(function*(threadId: string, origin = "unspecified", only?: string): Effect.fn.Return<boolean, AppServerRequestError> {
    const turnId = yield* notifications.currentTurnId(threadId);
    if (!turnId || (only !== undefined && !(yield* notifications.turnAliases(threadId)).has(only))) {
      return false;
    }
    yield* Effect.logInfo(`interrupting app-server turn thread=${threadId.slice(0, 8)} turn=${turnId} origin=${origin}`);
    yield* notifications.forgetTurn(threadId);
    yield* rpc.request("turn/interrupt", { threadId, turnId }, INTERRUPT_TIMEOUT);
    return true;
  });

  /**
   * Stops the app-server, the one sure way to stop a turn it does not answer for, as `cause`
   * says: which ends every turn it runs, every conversation's, goals' among them, so it is
   * done only where it gave no answer, saying which threads' turns it ends. The app-server
   * stopped is the one the turn was started on: a request of one that was replaced fails at
   * once, as its connection goes (./rpc-client.ts), so one that went unanswered for its whole
   * wait was of the app-server that runs still.
   */
  const recycle = (threadId: string, cause: { readonly message: string }): Effect.Effect<void> =>
    Effect.gen(function*() {
      const running = (yield* notifications.runningTurns).map(({ threadId: thread, turnId }) => `${thread.slice(0, 8)}/${turnId}`).join(",") || "none";
      yield* Effect.logWarning(`stopping the app-server, as it gave no answer for the turn stopped as it started thread=${threadId.slice(0, 8)} error=${cause.message} ending=${running}`);
      yield* rpc.stop;
      yield* notifications.clear;
    });

  /**
   * Says what became of the turn `turnId` the app-server answered the interrupt of with an
   * error: ended as the interrupt went, gone with its app-server, or, where neither, run on.
   */
  const refused = (threadId: string, turnId: string, error: AppServerRequestError): Effect.Effect<void> =>
    Effect.gen(function*() {
      if (error._tag !== "AppServerRequestFailed") {
        return yield* Effect.logInfo(`the turn stopped as it started is gone with its app-server thread=${threadId.slice(0, 8)} turn=${turnId}`);
      }
      const ended = !(yield* notifications.currentTurnId(threadId)) && (yield* notifications.turnAliases(threadId)).has(turnId);
      yield* ended
        ? Effect.logInfo(`the turn stopped as it started had ended as its interrupt went thread=${threadId.slice(0, 8)} turn=${turnId}`)
        : Effect.logWarning(`the app-server refused to interrupt the turn stopped as it started, which may run on thread=${threadId.slice(0, 8)} turn=${turnId} error=${error.message}`);
    });

  // A command Codex backgrounds runs on, its terminal listed, until its process exits,
  // whatever becomes of its turn: its item completes only then (codex-rs 0.153.3
  // core/src/unified_exec/async_watcher.rs spawn_exit_watcher), and an interrupt does not
  // end it (app-server/README.md, turn/interrupt). So the app-server's own list is read,
  // not the items it told of. A thread it has not loaded, as one an app-server gone had,
  // it answers as not found (app-server/src/request_processors/thread_processor.rs
  // load_thread), and it is asked of no more.
  const terminalsOf = (threadKey: string, threadId: string): Effect.Effect<number, AppServerRequestError> =>
    listAll("thread/backgroundTerminals/list", { threadId }).pipe(
      Effect.catchIf(
        (error) => error._tag === "AppServerRequestFailed" && error.message.startsWith("thread not found"),
        () => Effect.as(notifications.forgetThread(threadKey, threadId), []),
      ),
      Effect.map((terminals) => terminals.length),
    );

  return {
    ensureThread: Effect.fnUntraced(function*({ threadId, threadKey, cwd, env, config }) {
      yield* rpc.start({ env, cwd });
      const startedAt = yield* Clock.currentTimeNanos;
      const listed = yield* rpc.request("thread/loaded/list", {});
      // One loaded keeps the config it was loaded with, its bayma's among it, which is reached at
      // a loopback of alasio's whose bearer never lapses (../../mcp/bayma-proxy.ts).
      if (Array.isArray(listed?.data) && listed.data.includes(threadId)) {
        yield* Effect.logInfo(`thread already loaded thread=${threadId.slice(0, 8)} key=${JSON.stringify(threadKey)} ms=${yield* millisSince(startedAt)}`);
        yield* notifications.recordThread(threadKey, threadId);
        return threadId;
      }
      const response = yield* rpc.request("thread/resume", {
        threadId,
        excludeTurns: true,
        ...threadOverrides({ cwd, config }),
      });
      const resumedId = response?.thread?.id ?? threadId;
      yield* Effect.logInfo(`thread resumed thread=${resumedId.slice(0, 8)} key=${JSON.stringify(threadKey)} turns=${response?.thread?.turns?.length ?? "?"} ms=${yield* millisSince(startedAt)}`);
      yield* notifications.recordThread(threadKey, resumedId);
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
      yield* notifications.recordThread(threadKey, threadId);
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
      yield* notifications.recordThread(threadKey, forkedId);
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

    refreshLogin: ({ env, cwd }) => rpc.start({ env, cwd }).pipe(Effect.andThen(rpc.request("account/read", { refreshToken: true })), Effect.asVoid),

    loginModels: ({ env, cwd }) =>
      Effect.gen(function*() {
        yield* rpc.start({ env, cwd });
        const running = yield* rpc.running;
        const listing = yield* Effect.sync(() => {
          if (offered?.running === running) return { models: offered.models, first: false };
          offered = { running, models: Deferred.makeUnsafe<readonly v2.Model[]>() };
          return { models: offered.models, first: true };
        });
        // Listed apart from the turn that asks first, so one stopped as it waits leaves the list to the next.
        if (listing.first) {
          yield* apart(listAll("model/list", { includeHidden: true }).pipe(
            Effect.timeoutOrElse({ duration: LOGIN_MODELS_TIMEOUT, orElse: () => Effect.fail(`it did not answer within ${LOGIN_MODELS_TIMEOUT}`) }),
            // Kept only where it came: the turns waiting on this one run on the model chosen,
            // and the next turn asks again.
            Effect.catch((error) =>
              Effect.sync(() => {
                if (offered?.models === listing.models) offered = null;
              }).pipe(
                Effect.andThen(Effect.logWarning(`could not list the Codex login's models, so this turn runs on the model chosen, and the next asks again: ${typeof error === "string" ? error : error.message}`)),
                Effect.as([]),
              )
            ),
            Effect.flatMap((models) => Deferred.succeed(listing.models, models)),
            Effect.asVoid,
          ));
        }
        return yield* Deferred.await(listing.models);
      }),

    backgroundWork: (threadKey) =>
      Effect.gen(function*() {
        for (const threadId of yield* notifications.threadsOf(threadKey)) {
          if (yield* notifications.currentTurnId(threadId)) return true;
          if ((yield* terminalsOf(threadKey, threadId)) > 0) return true;
        }
        return false;
      }).pipe(
        Effect.catchTag("AppServerNotRunning", () => Effect.succeed(false)),
        Effect.timeoutOrElse({ duration: BACKGROUND_WORK_TIMEOUT, orElse: () => Effect.fail(new AppServerRequestTimeout({ method: "thread/backgroundTerminals/list" })) }),
      ),

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
      // The app-server starts the turn whether or not this waits for its answer still: so the
      // answer is taken, and its turn remembered, apart from the wait, and a wait stopped, as
      // a /stop stops it, stops the turn the app-server then starts, once it has.
      const started = yield* Effect.forkDetach(rpc.request("turn/start", {
        threadId,
        input: [{ type: "text", text: prompt, text_elements: [] }],
        cwd,
        model,
        ...(effort ? { effort } : {}),
        approvalPolicy: "never",
        sandboxPolicy: { type: "dangerFullAccess" },
      }).pipe(
        Effect.flatMap((response) => Effect.map(notifications.rememberTurn(threadId, response?.turn?.id), (remembered) => ({ response, remembered }))),
      ));
      /**
       * Stops the turn the app-server starts, once it says which, or the app-server itself,
       * its one sure way to stop it, where it does not say in time or cannot interrupt it.
       */
      const stopStarted = Fiber.join(started).pipe(
        Effect.timeoutOrElse({ duration: STOPPED_START_WAIT, orElse: () => Effect.fail(new StoppedStartUnanswered()) }),
        Effect.matchEffect({
          onFailure: (error) =>
            error._tag === "StoppedStartUnanswered"
              ? recycle(threadId, error)
              : Effect.logInfo(`the turn stopped as it started did not start thread=${threadId.slice(0, 8)} error=${error.message}`),
          onSuccess: ({ remembered }) =>
            remembered
              ? interrupt(threadId, "stopped-as-it-started", remembered).pipe(
                Effect.asVoid,
                Effect.catch((error) => (error._tag === "AppServerRequestTimeout" ? recycle(threadId, error) : refused(threadId, remembered, error))),
              )
              : Effect.void,
        }),
      );
      // Stopped as it waits, the turn waits for its stop there, which runs apart, in the
      // app-server's time, so that the app-server's stop waits for it too.
      const { response, remembered } = yield* Fiber.join(started).pipe(
        Effect.onInterrupt(() => Effect.flatMap(apart(stopStarted), Fiber.await)),
      );
      const responseTurnId = response?.turn?.id;
      const turnId = remembered ?? responseTurnId;
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
