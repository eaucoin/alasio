import { Context, Effect, Exit, Layer, Schema, type Scope, Stream } from "effect";

import { withLogScope } from "../../shared/log.ts";
import { makeAppServerNotifications } from "./notification-queue.ts";
import { type SpawnAppServer, spawnAppServer } from "./process.ts";
import {
  type AppServerEvent,
  type AppServerNotification,
  getNotificationTurnId,
  isIgnorableNotification,
  mapNotificationToSdkEvent,
  notificationMatchesTurn,
} from "./protocol.ts";
import { type AppServerGone, type AppServerStartError, makeAppServerRpc } from "./rpc-client.ts";
import {
  type AppServerThreads,
  type EnsureThreadOptions,
  makeAppServerThreads,
  type StaleTurnCleanupError,
  type ThreadIdMissing,
} from "./thread-client.ts";

const MAX_SKIPPED_NOTIFICATIONS = 1000;
const IGNORED_LOG_INTERVAL = 1000;
const UNKNOWN_LOG_INTERVAL = 100;
const SKIP_WARNING_INTERVAL = 100;

function incrementMethodCount(counts: Map<string, number>, method: string | undefined) {
  const key = method ?? "unknown";
  counts.set(key, (counts.get(key) ?? 0) + 1);
}

function summarizeMethodCounts(counts: ReadonlyMap<string, number>) {
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([method, count]) => `${method}:${count}`)
    .join(", ");
}

/** A turn's notifications kept coming without one of its own among them. */
export class StaleNotificationsExceeded extends Schema.TaggedError<StaleNotificationsExceeded>()("StaleNotificationsExceeded", {
  turnId: Schema.NullOr(Schema.String),
}) {
  override get message(): string {
    return `Exceeded ${MAX_SKIPPED_NOTIFICATIONS} skipped app-server notifications while waiting for turn ${this.turnId ?? "unknown"}`;
  }
}

/** How a turn's events stop before its end: its app-server went, or sent only other turns' notifications. */
export type AppServerEventsError = AppServerGone | StaleNotificationsExceeded;

/** A turn alasio starts in the app-server: the thread's options, and the prompt it runs. */
export interface AppServerTurnOptions extends EnsureThreadOptions {
  readonly prompt: string;
  readonly model?: string | undefined;
  readonly effort?: string | null | undefined;
  /** Called just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: (() => void) | undefined;
}

/** A Codex app-server alasio runs: its threads' work, each turn's events, and its stop. */
export interface AppServer extends Omit<AppServerThreads, "startTurn"> {
  /**
   * Starts a turn of the prompt. A turn left running that cannot be interrupted takes the
   * app-server with it: a new one resumes the thread, and the turn starts there.
   */
  readonly startTurn: (options: AppServerTurnOptions) => Effect.Effect<string, AppServerStartError | ThreadIdMissing | StaleTurnCleanupError>;
  /**
   * The turn's events, from the thread's notifications of it, ending with the turn's.
   * Stopped before then, the stream interrupts the turn, and if that fails, the
   * app-server, which is a turn's only way to be stopped for sure.
   */
  readonly eventsForTurn: (threadId: string, turnId: string | null | undefined) => Stream.Stream<AppServerEvent, AppServerEventsError>;
  /** Stops the app-server, and forgets every thread's turn; the next call starts another. */
  readonly stop: Effect.Effect<void>;
}

export interface AppServerOptions {
  /** How the app-server process is started; the local codex binary's when absent. */
  readonly spawn?: SpawnAppServer | undefined;
}

/** Puts every effect of `appServer` in the app-server's log scope; its streams' effects are put there as they are made. */
function inLogScope(appServer: AppServer): AppServer {
  const scoped = withLogScope("codex-app-server");
  return {
    ensureThread: (options) => scoped(appServer.ensureThread(options)),
    startThread: (options) => scoped(appServer.startThread(options)),
    forkThread: (options) => scoped(appServer.forkThread(options)),
    listThreads: (scope) => scoped(appServer.listThreads(scope)),
    listTurns: (scope) => scoped(appServer.listTurns(scope)),
    listModels: (scope) => scoped(appServer.listModels(scope)),
    startTurn: (options) => scoped(appServer.startTurn(options)),
    claimTurn: (threadId, turnId) => scoped(appServer.claimTurn(threadId, turnId)),
    waitForTurnId: (threadId, timeout) => scoped(appServer.waitForTurnId(threadId, timeout)),
    steerTurn: (options) => scoped(appServer.steerTurn(options)),
    interrupt: (threadId, origin) => scoped(appServer.interrupt(threadId, origin)),
    getGoal: (scope) => scoped(appServer.getGoal(scope)),
    setGoal: (options) => scoped(appServer.setGoal(options)),
    clearGoal: (scope) => scoped(appServer.clearGoal(scope)),
    eventsForTurn: appServer.eventsForTurn,
    stop: scoped(appServer.stop),
  };
}

/** A Codex app-server, run as asked until the scope closes. */
export const makeAppServer = Effect.fnUntraced(function*({ spawn = spawnAppServer }: AppServerOptions = {}): Effect.fn.Return<AppServer, never, Scope.Scope> {
  const notifications = yield* makeAppServerNotifications;
  const rpc = yield* makeAppServerRpc({ spawn, onNotification: notifications.observe });
  const threads = makeAppServerThreads(rpc, notifications);
  const stop = rpc.stop.pipe(Effect.andThen(notifications.clear));

  /** The turn's notifications, as the events they map to, until the turn's end. */
  const eventsForTurn = (threadId: string, turnId: string | null | undefined): Stream.Stream<AppServerEvent, AppServerEventsError> =>
    Stream.unwrap(Effect.gen(function*() {
      const gone = yield* rpc.whenGone;
      const acceptedTurnIds = yield* notifications.turnAliases(threadId);
      if (turnId) {
        acceptedTurnIds.add(turnId);
      }
      let finished = false;
      let skippedNotifications = 0;
      let ignoredNotifications = 0;
      let unknownNotifications = 0;
      const skippedMethods = new Map<string, number>();
      const ignoredMethods = new Map<string, number>();
      const unknownMethods = new Map<string, number>();

      /** What `message` is to the turn: an event of it, or nothing, counted and logged as it builds up. */
      const read = Effect.fnUntraced(function*(message: AppServerNotification): Effect.fn.Return<AppServerEvent | null, StaleNotificationsExceeded> {
        const notificationTurnId = getNotificationTurnId(message);
        if (message?.method === "turn/started" && notificationTurnId && !acceptedTurnIds.has(notificationTurnId)) {
          acceptedTurnIds.add(notificationTurnId);
          yield* Effect.logInfo(`adopted app-server notification turn id thread=${threadId.slice(0, 8)} turn=${notificationTurnId} response_turn=${turnId ?? "unknown"}`);
        }
        const matchesTurn = notificationMatchesTurn(message, acceptedTurnIds);
        const event = matchesTurn ? mapNotificationToSdkEvent(message) : null;
        if (matchesTurn && !event && isIgnorableNotification(message)) {
          ignoredNotifications += 1;
          incrementMethodCount(ignoredMethods, message?.method);
          if (ignoredNotifications % IGNORED_LOG_INTERVAL === 0) {
            yield* Effect.logInfo(`ignored ${ignoredNotifications} app-server progress notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} methods=${summarizeMethodCounts(ignoredMethods)}`);
          }
          return null;
        }
        if (matchesTurn && !event) {
          unknownNotifications += 1;
          incrementMethodCount(unknownMethods, message?.method);
          if (unknownNotifications % UNKNOWN_LOG_INTERVAL === 0) {
            yield* Effect.logWarning(`ignored ${unknownNotifications} unmapped same-turn app-server notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} methods=${summarizeMethodCounts(unknownMethods)}`);
          }
          return null;
        }
        ignoredNotifications = 0;
        unknownNotifications = 0;
        if (!matchesTurn || !event) {
          skippedNotifications += 1;
          incrementMethodCount(skippedMethods, message?.method);
          if (skippedNotifications % SKIP_WARNING_INTERVAL === 0) {
            yield* Effect.logWarning(`skipped ${skippedNotifications} stale app-server notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} notification_turn=${notificationTurnId ?? "none"} methods=${summarizeMethodCounts(skippedMethods)}`);
          }
          if (skippedNotifications >= MAX_SKIPPED_NOTIFICATIONS) {
            return yield* new StaleNotificationsExceeded({ turnId: turnId ?? null });
          }
          return null;
        }
        if (skippedNotifications > 0) {
          yield* Effect.logInfo(`resumed app-server notification stream after skipping ${skippedNotifications} stale notifications thread=${threadId.slice(0, 8)} turn=${turnId ?? "unknown"}`);
          skippedNotifications = 0;
          skippedMethods.clear();
        }
        finished = event.type === "turn.completed" || event.type === "turn.failed";
        return event;
      });

      /** The turn's next event, the thread's notifications in between read and let go. */
      const nextEvent: Effect.Effect<AppServerEvent, AppServerEventsError> = Effect.gen(function*() {
        while (true) {
          const event = yield* read(yield* notifications.nextForThread(threadId).pipe(Effect.raceFirst(gone)));
          if (event) return event;
        }
      }).pipe(withLogScope("codex-app-server"));

      return Stream.fromEffectRepeat(nextEvent).pipe(
        Stream.takeUntil(() => finished),
        Stream.onExit((exit) => {
          if (finished) return Effect.void;
          const origin = Exit.isSuccess(exit) ? "consumer-exit" : Exit.hasInterrupts(exit) ? "abort-signal" : "stream-error";
          return threads.interrupt(threadId, origin).pipe(
            Effect.asVoid,
            Effect.catch((error) =>
              Effect.logWarning(`app-server turn cleanup interrupt failed thread=${threadId.slice(0, 8)} origin=${origin} error=${error.message}`).pipe(
                Effect.andThen(stop),
              )
            ),
            withLogScope("codex-app-server"),
          );
        }),
      );
    }));

  return inLogScope({
    ...threads,
    startTurn: ({ threadId, threadKey, prompt, cwd, env, config, model, effort, onPromptDispatched }) =>
      threads.startTurn({ threadId, prompt, cwd, model, effort, onPromptDispatched }).pipe(
        Effect.catchTag("StaleTurnCleanupError", () =>
          Effect.gen(function*() {
            yield* Effect.logWarning(`recycling app-server after stale turn cleanup failure thread=${threadId.slice(0, 8)}`);
            yield* stop;
            const resumedThreadId = yield* threads.ensureThread({ threadId, threadKey, cwd, env, config });
            return yield* threads.startTurn({ threadId: resumedThreadId, prompt, cwd, model, effort, onPromptDispatched });
          })
        ),
      ),
    eventsForTurn,
    stop,
  });
});

/** The operator's Codex app-server: the one folder workspaces' turns run on. */
export class CodexAppServer extends Context.Service<CodexAppServer, AppServer>()("alasio/codex/app-server/CodexAppServer") {
  /** The local codex binary's app-server, started with the first call that needs it and stopped with the layer. */
  static readonly layer: Layer.Layer<CodexAppServer> = Layer.effect(CodexAppServer, makeAppServer());
}
