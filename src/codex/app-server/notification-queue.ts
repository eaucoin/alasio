import { Clock, type Duration, Effect, PubSub, Queue, type Scope, Semaphore } from "effect";

import { type AppServerNotification, getNotificationTurnId, itemIds, type NotificationIds } from "./protocol.ts";

const TURN_WAIT_TIMEOUT = "5 seconds";

/** The turn a thread is in, as its notifications and requests name it. */
interface ThreadTurn {
  preferredId: string | null;
  readonly ids: Set<string>;
  completed: boolean;
}

/**
 * A thread loaded in the app-server: the conversations it is loaded for, by their thread
 * keys, and when it was loaded with the config it has, or, one found loaded, first found so.
 */
interface LoadedThread {
  readonly keys: Set<string>;
  loadedAt: number;
}

/** A thread's turn, now known by `turnId`, which a wait for the thread's turn id is told of. */
interface RememberedTurn {
  readonly threadId: string;
  readonly turnId: string | null;
}

function getNotificationThreadId(message: AppServerNotification): string | null {
  const params: NotificationIds = message?.params ?? {};
  const item = itemIds(params.item);
  return params.threadId
    ?? params.thread_id
    ?? params.thread?.id
    ?? item?.threadId
    ?? item?.thread_id
    ?? item?.thread?.id
    ?? params.event?.threadId
    ?? params.event?.thread_id
    ?? params.event?.thread?.id
    ?? params.turn?.threadId
    ?? params.turn?.thread_id
    ?? params.turn?.thread?.id
    ?? null;
}

/**
 * An app-server's notifications, routed to the threads they are about, and the turn each
 * thread is in as they and alasio's requests name it.
 */
export interface AppServerNotifications {
  /** Takes in a notification: what it says of its thread's turn, and its place in the thread's queue. */
  readonly observe: (message: AppServerNotification) => Effect.Effect<void>;
  /** The thread's next notification, waiting for one as long as it takes. */
  readonly nextForThread: (threadId: string) => Effect.Effect<AppServerNotification>;
  /** The id of the thread's running turn, once it has one; null if none starts within `timeout`. */
  readonly waitForTurnId: (threadId: string, timeout?: Duration.Input) => Effect.Effect<string | null>;
  /** The id of the thread's running turn; undefined when its last turn completed or none is known. */
  readonly currentTurnId: (threadId: string) => Effect.Effect<string | null | undefined>;
  /** Every id the thread's current or last turn is known by. */
  readonly turnAliases: (threadId: string) => Effect.Effect<Set<string>>;
  /** Starts a new turn of the thread, its ids not yet known. */
  readonly beginTurn: (threadId: string) => Effect.Effect<void>;
  /** Adds `turnId` to the ids of the thread's turn, its preferred one if `prefer`; the preferred id after. */
  readonly rememberTurn: (threadId: string, turnId: string | null | undefined, options?: { readonly prefer?: boolean }) => Effect.Effect<string | null>;
  readonly forgetTurn: (threadId: string) => Effect.Effect<void>;
  /** Drops the thread's queued notifications of turns other than `turnId`'s; how many it dropped. */
  readonly discardStaleForTurn: (threadId: string, turnId: string | null | undefined) => Effect.Effect<number>;
  /** Records that `threadId` was loaded now for the conversation `threadKey`, with the config given now (started, resumed or forked). */
  readonly recordLoad: (threadKey: string, threadId: string) => Effect.Effect<void>;
  /**
   * Records that `threadId`, found loaded, is loaded for the conversation `threadKey` too: one
   * of the threads its background work is asked of; its config's time counted from now where
   * the app-server loaded it unasked.
   */
  readonly recordThread: (threadKey: string, threadId: string) => Effect.Effect<void>;
  /** When `threadId` was loaded with the config it has (recordLoad), or first found loaded (recordThread); null where it is not recorded. */
  readonly loadedAt: (threadId: string) => Effect.Effect<number | null>;
  /** The threads loaded for the conversation `threadKey` (recordLoad, recordThread). */
  readonly threadsOf: (threadKey: string) => Effect.Effect<ReadonlyArray<string>>;
  /** Forgets `threadId`, loaded for the conversation `threadKey`, as the app-server has it no more: for every conversation, with when it was loaded. */
  readonly forgetThread: (threadKey: string, threadId: string) => Effect.Effect<void>;
  /** Forgets every thread's turn and queued notifications, and the threads loaded, with when, as the app-server they were of is gone. */
  readonly clear: Effect.Effect<void>;
  /** The threads with a turn running, each with the turn's id: what stopping the app-server ends. */
  readonly runningTurns: Effect.Effect<ReadonlyArray<{ readonly threadId: string; readonly turnId: string }>>;
}

/**
 * Notification routing for one app-server: each thread's notifications in a queue of
 * its own, taken by whoever follows the thread's turn, and each turn id learned
 * published to whoever waits for one. A notification that names no thread belongs to
 * no turn, so no thread's stream would map it to an event: it is not queued.
 */
export const makeAppServerNotifications: Effect.Effect<AppServerNotifications, never, Scope.Scope> = Effect.gen(function*() {
  const turnByThread = new Map<string, ThreadTurn>();
  // The threads loaded, by their ids: one record of each, its conversations and its load.
  const loadedThreads = new Map<string, LoadedThread>();
  const recordOf = (threadId: string, now: number): LoadedThread => {
    const record = loadedThreads.get(threadId) ?? { keys: new Set<string>(), loadedAt: now };
    loadedThreads.set(threadId, record);
    return record;
  };
  let queues = new Map<string, Queue.Queue<AppServerNotification>>();
  const remembered = yield* Effect.acquireRelease(PubSub.unbounded<RememberedTurn>(), PubSub.shutdown);
  // A queue is added to, or sorted through, by one fiber at a time, so its order holds.
  const routing = yield* Semaphore.make(1);

  const queueOf = Effect.fnUntraced(function*(threadId: string) {
    const existing = queues.get(threadId);
    if (existing) return existing;
    const created = yield* Queue.unbounded<AppServerNotification>();
    // Made after a wait: another fiber may have made the thread's queue meanwhile.
    const raced = queues.get(threadId);
    if (raced) return raced;
    queues.set(threadId, created);
    return created;
  });

  const currentTurnIdNow = (threadId: string): string | null | undefined => {
    const turn = turnByThread.get(threadId);
    return turn?.completed ? undefined : turn?.preferredId;
  };

  const turnAliasesNow = (threadId: string): Set<string> => new Set(turnByThread.get(threadId)?.ids ?? []);

  const beginTurnNow = (threadId: string): void => {
    turnByThread.set(threadId, { preferredId: null, ids: new Set(), completed: false });
  };

  const rememberTurnNow = (threadId: string, turnId: string | null | undefined, { prefer = false }: { readonly prefer?: boolean } = {}): string | null => {
    if (!turnId) {
      return null;
    }
    const turn = turnByThread.get(threadId) ?? { preferredId: turnId, ids: new Set<string>(), completed: false };
    turn.ids.add(turnId);
    if (prefer) {
      turn.preferredId = turnId;
    }
    if (!turn.preferredId) {
      turn.preferredId = turnId;
    }
    turnByThread.set(threadId, turn);
    if (!turn.completed) {
      PubSub.publishUnsafe(remembered, { threadId, turnId: turn.preferredId });
    }
    return turn.preferredId;
  };

  /** Whether `message` is of the turn `accepted` names, or of none. */
  const ofTurn = (message: AppServerNotification, accepted: ReadonlySet<string>) => {
    const notificationTurnId = getNotificationTurnId(message);
    return !notificationTurnId || accepted.has(notificationTurnId);
  };

  return {
    observe: Effect.fnUntraced(function*(message) {
      const threadId = getNotificationThreadId(message);
      if (message.method === "turn/started" && threadId && message.params?.turn?.id) {
        const turn = turnByThread.get(threadId);
        if (turn?.completed && !turn.ids.has(message.params.turn.id)) {
          beginTurnNow(threadId);
        }
        rememberTurnNow(threadId, message.params.turn.id, { prefer: true });
      }
      if (message.method === "thread/goal/updated" && threadId && message.params?.turnId) {
        const turn = turnByThread.get(threadId);
        if (turn?.completed && !turn.ids.has(message.params.turnId)) {
          beginTurnNow(threadId);
        }
        rememberTurnNow(threadId, message.params.turnId, { prefer: true });
      }
      if (message.method === "turn/completed" && threadId) {
        const completedTurnId = getNotificationTurnId(message);
        const turn = turnByThread.get(threadId);
        if (!turn && completedTurnId) {
          turnByThread.set(threadId, { preferredId: completedTurnId, ids: new Set([completedTurnId]), completed: true });
        } else if (turn && (!completedTurnId || turn.ids.has(completedTurnId))) {
          turn.completed = true;
        }
      }
      if (threadId) {
        yield* routing.withPermit(Effect.flatMap(queueOf(threadId), (queue) => Queue.offer(queue, message)));
      }
    }),

    nextForThread: (threadId) => Effect.flatMap(queueOf(threadId), Queue.take),

    waitForTurnId: (threadId, timeout = TURN_WAIT_TIMEOUT) =>
      Effect.scoped(Effect.gen(function*() {
        // Subscribed before looking, so a turn remembered in between is not missed.
        const turns = yield* PubSub.subscribe(remembered);
        const current = currentTurnIdNow(threadId);
        if (current) {
          return current;
        }
        while (true) {
          const turn = yield* PubSub.take(turns);
          if (turn.threadId === threadId) return turn.turnId;
        }
      }).pipe(Effect.timeoutOrElse({ duration: timeout, orElse: () => Effect.succeed(null) }))),

    currentTurnId: (threadId) => Effect.sync(() => currentTurnIdNow(threadId)),
    runningTurns: Effect.sync(() =>
      [...turnByThread].flatMap(([threadId, turn]) => (!turn.completed && turn.preferredId ? [{ threadId, turnId: turn.preferredId }] : []))
    ),
    turnAliases: (threadId) => Effect.sync(() => turnAliasesNow(threadId)),
    beginTurn: (threadId) => Effect.sync(() => beginTurnNow(threadId)),
    rememberTurn: (threadId, turnId, options) => Effect.sync(() => rememberTurnNow(threadId, turnId, options)),
    forgetTurn: (threadId) => Effect.sync(() => void turnByThread.delete(threadId)),

    discardStaleForTurn: Effect.fnUntraced(function*(threadId, turnId) {
      const queue = queues.get(threadId);
      if (!turnId || !queue) {
        return 0;
      }
      const accepted = turnAliasesNow(threadId);
      accepted.add(turnId);
      const discarded = yield* routing.withPermit(Effect.gen(function*() {
        const queued = yield* Queue.clear(queue);
        const kept = queued.filter((message) => ofTurn(message, accepted));
        yield* Queue.offerAll(queue, kept);
        return queued.length - kept.length;
      }));
      if (discarded > 0) {
        yield* Effect.logInfo(`discarded ${discarded} stale app-server notifications thread=${threadId.slice(0, 8)} turn=${turnId}`);
      }
      return discarded;
    }),

    recordLoad: (threadKey, threadId) =>
      Effect.flatMap(Clock.currentTimeMillis, (now) =>
        Effect.sync(() => {
          const record = recordOf(threadId, now);
          record.keys.add(threadKey);
          record.loadedAt = now;
        })),
    recordThread: (threadKey, threadId) => Effect.flatMap(Clock.currentTimeMillis, (now) => Effect.sync(() => void recordOf(threadId, now).keys.add(threadKey))),
    loadedAt: (threadId) => Effect.sync(() => loadedThreads.get(threadId)?.loadedAt ?? null),
    threadsOf: (threadKey) => Effect.sync(() => [...loadedThreads].flatMap(([threadId, { keys }]) => (keys.has(threadKey) ? [threadId] : []))),
    forgetThread: (_threadKey, threadId) => Effect.sync(() => void loadedThreads.delete(threadId)),

    clear: Effect.sync(() => {
      // A wait on a forgotten queue goes on until the wait for the app-server it was on
      // (whose stop or end clears these) ends it.
      queues = new Map();
      turnByThread.clear();
      loadedThreads.clear();
    }),
  };
});
