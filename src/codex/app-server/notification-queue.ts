import type { Logger } from "../../shared/log.ts";
import { type AppServerNotification, getNotificationTurnId, itemIds, type NotificationIds } from "./protocol.ts";

const TURN_WAIT_TIMEOUT_MS = 5 * 1000;

/** The turn a thread is in, as its notifications and requests name it. */
interface ThreadTurn {
  preferredId: string | null;
  readonly ids: Set<string>;
  completed: boolean;
}

/** A wait for a thread's next notification. */
interface NotificationWaiter {
  readonly threadId: string;
  readonly resolve: (message: AppServerNotification) => void;
  readonly reject: (error: Error) => void;
  cleanup?: () => void;
}

/** A wait for the id of a thread's turn. */
interface TurnIdWaiter {
  readonly threadId: string;
  readonly resolve: (turnId: string | null) => void;
  readonly reject: (error: Error) => void;
  readonly timer: NodeJS.Timeout;
  cleanup?: () => void;
}

export interface NotificationQueueOptions {
  readonly log: Logger;
}

export interface TurnIdWaitOptions {
  readonly timeoutMs?: number | undefined;
  readonly signal?: AbortSignal;
}

export function getNotificationThreadId(message: AppServerNotification): string | null {
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

function belongsToThread(message: AppServerNotification, threadId: string) {
  const notificationThreadId = getNotificationThreadId(message);
  return !notificationThreadId || notificationThreadId === threadId;
}

function interruptionError(signal: AbortSignal) {
  return new Error(String(signal.reason ?? "Interrupted"));
}

export class AppServerNotificationQueue {
  private readonly log: Logger;
  private notifications: AppServerNotification[];
  waiters: NotificationWaiter[];
  private turnWaiters: TurnIdWaiter[];
  private readonly turnByThread: Map<string, ThreadTurn>;

  constructor({ log }: NotificationQueueOptions) {
    this.log = log;
    this.notifications = [];
    this.waiters = [];
    this.turnWaiters = [];
    this.turnByThread = new Map();
  }

  clear(): void {
    this.notifications = [];
    this.waiters = [];
    this.turnWaiters = [];
    this.turnByThread.clear();
  }

  fail(error: Error): void {
    for (const waiter of this.waiters) {
      waiter.cleanup?.();
      waiter.reject(error);
    }
    this.waiters = [];
    for (const waiter of this.turnWaiters) {
      clearTimeout(waiter.timer);
      waiter.cleanup?.();
      waiter.reject(error);
    }
    this.turnWaiters = [];
  }

  observe(message: AppServerNotification): void {
    const threadId = getNotificationThreadId(message);
    if (message.method === "turn/started" && threadId && message.params?.turn?.id) {
      const turn = this.turnByThread.get(threadId);
      if (turn?.completed && !turn.ids.has(message.params.turn.id)) {
        this.beginTurn(threadId);
      }
      this.rememberTurn(threadId, message.params.turn.id, { prefer: true });
    }
    if (message.method === "thread/goal/updated" && threadId && message.params?.turnId) {
      const turn = this.turnByThread.get(threadId);
      if (turn?.completed && !turn.ids.has(message.params.turnId)) {
        this.beginTurn(threadId);
      }
      this.rememberTurn(threadId, message.params.turnId, { prefer: true });
    }
    if (message.method === "turn/completed" && threadId) {
      const completedTurnId = getNotificationTurnId(message);
      const turn = this.turnByThread.get(threadId);
      if (!turn && completedTurnId) {
        this.turnByThread.set(threadId, {
          preferredId: completedTurnId,
          ids: new Set([completedTurnId]),
          completed: true,
        });
      } else if (turn && (!completedTurnId || turn.ids.has(completedTurnId))) {
        turn.completed = true;
      }
    }
    this.notifications.push(message);
    this.flushWaiters();
  }

  getCurrentTurnId(threadId: string): string | null | undefined {
    const turn = this.turnByThread.get(threadId);
    return turn?.completed ? undefined : turn?.preferredId;
  }

  getTurnAliases(threadId: string): Set<string> {
    return new Set(this.turnByThread.get(threadId)?.ids ?? []);
  }

  beginTurn(threadId: string): void {
    this.turnByThread.set(threadId, {
      preferredId: null,
      ids: new Set(),
      completed: false,
    });
  }

  rememberTurn(threadId: string, turnId: string | null | undefined, { prefer = false }: { readonly prefer?: boolean } = {}): string | null {
    if (!turnId) {
      return null;
    }
    const turn = this.turnByThread.get(threadId) ?? {
      preferredId: turnId,
      ids: new Set<string>(),
      completed: false,
    };
    turn.ids.add(turnId);
    if (prefer) {
      turn.preferredId = turnId;
    }
    if (!turn.preferredId) {
      turn.preferredId = turnId;
    }
    this.turnByThread.set(threadId, turn);
    if (!turn.completed) {
      this.resolveTurnWaiters(threadId, turn.preferredId);
    }
    return turn.preferredId;
  }

  forgetTurn(threadId: string): void {
    this.turnByThread.delete(threadId);
  }

  discardStaleForTurn(threadId: string, turnId: string | null | undefined): number {
    if (!turnId) {
      return 0;
    }
    const acceptedTurnIds = this.getTurnAliases(threadId);
    acceptedTurnIds.add(turnId);
    const before = this.notifications.length;
    this.notifications = this.notifications.filter((message) => {
      if (!belongsToThread(message, threadId)) {
        return true;
      }
      const notificationTurnId = getNotificationTurnId(message);
      return !notificationTurnId || acceptedTurnIds.has(notificationTurnId);
    });
    const discarded = before - this.notifications.length;
    if (discarded > 0) {
      this.log.info(`discarded ${discarded} stale app-server notifications thread=${threadId.slice(0, 8)} turn=${turnId}`);
    }
    return discarded;
  }

  nextForThread(threadId: string, signal?: AbortSignal | undefined): Promise<AppServerNotification> {
    if (signal?.aborted) {
      return Promise.reject(interruptionError(signal));
    }
    const existingIndex = this.notifications.findIndex((message) => belongsToThread(message, threadId));
    if (existingIndex >= 0) {
      // findIndex found it.
      const [message] = this.notifications.splice(existingIndex, 1);
      return Promise.resolve(message!);
    }
    return new Promise((resolve, reject) => {
      const waiter: NotificationWaiter = {
        threadId,
        resolve,
        reject,
      };
      if (signal) {
        const onAbort = () => {
          this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
          reject(interruptionError(signal));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        waiter.cleanup = () => signal.removeEventListener("abort", onAbort);
      }
      this.waiters.push(waiter);
    });
  }

  waitForTurnId(threadId: string, { timeoutMs = TURN_WAIT_TIMEOUT_MS, signal }: TurnIdWaitOptions = {}): Promise<string | null> {
    if (signal?.aborted) {
      return Promise.reject(interruptionError(signal));
    }
    const currentTurnId = this.getCurrentTurnId(threadId);
    if (currentTurnId) {
      return Promise.resolve(currentTurnId);
    }
    return new Promise((resolve, reject) => {
      const waiter: TurnIdWaiter = {
        threadId,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.turnWaiters = this.turnWaiters.filter((candidate) => candidate !== waiter);
          waiter.cleanup?.();
          resolve(null);
        }, timeoutMs),
      };
      if (signal) {
        const onAbort = () => {
          clearTimeout(waiter.timer);
          this.turnWaiters = this.turnWaiters.filter((candidate) => candidate !== waiter);
          reject(interruptionError(signal));
        };
        signal.addEventListener("abort", onAbort, { once: true });
        waiter.cleanup = () => signal.removeEventListener("abort", onAbort);
      }
      this.turnWaiters.push(waiter);
    });
  }

  private resolveTurnWaiters(threadId: string, turnId: string | null): void {
    for (const waiter of [...this.turnWaiters]) {
      if (waiter.threadId !== threadId) {
        continue;
      }
      this.turnWaiters = this.turnWaiters.filter((candidate) => candidate !== waiter);
      clearTimeout(waiter.timer);
      waiter.cleanup?.();
      waiter.resolve(turnId);
    }
  }

  private flushWaiters(): void {
    for (const waiter of [...this.waiters]) {
      const index = this.notifications.findIndex((message) => belongsToThread(message, waiter.threadId));
      if (index === -1) {
        continue;
      }
      // findIndex found it.
      const [message] = this.notifications.splice(index, 1);
      this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
      waiter.cleanup?.();
      waiter.resolve(message!);
    }
  }
}
