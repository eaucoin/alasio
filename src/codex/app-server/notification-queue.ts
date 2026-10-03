// @ts-nocheck
import { getNotificationTurnId } from "./protocol.ts";

const TURN_WAIT_TIMEOUT_MS = 5 * 1000;

export function getNotificationThreadId(message) {
  const params = message?.params ?? {};
  return params.threadId
    ?? params.thread_id
    ?? params.thread?.id
    ?? params.item?.threadId
    ?? params.item?.thread_id
    ?? params.item?.thread?.id
    ?? params.event?.threadId
    ?? params.event?.thread_id
    ?? params.event?.thread?.id
    ?? params.turn?.threadId
    ?? params.turn?.thread_id
    ?? params.turn?.thread?.id
    ?? null;
}

function belongsToThread(message, threadId) {
  const notificationThreadId = getNotificationThreadId(message);
  return !notificationThreadId || notificationThreadId === threadId;
}

function interruptionError(signal) {
  return new Error(String(signal.reason ?? "Interrupted"));
}

export class AppServerNotificationQueue {
  constructor({ log }) {
    this.log = log;
    this.notifications = [];
    this.waiters = [];
    this.turnWaiters = [];
    this.turnByThread = new Map();
  }

  clear() {
    this.notifications = [];
    this.waiters = [];
    this.turnWaiters = [];
    this.turnByThread.clear();
  }

  fail(error) {
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

  observe(message) {
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

  getCurrentTurnId(threadId) {
    const turn = this.turnByThread.get(threadId);
    return turn?.completed ? undefined : turn?.preferredId;
  }

  getTurnAliases(threadId) {
    return new Set(this.turnByThread.get(threadId)?.ids ?? []);
  }

  beginTurn(threadId) {
    this.turnByThread.set(threadId, {
      preferredId: null,
      ids: new Set(),
      completed: false,
    });
  }

  rememberTurn(threadId, turnId, { prefer = false } = {}) {
    if (!turnId) {
      return null;
    }
    const turn = this.turnByThread.get(threadId) ?? {
      preferredId: turnId,
      ids: new Set(),
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

  forgetTurn(threadId) {
    this.turnByThread.delete(threadId);
  }

  discardStaleForTurn(threadId, turnId) {
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

  nextForThread(threadId, signal) {
    if (signal?.aborted) {
      return Promise.reject(interruptionError(signal));
    }
    const existingIndex = this.notifications.findIndex((message) => belongsToThread(message, threadId));
    if (existingIndex >= 0) {
      const [message] = this.notifications.splice(existingIndex, 1);
      return Promise.resolve(message);
    }
    return new Promise((resolve, reject) => {
      const waiter = {
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

  waitForTurnId(threadId, { timeoutMs = TURN_WAIT_TIMEOUT_MS, signal } = {}) {
    if (signal?.aborted) {
      return Promise.reject(interruptionError(signal));
    }
    const currentTurnId = this.getCurrentTurnId(threadId);
    if (currentTurnId) {
      return Promise.resolve(currentTurnId);
    }
    return new Promise((resolve, reject) => {
      const waiter = {
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

  resolveTurnWaiters(threadId, turnId) {
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

  flushWaiters() {
    for (const waiter of [...this.waiters]) {
      const index = this.notifications.findIndex((message) => belongsToThread(message, waiter.threadId));
      if (index === -1) {
        continue;
      }
      const [message] = this.notifications.splice(index, 1);
      this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
      waiter.cleanup?.();
      waiter.resolve(message);
    }
  }
}
