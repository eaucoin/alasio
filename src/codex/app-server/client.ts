import type { v2 } from "../../../.types/codex/index.js";
import { AppServerNotificationQueue } from "./notification-queue.ts";
import type { SpawnAppServer } from "./process.ts";
import {
  type AppServerEvent,
  getNotificationTurnId,
  isIgnorableNotification,
  mapNotificationToSdkEvent,
  notificationMatchesTurn,
} from "./protocol.ts";
import { AppServerRpcClient, type AppServerScope } from "./rpc-client.ts";
import {
  AppServerThreadClient,
  StaleTurnCleanupError,
  type EnsureThreadOptions,
  type ForkThreadOptions,
  type SetGoalOptions,
  type SteerTurnOptions,
  type ThreadOptions,
  type ThreadScope,
} from "./thread-client.ts";
import { appServerLog as log } from "./log.ts";

const MAX_SKIPPED_NOTIFICATIONS = 1000;
const IGNORED_LOG_INTERVAL = 1000;
const UNKNOWN_LOG_INTERVAL = 100;
const SKIP_WARNING_INTERVAL = 100;

function yieldToEventLoop() {
  return new Promise<void>((resolve) => {
    setImmediate(resolve);
  });
}

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

export interface AppServerClientOptions {
  /** How the app-server process is started; the local codex binary when absent. */
  readonly spawnProcess?: SpawnAppServer;
}

/** A turn alasio starts in the app-server: the thread's options, and the prompt it runs. */
export interface AppServerTurnOptions extends EnsureThreadOptions {
  readonly prompt: string;
  readonly model?: string | undefined;
  readonly effort?: string | null | undefined;
  /** Called just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: (() => void) | undefined;
}

export class AppServerClient {
  readonly notifications: AppServerNotificationQueue;
  readonly rpc: AppServerRpcClient;
  readonly threads: AppServerThreadClient;

  constructor({ spawnProcess }: AppServerClientOptions = {}) {
    this.notifications = new AppServerNotificationQueue({
      log,
    });
    this.rpc = new AppServerRpcClient({
      log,
      onNotification: (message) => this.notifications.observe(message),
      onFailure: (error) => this.notifications.fail(error),
      spawnProcess,
    });
    this.threads = new AppServerThreadClient({
      rpc: this.rpc,
      notifications: this.notifications,
      log,
    });
  }

  stop(): void {
    this.rpc.stop();
    this.notifications.clear();
  }

  async ensureThread({ threadId, threadKey, cwd, env, config }: EnsureThreadOptions): Promise<string> {
    return await this.threads.ensureThread({ threadId, threadKey, cwd, env, config });
  }

  async startThread({ threadKey, cwd, env, config }: ThreadOptions): Promise<string> {
    return await this.threads.startThread({ threadKey, cwd, env, config });
  }

  async startTurn({ threadId, threadKey, prompt, cwd, env, config, model, effort, onPromptDispatched }: AppServerTurnOptions): Promise<string> {
    try {
      return await this.threads.startTurn({ threadId, prompt, cwd, model, effort, onPromptDispatched });
    } catch (error) {
      if (!(error instanceof StaleTurnCleanupError)) {
        throw error;
      }
      log.warn(`recycling app-server after stale turn cleanup failure thread=${threadId.slice(0, 8)}`);
      this.stop();
      const resumedThreadId = await this.ensureThread({ threadId, threadKey, cwd, env, config });
      return await this.threads.startTurn({ threadId: resumedThreadId, prompt, cwd, model, effort, onPromptDispatched });
    }
  }

  async forkThread({ threadId, beforeTurnId, threadKey, cwd, env, config }: ForkThreadOptions): Promise<string> {
    return await this.threads.forkThread({ threadId, beforeTurnId, threadKey, cwd, env, config });
  }

  async listThreads({ env, cwd }: AppServerScope): Promise<v2.Thread[]> {
    return await this.threads.listThreads({ env, cwd });
  }

  async listTurns({ threadId, env, cwd }: ThreadScope): Promise<v2.Turn[]> {
    return await this.threads.listTurns({ threadId, env, cwd });
  }

  async listModels({ env, cwd }: AppServerScope): Promise<v2.Model[]> {
    return await this.threads.listModels({ env, cwd });
  }

  claimTurn(threadId: string, turnId: string): void {
    this.threads.claimTurn(threadId, turnId);
  }

  async waitForTurnId(threadId: string, timeoutMs?: number): Promise<string | null> {
    return await this.threads.waitForTurnId(threadId, timeoutMs);
  }

  async steerTurn({ threadId, turnId, prompt }: SteerTurnOptions): Promise<v2.TurnSteerResponse> {
    return await this.threads.steerTurn({ threadId, turnId, prompt });
  }

  async interrupt(threadId: string, origin = "unspecified"): Promise<boolean> {
    return await this.threads.interrupt(threadId, origin);
  }

  async getGoal({ threadId, cwd, env }: ThreadScope): Promise<v2.ThreadGoalGetResponse> {
    return await this.threads.getGoal({ threadId, cwd, env });
  }

  async setGoal({ threadId, cwd, env, objective, status, tokenBudget }: SetGoalOptions): Promise<v2.ThreadGoalSetResponse> {
    return await this.threads.setGoal({ threadId, cwd, env, objective, status, tokenBudget });
  }

  async clearGoal({ threadId, cwd, env }: ThreadScope): Promise<v2.ThreadGoalClearResponse> {
    return await this.threads.clearGoal({ threadId, cwd, env });
  }

  async *eventsForTurn(threadId: string, turnId: string | null | undefined, signal?: AbortSignal): AsyncGenerator<AppServerEvent, void, undefined> {
    let streamFinished = false;
    let cleanupOrigin = "consumer-exit";
    let skippedNotifications = 0;
    let ignoredNotifications = 0;
    let unknownNotifications = 0;
    const acceptedTurnIds = this.notifications.getTurnAliases(threadId);
    if (turnId) {
      acceptedTurnIds.add(turnId);
    }
    const skippedMethods = new Map<string, number>();
    const ignoredMethods = new Map<string, number>();
    const unknownMethods = new Map<string, number>();
    try {
      while (true) {
        if (signal?.aborted) {
          cleanupOrigin = "abort-signal";
          throw new Error(String(signal.reason ?? "Interrupted"));
        }
        const message = await this.notifications.nextForThread(threadId, signal);
        const notificationTurnId = getNotificationTurnId(message);
        if (message?.method === "turn/started" && notificationTurnId && !acceptedTurnIds.has(notificationTurnId)) {
          acceptedTurnIds.add(notificationTurnId);
          log.info(`adopted app-server notification turn id thread=${threadId.slice(0, 8)} turn=${notificationTurnId} response_turn=${turnId ?? "unknown"}`);
        }
        const matchesTurn = notificationMatchesTurn(message, acceptedTurnIds);
        const event = matchesTurn ? mapNotificationToSdkEvent(message) : null;
        if (matchesTurn && !event && isIgnorableNotification(message)) {
          ignoredNotifications += 1;
          incrementMethodCount(ignoredMethods, message?.method);
          if (ignoredNotifications % IGNORED_LOG_INTERVAL === 0) {
            log.info(`ignored ${ignoredNotifications} app-server progress notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} methods=${summarizeMethodCounts(ignoredMethods)}`);
          }
          await yieldToEventLoop();
          continue;
        }
        if (matchesTurn && !event) {
          unknownNotifications += 1;
          incrementMethodCount(unknownMethods, message?.method);
          if (unknownNotifications % UNKNOWN_LOG_INTERVAL === 0) {
            log.warn(`ignored ${unknownNotifications} unmapped same-turn app-server notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} methods=${summarizeMethodCounts(unknownMethods)}`);
          }
          await yieldToEventLoop();
          continue;
        }
        ignoredNotifications = 0;
        unknownNotifications = 0;
        if (!matchesTurn || !event) {
          skippedNotifications += 1;
          incrementMethodCount(skippedMethods, message?.method);
          if (skippedNotifications % SKIP_WARNING_INTERVAL === 0) {
            log.warn(`skipped ${skippedNotifications} stale app-server notifications while waiting for turn=${turnId ?? "unknown"} thread=${threadId.slice(0, 8)} notification_turn=${notificationTurnId ?? "none"} methods=${summarizeMethodCounts(skippedMethods)}`);
          }
          if (skippedNotifications >= MAX_SKIPPED_NOTIFICATIONS) {
            throw new Error(`Exceeded ${MAX_SKIPPED_NOTIFICATIONS} skipped app-server notifications while waiting for turn ${turnId ?? "unknown"}`);
          }
          await yieldToEventLoop();
          continue;
        }
        if (skippedNotifications > 0) {
          log.info(`resumed app-server notification stream after skipping ${skippedNotifications} stale notifications thread=${threadId.slice(0, 8)} turn=${turnId ?? "unknown"}`);
          skippedNotifications = 0;
          skippedMethods.clear();
        }
        yield event;
        if (event.type === "turn.completed" || event.type === "turn.failed") {
          streamFinished = true;
          return;
        }
      }
    } catch (error) {
      cleanupOrigin = signal?.aborted ? "abort-signal" : "stream-error";
      throw error;
    } finally {
      if (!streamFinished) {
        await this.interrupt(threadId, cleanupOrigin).catch((error) => {
          log.warn(`app-server turn cleanup interrupt failed thread=${threadId.slice(0, 8)} origin=${cleanupOrigin} error=${error instanceof Error ? error.message : String(error)}`);
          this.stop();
        });
      }
    }
  }
}

export const codexAppServerClient = new AppServerClient();

export function stopCodexAppServer(): void {
  codexAppServerClient.stop();
}
