import type { v2 } from "../../../.types/codex/index.js";
import type { Logger } from "../../shared/log.ts";
import {
  ALASIO_CODEX_MODEL,
  ALASIO_CODEX_REASONING_EFFORT,
  withAlasioCodexModelConfig,
} from "../model.ts";
import type { CodexThreadConfig } from "../thread-config.ts";
import type { AppServerNotificationQueue } from "./notification-queue.ts";
import { elapsedMs } from "./process.ts";
import type { AppServerParams, AppServerResult } from "./protocol.ts";
import type { AppServerRpcClient, AppServerScope } from "./rpc-client.ts";

const INTERRUPT_TIMEOUT_MS = 5_000;
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

export interface AppServerThreadClientOptions {
  readonly rpc: Pick<AppServerRpcClient, "start" | "request">;
  readonly notifications: AppServerNotificationQueue;
  readonly log: Logger;
}

/** What every thread alasio starts, resumes, or forks is loaded with. */
function threadOverrides({ cwd, config }: { readonly cwd: string; readonly config: CodexThreadConfig }) {
  // Typed as an object type rather than as model.ts's interface, so the protocol's types
  // take it as the JSON object it is.
  const threadConfig: CodexThreadConfig & { readonly model_reasoning_effort: string } = withAlasioCodexModelConfig(config);
  return {
    cwd,
    model: ALASIO_CODEX_MODEL,
    approvalPolicy: "never",
    sandbox: "danger-full-access",
    config: threadConfig,
  } as const;
}

export class StaleTurnCleanupError extends Error {
  constructor(threadId: string, turnId: string, cause: unknown) {
    super(`Failed to clean up stale Codex turn ${turnId} for thread ${threadId}: ${cause instanceof Error ? cause.message : String(cause)}`);
    this.name = "StaleTurnCleanupError";
    this.cause = cause;
  }
}

export class AppServerThreadClient {
  private readonly rpc: Pick<AppServerRpcClient, "start" | "request">;
  private readonly notifications: AppServerNotificationQueue;
  private readonly log: Logger;

  constructor({ rpc, notifications, log }: AppServerThreadClientOptions) {
    this.rpc = rpc;
    this.notifications = notifications;
    this.log = log;
  }

  async ensureThread({ threadId, threadKey, cwd, env, config }: EnsureThreadOptions): Promise<string> {
    await this.rpc.start({ env, cwd });
    const startedAt = process.hrtime.bigint();
    const loaded = await this.rpc.request("thread/loaded/list", {});
    if (Array.isArray(loaded?.data) && loaded.data.includes(threadId)) {
      this.log.info(`thread already loaded thread=${threadId.slice(0, 8)} key=${JSON.stringify(threadKey)} ms=${elapsedMs(startedAt).toFixed(1)}`);
      return threadId;
    }
    const response = await this.rpc.request("thread/resume", {
      threadId,
      excludeTurns: true,
      ...threadOverrides({ cwd, config }),
    });
    const resumedId = response?.thread?.id ?? threadId;
    this.log.info(`thread resumed thread=${resumedId.slice(0, 8)} key=${JSON.stringify(threadKey)} turns=${response?.thread?.turns?.length ?? "?"} ms=${elapsedMs(startedAt).toFixed(1)}`);
    return resumedId;
  }

  async startThread({ threadKey, cwd, env, config }: ThreadOptions): Promise<string> {
    await this.rpc.start({ env, cwd });
    const startedAt = process.hrtime.bigint();
    const response = await this.rpc.request("thread/start", threadOverrides({ cwd, config }));
    const threadId = response?.thread?.id;
    if (!threadId) {
      throw new Error("Codex app-server thread/start did not return a thread id");
    }
    this.log.info(`thread started thread=${threadId.slice(0, 8)} key=${JSON.stringify(threadKey)} ms=${elapsedMs(startedAt).toFixed(1)}`);
    return threadId;
  }

  /**
   * Forks a thread before one of its turns, leaving that turn and every later
   * one out: Codex's own fork, into a new thread loaded as a resume loads
   * one. Returns the new thread's id.
   */
  async forkThread({ threadId, beforeTurnId, threadKey, cwd, env, config }: ForkThreadOptions): Promise<string> {
    await this.rpc.start({ env, cwd });
    const startedAt = process.hrtime.bigint();
    const response = await this.rpc.request("thread/fork", {
      threadId,
      beforeTurnId,
      excludeTurns: true,
      ...threadOverrides({ cwd, config }),
    });
    const forkedId = response?.thread?.id;
    if (!forkedId) {
      throw new Error("Codex app-server thread/fork did not return a thread id");
    }
    this.log.info(`thread forked thread=${forkedId.slice(0, 8)} from=${threadId.slice(0, 8)} before_turn=${beforeTurnId} key=${JSON.stringify(threadKey)} ms=${elapsedMs(startedAt).toFixed(1)}`);
    return forkedId;
  }

  /** The threads whose session ran in `cwd`, most recently updated first, without their turns. */
  async listThreads({ env, cwd }: AppServerScope): Promise<v2.Thread[]> {
    await this.rpc.start({ env, cwd });
    return await this.#listAll("thread/list", {
      cwd,
      sortKey: "updated_at",
      sourceKinds: LISTED_SOURCE_KINDS,
    });
  }

  /** A thread's turns, newest first, each with a summary of its items. */
  async listTurns({ threadId, env, cwd }: ThreadScope): Promise<v2.Turn[]> {
    await this.rpc.start({ env, cwd });
    return await this.#listAll("thread/turns/list", { threadId });
  }

  /** Every page of one of the app-server's paginated lists. */
  async #listAll<M extends ListMethod>(method: M, params: AppServerParams<M>): Promise<AppServerResult<M>["data"][number][]> {
    const all: AppServerResult<M>["data"][number][] = [];
    let cursor: string | null = null;
    do {
      const page: AppServerResult<M> = await this.rpc.request(method, { ...params, limit: LIST_PAGE_SIZE, ...(cursor ? { cursor } : {}) });
      all.push(...(page?.data ?? []));
      cursor = page?.nextCursor ?? null;
    } while (cursor);
    return all;
  }

  async startTurn({ threadId, prompt, cwd, model = ALASIO_CODEX_MODEL, effort = ALASIO_CODEX_REASONING_EFFORT }: StartTurnOptions): Promise<string> {
    const previousTurnId = this.notifications.getCurrentTurnId(threadId);
    if (previousTurnId) {
      this.log.warn(`interrupting leftover app-server turn before starting a new one thread=${threadId.slice(0, 8)} turn=${previousTurnId}`);
      try {
        await this.interrupt(threadId, "stale-turn-cleanup");
      } catch (error) {
        throw new StaleTurnCleanupError(threadId, previousTurnId, error);
      }
    }
    this.notifications.beginTurn(threadId);
    const startedAt = process.hrtime.bigint();
    const response = await this.rpc.request("turn/start", {
      threadId,
      input: [{ type: "text", text: prompt, text_elements: [] }],
      cwd,
      model,
      ...(effort ? { effort } : {}),
      approvalPolicy: "never",
      sandboxPolicy: { type: "dangerFullAccess" },
    });
    const responseTurnId = response?.turn?.id;
    const turnId = this.notifications.rememberTurn(threadId, responseTurnId) ?? responseTurnId;
    this.notifications.discardStaleForTurn(threadId, turnId);
    const turnDetail = responseTurnId && turnId !== responseTurnId ? ` response_turn=${responseTurnId}` : "";
    this.log.info(`turn start accepted thread=${threadId.slice(0, 8)} turn=${turnId ?? "unknown"}${turnDetail} ms=${elapsedMs(startedAt).toFixed(1)}`);
    return turnId;
  }

  /** The models this machine's Codex login can use, as the app-server reports them. */
  async listModels({ env, cwd }: AppServerScope): Promise<v2.Model[]> {
    await this.rpc.start({ env, cwd });
    return await this.#listAll("model/list", { includeHidden: false });
  }

  claimTurn(threadId: string, turnId: string): void {
    if (!this.notifications.getCurrentTurnId(threadId)
      && !this.notifications.getTurnAliases(threadId).has(turnId)) {
      this.notifications.beginTurn(threadId);
    }
    this.notifications.rememberTurn(threadId, turnId);
    this.notifications.discardStaleForTurn(threadId, turnId);
  }

  async waitForTurnId(threadId: string, timeoutMs?: number): Promise<string | null> {
    return await this.notifications.waitForTurnId(threadId, { timeoutMs });
  }

  async steerTurn({ threadId, turnId, prompt }: SteerTurnOptions): Promise<v2.TurnSteerResponse> {
    const activeTurnId = this.notifications.getCurrentTurnId(threadId) ?? turnId;
    if (!activeTurnId) {
      throw new Error("Cannot steer Codex without an active turn id");
    }
    return await this.rpc.request("turn/steer", {
      threadId,
      expectedTurnId: activeTurnId,
      input: [{ type: "text", text: prompt, text_elements: [] }],
    });
  }

  async interrupt(threadId: string, origin = "unspecified"): Promise<boolean> {
    const turnId = this.notifications.getCurrentTurnId(threadId);
    if (!turnId) {
      return false;
    }
    this.log.info(`interrupting app-server turn thread=${threadId.slice(0, 8)} turn=${turnId} origin=${origin}`);
    this.notifications.forgetTurn(threadId);
    await this.rpc.request("turn/interrupt", { threadId, turnId }, INTERRUPT_TIMEOUT_MS);
    return true;
  }

  async getGoal({ threadId, cwd, env }: ThreadScope): Promise<v2.ThreadGoalGetResponse> {
    await this.rpc.start({ env, cwd });
    return await this.rpc.request("thread/goal/get", { threadId });
  }

  async setGoal({ threadId, cwd, env, objective, status, tokenBudget }: SetGoalOptions): Promise<v2.ThreadGoalSetResponse> {
    await this.rpc.start({ env, cwd });
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
    return await this.rpc.request("thread/goal/set", params);
  }

  async clearGoal({ threadId, cwd, env }: ThreadScope): Promise<v2.ThreadGoalClearResponse> {
    await this.rpc.start({ env, cwd });
    return await this.rpc.request("thread/goal/clear", { threadId });
  }
}
