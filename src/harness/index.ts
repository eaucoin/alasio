import type { v2 } from "../../.types/codex/index.js";
import type { ResponseBlock } from "../codex/event-projection.ts";
import type { CodexRolloutsFacade } from "../codex/rollouts/index.ts";
import type { AlasioConfig } from "../config.ts";
import type { FolderBayma } from "../mcp/bayma.ts";
import type { ModelChoice } from "../persistence/conversation-repository.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { SessionFilesystems } from "../sandbox/index.ts";
import type { EffectRunner } from "../shared/effects.ts";
import { createClaudeHarness } from "./claude/index.ts";
import type { ClaudeQueryFactory } from "./claude/runtime.ts";
import type { NeonSessionStore } from "./claude/session-store.ts";
import type { ListedSession, RewindMessage } from "./claude/sessions.ts";
import { createCodexHarness } from "./codex.ts";
import {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  type HarnessName,
  getDefaultHarness,
  harnessDisplayName,
  isHarnessName,
  normalizeHarnessName,
} from "./names.ts";

export {
  CLAUDE_HARNESS,
  CODEX_HARNESS,
  HARNESS_NAMES,
  getDefaultHarness,
  harnessDisplayName,
  isHarnessName,
  normalizeHarnessName,
};

/**
 * The running turn of a conversation, as operator controls reach it: `abort` stops it
 * and resolves once it has finished, `steer` sends it more of the operator's guidance
 * and reports whether that was taken. `cliInitiated` marks a turn Claude Code started
 * on its own rather than one the operator's prompt started.
 */
export interface ActiveQuery {
  readonly abort: (reason: string) => Promise<void>;
  steer: (prompt: string) => Promise<boolean>;
  readonly cliInitiated?: boolean;
}

/** Each conversation's running turn, by thread key; a conversation in it is busy. */
export type ActiveQueries = Map<string, ActiveQuery>;

/** A turn already running upstream (a Codex goal's) that a turn attaches to instead of starting one. */
export interface AttachedTurn {
  readonly sessionId: string;
  readonly turnId: string;
}

/** The upstream session and turn a turn runs as, as far as they are known. */
export interface TransportTurn {
  readonly sessionId: string | null | undefined;
  readonly turnId: string | null | undefined;
}

/**
 * What a turn records in alasio's store as it runs: its response, its session and usage,
 * and any restart it causes; and, where the store keeps them, the conversation's model
 * choices.
 */
export type TurnPersistence =
  & Pick<
    SqliteStore,
    | "createPendingResponse"
    | "updateActiveTurnPendingResponseId"
    | "updatePendingSessionId"
    | "updateActiveTurnSessionId"
    | "appendBlockToPending"
    | "markPendingResponseComplete"
    | "markPendingAsPosted"
    | "updateSessionUsage"
    | "recordRestartEvent"
  >
  & Partial<Pick<SqliteStore, "getModelChoice">>;

/** A turn to run: the operator's prompt, the session it resumes, and where it reports. */
export interface TurnParams {
  readonly prompt: string;
  readonly resumeSession: string | null;
  readonly threadKey: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly workingDirectory: string;
  readonly persistence: TurnPersistence;
  readonly activeQueries: ActiveQueries;
  readonly attachedTurn?: AttachedTurn | null | undefined;
  /** Called as the turn starts and as each of its events arrives. */
  readonly onStarted?: (() => void) | undefined;
  /** Called just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: (() => void) | undefined;
  readonly onTransportStarted?: ((turn: TransportTurn) => void) | undefined;
  readonly onTransportCompleted?: ((turn: TransportTurn) => void) | undefined;
  /** Called when a harness that runs on between prompts has a reply of its own to deliver. */
  readonly onBackgroundResponse?: (() => void) | undefined;
  /** Called when such a harness frees the conversation. */
  readonly onIdle?: (() => void) | undefined;
}

/** How a turn ended: its response's blocks, the session it ran in, and whether it completed or was interrupted. */
export interface TurnResult {
  readonly blockSequence: ResponseBlock[];
  readonly sessionId: string | null | undefined;
  readonly pendingResponseId: string;
  readonly interrupted: boolean;
  readonly responseCompleted: boolean;
}

export interface FreshSessionParams {
  readonly threadKey: string;
  readonly workingDirectory?: string | undefined;
}

export interface WarmSessionParams {
  readonly sessionId: string | null;
  readonly threadKey: string;
  readonly workingDirectory?: string | null | undefined;
}

/** A model as /model offers it, from either harness's catalogue. */
export interface ModelOption {
  readonly id: string;
  readonly label: string;
  readonly description: string;
  readonly resolvedModel: string;
  readonly efforts: readonly string[];
  readonly defaultEffort: string | null;
  readonly isDefault: boolean;
}

/** A harness's sessions of one working directory, as the operator's panels list, rewind, and resume them. */
export interface HarnessSessions {
  listSessions(page?: number): Promise<ListedSession[]>;
  getTotalSessionPages(): Promise<number>;
  getSessionByNumber(num: number): Promise<string | null>;
  getSessionLastMessage(sessionId: string): Promise<string | null>;
  listSessionMessages(sessionId: string): Promise<RewindMessage[]>;
  getTotalRewindPages(sessionId: string): Promise<number>;
  /** A new session holding `sessionId`'s history before the message `beforeUuid`, for the conversation `threadKey`. */
  createForkedSession(sessionId: string, beforeUuid: string, options: { readonly threadKey: string }): Promise<string | null>;
}

/** A change to a thread's goal; what is left out stays as it is. */
export interface GoalUpdate {
  readonly threadId: string;
  readonly objective?: string | null | undefined;
  readonly status?: v2.ThreadGoalStatus | null | undefined;
}

/** A harness's goals on a thread, for operator/goal-control.ts. */
export interface HarnessGoals {
  read(params: { readonly threadId: string }): Promise<v2.ThreadGoal | null>;
  set(params: GoalUpdate): Promise<v2.ThreadGoal | null>;
  clear(params: { readonly threadId: string }): Promise<v2.ThreadGoalClearResponse>;
  waitForTurnId(threadId: string, timeoutMs?: number): Promise<string | null>;
}

/** An agent runtime alasio drives, bound to one working directory. */
export interface Harness {
  readonly name: HarnessName;
  readonly displayName: string;
  readonly supportsGoals: boolean;
  readonly supportsWarmup: boolean;
  readonly supportsSteer: boolean;
  readonly sessions: HarnessSessions;
  /** Present when the harness supports goals. */
  readonly goals?: HarnessGoals;
  /** A new, empty session for the conversation; resolves to its id. */
  startFreshSession(params: FreshSessionParams): Promise<string>;
  /** Loads a session ahead of its next turn; resolves to whether it did. */
  warmSession(params: WarmSessionParams): Promise<boolean>;
  executeTurn(params: TurnParams): Promise<TurnResult>;
  listModels(): Promise<ModelOption[]>;
  /** What a turn runs on when no /model choice is stored. */
  defaultModelChoice(): ModelChoice;
  /** Ends the live process of a conversation, for a harness that keeps one. */
  closeLiveSession?(threadKey: string, reason: string): void;
  shutdown(): void | Promise<void>;
}

/** What a harness is built with; each harness takes what it uses. */
export interface HarnessOptions {
  readonly workingDirectory: string;
  /** Claude Code's transcripts in Neon. */
  readonly sessionStore?: NeonSessionStore | null;
  /** Codex's rollouts in Neon. */
  readonly codexRollouts?: CodexRolloutsFacade | null;
  readonly sandbox?: SessionFilesystems | null;
  readonly sessionFsCodexRollouts?: CodexRolloutsFacade | null;
  /** A folder workspace's bayma: ../mcp/bayma.ts's, from the deployment's host profile, unless a test gives its own. */
  readonly folderBayma?: FolderBayma | undefined;
  /** How Claude Code is started: the Agent SDK's `query`, unless a test gives its own. */
  readonly claudeQueryFactory?: ClaudeQueryFactory | undefined;
  /** What runs a harness's effects: alasio's (src/alasio.ts), so they log and trace as it does; Effect's own where a test leaves it out. */
  readonly effects?: EffectRunner<never> | undefined;
}

/** Where a conversation's mounted harness and folder are read: alasio's store, or any part of it. */
export type MountStore = Partial<Pick<SqliteStore, "getActiveHarness" | "getWorkingDirectory">>;

/**
 * Resolve the active harness name for a conversation from any store shape.
 * Returns null when nothing is mounted; callers gate on that instead of
 * assuming a default.
 */
export function resolveHarnessName(store: MountStore | null | undefined, conversationId: string): HarnessName | null {
  const harness = store?.getActiveHarness?.(conversationId);
  return isHarnessName(harness) ? harness : null;
}

/**
 * Resolve the folder a conversation works in, or null until one is chosen.
 */
export function resolveWorkingDirectory(store: MountStore | null | undefined, conversationId: string): string | null {
  const workingDirectory = store?.getWorkingDirectory?.(conversationId);
  return typeof workingDirectory === "string" && workingDirectory.trim() ? workingDirectory : null;
}

export const NO_SERVICE_MOUNTED = "No service is mounted. Use /service to choose Codex or Claude.";
export const NO_WORKSPACE_MOUNTED = "No folder is mounted. Use /workspace to choose or create one.";

const FACTORIES: Readonly<Record<HarnessName, (options: HarnessOptions) => Harness>> = {
  [CODEX_HARNESS]: createCodexHarness,
  [CLAUDE_HARNESS]: createClaudeHarness,
};

export interface HarnessRegistryOptions extends Omit<HarnessOptions, "workingDirectory"> {
  readonly config?: Partial<Pick<AlasioConfig, "workingDirectory">> | undefined;
  /** Stand-ins (test doubles) for a harness in every folder. */
  readonly overrides?: Partial<Record<HarnessName, Harness>>;
}

export interface HarnessRegistry {
  readonly names: typeof HARNESS_NAMES;
  getFor(name: string, workingDirectory: string | null | undefined): Harness;
  get(name: string): Harness;
  forConversation(store: MountStore | null | undefined, conversationId: string): Harness | null;
  requireForConversation(store: MountStore | null | undefined, conversationId: string): Harness;
  shutdownAll(): Promise<void>;
}

/**
 * Registry of harness adapters. Adapters are bound to one folder, so one is
 * created lazily per (harness, working directory) pair and cached; overrides
 * (test doubles) stand in for every folder of their harness but still require
 * a mounted folder so gating behaves the same as production. `sessionStore`
 * keeps Claude Code's transcripts and `codexRollouts` Codex's rollouts; `sandbox`
 * and `sessionFsCodexRollouts` serve session-filesystem workspaces; `folderBayma`
 * and `claudeQueryFactory`, when given, stand in for a folder workspace's bayma and
 * for Claude Code in every adapter made; `effects` runs the adapters' effects, in
 * alasio's services where the adapters use them (the Codex harness's app-servers).
 */
export function createHarnessRegistry({
  config = {},
  overrides = {},
  sessionStore = null,
  codexRollouts = null,
  sandbox = null,
  sessionFsCodexRollouts = null,
  folderBayma,
  claudeQueryFactory,
  effects,
}: HarnessRegistryOptions = {}): HarnessRegistry {
  const adapters = new Map<string, Harness>();
  const getFor = (name: string, workingDirectory: string | null | undefined): Harness => {
    if (!isHarnessName(name)) {
      throw new Error(`Unknown harness: ${String(name)}`);
    }
    if (typeof workingDirectory !== "string" || !workingDirectory) {
      throw new Error(NO_WORKSPACE_MOUNTED);
    }
    const override = overrides[name];
    if (override) {
      return override;
    }
    const key = `${name}\0${workingDirectory}`;
    let adapter = adapters.get(key);
    if (!adapter) {
      adapter = FACTORIES[name]({ workingDirectory, sessionStore, codexRollouts, sandbox, sessionFsCodexRollouts, folderBayma, claudeQueryFactory, effects });
      adapters.set(key, adapter);
    }
    return adapter;
  };
  return {
    names: HARNESS_NAMES,
    getFor,
    /**
     * Adapter for a harness in the deployment's pre-mounted folder; only meaningful
     * when WORKING_DIRECTORY is configured.
     */
    get(name) {
      return getFor(name, config.workingDirectory);
    },
    forConversation(store, conversationId) {
      const name = resolveHarnessName(store, conversationId);
      const workingDirectory = resolveWorkingDirectory(store, conversationId);
      if (!name || !workingDirectory) {
        return null;
      }
      return getFor(name, workingDirectory);
    },
    requireForConversation(store, conversationId) {
      if (!resolveHarnessName(store, conversationId)) {
        throw new Error(NO_SERVICE_MOUNTED);
      }
      const adapter = this.forConversation(store, conversationId);
      if (!adapter) {
        throw new Error(NO_WORKSPACE_MOUNTED);
      }
      return adapter;
    },
    /** Shut every adapter down, each best-effort, and wait for all of them. */
    async shutdownAll() {
      const all = [...new Set([...adapters.values(), ...Object.values(overrides)])];
      await Promise.allSettled(all.map(async (adapter) => await adapter.shutdown()));
    },
  };
}

/**
 * Interrupt whichever harness owns the active turn for a thread key.
 */
export async function interruptActiveTurn(activeQueries: ActiveQueries, threadKey: string, reason = "Interrupted from Telegram"): Promise<boolean> {
  const activeQuery = activeQueries.get(threadKey);
  if (!activeQuery) {
    return false;
  }
  await activeQuery.abort(reason);
  return true;
}
