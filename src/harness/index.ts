import type { v2 } from "../../.types/codex/index.js";
import { Context, Effect, Layer, Option, Schema, type Scope } from "effect";

import type { CodexAppServer } from "../codex/app-server/client.ts";
import type { NoActiveTurn } from "../codex/app-server/thread-client.ts";
import type { ResponseBlock } from "../codex/event-projection.ts";
import type { KeptCodexRollouts } from "../codex/rollouts/index.ts";
import type { CodexSessionError } from "../codex/runtime.ts";
import { SessionFsCodex } from "../codex/sessionfs.ts";
import { type FolderBayma, folderBayma as hostFolderBayma } from "../mcp/bayma.ts";
import type { ModelChoice, Mount } from "../persistence/conversation-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import { type SessionFilesystemsDisabled, SessionSandboxes } from "../sandbox/index.ts";
import type { ActiveTurns } from "./active-turns.ts";
import { makeClaudeHarness } from "./claude/index.ts";
import type { ClaudeCodeError, ClaudeQueryFactory } from "./claude/runtime.ts";
import type { NeonSessionStore } from "./claude/session-store.ts";
import type { ListedSession, RewindMessage } from "./claude/sessions.ts";
import { makeCodexHarness } from "./codex.ts";
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

/** What a turn records in alasio's store as it runs: its response, its session and usage, and any restart it causes. */
export type TurnPersistence = Pick<
  Store["Service"],
  | "createPendingResponse"
  | "updateActiveTurnPendingResponseId"
  | "updatePendingSessionId"
  | "updateActiveTurnSessionId"
  | "appendBlocksToPending"
  | "markPendingResponseComplete"
  | "markPendingAsPosted"
  | "updateSessionUsage"
  | "recordRestartEvent"
>;

/** A turn to run: the operator's prompt, the session it resumes, and where it reports. */
export interface TurnParams {
  readonly prompt: string;
  readonly resumeSession: string | null;
  readonly threadKey: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly workingDirectory: string;
  /** The model the conversation chose for the harness with /model, read as the turn starts; null for its default. */
  readonly modelChoice: ModelChoice | null;
  readonly persistence: TurnPersistence;
  readonly attachedTurn?: AttachedTurn | null | undefined;
  /** Run just before the prompt is sent, after which the agent may act on it. */
  readonly onPromptDispatched?: Effect.Effect<void> | undefined;
  readonly onTransportStarted?: ((turn: TransportTurn) => Effect.Effect<void>) | undefined;
  readonly onTransportCompleted?: ((turn: TransportTurn) => Effect.Effect<void>) | undefined;
  /** Run when a harness that runs on between prompts has a reply of its own to deliver. */
  readonly onBackgroundResponse?: Effect.Effect<void> | undefined;
  /** Run when such a harness frees the conversation. */
  readonly onIdle?: Effect.Effect<void> | undefined;
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

/** What a harness's call failed with, its message the one the operator is shown. */
export type HarnessError = CodexSessionError | NoActiveTurn | ClaudeCodeError;

/** A harness's sessions of one working directory, as the operator's panels list, rewind, and resume them. */
export interface HarnessSessions {
  readonly listSessions: (page?: number) => Effect.Effect<ListedSession[], HarnessError>;
  readonly getTotalSessionPages: () => Effect.Effect<number, HarnessError>;
  readonly getSessionByNumber: (num: number) => Effect.Effect<string | null, HarnessError>;
  readonly getSessionLastMessage: (sessionId: string) => Effect.Effect<string | null, HarnessError>;
  readonly listSessionMessages: (sessionId: string) => Effect.Effect<RewindMessage[], HarnessError>;
  readonly getTotalRewindPages: (sessionId: string) => Effect.Effect<number, HarnessError>;
  /** A new session holding `sessionId`'s history before the message `beforeUuid`, for the conversation `threadKey`. */
  readonly createForkedSession: (sessionId: string, beforeUuid: string, options: { readonly threadKey: string }) => Effect.Effect<string | null, HarnessError>;
}

/** A change to a thread's goal; what is left out stays as it is. */
export interface GoalUpdate {
  readonly threadId: string;
  readonly objective?: string | null | undefined;
  readonly status?: v2.ThreadGoalStatus | null | undefined;
}

/** A harness's goals on a thread, for operator/goal-control.ts. */
export interface HarnessGoals {
  readonly read: (params: { readonly threadId: string }) => Effect.Effect<v2.ThreadGoal | null, HarnessError>;
  readonly set: (params: GoalUpdate) => Effect.Effect<v2.ThreadGoal | null, HarnessError>;
  readonly clear: (params: { readonly threadId: string }) => Effect.Effect<v2.ThreadGoalClearResponse, HarnessError>;
  readonly waitForTurnId: (threadId: string, timeoutMs?: number) => Effect.Effect<string | null, HarnessError>;
}

/** An agent runtime alasio drives, bound to one working directory, for as long as alasio runs. */
export interface Harness {
  readonly name: HarnessName;
  readonly displayName: string;
  readonly supportsGoals: boolean;
  readonly supportsWarmup: boolean;
  readonly sessions: HarnessSessions;
  /** Present when the harness supports goals. */
  readonly goals?: HarnessGoals;
  /** A new, empty session for the conversation: its id. */
  readonly startFreshSession: (params: FreshSessionParams) => Effect.Effect<string, HarnessError>;
  /** Loads a session ahead of its next turn: whether it did. */
  readonly warmSession: (params: WarmSessionParams) => Effect.Effect<boolean, HarnessError>;
  /**
   * Runs a turn to its end. It fails only as alasio's store does: what else goes wrong
   * ends up in its response. While it runs, it is its conversation's running turn in
   * ActiveTurns, which stops and steers it.
   */
  readonly runTurn: (params: TurnParams) => Effect.Effect<TurnResult, StoreError>;
  readonly listModels: () => Effect.Effect<ModelOption[], HarnessError>;
  /** What a turn runs on when no /model choice is stored. */
  readonly defaultModelChoice: () => ModelChoice;
}

/** What a harness is made with, besides the services it runs on; each harness takes what it uses. */
export interface HarnessOptions {
  readonly workingDirectory: string;
  /** Claude Code's transcripts in Neon. */
  readonly sessionStore?: NeonSessionStore | null | undefined;
  /** Codex's rollouts in Neon: the operator's Codex home's, and the session filesystems' app-server's. */
  readonly codexRollouts?: KeptCodexRollouts | null | undefined;
  readonly sessionFsCodexRollouts?: KeptCodexRollouts | null | undefined;
  /** Session filesystems, where the deployment enables them. */
  readonly sandbox: SessionSandboxes["Service"] | null;
  /** A folder workspace's bayma. */
  readonly folderBayma: FolderBayma;
  /** How Claude Code is started: the Agent SDK's `query`, unless a test gives its own. */
  readonly claudeQueryFactory?: ClaudeQueryFactory | undefined;
}

/** The mounted service's name as the operator reads it, or "No service". */
export function harnessLabelOf({ harness }: Pick<Mount, "harness">): string {
  return harness ? harnessDisplayName(harness) : "No service";
}

export const NO_SERVICE_MOUNTED ="No service is mounted. Use /service to choose Codex or Claude.";
export const NO_WORKSPACE_MOUNTED = "No folder is mounted. Use /workspace to choose or create one.";

/** The conversation has no service mounted. */
export class NoServiceMounted extends Schema.TaggedError<NoServiceMounted>()("NoServiceMounted", {}) {
  override get message(): string {
    return NO_SERVICE_MOUNTED;
  }
}

/** The conversation has no folder mounted. */
export class NoWorkspaceMounted extends Schema.TaggedError<NoWorkspaceMounted>()("NoWorkspaceMounted", {}) {
  override get message(): string {
    return NO_WORKSPACE_MOUNTED;
  }
}

/** A harness name alasio has no harness of, as an old row or a typo holds one. */
export class UnknownHarness extends Schema.TaggedError<UnknownHarness>()("UnknownHarness", {
  name: Schema.String,
}) {
  override get message(): string {
    return `Unknown harness: ${this.name}`;
  }
}

/** Why there is no harness to give: none of that name, no folder, or a workspace the deployment cannot serve. */
export type HarnessUnavailable = UnknownHarness | NoWorkspaceMounted | SessionFilesystemsDisabled;

/** What Harnesses.layer is given: what harnesses are made with, and test doubles. */
export interface HarnessesOptions extends Partial<Pick<HarnessOptions, "sessionStore" | "codexRollouts" | "sessionFsCodexRollouts" | "claudeQueryFactory">> {
  /** A folder workspace's bayma: the deployment's (HostBayma), unless a test gives its own. */
  readonly folderBayma?: FolderBayma | undefined;
  /** Stand-ins (test doubles) for a harness in every folder. */
  readonly overrides?: Partial<Record<HarnessName, Harness>> | undefined;
}

/**
 * alasio's harnesses, for as long as alasio runs. Harnesses are bound to one folder, so one
 * is made as it is first needed per (harness, working directory) pair and kept; overrides
 * (test doubles) stand in for every folder of their harness but still require a mounted
 * folder so gating behaves the same as production. What a harness keeps running (Claude
 * Code's live processes) ends with the scope the harnesses are made in, as alasio stops;
 * the Codex app-servers they run on are alasio's services of their own.
 */
export class Harnesses extends Context.Service<Harnesses, {
  readonly names: typeof HARNESS_NAMES;
  readonly getFor: (name: string, workingDirectory: string | null | undefined) => Effect.Effect<Harness, HarnessUnavailable>;
  /** A conversation's mounted harness in its mounted folder; none until both are mounted. */
  readonly forMount: (mount: Mount) => Effect.Effect<Option.Option<Harness>, HarnessUnavailable>;
  readonly requireForMount: (mount: Mount) => Effect.Effect<Harness, NoServiceMounted | HarnessUnavailable>;
}>()("alasio/harness/Harnesses") {
  static readonly layer = (options: HarnessesOptions = {}): Layer.Layer<Harnesses, never, CodexAppServer | ActiveTurns> =>
    Layer.effect(Harnesses, makeHarnesses(options));
}

const makeHarnesses = Effect.fnUntraced(function*({
  overrides = {},
  folderBayma,
  ...options
}: HarnessesOptions): Effect.fn.Return<Harnesses["Service"], never, CodexAppServer | ActiveTurns | Scope.Scope> {
  // What every harness is made on: the services alasio runs with, and the scope they last for.
  const services = yield* Effect.context<CodexAppServer | ActiveTurns | Scope.Scope>();
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  const sessionFsCodex = Option.getOrNull(yield* Effect.serviceOption(SessionFsCodex));
  const harnessOptions = { ...options, sandbox, folderBayma: folderBayma ?? (yield* hostFolderBayma) };
  const adapters = new Map<string, Harness>();

  const make = (name: HarnessName, workingDirectory: string): Effect.Effect<Harness, SessionFilesystemsDisabled> => {
    const made: Effect.Effect<Harness, SessionFilesystemsDisabled, CodexAppServer | ActiveTurns | Scope.Scope> = name === CODEX_HARNESS
      ? makeCodexHarness({ ...harnessOptions, workingDirectory, sessionFsCodex })
      : makeClaudeHarness({ ...harnessOptions, workingDirectory });
    return Effect.provideContext(made, services);
  };

  const getFor = (name: string, workingDirectory: string | null | undefined): Effect.Effect<Harness, HarnessUnavailable> =>
    Effect.suspend((): Effect.Effect<Harness, HarnessUnavailable> => {
      if (!isHarnessName(name)) {
        return Effect.fail(new UnknownHarness({ name: String(name) }));
      }
      if (typeof workingDirectory !== "string" || !workingDirectory) {
        return Effect.fail(new NoWorkspaceMounted());
      }
      const override = overrides[name];
      if (override) {
        return Effect.succeed(override);
      }
      const key = `${name}\0${workingDirectory}`;
      const kept = adapters.get(key);
      return kept
        ? Effect.succeed(kept)
        : Effect.tap(make(name, workingDirectory), (adapter) => Effect.sync(() => adapters.set(key, adapter)));
    });

  const forMount = ({ harness, workingDirectory }: Mount): Effect.Effect<Option.Option<Harness>, HarnessUnavailable> =>
    harness && workingDirectory ? Effect.asSome(getFor(harness, workingDirectory)) : Effect.succeedNone;

  return Harnesses.of({
    names: HARNESS_NAMES,
    getFor,
    forMount,
    requireForMount: (mount) =>
      mount.harness
        ? forMount(mount).pipe(Effect.flatMap(Option.match({ onNone: () => Effect.fail(new NoWorkspaceMounted()), onSome: Effect.succeed })))
        : Effect.fail(new NoServiceMounted()),
  });
});

