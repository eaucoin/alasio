import type { v2 } from "../../.types/codex/index.js";
import { Effect } from "effect";

import { CodexAppServer } from "../codex/app-server/client.ts";
import { resolveCodexModelChoice } from "../codex/model.ts";
import {
  type CodexScopeProvider,
  CodexScopeError,
  executeCodexTurn,
  forkCodexSession,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.ts";
import { type CodexListingScope, createCodexSessionApi, folderListingScope } from "../codex/sessions.ts";
import type { SessionFsCodex } from "../codex/sessionfs.ts";
import { SessionFilesystemsDisabled, type SessionSandboxes } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { withLogScope } from "../shared/log.ts";
import { ActiveTurns } from "./active-turns.ts";
import type { Harness, HarnessOptions, ModelOption } from "./index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "./names.ts";

/** The app-server's model entry, in the shape /model shows for either harness. */
function toModelOption(model: v2.Model): ModelOption {
  const efforts = (model.supportedReasoningEfforts ?? [])
    .map((entry) => (typeof entry === "string" ? entry : entry?.reasoningEffort))
    .filter(Boolean);
  return {
    id: model.id ?? model.model,
    label: model.displayName ?? model.id ?? model.model,
    description: model.description ?? "",
    resolvedModel: model.model ?? model.id,
    efforts,
    defaultEffort: model.defaultReasoningEffort ?? null,
    isDefault: Boolean(model.isDefault),
  };
}

/** A session-filesystem workspace's volume, with the Sandbox and the app-server that serve it. */
interface ServedSessionFs {
  readonly volumeId: string;
  readonly sandbox: SessionSandboxes["Service"];
  readonly sessionFsCodex: SessionFsCodex["Service"];
}

/** What makeCodexHarness is given: a harness's options, and the session filesystems' app-server where they are on. */
export interface CodexHarnessOptions extends HarnessOptions {
  readonly sessionFsCodex: SessionFsCodex["Service"] | null;
}

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them, on alasio's Codex services (CodexAppServer, and SessionFsCodex
 * where session filesystems are on). With Codex's rollouts kept in Neon (see
 * codex/rollouts/), a thread's rollout files missing here are written back before it is
 * resumed or forked, and a turn's thread is mirrored before its response is final.
 *
 * A folder workspace runs on the operator's app-server with the operator's Codex. A
 * session-filesystem workspace runs on the session-filesystem app-server
 * (codex/sessionfs.ts), in the workspace's harness directory, reaching the workspace
 * only through its bayma; its rollouts are that app-server's home's, mirrored by
 * `sessionFsCodexRollouts`.
 */
export const makeCodexHarness = Effect.fnUntraced(function*({
  workingDirectory,
  codexRollouts = null,
  sandbox,
  sessionFsCodex,
  sessionFsCodexRollouts = null,
  folderBayma,
}: CodexHarnessOptions): Effect.fn.Return<Harness, SessionFilesystemsDisabled, CodexAppServer | ActiveTurns> {
  const appServer = yield* CodexAppServer;
  const activeTurns = yield* ActiveTurns;
  const workspace = parseWorkspace(workingDirectory);
  let sessionFs: ServedSessionFs | null = null;
  if (workspace?.kind === "sessionfs") {
    if (!sandbox || !sessionFsCodex) {
      return yield* new SessionFilesystemsDisabled();
    }
    sessionFs = { volumeId: workspace.volumeId, sandbox, sessionFsCodex };
  }
  const rollouts = sessionFs ? sessionFsCodexRollouts : codexRollouts;
  const directory = sessionFs ? sessionFs.sandbox.harnessDirectory(sessionFs.volumeId) : workingDirectory;
  const scoped = withLogScope("codex-harness");

  // What each call runs against: `scope` for a thread's work (a session filesystem's
  // brings its Sandbox up; a folder's is built by the runtime), `listingScope` for
  // thread and model lists and goals, which need no Sandbox.
  const scope: CodexScopeProvider | null = sessionFs
    ? sessionFs.sandbox.ensureSession(sessionFs.volumeId).pipe(
      Effect.mapError((cause) => new CodexScopeError({ cause })),
      Effect.flatMap(({ bayma }) => sessionFs.sessionFsCodex.scope({ directory, bayma })),
    )
    : null;
  const listingScope: Effect.Effect<CodexListingScope, CodexScopeError> = sessionFs
    ? sessionFs.sessionFsCodex.listingScope({ directory })
    : folderListingScope(workingDirectory, appServer);
  const scopeParams = { workingDirectory, appServer, scope, folderBayma };

  const ensureRollouts = (sessionId: string | null | undefined): Effect.Effect<void> =>
    !rollouts || !sessionId
      ? Effect.void
      : rollouts.restore([sessionId]).pipe(
        Effect.catch((error) => Effect.logWarning(`could not check Neon for the rollouts of ${sessionId}: ${error.message}`)),
        Effect.asVoid,
      );

  const flushRollouts = (sessionId: string | null | undefined): Effect.Effect<void> =>
    !rollouts || !sessionId
      ? Effect.void
      : rollouts.flush(sessionId).pipe(
        Effect.catch((error) => Effect.logWarning(`the turn's response goes on before ${sessionId} was mirrored: ${error.message}`)),
        scoped,
      );

  /** The app-server goals are read and set through, and the directory and env each call names. */
  const goalScope = Effect.map(listingScope, ({ cwd, codexEnv, appServer }) => ({ appServer, cwd, env: codexEnv }));

  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: createCodexSessionApi({
      listingScope,
      fork: ({ sessionId, beforeTurnId, threadKey }) => forkCodexSession({ sessionId, beforeTurnId, threadKey, ...scopeParams }),
      beforeFork: ensureRollouts,
    }),
    /** Codex's goals on a thread, for operator/goal-control.ts. */
    goals: {
      read: ({ threadId }) =>
        Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.getGoal({ threadId, cwd, env })).pipe(Effect.map((response) => response?.goal ?? null), scoped),
      set: ({ threadId, objective, status }) =>
        Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.setGoal({ threadId, objective, status, cwd, env })).pipe(Effect.map((response) => response?.goal ?? null), scoped),
      clear: ({ threadId }) => Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.clearGoal({ threadId, cwd, env })).pipe(scoped),
      waitForTurnId: (threadId, timeoutMs) => Effect.flatMap(listingScope, ({ appServer }) => appServer.waitForTurnId(threadId, timeoutMs)).pipe(scoped),
    },
    startFreshSession: ({ threadKey }) => startFreshCodexSession({ threadKey, ...scopeParams }).pipe(scoped),
    warmSession: ({ sessionId, threadKey }) =>
      ensureRollouts(sessionId).pipe(Effect.andThen(warmCodexSession({ sessionId, threadKey, ...scopeParams })), scoped),
    runTurn: (params) =>
      ensureRollouts(params.resumeSession).pipe(
        Effect.andThen(executeCodexTurn({ ...params, ...scopeParams, beforeResponseComplete: flushRollouts })),
        Effect.provideService(ActiveTurns, activeTurns),
        scoped,
      ),
    listModels: () =>
      Effect.flatMap(listingScope, ({ cwd, codexEnv, appServer }) => appServer.listModels({ env: codexEnv, cwd })).pipe(
        Effect.map((models) => models.filter((model) => !model.hidden).map(toModelOption)),
        scoped,
      ),
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice: () => resolveCodexModelChoice(null),
  };
});
