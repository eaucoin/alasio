import type { v2 } from "../../.types/codex/index.js";
import { Effect, Option } from "effect";

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
import { SessionFsCodex } from "../codex/sessionfs.ts";
import type { SessionFilesystems } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { withLogScope } from "../shared/log.ts";
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

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** A session-filesystem workspace's volume, with the Sandbox and the app-server that serve it. */
interface ServedSessionFs {
  readonly volumeId: string;
  readonly sandbox: SessionFilesystems;
  readonly sessionFsCodex: SessionFsCodex["Service"];
}

/** What serves the workspace at `workingDirectory` if it is a session filesystem; null for a folder. */
function servedSessionFs(
  workingDirectory: string,
  sandbox: SessionFilesystems | null,
  sessionFsCodex: Option.Option<SessionFsCodex["Service"]>,
): ServedSessionFs | null {
  const workspace = parseWorkspace(workingDirectory);
  if (workspace?.kind !== "sessionfs") {
    return null;
  }
  if (!(sandbox && Option.isSome(sessionFsCodex))) {
    throw new Error("this conversation's workspace is a session filesystem, which this deployment does not enable");
  }
  return { volumeId: workspace.volumeId, sandbox, sessionFsCodex: sessionFsCodex.value };
}

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them: the promise façade of alasio's Codex services (CodexAppServer,
 * and SessionFsCodex where session filesystems are on), whose effects `effects` runs.
 * With Codex's rollouts kept in Neon (see codex/rollouts/), a thread's rollout files
 * missing here are written back before it is resumed or forked, and a turn's thread is
 * mirrored before its response is final.
 *
 * A folder workspace runs on the operator's app-server with the operator's Codex. A
 * session-filesystem workspace runs on the session-filesystem app-server
 * (codex/sessionfs.ts), in the workspace's harness directory, reaching the workspace
 * only through its bayma; its rollouts are that app-server's home's, mirrored by
 * `sessionFsCodexRollouts`.
 */
export function createCodexHarness({
  workingDirectory,
  codexRollouts = null,
  sandbox = null,
  sessionFsCodexRollouts = null,
  folderBayma,
  effects,
}: HarnessOptions): Harness {
  const sessionFs = servedSessionFs(workingDirectory, sandbox, effects ? effects.runSync(Effect.serviceOption(SessionFsCodex)) : Option.none());
  const appServer = effects && Option.getOrUndefined(effects.runSync(Effect.serviceOption(CodexAppServer)));
  if (!effects || !appServer) {
    throw new Error("the Codex harness runs on alasio's Codex app-server, and was given none");
  }
  const run = <A, E>(effect: Effect.Effect<A, E>): Promise<A> => effects.runPromise(effect.pipe(withLogScope("codex-harness")));
  const rollouts = sessionFs ? sessionFsCodexRollouts : codexRollouts;
  const directory = sessionFs ? sessionFs.sandbox.harnessDirectory(sessionFs.volumeId) : workingDirectory;

  // What each call runs against: `scope` for a thread's work (a session filesystem's
  // brings its Sandbox up; a folder's is built by the runtime), `listingScope` for
  // thread and model lists and goals, which need no Sandbox.
  const scope: CodexScopeProvider | null = sessionFs
    ? Effect.tryPromise({
      try: () => sessionFs.sandbox.ensureSession(sessionFs.volumeId),
      catch: (cause) => new CodexScopeError({ cause }),
    }).pipe(Effect.flatMap(({ bayma }) => sessionFs.sessionFsCodex.scope({ directory, bayma })))
    : null;
  const listingScope: Effect.Effect<CodexListingScope, CodexScopeError> = sessionFs
    ? sessionFs.sessionFsCodex.listingScope({ directory })
    : folderListingScope(workingDirectory, appServer);
  const scopeParams = { workingDirectory, appServer, scope, folderBayma };

  const ensureRollouts = (sessionId: string | null | undefined): Effect.Effect<void> =>
    !rollouts || !sessionId
      ? Effect.void
      : Effect.tryPromise(() => rollouts.restore([sessionId])).pipe(
        Effect.catch((error) => Effect.logWarning(`could not check Neon for the rollouts of ${sessionId}: ${errorText(error.cause)}`)),
      );

  const flushRollouts = (sessionId: string | null | undefined): Effect.Effect<void> =>
    !rollouts || !sessionId
      ? Effect.void
      : Effect.tryPromise(() => rollouts.flush(sessionId)).pipe(
        Effect.catch((error) => Effect.logWarning(`the turn's response goes on before ${sessionId} was mirrored: ${errorText(error.cause)}`)),
        withLogScope("codex-harness"),
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
      effects,
    }),
    /** Codex's goals on a thread, for operator/goal-control.ts. */
    goals: {
      read: ({ threadId }) =>
        run(Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.getGoal({ threadId, cwd, env })).pipe(Effect.map((response) => response?.goal ?? null))),
      set: ({ threadId, objective, status }) =>
        run(Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.setGoal({ threadId, objective, status, cwd, env })).pipe(Effect.map((response) => response?.goal ?? null))),
      clear: ({ threadId }) => run(Effect.flatMap(goalScope, ({ appServer, cwd, env }) => appServer.clearGoal({ threadId, cwd, env }))),
      waitForTurnId: (threadId, timeoutMs) => run(Effect.flatMap(listingScope, ({ appServer }) => appServer.waitForTurnId(threadId, timeoutMs))),
    },
    startFreshSession: ({ threadKey }) => run(startFreshCodexSession({ threadKey, ...scopeParams })),
    warmSession: ({ sessionId, threadKey }) =>
      run(ensureRollouts(sessionId).pipe(Effect.andThen(warmCodexSession({ sessionId, threadKey, ...scopeParams })))),
    executeTurn: (params) =>
      run(ensureRollouts(params.resumeSession).pipe(
        Effect.andThen(executeCodexTurn({ ...params, ...scopeParams, beforeResponseComplete: flushRollouts })),
      )),
    listModels: () =>
      run(Effect.flatMap(listingScope, ({ cwd, codexEnv, appServer }) => appServer.listModels({ env: codexEnv, cwd })).pipe(
        Effect.map((models) => models.filter((model) => !model.hidden).map(toModelOption)),
      )),
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return resolveCodexModelChoice(null);
    },
    /** Stops alasio's Codex app-servers, as alasio stops; a turn running on one ends with it. */
    shutdown: () =>
      run(Effect.gen(function*() {
        yield* appServer.stop;
        const sessionFsCodex = yield* Effect.serviceOption(SessionFsCodex);
        if (Option.isSome(sessionFsCodex)) yield* sessionFsCodex.value.stop;
      })),
  };
}
