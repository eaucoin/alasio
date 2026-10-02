import {
  executeCodexTurn,
  forkCodexSession,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.js";
import { resolveCodexModelChoice } from "../codex/model.js";
import { createCodexSessionApi, folderListingScope } from "../codex/sessions.js";
import { parseWorkspace } from "../workspace/kind.js";
import { createLogger } from "../shared/log.js";
import { CODEX_HARNESS, harnessDisplayName } from "./names.js";

const log = createLogger("codex-harness");

/** The app-server's model entry, in the shape /model shows for either harness. */
function toModelOption(model) {
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

const errorText = (error) => (error instanceof Error ? error.message : String(error));

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them. With Codex's rollouts kept in Neon (see
 * codex/rollouts/), a thread's rollout files missing here are written back
 * before it is resumed or forked, and a turn's thread is mirrored before its
 * response is final.
 *
 * A folder workspace runs on the shared app-server with the operator's Codex. A
 * session-filesystem workspace runs on the session-filesystem app-server
 * (`sessionFsCodex`, codex/sessionfs.js), in the workspace's harness directory, reaching
 * the workspace only through its bayma; its rollouts are that app-server's home's,
 * mirrored by `sessionFsCodexRollouts`.
 */
export function createCodexHarness({
  workingDirectory,
  codexRollouts = null,
  sandbox = null,
  sessionFsCodex = null,
  sessionFsCodexRollouts = null,
}) {
  const workspace = parseWorkspace(workingDirectory);
  const sessionFs = workspace?.kind === "sessionfs";
  if (sessionFs && !(sandbox && sessionFsCodex)) {
    throw new Error("this conversation's workspace is a session filesystem, which this deployment does not enable");
  }
  const rollouts = sessionFs ? sessionFsCodexRollouts : codexRollouts;
  const directory = sessionFs ? sandbox.harnessDirectory(workspace.volumeId) : workingDirectory;

  // What each call runs against: `scope()` for a thread's work (a session filesystem's
  // brings its Sandbox up; a folder's is built by the runtime), `listingScope()` for
  // thread and model lists and goals, which need no Sandbox.
  const scope = sessionFs
    ? async () => sessionFsCodex.scope({ directory, bayma: (await sandbox.ensureSession(workspace.volumeId)).bayma })
    : null;
  const listingScope = sessionFs
    ? async () => await sessionFsCodex.listingScope({ directory })
    : folderListingScope(workingDirectory);

  async function ensureRollouts(sessionId) {
    if (!rollouts || !sessionId) return;
    try {
      await rollouts.restore([sessionId]);
    } catch (error) {
      log.warn(`could not check Neon for the rollouts of ${sessionId}: ${errorText(error)}`);
    }
  }

  async function flushRollouts(sessionId) {
    if (!rollouts || !sessionId) return;
    try {
      await rollouts.flush(sessionId);
    } catch (error) {
      log.warn(`the turn's response goes on before ${sessionId} was mirrored: ${errorText(error)}`);
    }
  }

  async function goalCall(method, args) {
    const { cwd, codexEnv, client } = await listingScope();
    return await client[method]({ ...args, cwd, env: codexEnv });
  }

  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: createCodexSessionApi({
      workingDirectory,
      listingScope,
      fork: (params) => forkCodexSession({ ...params, scope }),
      beforeFork: ensureRollouts,
    }),
    /** Codex's goals on a thread, for operator/goal-control.js. */
    goals: {
      async read({ threadId }) {
        return (await goalCall("getGoal", { threadId }))?.goal ?? null;
      },
      async set({ threadId, objective, status }) {
        return (await goalCall("setGoal", { threadId, objective, status }))?.goal ?? null;
      },
      async clear({ threadId }) {
        return await goalCall("clearGoal", { threadId });
      },
      async waitForTurnId(threadId, timeoutMs) {
        const { client } = await listingScope();
        return await client.waitForTurnId(threadId, timeoutMs);
      },
    },
    async startFreshSession({ threadKey }) {
      return await startFreshCodexSession({ threadKey, workingDirectory, scope });
    },
    async warmSession({ sessionId, threadKey }) {
      await ensureRollouts(sessionId);
      return await warmCodexSession({ sessionId, threadKey, workingDirectory, scope });
    },
    async executeTurn(params) {
      await ensureRollouts(params.resumeSession);
      return await executeCodexTurn({ ...params, workingDirectory, beforeResponseComplete: flushRollouts, scope });
    },
    async listModels() {
      const { cwd, codexEnv, client } = await listingScope();
      const models = await client.listModels({ env: codexEnv, cwd });
      return models.filter((model) => !model.hidden).map(toModelOption);
    },
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return resolveCodexModelChoice(null);
    },
    shutdown() {
      shutdownCodexRuntime();
    },
  };
}
