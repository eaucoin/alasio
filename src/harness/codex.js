import {
  executeCodexTurn,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.js";
import { codexAppServerClient } from "../codex/app-server/client.js";
import { buildCodexEnv, codexHome } from "../codex/env.js";
import { resolveCodexModelChoice } from "../codex/model.js";
import { restoreRollouts } from "../codex/rollouts/restore.js";
import { createCodexSessionApi } from "../codex/sessions.js";
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

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them. With a rollout store (alasio's Neon, see
 * codex/rollouts/), a thread's rollout files missing here are written back
 * before it is resumed or forked.
 */
export function createCodexHarness({ workingDirectory, rolloutStore = null }) {
  async function ensureRollouts(sessionId) {
    if (!rolloutStore || !sessionId) return;
    try {
      await restoreRollouts({ store: rolloutStore, threadIds: [sessionId], home: codexHome() });
    } catch (error) {
      log.warn(`could not check the rollout store for ${sessionId}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: createCodexSessionApi({ workingDirectory, beforeFork: ensureRollouts }),
    async startFreshSession({ threadKey }) {
      return await startFreshCodexSession({ threadKey, workingDirectory });
    },
    async warmSession({ sessionId, threadKey }) {
      await ensureRollouts(sessionId);
      return await warmCodexSession({ sessionId, threadKey, workingDirectory });
    },
    async executeTurn(params) {
      await ensureRollouts(params.resumeSession);
      return await executeCodexTurn({ ...params, workingDirectory });
    },
    async listModels() {
      const models = await codexAppServerClient.listModels({ env: buildCodexEnv(), cwd: workingDirectory });
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
