import {
  executeCodexTurn,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.js";
import { codexAppServerClient } from "../codex/app-server/client.js";
import { buildCodexEnv } from "../codex/env.js";
import { resolveCodexModelChoice } from "../codex/model.js";
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

const errorText = (error) => (error instanceof Error ? error.message : String(error));

/**
 * Codex harness adapter over the app-server runtime, with its sessions as the
 * app-server reports them. With Codex's rollouts kept in Neon (see
 * codex/rollouts/), a thread's rollout files missing here are written back
 * before it is resumed or forked, and a turn's thread is mirrored before its
 * response is final.
 */
export function createCodexHarness({ workingDirectory, codexRollouts = null }) {
  async function ensureRollouts(sessionId) {
    if (!codexRollouts || !sessionId) return;
    try {
      await codexRollouts.restore([sessionId]);
    } catch (error) {
      log.warn(`could not check Neon for the rollouts of ${sessionId}: ${errorText(error)}`);
    }
  }

  async function flushRollouts(sessionId) {
    if (!codexRollouts || !sessionId) return;
    try {
      await codexRollouts.flush(sessionId);
    } catch (error) {
      log.warn(`the turn's response goes on before ${sessionId} was mirrored: ${errorText(error)}`);
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
      return await executeCodexTurn({ ...params, workingDirectory, beforeResponseComplete: flushRollouts });
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
