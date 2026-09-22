import {
  executeCodexTurn,
  shutdownCodexRuntime,
  startFreshCodexSession,
  warmCodexSession,
} from "../codex/runtime.js";
import { createForkedSession } from "../sessions/forking.js";
import {
  getSessionByNumber,
  getSessionLastMessage,
  getTotalRewindPages,
  getTotalSessionPages,
  listSessionMessages,
  listSessions,
} from "../sessions/index.js";
import { codexAppServerClient } from "../codex/app-server/client.js";
import { buildCodexEnv } from "../codex/env.js";
import { resolveCodexModelChoice } from "../codex/model.js";
import { CODEX_HARNESS, harnessDisplayName } from "./names.js";

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
 * Codex harness adapter over the existing app-server runtime and rollout
 * JSONL session discovery. Session functions stay synchronous; callers await
 * them uniformly with the Claude adapter.
 */
export function createCodexHarness({ workingDirectory }) {
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: true,
    supportsWarmup: true,
    supportsSteer: true,
    sessions: {
      listSessions,
      getTotalSessionPages,
      getSessionByNumber,
      getSessionLastMessage,
      listSessionMessages,
      getTotalRewindPages,
      createForkedSession,
    },
    async startFreshSession({ threadKey }) {
      return await startFreshCodexSession({ threadKey, workingDirectory });
    },
    async warmSession({ sessionId, threadKey }) {
      return await warmCodexSession({ sessionId, threadKey, workingDirectory });
    },
    async executeTurn(params) {
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
