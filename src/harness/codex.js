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
import { CODEX_HARNESS, harnessDisplayName } from "./names.js";

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
    shutdown() {
      shutdownCodexRuntime();
    },
  };
}
