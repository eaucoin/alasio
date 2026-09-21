import { CLAUDE_HARNESS, harnessDisplayName } from "../names.js";
import { executeClaudeTurn, startFreshClaudeSession } from "./runtime.js";
import { createClaudeSessionApi } from "./sessions.js";

/**
 * Claude Code harness adapter. Sessions live in the Claude project transcript
 * store and every turn is a fresh Agent SDK query over the mounted session.
 */
export function createClaudeHarness({ workingDirectory, sessionApi = null }) {
  const sessions = sessionApi ?? createClaudeSessionApi({ workingDirectory });
  return {
    name: CLAUDE_HARNESS,
    displayName: harnessDisplayName(CLAUDE_HARNESS),
    supportsGoals: false,
    supportsWarmup: false,
    supportsSteer: true,
    sessions,
    async startFreshSession({ threadKey }) {
      return startFreshClaudeSession({ threadKey });
    },
    async warmSession() {
      return false;
    },
    async executeTurn(params) {
      return await executeClaudeTurn({ ...params, workingDirectory, sessions });
    },
    shutdown() {
      // Claude Code processes are per-turn; interrupted turns are aborted through activeQueries.
    },
  };
}
