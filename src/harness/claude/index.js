import { CLAUDE_HARNESS, harnessDisplayName } from "../names.js";
import { getClaudeEffort, getClaudeModel } from "./model.js";
import { listClaudeModels } from "./models.js";
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
    async listModels() {
      return await listClaudeModels({ workingDirectory });
    },
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return { model: getClaudeModel(), effort: getClaudeEffort() };
    },
    shutdown() {
      // Claude Code processes are per-turn; interrupted turns are aborted through activeQueries.
    },
  };
}
