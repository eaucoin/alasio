import { CLAUDE_HARNESS, harnessDisplayName } from "../names.js";
import { getClaudeEffort, getClaudeModel } from "./model.js";
import { listClaudeModels } from "./models.js";
import { createClaudeLiveSessions } from "./live-sessions.js";
import { executeClaudeTurn, startFreshClaudeSession } from "./runtime.js";
import { createClaudeSessionApi } from "./sessions.js";

/**
 * Claude Code harness adapter. Sessions live in the Claude project transcript
 * store, mirrored to alasio's Neon through `sessionStore` when one is given,
 * and each mounted session is served by one long-lived Claude Code process
 * that every turn is pushed into.
 */
export function createClaudeHarness({ workingDirectory, sessionStore = null, sandbox = null, sessionApi = null, queryFactory = undefined }) {
  const sessions = sessionApi ?? createClaudeSessionApi({ workingDirectory, store: sessionStore });
  const liveSessions = createClaudeLiveSessions({ workingDirectory, sessions, sessionStore, sandbox, queryFactory });
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
      return await executeClaudeTurn({ ...params, workingDirectory, sessions, liveSessions });
    },
    async listModels() {
      return await listClaudeModels({ workingDirectory });
    },
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice() {
      return { model: getClaudeModel(), effort: getClaudeEffort() };
    },
    /** Live processes are replaced on demand; this ends one when its conversation is unmounted. */
    closeLiveSession(threadKey, reason) {
      liveSessions.close(threadKey, reason);
    },
    shutdown() {
      return liveSessions.closeAll("shutdown");
    },
  };
}
