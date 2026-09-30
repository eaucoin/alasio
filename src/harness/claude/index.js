import { CLAUDE_HARNESS, harnessDisplayName } from "../names.js";
import { getClaudeEffort, getClaudeModel } from "./model.js";
import { listClaudeModels } from "./models.js";
import { createClaudeLiveSessions } from "./live-sessions.js";
import { executeClaudeTurn, startFreshClaudeSession } from "./runtime.js";
import { createClaudeSessionApi } from "./sessions.js";
import { parseWorkspace } from "../../workspace/kind.js";

/**
 * Claude Code harness adapter. Sessions live in the Claude project transcript
 * store, mirrored to alasio's Neon through `sessionStore` when one is given,
 * and each mounted session is served by one long-lived Claude Code process
 * that every turn is pushed into.
 *
 * A session-filesystem workspace's CLI runs in the workspace's harness directory, and
 * its sessions are that directory's, confined to the workspace's bayma (sessionfs.js).
 */
export function createClaudeHarness({ workingDirectory, sessionStore = null, sandbox = null, sessionApi = null, queryFactory = undefined }) {
  const workspace = parseWorkspace(workingDirectory);
  const sessionFs = workspace?.kind === "sessionfs";
  if (sessionFs && !sandbox) {
    throw new Error("this conversation's workspace is a session filesystem, which this deployment does not enable");
  }
  const directory = sessionFs ? sandbox.harnessDirectory(workspace.volumeId) : workingDirectory;
  const sessionFsBayma = sessionFs ? async () => (await sandbox.ensureSession(workspace.volumeId)).bayma : null;
  const sessions = sessionApi ?? createClaudeSessionApi({ workingDirectory: directory, store: sessionStore });
  const liveSessions = createClaudeLiveSessions({ workingDirectory: directory, sessions, sessionStore, sessionFsBayma, queryFactory });
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
      return await executeClaudeTurn({ ...params, workingDirectory: directory, sessions, liveSessions });
    },
    async listModels() {
      return await listClaudeModels({ workingDirectory: directory });
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
