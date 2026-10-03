import { CLAUDE_HARNESS, harnessDisplayName } from "../names.ts";
import type { Harness, HarnessOptions } from "../index.ts";
import { getClaudeEffort, getClaudeModel } from "./model.ts";
import { listClaudeModels } from "./models.ts";
import { createClaudeLiveSessions } from "./live-sessions.ts";
import { executeClaudeTurn, startFreshClaudeSession } from "./runtime.ts";
import { createClaudeSessionApi, type ClaudeSessionApi } from "./sessions.ts";
import { parseWorkspace } from "../../workspace/kind.ts";

/** What createClaudeHarness is given: a harness's options, and a stand-in session api for tests. */
export interface ClaudeHarnessOptions extends HarnessOptions {
  readonly sessionApi?: ClaudeSessionApi | null;
}

/**
 * Claude Code harness adapter. Sessions live in the Claude project transcript
 * store, mirrored to alasio's Neon through `sessionStore` when one is given,
 * and each mounted session is served by one long-lived Claude Code process
 * that every turn is pushed into.
 *
 * A session-filesystem workspace's CLI runs in the workspace's harness directory, and
 * its sessions are that directory's, confined to the workspace's bayma (sessionfs.ts).
 */
export function createClaudeHarness({
  workingDirectory,
  sessionStore = null,
  sandbox = null,
  sessionApi = null,
  folderBayma,
  claudeQueryFactory,
}: ClaudeHarnessOptions): Harness {
  const workspace = parseWorkspace(workingDirectory);
  const sessionFs = workspace?.kind === "sessionfs";
  if (sessionFs && !sandbox) {
    throw new Error("this conversation's workspace is a session filesystem, which this deployment does not enable");
  }
  // `sandbox` is there whenever `sessionFs` is, as just checked.
  const directory = sessionFs && sandbox ? sandbox.harnessDirectory(workspace.volumeId) : workingDirectory;
  const sessionFsBayma = sessionFs && sandbox ? async () => (await sandbox.ensureSession(workspace.volumeId)).bayma : null;
  const sessions = sessionApi ?? createClaudeSessionApi({ workingDirectory: directory, store: sessionStore });
  const liveSessions = createClaudeLiveSessions({
    workingDirectory: directory,
    sessions,
    sessionStore,
    sessionFsBayma,
    ...(folderBayma ? { folderBayma: ({ threadKey }) => folderBayma({ harness: CLAUDE_HARNESS, threadKey }) } : {}),
    queryFactory: claudeQueryFactory,
  });
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
