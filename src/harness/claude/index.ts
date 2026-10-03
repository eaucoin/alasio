import { Effect, type Scope } from "effect";

import { SessionFilesystemsDisabled } from "../../sandbox/index.ts";
import { parseWorkspace } from "../../workspace/kind.ts";
import type { ActiveTurns } from "../active-turns.ts";
import type { Harness, HarnessOptions, HarnessSessions } from "../index.ts";
import { CLAUDE_HARNESS, harnessDisplayName } from "../names.ts";
import { makeClaudeLiveSessions } from "./live-sessions.ts";
import { getClaudeEffort, getClaudeModel } from "./model.ts";
import { listClaudeModels } from "./models.ts";
import { ClaudeCodeError, startFreshClaudeSession } from "./runtime.ts";
import { createClaudeSessionApi, type ClaudeSessionApi } from "./sessions.ts";

/** What makeClaudeHarness is given: a harness's options, and a stand-in session api for tests. */
export interface ClaudeHarnessOptions extends HarnessOptions {
  readonly sessionApi?: ClaudeSessionApi | null;
}

/** A promise of the session api, failing as the api says. */
const asked = <A>(ask: () => Promise<A>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.tryPromise({ try: ask, catch: (cause) => new ClaudeCodeError({ cause }) });

/** Claude Code's session api, which the Agent SDK's helpers make promises, as a harness's sessions. */
function claudeSessions(api: ClaudeSessionApi): HarnessSessions {
  return {
    listSessions: (page) => asked(() => api.listSessions(page)),
    getTotalSessionPages: () => asked(() => api.getTotalSessionPages()),
    getSessionByNumber: (num) => asked(() => api.getSessionByNumber(num)),
    getSessionLastMessage: (sessionId) => asked(() => api.getSessionLastMessage(sessionId)),
    listSessionMessages: (sessionId) => asked(() => api.listSessionMessages(sessionId)),
    getTotalRewindPages: (sessionId) => asked(() => api.getTotalRewindPages(sessionId)),
    createForkedSession: (sessionId, beforeUuid) => asked(() => api.createForkedSession(sessionId, beforeUuid)),
  };
}

/**
 * Claude Code harness adapter. Sessions live in the Claude project transcript
 * store, mirrored to alasio's Neon through `sessionStore` when one is given,
 * and each mounted session is served by one long-lived Claude Code process
 * that every turn is pushed into: its live sessions (live-sessions.ts), which last as
 * long as the scope the harness is made in.
 *
 * A session-filesystem workspace's CLI runs in the workspace's harness directory, and
 * its sessions are that directory's, confined to the workspace's bayma (sessionfs.ts).
 */
export const makeClaudeHarness = Effect.fnUntraced(function*({
  workingDirectory,
  sessionStore = null,
  sandbox,
  sessionApi = null,
  folderBayma,
  claudeQueryFactory,
}: ClaudeHarnessOptions): Effect.fn.Return<Harness, SessionFilesystemsDisabled, ActiveTurns | Scope.Scope> {
  const workspace = parseWorkspace(workingDirectory);
  const sessionFs = workspace?.kind === "sessionfs" ? workspace : null;
  if (sessionFs && !sandbox) {
    return yield* new SessionFilesystemsDisabled();
  }
  // `sandbox` is there whenever `sessionFs` is, as just checked.
  const directory = sessionFs && sandbox ? sandbox.harnessDirectory(sessionFs.volumeId) : workingDirectory;
  const sessionFsBayma = sessionFs && sandbox ? Effect.map(sandbox.ensureSession(sessionFs.volumeId), ({ bayma }) => bayma) : null;
  const sessions = sessionApi ?? createClaudeSessionApi({ workingDirectory: directory, store: sessionStore });
  const liveSessions = yield* makeClaudeLiveSessions({
    workingDirectory: directory,
    sessions,
    sessionStore,
    sessionFsBayma,
    folderBayma: ({ threadKey }) => folderBayma({ harness: CLAUDE_HARNESS, threadKey }),
    queryFactory: claudeQueryFactory,
  });
  return {
    name: CLAUDE_HARNESS,
    displayName: harnessDisplayName(CLAUDE_HARNESS),
    supportsGoals: false,
    supportsWarmup: false,
    sessions: claudeSessions(sessions),
    startFreshSession: ({ threadKey }) => startFreshClaudeSession({ threadKey }),
    warmSession: () => Effect.succeed(false),
    /**
     * One operator prompt on the conversation's live Claude Code process, starting or
     * replacing that process when the mounted session, folder or model differs from
     * the one it serves.
     */
    runTurn: (params) => liveSessions.runTurn({ ...params, workingDirectory: directory }),
    listModels: () => listClaudeModels({ workingDirectory: directory }),
    /** What a turn runs on when no /model choice is stored. */
    defaultModelChoice: () => ({ model: getClaudeModel(), effort: getClaudeEffort() }),
  };
});
