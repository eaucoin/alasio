// @ts-nocheck
/**
 * Codex's sessions for the operator's session panels, as Codex's own
 * app-server reports them: the threads whose session ran in the working
 * directory, their turns as rewind points, and rewind as Codex's fork before
 * a turn. alasio reads and writes none of Codex's files for them.
 */
import { createLogger } from "../shared/log.ts";
import { SESSIONS_PER_PAGE } from "../shared/runtime-constants.ts";
import { dateLabel, sessionLabel } from "../shared/session-labels.ts";
import { codexAppServerClient } from "./app-server/client.ts";
import { buildCodexEnv } from "./env.ts";
import { forkCodexSession } from "./runtime.ts";

const log = createLogger("codex-sessions");

const errorText = (error) => (error instanceof Error ? error.message : String(error));

/** What the operator wrote to start a turn. */
function promptOf(turn) {
  return (turn.items ?? [])
    .filter((item) => item.type === "userMessage")
    .flatMap((item) => item.content ?? [])
    .filter((part) => part.type === "text" && typeof part.text === "string")
    .map((part) => part.text)
    .join("\n")
    .trim();
}

/** What Codex last said in a turn, or "" if it said nothing. */
function answerOf(turn) {
  const messages = (turn.items ?? []).filter((item) => item.type === "agentMessage" && item.text?.trim());
  return messages.at(-1)?.text.trim() ?? "";
}

/**
 * The app-server and directory a folder workspace's threads are listed from: the shared
 * app-server, in the folder itself.
 */
export function folderListingScope(workingDirectory) {
  return async () => ({ cwd: workingDirectory, codexEnv: buildCodexEnv(), client: codexAppServerClient });
}

/**
 * `listingScope()` gives the app-server and directory the threads are listed from
 * (`{ cwd, codexEnv, client }`), and `fork` makes a rewind's thread; both default to
 * a folder workspace's, and a session filesystem's harness passes its own. `beforeFork`
 * runs first with the thread forked from, for the rollout store to write back any file
 * of it missing here.
 */
export function createCodexSessionApi({
  workingDirectory,
  listingScope = folderListingScope(workingDirectory),
  fork = forkCodexSession,
  beforeFork = async () => {},
}) {
  async function listAll() {
    const { cwd, codexEnv, client } = await listingScope();
    return await client.listThreads({ env: codexEnv, cwd });
  }

  /** A thread's turns, newest first; none for a thread Codex cannot read. */
  async function turnsOf(sessionId) {
    try {
      const { cwd, codexEnv, client } = await listingScope();
      return await client.listTurns({ threadId: sessionId, env: codexEnv, cwd });
    } catch (error) {
      log.warn(`could not list the turns of ${sessionId}: ${errorText(error)}`);
      return [];
    }
  }

  return {
    async listSessions(page = 1) {
      const all = await listAll();
      const startIdx = (page - 1) * SESSIONS_PER_PAGE;
      return all.slice(startIdx, startIdx + SESSIONS_PER_PAGE).map((thread) => ({
        uuid: thread.id,
        timestamp: dateLabel(thread.updatedAt * 1000),
        label: sessionLabel(thread.name || thread.preview || thread.id),
      }));
    },

    async getTotalSessionPages() {
      const all = await listAll();
      return Math.ceil(all.length / SESSIONS_PER_PAGE) || 1;
    },

    async getSessionByNumber(num) {
      const all = await listAll();
      return all[num - 1]?.id ?? null;
    },

    async getSessionLastMessage(sessionId) {
      for (const turn of await turnsOf(sessionId)) {
        const answer = answerOf(turn);
        if (answer) {
          return answer;
        }
      }
      return null;
    },

    /** The operator's prompts, newest first, each a point to rewind to: its uuid is its turn's id. */
    async listSessionMessages(sessionId) {
      const turns = await turnsOf(sessionId);
      return turns
        .map((turn) => ({ turn, text: promptOf(turn) }))
        .filter(({ text }) => text)
        .map(({ turn, text }, index) => ({
          index: -(index + 1),
          timestamp: turn.startedAt ? new Date(turn.startedAt * 1000).toISOString() : "",
          text,
          uuid: turn.id,
        }));
    },

    async getTotalRewindPages(sessionId) {
      const messages = await this.listSessionMessages(sessionId);
      return Math.ceil(messages.length / SESSIONS_PER_PAGE) || 1;
    },

    /** A new thread holding the session's history before the turn `beforeUuid`, for the conversation `threadKey`. */
    async createForkedSession(sessionId, beforeUuid, { threadKey }) {
      try {
        await beforeFork(sessionId);
        return await fork({ sessionId, beforeTurnId: beforeUuid, threadKey, workingDirectory });
      } catch (error) {
        log.warn(`could not fork ${sessionId} before turn ${beforeUuid}: ${errorText(error)}`);
        return null;
      }
    },
  };
}
