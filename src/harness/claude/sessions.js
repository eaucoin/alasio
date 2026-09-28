import { randomUUID } from "node:crypto";
import {
  forkSession,
  getSessionInfo,
  getSessionMessages,
  listSessions as listSdkSessions,
} from "@anthropic-ai/claude-agent-sdk";
import { createLogger } from "../../shared/log.js";
import { SESSIONS_PER_PAGE } from "../../shared/runtime-constants.js";
import { dateLabel, sessionLabel } from "../../shared/session-labels.js";
import { ensureLocalTranscript } from "./transcripts.js";

const log = createLogger("claude-sessions");

/**
 * Claude Code session discovery, scoped to the alasio working directory so the
 * Telegram operator only sees this workspace. With a session store (alasio's
 * Neon, see session-store.js) the SDK's helpers read the store, the durable
 * copy; without one, or while it cannot be reached, the local transcripts
 * under `~/.claude/projects/<cwd-slug>/`.
 */
function textFromMessageContent(content) {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .filter((block) => block && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n");
}

function isOperatorUserMessage(entry) {
  if (!entry || entry.type !== "user" || entry.parent_tool_use_id) {
    return false;
  }
  const content = entry.message?.content;
  if (Array.isArray(content) && content.some((block) => block?.type === "tool_result")) {
    return false;
  }
  return Boolean(textFromMessageContent(content).trim());
}

export function createClaudeSessionApi({
  workingDirectory,
  store = null,
  sdk = { listSessions: listSdkSessions, getSessionMessages, forkSession, getSessionInfo },
}) {
  const local = { dir: workingDirectory };

  /** Runs an SDK helper against the store, or the local files without one. */
  async function fromStore(what, call) {
    if (!store) return call(local);
    try {
      return await call({ ...local, sessionStore: store });
    } catch (error) {
      log.warn(`${what} fell back to local transcripts: ${error instanceof Error ? error.message : String(error)}`);
      return call(local);
    }
  }

  async function listAll() {
    const sessions = await fromStore("listing sessions", (options) => sdk.listSessions(options));
    return [...sessions].sort((a, b) => Number(b.lastModified ?? 0) - Number(a.lastModified ?? 0));
  }

  async function readMessages(sessionId) {
    try {
      return await fromStore("reading a session", (options) => sdk.getSessionMessages(sessionId, options));
    } catch {
      return [];
    }
  }

  return {
    async listSessions(page = 1) {
      const all = await listAll();
      const startIdx = (page - 1) * SESSIONS_PER_PAGE;
      return all.slice(startIdx, startIdx + SESSIONS_PER_PAGE).map((session) => ({
        uuid: session.sessionId,
        timestamp: dateLabel(session.lastModified),
        label: sessionLabel(session.customTitle || session.summary || session.firstPrompt || session.sessionId),
      }));
    },

    async getTotalSessionPages() {
      const all = await listAll();
      return Math.ceil(all.length / SESSIONS_PER_PAGE) || 1;
    },

    async getSessionByNumber(num) {
      const all = await listAll();
      const idx = num - 1;
      if (idx < 0 || idx >= all.length) {
        return null;
      }
      return all[idx].sessionId;
    },

    async getSessionLastMessage(sessionId) {
      const messages = await readMessages(sessionId);
      for (let i = messages.length - 1; i >= 0; i -= 1) {
        const entry = messages[i];
        if (entry?.type !== "assistant" || entry.parent_tool_use_id) {
          continue;
        }
        const text = textFromMessageContent(entry.message?.content).trim();
        if (text) {
          return text;
        }
      }
      return null;
    },

    async listSessionMessages(sessionId) {
      const messages = await readMessages(sessionId);
      const userMessages = messages
        .filter(isOperatorUserMessage)
        .map((entry) => ({
          timestamp: entry.timestamp ?? "",
          text: textFromMessageContent(entry.message?.content).trim(),
          uuid: entry.uuid,
        }));
      return [...userMessages].reverse().map((message, index) => ({
        index: -(index + 1),
        timestamp: message.timestamp,
        text: message.text,
        uuid: message.uuid,
      }));
    },

    async getTotalRewindPages(sessionId) {
      const messages = await this.listSessionMessages(sessionId);
      return Math.ceil(messages.length / SESSIONS_PER_PAGE) || 1;
    },

    async createForkedSession(sessionId, beforeUuid) {
      const messages = await readMessages(sessionId);
      const targetIndex = messages.findIndex((entry) => entry?.uuid === beforeUuid);
      if (targetIndex < 0) {
        return null;
      }
      if (targetIndex === 0) {
        return randomUUID();
      }
      try {
        const forked = await fromStore("forking a session", (options) =>
          sdk.forkSession(sessionId, { ...options, upToMessageId: messages[targetIndex - 1].uuid }),
        );
        return forked?.sessionId ?? null;
      } catch {
        return null;
      }
    },

    async sessionExists(sessionId) {
      if (!sessionId) {
        return false;
      }
      // A resume runs from the local transcript: write it back from the
      // store first if it is gone.
      if (store) {
        try {
          if (await ensureLocalTranscript(store, sessionId)) return true;
        } catch (error) {
          log.warn(`could not check the store for ${sessionId}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
      try {
        return Boolean(await sdk.getSessionInfo(sessionId, local));
      } catch {
        return false;
      }
    },
  };
}
