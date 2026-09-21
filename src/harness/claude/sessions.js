import { randomUUID } from "node:crypto";
import {
  forkSession,
  getSessionInfo,
  getSessionMessages,
  listSessions as listSdkSessions,
} from "@anthropic-ai/claude-agent-sdk";
import { SESSIONS_PER_PAGE } from "../../shared/runtime-constants.js";

/**
 * Claude Code session discovery over the SDK's canonical project transcript
 * store (`~/.claude/projects/<cwd-slug>/`). Sessions are scoped to the alasio
 * working directory so the Telegram operator only sees this workspace.
 */
function truncateSessionText(text, maxChars = 40, suffix = "...") {
  const normalized = String(text ?? "").replace(/\s+/g, " ").trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  if (maxChars <= suffix.length) {
    return suffix.slice(0, maxChars);
  }
  return `${normalized.slice(0, maxChars - suffix.length).trimEnd()}${suffix}`;
}

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

function dateLabel(epochMs) {
  const value = Number(epochMs);
  if (!Number.isFinite(value) || value <= 0) {
    return "-";
  }
  return new Date(value).toISOString().slice(0, 10);
}

export function createClaudeSessionApi({ workingDirectory, sdk = { listSessions: listSdkSessions, getSessionMessages, forkSession, getSessionInfo } }) {
  async function listAll() {
    const sessions = await sdk.listSessions({ dir: workingDirectory });
    return [...sessions].sort((a, b) => Number(b.lastModified ?? 0) - Number(a.lastModified ?? 0));
  }

  async function readMessages(sessionId) {
    try {
      return await sdk.getSessionMessages(sessionId, { dir: workingDirectory });
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
        label: truncateSessionText(session.customTitle || session.summary || session.firstPrompt || session.sessionId, 40),
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
        const forked = await sdk.forkSession(sessionId, {
          dir: workingDirectory,
          upToMessageId: messages[targetIndex - 1].uuid,
        });
        return forked?.sessionId ?? null;
      } catch {
        return null;
      }
    },

    async sessionExists(sessionId) {
      if (!sessionId) {
        return false;
      }
      try {
        return Boolean(await sdk.getSessionInfo(sessionId, { dir: workingDirectory }));
      } catch {
        return false;
      }
    },
  };
}
