import { randomUUID } from "node:crypto";
import {
  forkSession,
  getSessionInfo,
  getSessionMessages,
  listSessions as listSdkSessions,
  type SDKAssistantMessage,
  type SDKUserMessage,
  type SessionMessage,
  type SessionStore,
} from "@anthropic-ai/claude-agent-sdk";
import { createLogger } from "../../shared/log.ts";
import { SESSIONS_PER_PAGE } from "../../shared/runtime-constants.ts";
import { dateLabel, sessionLabel } from "../../shared/session-labels.ts";
import type { NeonSessionStore } from "./session-store.ts";
import { ensureLocalTranscript } from "./transcripts.ts";

const log = createLogger("claude-sessions");

/** A session as a harness lists it for !sessions: its id, its date label, and its one-line label. */
export interface ListedSession {
  readonly uuid: string;
  readonly timestamp: string;
  readonly label: string;
}

/** An operator message of a session that !rewind can rewind to: by its index, newest first from -1. */
export interface RewindMessage {
  readonly index: number;
  readonly timestamp: string;
  readonly text: string;
  readonly uuid: string;
}

/** The SDK's session helpers the session api reads through: the SDK's own, or a test's. */
export interface ClaudeSessionSdk {
  readonly listSessions: typeof listSdkSessions;
  readonly getSessionMessages: typeof getSessionMessages;
  readonly forkSession: typeof forkSession;
  readonly getSessionInfo: typeof getSessionInfo;
}

/**
 * The transcript store the session api reads through: the SDK's store, which can also
 * say which project holds a session and which subagent transcripts it has.
 */
export type ClaudeTranscriptStore = SessionStore & Pick<NeonSessionStore, "projectKeyOf" | "listSubkeys">;

export interface ClaudeSessionApiOptions {
  readonly workingDirectory: string;
  readonly store?: ClaudeTranscriptStore | null;
  readonly sdk?: ClaudeSessionSdk;
}

/** Claude Code's sessions of one working directory, as the operator's commands list, rewind, and resume them. */
export interface ClaudeSessionApi {
  listSessions(page?: number): Promise<ListedSession[]>;
  getTotalSessionPages(): Promise<number>;
  getSessionByNumber(num: number): Promise<string | null>;
  getSessionLastMessage(sessionId: string): Promise<string | null>;
  listSessionMessages(sessionId: string): Promise<RewindMessage[]>;
  getTotalRewindPages(sessionId: string): Promise<number>;
  createForkedSession(sessionId: string, beforeUuid: string): Promise<string | null>;
  sessionExists(sessionId: string): Promise<boolean>;
}

/** Where the SDK's helpers read transcripts: the working directory's local files, or the store. */
interface TranscriptSource {
  readonly dir: string;
  readonly sessionStore?: SessionStore;
}

/**
 * A message of a transcript as the SDK reads it. The SDK leaves `message`
 * untyped; Claude Code writes the Messages API's.
 */
type TranscriptMessage = SDKUserMessage["message"] | SDKAssistantMessage["message"];
type MessageContent = TranscriptMessage["content"];
type TextBlock = Extract<Exclude<MessageContent, string>[number], { type: "text" }>;

/**
 * A session message as read here. The SDK's messages carry no `timestamp`,
 * though the rewind list reads one.
 */
type ReadMessage = SessionMessage & { readonly timestamp?: string };

function contentOf(entry: SessionMessage): MessageContent | undefined {
  // The protocol's message, which the SDK types as unknown.
  return (entry.message as TranscriptMessage | null | undefined)?.content;
}

/**
 * Claude Code session discovery, scoped to the alasio working directory so the
 * Telegram operator only sees this workspace. With a session store (alasio's
 * Neon, see session-store.ts) the SDK's helpers read the store, the durable
 * copy; without one, or while it cannot be reached, the local transcripts
 * under `~/.claude/projects/<cwd-slug>/`.
 */
function textFromMessageContent(content: MessageContent | undefined): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .filter((block): block is TextBlock => Boolean(block) && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n");
}

function isOperatorUserMessage(entry: SessionMessage): boolean {
  if (!entry || entry.type !== "user" || entry.parent_tool_use_id) {
    return false;
  }
  const content = contentOf(entry);
  if (Array.isArray(content) && content.some((block) => block?.type === "tool_result")) {
    return false;
  }
  return Boolean(textFromMessageContent(content).trim());
}

export function createClaudeSessionApi({
  workingDirectory,
  store = null,
  sdk = { listSessions: listSdkSessions, getSessionMessages, forkSession, getSessionInfo },
}: ClaudeSessionApiOptions): ClaudeSessionApi {
  const local: TranscriptSource = { dir: workingDirectory };

  /** Runs an SDK helper against the store, or the local files without one. */
  async function fromStore<T>(what: string, call: (options: TranscriptSource) => Promise<T>): Promise<T> {
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

  async function readMessages(sessionId: string): Promise<ReadMessage[]> {
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
      return all[idx]?.sessionId ?? null;
    },

    async getSessionLastMessage(sessionId) {
      const messages = await readMessages(sessionId);
      for (let i = messages.length - 1; i >= 0; i -= 1) {
        const entry = messages[i];
        if (entry?.type !== "assistant" || entry.parent_tool_use_id) {
          continue;
        }
        const text = textFromMessageContent(contentOf(entry)).trim();
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
          text: textFromMessageContent(contentOf(entry)).trim(),
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
      // targetIndex is at least 1 here, so the message before it is there.
      const previous = messages[targetIndex - 1]!;
      try {
        const forked = await fromStore("forking a session", (options) =>
          sdk.forkSession(sessionId, { ...options, upToMessageId: previous.uuid }),
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
