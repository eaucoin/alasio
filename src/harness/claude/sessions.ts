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
import { Effect } from "effect";

import { withLogScope } from "../../shared/log.ts";
import { SESSIONS_PER_PAGE } from "../../shared/runtime-constants.ts";
import { dateLabel, sessionLabel } from "../../shared/session-labels.ts";
import type { HarnessSessions } from "../index.ts";
import { ClaudeCodeError } from "./runtime.ts";
import type { NeonSessionStore } from "./session-store.ts";
import { ensureLocalTranscript } from "./transcripts.ts";

const LOG_SCOPE = "claude-sessions";

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

/**
 * Claude Code's sessions of one working directory, as the operator's commands list,
 * rewind, and resume them, and as a turn finds the session it resumes. Listing fails as
 * the SDK fails it; reading a session reads an unreadable one as empty.
 */
export interface ClaudeSessionApi extends HarnessSessions {
  readonly listSessions: (page?: number) => Effect.Effect<ListedSession[], ClaudeCodeError>;
  readonly getTotalSessionPages: () => Effect.Effect<number, ClaudeCodeError>;
  readonly getSessionByNumber: (num: number) => Effect.Effect<string | null, ClaudeCodeError>;
  readonly getSessionLastMessage: (sessionId: string) => Effect.Effect<string | null>;
  readonly listSessionMessages: (sessionId: string) => Effect.Effect<RewindMessage[]>;
  readonly getTotalRewindPages: (sessionId: string) => Effect.Effect<number>;
  readonly createForkedSession: (sessionId: string, beforeUuid: string) => Effect.Effect<string | null>;
  /** Whether the session can be resumed here, its transcript written back from the store first if it is gone. */
  readonly sessionExists: (sessionId: string) => Effect.Effect<boolean>;
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

/** A promise of the SDK's session helpers, failing as they failed. */
const fromSdk = <A>(call: () => Promise<A>): Effect.Effect<A, ClaudeCodeError> =>
  Effect.tryPromise({ try: call, catch: (cause) => new ClaudeCodeError({ cause }) });

export function createClaudeSessionApi({
  workingDirectory,
  store = null,
  sdk = { listSessions: listSdkSessions, getSessionMessages, forkSession, getSessionInfo },
}: ClaudeSessionApiOptions): ClaudeSessionApi {
  const local: TranscriptSource = { dir: workingDirectory };
  const scoped = withLogScope(LOG_SCOPE);

  /** Runs an SDK helper against the store, or the local files without one. */
  const fromStore = <A>(what: string, call: (options: TranscriptSource) => Promise<A>): Effect.Effect<A, ClaudeCodeError> =>
    store
      ? fromSdk(() => call({ ...local, sessionStore: store })).pipe(
        Effect.catch((error) =>
          Effect.logWarning(`${what} fell back to local transcripts: ${error.message}`).pipe(Effect.andThen(fromSdk(() => call(local))))
        ),
      )
      : fromSdk(() => call(local));

  const listAll = Effect.map(
    fromStore("listing sessions", (options) => sdk.listSessions(options)),
    (sessions) => [...sessions].sort((a, b) => Number(b.lastModified ?? 0) - Number(a.lastModified ?? 0)),
  );

  const readMessages = (sessionId: string): Effect.Effect<ReadMessage[]> =>
    fromStore("reading a session", (options): Promise<ReadMessage[]> => sdk.getSessionMessages(sessionId, options)).pipe(
      Effect.catch(() => Effect.succeed([])),
    );

  const listSessionMessages = (sessionId: string): Effect.Effect<RewindMessage[]> =>
    Effect.map(readMessages(sessionId), (messages) => {
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
    });

  return {
    listSessions: (page = 1) =>
      Effect.map(listAll, (all) => {
        const startIdx = (page - 1) * SESSIONS_PER_PAGE;
        return all.slice(startIdx, startIdx + SESSIONS_PER_PAGE).map((session) => ({
          uuid: session.sessionId,
          timestamp: dateLabel(session.lastModified),
          label: sessionLabel(session.customTitle || session.summary || session.firstPrompt || session.sessionId),
        }));
      }).pipe(scoped),

    getTotalSessionPages: () => Effect.map(listAll, (all) => Math.ceil(all.length / SESSIONS_PER_PAGE) || 1).pipe(scoped),

    getSessionByNumber: (num) =>
      Effect.map(listAll, (all) => {
        const idx = num - 1;
        if (idx < 0 || idx >= all.length) {
          return null;
        }
        return all[idx]?.sessionId ?? null;
      }).pipe(scoped),

    getSessionLastMessage: (sessionId) =>
      Effect.map(readMessages(sessionId), (messages) => {
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
      }).pipe(scoped),

    listSessionMessages: (sessionId) => listSessionMessages(sessionId).pipe(scoped),

    getTotalRewindPages: (sessionId) =>
      Effect.map(listSessionMessages(sessionId), (messages) => Math.ceil(messages.length / SESSIONS_PER_PAGE) || 1).pipe(scoped),

    createForkedSession: Effect.fnUntraced(function*(sessionId: string, beforeUuid: string) {
      const messages = yield* readMessages(sessionId);
      const targetIndex = messages.findIndex((entry) => entry?.uuid === beforeUuid);
      if (targetIndex < 0) {
        return null;
      }
      if (targetIndex === 0) {
        return randomUUID();
      }
      // targetIndex is at least 1 here, so the message before it is there.
      const previous = messages[targetIndex - 1]!;
      return yield* fromStore("forking a session", (options) => sdk.forkSession(sessionId, { ...options, upToMessageId: previous.uuid })).pipe(
        Effect.map((forked) => forked?.sessionId ?? null),
        Effect.catch(() => Effect.succeed(null)),
      );
    }, scoped),

    sessionExists: Effect.fnUntraced(function*(sessionId: string) {
      if (!sessionId) {
        return false;
      }
      // A resume runs from the local transcript: write it back from the
      // store first if it is gone.
      if (store) {
        const present = yield* ensureLocalTranscript(store, sessionId).pipe(
          Effect.catch((error) => Effect.logWarning(`could not check the store for ${sessionId}: ${error.message}`).pipe(Effect.as(false))),
        );
        if (present) return true;
      }
      return yield* fromSdk(() => sdk.getSessionInfo(sessionId, local)).pipe(
        Effect.map(Boolean),
        Effect.catch(() => Effect.succeed(false)),
      );
    }, scoped),
  };
}
