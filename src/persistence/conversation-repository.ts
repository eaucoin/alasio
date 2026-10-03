import type { Database } from "better-sqlite3";
import { CLAUDE_HARNESS, CODEX_HARNESS, getDefaultHarness, type HarnessName, isHarnessName } from "../harness/names.ts";

type SessionColumn = "codex_session_id" | "claude_session_id";

interface ModelColumns {
  readonly model: "claude_model" | "codex_model";
  readonly effort: "claude_effort" | "codex_effort";
}

const SESSION_COLUMNS: Readonly<Record<HarnessName, SessionColumn>> = Object.freeze({
  [CODEX_HARNESS]: "codex_session_id",
  [CLAUDE_HARNESS]: "claude_session_id",
});

const MODEL_COLUMNS: Readonly<Record<HarnessName, ModelColumns>> = {
  [CLAUDE_HARNESS]: { model: "claude_model", effort: "claude_effort" },
  [CODEX_HARNESS]: { model: "codex_model", effort: "codex_effort" },
};

function modelColumns(harness: HarnessName): ModelColumns {
  const columns = MODEL_COLUMNS[harness];
  if (!columns) {
    throw new Error(`Unknown harness: ${String(harness)}`);
  }
  return columns;
}

function sessionColumn(harness: HarnessName): SessionColumn {
  const column = SESSION_COLUMNS[harness];
  if (!column) {
    throw new Error(`Unknown harness: ${String(harness)}`);
  }
  return column;
}

/**
 * A row of `conversations`: one Telegram chat, with the harness, folder, sessions,
 * and models it has mounted.
 */
export interface Conversation {
  readonly id: string;
  readonly transport: string;
  readonly chat_id: string;
  readonly user_id: string | null;
  readonly username: string | null;
  readonly first_name: string | null;
  readonly last_name: string | null;
  readonly codex_session_id: string | null;
  readonly claude_session_id: string | null;
  readonly active_harness: HarnessName | null;
  readonly working_directory: string | null;
  readonly claude_model: string | null;
  readonly claude_effort: string | null;
  readonly codex_model: string | null;
  readonly codex_effort: string | null;
  readonly created_at: string;
  readonly updated_at: string;
}

/** The Telegram user a conversation is with, as far as the conversation keeps them. */
export interface ConversationUser {
  readonly id: number | string;
  readonly username?: string | undefined;
  readonly first_name?: string | undefined;
  readonly last_name?: string | undefined;
}

export interface NewConversation {
  readonly chatId: number | string;
  readonly user?: ConversationUser | undefined;
  readonly sessionId?: string | null | undefined;
}

/** A conversation whose mounted harness has a session, in the folder it runs in. */
export interface LinkedConversation {
  readonly id: string;
  readonly session_id: string;
  /** The session again, under the name it had when only Codex had sessions. */
  readonly codex_session_id: string;
  readonly active_harness: HarnessName;
  readonly working_directory: string;
}

/** A harness session alasio points at, and the folder it runs in. */
export interface HarnessSessionReference {
  readonly sessionId: string;
  readonly workingDirectory: string;
}

/** A model, and optionally a reasoning effort, chosen for a harness with /model. */
export interface ModelChoice {
  readonly model: string;
  readonly effort: string | null;
}

/** A model to choose with /model; without an effort, the model's default applies. */
export interface NewModelChoice {
  readonly model: string;
  readonly effort?: string | null | undefined;
}

export interface ConversationRepositoryOptions {
  readonly defaultWorkingDirectory?: string | null | undefined;
}

export class SqliteConversationRepository {
  private readonly db: Database;
  private readonly defaultWorkingDirectory: string | null;

  constructor(db: Database, { defaultWorkingDirectory = null }: ConversationRepositoryOptions = {}) {
    this.db = db;
    this.defaultWorkingDirectory = defaultWorkingDirectory;
  }

  upsertConversation({ chatId, user, sessionId }: NewConversation): string {
    const id = `telegram:${chatId}`;
    this.db.prepare<[
      id: string,
      chatId: string,
      userId: string | null,
      username: string | null,
      firstName: string | null,
      lastName: string | null,
      codexSessionId: string | null,
      activeHarness: HarnessName | null,
      workingDirectory: string | null,
    ]>(`
      insert into conversations (id, transport, chat_id, user_id, username, first_name, last_name, codex_session_id, active_harness, working_directory)
      values (?, 'telegram', ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(id) do update set
        user_id = coalesce(excluded.user_id, conversations.user_id),
        username = coalesce(excluded.username, conversations.username),
        first_name = coalesce(excluded.first_name, conversations.first_name),
        last_name = coalesce(excluded.last_name, conversations.last_name),
        codex_session_id = coalesce(excluded.codex_session_id, conversations.codex_session_id),
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    `).run(
      id,
      String(chatId),
      user?.id != null ? String(user.id) : null,
      user?.username ?? null,
      user?.first_name ?? null,
      user?.last_name ?? null,
      sessionId ?? null,
      getDefaultHarness(),
      this.defaultWorkingDirectory,
    );
    return id;
  }

  getConversationByChatId(chatId: number | string): Conversation | null {
    return this.db.prepare<[string], Conversation>("select * from conversations where transport = 'telegram' and chat_id = ?").get(String(chatId)) ?? null;
  }

  getConversation(conversationId: string): Conversation | null {
    return this.db.prepare<[string], Conversation>("select * from conversations where id = ?").get(conversationId) ?? null;
  }

  listConversationsWithSessions(harness: HarnessName = CODEX_HARNESS): LinkedConversation[] {
    const column = sessionColumn(harness);
    return this.db.prepare<[HarnessName], LinkedConversation>(`
      select id, ${column} as session_id, ${column} as codex_session_id, active_harness, working_directory
      from conversations
      where ${column} is not null and ${column} <> '' and active_harness = ? and working_directory is not null
      order by updated_at desc
    `).all(harness);
  }

  /**
   * Every session of a harness alasio points at, with the folder it runs in:
   * each conversation's mounted one, the ones parked per folder, and those of
   * active turns. Distinct by session.
   */
  listHarnessSessionReferences(harness: HarnessName): HarnessSessionReference[] {
    const column = sessionColumn(harness);
    return this.db.prepare<[HarnessName, HarnessName], HarnessSessionReference>(`
      select session_id as sessionId, min(working_directory) as workingDirectory from (
        select ${column} as session_id, working_directory from conversations
        where ${column} is not null and ${column} <> '' and working_directory is not null
        union
        select session_id, working_directory from workspace_sessions
        where harness = ? and session_id is not null and session_id <> ''
        union
        select turns.session_id, conversations.working_directory from turns
        join conversations on conversations.id = turns.conversation_id
        where turns.harness = ? and turns.session_id is not null and turns.session_id <> ''
          and conversations.working_directory is not null
      ) group by session_id
    `).all(harness, harness);
  }

  /**
   * Folder the conversation's harness runs in, or null until one is chosen.
   */
  getWorkingDirectory(threadKey: string): string | null {
    return this.getConversation(threadKey)?.working_directory ?? null;
  }

  /**
   * The model and effort chosen for one harness in this conversation, or null
   * when none has been chosen and the harness's pinned default applies.
   */
  getModelChoice(threadKey: string, harness: HarnessName): ModelChoice | null {
    const { model, effort } = modelColumns(harness);
    const row = this.getConversation(threadKey);
    if (!row?.[model]) {
      return null;
    }
    return { model: row[model], effort: row[effort] ?? null };
  }

  setModelChoice(threadKey: string, harness: HarnessName, { model, effort = null }: NewModelChoice): void {
    if (typeof model !== "string" || !model.trim()) {
      throw new Error("Model must be a non-empty id");
    }
    const columns = modelColumns(harness);
    this.db.prepare<[model: string, effort: string | null, id: string]>(`
      update conversations
      set ${columns.model} = ?, ${columns.effort} = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(model, effort, threadKey);
  }

  clearModelChoice(threadKey: string, harness: HarnessName): void {
    const columns = modelColumns(harness);
    this.db.prepare<[string]>(`
      update conversations
      set ${columns.model} = null, ${columns.effort} = null, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(threadKey);
  }

  /**
   * Mount a folder. Sessions belong to one harness and one folder, so the
   * current session pointers are parked under the outgoing folder and whatever
   * was parked for the incoming folder is restored.
   */
  setWorkingDirectory(threadKey: string, workingDirectory: string): void {
    if (typeof workingDirectory !== "string" || !workingDirectory.trim()) {
      throw new Error("Working directory must be a non-empty path");
    }
    const conversation = this.getConversation(threadKey);
    if (!conversation) {
      throw new Error(`Unknown conversation: ${threadKey}`);
    }
    const previous = conversation.working_directory ?? null;
    if (previous === workingDirectory) {
      return;
    }
    const park = this.db.prepare<[conversationId: string, harness: string, workingDirectory: string, sessionId: string | null]>(`
      insert into workspace_sessions (conversation_id, harness, working_directory, session_id, updated_at)
      values (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(conversation_id, harness, working_directory) do update set
        session_id = excluded.session_id,
        updated_at = excluded.updated_at
    `);
    const parked = this.db.prepare<[conversationId: string, harness: string, workingDirectory: string], { session_id: string | null }>(`
      select session_id from workspace_sessions
      where conversation_id = ? and harness = ? and working_directory = ?
    `);
    this.db.transaction(() => {
      const restored: Record<SessionColumn, string | null> = { codex_session_id: null, claude_session_id: null };
      for (const [harness, column] of Object.entries(SESSION_COLUMNS)) {
        if (previous) {
          park.run(threadKey, harness, previous, conversation[column] ?? null);
        }
        restored[column] = parked.get(threadKey, harness, workingDirectory)?.session_id ?? null;
      }
      this.db.prepare<[workingDirectory: string, codexSessionId: string | null, claudeSessionId: string | null, id: string]>(`
        update conversations
        set working_directory = ?, codex_session_id = ?, claude_session_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        where id = ?
      `).run(workingDirectory, restored.codex_session_id, restored.claude_session_id, threadKey);
    })();
  }

  /**
   * Active harness for a conversation, or null while nothing is mounted.
   */
  getActiveHarness(threadKey: string): HarnessName | null {
    const harness = this.getConversation(threadKey)?.active_harness;
    return isHarnessName(harness) ? harness : null;
  }

  requireActiveHarness(threadKey: string): HarnessName {
    const harness = this.getActiveHarness(threadKey);
    if (!harness) {
      throw new Error(`No service is mounted for ${threadKey}`);
    }
    return harness;
  }

  setActiveHarness(threadKey: string, harness: HarnessName): void {
    if (!isHarnessName(harness)) {
      throw new Error(`Unknown harness: ${String(harness)}`);
    }
    this.db.prepare<[harness: HarnessName, id: string]>(`
      update conversations
      set active_harness = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(harness, threadKey);
  }

  getHarnessSessionId(threadKey: string, harness: HarnessName): string | undefined {
    return this.getConversation(threadKey)?.[sessionColumn(harness)] ?? undefined;
  }

  setHarnessSessionId(threadKey: string, harness: HarnessName, sessionId: string | null): void {
    this.db.prepare<[sessionId: string | null, id: string]>(`
      update conversations
      set ${sessionColumn(harness)} = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(sessionId, threadKey);
  }

  getSessionId(threadKey: string): string | undefined {
    const conversation = this.getConversation(threadKey);
    if (!conversation) {
      return undefined;
    }
    const harness = isHarnessName(conversation.active_harness) ? conversation.active_harness : null;
    return harness ? conversation[sessionColumn(harness)] ?? undefined : undefined;
  }

  setSessionId(threadKey: string, sessionId: string | null): void {
    this.setHarnessSessionId(threadKey, this.requireActiveHarness(threadKey), sessionId);
  }

  clearSessionId(threadKey: string): void {
    this.setHarnessSessionId(threadKey, this.requireActiveHarness(threadKey), null);
  }
}
