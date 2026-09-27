import { CLAUDE_HARNESS, CODEX_HARNESS, getDefaultHarness, isHarnessName } from "../harness/names.js";

const SESSION_COLUMNS = Object.freeze({
  [CODEX_HARNESS]: "codex_session_id",
  [CLAUDE_HARNESS]: "claude_session_id",
});

const MODEL_COLUMNS = {
  [CLAUDE_HARNESS]: { model: "claude_model", effort: "claude_effort" },
  [CODEX_HARNESS]: { model: "codex_model", effort: "codex_effort" },
};

function modelColumns(harness) {
  const columns = MODEL_COLUMNS[harness];
  if (!columns) {
    throw new Error(`Unknown harness: ${String(harness)}`);
  }
  return columns;
}

function sessionColumn(harness) {
  const column = SESSION_COLUMNS[harness];
  if (!column) {
    throw new Error(`Unknown harness: ${String(harness)}`);
  }
  return column;
}

export class SqliteConversationRepository {
  constructor(db, { defaultWorkingDirectory = null } = {}) {
    this.db = db;
    this.defaultWorkingDirectory = defaultWorkingDirectory;
  }

  upsertConversation({ chatId, user, sessionId }) {
    const id = `telegram:${chatId}`;
    this.db.prepare(`
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

  getConversationByChatId(chatId) {
    return this.db.prepare("select * from conversations where transport = 'telegram' and chat_id = ?").get(String(chatId)) ?? null;
  }

  getConversation(conversationId) {
    return this.db.prepare("select * from conversations where id = ?").get(conversationId) ?? null;
  }

  listConversationsWithSessions(harness = CODEX_HARNESS) {
    const column = sessionColumn(harness);
    return this.db.prepare(`
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
  listHarnessSessionReferences(harness) {
    const column = sessionColumn(harness);
    return this.db.prepare(`
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
  getWorkingDirectory(threadKey) {
    return this.getConversation(threadKey)?.working_directory ?? null;
  }

  /**
   * The model and effort chosen for one harness in this conversation, or null
   * when none has been chosen and the harness's pinned default applies.
   */
  getModelChoice(threadKey, harness) {
    const { model, effort } = modelColumns(harness);
    const row = this.getConversation(threadKey);
    if (!row?.[model]) {
      return null;
    }
    return { model: row[model], effort: row[effort] ?? null };
  }

  setModelChoice(threadKey, harness, { model, effort = null }) {
    if (typeof model !== "string" || !model.trim()) {
      throw new Error("Model must be a non-empty id");
    }
    const columns = modelColumns(harness);
    this.db.prepare(`
      update conversations
      set ${columns.model} = ?, ${columns.effort} = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(model, effort, threadKey);
  }

  clearModelChoice(threadKey, harness) {
    const columns = modelColumns(harness);
    this.db.prepare(`
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
  setWorkingDirectory(threadKey, workingDirectory) {
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
    const park = this.db.prepare(`
      insert into workspace_sessions (conversation_id, harness, working_directory, session_id, updated_at)
      values (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(conversation_id, harness, working_directory) do update set
        session_id = excluded.session_id,
        updated_at = excluded.updated_at
    `);
    const parked = this.db.prepare(`
      select session_id from workspace_sessions
      where conversation_id = ? and harness = ? and working_directory = ?
    `);
    this.db.transaction(() => {
      const restored = {};
      for (const [harness, column] of Object.entries(SESSION_COLUMNS)) {
        if (previous) {
          park.run(threadKey, harness, previous, conversation[column] ?? null);
        }
        restored[column] = parked.get(threadKey, harness, workingDirectory)?.session_id ?? null;
      }
      this.db.prepare(`
        update conversations
        set working_directory = ?, codex_session_id = ?, claude_session_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        where id = ?
      `).run(workingDirectory, restored.codex_session_id, restored.claude_session_id, threadKey);
    })();
  }

  /**
   * Active harness for a conversation, or null while nothing is mounted.
   */
  getActiveHarness(threadKey) {
    const harness = this.getConversation(threadKey)?.active_harness;
    return isHarnessName(harness) ? harness : null;
  }

  requireActiveHarness(threadKey) {
    const harness = this.getActiveHarness(threadKey);
    if (!harness) {
      throw new Error(`No service is mounted for ${threadKey}`);
    }
    return harness;
  }

  setActiveHarness(threadKey, harness) {
    if (!isHarnessName(harness)) {
      throw new Error(`Unknown harness: ${String(harness)}`);
    }
    this.db.prepare(`
      update conversations
      set active_harness = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(harness, threadKey);
  }

  getHarnessSessionId(threadKey, harness) {
    return this.getConversation(threadKey)?.[sessionColumn(harness)] ?? undefined;
  }

  setHarnessSessionId(threadKey, harness, sessionId) {
    this.db.prepare(`
      update conversations
      set ${sessionColumn(harness)} = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(sessionId, threadKey);
  }

  getSessionId(threadKey) {
    const conversation = this.getConversation(threadKey);
    if (!conversation) {
      return undefined;
    }
    const harness = isHarnessName(conversation.active_harness) ? conversation.active_harness : null;
    return harness ? conversation[sessionColumn(harness)] ?? undefined : undefined;
  }

  setSessionId(threadKey, sessionId) {
    this.setHarnessSessionId(threadKey, this.requireActiveHarness(threadKey), sessionId);
  }

  clearSessionId(threadKey) {
    this.setHarnessSessionId(threadKey, this.requireActiveHarness(threadKey), null);
  }
}
