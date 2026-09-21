import { CLAUDE_HARNESS, CODEX_HARNESS, getDefaultHarness, isHarnessName } from "../harness/names.js";

const SESSION_COLUMNS = Object.freeze({
  [CODEX_HARNESS]: "codex_session_id",
  [CLAUDE_HARNESS]: "claude_session_id",
});

function sessionColumn(harness) {
  const column = SESSION_COLUMNS[harness];
  if (!column) {
    throw new Error(`Unknown harness: ${String(harness)}`);
  }
  return column;
}

export class SqliteConversationRepository {
  constructor(db) {
    this.db = db;
  }

  upsertConversation({ chatId, user, sessionId }) {
    const id = `telegram:${chatId}`;
    this.db.prepare(`
      insert into conversations (id, transport, chat_id, user_id, username, first_name, last_name, codex_session_id, active_harness)
      values (?, 'telegram', ?, ?, ?, ?, ?, ?, ?)
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
      select id, ${column} as session_id, ${column} as codex_session_id, active_harness
      from conversations
      where ${column} is not null and ${column} <> '' and active_harness = ?
      order by updated_at desc
    `).all(harness);
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
