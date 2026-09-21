export class SqliteConversationRepository {
  constructor(db) {
    this.db = db;
  }

  upsertConversation({ chatId, user, sessionId }) {
    const id = `telegram:${chatId}`;
    this.db.prepare(`
      insert into conversations (id, transport, chat_id, user_id, username, first_name, last_name, codex_session_id)
      values (?, 'telegram', ?, ?, ?, ?, ?, ?)
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
    );
    return id;
  }

  getConversationByChatId(chatId) {
    return this.db.prepare("select * from conversations where transport = 'telegram' and chat_id = ?").get(String(chatId)) ?? null;
  }

  getConversation(conversationId) {
    return this.db.prepare("select * from conversations where id = ?").get(conversationId) ?? null;
  }

  listConversationsWithSessions() {
    return this.db.prepare(`
      select id, codex_session_id
      from conversations
      where codex_session_id is not null and codex_session_id <> ''
      order by updated_at desc
    `).all();
  }

  getSessionId(threadKey) {
    return this.getConversation(threadKey)?.codex_session_id ?? undefined;
  }

  setSessionId(threadKey, sessionId) {
    this.db.prepare(`
      update conversations
      set codex_session_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(sessionId, threadKey);
  }

  clearSessionId(threadKey) {
    this.db.prepare(`
      update conversations
      set codex_session_id = null, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
      where id = ?
    `).run(threadKey);
  }
}
