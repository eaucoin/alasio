export class SqliteUsageRepository {
  constructor(db) {
    this.db = db;
  }

  updateSessionUsage(sessionId, usage) {
    if (!sessionId || !usage) {
      return;
    }
    const tokens = usage.cacheReadInputTokens ?? 0;
    if (tokens <= 0) {
      return;
    }
    this.db.prepare(`
      insert into session_usage (session_id, cache_read_input_tokens, updated_at)
      values (?, ?, ?)
      on conflict(session_id) do update set
        cache_read_input_tokens = excluded.cache_read_input_tokens,
        updated_at = excluded.updated_at
    `).run(sessionId, tokens, new Date().toISOString());
  }

  getSessionTokens(sessionId) {
    return this.db.prepare("select cache_read_input_tokens from session_usage where session_id = ?").get(sessionId)?.cache_read_input_tokens ?? 0;
  }
}
