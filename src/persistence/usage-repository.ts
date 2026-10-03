import type { Database } from "better-sqlite3";

/** Token usage a harness reports for a session's latest turn. */
export interface SessionUsage {
  readonly cacheReadInputTokens?: number | null | undefined;
}

export class SqliteUsageRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  updateSessionUsage(sessionId: string | null | undefined, usage: SessionUsage | null | undefined): void {
    if (!sessionId || !usage) {
      return;
    }
    const tokens = usage.cacheReadInputTokens ?? 0;
    if (tokens <= 0) {
      return;
    }
    this.db.prepare<[sessionId: string, cacheReadInputTokens: number, updatedAt: string]>(`
      insert into session_usage (session_id, cache_read_input_tokens, updated_at)
      values (?, ?, ?)
      on conflict(session_id) do update set
        cache_read_input_tokens = excluded.cache_read_input_tokens,
        updated_at = excluded.updated_at
    `).run(sessionId, tokens, new Date().toISOString());
  }

  getSessionTokens(sessionId: string): number {
    return this.db.prepare<[string], { cache_read_input_tokens: number }>("select cache_read_input_tokens from session_usage where session_id = ?").get(sessionId)?.cache_read_input_tokens ?? 0;
  }
}
