import type { Database } from "better-sqlite3";

/** What restarted alasio under a turn: the turn itself, the operator, or something else. */
export type RestartCause = "self_induced" | "operator_induced" | "external_or_unknown";

/**
 * A restart that cut a conversation's turn short, kept (as the payload of a row of
 * `restart_events`) until the turn is resumed after it.
 */
export interface RestartEvent {
  readonly cause: RestartCause;
  readonly thread_key: string;
  readonly channel: string | number;
  readonly thread_ts: string | number;
  readonly session_id: string | null;
  /** The command that restarted alasio, for a self-induced restart. */
  readonly command?: string;
  readonly timestamp: number;
}

export class SqliteRestartRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  recordRestartEvent(event: RestartEvent): void {
    this.db.prepare<[threadKey: string, payloadJson: string, createdAt: number]>(`
      insert into restart_events (thread_key, payload_json, created_at)
      values (?, ?, ?)
      on conflict(thread_key) do update set payload_json = excluded.payload_json, created_at = excluded.created_at
    `).run(event.thread_key, JSON.stringify(event), Date.now() / 1000);
  }

  getRestartEvent(threadKey: string): RestartEvent | null {
    const row = this.db.prepare<[string], { payload_json: string }>("select payload_json from restart_events where thread_key = ?").get(threadKey);
    // recordRestartEvent is the only writer of the payload.
    return row ? JSON.parse(row.payload_json) as RestartEvent : null;
  }

  clearRestartEvent(threadKey: string): void {
    this.db.prepare<[string]>("delete from restart_events where thread_key = ?").run(threadKey);
  }

  consumeRestartEvent(threadKey: string): RestartEvent | null {
    const event = this.getRestartEvent(threadKey);
    if (event) {
      this.clearRestartEvent(threadKey);
    }
    return event;
  }
}
