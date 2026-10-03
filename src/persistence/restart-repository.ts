// @ts-nocheck
export class SqliteRestartRepository {
  constructor(db) {
    this.db = db;
  }

  recordRestartEvent(event) {
    this.db.prepare(`
      insert into restart_events (thread_key, payload_json, created_at)
      values (?, ?, ?)
      on conflict(thread_key) do update set payload_json = excluded.payload_json, created_at = excluded.created_at
    `).run(event.thread_key, JSON.stringify(event), Date.now() / 1000);
  }

  getRestartEvent(threadKey) {
    const row = this.db.prepare("select payload_json from restart_events where thread_key = ?").get(threadKey);
    return row ? JSON.parse(row.payload_json) : null;
  }

  clearRestartEvent(threadKey) {
    this.db.prepare("delete from restart_events where thread_key = ?").run(threadKey);
  }

  consumeRestartEvent(threadKey) {
    const event = this.getRestartEvent(threadKey);
    if (event) {
      this.clearRestartEvent(threadKey);
    }
    return event;
  }
}
