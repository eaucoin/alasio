// @ts-nocheck
export class SqliteStateRepository {
  constructor(db) {
    this.db = db;
  }

  getState(key) {
    return this.db.prepare("select value from bot_state where key = ?").get(key)?.value ?? null;
  }

  setState(key, value) {
    this.db.prepare(`
      insert into bot_state (key, value, updated_at)
      values (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at
    `).run(key, String(value));
  }

  getTelegramOffset() {
    const value = this.getState("telegram_update_offset");
    return value ? Number(value) : undefined;
  }

  setTelegramOffset(offset) {
    this.setState("telegram_update_offset", String(offset));
  }
}
