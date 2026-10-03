import type { Database } from "better-sqlite3";

export class SqliteStateRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  getState(key: string): string | null {
    return this.db.prepare<[string], { value: string }>("select value from bot_state where key = ?").get(key)?.value ?? null;
  }

  setState(key: string, value: string | number): void {
    this.db.prepare<[key: string, value: string]>(`
      insert into bot_state (key, value, updated_at)
      values (?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at
    `).run(key, String(value));
  }

  getTelegramOffset(): number | undefined {
    const value = this.getState("telegram_update_offset");
    return value ? Number(value) : undefined;
  }

  setTelegramOffset(offset: number): void {
    this.setState("telegram_update_offset", String(offset));
  }
}
