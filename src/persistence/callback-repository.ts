// @ts-nocheck
import { newId } from "../shared/ids.ts";

export class SqliteCallbackRepository {
  constructor(db) {
    this.db = db;
  }

  createCallbackAction({ conversationId, kind, payload }) {
    const id = newId().replace(/-/g, "").slice(0, 24);
    this.db.prepare(`
      insert into callback_actions (id, conversation_id, kind, payload_json)
      values (?, ?, ?, ?)
    `).run(id, conversationId, kind, JSON.stringify(payload ?? {}));
    return id;
  }

  consumeCallbackAction(id) {
    const row = this.db.prepare(`
      select * from callback_actions
      where id = ? and consumed_at is null
    `).get(id);
    if (!row) {
      return null;
    }
    this.db.prepare("update callback_actions set consumed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where id = ?").run(id);
    return {
      id: row.id,
      conversationId: row.conversation_id,
      kind: row.kind,
      payload: JSON.parse(row.payload_json),
    };
  }
}
