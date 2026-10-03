import type { Database } from "better-sqlite3";
import { newId } from "../shared/ids.ts";

/**
 * What an inline button carries back when pressed. Its fields depend on the action's
 * kind, and it has been through JSON, so readers check the ones they use.
 */
export type CallbackPayload = Readonly<Record<string, unknown>>;

/** A row of `callback_actions`: an inline button's action, waiting to be pressed once. */
export interface CallbackActionRow {
  readonly id: string;
  readonly conversation_id: string;
  readonly kind: string;
  readonly payload_json: string;
  readonly consumed_at: string | null;
  readonly created_at: string;
}

export interface NewCallbackAction {
  readonly conversationId: string;
  readonly kind: string;
  readonly payload?: CallbackPayload | undefined;
}

/** A pressed button's action. */
export interface CallbackAction {
  readonly id: string;
  readonly conversationId: string;
  readonly kind: string;
  readonly payload: CallbackPayload;
}

export class SqliteCallbackRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  createCallbackAction({ conversationId, kind, payload }: NewCallbackAction): string {
    const id = newId().replace(/-/g, "").slice(0, 24);
    this.db.prepare<[id: string, conversationId: string, kind: string, payloadJson: string]>(`
      insert into callback_actions (id, conversation_id, kind, payload_json)
      values (?, ?, ?, ?)
    `).run(id, conversationId, kind, JSON.stringify(payload ?? {}));
    return id;
  }

  consumeCallbackAction(id: string): CallbackAction | null {
    const row = this.db.prepare<[string], CallbackActionRow>(`
      select * from callback_actions
      where id = ? and consumed_at is null
    `).get(id);
    if (!row) {
      return null;
    }
    this.db.prepare<[string]>("update callback_actions set consumed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where id = ?").run(id);
    return {
      id: row.id,
      conversationId: row.conversation_id,
      kind: row.kind,
      // createCallbackAction is the only writer, and it writes an object.
      payload: JSON.parse(row.payload_json) as CallbackPayload,
    };
  }
}
