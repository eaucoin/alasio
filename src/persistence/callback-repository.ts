import { Effect } from "effect";

import { newId } from "../shared/ids.ts";
import { sessionOf } from "./conversation-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

/**
 * What an inline button carries back when pressed. Its fields depend on the action's
 * kind, and it has been through JSON, so readers check the ones they use.
 */
export type CallbackPayload = Readonly<Record<string, unknown>>;

/** A row of `callback_actions`: an inline button's action, kept until it is pressed. */
export interface CallbackActionRow {
  readonly id: string;
  readonly conversation_id: string;
  readonly kind: string;
  readonly payload: CallbackPayload;
  readonly expected_session_id: string | null;
  readonly expected_harness: string | null;
  readonly created_at: Date;
}

/** A button's action, before it is kept. */
export interface NewCallbackAction {
  readonly kind: string;
  readonly payload?: CallbackPayload | undefined;
}

/**
 * A pressed button's action. Its payload carries, besides what the button was made
 * with, what the conversation had mounted then: `expectedSessionId` and `expectedHarness`.
 */
export interface CallbackAction {
  readonly id: string;
  readonly conversationId: string;
  readonly kind: string;
  readonly payload: CallbackPayload;
}

export class NeonCallbackRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Buttons' actions in the conversation, each with what the conversation has mounted now: their ids, in order. */
  createCallbackActions(conversationId: string, actions: readonly NewCallbackAction[]): Effect.Effect<string[], StoreError> {
    // Short, as Telegram holds a button's callback data to 64 bytes.
    const ids = actions.map(() => newId().replace(/-/g, "").slice(0, 24));
    return this.#sql.query(
      `insert into ${this.#schema}.callback_actions (id, conversation_id, kind, payload, expected_session_id, expected_harness)
       select action.id, conversations.id, action.kind, action.payload, ${sessionOf("active_harness")}, active_harness
       from unnest($2::text[], $3::text[], $4::json[]) as action (id, kind, payload), ${this.#schema}.conversations
       where conversations.id = $1`,
      [conversationId, ids, actions.map((action) => action.kind), actions.map((action) => JSON.stringify(action.payload ?? {}))],
    ).pipe(Effect.as(ids));
  }

  /** The action of a button pressed for the first time, deleted as it acts once; null for one pressed before, or never made. */
  consumeCallbackAction(id: string): Effect.Effect<CallbackAction | null, StoreError> {
    return this.#sql.query<CallbackActionRow>(
      `delete from ${this.#schema}.callback_actions where id = $1 returning *`,
      [id],
    ).pipe(Effect.map(([row]) =>
      row
        ? {
          id: row.id,
          conversationId: row.conversation_id,
          kind: row.kind,
          payload: { ...row.payload, expectedSessionId: row.expected_session_id, expectedHarness: row.expected_harness },
        }
        : null
    ));
  }
}
