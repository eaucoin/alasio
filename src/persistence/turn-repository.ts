import { Effect } from "effect";

import type { HarnessName } from "../harness/names.ts";
import { assignSession } from "./conversation-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

export type TurnState = "active" | "completed";

/**
 * A row of `turns`: a conversation's turn with its harness, kept while it runs so
 * that a restart can resume it. A conversation has one row, its latest turn.
 */
export interface Turn {
  readonly id: string;
  readonly conversation_id: string;
  readonly thread_key: string;
  readonly channel: string;
  readonly thread_ts: string;
  readonly session_id: string | null;
  readonly harness: HarnessName;
  readonly pending_response_id: string | null;
  readonly prompt: string | null;
  readonly state: TurnState;
  readonly started_at: Date;
  readonly completed_at: Date | null;
}

/** A turn starting, as recorded by upsertActiveTurn; its thread is its conversation's. */
export interface ActiveTurn {
  readonly conversationId: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly sessionId?: string | null | undefined;
  /** The harness the turn runs on. */
  readonly harness: HarnessName;
  readonly pendingResponseId?: string | null | undefined;
  readonly prompt?: string | null | undefined;
}

export class NeonTurnRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  upsertActiveTurn(turn: ActiveTurn): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.turns
         (id, conversation_id, thread_key, channel, thread_ts, session_id, harness, pending_response_id, prompt, state, started_at)
       values ($1, $1, $1, $2, $3, $4, $5, $6, $7, 'active', now())
       on conflict (id) do update set
         channel = excluded.channel,
         thread_ts = excluded.thread_ts,
         session_id = excluded.session_id,
         harness = excluded.harness,
         pending_response_id = excluded.pending_response_id,
         prompt = excluded.prompt,
         state = 'active',
         started_at = excluded.started_at,
         completed_at = null`,
      [turn.conversationId, turn.chatId, turn.messageId, turn.sessionId ?? null, turn.harness, turn.pendingResponseId ?? null, turn.prompt ?? null],
    ));
  }

  /**
   * Records the session the conversation's active turn runs in, on the turn and as its
   * harness's session in the conversation (the mounted harness's, when no turn is active).
   */
  updateActiveTurnSessionId(threadKey: string, sessionId: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with turn as (
         update ${this.#schema}.turns set session_id = $2 where id = $1 and state = 'active' returning harness
       )
       update ${this.#schema}.conversations set ${assignSession("coalesce((select harness from turn), active_harness)", "$2")}, updated_at = now()
       where id = $1`,
      [threadKey, sessionId],
    ));
  }

  updateActiveTurnPendingResponseId(threadKey: string, pendingResponseId: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.turns set pending_response_id = $2 where id = $1 and state = 'active'`,
      [threadKey, pendingResponseId],
    ));
  }

  /** Completes the conversation's active turn; given a pending response, only the turn of that response. */
  clearActiveTurn(threadKey: string, pendingResponseId: string | null = null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.turns set state = 'completed', completed_at = now()
       where id = $1 and state = 'active' and ($2::text is null or pending_response_id = $2)`,
      [threadKey, pendingResponseId],
    ));
  }

  getActiveTurns(): Effect.Effect<Turn[], StoreError> {
    return this.#sql.query<Turn>(`select * from ${this.#schema}.turns where state = 'active' order by started_at`);
  }

  /** The conversation's turn, while it is active. */
  getActiveTurn(threadKey: string): Effect.Effect<Turn | null, StoreError> {
    return this.#sql.query<Turn>(`select * from ${this.#schema}.turns where id = $1 and state = 'active'`, [threadKey]).pipe(
      Effect.map(([row]) => row ?? null),
    );
  }
}
