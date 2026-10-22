import { Effect } from "effect";

import type { HarnessName } from "../harness/names.ts";
import { assignSession } from "./conversation-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

export type TurnState = "active" | "completed";

/** What the turn's harness says it is doing: working, waiting on what `waiting_on` says (a workflow run), left interrupted by a stop of alasio's, or ended. */
export type TurnStatus = "working" | "waiting" | "interrupted" | "ended";

/**
 * How a turn ended: completed, without its answer, interrupted by the operator, stopped
 * by alasio stopping under it (a restart, or a crash), or failed.
 */
export type TurnOutcome = "completed" | "incomplete" | "interrupted" | "stopped" | "failed";

/**
 * A row of `turns`: a conversation's turn with its harness, kept while it runs so
 * that a restart can resume it. A conversation has one row, its latest turn, and its
 * epoch: how many turns have claimed it, each turn's writes, and its harness's, made on
 * the condition that no turn has claimed it since (TurnFenced).
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
  readonly epoch: string;
  readonly status: TurnStatus;
  readonly waiting_on: string | null;
  readonly outcome: TurnOutcome | null;
  /** Its status message's outbox entry, once queued. */
  readonly status_outbox_id: string | null;
  /** The binding of the conversation's harness it runs on (setBindingAt), or none yet. */
  readonly binding: string | null;
  readonly started_at: Date;
  readonly completed_at: Date | null;
}

/** A turn starting, as recorded by claim; its thread is its conversation's. */
export interface ActiveTurn {
  readonly conversationId: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly sessionId?: string | null | undefined;
  /** The harness the turn runs on. */
  readonly harness: HarnessName;
  readonly pendingResponseId?: string | null | undefined;
  readonly prompt?: string | null | undefined;
  /**
   * The binding of the conversation's harness it runs on, as a pod's host knows its own, of a turn
   * its harness starts on its own; none, of one alasio claims, until alasio drives it on one (setBindingAt).
   */
  readonly binding?: string | null | undefined;
}

/**
 * The turns of each conversation. A write of the turn of an epoch is conditioned on the
 * row's epoch in the statement that makes it: an update of the row takes its lock, and
 * one of another table locks it for share (lockAt), so a claim committed while the write
 * waits on it is seen, and refuses the write.
 */
export class NeonTurnRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Claims the conversation for a turn starting, working: the turn's epoch, one more than the turn's before it. */
  claim(turn: ActiveTurn): Effect.Effect<number, StoreError> {
    return this.#sql.query<{ readonly epoch: string }>(
      `insert into ${this.#schema}.turns
         (id, conversation_id, thread_key, channel, thread_ts, session_id, harness, pending_response_id, prompt, binding, state, epoch, status, started_at)
       values ($1, $1, $1, $2, $3, $4, $5, $6, $7, $8, 'active', 1, 'working', now())
       on conflict (id) do update set
         channel = excluded.channel,
         thread_ts = excluded.thread_ts,
         session_id = excluded.session_id,
         harness = excluded.harness,
         pending_response_id = excluded.pending_response_id,
         prompt = excluded.prompt,
         state = 'active',
         epoch = ${this.#schema}.turns.epoch + 1,
         status = 'working',
         waiting_on = null,
         outcome = null,
         status_outbox_id = null,
         binding = excluded.binding,
         started_at = excluded.started_at,
         completed_at = null
       returning epoch`,
      [turn.conversationId, turn.chatId, turn.messageId, turn.sessionId ?? null, turn.harness, turn.pendingResponseId ?? null, turn.prompt ?? null, turn.binding ?? null],
    ).pipe(Effect.map(([row]) => Number(row?.epoch)));
  }

  /** Records the binding of the conversation's harness its turn of `epoch` runs on, while it runs: whether no turn has claimed it since. */
  setBindingAt(threadKey: string, epoch: number, binding: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.turns set binding = $3 where id = $1 and epoch = $2 and state = 'active' returning id`,
      [threadKey, epoch, binding],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Records the session the conversation's turn of `epoch` runs in, on the turn and as its
   * harness's session in the conversation: whether no turn has claimed it since (a turn
   * that ended records under its epoch until the next claim).
   */
  updateSessionIdAt(threadKey: string, epoch: number, sessionId: string | null): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `with turn as (
         update ${this.#schema}.turns set session_id = $3 where id = $1 and epoch = $2 returning harness
       )
       update ${this.#schema}.conversations set ${assignSession("(select harness from turn)", "$3")}, updated_at = now()
       where id = $1 and exists (select 1 from turn)
       returning id`,
      [threadKey, epoch, sessionId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Records the response the conversation's turn of `epoch` is writing: whether no turn has claimed it since. */
  updatePendingResponseIdAt(threadKey: string, epoch: number, pendingResponseId: string | null): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.turns set pending_response_id = $3 where id = $1 and epoch = $2 returning id`,
      [threadKey, epoch, pendingResponseId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Says what the conversation's active turn of `epoch` is doing: whether the turn still holds it. */
  setStatus(threadKey: string, epoch: number, status: TurnStatus, waitingOn: string | null = null): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.turns set status = $3, waiting_on = $4 where id = $1 and epoch = $2 and state = 'active' returning id`,
      [threadKey, epoch, status, waitingOn],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Says that the conversation's turn of `epoch` waits on `waitingOn`, or works again for
   * null, while it works: whether no turn has claimed the conversation since. A turn
   * ended, or left interrupted for a restart, says what it said.
   */
  setWaitingAt(threadKey: string, epoch: number, waitingOn: string | null): Effect.Effect<boolean, StoreError> {
    const working = `state = 'active' and status in ('working', 'waiting')`;
    return this.#sql.query(
      `update ${this.#schema}.turns set
         status = case when ${working} then (case when $3::text is null then 'working' else 'waiting' end) else status end,
         waiting_on = case when ${working} then $3 else waiting_on end
       where id = $1 and epoch = $2
       returning id`,
      [threadKey, epoch, waitingOn],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Records the status message the conversation's turn of `epoch` queued: whether no turn has claimed it since. */
  setStatusMessageAt(threadKey: string, epoch: number, outboxId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.turns set status_outbox_id = $3 where id = $1 and epoch = $2 returning id`,
      [threadKey, epoch, outboxId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Ends the conversation's active turn of `epoch`, as `outcome` says it ended: whether it was that turn, and active. */
  end(threadKey: string, epoch: number, outcome: TurnOutcome): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.turns set state = 'completed', status = 'ended', waiting_on = null, outcome = $3, completed_at = now()
       where id = $1 and epoch = $2 and state = 'active'
       returning id`,
      [threadKey, epoch, outcome],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Whether no turn has claimed the conversation since the turn of `epoch` did, its row
   * locked for share until the transaction asking ends, so no claim lands before what
   * the transaction writes on it.
   */
  lockAt(threadKey: string, epoch: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(`select 1 from ${this.#schema}.turns where id = $1 and epoch = $2 for share`, [threadKey, epoch]).pipe(
      Effect.map((rows) => rows.length > 0),
    );
  }

  getActiveTurns(): Effect.Effect<Turn[], StoreError> {
    return this.#sql.query<Turn>(`select * from ${this.#schema}.turns where state = 'active' order by started_at`);
  }

  /**
   * Holds each of the conversations busy for `reason`, as no turn (a fork), as `holder`'s,
   * until `ttlMs` from now unless renewed, where none is held, or its hold has expired: the
   * rows made, of those it could. Run in a transaction (hold), which keeps them only where
   * it made every one.
   */
  holdEach(conversationIds: readonly string[], reason: string, holder: string, ttlMs: number): Effect.Effect<number, StoreError> {
    return this.#sql.query<{ made: string }>(
      `with made as (
         insert into ${this.#schema}.conversation_holds as held (conversation_id, reason, holder, expires_at)
         select id, $2, $3, now() + $4 * interval '1 millisecond' from unnest($1::text[]) as wanted (id)
         on conflict (conversation_id) do update
           set reason = excluded.reason, holder = excluded.holder, held_at = now(), expires_at = excluded.expires_at
           where held.expires_at <= now()
         returning conversation_id
       )
       select count(*) as made from made`,
      [conversationIds, reason, holder, ttlMs],
    ).pipe(Effect.map(([row]) => Number(row?.made ?? 0)));
  }

  /** Keeps `holder`'s holds until `ttlMs` from now, those that have not expired: how many it still had. */
  renew(holder: string, ttlMs: number): Effect.Effect<number, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.conversation_holds set expires_at = now() + $2 * interval '1 millisecond' where holder = $1 and expires_at > now() returning conversation_id`,
      [holder, ttlMs],
    ).pipe(Effect.map((rows) => rows.length));
  }

  /** Whether the conversation is held busy, as no turn, by a hold that has not expired. */
  isHeld(conversationId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(`select 1 from ${this.#schema}.conversation_holds where conversation_id = $1 and expires_at > now()`, [conversationId]).pipe(
      Effect.map((rows) => rows.length > 0),
    );
  }

  /** Lets go of `holder`'s holds; of every hold, held by any, with `null`, as the alasio that held them is gone. */
  release(holder: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.conversation_holds where $1::text is null or holder = $1`, [holder]));
  }

  /** The conversation's latest turn, active or ended, or null when none has run. */
  getTurn(threadKey: string): Effect.Effect<Turn | null, StoreError> {
    return this.#sql.query<Turn>(`select * from ${this.#schema}.turns where id = $1`, [threadKey]).pipe(Effect.map(([row]) => row ?? null));
  }

  /** The conversation's turn, while it is active. */
  getActiveTurn(threadKey: string): Effect.Effect<Turn | null, StoreError> {
    return this.#sql.query<Turn>(`select * from ${this.#schema}.turns where id = $1 and state = 'active'`, [threadKey]).pipe(
      Effect.map(([row]) => row ?? null),
    );
  }
}
