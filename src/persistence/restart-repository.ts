import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** What restarted alasio under a turn: the turn itself, the operator, or something else. */
export type RestartCause = "self_induced" | "operator_induced" | "external_or_unknown";

/** A restart that cut a conversation's turn short, to record until the turn is resumed after it. */
export interface NewRestartEvent {
  readonly cause: RestartCause;
  readonly thread_key: string;
  readonly channel: string;
  readonly thread_ts: string;
  readonly session_id: string | null;
  /** The command that restarted alasio, for a self-induced restart. */
  readonly command?: string | undefined;
}

/** A row of `restart_events`: a restart recorded, and when. */
export interface RestartEvent extends Required<Omit<NewRestartEvent, "command">> {
  readonly command: string | null;
  readonly recorded_at: Date;
}

export class NeonRestartRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  recordRestartEvent({ cause, thread_key, channel, thread_ts, session_id, command }: NewRestartEvent): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.restart_events (thread_key, cause, channel, thread_ts, session_id, command) values ($1, $2, $3, $4, $5, $6)
       on conflict (thread_key) do update set
         cause = excluded.cause, channel = excluded.channel, thread_ts = excluded.thread_ts,
         session_id = excluded.session_id, command = excluded.command, recorded_at = excluded.recorded_at`,
      [thread_key, cause, channel, thread_ts, session_id, command ?? null],
    ));
  }

  /**
   * Records that the conversation's active turn was cut short by a restart alasio cannot
   * attribute, unless it has no active turn or a restart of it is already recorded.
   */
  recordExternalRestartEvent(threadKey: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.restart_events (thread_key, cause, channel, thread_ts, session_id)
       select thread_key, 'external_or_unknown', channel, thread_ts, session_id from ${this.#schema}.turns where id = $1 and state = 'active'
       on conflict (thread_key) do nothing`,
      [threadKey],
    ));
  }

  getRestartEvent(threadKey: string): Effect.Effect<RestartEvent | null, StoreError> {
    return this.#sql.query<RestartEvent>(`select * from ${this.#schema}.restart_events where thread_key = $1`, [threadKey]).pipe(
      Effect.map(([row]) => row ?? null),
    );
  }

  clearRestartEvent(threadKey: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.restart_events where thread_key = $1`, [threadKey]));
  }
}
