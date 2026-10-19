import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** What alasio keeps of the environment's events: which it handled (`events_handled`). */
export class NeonEventRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Records the event `id`, of `kind`, handled, unless it was already: whether it is to be handled now. */
  recordHandled(id: string, kind: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `insert into ${this.#schema}.events_handled (id, kind) values ($1, $2) on conflict (id) do nothing returning id`,
      [id, kind],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Forgets the events handled more than `ageMs` ago. */
  pruneHandled(ageMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.events_handled where handled_at < now() - make_interval(secs => $1)`, [ageMs / 1000]));
  }
}
