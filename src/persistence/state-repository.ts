import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

export class NeonStateRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  getState(key: string): Effect.Effect<string | null, StoreError> {
    return this.#sql.query<{ value: string }>(`select value from ${this.#schema}.bot_state where key = $1`, [key]).pipe(
      Effect.map(([row]) => row?.value ?? null),
    );
  }

  /**
   * Sets the record `key` to `value`, now, unless it was set within `withinMs`: whether it
   * did, in one statement, so of those that ask at once one alone does.
   */
  claimUnlessWithin(key: string, value: string, withinMs: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `insert into ${this.#schema}.bot_state (key, value) values ($1, $2)
       on conflict (key) do update set value = excluded.value, updated_at = now()
       where bot_state.updated_at <= now() - make_interval(secs => $3)
       returning key`,
      [key, value, withinMs / 1000],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }
}
