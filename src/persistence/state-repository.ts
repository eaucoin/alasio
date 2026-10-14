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
}
