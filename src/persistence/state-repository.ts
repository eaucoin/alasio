import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

const TELEGRAM_OFFSET = "telegram_update_offset";

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

  setState(key: string, value: string | number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.bot_state (key, value) values ($1, $2)
       on conflict (key) do update set value = excluded.value, updated_at = now()`,
      [key, String(value)],
    ));
  }

  /** The key's value: `value` if the key had none, which it then keeps. */
  claimState(key: string, value: string): Effect.Effect<string, StoreError> {
    // The no-op update makes the conflicting row the one returned.
    return this.#sql.query<{ value: string }>(
      `insert into ${this.#schema}.bot_state (key, value) values ($1, $2)
       on conflict (key) do update set key = excluded.key
       returning value`,
      [key, value],
    ).pipe(Effect.map(([row]) => row!.value));
  }

  getTelegramOffset(): Effect.Effect<number | undefined, StoreError> {
    return this.getState(TELEGRAM_OFFSET).pipe(Effect.map((value) => (value ? Number(value) : undefined)));
  }

  setTelegramOffset(offset: number): Effect.Effect<void, StoreError> {
    return this.setState(TELEGRAM_OFFSET, offset);
  }
}
