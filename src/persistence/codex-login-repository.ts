import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

export class NeonCodexLoginRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** The text of Codex's auth.json as last kept, or null when none is. */
  getCodexLogin(): Effect.Effect<string | null, StoreError> {
    return this.#sql.query<{ auth: string }>(`select auth from ${this.#schema}.codex_login`).pipe(Effect.map(([row]) => row?.auth ?? null));
  }

  /** Keeps `auth` as Codex's login; null, Codex logged out, keeps none. */
  setCodexLogin(auth: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(auth === null
      ? this.#sql.query(`delete from ${this.#schema}.codex_login`)
      : this.#sql.query(
        `insert into ${this.#schema}.codex_login (auth) values ($1)
         on conflict (one) do update set auth = excluded.auth, updated_at = now()`,
        [auth],
      ));
  }
}
