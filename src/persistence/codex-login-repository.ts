import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** Codex's login as the store keeps it: its auth.json, and when it was kept, in milliseconds since the epoch, by the store's own clock. */
export interface CodexLoginRecord {
  readonly auth: string;
  readonly savedAt: number;
}

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

  /** Codex's login as last kept, with when it was kept (ms since the epoch, the store's own clock), or null when none is. */
  getCodexLoginRecord(): Effect.Effect<CodexLoginRecord | null, StoreError> {
    return this.#sql.query<{ auth: string; saved_at: number }>(`select auth, (extract(epoch from updated_at) * 1000)::float8 as saved_at from ${this.#schema}.codex_login`).pipe(
      Effect.map(([row]) => (row ? { auth: row.auth, savedAt: Number(row.saved_at) } : null)),
    );
  }

  /** Keeps `auth` as Codex's login: when it was kept, as getCodexLoginRecord says it. */
  saveCodexLogin(auth: string): Effect.Effect<number, StoreError> {
    return this.#sql.query<{ saved_at: number }>(
      `insert into ${this.#schema}.codex_login (auth) values ($1)
       on conflict (one) do update set auth = excluded.auth, updated_at = now()
       returning (extract(epoch from updated_at) * 1000)::float8 as saved_at`,
      [auth],
    ).pipe(Effect.map(([row]) => Number(row?.saved_at)));
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
