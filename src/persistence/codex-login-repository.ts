import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/**
 * Codex's login as the store keeps it: its auth.json, when it was kept, and when its refresh
 * was refused, its refresh token spent or revoked, if it was (../codex/login-refresh.ts): in
 * milliseconds since the epoch, by the store's own clock.
 */
export interface CodexLoginRecord {
  readonly auth: string;
  readonly savedAt: number;
  readonly refusedAt: number | null;
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

  /** Codex's login as last kept, with when it was kept (ms since the epoch, the store's own clock) and its refusal, or null when none is. */
  getCodexLoginRecord(): Effect.Effect<CodexLoginRecord | null, StoreError> {
    return this.#sql.query<{ auth: string; saved_at: number; refused_at: number | null }>(
      `select login.auth, (extract(epoch from login.updated_at) * 1000)::float8 as saved_at,
              (extract(epoch from refusal.refused_at) * 1000)::float8 as refused_at
       from ${this.#schema}.codex_login login
       left join ${this.#schema}.codex_login_refusal refusal on refusal.saved_at = login.updated_at`,
    ).pipe(
      Effect.map(([row]) => (row ? { auth: row.auth, savedAt: Number(row.saved_at), refusedAt: row.refused_at === null ? null : Number(row.refused_at) } : null)),
    );
  }

  /** Keeps `auth` as Codex's login, refused by none yet, as its stamp is new: when it was kept, as getCodexLoginRecord says it. */
  saveCodexLogin(auth: string): Effect.Effect<number, StoreError> {
    return this.#sql.query<{ saved_at: number }>(
      `insert into ${this.#schema}.codex_login (auth) values ($1)
       on conflict (one) do update set auth = excluded.auth, updated_at = now()
       returning (extract(epoch from updated_at) * 1000)::float8 as saved_at`,
      [auth],
    ).pipe(Effect.map(([row]) => Number(row?.saved_at)));
  }

  /**
   * Records the login kept at `savedAt` as refused as it was refreshed: none, where another was
   * kept since. The refusals of logins kept before it, which say nothing now, go.
   */
  refuseCodexLogin(savedAt: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with gone as (
         delete from ${this.#schema}.codex_login_refusal
         where saved_at is distinct from (select updated_at from ${this.#schema}.codex_login)
       )
       insert into ${this.#schema}.codex_login_refusal (saved_at)
       select updated_at from ${this.#schema}.codex_login where (extract(epoch from updated_at) * 1000)::float8 = $1
       on conflict (saved_at) do nothing`,
      [savedAt],
    ));
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
