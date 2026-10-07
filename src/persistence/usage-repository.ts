import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** Token usage a harness reports for a session's latest turn. */
export interface SessionUsage {
  readonly cacheReadInputTokens?: number | null | undefined;
}

export class NeonUsageRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  updateSessionUsage(sessionId: string | null | undefined, usage: SessionUsage | null | undefined): Effect.Effect<void, StoreError> {
    const tokens = usage?.cacheReadInputTokens ?? 0;
    if (!sessionId || tokens <= 0) {
      return Effect.void;
    }
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.session_usage (session_id, cache_read_input_tokens) values ($1, $2)
       on conflict (session_id) do update set cache_read_input_tokens = excluded.cache_read_input_tokens, updated_at = now()`,
      [sessionId, tokens],
    ));
  }

  getSessionTokens(sessionId: string): Effect.Effect<number, StoreError> {
    return this.#sql.query<{ cache_read_input_tokens: number }>(
      `select cache_read_input_tokens from ${this.#schema}.session_usage where session_id = $1`,
      [sessionId],
    ).pipe(Effect.map(([row]) => row?.cache_read_input_tokens ?? 0));
  }
}
