/**
 * The grants of the conversations' harness pool pods (../harness/grant.ts), as alasio keeps
 * them: one of each binding of a conversation's harness, by its digest, kept until the binding
 * is let go of (harness_grants, ./schema.ts), which the policies of the harness host's role
 * read (./harness-grants.ts).
 */
import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

export class NeonHarnessGrantRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Keeps `digest` as the grant of the conversation's binding `binding`; another binding's of it is kept until that one is let go of (revoke). */
  issue(conversationId: string, binding: string, digest: Buffer): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.harness_grants (conversation_id, binding, secret_sha256) values ($1, $2, $3)
       on conflict (conversation_id, binding) do update set secret_sha256 = excluded.secret_sha256, issued_at = now()
       where harness_grants.secret_sha256 is distinct from excluded.secret_sha256`,
      [conversationId, binding, digest],
    ));
  }

  /** Deletes the grant of the conversation's binding `binding`, as the binding is let go of; another binding's of it is kept. */
  revoke(conversationId: string, binding: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.harness_grants where conversation_id = $1 and binding = $2`, [conversationId, binding]));
  }
}
