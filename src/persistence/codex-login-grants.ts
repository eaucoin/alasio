/**
 * What the Codex login relay's role (src/neon/codex-login.ts) may do in alasio's state: keep
 * the environment's Codex login, as alasio does, and nothing more. It reads and writes the login
 * and the record of its refusal (./codex-login-repository.ts), under the store's lock of it, a
 * transaction's advisory lock, which no grant holds back (./store.ts withCodexLoginLock). Every
 * other table of the state, the conversations, the turns, Telegram's updates and the outbox among
 * them, it reaches none of.
 */
import { Effect } from "effect";

import { setupLock } from "./schema.ts";
import type { Sql, StoreError } from "./sql.ts";

/** The tables of the state the role reaches, each wholly, as the login is the environment's one. */
const LOGIN_TABLES = ["codex_login", "codex_login_refusal"] as const;

/** Grants `role` what the module says in `schema`'s state, under the setup's lock, its grants of before taken back first. */
export const grantCodexLogin = (sql: Sql, schema: string, role: string): Effect.Effect<void, StoreError> =>
  Effect.asVoid(sql.query([
    setupLock(schema),
    `grant usage on schema ${schema} to ${role}`,
    `revoke all on all tables in schema ${schema} from ${role}`,
    ...LOGIN_TABLES.map((table) => `grant select, insert, update, delete on ${schema}.${table} to ${role}`),
  ].join(";\n")));
