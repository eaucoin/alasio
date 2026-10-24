/**
 * The Codex login relay's role (`alasio codex-login`, ../codex/relay.ts), which reads and
 * writes the environment's Codex login and nothing else, as the relay reaches Neon.
 *
 * Made here, by alasio, as the harness host's is (./harness.ts), rather than in the compute's
 * spec, which makes every role a member of neon_superuser. Its grants, the login's tables of
 * alasio's state alone, are the store's (../persistence/codex-login-grants.ts), made once its
 * tables are.
 */
import { escapeIdentifier, escapeLiteral, type Pool } from "pg";

import { CODEX_LOGIN_ROLE } from "../../neon/control/secrets.ts";

/**
 * Makes the relay's role, a login that is a member of no other role, its password stored as
 * `verifier`, the one the deployment gives (neon/control/scram.ts), which may connect to
 * alasio's database, the pool's. Idempotent.
 */
export async function ensureCodexLoginRole(pool: Pool, verifier: string): Promise<void> {
  const role = await pool.query("select 1 from pg_roles where rolname = $1", [CODEX_LOGIN_ROLE]);
  if (role.rows.length === 0) await pool.query(`create role ${CODEX_LOGIN_ROLE} login`);
  await pool.query(`alter role ${CODEX_LOGIN_ROLE} with login password ${escapeLiteral(verifier)}`);
  const { rows: [database] } = await pool.query<{ name: string }>("select current_database() as name");
  await pool.query(`grant connect on database ${escapeIdentifier(database?.name ?? "")} to ${CODEX_LOGIN_ROLE}`);
}
