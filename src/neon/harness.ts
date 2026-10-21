/**
 * The harness host's role (`alasio harness`, ../harness/main.ts), which writes a turn's part
 * of alasio's state and nothing else, as the deployment's harness pods reach Neon.
 *
 * Made here, by alasio, as the lake's role is (./lake.ts), rather than in the compute's
 * spec, which makes every role a member of neon_superuser. Its grants, in alasio's state and
 * in the stores of Claude Code's transcripts and Codex's rollouts, and the policies that hold
 * each pod to its own conversation's rows, are the store's (../persistence/harness-grants.ts),
 * made once its tables are.
 */
import { escapeIdentifier, escapeLiteral, type Pool } from "pg";

import { HARNESS_ROLE } from "../../neon/control/secrets.ts";

/**
 * Makes the harness host's role, a login that is a member of no other role, its password
 * stored as `verifier`, the one the deployment gives (neon/control/scram.ts), which may
 * connect to alasio's database, the pool's. Idempotent.
 */
export async function ensureHarnessRole(pool: Pool, verifier: string): Promise<void> {
  const role = await pool.query("select 1 from pg_roles where rolname = $1", [HARNESS_ROLE]);
  if (role.rows.length === 0) await pool.query(`create role ${HARNESS_ROLE} login`);
  await pool.query(`alter role ${HARNESS_ROLE} with login password ${escapeLiteral(verifier)}`);
  const { rows: [database] } = await pool.query<{ name: string }>("select current_database() as name");
  await pool.query(`grant connect on database ${escapeIdentifier(database?.name ?? "")} to ${HARNESS_ROLE}`);
}
