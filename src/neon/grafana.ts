/**
 * alasio's side of Grafana (cli/src/manifests/grafana.ts): its role and its database,
 * which Grafana keeps all it has in. They are made here, by alasio, as the lake's are
 * (./lake.ts), rather than in the compute's spec, whose roles Neon makes members of
 * neon_superuser.
 */
import pg, { escapeLiteral, type Pool } from "pg";

import { GRAFANA_ROLE } from "../../neon/control/secrets.ts";

/** Grafana's database. */
const GRAFANA_DATABASE = "grafana";

/**
 * Makes Grafana's role, a login that is a member of no other role, its password stored
 * as `verifier`, the one the deployment gives (neon/control/scram.ts), and its
 * database, which only it may connect to, and in whose public schema it makes its
 * tables. The database is alasio's: alasio may not act as the roles it makes, so it
 * cannot give the database away, and grants Grafana its schema instead, on a connection
 * to the database, at the server `databaseUrl` reaches alasio's on. Grafana may log in
 * last, once all of that is made: so it can log in once its database is ready for it,
 * which its pod waits for (cli/src/manifests/grafana.ts). Idempotent.
 */
export async function ensureGrafanaRole(pool: Pool, databaseUrl: string, verifier: string): Promise<void> {
  const role = await pool.query("select 1 from pg_roles where rolname = $1", [GRAFANA_ROLE]);
  if (role.rows.length === 0) await pool.query(`create role ${GRAFANA_ROLE} nologin`);
  const database = await pool.query("select 1 from pg_database where datname = $1", [GRAFANA_DATABASE]);
  if (database.rows.length === 0) await pool.query(`create database ${GRAFANA_DATABASE}`);
  await pool.query(`revoke all on database ${GRAFANA_DATABASE} from public`);
  await pool.query(`grant connect on database ${GRAFANA_DATABASE} to ${GRAFANA_ROLE}`);
  const url = new URL(databaseUrl);
  url.pathname = `/${GRAFANA_DATABASE}`;
  // As patient as the pool's own connections.
  const client = new pg.Client({ connectionString: url.toString(), connectionTimeoutMillis: pool.options.connectionTimeoutMillis });
  // A connection lost fails the grant, and so the setup, not the process.
  client.on("error", () => {});
  try {
    await client.connect();
    await client.query(`grant usage, create on schema public to ${GRAFANA_ROLE}`);
  } finally {
    await client.end().catch(() => {});
  }
  await pool.query(`alter role ${GRAFANA_ROLE} with login password ${escapeLiteral(verifier)}`);
}
