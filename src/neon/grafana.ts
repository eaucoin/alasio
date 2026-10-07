/**
 * alasio's side of Grafana (cli/src/manifests/grafana.ts): its role and its database,
 * which Grafana keeps all it has in. They are made here, by alasio, as the lake's are
 * (./lake.ts), rather than in the compute's spec, whose roles Neon makes members of
 * neon_superuser.
 */
import pg, { type Pool } from "pg";

import { scramVerifier } from "../../neon/control/scram.ts";

/** The role Grafana connects as, and its database. */
export const GRAFANA_ROLE = "grafana";
const GRAFANA_DATABASE = "grafana";

/**
 * Makes Grafana's role, a login that is a member of no other role, with `password`
 * (stored as a SCRAM verifier), and its database, which only it may connect to, and in
 * whose public schema it makes its tables. The database is alasio's: alasio may not
 * act as the roles it makes, so it cannot give the database away, and grants Grafana
 * its schema instead, on a connection to the database, at the server `databaseUrl`
 * reaches alasio's on. Idempotent.
 */
export async function ensureGrafanaRole(pool: Pool, databaseUrl: string, password: string): Promise<void> {
  const role = await pool.query("select 1 from pg_roles where rolname = $1", [GRAFANA_ROLE]);
  if (role.rows.length === 0) await pool.query(`create role ${GRAFANA_ROLE} login`);
  await pool.query(`alter role ${GRAFANA_ROLE} with login password '${scramVerifier(password)}'`);
  const database = await pool.query("select 1 from pg_database where datname = $1", [GRAFANA_DATABASE]);
  if (database.rows.length === 0) await pool.query(`create database ${GRAFANA_DATABASE}`);
  await pool.query(`revoke all on database ${GRAFANA_DATABASE} from public`);
  await pool.query(`grant connect on database ${GRAFANA_DATABASE} to ${GRAFANA_ROLE}`);
  const url = new URL(databaseUrl);
  url.pathname = `/${GRAFANA_DATABASE}`;
  const client = new pg.Client({ connectionString: url.toString(), connectionTimeoutMillis: 30_000 });
  try {
    await client.connect();
    await client.query(`grant usage, create on schema public to ${GRAFANA_ROLE}`);
  } finally {
    await client.end().catch(() => {});
  }
}
