// @ts-nocheck
/**
 * alasio's side of the analytics lake (neon/lake/src/model.ts): whether it runs, its role
 * and catalog database, and what it may read.
 *
 * The role is made here, by alasio, rather than in the compute's spec: Neon makes every
 * role in the spec a member of neon_superuser, which may read and write every table,
 * where the lake's should read what it loads and nothing else. The role, its password,
 * and its database exist whether or not the lake runs, so turning the lake on or off
 * restarts nothing but the lake; it may read alasio's data only while it runs.
 */
import { scramVerifier } from "../../neon/control/scram.ts";

/** Whether the lake runs: `ALASIO_LAKE_ENABLED=1`. Off unless set. */
export function lakeEnabled(env = process.env) {
  return env.ALASIO_LAKE_ENABLED?.trim() === "1";
}

/** The role the lake connects as, and the database its catalog is kept in. */
export const LAKE_ROLE = "lake";
export const LAKE_DATABASE = "lake";

/** What the lake reads: the schemas and tables it loads from, as alasio's stores make them. */
export const LAKE_SOURCES = {
  claude_sessions: ["entries"],
  codex_sessions: ["rollouts", "rollout_chunks"],
  codex_sessionfs_sessions: ["rollouts", "rollout_chunks"],
};

/**
 * Makes the lake's role, a login that is a member of no other role, with `password`
 * (stored as a SCRAM verifier, never in the clear), and its catalog's database, which
 * only the lake may connect to, make its catalog's schema in, and stage DuckLake's
 * updates in temporary tables in. Idempotent.
 */
export async function ensureLakeRole(pool, password) {
  const role = await pool.query("select 1 from pg_roles where rolname = $1", [LAKE_ROLE]);
  if (role.rows.length === 0) await pool.query(`create role ${LAKE_ROLE} login`);
  await pool.query(`alter role ${LAKE_ROLE} with login password '${scramVerifier(password)}'`);
  const database = await pool.query("select 1 from pg_database where datname = $1", [LAKE_DATABASE]);
  if (database.rows.length === 0) await pool.query(`create database ${LAKE_DATABASE}`);
  await pool.query(`revoke all on database ${LAKE_DATABASE} from public`);
  await pool.query(`grant connect, create, temporary on database ${LAKE_DATABASE} to ${LAKE_ROLE}`);
}

/**
 * Grants the lake's role read access to every source table that exists, or with
 * `enabled` false revokes it. Idempotent; a store made later (the session-filesystem
 * Codex's) is covered by calling it again once that store exists.
 */
export async function syncLakeReads(pool, enabled) {
  const { rows } = await pool.query(
    `select table_schema, table_name from information_schema.tables
     where (table_schema, table_name) in (select * from unnest($1::text[], $2::text[]))`,
    [
      Object.entries(LAKE_SOURCES).flatMap(([schema, tables]) => tables.map(() => schema)),
      Object.values(LAKE_SOURCES).flat(),
    ],
  );
  const schemas = [...new Set(rows.map((row) => row.table_schema))];
  const tables = rows.map((row) => `${row.table_schema}.${row.table_name}`);
  if (tables.length === 0) return;
  if (enabled) {
    await pool.query(`grant usage on schema ${schemas.join(", ")} to ${LAKE_ROLE}`);
    await pool.query(`grant select on ${tables.join(", ")} to ${LAKE_ROLE}`);
  } else {
    await pool.query(`revoke select on ${tables.join(", ")} from ${LAKE_ROLE}`);
    await pool.query(`revoke usage on schema ${schemas.join(", ")} from ${LAKE_ROLE}`);
  }
}
