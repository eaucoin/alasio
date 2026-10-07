/**
 * The roles alasio makes in its Neon for what reads it beside alasio (src/neon/lake.ts,
 * src/neon/grafana.ts), made as Neon's alasio makes them: as a role that may make roles
 * and databases, and is no superuser. A test file of its own, as roles are a server's:
 * here alasio makes every one of them, as it does in its Neon.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { ConfigProvider, Effect } from "effect";
import pg from "pg";

import { Neon } from "../src/neon/connect.ts";
import { ensureGrafanaRole } from "../src/neon/grafana.ts";
import { startPostgres } from "./support/postgres.ts";

/** Runs `statements` on a connection to `url`: each one's rows. */
async function run(url: string, ...statements: string[]): Promise<Record<string, unknown>[][]> {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    const results: Record<string, unknown>[][] = [];
    for (const statement of statements) results.push((await client.query(statement)).rows);
    return results;
  } finally {
    await client.end();
  }
}

test("alasio makes the lake reader's role and Grafana's, with Grafana's database, when given their passwords, and each may do what it is for alone", async () => {
  const database = await startPostgres();
  /** `database`'s server, as `user`, in the database `name`. */
  const as = (user: string, password: string, name: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: `/${name}` });
    return url.toString();
  };
  try {
    await run(database.url, "create role alasio login password 'alasio-password' createrole createdb", "create database alasio owner alasio");
    const env = {
      ALASIO_DATABASE_URL: as("alasio", "alasio-password", "alasio"),
      ALASIO_LAKE_PASSWORD: "lake-password",
      ALASIO_LAKE_READER_PASSWORD: "reader-password",
      ALASIO_GRAFANA_PASSWORD: "grafana-password",
    };
    // Grafana may not log in until all it needs is made: here its schema cannot be granted, the database's server not reached.
    const alasio = new pg.Pool({ connectionString: env.ALASIO_DATABASE_URL });
    try {
      const unreached = new URL(env.ALASIO_DATABASE_URL);
      unreached.port = "1";
      await assert.rejects(ensureGrafanaRole(alasio, unreached.toString(), "grafana-password"), /ECONNREFUSED/u);
    } finally {
      await alasio.end();
    }
    await assert.rejects(run(as("grafana", "grafana-password", "grafana")), /password authentication failed for user "grafana"/u);
    // Twice, as alasio starts again: the second changes nothing.
    for (const _ of [1, 2]) await Effect.runPromise(Effect.scoped(Effect.asVoid(Neon.make)).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })))));

    // Grafana makes its tables in its database, and takes the session lock its migrations take.
    const [, lock] = await run(as("grafana", "grafana-password", "grafana"), "create table migration_log (id int)", "select pg_try_advisory_lock(4004004031) as held");
    assert.deepEqual(lock, [{ held: true }]);
    await assert.rejects(run(as("lake_reader", "reader-password", "lake"), "create table written (id int)"), /permission denied/);
    const [roles, privileges] = await run(
      database.url,
      "select r.rolname from pg_auth_members m join pg_roles r on r.oid = m.member where r.rolname in ('grafana', 'lake_reader')",
      `select has_database_privilege('public', 'grafana', 'connect') as public_connects,
        has_database_privilege('lake_reader', 'grafana', 'connect') as reader_connects,
        has_database_privilege('lake_reader', 'lake', 'connect') as reader_reads_lake`,
    );
    assert.deepEqual(roles, [], "neither is a member of another role");
    assert.deepEqual(privileges, [{ public_connects: false, reader_connects: false, reader_reads_lake: true }]);
  } finally {
    await database.stop();
  }
});
