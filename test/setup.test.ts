/**
 * `alasio setup` (src/setup/program.ts): alasio's state made in its Neon as alasio makes it as it
 * starts, before alasio serves, so a cut-over may act between: made again, as alasio starts after,
 * it changes nothing. A test file of its own, as roles are a server's.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { ConfigProvider, Effect } from "effect";
import pg from "pg";

import { HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { setUpState } from "../src/setup/program.ts";
import { startPostgres } from "./support/postgres.ts";

test("alasio setup makes alasio's state, the harness host's grants and policies among it, as alasio would, and making it again changes nothing", { timeout: 60_000 }, async () => {
  const database = await startPostgres();
  const url = new URL(database.url);
  Object.assign(url, { username: "alasio", password: "alasio-password", pathname: "/alasio" });
  const admin = new pg.Client({ connectionString: database.url });
  await admin.connect();
  try {
    await admin.query("create role alasio login password 'alasio-password' createrole createdb");
    await admin.query("create database alasio owner alasio");
    const env = { ALASIO_DATABASE_URL: url.toString(), ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"), ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password"), ALASIO_CODEX_LOGIN_VERIFIER: scramVerifier("codex-login-password") };
    const setUp = Effect.scoped(setUpState({ workingDirectory: null, branch: null })).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))));
    await Effect.runPromise(setUp);
    const owner = new pg.Client({ connectionString: url.toString() });
    await owner.connect();
    try {
      /** What was made: the sessions' conversations, the policies on each table, row-level security on, and the harness host's reach of the stores. */
      const made = async () => ({
        registry: (await owner.query("select to_regclass('state.conversation_sessions') is not null as made")).rows[0]?.made,
        policies: (await owner.query("select tablename, policyname from pg_policies order by 1, 2")).rows.length,
        secured: (await owner.query("select relname from pg_class where relrowsecurity order by 1")).rows.map(({ relname }) => relname as string),
        reaches: (await owner.query("select has_table_privilege($1, 'claude_sessions.entries', 'insert') as reaches", [HARNESS_ROLE])).rows[0]?.reaches,
      });
      const first = await made();
      assert.equal(first.registry, true);
      assert.ok(first.secured.includes("turns") && first.secured.includes("entries"), first.secured.join(", "));
      assert.equal(first.policies, first.secured.length * 2);
      assert.equal(first.reaches, true);
      await Effect.runPromise(setUp);
      assert.deepEqual(await made(), first);
    } finally {
      await owner.end();
    }
  } finally {
    await admin.end();
    await database.stop();
  }
});
