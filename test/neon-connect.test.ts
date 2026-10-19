import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { Array as Arr, ConfigProvider, Effect, Exit, Fiber, Layer, Logger } from "effect";
import { TestClock } from "effect/testing";
import pg from "pg";

import { scramVerifier } from "../neon/control/scram.ts";
import { lsnValue } from "../neon/lake/src/loader.ts";
import { Neon, type NeonError } from "../src/neon/connect.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { startPostgres, type TestPostgres } from "./support/postgres.ts";

/** The lake's verifier and the harness host's, as the deployment gives them. */
const LAKE_VERIFIER = scramVerifier("lake-password");
const HARNESS_VERIFIER = scramVerifier("harness-password");

/** Neon as alasio connects to it, with `env` for its environment, its log lines kept in `lines`, and `wake` told of its stores' writes. */
function connecting<A, E>(
  env: Record<string, string>,
  lines: string[],
  use: (neon: Neon["Service"]) => Effect.Effect<A, E>,
  wake: LakeWake["Service"] = LakeWake.none,
): Effect.Effect<A, E | NeonError> {
  const logger = Logger.make(({ message }) => {
    lines.push(Arr.ensure(message).join(" "));
  });
  return Effect.scoped(Effect.flatMap(Neon.make, use)).pipe(
    Effect.provideService(LakeWake, wake),
    Effect.provide(Layer.mergeAll(ConfigProvider.layer(ConfigProvider.fromEnv({ env })), Logger.layer([logger]))),
  );
}

test("Neon's settings name what is missing", async () => {
  const exit = await Effect.runPromiseExit(connecting({ ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER }, [], () => Effect.void));
  assert.ok(Exit.isFailure(exit));
  const error = Exit.findErrorOption(exit);
  assert.equal(error._tag, "Some");
  assert.equal(error.value.message, "ALASIO_DATABASE_URL or ALASIO_DATABASE_URL_FILE must be set");
  // A password where a role's verifier belongs is refused, never set as a role's password, as is
  // anything but a whole verifier, which alasio writes into the statement that sets it.
  const forged = `${LAKE_VERIFIER.slice(0, -1)}'; drop role alasio; --`;
  for (
    const env of [
      { ALASIO_LAKE_VERIFIER: "lake-password", ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER },
      { ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: "harness-password" },
      { ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER, ALASIO_GRAFANA_VERIFIER: "grafana-password" },
      { ALASIO_LAKE_VERIFIER: forged, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER },
    ]
  ) {
    const refused = Exit.findErrorOption(await Effect.runPromiseExit(connecting({ ALASIO_DATABASE_URL: "postgresql://alasio@127.0.0.1:1/alasio", ...env }, [], () => Effect.void)));
    assert.equal(refused._tag, "Some");
    assert.match(refused.value.message, /^ALASIO_(LAKE|HARNESS|GRAFANA)_VERIFIER must be a SCRAM-SHA-256 verifier, as Postgres stores one, not a password$/u);
  }
});

test("a Neon that does not answer is waited for ten minutes, saying why once a minute", async () => {
  const lines: string[] = [];
  const env = { ALASIO_DATABASE_URL: "postgresql://alasio:secret@127.0.0.1:1/alasio", ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER };
  const waited = await Effect.runPromise(Effect.gen(function*() {
    const connect = yield* Effect.forkChild(Effect.flip(connecting(env, lines, () => Effect.void)));
    let minutes = 0;
    while (connect.pollUnsafe() === undefined) {
      // Real time for an attempt to fail, then the five seconds it waits before the next.
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 20)));
      yield* TestClock.adjust("5 seconds");
      minutes += 5 / 60;
    }
    const error = yield* Fiber.join(connect);
    assert.equal(error._tag, "NeonUnavailable");
    assert.match(error.message, /ECONNREFUSED/);
    return minutes;
  }).pipe(Effect.provide(TestClock.layer())));
  assert.ok(waited >= 10 && waited < 11, `gave up after ${waited} minutes`);
  assert.ok(lines.length >= 10 && lines.length <= 11, `said so ${lines.length} times`);
  for (const line of lines) assert.match(line, /^waiting for Neon: connect ECONNREFUSED 127\.0\.0\.1:1$/);
});

describe("Neon, answering", () => {
  // Set by the first hook.
  let database: TestPostgres | undefined;
  let admin: pg.Pool;
  let secrets: string | undefined;

  before(async () => {
    database = await startPostgres();
    admin = new pg.Pool({ connectionString: database.url, max: 2 });
    secrets = mkdtempSync(join(tmpdir(), "alasio-neon-"));
  });

  after(async () => {
    await admin?.end();
    await database?.stop();
    if (secrets) rmSync(secrets, { recursive: true, force: true });
  });

  const tables = async () =>
    (await admin.query<{ name: string }>(
      `select table_schema || '.' || table_name as name from information_schema.tables
       where table_schema in ('claude_sessions', 'codex_sessions', 'codex_sessionfs_sessions') order by 1`,
    )).rows.map((row) => row.name);
  const lakeReads = async (table: string) =>
    (await admin.query<{ reads: boolean }>("select has_table_privilege('lake', $1, 'select') as reads", [table])).rows[0]?.reads;
  const harnessWrites = async (table: string) =>
    (await admin.query<{ writes: boolean }>("select has_table_privilege('harness', $1, 'select, insert, update, delete') as writes", [table])).rows[0]?.writes;

  test("connects from the deployment's files, makes what alasio keeps, the harness host's role granted its stores, and ends its pool as its scope closes", async () => {
    assert.ok(database && secrets, "set up before the tests");
    const urlFile = join(secrets, "database-url");
    writeFileSync(urlFile, `${database.url}\n`);
    const lines: string[] = [];
    const env = { ALASIO_DATABASE_URL_FILE: urlFile, ALASIO_DATABASE_URL: "ignored for the file", ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER, ALASIO_LAKE_ENABLED: "1" };
    const pool = await Effect.runPromise(connecting(env, lines, (neon) =>
      Effect.gen(function*() {
        assert.equal(neon.lake, true);
        assert.deepEqual(yield* Effect.promise(tables), [
          "claude_sessions.entries",
          "claude_sessions.summaries",
          "codex_sessions.rollout_chunks",
          "codex_sessions.rollouts",
        ]);
        assert.equal(yield* Effect.promise(() => lakeReads("codex_sessions.rollouts")), true);
        // The harness host reads and writes every table of the transcripts' and rollouts' stores.
        for (const table of yield* Effect.promise(tables)) assert.equal(yield* Effect.promise(() => harnessWrites(table)), true, table);
        // The session-filesystem home's store, made when asked for, readable by the lake and the harness host's.
        const sessionFs = yield* neon.sessionFsRollouts;
        assert.deepEqual(yield* Effect.promise(() => sessionFs.list()), []);
        assert.equal(yield* Effect.promise(() => lakeReads("codex_sessionfs_sessions.rollouts")), true);
        assert.equal(yield* Effect.promise(() => harnessWrites("codex_sessionfs_sessions.rollouts")), true);
        assert.equal(neon.pool.ended, false);
        return neon.pool;
      })
    ));
    assert.equal(pool.ended, true);
    assert.deepEqual(lines, ["connected to Neon"]);
  });

  test("tells the lake's loader of each store's writes, with the primary's WAL position as its record is written, past the writes", async () => {
    assert.ok(database);
    const env = { ALASIO_DATABASE_URL: database.url, ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER, ALASIO_LAKE_ENABLED: "1" };
    const told: (() => Promise<string>)[] = [];
    const wake = LakeWake.of({ written: (lsn) => void told.push(lsn) });
    await Effect.runPromise(connecting(env, [], (neon) =>
      Effect.promise(async () => {
        await neon.sessionStore.append({ projectKey: "-home-operator-wake", sessionId: "44444444-4444-4444-8444-444444444444" }, [{ type: "user", uuid: "w1", message: { role: "user", content: "told" } }]);
        assert.equal(told.length, 1);
        const after = (await admin.query<{ lsn: string }>("select pg_current_wal_lsn()::text as lsn")).rows[0]?.lsn ?? "";
        assert.ok(lsnValue(await told[0]!()) >= lsnValue(after), "the position read is before the write it tells of");
      }), wake));
  });

  test("with the lake off, revokes its reads", async () => {
    assert.ok(database);
    const env = { ALASIO_DATABASE_URL: database.url, ALASIO_LAKE_VERIFIER: LAKE_VERIFIER, ALASIO_HARNESS_VERIFIER: HARNESS_VERIFIER };
    await Effect.runPromise(connecting(env, [], (neon) =>
      Effect.gen(function*() {
        assert.equal(neon.lake, false);
        assert.equal(yield* Effect.promise(() => lakeReads("codex_sessions.rollouts")), false);
      })
    ));
  });
});
