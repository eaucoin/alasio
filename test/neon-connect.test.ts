import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { Array as Arr, ConfigProvider, Effect, Exit, Fiber, Layer, Logger } from "effect";
import { TestClock } from "effect/testing";
import pg from "pg";

import { Neon, type NeonError } from "../src/neon/connect.ts";
import { dockerAvailable, startPostgres, type TestPostgres } from "./support/postgres.ts";

const skip = !dockerAvailable() && "needs Docker for a throwaway Postgres";

/** Neon as alasio connects to it, with `env` for its environment and its log lines kept in `lines`. */
function connecting<A, E>(
  env: Record<string, string>,
  lines: string[],
  use: (neon: Neon["Service"]) => Effect.Effect<A, E>,
): Effect.Effect<A, E | NeonError> {
  const logger = Logger.make(({ message }) => {
    lines.push(Arr.ensure(message).join(" "));
  });
  return Effect.scoped(Effect.flatMap(Neon.make, use)).pipe(
    Effect.provide(Layer.mergeAll(ConfigProvider.layer(ConfigProvider.fromEnv({ env })), Logger.layer([logger]))),
  );
}

test("Neon's settings name what is missing", async () => {
  const exit = await Effect.runPromiseExit(connecting({ ALASIO_LAKE_PASSWORD: "p" }, [], () => Effect.void));
  assert.ok(Exit.isFailure(exit));
  const error = Exit.findErrorOption(exit);
  assert.equal(error._tag, "Some");
  assert.equal(error.value.message, "ALASIO_DATABASE_URL or ALASIO_DATABASE_URL_FILE must be set");
});

test("a Neon that does not answer is waited for ten minutes, saying why once a minute", async () => {
  const lines: string[] = [];
  const env = { ALASIO_DATABASE_URL: "postgresql://alasio:secret@127.0.0.1:1/alasio", ALASIO_LAKE_PASSWORD: "p" };
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

describe("Neon, answering", { skip }, () => {
  // Set by the first hook, which runs unless the suite is skipped.
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

  test("connects from the deployment's files, makes what alasio keeps, and ends its pool as its scope closes", async () => {
    assert.ok(database && secrets, "set up before the tests");
    const urlFile = join(secrets, "database-url");
    writeFileSync(urlFile, `${database.url}\n`);
    const lines: string[] = [];
    const env = { ALASIO_DATABASE_URL_FILE: urlFile, ALASIO_DATABASE_URL: "ignored for the file", ALASIO_LAKE_PASSWORD: "lake-password", ALASIO_LAKE_ENABLED: "1" };
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
        // The session-filesystem home's store, made when asked for, and readable by the lake.
        const sessionFs = yield* neon.sessionFsRollouts;
        assert.deepEqual(yield* Effect.promise(() => sessionFs.list()), []);
        assert.equal(yield* Effect.promise(() => lakeReads("codex_sessionfs_sessions.rollouts")), true);
        assert.equal(neon.pool.ended, false);
        return neon.pool;
      })
    ));
    assert.equal(pool.ended, true);
    assert.deepEqual(lines, ["connected to Neon"]);
  });

  test("with the lake off, revokes its reads", async () => {
    assert.ok(database);
    const env = { ALASIO_DATABASE_URL: database.url, ALASIO_LAKE_PASSWORD: "lake-password" };
    await Effect.runPromise(connecting(env, [], (neon) =>
      Effect.gen(function*() {
        assert.equal(neon.lake, false);
        assert.equal(yield* Effect.promise(() => lakeReads("codex_sessions.rollouts")), false);
      })
    ));
  });
});
