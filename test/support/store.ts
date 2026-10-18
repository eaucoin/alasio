/**
 * alasio's store in tests: on the test file's one throwaway Postgres (./postgres.ts),
 * started as its first test needs it and stopped after its last, each store in a
 * schema of its own.
 */
import { after } from "node:test";

import { type Duration, Effect } from "effect";
import pg from "pg";

import type { StoreError } from "../../src/persistence/sql.ts";
import { type ChatLock, chatLockName, Store, type TurnFenced, type TurnPersistence } from "../../src/persistence/store.ts";
import { CODEX_HARNESS } from "../../src/harness/names.ts";
import { startPostgres, type TestPostgres } from "./postgres.ts";

/** The test file's Postgres, and the pool its stores share. */
interface TestDatabase {
  readonly postgres: TestPostgres;
  readonly pool: pg.Pool;
}

let started: Promise<TestDatabase> | undefined;
let schemas = 0;

const database = (): Promise<TestDatabase> =>
  started ??= startPostgres().then((postgres) => ({ postgres, pool: new pg.Pool({ connectionString: postgres.url, max: 4 }) }));

after(async () => {
  if (!started) return;
  const { postgres, pool } = await started;
  await pool.end();
  await postgres.stop();
});

/** Where the test file's Postgres is, for a process of alasio's to reach. */
export async function testDatabaseUrl(): Promise<string> {
  return (await database()).postgres.url;
}

/** The pool the test file's stores share, for services a test makes its store in. */
export async function testPool(): Promise<pg.Pool> {
  return (await database()).pool;
}

/** The holds the store in `schema` keeps (conversation_holds), as [conversation, reason], in the order held. */
export async function holdsIn(schema: string): Promise<(readonly [string, string])[]> {
  const { rows } = await (await database()).pool.query<{ conversation_id: string; reason: string }>(`select conversation_id, reason from ${schema}.conversation_holds order by held_at`);
  return rows.map(({ conversation_id, reason }) => [conversation_id, reason] as const);
}

/** A schema no store of the test file's has used. */
export function newSchema(): string {
  schemas += 1;
  return `alasio_test_${schemas}`;
}

/**
 * The store in `schema` (a new one unless given), made empty where it was not made, a
 * branch environment's when `branch` names one, waiting for a chat's handling lock as long
 * as `handlingLockTimeout` says, and bounding a statement under a chat's lock as
 * `lockedStatementTimeout` does, where they say.
 */
export async function testStore({ schema = newSchema(), workingDirectory = null, branch = null, handlingLockTimeout, lockedStatementTimeout }: {
  readonly schema?: string | undefined;
  readonly workingDirectory?: string | null | undefined;
  readonly branch?: string | null | undefined;
  readonly handlingLockTimeout?: Duration.Input | undefined;
  readonly lockedStatementTimeout?: Duration.Input | undefined;
} = {}): Promise<Store["Service"]> {
  const { pool } = await database();
  return Effect.runPromise(Effect.provide(Store, Store.layer({ pool, schema, workingDirectory, branch, handlingLockTimeout, lockedStatementTimeout })));
}

/** The backend that holds the chat's `lock` of the store in `schema`, by its pid, for a test to end it as a lost connection is. */
export async function lockHolder(schema: string, lock: ChatLock, chatId: string | number): Promise<number> {
  // A bigint advisory lock is listed by its key's halves: classid the high, objid the low.
  const { rows: [row] } = await (await database()).pool.query<{ pid: number }>(
    `select pid from pg_locks, (select hashtextextended($1, 0) as key) as named
     where locktype = 'advisory' and granted and objsubid = 1
       and classid::bigint = ((named.key >> 32) & 4294967295) and objid::bigint = (named.key & 4294967295)`,
    [chatLockName(schema, lock, chatId)],
  );
  if (!row) throw new Error(`no backend holds ${chatLockName(schema, lock, chatId)}`);
  return row.pid;
}

/**
 * Holds the chat's `lock` of the store in `schema` as another alasio does, until what it
 * gives is called, which lets go of it, the first time.
 */
export async function holdChatLock(schema: string, lock: ChatLock, chatId: string | number): Promise<() => Promise<void>> {
  const other = await (await database()).pool.connect();
  await other.query("begin");
  await other.query("select pg_advisory_xact_lock(hashtextextended($1, 0))", [chatLockName(schema, lock, chatId)]);
  let held = true;
  return async () => {
    if (!held) return;
    held = false;
    try {
      await other.query("commit");
    } finally {
      other.release();
    }
  };
}

/** Runs what a test asks of a store, or of a turn's writes to it: its value. */
export const run = <A>(effect: Effect.Effect<A, StoreError | TurnFenced>): Promise<A> => Effect.runPromise(effect);

/**
 * What the conversation's turn writes, as a harness running in it would: under the epoch
 * of its active turn, or of one claimed now (on the chat `chatId`) when none is active.
 */
export async function writerOf(store: Store["Service"], conversationId: string, chatId = "123"): Promise<TurnPersistence> {
  const active = await run(store.getActiveTurn(conversationId));
  const epoch = active ? Number(active.epoch) : await run(store.claimTurn({ conversationId, chatId, messageId: "writer", harness: CODEX_HARNESS }));
  return store.turnPersistence(conversationId, epoch);
}
