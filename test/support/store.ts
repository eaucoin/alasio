/**
 * alasio's store in tests: on the test file's one throwaway Postgres (./postgres.ts),
 * started as its first test needs it and stopped after its last, each store in a
 * schema of its own.
 */
import { after } from "node:test";

import { Effect } from "effect";
import pg from "pg";

import type { StoreError } from "../../src/persistence/sql.ts";
import { Store } from "../../src/persistence/store.ts";
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

/** A schema no store of the test file's has used. */
export function newSchema(): string {
  schemas += 1;
  return `alasio_test_${schemas}`;
}

/** The store in `schema` (a new one unless given), made empty where it was not made, a branch environment's when `branch` names one. */
export async function testStore({ schema = newSchema(), workingDirectory = null, branch = null }: {
  readonly schema?: string | undefined;
  readonly workingDirectory?: string | null | undefined;
  readonly branch?: string | null | undefined;
} = {}): Promise<Store["Service"]> {
  const { pool } = await database();
  return Effect.runPromise(Effect.provide(Store, Store.layer({ pool, schema, workingDirectory, branch })));
}

/** Runs what a test asks of a store: its value. */
export const run = <A>(effect: Effect.Effect<A, StoreError>): Promise<A> => Effect.runPromise(effect);
