/**
 * A chat's locks, which every alasio on the store takes (src/persistence/store.ts): its
 * handling lock waited for while another holds it, but not for ever, and the marks made
 * under it rolled back where it is lost; its delivery lock's taking cut short as alasio
 * stops, and the statements under each bounded in time.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import type { Update } from "@grammyjs/types";
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Option, type Scheduler } from "effect";
import pg from "pg";

import { poolDatabase } from "../src/persistence/sql.ts";
import { LOCKED_STATEMENT_TIMEOUT, Store } from "../src/persistence/store.ts";
import { holdChatLock, lockHolder, newSchema, testDatabaseUrl, testPool, testStore } from "./support/store.ts";

test("handling waits for the chat's handling lock another holds, and fails, its work not run, past the store's timeout", { timeout: 20_000 }, async () => {
  const schema = newSchema();
  const store = await testStore({ schema, handlingLockTimeout: "300 millis" });
  let handled = 0;
  const handle = store.withHandlingLock(42, () => Effect.sync(() => void (handled += 1)));
  const letGo = await holdChatLock(schema, "handling", 42);
  try {
    const started = Date.now();
    const timedOut = await Effect.runPromiseExit(handle);
    assert.ok(Exit.isFailure(timedOut));
    assert.match(String(Exit.isFailure(timedOut) && timedOut.cause), /lock timeout/u);
    assert.ok(Date.now() - started < 5000, "it gave up past the timeout, not as the holder let go");
    assert.equal(handled, 0);
    // Another chat's is its own.
    await Effect.runPromise(store.withHandlingLock(43, () => Effect.void));
  } finally {
    await letGo();
  }
  await Effect.runPromise(handle);
  assert.equal(handled, 1);
});

test("an update's marks made under a handling lock that is lost, its connection gone, are rolled back with it, so the update is handled again", { timeout: 20_000 }, async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const update: Update = { update_id: 7001, message: { message_id: 1, date: 0, chat: { id: 42, type: "private", first_name: "Operator" }, from: { id: 42, is_bot: false, first_name: "Operator" }, text: "hi" } };
  const marked = Effect.runSync(Deferred.make<void>());
  const killed = Effect.runSync(Deferred.make<void>());
  const handling = Effect.runFork(store.withHandlingLock(42, (marks) =>
    Effect.gen(function*() {
      yield* marks.recordTelegramUpdate(update);
      yield* marks.markTelegramUpdateProcessed(update.update_id);
      yield* Deferred.succeed(marked, undefined);
      // Still handling it as its lock's connection is lost.
      yield* Deferred.await(killed);
    })
  ));
  await Effect.runPromise(Deferred.await(marked));
  await (await testPool()).query("select pg_terminate_backend($1)", [await lockHolder(schema, "handling", 42)]);
  await Effect.runPromise(Deferred.succeed(killed, undefined));
  assert.ok(Exit.isFailure(await Effect.runPromise(Fiber.await(handling))));
  // Nothing of it kept: kept again, it is yet to be processed.
  assert.equal(await Effect.runPromise(store.recordTelegramUpdate(update)), true);
});

test("a delivery waiting for a connection of a pool that has none to give is stopped at once, not after the connection's timeout, and the connection that comes later is given back", { timeout: 30_000 }, async () => {
  const schema = newSchema();
  const pool = new pg.Pool({ connectionString: await testDatabaseUrl(), max: 1, connectionTimeoutMillis: 10_000 });
  try {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool, schema })));
    // The pool's one connection, held elsewhere.
    const held = await pool.connect();
    let ran = false;
    try {
      const waiting = Effect.runFork(store.withDeliveryLock(42, () => Effect.sync(() => void (ran = true))));
      await sleep(300);
      const started = Date.now();
      await Effect.runPromise(Fiber.interrupt(waiting));
      assert.ok(Date.now() - started < 2000, "stopped as it waited, not as the connection timed out");
    } finally {
      held.release();
    }
    assert.equal(ran, false);
    // The connection the stopped wait would have had is the pool's again.
    assert.deepEqual(await Effect.runPromise(store.withDeliveryLock(42, () => Effect.succeed("delivered")).pipe(Effect.timeout("5 seconds"))), Option.some("delivered"));
  } finally {
    await pool.end();
  }
});

test("a connection that comes as its transaction is stopped, the transaction stopped before it holds the connection, is given back once", async () => {
  let released = 0;
  const client = { query: async () => ({ rows: [] }), release: () => void (released += 1) } as unknown as pg.PoolClient;
  let come: (() => void) | undefined;
  // A pool whose connection comes when the test says.
  const pool = { connect: () => ({ then: (connected: (made: pg.PoolClient) => void) => void (come = () => connected(client)) }) } as unknown as pg.Pool;
  // A scheduler that, once held, has the transaction yield at its next step and keeps that step
  // for the test to run: the connection has come, and nothing yet holds it to give back.
  let held = false;
  const steps: Array<() => void> = [];
  const scheduler: Scheduler.Scheduler = {
    executionMode: "async",
    shouldYield: () => held,
    makeDispatcher: () => ({ scheduleTask: (step) => void (held ? steps.push(step) : queueMicrotask(step)), flush: () => {} }),
  };
  const transaction = Effect.runFork(poolDatabase(pool).guardedTransaction(() => Effect.succeed(true), () => Effect.void), { scheduler });
  while (!come) await sleep(5);
  held = true;
  come();
  assert.equal(steps.length, 1, "the transaction yields as its connection comes");
  held = false;
  transaction.interruptUnsafe();
  for (const step of steps.splice(0)) step();
  const stopped = await Effect.runPromise(Fiber.await(transaction));
  assert.ok(Exit.isFailure(stopped) && Cause.hasInterruptsOnly(stopped.cause), String(stopped));
  assert.equal(released, 1);
});

test("a delivery whose mark is stuck behind a lock no one lets go of ends as its lock's statements are bounded, rather than holding the delivery", { timeout: 30_000 }, async () => {
  const schema = newSchema();
  const other = await (await testPool()).connect();
  try {
    const store = await testStore({ schema, lockedStatementTimeout: "500 millis" });
    const id = await Effect.runPromise(store.enqueueOutboxText({ chatId: 42, text: "stuck" }));
    // The entry's row held by another, as a statement of a frozen compute would hold it.
    await other.query("begin");
    await other.query(`select 1 from ${schema}.telegram_outbox where id = $1 for update`, [id]);
    const started = Date.now();
    const marked = await Effect.runPromiseExit(store.withDeliveryLock(42, (marks) => marks.markOutboxSent(id)));
    assert.ok(Exit.isFailure(marked));
    assert.match(String(Exit.isFailure(marked) && marked.cause), /statement timeout/u);
    assert.ok(Date.now() - started < 5000, "ended by the statement timeout");
  } finally {
    await other.query("rollback").catch(() => {});
    other.release();
  }
});

test("a statement under a chat's lock is ended by Postgres well within the grace alasio's pod is stopped with", () => {
  assert.ok(Duration.isLessThan(LOCKED_STATEMENT_TIMEOUT, Duration.seconds(60)));
});
