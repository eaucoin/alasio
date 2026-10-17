/**
 * A chat's handling lock, which every alasio on the store takes to handle the chat's
 * update (src/telegram/app.ts): waited for while another holds it, but not for ever; and
 * the marks made under it rolled back where it is lost.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { Update } from "@grammyjs/types";
import { Deferred, Effect, Exit, Fiber } from "effect";

import { holdChatLock, lockHolder, newSchema, testPool, testStore } from "./support/store.ts";

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
