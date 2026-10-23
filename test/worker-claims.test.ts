/**
 * The claims of conversations' workers as an alasio holds them (src/codex/worker-claims.ts): what
 * runs under one found taken over as it is renewed interrupted, and the claims of a pod's
 * processes before it let go of as one starts in it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Cause, Deferred, Effect, Exit, Fiber, Option, Scope } from "effect";

import { makeWorkerClaims, WorkerClaimLost, type WorkerClaimsOptions } from "../src/codex/worker-claims.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { poolDatabase } from "../src/persistence/sql.ts";
import { Store } from "../src/persistence/store.ts";
import { NeonWorkerRepository } from "../src/persistence/worker-repository.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";

/** A store in a schema of its own, and conversations of it. */
async function conversations(count: number) {
  const schema = newSchema();
  const store = await testStore({ schema });
  const ids: string[] = [];
  for (let n = 0; n < count; n += 1) {
    const id = await run(store.upsertConversation({ chatId: String(70 + n), user: { id: 70 + n } }));
    await run(store.setActiveHarness(id, CODEX_HARNESS));
    ids.push(id);
  }
  return { store, schema, ids };
}

/** An alasio's claims over `store`, made as `options` says, until `scope` closes. */
const claimsOf = (store: Store["Service"], scope: Scope.Closeable, options: WorkerClaimsOptions) =>
  Effect.runPromise(makeWorkerClaims(options).pipe(Effect.provideService(Store, store), Scope.provide(scope)));

test("what runs under a claim another alasio took over is interrupted as a renewal finds it, failing with WorkerClaimLost", async () => {
  const { store, schema, ids: [conversationId = ""] } = await conversations(1);
  const scope = Effect.runSync(Scope.make());
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a", timings: { life: "2 seconds", renewal: "100 millis" } });
    const holding = Effect.runFork(claims.holding(conversationId, Effect.never, Effect.void));
    await sleep(250);
    assert.equal(holding.pollUnsafe(), undefined);
    // Taken over, as another alasio takes a claim that lapsed: one generation past this one's.
    await (await testPool()).query(`update ${schema}.conversation_workers set holder = 'pod-b/x', generation = generation + 1, held_until = now() + interval '1 minute'`);
    const ended = await Effect.runPromise(Fiber.await(holding).pipe(Effect.timeout("2 seconds")));
    assert.ok(Exit.isFailure(ended));
    assert.ok(Cause.squash(ended.cause) instanceof WorkerClaimLost, String(Cause.squash(ended.cause)));
    // The other's claim is left as it is.
    assert.equal(await run(store.claimWorker(conversationId, "pod-c/y", 60_000)), null);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("an alasio starting in a pod lets go of the claims its pod's processes before it held, which it then takes at once, and of no other pod's", async () => {
  const { store, ids: [own = "", others = ""] } = await conversations(2);
  // As a container that crashed in its pod left its claims, held as an alasio holds them, and another pod holds its own.
  const [before, scope] = [Effect.runSync(Scope.make()), Effect.runSync(Scope.make())];
  const holders: Fiber.Fiber<unknown, unknown>[] = [];
  try {
    for (const [pod, conversationId] of [["pod-a", own], ["pod-b", others]] as const) {
      const held = Effect.runSync(Deferred.make<void>());
      holders.push(Effect.runFork((await claimsOf(store, before, { pod })).holding(conversationId, Effect.andThen(Deferred.succeed(held, undefined), Effect.never), Effect.void)));
      await Effect.runPromise(Deferred.await(held));
    }
    const claims = await claimsOf(store, scope, { pod: "pod-a" });
    assert.equal(await Effect.runPromise(claims.lapseOwn), 1);
    assert.equal(Option.getOrNull(await Effect.runPromise(claims.holding(own, Effect.succeed("driven"), Effect.void))), "driven");
    assert.ok(Option.isNone(await Effect.runPromise(claims.holding(others, Effect.succeed("driven"), Effect.void))));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(Fiber.interruptAll(holders));
    await Effect.runPromise(Scope.close(before, Exit.void));
  }
});

test("a take of one conversation's claim that waits on its row, locked by a write under way, holds up no other conversation's, and is interrupted at once as alasio stops", { timeout: 30_000 }, async () => {
  const { store, schema, ids: [stuck = "", other = ""] } = await conversations(2);
  // Another alasio's claim, lapsed, its row read for share by a write of its under way.
  const fence = await run(store.claimWorker(stuck, "pod-b/stalled", 50));
  assert.ok(fence);
  await sleep(100);
  const open = Effect.runSync(Deferred.make<void>());
  const locked = Effect.runSync(Deferred.make<void>());
  const writing = Effect.runFork(poolDatabase(await testPool()).transaction((sql) =>
    Effect.andThen(new NeonWorkerRepository(sql, schema).holds(fence), Effect.andThen(Deferred.succeed(locked, undefined), Deferred.await(open)))
  ));
  await Effect.runPromise(Deferred.await(locked));
  const scope = Effect.runSync(Scope.make());
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a" });
    const waiting = Effect.runFork(claims.holding(stuck, Effect.succeed("driven"), Effect.void));
    await sleep(200);
    assert.equal(waiting.pollUnsafe(), undefined);
    // Another conversation's claim is taken meanwhile: not waited for past 3 s, which a take held
    // up behind the other's would never end within, its wait for the lock uninterruptible.
    const started = Date.now();
    const driven = Effect.runPromise(claims.holding(other, Effect.succeed("driven"), Effect.void));
    assert.equal(await Promise.race([driven.then(Option.getOrNull), sleep(3_000).then(() => "held up")]), "driven");
    assert.ok(Date.now() - started < 1_000, `another conversation's claim waited ${Date.now() - started} ms`);
    // Stopping, the take waiting is interrupted at once.
    const interrupting = Date.now();
    await Effect.runPromise(Fiber.interrupt(waiting).pipe(Effect.timeout("2 seconds")));
    assert.ok(Date.now() - interrupting < 1_000, `the take waited ${Date.now() - interrupting} ms to be interrupted`);
  } finally {
    await Effect.runPromise(Deferred.succeed(open, undefined));
    await Effect.runPromise(Fiber.join(writing));
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
