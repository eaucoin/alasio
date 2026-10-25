/**
 * The claims of conversations' workers as an alasio holds them (src/codex/worker-claims.ts): what
 * runs under one found taken over as it is renewed interrupted, and the claims of a pod's
 * processes before it let go of as one starts in it; each ask timed, and each claim lost counted.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Cause, Deferred, Effect, Exit, Fiber, FiberSet, Option, Scope } from "effect";

import { added } from "./support/metrics.ts";
import { CLAIM_LOSSES, CLAIM_WAITS, makeWorkerClaims, WorkerClaimLost, type WorkerClaimsOptions } from "../src/codex/worker-claims.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { poolDatabase, StoreError } from "../src/persistence/sql.ts";
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

test("what runs under a claim another alasio took over is interrupted as a renewal finds it, failing with WorkerClaimLost, the claim counted lost", async () => {
  const { store, schema, ids: [conversationId = ""] } = await conversations(1);
  const scope = Effect.runSync(Scope.make());
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a", timings: { life: "2 seconds", renewal: "100 millis" } });
    const holding = Effect.runFork(claims.holding(conversationId, Effect.never, Effect.void));
    await sleep(250);
    assert.equal(holding.pollUnsafe(), undefined);
    // Taken over, as another alasio takes a claim that lapsed: one generation past this one's.
    let ended: Exit.Exit<unknown, unknown> | undefined;
    const lost = await added(CLAIM_LOSSES, async () => {
      await (await testPool()).query(`update ${schema}.conversation_workers set holder = 'pod-b/x', generation = generation + 1, held_until = now() + interval '1 minute'`);
      ended = await Effect.runPromise(Fiber.await(holding).pipe(Effect.timeout("2 seconds")));
    });
    assert.deepEqual(lost, { "k8s.pod.name=pod-a,alasio.worker_claim.outcome=lost": 1 });
    assert.ok(ended);
    assert.ok(Exit.isFailure(ended));
    assert.ok(Cause.squash(ended.cause) instanceof WorkerClaimLost, String(Cause.squash(ended.cause)));
    // The other's claim is left as it is.
    assert.equal(await run(store.claimWorker(conversationId, "pod-c/y", 60_000)), null);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("told to stop, an alasio interrupts what holds its claims and lets them go at once, its scope open still, and takes none from then on", { timeout: 30_000 }, async () => {
  const { store, ids: [driven = "", later = ""] } = await conversations(2);
  const scope = Effect.runSync(Scope.make());
  try {
    const told = Effect.runSync(Deferred.make<void>());
    const claims = await claimsOf(store, scope, { pod: "pod-a", told: Deferred.await(told) });
    const interrupted = { work: false };
    const holding = Effect.runFork(claims.holding(driven, Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void (interrupted.work = true)))), Effect.void));
    await sleep(200);
    // Held: no other alasio takes it.
    assert.equal(await run(store.claimWorker(driven, "pod-b/x", 60_000)), null);
    Effect.runSync(Deferred.succeed(told, undefined));
    const ended = await Effect.runPromise(Fiber.await(holding).pipe(Effect.timeout("2 seconds")));
    assert.ok(Exit.isFailure(ended) && Cause.hasInterruptsOnly(ended.cause), String(ended));
    assert.equal(interrupted.work, true);
    // Let go of: another alasio takes it at once.
    assert.notEqual(await run(store.claimWorker(driven, "pod-b/x", 60_000)), null);
    // And none taken from then on: nothing runs under one.
    const ran = { work: false };
    assert.deepEqual(await Effect.runPromise(claims.holding(later, Effect.sync(() => void (ran.work = true)), Effect.void)), Option.none());
    assert.equal(ran.work, false);
    assert.notEqual(await run(store.claimWorker(later, "pod-b/x", 60_000)), null);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("what joins a claim this alasio took, as a press's steer joins the claim the conversation's worker took as alasio started, runs only once what the last holder left is recovered, and fails where its recovery failed", { timeout: 30_000 }, async () => {
  const { store, ids: [recovered = "", failed = ""] } = await conversations(2);
  const scope = Effect.runSync(Scope.make());
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a" });
    const seen: string[] = [];
    // The worker takes the claim, its recovery of what the last holder left under way.
    const recovering = Effect.runSync(Deferred.make<void>());
    const worker = Effect.runFork(claims.holding(recovered, Effect.sync(() => void seen.push("worker's work")), Effect.andThen(Deferred.await(recovering), Effect.sync(() => void seen.push("recovered")))));
    await sleep(200);
    // A press joins it meanwhile, and a turn forked under it, as recovery forks one to attach to again.
    const press = Effect.runFork(claims.holding(recovered, Effect.sync(() => void seen.push("press's work")), Effect.die("a joiner recovers nothing")));
    const fibers = Effect.runSync(FiberSet.make<void>().pipe(Scope.provide(scope)));
    await Effect.runPromise(claims.forkHolding(recovered, Effect.sync(() => void seen.push("forked work")), fibers));
    await sleep(200);
    assert.equal(seen.length, 0, seen.join(", "));
    Effect.runSync(Deferred.succeed(recovering, undefined));
    await Effect.runPromise(Effect.all([Fiber.join(worker), Fiber.join(press)]));
    await Effect.runPromise(FiberSet.awaitEmpty(fibers));
    assert.equal(seen[0], "recovered");
    assert.deepEqual(seen.slice(1).sort(), ["forked work", "press's work", "worker's work"]);
    // A recovery that failed fails what joined its claim, which acts on nothing the last holder left.
    const failing = Effect.runSync(Deferred.make<void>());
    const failedWorker = Effect.runFork(claims.holding(failed, Effect.void, Effect.andThen(Deferred.await(failing), Effect.fail(new StoreError({ cause: new Error("the store answered nothing") })))));
    await sleep(200);
    let ranAfterFailure = false;
    const joined = Effect.runFork(claims.holding(failed, Effect.sync(() => void (ranAfterFailure = true)), Effect.void));
    await sleep(100);
    Effect.runSync(Deferred.succeed(failing, undefined));
    for (const fiber of [failedWorker, joined]) assert.ok(Exit.isFailure(await Effect.runPromise(Fiber.await(fiber))));
    assert.equal(ranAfterFailure, false);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a claim's recovery is its own: what took the claim, interrupted as the recovery runs, stops it not, and what joined the claim runs once it ends", { timeout: 30_000 }, async () => {
  const { store, ids: [conversationId = ""] } = await conversations(1);
  const scope = Effect.runSync(Scope.make());
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a" });
    const gate = Effect.runSync(Deferred.make<void>());
    let recovered = false;
    const taker = Effect.runFork(claims.holding(conversationId, Effect.void, Effect.andThen(Deferred.await(gate), Effect.sync(() => void (recovered = true)))));
    await sleep(200);
    let joined = false;
    const joiner = Effect.runFork(claims.holding(conversationId, Effect.sync(() => void (joined = true)), Effect.die("a joiner recovers nothing")));
    await sleep(100);
    await Effect.runPromise(Fiber.interrupt(taker));
    assert.equal(joined, false);
    Effect.runSync(Deferred.succeed(gate, undefined));
    assert.ok(Exit.isSuccess(await Effect.runPromise(Fiber.await(joiner))));
    assert.deepEqual([recovered, joined], [true, true]);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a claim's recovery holds the claim as it runs, past what took it, and lets it go once it ends with nothing else holding it", { timeout: 30_000 }, async () => {
  const { store, ids: [conversationId = ""] } = await conversations(1);
  const [scope, other] = [Effect.runSync(Scope.make()), Effect.runSync(Scope.make())];
  try {
    const claims = await claimsOf(store, scope, { pod: "pod-a" });
    const another = await claimsOf(store, other, { pod: "pod-b" });
    const gate = Effect.runSync(Deferred.make<void>());
    // What took it ends, interrupted, as its recovery runs on.
    const taking = Effect.runFork(claims.holding(conversationId, Effect.void, Deferred.await(gate)));
    await sleep(200);
    await Effect.runPromise(Fiber.interrupt(taking));
    assert.ok(Option.isNone(await Effect.runPromise(another.holding(conversationId, Effect.succeed("taken over"), Effect.void))), "another alasio took the claim mid-recovery");
    Effect.runSync(Deferred.succeed(gate, undefined));
    // Let go of once the recovery ended: another alasio takes it at once.
    let taken = Option.none<string>();
    for (let tries = 0; Option.isNone(taken) && tries < 20; tries += 1) {
      taken = await Effect.runPromise(another.holding(conversationId, Effect.succeed("taken"), Effect.void));
      if (Option.isNone(taken)) await sleep(50);
    }
    assert.equal(Option.getOrNull(taken), "taken");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await Effect.runPromise(Scope.close(other, Exit.void));
  }
});

test("a claim's recovery never outlives alasio's stop, ending before its claim is let go of, nor the claim once another alasio took it over", { timeout: 30_000 }, async () => {
  const { store, schema, ids: [stopping = "", lost = ""] } = await conversations(2);
  /** What came to pass as alasio stopped, in order: a recovery ending, a claim let go of. */
  const stopped: string[] = [];
  const recoveryOf = () => {
    const ended = { interrupted: false };
    return { ended, recover: Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void ((ended.interrupted = true), stopped.push("recovery ended"))))) };
  };
  // alasio stops: its scope closes, and the recovery with it, before its claim is let go of.
  const scope = Effect.runSync(Scope.make());
  const releasing: Store["Service"] = { ...store, releaseWorker: (fence) => Effect.andThen(Effect.sync(() => void stopped.push("claim let go of")), store.releaseWorker(fence)) };
  const claims = await claimsOf(releasing, scope, { pod: "pod-a", timings: { life: "2 seconds", renewal: "100 millis" } });
  const asStopping = recoveryOf();
  const held = Effect.runFork(claims.holding(stopping, Effect.void, asStopping.recover));
  const asLost = recoveryOf();
  const lostHeld = Effect.runFork(claims.holding(lost, Effect.void, asLost.recover));
  await sleep(300);
  // Taken over, as another alasio takes a claim that lapsed: the renewal finds it, and the recovery ends.
  await (await testPool()).query(`update ${schema}.conversation_workers set holder = 'pod-b/x', generation = generation + 1, held_until = now() + interval '1 minute' where conversation_id = $1`, [lost]);
  for (let waited = 0; !asLost.ended.interrupted && waited < 3_000; waited += 50) await sleep(50);
  assert.equal(asLost.ended.interrupted, true);
  assert.ok(Exit.isFailure(await Effect.runPromise(Fiber.await(lostHeld))));
  assert.equal(asStopping.ended.interrupted, false);
  await Effect.runPromise(Fiber.interrupt(held));
  stopped.length = 0;
  await Effect.runPromise(Scope.close(scope, Exit.void));
  assert.equal(asStopping.ended.interrupted, true);
  assert.deepEqual(stopped.slice(0, 2), ["recovery ended", "claim let go of"]);
});

test("an alasio starting in a pod lets go of the claims its pod's processes before it held, counted lapsed, which it then takes at once, and of no other pod's", async () => {
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
    const lapsed = await added(CLAIM_LOSSES, async () => assert.equal(await Effect.runPromise(claims.lapseOwn), 1));
    assert.deepEqual(lapsed, { "k8s.pod.name=pod-a,alasio.worker_claim.outcome=lapsed": 1 });
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

test("each ask for a conversation's claim is timed by how it was answered: taken where none held it, joined where this alasio did, held where another does", async () => {
  const { store, ids: [own = ""] } = await conversations(1);
  const scope = Effect.runSync(Scope.make());
  const holders: Fiber.Fiber<unknown, unknown>[] = [];
  try {
    const [claims, another] = [await claimsOf(store, scope, { pod: "pod-a" }), await claimsOf(store, scope, { pod: "pod-b" })];
    const asked = (pod: string, outcome: string) => `k8s.pod.name=${pod},alasio.worker_claim.outcome=${outcome}`;
    const timed = await added(CLAIM_WAITS, async () => {
      const held = Effect.runSync(Deferred.make<void>());
      holders.push(Effect.runFork(claims.holding(own, Effect.andThen(Deferred.succeed(held, undefined), Effect.never), Effect.void)));
      await Effect.runPromise(Deferred.await(held));
      assert.equal(Option.getOrNull(await Effect.runPromise(claims.holding(own, Effect.succeed("joined"), Effect.void))), "joined");
      assert.ok(Option.isNone(await Effect.runPromise(another.holding(own, Effect.succeed("held"), Effect.void))));
    });
    assert.deepEqual(timed, { [asked("pod-a", "taken")]: 1, [asked("pod-a", "joined")]: 1, [asked("pod-b", "held")]: 1 });
  } finally {
    await Effect.runPromise(Fiber.interruptAll(holders));
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
