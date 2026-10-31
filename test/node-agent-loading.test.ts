/**
 * The restores loading on a node (src/node-agent/loading.ts), which the agent's background work
 * yields to: what waits until none loads, and what is paused while any does.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";

import { makeLoading } from "../src/node-agent/loading.ts";

/** Whether `fiber` has ended, the scheduler let run first. */
const ended = (fiber: Fiber.Fiber<unknown, unknown>) => Effect.yieldNow.pipe(Effect.andThen(Effect.sync(() => fiber.pollUnsafe() !== undefined)));

test("what waits for quiet waits while any restore loads, until each has ended or loaded for as long as it was given", () =>
  Effect.runPromise(
    Effect.gen(function*() {
      const loading = yield* makeLoading;
      yield* loading.quiet;
      yield* loading.begin("main/fs-a", 60_000);
      yield* loading.begin("main/fs-b", 10_000);
      const waiting = yield* Effect.forkChild(loading.quiet);
      assert.equal(yield* ended(waiting), false);
      yield* loading.end("main/fs-a");
      assert.equal(yield* ended(waiting), false);
      // The other was given 10 s.
      yield* TestClock.adjust("9 seconds");
      assert.equal(yield* ended(waiting), false);
      yield* TestClock.adjust("1 second");
      assert.equal(yield* ended(waiting), true);
      // One that ended does not end later what began again under its key.
      yield* loading.begin("main/fs-a", 60_000);
      yield* TestClock.adjust("59 seconds");
      const again = yield* Effect.forkChild(loading.quiet);
      assert.equal(yield* ended(again), false);
      yield* loading.end("main/fs-b");
      assert.equal(yield* ended(again), false);
      yield* loading.end("main/fs-a");
      assert.equal(yield* ended(again), true);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  ));

test("what pauses is told to as the first restore begins loading, at once where one does, and to go on once none does, until it stops listening", () =>
  Effect.runPromise(
    Effect.gen(function*() {
      const loading = yield* makeLoading;
      const told: boolean[] = [];
      const stop = loading.pausing((paused) => told.push(paused));
      assert.deepEqual(told, []);
      yield* loading.begin("main/fs-a", 60_000);
      yield* loading.begin("main/fs-b", 60_000);
      assert.deepEqual(told, [true]);
      yield* loading.end("main/fs-a");
      assert.deepEqual(told, [true]);
      yield* loading.end("main/fs-b");
      assert.deepEqual(told, [true, false]);
      // Ending one that does not load says nothing.
      yield* loading.end("main/fs-b");
      assert.deepEqual(told, [true, false]);
      yield* loading.begin("main/fs-a", 60_000);
      const late: boolean[] = [];
      loading.pausing((paused) => late.push(paused));
      assert.deepEqual(late, [true]);
      stop();
      yield* loading.end("main/fs-a");
      assert.deepEqual(told, [true, false, true]);
      assert.deepEqual(late, [true, false]);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  ));
