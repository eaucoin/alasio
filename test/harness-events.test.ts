/**
 * A harness pool's pods' events as alasio hears them (src/harness/host-events.ts
 * HarnessEvents.fromLog), its handling added to the environment's one member of its events'
 * group (EventsConsumer): each heard once, however often it is delivered, its reply to deliver
 * and its letting go of the conversation passed on to the turns, none read before they listen;
 * what the sandbox gateway tells of its bindings let go of, released or lost, said in alasio's
 * log and passed on to the turns as their conversations' letting go.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Context, Effect, Exit, Layer, Option, PubSub, Scope } from "effect";

import { EnvironmentEvents, EventsConsumer } from "../src/events/consume.ts";
import { eventRecord, type EventInput } from "../src/events/events.ts";
import { HarnessEvents, type HostEvent } from "../src/harness/host-events.ts";
import { Store } from "../src/persistence/store.ts";
import { testStore } from "./support/store.ts";
import { memoryTopic } from "./support/topics.ts";

const ofConversation = (kind: "harness.background-response" | "harness.idle"): EventInput => ({ kind, environment: "main", source: "harness", subject: { kind: "conversation", key: "telegram:7" } });

test("a pool's pod's events are heard once each, however often delivered, and passed on to the turns; the gateway's, of bindings released or lost, free their conversations", async () => {
  const store = await testStore();
  const reply = eventRecord(ofConversation("harness.background-response"), 1, "00000000-0000-4000-8000-000000000001").value;
  const idle = eventRecord(ofConversation("harness.idle"), 2, "00000000-0000-4000-8000-000000000002").value;
  const released = eventRecord(
    { kind: "harness.released", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:7" }, binding: "binding-1", reason: "idle" },
    3,
    "00000000-0000-4000-8000-000000000003",
  ).value;
  const lost = eventRecord(
    { kind: "harness.lost", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:8" }, binding: "binding-2", pod: "harness-codex-1", reason: "its pod went while it was bound" },
    4,
    "00000000-0000-4000-8000-000000000004",
  ).value;
  const topic = memoryTopic("events.main", [reply, reply, idle, released, idle, lost]);
  const scope = Effect.runSync(Scope.make());
  try {
    // As alasio assembles it: its handling added, then the member started.
    const consumer = EventsConsumer.layer.pipe(Layer.provide(Layer.mergeAll(Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({ join: topic.join })), Layer.succeed(Store, store))));
    const built = await Effect.runPromise(Layer.buildWithScope(HarnessEvents.fromLog.pipe(Layer.provideMerge(consumer)), scope));
    await Effect.runPromise(Context.get(built, EventsConsumer).start.pipe(Scope.provide(scope)));
    // The turns listening a while after, as they are made after it: nothing told meanwhile is lost.
    await sleep(2_000);
    assert.equal(topic.committed(), 0, "read before the turns listened");
    const heard = await Effect.runPromise(Context.get(built, HarnessEvents).subscribe.pipe(Scope.provide(scope)));
    // Each heard once, in turn; nothing more of what was heard already.
    const next = (within: number) => Effect.runPromise(Effect.timeoutOption(PubSub.take(heard), within)).then(Option.getOrNull);
    const taken: (HostEvent | null)[] = [await next(15_000), await next(15_000), await next(15_000), await next(15_000), await next(500)];
    assert.deepEqual(taken, [
      { kind: "background-response", conversationId: "telegram:7" },
      { kind: "idle", conversationId: "telegram:7" },
      // Released, then lost: each conversation let go of, its waiting prompts run.
      { kind: "idle", conversationId: "telegram:7" },
      { kind: "idle", conversationId: "telegram:8" },
      null,
    ]);
    for (const deadline = Date.now() + 15_000; topic.committed() < 6; await sleep(20)) assert.ok(Date.now() < deadline, `${topic.committed()} of 6 told`);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
