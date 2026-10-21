/**
 * A harness pool's pods' events as alasio hears them (src/harness/host-events.ts
 * HarnessEvents.fromLog), its handling added to the environment's one member of its events'
 * group (EventsConsumer): each heard once, however often it is delivered, its reply to deliver
 * and its letting go of the conversation passed on to the turns, none read before they listen;
 * what the sandbox gateway tells of its bindings let go of, released or lost, said in alasio's
 * log, the binding's grant deleted, and passed on to the turns as their conversations' letting go.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { createServer } from "node:http";
import type { AddressInfo } from "node:net";

import { Array as Arr, Context, Effect, Exit, Fiber, Layer, Logger, Option, PubSub, Scope } from "effect";
import { TestClock } from "effect/testing";

import { EnvironmentEvents, EventsConsumer } from "../src/events/consume.ts";
import { eventRecord, type EventInput } from "../src/events/events.ts";
import { grantDigest } from "../src/harness/grant.ts";
import { HarnessEvents, type HostEvent, HostEvents } from "../src/harness/host-events.ts";
import { Store } from "../src/persistence/store.ts";
import { newSchema, testPool, testStore } from "./support/store.ts";
import { memoryTopic } from "./support/topics.ts";

const ofConversation = (kind: "harness.background-response" | "harness.idle"): EventInput => ({ kind, environment: "main", source: "harness", subject: { kind: "conversation", key: "telegram:7" } });

test("a pool's pod's events are heard once each, however often delivered, and passed on to the turns; the gateway's, of bindings released or lost, free their conversations and end their grants; one of a kind told by what tells of none of it is passed over", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  // The grants of the bindings let go of, as alasio kept them as it bound them.
  for (const [chatId, binding] of [["7", "binding-1"], ["8", "binding-2"], ["9", "binding-9"]] as const) {
    const conversationId = await Effect.runPromise(store.upsertConversation({ chatId, user: { id: Number(chatId) } }));
    await Effect.runPromise(store.issueHarnessGrant(conversationId, binding, grantDigest(`grant-${binding}`)));
  }
  const grants = async () => (await (await testPool()).query(`select conversation_id, binding from ${schema}.harness_grants order by 1`)).rows;
  const forged = { conversation_id: "telegram:9", binding: "binding-9" };
  assert.deepEqual(await grants(), [{ conversation_id: "telegram:7", binding: "binding-1" }, { conversation_id: "telegram:8", binding: "binding-2" }, forged]);
  // A host's kind told as the gateway's, and a binding's release told as a host's: what tells of neither.
  const forgedIdle = eventRecord({ kind: "harness.idle", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:9" } }, 0, "00000000-0000-4000-8000-000000000008").value;
  const forgedRelease = eventRecord(
    { kind: "harness.released", environment: "main", source: "harness", subject: { kind: "conversation", key: "telegram:9" }, binding: "binding-9", reason: "idle" },
    0,
    "00000000-0000-4000-8000-000000000009",
  ).value;
  /** How often the lake's loader was told of what the hosts committed. */
  let wakes = 0;
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
  const topic = memoryTopic("events.main", [forgedIdle, forgedRelease, reply, reply, idle, released, idle, lost]);
  const scope = Effect.runSync(Scope.make());
  try {
    // As alasio assembles it: its handling added, then the member started.
    const consumer = EventsConsumer.layer.pipe(Layer.provide(Layer.mergeAll(Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({ join: topic.join })), Layer.succeed(Store, store))));
    const built = await Effect.runPromise(Layer.buildWithScope(HarnessEvents.fromLog({ written: () => void (wakes += 1) }).pipe(Layer.provideMerge(consumer)), scope));
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
    for (const deadline = Date.now() + 15_000; topic.committed() < 8; await sleep(20)) assert.ok(Date.now() < deadline, `${topic.committed()} of 8 told`);
    assert.deepEqual(await grants(), [forged]);
    // The lake's loader told of what the hosts committed as each was heard.
    assert.equal(wakes, 4);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

/** A stand-in for the gateway's front at the loopback, answering each telling as `answer` says, the tellings it was asked recorded. */
async function frontTold(answer: (told: number) => number): Promise<{ readonly url: string; readonly told: string[]; readonly close: () => void }> {
  const told: string[] = [];
  const server = createServer((incoming, outgoing) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => {
      told.push(Buffer.concat(chunks).toString("utf8"));
      outgoing.writeHead(answer(told.length)).end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, told, close: () => server.close() };
}

test("a pod tells the front again while it answers 503, as its watch catches up, until it takes it; and gives up past its patience, saying so", async () => {
  /** `tell`, run on a clock of the test's, which it moves on a second at a time, while what it waits on answers: what it logged. */
  const telling = (url: string) =>
    Effect.gen(function*() {
      const { tell } = yield* HostEvents;
      const fiber = yield* Effect.forkChild(tell({ kind: "idle", conversationId: "telegram:7" }));
      for (let second = 0; second < 90 && fiber.pollUnsafe() === undefined; second += 1) {
        yield* Effect.promise(() => sleep(10));
        yield* TestClock.adjust("1 second");
      }
      yield* Fiber.join(fiber);
    }).pipe(Effect.provide(Layer.mergeAll(HostEvents.throughFront({ url, token: "binding-token" }), TestClock.layer())));
  const logged = (lines: string[]) => Logger.layer([Logger.make(({ message }) => void lines.push(Arr.ensure(message).join(" ")))]);
  // Taken at its third telling.
  const takes = await frontTold((told) => (told < 3 ? 503 : 202));
  try {
    const lines: string[] = [];
    await Effect.runPromise(telling(takes.url).pipe(Effect.provide(logged(lines))));
    assert.deepEqual([takes.told.length, lines], [3, []]);
    assert.deepEqual(JSON.parse(takes.told[0] ?? ""), { kind: "harness.idle", subject: { kind: "conversation", key: "telegram:7" } });
  } finally {
    takes.close();
  }
  // Never taken: told every second for its minute's patience, then given up on, as its log says.
  const never = await frontTold(() => 503);
  try {
    const lines: string[] = [];
    await Effect.runPromise(telling(never.url).pipe(Effect.provide(logged(lines))));
    assert.ok(never.told.length >= 55 && never.told.length <= 62, `told ${never.told.length} times`);
    assert.equal(lines.length, 1);
    assert.match(lines[0] ?? "", /^could not tell of harness\.idle of telegram:7: the sandbox gateway answered 503/u);
  } finally {
    never.close();
  }
});
