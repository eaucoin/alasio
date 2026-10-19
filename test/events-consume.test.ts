/**
 * The environment's events as alasio reads them (src/events/consume.ts): each handled once,
 * its writes kept with its record as handled, whichever alasio it is delivered to and however
 * often; one whose handling failed handled again as it is delivered again; and what alasio
 * cannot read, or has no handling of, passed over.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Effect, Fiber, Layer } from "effect";

import { consumeEvents, EnvironmentEvents, type EventHandlers } from "../src/events/consume.ts";
import { eventRecord, type EventInput } from "../src/events/events.ts";
import type { TopicRecord } from "../src/log/topic.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { Store } from "../src/persistence/store.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";

/** The record id `n`, a UUID as the gateways make one. */
const idOf = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}` as const;

const RELEASED: EventInput = {
  kind: "workspace.released",
  environment: "main",
  source: "sandbox-gateway",
  subject: { kind: "workspace", key: "sessionfs:fs-1" },
  binding: "binding-1",
  reason: "idle",
};

/** A topic of `values`, its group told of each committed, a member joining again reading on from there. */
function topicOf(values: readonly string[]): { readonly layer: Layer.Layer<EnvironmentEvents>; readonly committed: () => number } {
  let committed = 0;
  const layer = Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({
    join: Effect.sync(() => {
      let next = committed;
      let closed = false;
      return {
        async next(): Promise<TopicRecord> {
          for (;;) {
            if (closed) throw new Error("the member was closed");
            if (next < values.length) {
              const at = next++;
              return { value: Buffer.from(values[at] ?? ""), position: `events.main/0/${at}`, commit: async () => void (committed = Math.max(committed, at + 1)) };
            }
            await sleep(20);
          }
        },
        async close() {
          closed = true;
        },
      };
    }),
  }));
  return { layer, committed: () => committed };
}

/** Runs `handlers` on `values` with the store in `schema` until every record is told, then stops. */
async function consume(schema: string, values: readonly string[], handlers: EventHandlers): Promise<void> {
  const store = await testStore({ schema });
  const topic = topicOf(values);
  const consumer = Effect.runFork(consumeEvents(handlers).pipe(Effect.provide(Layer.mergeAll(topic.layer, Layer.succeed(Store, store)))));
  for (const deadline = Date.now() + 15_000; topic.committed() < values.length; await sleep(20)) {
    if (Date.now() > deadline) assert.fail(`${topic.committed()} of ${values.length} records told`);
  }
  await Effect.runPromise(Fiber.interrupt(consumer));
}

/** What the outbox of the store in `schema` holds, in order. */
async function queued(schema: string): Promise<string[]> {
  return (await (await testPool()).query<{ text: string }>(`select text from ${schema}.telegram_outbox order by seq`)).rows.map(({ text }) => text);
}

/** Handlers that queue a reply for each workspace released, counting each handling. */
function replying(handled: string[]): EventHandlers {
  return {
    "workspace.released": (event, writes) => Effect.sync(() => void handled.push(event.id)).pipe(Effect.andThen(writes.enqueueOutboxText({ chatId: 42, text: `released ${event.binding}` }))),
  };
}

test("an event delivered twice, to one alasio and then to another, is handled once, its writes kept with it", async () => {
  const schema = newSchema();
  const record = eventRecord(RELEASED, 1, idOf(1)).value;
  const handled: string[] = [];
  await consume(schema, [record, record], replying(handled));
  // Another alasio, the event delivered to it as well.
  await consume(schema, [record], replying(handled));
  assert.deepEqual(handled, [idOf(1)]);
  assert.deepEqual(await queued(schema), ["released binding-1"]);
});

test("an event whose handling fails is handled again as it is delivered again, nothing of the failed handling kept", { timeout: 30_000 }, async () => {
  const schema = newSchema();
  const handled: string[] = [];
  let failures = 1;
  const handlers: EventHandlers = {
    "workspace.released": (event, writes) =>
      writes.enqueueOutboxText({ chatId: 42, text: `released ${event.binding}` }).pipe(
        Effect.andThen(Effect.suspend(() => (failures-- > 0 ? Effect.fail(new StoreError({ cause: new Error("Neon is away") })) : Effect.sync(() => void handled.push(event.id))))),
      ),
  };
  await consume(schema, [eventRecord(RELEASED, 1, idOf(2)).value], handlers);
  assert.deepEqual(handled, [idOf(2)]);
  assert.deepEqual(await queued(schema), ["released binding-1"]);
});

test("a record alasio cannot read, of a later version, of a kind it does not know, or of a kind it has no handling of, is passed over, and those after it handled", async () => {
  const schema = newSchema();
  const handled: string[] = [];
  const later = JSON.stringify({ ...JSON.parse(eventRecord(RELEASED, 1, idOf(90)).value), v: 99 });
  const unknown = JSON.stringify({ ...JSON.parse(eventRecord(RELEASED, 1, idOf(91)).value), kind: "workspace.renamed" });
  const unhandled = eventRecord({ kind: "workspace.lost", environment: "main", source: "sandbox-gateway", subject: { kind: "workspace", key: "sessionfs:fs-1" }, binding: "binding-1", pod: "pod-1", reason: "gone" }, 1, idOf(92)).value;
  await consume(schema, ["not json", later, unknown, unhandled, eventRecord(RELEASED, 1, idOf(3)).value], replying(handled));
  assert.deepEqual(handled, [idOf(3)]);
  assert.deepEqual((await (await testPool()).query<{ id: string }>(`select id from ${schema}.events_handled order by id`)).rows.map(({ id }) => id), [idOf(3)]);
});

test("the events handled are forgotten with what else is kept only in flight, once their topic keeps them no more", async () => {
  const schema = newSchema();
  await consume(schema, [eventRecord(RELEASED, 1, idOf(4)).value], replying([]));
  const store = await testStore({ schema });
  await (await testPool()).query(`update ${schema}.events_handled set handled_at = now() - interval '8 days'`);
  await run(store.pruneTransient("7 days"));
  assert.equal((await (await testPool()).query(`select 1 from ${schema}.events_handled`)).rowCount, 0);
});
