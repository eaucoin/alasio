/**
 * How long alasio, started, took to answer the first update it handled (src/telegram/wake-latency.ts):
 * from its message's date to the first answer queued to its chat in its trace, once a start,
 * none timed past the wait for it; nothing where the start's first update was a press, which has
 * no date of its own, or came in no trace; and the outbox telling it, with the trace each is
 * queued in, what it queues to a chat.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { Update } from "@grammyjs/types";
import { context, type Span, trace } from "@opentelemetry/api";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Effect, Layer } from "effect";

import { recorded } from "./support/metrics.ts";
import { Store } from "../src/persistence/store.ts";
import { currentTraceparent } from "../src/telemetry/index.ts";
import { Outbox } from "../src/telegram/outbox.ts";
import { TelegramClient } from "../src/telegram/client.ts";
import { answeringClient, WAKE_LATENCY, WakeLatency, type WakeLatencyOptions } from "../src/telegram/wake-latency.ts";
import { botApiLayer } from "./support/bot-api.ts";
import { testStore } from "./support/store.ts";

// Traces as alasio's are, its SDK registered, so what is queued is queued in the trace it runs in; kept in memory.
new NodeSDK({ autoDetectResources: false, spanProcessors: [new SimpleSpanProcessor(new InMemorySpanExporter())], instrumentations: [] }).start();

/** A message of the chat `chatId`, sent `agoSeconds` before now, as Telegram dates it, to the second. */
const message = (updateId: number, chatId: number, agoSeconds: number): Update => ({
  update_id: updateId,
  message: {
    message_id: updateId,
    date: Math.floor(Date.now() / 1_000) - agoSeconds,
    from: { id: chatId, is_bot: false, first_name: "Operator" },
    chat: { id: chatId, type: "private", first_name: "Operator" },
    text: "hello",
  },
});

/** A press of a button of the chat `chatId`'s message, dated as Telegram dates the message pressed, not the press. */
const press = (updateId: number, chatId: number): Update => ({
  update_id: updateId,
  callback_query: {
    id: String(updateId),
    from: { id: chatId, is_bot: false, first_name: "Operator" },
    chat_instance: "chat",
    data: "action",
    message: { message_id: 1, date: 0, chat: { id: chatId, type: "private", first_name: "Operator" }, text: "Run it?" },
  },
});

/** A trace of its own, as each update's is, and its traceparent. */
function newTrace(): { readonly span: Span; readonly traceparent: string } {
  const span = trace.getTracer("test").startSpan("alasio.update", {}, context.active());
  const traceparent = context.with(trace.setSpan(context.active(), span), () => currentTraceparent());
  assert.ok(traceparent);
  return { span, traceparent };
}

/** `effect` run in `span`'s trace, as what an update leads to runs in its. */
const inTrace = <A, E>(span: Span, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  Effect.promise(() => context.with(trace.setSpan(context.active(), span), () => Effect.runPromiseExit(effect))).pipe(Effect.flatten);

/** What `body` records of WAKE_LATENCY, run on a started alasio's timing, made as `options` say, and an outbox of a new store, telling it what it queues. */
async function timed(body: (wake: WakeLatency["Service"], outbox: Outbox["Service"], store: Store["Service"]) => Effect.Effect<void, unknown>, options: WakeLatencyOptions = {}) {
  const store = await testStore();
  const telegram = botApiLayer(async () => ({ message_id: 1, date: 0, chat: { id: 41, type: "private", first_name: "Operator" } }));
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store), WakeLatency.layerWith(options))));
  return await recorded(WAKE_LATENCY, () =>
    Effect.runPromise(Effect.scoped(Effect.flatMap(Effect.all([WakeLatency, Outbox]), ([wake, outbox]) => body(wake, outbox, store)).pipe(Effect.provide(layer)))));
}

test("the first update an alasio handles after it starts is timed from its message's date to the first answer queued to its chat in its trace, once: not by what is queued to another chat, or in another trace", async () => {
  const { span, traceparent } = newTrace();
  const background = newTrace();
  const latency = await timed((wake, outbox) =>
    Effect.gen(function*() {
      yield* wake.handling(message(1, 41, 5), traceparent);
      // Another chat's, as what the last alasio left is delivered, in the update's trace or not: not its answer.
      yield* inTrace(span, outbox.enqueueText({ chatId: 42, text: "left from before" }));
      // A reply of a turn that ran on meanwhile, to the update's chat, in that turn's trace: not its answer either.
      yield* inTrace(background.span, outbox.enqueueText({ chatId: 41, text: "a turn of before is done" }));
      // Nor what is queued in no trace.
      yield* outbox.enqueueText({ chatId: 41, text: "in no trace" });
      yield* Effect.sleep("2 seconds");
      yield* inTrace(span, outbox.enqueueStatus({ chatId: 41, text: "Codex is working" }));
      // Neither the reply after it nor a later update's is timed.
      yield* inTrace(span, outbox.enqueueText({ chatId: 41, text: "Done." }));
      const later = newTrace();
      yield* wake.handling(message(2, 41, 1), later.traceparent);
      yield* inTrace(later.span, outbox.enqueueText({ chatId: 41, text: "Done again." }));
    })
  );
  const { count = 0, sum = 0 } = latency[""] ?? {};
  assert.equal(count, 1, JSON.stringify(latency));
  // Sent five seconds before the two waited for the answer, to the second: between seven and nine, as the date is
  // floored and the test takes its time; not the five of what was queued before the wait.
  assert.ok(sum >= 7 && sum < 9, `timed at ${sum} s`);
});

test("the start's first update whose answer is not queued within the wait for it is given up on, timing nothing", async () => {
  const { span, traceparent } = newTrace();
  const latency = await timed((wake, outbox) =>
    Effect.gen(function*() {
      yield* wake.handling(message(1, 41, 5), traceparent);
      yield* Effect.sleep("1500 millis");
      yield* inTrace(span, outbox.enqueueStatus({ chatId: 41, text: "Codex is working" }));
    }), { answerWithin: "1 second" });
  assert.deepEqual(latency, {});
});

test("a start whose first update is a press, which has no date of its own, or came in no trace, times nothing, nor the message after it", async () => {
  for (const first of [(wake: WakeLatency["Service"]) => wake.handling(press(1, 41), newTrace().traceparent), (wake: WakeLatency["Service"]) => wake.handling(message(1, 41, 5), null)]) {
    const { span, traceparent } = newTrace();
    const latency = await timed((wake, outbox) =>
      Effect.gen(function*() {
        yield* first(wake);
        // An answer in no trace, as the first's in no trace would be, and the next message's in its own.
        yield* outbox.enqueueText({ chatId: 41, text: "Done." });
        yield* wake.handling(message(2, 41, 5), traceparent);
        yield* inTrace(span, outbox.enqueueText({ chatId: 41, text: "Done." }));
      })
    );
    assert.deepEqual(latency, {});
  }
});

test("an answer queued before the start's first update, or a notice not queued as the chat was told it already, times nothing", async () => {
  const { span, traceparent } = newTrace();
  const latency = await timed((wake, outbox, store) =>
    Effect.gen(function*() {
      const conversationId = yield* store.upsertConversation({ chatId: "41", user: { id: 41 } });
      yield* inTrace(span, outbox.enqueueText({ chatId: 41, text: "left from before" }));
      assert.equal(yield* inTrace(span, outbox.enqueueNotice({ conversationId, chatId: 41, text: "told once" })), true);
      yield* wake.handling(message(1, 41, 5), traceparent);
      assert.equal(yield* inTrace(span, outbox.enqueueNotice({ conversationId, chatId: 41, text: "told once" })), false);
    })
  );
  assert.deepEqual(latency, {});
});

test("an answer sent at once, as a command's is, which no outbox queues, is the first update's answer too, in its trace alone", async () => {
  const { span, traceparent } = newTrace();
  const background = newTrace();
  const sent: string[] = [];
  const telegram = botApiLayer(async (call) => {
    sent.push(call.method);
    return { message_id: 1, date: 0, chat: { id: 41, type: "private", first_name: "Operator" } };
  });
  const layer = answeringClient.pipe(Layer.provideMerge(Layer.mergeAll(telegram, WakeLatency.layer)));
  const latency = await recorded(WAKE_LATENCY, () =>
    Effect.runPromise(Effect.scoped(Effect.flatMap(Effect.all([WakeLatency, TelegramClient]), ([wake, client]) =>
      Effect.gen(function*() {
        yield* wake.handling(message(1, 41, 5), traceparent);
        yield* inTrace(background.span, client.sendMessage(41, "a turn of before is done", { format: "plain" }));
        yield* client.sendMessage(41, "in no trace", { format: "plain" });
        yield* Effect.sleep("2 seconds");
        yield* inTrace(span, client.sendMessage(41, "Active: Codex", { format: "plain" }));
      })
    ).pipe(Effect.provide(layer)))));
  assert.equal(sent.length, 3);
  const { count = 0, sum = 0 } = latency[""] ?? {};
  assert.equal(count, 1, JSON.stringify(latency));
  assert.ok(sum >= 7 && sum < 9, `timed at ${sum} s`);
});
