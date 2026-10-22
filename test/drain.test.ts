/**
 * What an alasio told to stop delivers before it ends (src/telegram/drain.ts): the albums
 * due made prompts of, then every reply due, one Telegram held off among them, until none is
 * due, or its bound, counted from when it was told to stop, past which what is left waits for
 * the next alasio, as does one put off after it failed otherwise.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { setTimeout as sleep } from "node:timers/promises";

import { Duration, Effect, Layer } from "effect";

import { Store } from "../src/persistence/store.ts";
import { drain } from "../src/telegram/drain.ts";
import { MediaGroups } from "../src/telegram/media-group-buffer.ts";
import { Outbox } from "../src/telegram/outbox.ts";
import { botApiError, botApiLayer } from "./support/bot-api.ts";
import { testStore } from "./support/store.ts";

/** How a drain is run (drained): how long it is bound to, from how long ago alasio was told to stop (from the drain's start, unless given), and whether its replies are queued straight into the store, waking no delivery of the outbox's own, so the drain's delivers them. */
interface Drained {
  readonly bound: Duration.Input;
  readonly toldAgo?: Duration.Input;
  readonly unwoken?: boolean;
}

/**
 * Drains, as `how` says, the outbox of a new store holding `texts`, Telegram answering each
 * send as `answer` says (an error's status and how many seconds it asks to be waited for, or
 * null to accept): what was sent, the albums flushed, and how many replies are left.
 */
async function drained(texts: readonly string[], answer: (text: string, asked: number) => { readonly status: number; readonly retryAfter: number } | null, { bound, toldAgo, unwoken = false }: Drained) {
  const store = await testStore();
  const sent: string[] = [];
  const asked = new Map<string, number>();
  let flushed = 0;
  const telegram = botApiLayer(async (call) => {
    const text = (call.params as { readonly text?: string }).text ?? "";
    asked.set(text, (asked.get(text) ?? 0) + 1);
    const refusal = answer(text, asked.get(text) ?? 0);
    if (refusal) throw botApiError("Too Many Requests", refusal.status, refusal.retryAfter);
    sent.push(text);
    return { message_id: sent.length, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  const albums = Layer.succeed(MediaGroups, MediaGroups.of({ buffer: () => Effect.die("no album comes as alasio stops"), flushDue: Effect.sync(() => void flushed++) }));
  const layer = Layer.mergeAll(Outbox.layer, albums).pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
  const left = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const outbox = yield* Outbox;
    if (unwoken) {
      // Once the outbox's own delivery has made its first pass, of none, the next of which is seconds away.
      yield* Effect.promise(() => sleep(500));
      for (const text of texts) yield* store.enqueueOutboxText({ chatId: 42, text, options: { format: "plain" }, traceparent: null });
    } else {
      for (const text of texts) yield* outbox.enqueueText({ chatId: 42, text, options: { format: "plain" } });
    }
    yield* drain(toldAgo === undefined ? { bound } : { bound, toldAt: Effect.succeed(Date.now() - Duration.toMillis(toldAgo)) });
    return yield* store.getPendingOutboxCount;
  }).pipe(Effect.provide(layer))));
  return { sent, flushed, left };
}

test("delivers every reply queued, one Telegram holds off for a while among them, the albums due flushed first, before it ends", async () => {
  // The first refused once, Telegram asking to be waited for a second.
  const { sent, flushed, left } = await drained(["first", "second"], (text, asked) => (text === "first" && asked === 1 ? { status: 429, retryAfter: 1 } : null), { bound: "30 seconds" });
  assert.deepEqual([sent, flushed, left], [["first", "second"], 1, 0]);
});

test("leaves a reply put off after it failed otherwise for the next alasio, and ends without waiting for it", async () => {
  const started = Date.now();
  const { sent, left } = await drained(["refused", "after"], (text) => (text === "refused" ? { status: 500, retryAfter: 0 } : null), { bound: "60 seconds" });
  // The chat's next waits behind it, in its order.
  assert.deepEqual([sent, left], [[], 2]);
  assert.ok(Date.now() - started < 10_000, "the drain waited for a reply put off");
});

test("ends at its bound with a reply whose delivery waits out Telegram's wait past it, the delivery cut short and the reply left for the next alasio", async () => {
  const started = Date.now();
  // Delivered by the drain itself, its client waiting a minute before it sends again.
  const { sent, left } = await drained(["held"], () => ({ status: 429, retryAfter: 60 }), { bound: "2 seconds", unwoken: true });
  assert.deepEqual([sent, left], [[], 1]);
  assert.ok(Date.now() - started < 10_000, "the drain ran past its bound");
});

test("counts its bound from when alasio was told to stop, not from its own start", async () => {
  const started = Date.now();
  // Told to stop 28 s into a 30 s bound, as the update in hand took them.
  const { sent, left } = await drained(["held"], () => ({ status: 429, retryAfter: 60 }), { bound: "30 seconds", toldAgo: "28 seconds", unwoken: true });
  assert.deepEqual([sent, left], [[], 1]);
  assert.ok(Date.now() - started < 10_000, "the drain counted its bound from its own start");
});
