/**
 * Presses kept as their controls run on past their updates (src/telegram/presses.ts): their
 * answers through the outbox once kept; and a press whose control is done but whose answer the
 * store would not take kept with its answer, which the next look queues alone, its control
 * never run twice.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { CallbackQuery } from "@grammyjs/types";
import { Duration, Effect, FiberSet, Layer } from "effect";

import { Store } from "../src/persistence/store.ts";
import { TelegramClient } from "../src/telegram/client.ts";
import { holdWhileRunning, PRESS_HOLD, pressAnswers, recoverPresses } from "../src/telegram/presses.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { recordingOutbox, withServices } from "./support/turns.ts";

const CHAT = 4242;

const query: CallbackQuery = {
  id: "press-1",
  from: { id: CHAT, is_bot: false, first_name: "Operator" },
  chat_instance: "1",
  data: "action-1",
  message: { message_id: 7, date: 0, chat: { id: CHAT, type: "private", first_name: "Operator" } },
};

/** A press whose control answers `text`, as one done answers it. */
const answering = (text: string) => Effect.asVoid(Effect.flatMap(TelegramClient, (client) => client.answerCallbackQuery(query.id, text)));

/** The kept presses of the store in `schema`, each with whether its hold is let go of. */
async function kept(schema: string): Promise<{ id: string; lapsed: boolean }[]> {
  return (await (await testPool()).query<{ id: string; lapsed: boolean }>(`select id, held_until <= now() as lapsed from ${schema}.press_jobs`)).rows;
}

/** Runs the press `handling`, kept as its update was, held while it runs, with the outbox `outbox`, until it is done. */
async function runKept(schema: string, outbox: ReturnType<typeof recordingOutbox>) {
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  const action = { id: "action-1", conversationId, kind: "workspace:refresh", payload: {} };
  await run(store.keepPress({ callbackQuery: query, action, holder: "this", holdMs: Duration.toMillis(PRESS_HOLD) }));
  const telegram = recordingTelegram();
  const answers = pressAnswers(query, { kept: true });
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const presses = yield* FiberSet.make();
    const pressing = yield* FiberSet.run(presses, answers.through(answering("Refreshed.")).pipe(Effect.orDie) as Effect.Effect<void>);
    yield* holdWhileRunning(query.id, "this", pressing, answers.unsent);
  })).pipe(Effect.provide(Layer.mergeAll(outbox.layer, telegram.layer).pipe(Layer.provideMerge(Layer.succeed(Store, store))))));
  return { store, telegram, unsent: answers.unsent() };
}

test("a kept press's answer goes through the outbox, not to its query, and the press is forgotten once done", async () => {
  const schema = newSchema();
  const outbox = recordingOutbox();
  const { telegram, unsent } = await runKept(schema, outbox);
  assert.deepEqual(outbox.queued.texts.map(({ chatId, text }) => [chatId, text]), [[CHAT, "Refreshed."]]);
  assert.deepEqual(telegram.calls.answerCallbackQuery, []);
  assert.deepEqual(unsent, []);
  assert.deepEqual(await kept(schema), []);
});

test("a kept press whose control is done but whose answer the store will not take, though queued again, keeps its answer for the next look, which queues it alone, its control never run again", async () => {
  const schema = newSchema();
  const { store, unsent } = await runKept(schema, recordingOutbox({ refusesText: () => true }));
  assert.deepEqual(unsent, ["Refreshed."]);
  assert.deepEqual(await kept(schema), [{ id: query.id, lapsed: true }]);
  // The next look, the store taking it now: the answer queued, the press forgotten, its control not run.
  const outbox = recordingOutbox();
  const telegram = recordingTelegram();
  await withServices({ store, outbox: outbox.layer, telegram: telegram.layer }, (alasio) =>
    alasio.runPromise(Effect.scoped(Effect.flatMap(FiberSet.make(), (presses) => recoverPresses(presses)))));
  assert.deepEqual(outbox.queued.texts.map(({ chatId, text }) => [chatId, text]), [[CHAT, "Refreshed."]]);
  assert.deepEqual([telegram.calls.editMessageText, telegram.calls.answerCallbackQuery], [[], []]);
  assert.deepEqual(await kept(schema), []);
});
