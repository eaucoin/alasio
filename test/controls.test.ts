/**
 * Controls kept as they run on past their updates (src/telegram/controls.ts): a press's
 * answers through the outbox once kept; a press done but whose answer the store would not take
 * kept with its answer, which the next look queues alone, its control never run twice; and a
 * command its holder let go of, as it stopped, run again by the next look, once.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { CallbackQuery } from "@grammyjs/types";
import { Duration, Effect, Exit, FiberSet, Layer, Option } from "effect";

import type { Control, NewKeptControl } from "../src/persistence/control-repository.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { Store } from "../src/persistence/store.ts";
import { TelegramClient } from "../src/telegram/client.ts";
import { type Awaited, awaitOrKeep, CONTROL_HOLD, holdWhileRunning, pressAnswers, recoverControls, rolledBack } from "../src/telegram/controls.ts";
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

/** The kept controls of the store in `schema`, each with whether its hold is let go of. */
async function kept(schema: string): Promise<{ id: string; lapsed: boolean }[]> {
  return (await (await testPool()).query<{ id: string; lapsed: boolean }>(`select id, held_until <= now() as lapsed from ${schema}.control_jobs`)).rows;
}

/** Runs the press `handling`, kept as its update was, held while it runs, with the outbox `outbox`, until it is done. */
async function runKept(schema: string, outbox: ReturnType<typeof recordingOutbox>) {
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  const action = { id: "action-1", conversationId, kind: "workspace:refresh", payload: {} };
  await run(store.keepControl({ control: { kind: "press", callbackQuery: query, action }, holder: "this", holdMs: Duration.toMillis(CONTROL_HOLD) }));
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
    alasio.runPromise(Effect.scoped(Effect.flatMap(FiberSet.make(), (presses) => recoverControls(presses)))));
  assert.deepEqual(outbox.queued.texts.map(({ chatId, text }) => [chatId, text]), [[CHAT, "Refreshed."]]);
  assert.deepEqual([telegram.calls.editMessageText, telegram.calls.answerCallbackQuery], [[], []]);
  assert.deepEqual(await kept(schema), []);
});

/** A command of the operator's chat, its message `messageId`, as a control. */
const command = (conversationId: string, messageId: number, text: string): Control => ({ kind: "command", conversationId, chatId: CHAT, messageId, text });

test("a kept command its holder let go of, as its alasio stopped, is run again from its message by the next look, once, and forgotten", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: CHAT, user: { id: CHAT } }));
  await run(store.keepControl({ control: command(conversationId, 9, "/stop"), holder: "stopped", holdMs: 0 }));
  const telegram = recordingTelegram();
  await withServices({ store, telegram: telegram.layer }, async (alasio) => {
    await alasio.runPromise(Effect.scoped(Effect.flatMap(FiberSet.make(), (handling) => Effect.andThen(recoverControls(handling), FiberSet.awaitEmpty(handling)))));
    // The next look finds none to run again.
    await alasio.runPromise(Effect.scoped(Effect.flatMap(FiberSet.make(), (handling) => Effect.andThen(recoverControls(handling), FiberSet.awaitEmpty(handling)))));
  });
  assert.deepEqual(telegram.calls.sendMessage.map(([, text]) => text), ["No active query to stop."]);
  assert.deepEqual(await kept(schema), []);
});

test("a command cut short as its update awaits it, as a stop interrupts it, is kept to run again, not taken as done", async () => {
  const keptControls: NewKeptControl[] = [];
  const marks = { keepControl: (kept: NewKeptControl) => Effect.sync(() => void keptControls.push(kept)) };
  const awaited = await Effect.runPromise(Effect.scoped(Effect.flatMap(FiberSet.make(), (handling) => awaitOrKeep(handling, command("telegram:1", 9, "/sessions new"), Effect.interrupt, marks))));
  assert.ok(Option.isSome(awaited.kept));
  assert.deepEqual(keptControls.map(({ control }) => control), [command("telegram:1", 9, "/sessions new")]);
});

test("a command whose keeping fails, its update's transaction with it, is cut short, so it never runs beside the update handled again", async () => {
  const refusing = { keepControl: () => Effect.fail(new StoreError({ cause: new Error("the store is down") })) };
  let cut = false;
  const work = Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void (cut = true))));
  // Whether it was cut short as it failed, its handling, which a stop would cut it short with, still open.
  const cutAsItFailed = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const handling = yield* FiberSet.make();
    const exit = yield* Effect.exit(awaitOrKeep(handling, command("telegram:1", 9, "/sessions new"), work, refusing));
    assert.ok(Exit.isFailure(exit));
    return cut;
  })));
  assert.equal(cutAsItFailed, true);
});

test("a command kept as its update's transaction fails after all, keeping nothing of it, is cut short as the transaction rolls back", async () => {
  const marks = { keepControl: () => Effect.void };
  let cut = false;
  const work = Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void (cut = true))));
  const awaited: Awaited[] = [];
  const cutAsItFailed = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const handling = yield* FiberSet.make();
    // As an update's transaction does: the command kept, then its update's mark failing.
    const exit = yield* Effect.exit(Effect.gen(function*() {
      awaited.push(yield* awaitOrKeep(handling, command("telegram:1", 9, "/sessions new"), work, marks));
      return yield* Effect.fail(new StoreError({ cause: new Error("the store is down") }));
    }).pipe(Effect.onError(() => rolledBack(awaited))));
    assert.ok(Exit.isFailure(exit));
    return cut;
  })));
  assert.deepEqual([awaited.length, cutAsItFailed], [1, true]);
});

test("a press whose keeping fails, its update's transaction with it, runs on, as its action is consumed already and the update handled again does nothing of it", async () => {
  const refusing = { keepControl: () => Effect.fail(new StoreError({ cause: new Error("the store is down") })) };
  let cut = false;
  const work = Effect.never.pipe(Effect.onInterrupt(() => Effect.sync(() => void (cut = true))));
  const action = { id: "action-1", conversationId: "telegram:1", kind: "workspace:refresh", payload: {} };
  const cutAsItFailed = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const handling = yield* FiberSet.make();
    const exit = yield* Effect.exit(awaitOrKeep(handling, { kind: "press", callbackQuery: query, action }, work, refusing));
    assert.ok(Exit.isFailure(exit));
    return cut;
  })));
  assert.equal(cutAsItFailed, false);
});
