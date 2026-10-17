/**
 * A turn's status message and its edits, delivered through the outbox
 * (src/telegram/outbox.ts): in each chat's order with its replies, the status message's
 * Telegram id recorded for its edits, and a status message or an edit Telegram refuses for
 * good, or an edit of a status message with no message, given up on rather than holding
 * the chat back; an entry being delivered as alasio stops is delivered whole.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect";
import pg from "pg";

import { Store } from "../src/persistence/store.ts";
import { Outbox } from "../src/telegram/outbox.ts";
import { botApiError, botApiLayer } from "./support/bot-api.ts";
import { deliverAll } from "./support/outbox.ts";
import { holdChatLock, lockHolder, newSchema, testDatabaseUrl, testPool, testStore } from "./support/store.ts";

/** What the stand-in Bot API was asked: each call's method, the message it named, and its text. */
interface Asked {
  readonly method: string;
  readonly messageId?: number;
  readonly text: string;
}

/**
 * Runs `body` with the outbox of a new store, Telegram answering each call, once `answer`
 * is done with it, as it says: an error's status, or null to accept.
 */
async function withOutbox(
  answer: (method: string, text: string) => number | null | Promise<number | null>,
  body: (outbox: Outbox["Service"], store: Store["Service"]) => Effect.Effect<void, unknown>,
): Promise<readonly Asked[]> {
  const store = await testStore();
  const asked: Asked[] = [];
  let next = 500;
  const telegram = botApiLayer(async (call) => {
    const params = call.params as { readonly message_id?: number; readonly text?: string };
    asked.push({ method: call.method, ...(params.message_id === undefined ? {} : { messageId: params.message_id }), text: params.text ?? "" });
    const status = await answer(call.method, params.text ?? "");
    if (status !== null) throw botApiError("Bad Request: message is not modified", status);
    return call.method === "editMessageText" ? true : { message_id: next++, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
  await Effect.runPromise(Effect.scoped(Effect.flatMap(Outbox, (outbox) => body(outbox, store)).pipe(Effect.provide(layer))));
  return asked;
}

test("a status message is sent before what is queued after it, its id recorded, and its edits edit that message in order", async () => {
  const asked = await withOutbox(() => null, (outbox, store) =>
    Effect.gen(function*() {
      const status = yield* outbox.enqueueStatus({ chatId: 42, text: "Codex is working", options: { parse_mode: "HTML" } });
      yield* outbox.enqueueEdit(status, { chatId: 42, text: "Codex is watching workflow 7" });
      yield* outbox.enqueueEdit(status, { chatId: 42, text: "Codex worked for a moment." });
      yield* outbox.enqueueText({ chatId: 42, text: "Done", options: { format: "plain" } });
      yield* deliverAll(outbox, store);
    }));
  assert.deepEqual(asked, [
    { method: "sendMessage", text: "Codex is working" },
    { method: "editMessageText", messageId: 500, text: "Codex is watching workflow 7" },
    { method: "editMessageText", messageId: 500, text: "Codex worked for a moment." },
    { method: "sendMessage", text: "Done" },
  ]);
});

test("an edit Telegram refuses for good, or of a status message with no message, is given up on, and what follows it is sent", async () => {
  const asked = await withOutbox((method) => (method === "editMessageText" ? 400 : null), (outbox, store) =>
    Effect.gen(function*() {
      const status = yield* outbox.enqueueStatus({ chatId: 42, text: "Codex is working" });
      yield* outbox.enqueueEdit(status, { chatId: 42, text: "Codex worked for a moment." });
      // Of an entry the outbox does not hold, as one pruned.
      yield* outbox.enqueueEdit("pruned", { chatId: 42, text: "Codex interrupted." });
      yield* outbox.enqueueText({ chatId: 42, text: "Done", options: { format: "plain" } });
      yield* deliverAll(outbox, store);
      assert.equal(yield* store.getPendingOutboxCount, 0);
    }));
  assert.deepEqual(asked.map(({ method, text }) => [method, text]), [
    ["sendMessage", "Codex is working"],
    ["editMessageText", "Codex worked for a moment."],
    ["sendMessage", "Done"],
  ]);
});

test("two alasios on one store deliver each chat's replies once, in their order, each delivering a chat's by its lock", async () => {
  const schema = newSchema();
  const sent: { readonly chat: number; readonly text: string }[] = [];
  // Telegram slow to take each, so both alasios' passes find the same entries due.
  const telegram = botApiLayer(async (call) => {
    const { chat_id: chat, text = "" } = call.params as { readonly chat_id: number; readonly text?: string };
    await sleep(20);
    sent.push({ chat: Number(chat), text });
    return { message_id: 500, date: 0, chat: { id: Number(chat), type: "private", first_name: "Operator" } };
  });
  const store = await testStore({ schema });
  const outboxOf = async () => Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, await testStore({ schema })))));
  const [first, second] = [await outboxOf(), await outboxOf()];
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const one = Context.get(yield* Layer.build(first), Outbox);
    const other = Context.get(yield* Layer.build(second), Outbox);
    for (const n of [1, 2, 3]) {
      for (const chatId of [42, 43]) yield* one.enqueueText({ chatId, text: `${chatId}: ${n}`, options: { format: "plain" } });
    }
    yield* Effect.all([deliverAll(one, store), deliverAll(other, store), deliverAll(one, store), deliverAll(other, store)], { concurrency: "unbounded", discard: true });
  })));
  for (const chat of [42, 43]) {
    assert.deepEqual(sent.filter((each) => each.chat === chat).map(({ text }) => text), [1, 2, 3].map((n) => `${chat}: ${n}`));
  }
});

test("a pass passes over a chat whose delivery another holds, waiting for nothing, and delivers the rest; the next delivers it once let go of", { timeout: 20_000 }, async () => {
  const schema = newSchema();
  const sent: string[] = [];
  const telegram = botApiLayer(async (call) => {
    sent.push((call.params as { readonly text?: string }).text ?? "");
    return { message_id: 500, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  const store = await testStore({ schema });
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
  // Another alasio, delivering chat 42's.
  const letGo = await holdChatLock(schema, "delivery", 42);
  try {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const outbox = Context.get(yield* Layer.build(layer), Outbox);
      yield* outbox.enqueueText({ chatId: 42, text: "to 42", options: { format: "plain" } });
      yield* outbox.enqueueText({ chatId: 43, text: "to 43", options: { format: "plain" } });
      // Bounded, as a pass that waited on the lock would wait as long as the test holds it.
      yield* outbox.deliverDue.pipe(Effect.timeout("5 seconds"));
      // 43's, by this pass or the outbox's own beside it, and marked so as its lock's
      // transaction commits; 42's left due.
      const dueTexts = () => Effect.map(store.getDueOutbox(), (due) => due.map(({ text }) => text));
      for (const deadline = Date.now() + 5000; (yield* dueTexts()).includes("to 43"); yield* Effect.sleep("10 millis")) {
        if (Date.now() > deadline) assert.fail("43's reply was not delivered");
      }
      assert.deepEqual([sent, (yield* store.getDueOutbox()).map(({ text }) => text)], [["to 43"], ["to 42"]]);
      yield* Effect.promise(letGo);
      yield* deliverAll(outbox, store);
      assert.deepEqual(sent, ["to 43", "to 42"]);
    })));
  } finally {
    await letGo();
  }
});

test("an entry whose delivery lock is lost, its connection gone, as it is about to be sent, is not sent by that pass, and the next sends it once", { timeout: 30_000 }, async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const sent: string[] = [];
  const telegram = botApiLayer(async (call) => {
    sent.push((call.params as { readonly text?: string }).text ?? "");
    return { message_id: 500, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  // The pass reads the entry's media, under the lock, just before it sends it: there the lock's connection is lost.
  const reading = Effect.runSync(Deferred.make<void>());
  const killed = Effect.runSync(Deferred.make<void>());
  let paused = false;
  const pausing: Store["Service"] = {
    ...store,
    getOutboxMedia: (id) =>
      Effect.suspend(() => {
        if (paused) return store.getOutboxMedia(id);
        paused = true;
        return Deferred.succeed(reading, undefined).pipe(Effect.andThen(Deferred.await(killed)), Effect.andThen(store.getOutboxMedia(id)));
      }),
  };
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, pausing))));
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const outbox = Context.get(yield* Layer.build(layer), Outbox);
    yield* outbox.enqueueText({ chatId: 42, text: "once", options: { format: "plain" } });
    yield* Deferred.await(reading);
    yield* Effect.promise(async () => (await testPool()).query("select pg_terminate_backend($1)", [await lockHolder(schema, "delivery", 42)]));
    yield* Deferred.succeed(killed, undefined);
    // That pass sends nothing, and marks nothing: the next pass, of the outbox's own or this, sends it, once.
    yield* deliverAll(outbox, store);
    yield* Effect.sleep("1 second");
    assert.deepEqual(sent, ["once"]);
    assert.equal(yield* store.getPendingOutboxCount, 0);
  })));
});

test("alasio stopping in many chats at once delivers each one's restarting status on a small pool, its passes holding no more of it than they may", { timeout: 60_000 }, async () => {
  const schema = newSchema();
  // A pool of three: as many lock holders at once as chats would take every connection, and leave their reads none.
  const pool = new pg.Pool({ connectionString: await testDatabaseUrl(), max: 3 });
  try {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool, schema })));
    const sent: string[] = [];
    const telegram = botApiLayer(async (call) => {
      await sleep(30);
      sent.push((call.params as { readonly text?: string }).text ?? "");
      return { message_id: 500, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
    });
    const chats = [101, 102, 103, 104, 105, 106];
    const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const outbox = Context.get(yield* Layer.build(layer), Outbox);
      for (const chatId of chats) yield* outbox.enqueueText({ chatId, text: `restarting in ${chatId}`, options: { format: "plain" } });
      // Each as status.restarting delivers it, all at once, each within the bound it is given.
      yield* Effect.all(chats.map((chatId) => outbox.deliverChat(chatId).pipe(Effect.timeout("10 seconds"))), { concurrency: "unbounded", discard: true });
    })));
    assert.deepEqual(sent.toSorted(), chats.map((chatId) => `restarting in ${chatId}`).toSorted());
  } finally {
    await pool.end();
  }
});

test("a status message Telegram refuses for good is given up on, its edits with it, and the chat's reply after it is sent", async () => {
  const asked = await withOutbox((method, text) => (method === "sendMessage" && text === "Codex is working" ? 400 : null), (outbox, store) =>
    Effect.gen(function*() {
      const status = yield* outbox.enqueueStatus({ chatId: 42, text: "Codex is working" });
      yield* outbox.enqueueEdit(status, { chatId: 42, text: "Codex worked for a moment." });
      yield* outbox.enqueueText({ chatId: 42, text: "Done", options: { format: "plain" } });
      yield* deliverAll(outbox, store);
      assert.equal(yield* store.getPendingOutboxCount, 0);
    }));
  assert.deepEqual(asked.map(({ method, text }) => [method, text]), [
    ["sendMessage", "Codex is working"],
    ["sendMessage", "Done"],
  ]);
});

test("an entry being delivered as alasio stops is delivered whole and marked sent, so it is not sent again; what is queued after it waits for the next alasio", async () => {
  const store = await testStore();
  const reached = Effect.runSync(Deferred.make<void>());
  const answered = Effect.runSync(Deferred.make<void>());
  const asked: string[] = [];
  const telegram = botApiLayer(async (call) => {
    const text = (call.params as { readonly text?: string }).text ?? "";
    asked.push(text);
    if (text === "first") {
      Effect.runSync(Deferred.succeed(reached, undefined));
      await Effect.runPromise(Deferred.await(answered));
    }
    return { message_id: 500, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
  await Effect.runPromise(Effect.gen(function*() {
    const scope = yield* Scope.make();
    const outbox = yield* Layer.buildWithScope(layer, scope).pipe(Effect.map(Context.get(Outbox)));
    yield* outbox.enqueueText({ chatId: 42, text: "first", options: { format: "plain" } });
    yield* outbox.enqueueText({ chatId: 42, text: "second", options: { format: "plain" } });
    yield* Deferred.await(reached);
    // alasio stops while Telegram has the first: its stopping waits for that delivery.
    const stopping = yield* Effect.forkChild(Scope.close(scope, Exit.void));
    yield* Deferred.succeed(answered, undefined);
    yield* Fiber.join(stopping);
    assert.equal(yield* store.getPendingOutboxCount, 1);
    // The next alasio delivers what was left, and the first no more.
    yield* Effect.scoped(Effect.flatMap(Layer.build(layer), (context) => deliverAll(Context.get(context, Outbox), store)));
    assert.equal(yield* store.getPendingOutboxCount, 0);
  }));
  assert.deepEqual(asked, ["first", "second"]);
});

test("a reply Telegram rate-limits on a part after its first, as alasio stops, is sent on to its end, not again from its start", async () => {
  const store = await testStore();
  const limited = Effect.runSync(Deferred.make<void>());
  const asked: string[] = [];
  let refusals = 0;
  const telegram = botApiLayer(async (call) => {
    const text = (call.params as { readonly text?: string }).text ?? "";
    asked.push(text.slice(0, 6));
    // Its second part waits a second, the first time.
    if (text.startsWith("second") && refusals++ === 0) {
      Effect.runSync(Deferred.succeed(limited, undefined));
      throw botApiError("Too Many Requests: retry after 1", 429, 1);
    }
    return { message_id: 500, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
  });
  const layer = Outbox.layer.pipe(Layer.provideMerge(Layer.mergeAll(telegram, Layer.succeed(Store, store))));
  // Two parts: the text is longer than a message takes.
  const reply = `first${"a".repeat(4090)}\nsecond${"b".repeat(100)}`;
  await Effect.runPromise(Effect.gen(function*() {
    const scope = yield* Scope.make();
    const outbox = yield* Layer.buildWithScope(layer, scope).pipe(Effect.map(Context.get(Outbox)));
    yield* outbox.enqueueText({ chatId: 42, text: reply, options: { format: "plain" } });
    yield* Deferred.await(limited);
    // alasio stops as the second part waits out its limit: its stopping waits for the rest of the reply.
    yield* Scope.close(scope, Exit.void);
    assert.equal(yield* store.getPendingOutboxCount, 0);
  }));
  assert.deepEqual(asked, ["firsta", "second", "second"]);
});
