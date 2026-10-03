import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import type { CallbackQuery, Message } from "@grammyjs/types";
import { Effect, Layer } from "effect";

import { SqliteStore, Store } from "../src/persistence/store.ts";
import { Authorizer } from "../src/telegram/authorizer.ts";
import { handleCallbackQuery } from "../src/telegram/callback-handler.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

async function withStore(run: (store: SqliteStore) => Promise<void>): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "alasio-authorization-"));
  const store = new SqliteStore(root);
  try {
    await run(store);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

/** The Authorizer for `allowedUserIds`, keeping its operator in `store`. */
function authorizerOf(allowedUserIds: string, store: SqliteStore): Authorizer["Service"] {
  return Effect.runSync(Effect.provide(Authorizer, Authorizer.layer(allowedUserIds).pipe(Layer.provide(Layer.succeed(Store, store)))));
}

function privateMessage(userId: number, chatId = userId): Message {
  return {
    message_id: 1,
    date: 0,
    from: { id: userId, is_bot: false, first_name: "Operator" },
    chat: { id: chatId, type: "private", first_name: "Operator" },
  };
}

function privateCallback(userId: number, chatId = userId, data = "action-1"): CallbackQuery {
  return {
    id: "callback-1",
    chat_instance: "chat-instance-1",
    from: { id: userId, is_bot: false, first_name: "Operator" },
    message: {
      message_id: 10,
      date: 0,
      chat: { id: chatId, type: "private", first_name: "Operator" },
    },
    data,
  };
}

test("explicit allowlist owns private messages and callback queries", async () => {
  await withStore(async (store) => {
    const authorizer = authorizerOf("123", store);
    const message = (m: Message) => Effect.runSync(authorizer.isAuthorizedMessage(m));
    const press = (q: CallbackQuery) => Effect.runSync(authorizer.isAuthorizedCallbackQuery(q));

    assert.equal(message(privateMessage(123)), true);
    assert.equal(message(privateMessage(456)), false);
    assert.equal(message(privateMessage(123, 456)), false);
    assert.equal(message({
      message_id: 1,
      date: 0,
      from: { id: 123, is_bot: false, first_name: "Operator" },
      chat: { id: -1, type: "group", title: "Group" },
    }), false);
    assert.equal(press(privateCallback(123)), true);
    assert.equal(press(privateCallback(456)), false);
    assert.equal(press(privateCallback(123, 456)), false);
  });
});

test("callback queries cannot claim an empty bootstrap allowlist", async () => {
  await withStore(async (store) => {
    const authorizer = authorizerOf("", store);
    const message = (m: Message) => Effect.runSync(authorizer.isAuthorizedMessage(m));
    const press = (q: CallbackQuery) => Effect.runSync(authorizer.isAuthorizedCallbackQuery(q));

    assert.equal(press(privateCallback(123)), false);
    assert.equal(store.getState("telegram_bootstrap_user_id"), null);
    assert.equal(message(privateMessage(123)), true);
    assert.equal(press(privateCallback(123)), true);
    assert.equal(press(privateCallback(456)), false);
  });
});

test("unauthorized callback is rejected before consuming its action", async () => {
  await withStore(async (store) => {
    const conversationId = store.upsertConversation({ chatId: "456", user: { id: 456 } });
    const actionId = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
    const telegram = recordingTelegram();

    await withServices({ store, telegram: telegram.layer, allowedUserIds: "123" }, (alasio) =>
      alasio.runPromise(handleCallbackQuery(privateCallback(456, 456, actionId))));

    // The action is still there to be pressed by someone who may.
    assert.notEqual(store.consumeCallbackAction(actionId), null);
    assert.deepEqual(telegram.calls.answerCallbackQuery, [["callback-1", "This action is not authorized for this Telegram user."]]);
    assert.equal(telegram.calls.editMessageText.length + telegram.calls.sendMessage.length + telegram.calls.deleteMessage.length, 0);
  });
});
