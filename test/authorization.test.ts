import assert from "node:assert/strict";
import test from "node:test";

import type { CallbackQuery, Message } from "@grammyjs/types";
import { Effect } from "effect";

import { Authorizer } from "../src/telegram/authorizer.ts";
import { handleCallbackQuery } from "../src/telegram/callback-handler.ts";
import { run, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { withServices } from "./support/turns.ts";

/** The Authorizer for `allowedUserIds`. */
function authorizerOf(allowedUserIds: string): Authorizer["Service"] {
  return Effect.runSync(Effect.provide(Authorizer, Authorizer.layer(allowedUserIds)));
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
  const authorizer = authorizerOf("123");
  const message = (m: Message) => run(authorizer.isAuthorizedMessage(m));
  const press = (q: CallbackQuery) => run(authorizer.isAuthorizedCallbackQuery(q));

  assert.equal(await message(privateMessage(123)), true);
  assert.equal(await message(privateMessage(456)), false);
  assert.equal(await message(privateMessage(123, 456)), false);
  assert.equal(await message({
    message_id: 1,
    date: 0,
    from: { id: 123, is_bot: false, first_name: "Operator" },
    chat: { id: -1, type: "group", title: "Group" },
  }), false);
  assert.equal(await press(privateCallback(123)), true);
  assert.equal(await press(privateCallback(456)), false);
  assert.equal(await press(privateCallback(123, 456)), false);
});

test("with no user allowed, no one is", async () => {
  const authorizer = authorizerOf("");
  assert.equal(await run(authorizer.isAuthorizedMessage(privateMessage(123))), false);
  assert.equal(await run(authorizer.isAuthorizedCallbackQuery(privateCallback(123))), false);
});

test("unauthorized callback is rejected before consuming its action", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "456", user: { id: 456 } }));
  const [actionId = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "queue", payload: { prompt: "later" } }]));
  const telegram = recordingTelegram();

  await withServices({ store, telegram: telegram.layer, allowedUserIds: "123" }, (alasio) =>
    alasio.runPromise(handleCallbackQuery(privateCallback(456, 456, actionId))));

  // The action is still there to be pressed by someone who may.
  assert.notEqual(await run(store.consumeCallbackAction(actionId)), null);
  assert.deepEqual(telegram.calls.answerCallbackQuery, [["callback-1", "This action is not authorized for this Telegram user."]]);
  assert.equal(telegram.calls.editMessageText.length + telegram.calls.sendMessage.length + telegram.calls.deleteMessage.length, 0);
});
