import assert from "node:assert/strict";
import test from "node:test";
import { Authorizer } from "../src/telegram/authorizer.js";
import { CallbackHandler } from "../src/telegram/callback-handler.js";

const silentLog = { info() {}, warn() {}, error() {} };

function createStateStore() {
  const state = new Map();
  return {
    getState: (key) => state.get(key) ?? null,
    setState: (key, value) => state.set(key, value),
  };
}

function privateMessage(userId, chatId = userId) {
  return {
    from: { id: userId },
    chat: { id: chatId, type: "private" },
  };
}

function privateCallback(userId, chatId = userId) {
  return {
    id: "callback-1",
    from: { id: userId },
    message: {
      message_id: 10,
      chat: { id: chatId, type: "private" },
    },
    data: "action-1",
  };
}

test("explicit allowlist owns private messages and callback queries", () => {
  const authorizer = new Authorizer({
    allowedUserIds: "123",
    store: createStateStore(),
    log: silentLog,
  });

  assert.equal(authorizer.isAuthorizedMessage(privateMessage(123)), true);
  assert.equal(authorizer.isAuthorizedMessage(privateMessage(456)), false);
  assert.equal(authorizer.isAuthorizedMessage(privateMessage(123, 456)), false);
  assert.equal(authorizer.isAuthorizedMessage({ from: { id: 123 }, chat: { id: -1, type: "group" } }), false);
  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(123)), true);
  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(456)), false);
  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(123, 456)), false);
});

test("callback queries cannot claim an empty bootstrap allowlist", () => {
  const store = createStateStore();
  const authorizer = new Authorizer({ allowedUserIds: "", store, log: silentLog });

  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(123)), false);
  assert.equal(store.getState("telegram_bootstrap_user_id"), null);
  assert.equal(authorizer.isAuthorizedMessage(privateMessage(123)), true);
  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(123)), true);
  assert.equal(authorizer.isAuthorizedCallbackQuery(privateCallback(456)), false);
});

test("unauthorized callback is rejected before consuming its action", async () => {
  let consumed = false;
  const answers = [];
  const handler = new CallbackHandler({
    authorizer: { isAuthorizedCallbackQuery: () => false },
    client: { answerCallbackQuery: async (...args) => answers.push(args) },
    config: {},
    store: { consumeCallbackAction: () => { consumed = true; } },
    turns: {},
    activeQueries: new Map(),
  });

  await handler.handle(privateCallback(456));

  assert.equal(consumed, false);
  assert.deepEqual(answers, [["callback-1", "This action is not authorized for this Telegram user."]]);
});
