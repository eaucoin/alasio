import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import type { CallbackQuery, Message } from "@grammyjs/types";

import { noActiveTurns } from "../src/harness/active-turns.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import type { Logger } from "../src/shared/log.ts";
import { Authorizer } from "../src/telegram/authorizer.ts";
import { type CallbackClient, CallbackHandler, type CallbackTurns } from "../src/telegram/callback-handler.ts";

const silentLog: Logger = { info() {}, warn() {}, error() {} };

function createStateStore(): Pick<SqliteStore, "getState" | "setState"> {
  const state = new Map<string, string>();
  return {
    getState: (key) => state.get(key) ?? null,
    setState: (key, value) => state.set(key, String(value)),
  };
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

test("explicit allowlist owns private messages and callback queries", () => {
  const authorizer = new Authorizer({
    allowedUserIds: "123",
    store: createStateStore(),
    log: silentLog,
  });

  assert.equal(authorizer.isAuthorizedMessage(privateMessage(123)), true);
  assert.equal(authorizer.isAuthorizedMessage(privateMessage(456)), false);
  assert.equal(authorizer.isAuthorizedMessage(privateMessage(123, 456)), false);
  assert.equal(authorizer.isAuthorizedMessage({
    message_id: 1,
    date: 0,
    from: { id: 123, is_bot: false, first_name: "Operator" },
    chat: { id: -1, type: "group", title: "Group" },
  }), false);
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

// An unauthorized press is answered before the handler reaches any turn.
const unreachableTurns: CallbackTurns = {
  harnessFor: () => assert.fail("harnessFor"),
  switchHarness: () => assert.fail("switchHarness"),
  switchWorkspace: () => assert.fail("switchWorkspace"),
  createSessionWorkspace: () => assert.fail("createSessionWorkspace"),
  sandboxEnabled: false,
  startNewSession: () => assert.fail("startNewSession"),
  setPromptDisposition: () => assert.fail("setPromptDisposition"),
  enqueueMessage: () => assert.fail("enqueueMessage"),
  runGoalTurn: () => assert.fail("runGoalTurn"),
  scheduleConversation: () => assert.fail("scheduleConversation"),
};

test("unauthorized callback is rejected before consuming its action", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-authorization-"));
  const store = new SqliteStore(root);
  try {
    const conversationId = store.upsertConversation({ chatId: "456", user: { id: 456 } });
    const actionId = store.createCallbackAction({ conversationId, kind: "queue", payload: { prompt: "later" } });
    const answers: Parameters<CallbackClient["answerCallbackQuery"]>[] = [];
    const handler = new CallbackHandler({
      authorizer: { isAuthorizedCallbackQuery: () => false },
      client: {
        answerCallbackQuery: async (...args) => {
          answers.push(args);
          return true;
        },
        editMessageText: () => assert.fail("editMessageText"),
        deleteMessage: () => assert.fail("deleteMessage"),
        sendMessage: () => assert.fail("sendMessage"),
      },
      config: { workspaceRoot: root },
      store,
      turns: unreachableTurns,
      activeTurns: noActiveTurns,
    });

    await handler.handle(privateCallback(456, 456, actionId));

    // The action is still there to be pressed by someone who may.
    assert.notEqual(store.consumeCallbackAction(actionId), null);
    assert.deepEqual(answers, [["callback-1", "This action is not authorized for this Telegram user."]]);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});
