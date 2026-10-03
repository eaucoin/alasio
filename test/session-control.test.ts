// @ts-nocheck
import assert from "node:assert/strict";
import { test } from "node:test";

import { parseCommand } from "../src/operator/command-parser.ts";
import { handleSessionControlCallback } from "../src/operator/session-control.ts";

test("command parser accepts Telegram-native session commands", () => {
  assert.deepEqual(parseCommand("/session"), { type: "session_panel" });
  assert.deepEqual(parseCommand("/session@AlasioBot"), { type: "session_panel" });
  assert.deepEqual(parseCommand("/sessions"), { type: "sessions_panel" });
  assert.deepEqual(parseCommand("/sessions@AlasioBot"), { type: "sessions_panel" });
  assert.deepEqual(parseCommand("/sessions page 2"), { type: "sessions", page: 2 });
  assert.deepEqual(parseCommand("/sessions new"), { type: "sessions_new" });
  assert.deepEqual(parseCommand("/goal"), { type: "goal", args: "" });
  assert.deepEqual(parseCommand("/goal@AlasioBot pause"), { type: "goal", args: "pause" });
  assert.deepEqual(parseCommand("/goal refactor Alasio carefully"), { type: "goal", args: "refactor Alasio carefully" });
  assert.deepEqual(parseCommand("/stop"), { type: "stop" });
});

test("command parser accepts bang session command aliases", () => {
  assert.deepEqual(parseCommand("!sessions"), { type: "sessions", page: 1 });
  assert.deepEqual(parseCommand("!sessions page 3"), { type: "sessions", page: 3 });
  assert.deepEqual(parseCommand("!resume 2 follow up"), { type: "resume", ref: "2", followUp: "follow up" });
  assert.deepEqual(parseCommand("!sessions new"), { type: "sessions_new" });
  assert.deepEqual(parseCommand("!stop"), { type: "stop" });
});

function createClient() {
  const calls = {
    answerCallbackQuery: [],
    editMessageText: [],
  };
  return {
    calls,
    async answerCallbackQuery(...args) {
      calls.answerCallbackQuery.push(args);
    },
    async editMessageText(...args) {
      calls.editMessageText.push(args);
    },
  };
}

function createStore() {
  let sessionId = null;
  return {
    getSessionId: () => sessionId,
    setSessionId: (_conversationId, mountedSessionId) => {
      sessionId = mountedSessionId;
    },
    getSessionTokens: () => null,
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createHarness(sessions = {}) {
  return {
    displayName: "Codex",
    sessions: {
      getSessionLastMessage: async () => null,
      ...sessions,
    },
  };
}

test("new-session callback starts and mounts a fresh session", async () => {
  const client = createClient();
  const store = createStore();
  const startCalls = [];

  await handleSessionControlCallback({
    client,
    store,
    harness: createHarness(),
    activeQueries: new Map(),
    action: { kind: "control:new", conversationId: "conversation-1", payload: {} },
    startNewSession: async (args) => {
      startCalls.push(args);
      store.setSessionId(args.conversationId, "fresh-session");
      return "fresh-session";
    },
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
  });

  assert.deepEqual(startCalls, [{ conversationId: "conversation-1" }]);
  assert.deepEqual(client.calls.answerCallbackQuery[0], ["callback-1", "New session mounted: fresh-se."]);
  assert.match(client.calls.editMessageText[0][2], /Session: fresh-se/);
});

test("rewind forks before the chosen message for the conversation and mounts the fork", async () => {
  const client = createClient();
  const store = createStore();
  const forkCalls = [];
  const harness = createHarness({
    listSessionMessages: async () => [
      { index: -1, timestamp: "", text: "second", uuid: "turn-2" },
      { index: -2, timestamp: "", text: "first", uuid: "turn-1" },
    ],
    createForkedSession: async (...args) => {
      forkCalls.push(args);
      return "forked-session";
    },
  });

  await handleSessionControlCallback({
    client,
    store,
    harness,
    activeQueries: new Map(),
    action: { kind: "control:rewind_fork", conversationId: "conversation-1", payload: { sessionId: "source-session", index: -1 } },
    startNewSession: async () => assert.fail("a rewind starts no new session"),
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
  });

  assert.deepEqual(forkCalls, [["source-session", "turn-2", { threadKey: "conversation-1" }]]);
  assert.equal(store.getSessionId("conversation-1"), "forked-session");
  assert.deepEqual(client.calls.answerCallbackQuery[0], ["callback-1", "Fork mounted."]);
});
