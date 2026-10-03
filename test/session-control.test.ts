import assert from "node:assert/strict";
import { test } from "node:test";

import { noActiveTurns } from "../src/harness/active-turns.ts";
import type { HarnessSessionsFacade } from "../src/harness/index.ts";
import { parseCommand } from "../src/operator/command-parser.ts";
import {
  type SessionControlCallback,
  type SessionControlHarness,
  type SessionControlStore,
  type StartNewSession,
  handleSessionControlCallback,
} from "../src/operator/session-control.ts";
import type { Client } from "../src/telegram/client.ts";

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
  const calls: {
    answerCallbackQuery: Parameters<Client["answerCallbackQuery"]>[];
    editMessageText: Parameters<Client["editMessageText"]>[];
  } = {
    answerCallbackQuery: [],
    editMessageText: [],
  };
  const client: SessionControlCallback["client"] = {
    async answerCallbackQuery(...args) {
      calls.answerCallbackQuery.push(args);
      return true;
    },
    async editMessageText(...args) {
      calls.editMessageText.push(args);
      return true;
    },
    deleteMessage: () => assert.fail("deleteMessage"),
  };
  return { calls, client };
}

function createStore(): SessionControlStore {
  let sessionId: string | undefined;
  return {
    getSessionId: () => sessionId,
    setSessionId: (_conversationId, mountedSessionId) => {
      sessionId = mountedSessionId ?? undefined;
    },
    getSessionTokens: () => 0,
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createHarness(sessions: Partial<HarnessSessionsFacade> = {}): SessionControlHarness {
  return {
    displayName: "Codex",
    sessions: {
      listSessions: () => assert.fail("listSessions"),
      getTotalSessionPages: () => assert.fail("getTotalSessionPages"),
      getSessionByNumber: () => assert.fail("getSessionByNumber"),
      getSessionLastMessage: async () => null,
      listSessionMessages: () => assert.fail("listSessionMessages"),
      getTotalRewindPages: () => assert.fail("getTotalRewindPages"),
      createForkedSession: () => assert.fail("createForkedSession"),
      ...sessions,
    },
  };
}

test("new-session callback starts and mounts a fresh session", async () => {
  const { calls, client } = createClient();
  const store = createStore();
  const startCalls: Parameters<StartNewSession>[0][] = [];

  await handleSessionControlCallback({
    client,
    store,
    harness: createHarness(),
    activeTurns: noActiveTurns,
    action: { id: "action-1", kind: "control:new", conversationId: "conversation-1", payload: {} },
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
  assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "New session mounted: fresh-se."]);
  assert.match(calls.editMessageText[0]?.[2] ?? "", /Session: fresh-se/);
});

test("rewind forks before the chosen message for the conversation and mounts the fork", async () => {
  const { calls, client } = createClient();
  const store = createStore();
  const forkCalls: Parameters<HarnessSessionsFacade["createForkedSession"]>[] = [];
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
    activeTurns: noActiveTurns,
    action: { id: "action-1", kind: "control:rewind_fork", conversationId: "conversation-1", payload: { sessionId: "source-session", index: -1 } },
    startNewSession: async () => assert.fail("a rewind starts no new session"),
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
  });

  assert.deepEqual(forkCalls, [["source-session", "turn-2", { threadKey: "conversation-1" }]]);
  assert.equal(store.getSessionId("conversation-1"), "forked-session");
  assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "Fork mounted."]);
});
