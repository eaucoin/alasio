import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect } from "effect";

import type { Turns } from "../src/codex/turn-controller.ts";
import type { HarnessSessions } from "../src/harness/index.ts";
import { parseCommand } from "../src/operator/command-parser.ts";
import { type SessionControlHarness, handleSessionControlCallback } from "../src/operator/session-control.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { type TestAlasio, turnsStub, withServices } from "./support/turns.ts";

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

/** The conversation of chat 123, on Codex. */
const CONVERSATION = "telegram:123";

/** The session controls' services over a Codex conversation, the turns they run (made on its store) standing in, for one test. */
async function withSessionControls<T>(
  turns: (store: SqliteStore) => Partial<Turns["Service"]>,
  use: (alasio: TestAlasio, telegram: RecordingTelegram, store: SqliteStore) => Promise<T>,
): Promise<T> {
  const root = mkdtempSync(join(tmpdir(), "alasio-session-control-"));
  const store = new SqliteStore(root);
  try {
    store.setActiveHarness(store.upsertConversation({ chatId: "123", user: { id: 123 } }), "codex");
    const telegram = recordingTelegram();
    return await withServices({ store, telegram: telegram.layer, turns: turnsStub(turns(store)) }, (alasio) => use(alasio, telegram, store));
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
}

function createHarness(sessions: Partial<HarnessSessions> = {}): SessionControlHarness {
  return {
    displayName: "Codex",
    sessions: {
      listSessions: () => assert.fail("listSessions"),
      getTotalSessionPages: () => assert.fail("getTotalSessionPages"),
      getSessionByNumber: () => assert.fail("getSessionByNumber"),
      getSessionLastMessage: () => Effect.succeed(null),
      listSessionMessages: () => assert.fail("listSessionMessages"),
      getTotalRewindPages: () => assert.fail("getTotalRewindPages"),
      createForkedSession: () => assert.fail("createForkedSession"),
      ...sessions,
    },
  };
}

test("new-session callback starts and mounts a fresh session", async () => {
  const startCalls: string[] = [];
  await withSessionControls((store) => ({
    startNewSession: (conversationId) =>
      Effect.sync(() => {
        startCalls.push(conversationId);
        store.setSessionId(conversationId, "fresh-session");
        return "fresh-session";
      }),
  }), async (alasio, { calls }) => {
    await alasio.runPromise(handleSessionControlCallback({
      harness: createHarness(),
      action: { id: "action-1", kind: "control:new", conversationId: CONVERSATION, payload: {} },
      callbackQueryId: "callback-1",
      chatId: 123,
      messageId: 456,
    }));
    assert.deepEqual(startCalls, [CONVERSATION]);
    assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "New session mounted: fresh-se."]);
    assert.match(calls.editMessageText[0]?.[2] ?? "", /Session: fresh-se/);
  });
});

test("rewind forks before the chosen message for the conversation and mounts the fork", async () => {
  const forkCalls: Parameters<HarnessSessions["createForkedSession"]>[] = [];
  const harness = createHarness({
    listSessionMessages: () =>
      Effect.succeed([
        { index: -1, timestamp: "", text: "second", uuid: "turn-2" },
        { index: -2, timestamp: "", text: "first", uuid: "turn-1" },
      ]),
    createForkedSession: (...args) =>
      Effect.sync(() => {
        forkCalls.push(args);
        return "forked-session";
      }),
  });
  await withSessionControls(() => ({}), async (alasio, { calls }, store) => {
    await alasio.runPromise(handleSessionControlCallback({
      harness,
      action: { id: "action-1", kind: "control:rewind_fork", conversationId: CONVERSATION, payload: { sessionId: "source-session", index: -1 } },
      callbackQueryId: "callback-1",
      chatId: 123,
      messageId: 456,
    }));
    assert.deepEqual(forkCalls, [["source-session", "turn-2", { threadKey: CONVERSATION }]]);
    assert.equal(store.getSessionId(CONVERSATION), "forked-session");
    assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "Fork mounted."]);
  });
});
