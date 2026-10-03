import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { v2 } from "../.types/codex/index.js";
import { TurnController } from "../src/codex/turn-controller.ts";
import type { GoalUpdate, HarnessGoals } from "../src/harness/index.ts";
import {
  type GoalControlStore,
  type GoalPanelTarget,
  type GoalTurnRequest,
  buildGoalPanel,
  buildNoActiveGoalPanel,
  buildNoMountedGoalPanel,
  buildReplaceGoalPanel,
  handleGoalControlCallback,
  handleGoalTextCommand,
} from "../src/operator/goal-control.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import type { Client } from "../src/telegram/client.ts";

/** A thread goal as Codex reports it, with what a test does not care about filled in. */
function threadGoal(fields: Partial<v2.ThreadGoal>): v2.ThreadGoal {
  return {
    threadId: "session-1",
    objective: "Goal",
    status: "active",
    tokenBudget: null,
    tokensUsed: 0,
    timeUsedSeconds: 0,
    createdAt: 0,
    updatedAt: 0,
    ...fields,
  };
}

function createStore(): GoalPanelTarget["store"] {
  return {
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createClient() {
  const calls: {
    sendMessage: Parameters<Client["sendMessage"]>[];
    editMessageText: Parameters<Client["editMessageText"]>[];
    answerCallbackQuery: Parameters<Client["answerCallbackQuery"]>[];
  } = {
    sendMessage: [],
    editMessageText: [],
    answerCallbackQuery: [],
  };
  const client: Pick<Client, "sendMessage" | "editMessageText" | "answerCallbackQuery" | "deleteMessage"> = {
    async sendMessage(...args) {
      calls.sendMessage.push(args);
      return [];
    },
    async editMessageText(...args) {
      calls.editMessageText.push(args);
      return true;
    },
    async answerCallbackQuery(...args) {
      calls.answerCallbackQuery.push(args);
      return true;
    },
    deleteMessage: () => assert.fail("deleteMessage"),
  };
  return { calls, client };
}

interface FakeGoalOptions {
  readonly currentGoal?: v2.ThreadGoal | null;
  readonly updatedGoal?: v2.ThreadGoal;
  readonly waitTurnId?: string | null;
}

function createGoalApi({ currentGoal = null, updatedGoal, waitTurnId = null }: FakeGoalOptions = {}) {
  const calls: {
    read: { readonly threadId: string }[];
    set: GoalUpdate[];
    clear: { readonly threadId: string }[];
    waitForTurnId: string[];
  } = {
    read: [],
    set: [],
    clear: [],
    waitForTurnId: [],
  };
  const events: string[] = [];
  const goalApi: HarnessGoals = {
    async read(args) {
      calls.read.push(args);
      events.push("read");
      return currentGoal;
    },
    async set(args) {
      calls.set.push(args);
      events.push("set");
      return updatedGoal ?? threadGoal({
        objective: args.objective ?? currentGoal?.objective ?? "Goal",
        status: args.status ?? currentGoal?.status ?? "active",
        tokensUsed: 0,
      });
    },
    async clear(args) {
      calls.clear.push(args);
      events.push("clear");
      return { cleared: true };
    },
    async waitForTurnId(sessionId) {
      calls.waitForTurnId.push(sessionId);
      events.push("waitForTurnId");
      return waitTurnId;
    },
  };
  return { calls, events, goalApi };
}

function createMountedStore(sessionId = "session-1"): GoalControlStore {
  return {
    getSessionId: () => sessionId,
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createUnmountedStore(): GoalControlStore & Pick<SqliteStore, "setSessionId"> {
  let sessionId: string | undefined;
  return {
    getSessionId: () => sessionId,
    setSessionId: (_conversationId, mountedSessionId) => {
      sessionId = mountedSessionId ?? undefined;
    },
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}
test("goal panel renders no-mounted empty state", () => {
  const panel = buildNoMountedGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
  });

  assert.match(panel.text, /^Goal\n\nNo Codex session is mounted\./);
  assert.deepEqual(
    panel.options.reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Sessions"], ["New Session"], ["Close"]],
  );
});

test("goal panel renders no-active empty state", () => {
  const panel = buildNoActiveGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    sessionId: "12345678-aaaa-bbbb-cccc-123456789abc",
  });

  assert.match(panel.text, /No active goal is set for this session\./);
  assert.match(panel.text, /Session: 12345678/);
  assert.match(panel.text, /\/goal <objective>/);
  assert.deepEqual(
    panel.options.reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Close"]],
  );
});

test("goal panel renders active goal controls", () => {
  const panel = buildGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    goal: threadGoal({
      objective: "Ship a clean Telegram /goal surface",
      status: "active",
      tokenBudget: 500000,
      tokensUsed: 184000,
      timeUsedSeconds: 8040,
    }),
  });

  assert.match(panel.text, /Ship a clean Telegram \/goal surface/);
  assert.match(panel.text, /Status: active/);
  assert.match(panel.text, /Time: 2h 14m/);
  assert.match(panel.text, /Tokens: 184K \/ 500K/);
  assert.deepEqual(
    panel.options.reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Pause", "Clear"], ["Close"]],
  );
});

test("goal panel can distinguish active goal state from active turn state", () => {
  const panel = buildGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    turnState: "working",
    goal: threadGoal({
      objective: "Keep working",
      status: "active",
      tokenBudget: null,
      tokensUsed: 10,
      timeUsedSeconds: 0,
    }),
  });

  assert.match(panel.text, /Status: active/);
  assert.match(panel.text, /Turn: working/);
});

test("goal panel renders inactive unfinished goal controls", () => {
  const panel = buildGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    goal: threadGoal({
      objective: "Paused objective",
      status: "paused",
      tokenBudget: null,
      tokensUsed: 1200,
      timeUsedSeconds: 0,
    }),
  });

  assert.match(panel.text, /Status: paused/);
  assert.match(panel.text, /Tokens: 1.2K/);
  assert.deepEqual(
    panel.options.reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Resume", "Clear"], ["Close"]],
  );
});

test("replace panel requires explicit confirmation", () => {
  const panel = buildReplaceGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    currentGoal: threadGoal({
      objective: "Current objective",
    }),
    objective: "New objective",
  });

  assert.match(panel.text, /^Replace goal\?/);
  assert.match(panel.text, /Current objective/);
  assert.match(panel.text, /New objective/);
  assert.deepEqual(
    panel.options.reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Replace Goal"], ["Keep Current Goal"], ["Close"]],
  );
});

test("goal text command starts a fallback turn when upstream does not create one", async () => {
  const { calls, client } = createClient();
  const goal = createGoalApi({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalTextCommand({
    client,
    store: createMountedStore(),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi: goal.goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.waitForTurnId.length, 1);
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.turnId, null);
  assert.match(runCalls[0]?.prompt ?? "", /Continue working toward this Codex goal\./);
  assert.match(runCalls[0]?.prompt ?? "", /Refactor CI/);
  assert.equal(calls.sendMessage.length, 0);
});

test("goal text command bootstraps a fresh session when none is mounted", async () => {
  const { calls, client } = createClient();
  const store = createUnmountedStore();
  const goal = createGoalApi({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const startCalls: { readonly conversationId: string }[] = [];
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalTextCommand({
    client,
    store,
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi: goal.goalApi,
    startNewSession: async (args) => {
      startCalls.push(args);
      store.setSessionId(args.conversationId, "fresh-session");
      return "fresh-session";
    },
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.deepEqual(startCalls, [{ conversationId: "conversation-1" }]);
  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.set[0]?.threadId, "fresh-session");
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.sessionId, "fresh-session");
  assert.equal(calls.sendMessage.length, 0);
});

test("goal text command clears completed goal state before setting a new objective", async () => {
  const { client } = createClient();
  const goal = createGoalApi({
    currentGoal: threadGoal({ objective: "Old goal", status: "complete", tokensUsed: 1200 }),
    updatedGoal: threadGoal({ objective: "New goal", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalTextCommand({
    client,
    store: createMountedStore("session-1"),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "New goal",
    goalApi: goal.goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goal.calls.clear.length, 1);
  assert.equal(goal.calls.clear[0]?.threadId, "session-1");
  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.set[0]?.objective, "New goal");
  assert.ok(goal.events.indexOf("clear") < goal.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});

test("goal replace callback clears stale goal state before setting replacement", async () => {
  const { client } = createClient();
  const goal = createGoalApi({
    updatedGoal: threadGoal({ objective: "New goal", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalControlCallback({
    client,
    store: createMountedStore("session-1"),
    action: { id: "action-1", kind: "goal:replace", conversationId: "conversation-1", payload: { objective: "New goal" } },
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
    goalApi: goal.goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goal.calls.clear.length, 1);
  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.set[0]?.objective, "New goal");
  assert.ok(goal.events.indexOf("clear") < goal.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});
test("goal text command attaches to an upstream-created goal turn when present", async () => {
  const { client } = createClient();
  const goal = createGoalApi({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: "goal-turn",
  });
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalTextCommand({
    client,
    store: createMountedStore("session-1"),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi: goal.goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.sessionId, "session-1");
  assert.equal(runCalls[0]?.turnId, "goal-turn");
});

test("goal resume callback edits to starting before fallback turn execution", async () => {
  const { calls, client } = createClient();
  const goal = createGoalApi({
    currentGoal: threadGoal({ objective: "Refactor CI", status: "paused", tokensUsed: 0 }),
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];

  await handleGoalControlCallback({
    client,
    store: createMountedStore("session-1"),
    action: { id: "action-1", kind: "goal:resume", conversationId: "conversation-1", payload: {} },
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
    goalApi: goal.goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "Starting."]);
  assert.match(calls.editMessageText[0]?.[2] ?? "", /Turn: starting/);
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.turnId, null);
});

test("goal turns use normal concurrent-message decision panel when Codex is already working", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-goal-control-"));
  const store = new SqliteStore(root);
  try {
    const conversationId = store.upsertConversation({ chatId: "123", user: { id: 123 } });
    store.setActiveHarness(conversationId, "codex");
    const { calls, client } = createClient();
    const turns = new TurnController({
      config: { workspaceRoot: root, workingDirectory: "/repo" },
      client,
      store,
      outbox: { enqueueText: () => "outbox-1" },
      activeQueries: new Map([[conversationId, { abort: async () => undefined, steer: async () => true }]]),
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      isStopping: () => false,
    });

    const handled = await turns.runGoalTurn({
      conversationId,
      chatId: 123,
      messageId: 456,
      sessionId: "session-1",
      turnId: null,
      prompt: "Continue working toward this Codex goal.\n\nRefactor CI",
    });

    assert.equal(handled, true);
    assert.match(calls.sendMessage[0]?.[1] ?? "", /Codex is currently working/);
    assert.deepEqual(
      calls.sendMessage[0]?.[2]?.reply_markup?.inline_keyboard.map((row) => row.map((button) => button.text)),
      [["Steer", "Queue"], ["Swerve", "Discard"]],
    );
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});
