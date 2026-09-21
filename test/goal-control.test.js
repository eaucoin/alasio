import assert from "node:assert/strict";
import { test } from "node:test";

import {
  buildGoalPanel,
  buildNoActiveGoalPanel,
  buildNoMountedGoalPanel,
  buildReplaceGoalPanel,
  handleGoalControlCallback,
  handleGoalTextCommand,
} from "../src/operator/goal-control.js";
import { TurnController } from "../src/codex/turn-controller.js";

function createStore() {
  return {
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createClient() {
  const calls = {
    sendMessage: [],
    editMessageText: [],
    answerCallbackQuery: [],
  };
  return {
    calls,
    async sendMessage(...args) {
      calls.sendMessage.push(args);
    },
    async editMessageText(...args) {
      calls.editMessageText.push(args);
    },
    async answerCallbackQuery(...args) {
      calls.answerCallbackQuery.push(args);
    },
  };
}

function createGoalApi({ currentGoal = null, updatedGoal, waitTurnId = null } = {}) {
  return {
    calls: {
      read: [],
      set: [],
      clear: [],
      waitForTurnId: [],
    },
    events: [],
    async read(args) {
      this.calls.read.push(args);
      this.events.push("read");
      return currentGoal;
    },
    async set(args) {
      this.calls.set.push(args);
      this.events.push("set");
      return updatedGoal ?? {
        objective: args.objective ?? currentGoal?.objective ?? "Goal",
        status: args.status ?? currentGoal?.status ?? "active",
        tokensUsed: 0,
      };
    },
    async clear(args) {
      this.calls.clear.push(args);
      this.events.push("clear");
      return {};
    },
    async waitForTurnId(sessionId) {
      this.calls.waitForTurnId.push(sessionId);
      this.events.push("waitForTurnId");
      return waitTurnId;
    },
  };
}

function createMountedStore(sessionId = "session-1") {
  return {
    getSessionId: () => sessionId,
    setSessionId: () => undefined,
    createCallbackAction({ kind, payload }) {
      return `${kind}:${Object.keys(payload ?? {}).length}`;
    },
  };
}

function createUnmountedStore() {
  let sessionId = null;
  return {
    getSessionId: () => sessionId,
    setSessionId: (_conversationId, mountedSessionId) => {
      sessionId = mountedSessionId;
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
    goal: {
      objective: "Ship a clean Telegram /goal surface",
      status: "active",
      tokenBudget: 500000,
      tokensUsed: 184000,
      timeUsedSeconds: 8040,
    },
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
    goal: {
      objective: "Keep working",
      status: "active",
      tokenBudget: null,
      tokensUsed: 10,
      timeUsedSeconds: 0,
    },
  });

  assert.match(panel.text, /Status: active/);
  assert.match(panel.text, /Turn: working/);
});

test("goal panel renders inactive unfinished goal controls", () => {
  const panel = buildGoalPanel({
    store: createStore(),
    conversationId: "conversation-1",
    goal: {
      objective: "Paused objective",
      status: "paused",
      tokenBudget: null,
      tokensUsed: 1200,
      timeUsedSeconds: 0,
    },
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
    currentGoal: {
      objective: "Current objective",
    },
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
  const client = createClient();
  const goalApi = createGoalApi({
    updatedGoal: { objective: "Refactor CI", status: "active", tokensUsed: 0 },
    waitTurnId: null,
  });
  const runCalls = [];

  await handleGoalTextCommand({
    client,
    config: { workingDirectory: "/repo" },
    store: createMountedStore(),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goalApi.calls.set.length, 1);
  assert.equal(goalApi.calls.waitForTurnId.length, 1);
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0].turnId, null);
  assert.match(runCalls[0].prompt, /Continue working toward this Codex goal\./);
  assert.match(runCalls[0].prompt, /Refactor CI/);
  assert.equal(client.calls.sendMessage.length, 0);
});

test("goal text command bootstraps a fresh session when none is mounted", async () => {
  const client = createClient();
  const store = createUnmountedStore();
  const goalApi = createGoalApi({
    updatedGoal: { objective: "Refactor CI", status: "active", tokensUsed: 0 },
    waitTurnId: null,
  });
  const startCalls = [];
  const runCalls = [];

  await handleGoalTextCommand({
    client,
    config: { workingDirectory: "/repo" },
    store,
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi,
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
  assert.equal(goalApi.calls.set.length, 1);
  assert.equal(goalApi.calls.set[0].threadId, "fresh-session");
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0].sessionId, "fresh-session");
  assert.equal(client.calls.sendMessage.length, 0);
});

test("goal text command clears completed goal state before setting a new objective", async () => {
  const client = createClient();
  const goalApi = createGoalApi({
    currentGoal: { objective: "Old goal", status: "complete", tokensUsed: 1200 },
    updatedGoal: { objective: "New goal", status: "active", tokensUsed: 0 },
    waitTurnId: null,
  });
  const runCalls = [];

  await handleGoalTextCommand({
    client,
    config: { workingDirectory: "/repo" },
    store: createMountedStore("session-1"),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "New goal",
    goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goalApi.calls.clear.length, 1);
  assert.equal(goalApi.calls.clear[0].threadId, "session-1");
  assert.equal(goalApi.calls.set.length, 1);
  assert.equal(goalApi.calls.set[0].objective, "New goal");
  assert.ok(goalApi.events.indexOf("clear") < goalApi.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});

test("goal replace callback clears stale goal state before setting replacement", async () => {
  const client = createClient();
  const goalApi = createGoalApi({
    updatedGoal: { objective: "New goal", status: "active", tokensUsed: 0 },
    waitTurnId: null,
  });
  const runCalls = [];

  await handleGoalControlCallback({
    client,
    config: { workingDirectory: "/repo" },
    store: createMountedStore("session-1"),
    action: { kind: "goal:replace", conversationId: "conversation-1", payload: { objective: "New goal" } },
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
    goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(goalApi.calls.clear.length, 1);
  assert.equal(goalApi.calls.set.length, 1);
  assert.equal(goalApi.calls.set[0].objective, "New goal");
  assert.ok(goalApi.events.indexOf("clear") < goalApi.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});
test("goal text command attaches to an upstream-created goal turn when present", async () => {
  const client = createClient();
  const goalApi = createGoalApi({
    updatedGoal: { objective: "Refactor CI", status: "active", tokensUsed: 0 },
    waitTurnId: "goal-turn",
  });
  const runCalls = [];

  await handleGoalTextCommand({
    client,
    config: { workingDirectory: "/repo" },
    store: createMountedStore("session-1"),
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    args: "Refactor CI",
    goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0].sessionId, "session-1");
  assert.equal(runCalls[0].turnId, "goal-turn");
});

test("goal resume callback edits to starting before fallback turn execution", async () => {
  const client = createClient();
  const goalApi = createGoalApi({
    currentGoal: { objective: "Refactor CI", status: "paused", tokensUsed: 0 },
    updatedGoal: { objective: "Refactor CI", status: "active", tokensUsed: 0 },
    waitTurnId: null,
  });
  const runCalls = [];

  await handleGoalControlCallback({
    client,
    config: { workingDirectory: "/repo" },
    store: createMountedStore("session-1"),
    action: { kind: "goal:resume", conversationId: "conversation-1", payload: {} },
    callbackQueryId: "callback-1",
    chatId: 123,
    messageId: 456,
    goalApi,
    runGoalTurn: async (args) => {
      runCalls.push(args);
      return true;
    },
  });

  assert.deepEqual(client.calls.answerCallbackQuery[0], ["callback-1", "Starting."]);
  assert.match(client.calls.editMessageText[0][2], /Turn: starting/);
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0].turnId, null);
});

test("goal turns use normal concurrent-message decision panel when Codex is already working", async () => {
  const client = createClient();
  const store = {
    createCallbackAction({ kind }) {
      return kind;
    },
  };
  const turns = new TurnController({
    config: { workingDirectory: "/repo" },
    client,
    store,
    outbox: { enqueueText() {} },
    activeQueries: new Map([["conversation-1", { steer: async () => true }]]),
    workflowWaits: new Map(),
    workflowWakeEvents: new Map(),
    isStopping: () => false,
  });

  const handled = await turns.runGoalTurn({
    conversationId: "conversation-1",
    chatId: 123,
    messageId: 456,
    sessionId: "session-1",
    turnId: null,
    prompt: "Continue working toward this Codex goal.\n\nRefactor CI",
  });

  assert.equal(handled, true);
  assert.match(client.calls.sendMessage[0][1], /Codex is currently working/);
  assert.deepEqual(
    client.calls.sendMessage[0][2].reply_markup.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Steer", "Queue"], ["Swerve", "Discard"]],
  );
});
