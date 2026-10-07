import assert from "node:assert/strict";
import { test } from "node:test";

import type { v2 } from "../.types/codex/index.js";
import { Effect } from "effect";

import { NoActiveTurn } from "../src/codex/app-server/thread-client.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { GoalUpdate, HarnessGoals } from "../src/harness/index.ts";
import {
  type GoalTurnRequest,
  buildGoalPanel,
  buildNoActiveGoalPanel,
  buildNoMountedGoalPanel,
  buildReplaceGoalPanel,
  handleGoalControlCallback,
  handleGoalTextCommand,
} from "../src/operator/goal-control.ts";
import type { Store } from "../src/persistence/store.ts";
import { run, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";
import { type TestAlasio, turnsStub, withServices } from "./support/turns.ts";

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

interface FakeGoalOptions {
  readonly currentGoal?: v2.ThreadGoal | null;
  readonly updatedGoal?: v2.ThreadGoal;
  readonly waitTurnId?: string | null;
}

function createGoals({ currentGoal = null, updatedGoal, waitTurnId = null }: FakeGoalOptions = {}) {
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
  const goals: HarnessGoals = {
    read: (args) =>
      Effect.sync(() => {
        calls.read.push(args);
        events.push("read");
        return currentGoal;
      }),
    set: (args) =>
      Effect.sync(() => {
        calls.set.push(args);
        events.push("set");
        return updatedGoal ?? threadGoal({
          objective: args.objective ?? currentGoal?.objective ?? "Goal",
          status: args.status ?? currentGoal?.status ?? "active",
          tokensUsed: 0,
        });
      }),
    clear: (args) =>
      Effect.sync(() => {
        calls.clear.push(args);
        events.push("clear");
        return { cleared: true };
      }),
    waitForTurnId: (sessionId) =>
      Effect.sync(() => {
        calls.waitForTurnId.push(sessionId);
        events.push("waitForTurnId");
        return waitTurnId;
      }),
  };
  return { calls, events, goals };
}

/** The conversation of chat 123, on Codex, with `sessionId` mounted when given. */
const CONVERSATION = "telegram:123";

/** The goal controls' services over a Codex conversation, the turns they run (made on its store) standing in, for one test. */
async function withGoalControls<T>(
  { sessionId = "session-1", turns = () => ({}) }: { readonly sessionId?: string | null; readonly turns?: (store: Store["Service"]) => Partial<Turns["Service"]> },
  use: (alasio: TestAlasio, telegram: RecordingTelegram, store: Store["Service"]) => Promise<T>,
): Promise<T> {
  const store = await codexConversation();
  if (sessionId) await run(store.setSessionId(CONVERSATION, sessionId));
  const telegram = recordingTelegram();
  return await withServices({ store, telegram: telegram.layer, turns: turnsStub(turns(store)) }, (alasio) => use(alasio, telegram, store));
}

/** A store holding the conversation of chat 123, on Codex. */
async function codexConversation(): Promise<Store["Service"]> {
  const store = await testStore();
  await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "123", user: { id: 123 } })), "codex"));
  return store;
}

/** A runGoalTurn that records each goal turn asked for, and takes it over. */
function recordGoalTurns(runCalls: GoalTurnRequest[]): Pick<Turns["Service"], "runGoalTurn"> {
  return {
    runGoalTurn: (args) =>
      Effect.sync(() => {
        runCalls.push(args);
        return true;
      }),
  };
}

test("goal panel renders no-mounted empty state", () => {
  const panel = buildNoMountedGoalPanel();

  assert.match(panel.text, /^Goal\n\nNo Codex session is mounted\./);
  assert.deepEqual(
    panel.keyboard.map((row) => row.map((button) => button.text)),
    [["Sessions"], ["New Session"], ["Close"]],
  );
});

test("goal panel renders no-active empty state", () => {
  const panel = buildNoActiveGoalPanel({
    sessionId: "12345678-aaaa-bbbb-cccc-123456789abc",
  });

  assert.match(panel.text, /No active goal is set for this session\./);
  assert.match(panel.text, /Session: 12345678/);
  assert.match(panel.text, /\/goal <objective>/);
  assert.deepEqual(
    panel.keyboard.map((row) => row.map((button) => button.text)),
    [["Close"]],
  );
});

test("goal panel renders active goal controls", () => {
  const panel = buildGoalPanel({
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
    panel.keyboard.map((row) => row.map((button) => button.text)),
    [["Pause", "Clear"], ["Close"]],
  );
});

test("goal panel can distinguish active goal state from active turn state", () => {
  const panel = buildGoalPanel({
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
    panel.keyboard.map((row) => row.map((button) => button.text)),
    [["Resume", "Clear"], ["Close"]],
  );
});

test("replace panel requires explicit confirmation", () => {
  const panel = buildReplaceGoalPanel({
    currentGoal: threadGoal({
      objective: "Current objective",
    }),
    objective: "New objective",
  });

  assert.match(panel.text, /^Replace goal\?/);
  assert.match(panel.text, /Current objective/);
  assert.match(panel.text, /New objective/);
  assert.deepEqual(
    panel.keyboard.map((row) => row.map((button) => button.text)),
    [["Replace Goal"], ["Keep Current Goal"], ["Close"]],
  );
});

test("goal text command starts a fallback turn when upstream does not create one", async () => {
  const goal = createGoals({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({ turns: () => recordGoalTurns(runCalls) }, async (alasio, { calls }) => {
    await alasio.runPromise(handleGoalTextCommand({ conversationId: CONVERSATION, chatId: 123, messageId: 456, args: "Refactor CI", goals: goal.goals }));
    assert.equal(goal.calls.set.length, 1);
    assert.equal(goal.calls.waitForTurnId.length, 1);
    assert.equal(runCalls.length, 1);
    assert.equal(runCalls[0]?.turnId, null);
    assert.match(runCalls[0]?.prompt ?? "", /Continue working toward this Codex goal\./);
    assert.match(runCalls[0]?.prompt ?? "", /Refactor CI/);
    assert.equal(calls.sendMessage.length, 0);
  });
});

test("goal text command bootstraps a fresh session when none is mounted", async () => {
  const goal = createGoals({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const startCalls: string[] = [];
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({
    sessionId: null,
    turns: (store) => ({
      ...recordGoalTurns(runCalls),
      startNewSession: (conversationId) =>
        Effect.sync(() => startCalls.push(conversationId)).pipe(
          Effect.andThen(store.setSessionId(conversationId, "fresh-session")),
          Effect.as("fresh-session"),
        ),
    }),
  }, async (alasio, { calls }, store) => {
    await alasio.runPromise(handleGoalTextCommand({ conversationId: CONVERSATION, chatId: 123, messageId: 456, args: "Refactor CI", goals: goal.goals }));
    assert.deepEqual(startCalls, [CONVERSATION]);
    assert.equal((await run(store.getMount(CONVERSATION))).sessionId, "fresh-session");
    assert.equal(goal.calls.set.length, 1);
    assert.equal(goal.calls.set[0]?.threadId, "fresh-session");
    assert.equal(runCalls.length, 1);
    assert.equal(runCalls[0]?.sessionId, "fresh-session");
    assert.equal(calls.sendMessage.length, 0);
  });
});

test("goal text command clears completed goal state before setting a new objective", async () => {
  const goal = createGoals({
    currentGoal: threadGoal({ objective: "Old goal", status: "complete", tokensUsed: 1200 }),
    updatedGoal: threadGoal({ objective: "New goal", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({ turns: () => recordGoalTurns(runCalls) }, async (alasio) => {
    await alasio.runPromise(handleGoalTextCommand({ conversationId: CONVERSATION, chatId: 123, messageId: 456, args: "New goal", goals: goal.goals }));
  });
  assert.equal(goal.calls.clear.length, 1);
  assert.equal(goal.calls.clear[0]?.threadId, "session-1");
  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.set[0]?.objective, "New goal");
  assert.ok(goal.events.indexOf("clear") < goal.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});

test("goal replace callback clears stale goal state before setting replacement", async () => {
  const goal = createGoals({
    updatedGoal: threadGoal({ objective: "New goal", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({ turns: () => recordGoalTurns(runCalls) }, async (alasio) => {
    await alasio.runPromise(handleGoalControlCallback({
      action: { id: "action-1", kind: "goal:replace", conversationId: CONVERSATION, payload: { objective: "New goal" } },
      callbackQueryId: "callback-1",
      chatId: 123,
      messageId: 456,
      goals: goal.goals,
    }));
  });
  assert.equal(goal.calls.clear.length, 1);
  assert.equal(goal.calls.set.length, 1);
  assert.equal(goal.calls.set[0]?.objective, "New goal");
  assert.ok(goal.events.indexOf("clear") < goal.events.indexOf("set"));
  assert.equal(runCalls.length, 1);
});

test("goal text command attaches to an upstream-created goal turn when present", async () => {
  const goal = createGoals({
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: "goal-turn",
  });
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({ turns: () => recordGoalTurns(runCalls) }, async (alasio) => {
    await alasio.runPromise(handleGoalTextCommand({ conversationId: CONVERSATION, chatId: 123, messageId: 456, args: "Refactor CI", goals: goal.goals }));
  });
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.sessionId, "session-1");
  assert.equal(runCalls[0]?.turnId, "goal-turn");
});

test("goal resume callback edits to starting before fallback turn execution", async () => {
  const goal = createGoals({
    currentGoal: threadGoal({ objective: "Refactor CI", status: "paused", tokensUsed: 0 }),
    updatedGoal: threadGoal({ objective: "Refactor CI", status: "active", tokensUsed: 0 }),
    waitTurnId: null,
  });
  const runCalls: GoalTurnRequest[] = [];
  await withGoalControls({ turns: () => recordGoalTurns(runCalls) }, async (alasio, { calls }) => {
    await alasio.runPromise(handleGoalControlCallback({
      action: { id: "action-1", kind: "goal:resume", conversationId: CONVERSATION, payload: {} },
      callbackQueryId: "callback-1",
      chatId: 123,
      messageId: 456,
      goals: goal.goals,
    }));
    assert.deepEqual(calls.answerCallbackQuery[0], ["callback-1", "Starting."]);
    assert.match(calls.editMessageText[0]?.[2] ?? "", /Turn: starting/);
  });
  assert.equal(runCalls.length, 1);
  assert.equal(runCalls[0]?.turnId, null);
});

test("a goal that cannot be changed says why on its panel", async () => {
  const goals: HarnessGoals = {
    ...createGoals().goals,
    set: () => Effect.fail(new NoActiveTurn()),
  };
  await withGoalControls({}, async (alasio, { calls }) => {
    await alasio.runPromise(handleGoalTextCommand({ conversationId: CONVERSATION, chatId: 123, messageId: 456, args: "Refactor CI", goals }));
    assert.equal(calls.sendMessage.at(-1)?.[1], "Goal\n\nFailed to update goal: Cannot steer Codex without an active turn id");
  });
});

test("goal turns use normal concurrent-message decision panel when Codex is already working", async () => {
  const store = await codexConversation();
  const { calls, layer } = recordingTelegram();
  const handled = await withServices({ store, telegram: layer }, (alasio) =>
    // Codex is working: a turn of the conversation is running.
    alasio.runPromise(Effect.scoped(Effect.gen(function*() {
      const activeTurns = yield* ActiveTurns;
      yield* activeTurns.register(CONVERSATION, { stop: () => Effect.void, steer: () => Effect.succeed(true), cliInitiated: false });
      const turns = yield* Turns;
      return yield* turns.runGoalTurn({
        conversationId: CONVERSATION,
        chatId: 123,
        messageId: 456,
        sessionId: "session-1",
        turnId: null,
        prompt: "Continue working toward this Codex goal.\n\nRefactor CI",
      });
    }))));

  assert.equal(handled, true);
  assert.match(calls.sendMessage[0]?.[1] ?? "", /Codex is currently working/);
  assert.deepEqual(
    calls.sendMessage[0]?.[2]?.reply_markup?.inline_keyboard.map((row) => row.map((button) => button.text)),
    [["Steer", "Queue"], ["Swerve", "Discard"]],
  );
});
