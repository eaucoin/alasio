/**
 * alasio's turns, as Turns runs them on a harness a test scripts: when a turn may start,
 * what it ends, what a harness's own turn claims, what its status message says, and what a
 * start does with what the last alasio left.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Deferred, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";
import { TestClock } from "effect/testing";

import { createCommandEventPolicy } from "../src/codex/command-event-policy.ts";
import { makeStatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { Harness, OwnTurns, TurnParams, TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { Store, TurnFenced } from "../src/persistence/store.ts";
import { forgetfulPersistence } from "./support/persistence.ts";
import { run, testStore } from "./support/store.ts";
import { type QueuedToOutbox, recordingOutbox, type TestAlasio, withServices } from "./support/turns.ts";

/** A Codex harness whose turns run as `turn` says, given what alasio gives each. */
function scriptedHarness(turn: (params: TurnParams) => Effect.Effect<TurnResult, StoreError | TurnFenced>): Harness {
  const unused = (name: string) => () => assert.fail(`the scripted harness has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
    supportsWarmup: false,
    sessions: {
      listSessions: unused("listSessions"),
      getTotalSessionPages: unused("getTotalSessionPages"),
      getSessionByNumber: unused("getSessionByNumber"),
      getSessionLastMessage: unused("getSessionLastMessage"),
      listSessionMessages: unused("listSessionMessages"),
      getTotalRewindPages: unused("getTotalRewindPages"),
      createForkedSession: unused("createForkedSession"),
    },
    startFreshSession: unused("startFreshSession"),
    warmSession: () => Effect.succeed(false),
    runTurn: turn,
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/** A turn's answer, `text`, written as a harness writes it: begun, its blocks, completed. */
const answered = (params: TurnParams, text: string): Effect.Effect<TurnResult, StoreError | TurnFenced> =>
  Effect.gen(function*() {
    const pendingResponseId = yield* params.persistence.createPendingResponse(params.chatId, params.messageId, null);
    const blockSequence = [{ type: "text" as const, content: text, phase: "final_answer" as const }];
    yield* params.persistence.appendBlocksToPending(pendingResponseId, blockSequence);
    yield* params.persistence.markPendingResponseComplete(pendingResponseId);
    return { blockSequence, sessionId: "session-1", pendingResponseId, interrupted: false, responseCompleted: true };
  });

const CONVERSATION_CHAT = 41;

/** A Codex conversation on a folder, in a new store. */
async function conversation(): Promise<{ readonly store: Store["Service"]; readonly conversationId: string }> {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: String(CONVERSATION_CHAT), user: { id: CONVERSATION_CHAT } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  return { store, conversationId };
}

/** Runs `use` on alasio's services over `store`, its Codex the scripted `harness`, what it queues for Telegram recorded. */
async function withTurns<T>(
  store: Store["Service"],
  harness: Harness,
  use: (alasio: TestAlasio, queued: QueuedToOutbox) => Promise<T>,
): Promise<T> {
  const outbox = recordingOutbox();
  return await withServices({ store, harnesses: { [CODEX_HARNESS]: harness }, outbox: outbox.layer }, (alasio) => use(alasio, outbox.queued));
}

/** A turn of the conversation, as alasio runs one outside its prompt jobs. */
const turnOf = (conversationId: string, prompt: string) =>
  Effect.flatMap(Turns, (turns) => turns.run({ conversationId, chatId: CONVERSATION_CHAT, messageId: prompt, prompt, traceparent: null }));

/** Waits for `check` to hold, a little at a time. */
async function until(what: string, check: () => Promise<boolean> | boolean): Promise<void> {
  for (let waited = 0; waited < 5000; waited += 20) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(`timed out waiting for ${what}`);
}

test("a turn holds its conversation from before its claim until it lets go: one decided meanwhile is refused, and claims nothing", async () => {
  const { store, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const harness = scriptedHarness((params) => Deferred.await(release).pipe(Effect.andThen(answered(params, "Done."))));
  await withTurns(store, harness, async (alasio) => {
    const first = alasio.runFork(turnOf(conversationId, "first"));
    await until("the first turn to claim the conversation", async () => (await run(store.getActiveTurn(conversationId))) !== null);
    const refused = await alasio.runPromise(Effect.flip(turnOf(conversationId, "second")));
    assert.equal(refused._tag, "ConversationBusy");
    assert.equal((await run(store.getActiveTurn(conversationId)))?.epoch, "1");
    await alasio.runPromise(Deferred.succeed(release, undefined));
    assert.equal(await alasio.runPromise(Fiber.join(first)), true);
    const ended = await run(store.getTurn(conversationId));
    assert.deepEqual([ended?.epoch, ended?.state, ended?.outcome], ["1", "completed", "completed"]);
    assert.equal(await alasio.runPromise(Effect.flatMap(ActiveTurns, (turns) => turns.isBusy(conversationId))), false);
  });
});

test("a turn superseded fails with TurnFenced, as no failure of the store's, and ends nothing of the turn that claimed the conversation since", async () => {
  const { store, conversationId } = await conversation();
  // Another claim lands while the turn runs, as one of another alasio's would.
  const harness = scriptedHarness((params) =>
    store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "elsewhere", harness: CODEX_HARNESS }).pipe(
      Effect.andThen(answered(params, "Too late.")),
    )
  );
  await withTurns(store, harness, async (alasio) => {
    const exit = await alasio.runPromiseExit(turnOf(conversationId, "superseded"));
    assert.ok(Exit.isFailure(exit));
    const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail");
    assert.equal(failure?._tag === "Fail" ? failure.error._tag : undefined, "TurnFenced");
    const turn = await run(store.getTurn(conversationId));
    assert.deepEqual([turn?.epoch, turn?.state, turn?.status, turn?.thread_ts], ["2", "active", "working", "elsewhere"]);
    assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  });
});

test("a turn a harness starts on its own claims the conversation, as alasio's turns do, writes under its claim, and ends it", async () => {
  const { store, conversationId } = await conversation();
  let ownTurns: OwnTurns | undefined;
  const harness = scriptedHarness((params) => {
    ownTurns = params.ownTurns;
    return answered(params, "Started the build; I will report.");
  });
  await withTurns(store, harness, async (alasio) => {
    assert.equal(await alasio.runPromise(turnOf(conversationId, "build it")), true);
    assert.ok(ownTurns);
    const own = await run(ownTurns.claim("claude-cli-turn:1", "session-1"));
    const claimed = await run(store.getActiveTurn(conversationId));
    assert.deepEqual([claimed?.epoch, claimed?.thread_ts, claimed?.session_id, claimed?.harness], ["2", "claude-cli-turn:1", "session-1", CODEX_HARNESS]);
    const reply = await run(own.persistence.createPendingResponse(String(CONVERSATION_CHAT), "claude-cli-turn:1", "session-1"));
    await run(own.persistence.appendBlocksToPending(reply, [{ type: "text", content: "The build passed.", phase: "final_answer" }]));
    await run(own.persistence.markPendingResponseComplete(reply));
    await run(own.end("completed"));
    const ended = await run(store.getTurn(conversationId));
    assert.deepEqual([ended?.epoch, ended?.state, ended?.outcome], ["2", "completed", "completed"]);
  });
});

test("alasio starting ends each turn the last one left running, its status message saying how", async () => {
  const { store, conversationId } = await conversation();
  const harness = scriptedHarness((params) => answered(params, "unused"));
  // Left by an alasio killed mid-turn: its turn, working, with its status message.
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "9", harness: CODEX_HARNESS }));
  assert.equal(await run(store.setTurnStatusMessage(conversationId, epoch, "status-of-the-killed")), true);
  const killed = store.turnPersistence(conversationId, epoch);
  const partial = await run(killed.createPendingResponse(String(CONVERSATION_CHAT), "9", null));
  await run(killed.updateActiveTurnPendingResponseId(partial));
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => Effect.andThen(turns.reconcilePersistentState, turns.recoverInterruptedTurns)));
    const ended = await run(store.getTurn(conversationId));
    assert.deepEqual([ended?.state, ended?.status, ended?.outcome], ["completed", "ended", "stopped"]);
    assert.deepEqual(queued.edits.map(({ edits, text }) => [edits, text]), [[
      "status-of-the-killed",
      "alasio stopped during this turn, and Codex did not finish it. Send the message again to run it.",
    ]]);
    // Its partial response is never delivered.
    await run(killed.markPendingResponseComplete(partial));
    assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  });
});

test("alasio starting ends a turn whose job the agent finished as the last alasio stopped, completed, its status message saying so", async () => {
  const { store, conversationId } = await conversation();
  const harness = scriptedHarness((params) => answered(params, "unused"));
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "5", prompt: "finish" }));
  await run(store.claimNextPromptJob(conversationId));
  await run(store.markPromptJobUpstreamCompleted(job.id, "session-1", "turn-1"));
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "5", harness: CODEX_HARNESS }));
  await run(store.setTurnStatusMessage(conversationId, epoch, "status-of-the-finished"));
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.reconcilePersistentState));
    assert.deepEqual([(await run(store.getTurn(conversationId)))?.outcome, (await run(store.getPromptJob(job.id)))?.state], ["completed", "completed"]);
    assert.deepEqual(queued.edits.map(({ edits, text }) => [edits, text]), [["status-of-the-finished", "Codex finished this turn as alasio stopped."]]);
  });
});

test("a turn's status message says what its row says it does, as the row changes, and is recorded on it", async () => {
  const { store, conversationId } = await conversation();
  const outbox = recordingOutbox();
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "1", harness: CODEX_HARNESS }));
  const writer = store.turnPersistence(conversationId, epoch);
  const settle = Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 200)));
  const edits = () => outbox.queued.edits.map(({ text }) => text.replace(/ · started .*$/u, ""));
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const status = yield* makeStatusReporter();
    yield* status.statusUpdates({ chatId: CONVERSATION_CHAT, conversationId, epoch, harnessName: "Codex" });
    yield* settle;
    assert.equal((yield* store.getTurn(conversationId))?.status_outbox_id, outbox.queued.statuses[0]?.id);
    yield* writer.setWaiting("workflow run 42 (watch)");
    yield* TestClock.adjust("30 seconds");
    yield* settle;
    assert.deepEqual(edits(), ["Codex is waiting on workflow run 42 (watch)"]);
    yield* writer.setWaiting(null);
    yield* TestClock.adjust("30 seconds");
    yield* settle;
    assert.deepEqual(edits(), ["Codex is waiting on workflow run 42 (watch)", "Codex is working"]);
    // Ended, the turn changes its status message no more.
    yield* store.endTurn(conversationId, epoch, "completed");
    yield* TestClock.adjust("30 seconds");
    yield* settle;
    assert.equal(outbox.queued.edits.length, 2);
  })).pipe(Effect.provide(Layer.mergeAll(Layer.succeed(Store, store), outbox.layer, TestClock.layer()))));
});

test("a job made pending again keeps its place in line unless told another", async () => {
  const { store, conversationId } = await conversation();
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "first", prompt: "first", priority: 1 }));
  await run(store.claimNextPromptJob(conversationId));
  await run(store.setPromptJobDisposition(job.id, "pending"));
  assert.equal((await run(store.getPromptJob(job.id)))?.priority, 1);
  await run(store.setPromptJobDisposition(job.id, "pending", 0));
  assert.equal((await run(store.getPromptJob(job.id)))?.priority, 0);
});

test("a turn waiting to hold its conversation does so once the turn holding it lets go", async () => {
  const { conversationId } = await conversation();
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const activeTurns = yield* ActiveTurns;
    const turn = (cliInitiated: boolean) => ({ stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated });
    const holding = yield* Scope.make();
    yield* activeTurns.register(conversationId, turn(false)).pipe(Scope.provide(holding));
    const waiting = yield* Scope.make();
    let registered = false;
    const registering = yield* Effect.forkChild(activeTurns.registerWhenIdle(conversationId, turn(true)).pipe(
      Scope.provide(waiting),
      Effect.andThen(Effect.sync(() => void (registered = true))),
    ));
    yield* Effect.sleep("50 millis");
    assert.equal(registered, false, "it registered over a turn running");
    yield* Scope.close(holding, Exit.void);
    yield* Fiber.join(registering);
    const running = yield* activeTurns.get(conversationId);
    assert.equal(Option.getOrUndefined(running)?.cliInitiated, true);
    yield* Scope.close(waiting, Exit.void);
    assert.equal(yield* activeTurns.isBusy(conversationId), false);
  })).pipe(Effect.provide(ActiveTurns.layer)));
});

test("a turn's command that waits on a workflow run is recorded as its wait, the store failing to record it failing no turn, a turn superseded told so", async () => {
  const waits: (string | null)[] = [];
  const policyOn = (setWaiting: (waitingOn: string | null) => Effect.Effect<void, StoreError | TurnFenced>) =>
    createCommandEventPolicy({ persistence: { ...forgetfulPersistence, setWaiting }, chatId: "41", messageId: "1" });
  const recorded = policyOn((waitingOn) => Effect.sync(() => void waits.push(waitingOn)));
  await Effect.runPromise(recorded.inspectCommand({ command: "gh run watch 12345678", sessionId: "s-1" }));
  await Effect.runPromise(recorded.inspectCommand({ command: "gh run watch 12345678", sessionId: "s-1" }));
  await Effect.runPromise(recorded.inspectCommand({ command: "ls", sessionId: "s-1" }));
  assert.deepEqual(waits, ["workflow run 12345678 (watch)", null]);
  const failing = policyOn(() => Effect.fail(new StoreError({ cause: new Error("the store is down") })));
  assert.deepEqual(await Effect.runPromise(failing.inspectCommand({ command: "gh run watch 12345678", sessionId: "s-1" })), { blocked: false });
  const fenced = policyOn(() => Effect.fail(new TurnFenced({ threadKey: "telegram:41", epoch: 1 })));
  const exit = await Effect.runPromiseExit(fenced.inspectCommand({ command: "gh run watch 12345678", sessionId: "s-1" }));
  assert.ok(Exit.isFailure(exit) && String(exit.cause).includes("TurnFenced"));
});

test("a stop is done once the turn has let go of its conversation: its harness's registration, and alasio's reservation under it", async () => {
  const { conversationId } = await conversation();
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const activeTurns = yield* ActiveTurns;
    const reservation = yield* Scope.make();
    assert.equal(yield* activeTurns.reserve(conversationId).pipe(Scope.provide(reservation)), true);
    const registration = yield* Scope.make();
    // The harness's turn stops by letting go of its registration.
    yield* activeTurns.register(conversationId, { stop: () => Scope.close(registration, Exit.void), steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(registration));
    let stopped = false;
    const stopping = yield* Effect.forkChild(activeTurns.stop(conversationId, "interrupt").pipe(Effect.andThen(Effect.sync(() => void (stopped = true)))));
    yield* Effect.sleep("50 millis");
    assert.equal(stopped, false, "the stop was done while alasio still held the conversation for the turn");
    yield* Scope.close(reservation, Exit.void);
    yield* Fiber.join(stopping);
    assert.equal(yield* activeTurns.isBusy(conversationId), false);
  })).pipe(Effect.provide(ActiveTurns.layer)));
});