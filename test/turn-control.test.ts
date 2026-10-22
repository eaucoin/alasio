/**
 * alasio's turns, as Turns runs them on a harness a test scripts: when a turn may start,
 * what it ends, what a harness's own turn claims, what its status message says, what
 * becomes of the prompts held while a turn ran and of the buttons that settle them, and
 * what a start does with what the last alasio left.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Deferred, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";
import { TestClock } from "effect/testing";

import { createCommandEventPolicy } from "../src/codex/command-event-policy.ts";
import { makeStatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { discardHeld, queueHeld, steerWithHeld, swerveTo } from "../src/harness/commands.ts";
import { HarnessHost } from "../src/harness/host.ts";
import { HARNESS_BINDING } from "../src/harness/pooled-host.ts";
import type { Harness, OwnTurns, TurnParams, TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { Store, TurnFenced } from "../src/persistence/store.ts";
import { forgetfulPersistence } from "./support/persistence.ts";
import { bindHarness, holdsOf, POOLED_SCHEMA, Pools, pooledStore, poolOf } from "./support/pools.ts";
import { run } from "./support/store.ts";
import { type QueuedToOutbox, recordingOutbox, type TestAlasio, withServices } from "./support/turns.ts";

/** A Codex harness whose turns run as `turn` says, given what alasio gives each. */
function scriptedHarness(turn: (params: TurnParams) => Effect.Effect<TurnResult, StoreError | TurnFenced>): Harness {
  const unused = (name: string) => () => assert.fail(`the scripted harness has no ${name}`);
  return {
    name: CODEX_HARNESS,
    displayName: harnessDisplayName(CODEX_HARNESS),
    supportsGoals: false,
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
async function conversation(): Promise<{ readonly store: Store["Service"]; readonly schema: string; readonly conversationId: string }> {
  const schema = POOLED_SCHEMA;
  const store = await pooledStore();
  const conversationId = await run(store.upsertConversation({ chatId: String(CONVERSATION_CHAT), user: { id: CONVERSATION_CHAT } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  return { store, schema, conversationId };
}

/** Runs `use` on alasio's services over `store`, its Codex the scripted `harness`, what it queues for Telegram recorded. */
async function withTurns<T>(
  store: Store["Service"],
  harness: Harness,
  use: (alasio: TestAlasio, queued: QueuedToOutbox) => Promise<T>,
  outboxOptions: Parameters<typeof recordingOutbox>[0] = {},
): Promise<T> {
  const outbox = recordingOutbox(outboxOptions);
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

/** The conversation's prompt held for the operator's choice, as one sent while a turn ran is, and its four buttons. */
async function heldPrompt(store: Store["Service"], conversationId: string, prompt: string, visibleText = prompt) {
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: prompt, prompt, visibleText, state: "awaiting_choice" }));
  const payload = { jobId: job.id, prompt };
  const buttons = await run(store.createCallbackActions(conversationId, ["steer", "queue", "swerve", "discard"].map((kind) => ({ kind, payload }))));
  return { job, buttons };
}

test("a held prompt is settled by the first of its buttons pressed alone: the others find it gone, and are forgotten", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  await withTurns(store, harness, async (alasio) => {
    const discarded = await heldPrompt(store, conversationId, "never mind");
    assert.equal(await alasio.runPromise(discardHeld(discarded.job.id)), true);
    assert.equal((await run(store.getPromptJob(discarded.job.id)))?.state, "cancelled");
    assert.equal(await alasio.runPromise(discardHeld(discarded.job.id)), false);
    assert.equal(await alasio.runPromise(queueHeld(discarded.job.id)), false);
    assert.equal(await alasio.runPromise(swerveTo(conversationId, discarded.job.id)), false);
    assert.deepEqual(await alasio.runPromise(steerWithHeld(conversationId, discarded.job.id)), { outcome: "gone" });
    for (const id of discarded.buttons) assert.equal(await run(store.consumeCallbackAction(id)), null, "a button of a settled prompt is kept");
    assert.equal((await run(store.getPromptJob(discarded.job.id)))?.state, "cancelled");

    // Queued, it runs once: a swerve, a steer or a queue after finds it gone, and runs it no more.
    const queued = await heldPrompt(store, conversationId, "run me once");
    assert.equal(await alasio.runPromise(queueHeld(queued.job.id)), true);
    await until("the queued prompt to run", async () => (await run(store.getPromptJob(queued.job.id)))?.state === "completed");
    assert.equal(await alasio.runPromise(swerveTo(conversationId, queued.job.id)), false);
    assert.deepEqual(await alasio.runPromise(steerWithHeld(conversationId, queued.job.id)), { outcome: "gone" });
    assert.equal(await alasio.runPromise(queueHeld(queued.job.id)), false);
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual(ran, ["run me once"]);

    // A steer with no turn running takes it as queued, and it runs after.
    const steered = await heldPrompt(store, conversationId, "steer me");
    assert.deepEqual(await alasio.runPromise(steerWithHeld(conversationId, steered.job.id)), { outcome: "queued", reason: "idle" });
    await until("the steered prompt to run", async () => (await run(store.getPromptJob(steered.job.id)))?.state === "completed");
    assert.deepEqual(ran, ["run me once", "steer me"]);
  });
});

test("the prompts held while a turn ran are asked about again once the conversation is free, each once, and run as the operator says", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  await withTurns(store, harness, async (alasio, queued) => {
    // Sent with a file, which its prompt names: the question quotes what the operator wrote.
    const { job } = await heldPrompt(store, conversationId, "held while it ran\n\n[The operator sent the file /state/files/notes.txt]", "held while it ran");
    const schedule = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.schedule(conversationId)));
    await schedule();
    await until("the held prompt to be asked about again", () => queued.texts.length > 0);
    await schedule();
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual(queued.texts.map(({ text }) => text), ["The turn has ended. What should I do with this message?\n\nheld while it ran"]);
    const keyboard = queued.texts[0]?.options?.reply_markup;
    assert.ok(keyboard && "inline_keyboard" in keyboard);
    assert.deepEqual(keyboard.inline_keyboard.map((row) => row.map(({ text }) => text)), [["Run it", "Discard"]]);
    const [runIt] = keyboard.inline_keyboard[0] ?? [];
    const action = await run(store.consumeCallbackAction(runIt && "callback_data" in runIt ? runIt.callback_data ?? "" : ""));
    assert.deepEqual([action?.kind, action?.payload["jobId"]], ["queue", job.id]);
    assert.equal(await alasio.runPromise(queueHeld(job.id)), true);
    await until("the held prompt to run", () => ran.length === 1);
    assert.deepEqual(ran, ["held while it ran\n\n[The operator sent the file /state/files/notes.txt]"]);
  });
});

test("a prompt the last alasio was handing to a turn to steer as it stopped is asked about, quoted, once, and run only as the operator says", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  // Left steering: taken from being held, and handed to the turn, which may or may not have taken it, as alasio stopped.
  const { job } = await heldPrompt(store, conversationId, "steer this");
  assert.equal((await run(store.settleHeldPromptJob(job.id, "steering")))?.state, "steering");
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => Effect.andThen(turns.reconcilePersistentState, turns.resumePendingPrompts)));
    await until("the steer to be asked about", () => queued.texts.length > 0);
    await new Promise((resolve) => setTimeout(resolve, 100));
    // Neither run again nor asked about as an ordinary held prompt.
    assert.deepEqual(ran, []);
    assert.deepEqual(queued.texts.map(({ text }) => text), [
      "alasio restarted as it sent this message to steer the running turn, which may not have taken it. What should I do with it?\n\nsteer this",
    ]);
    assert.equal((await run(store.getPromptJob(job.id)))?.state, "awaiting_choice");
    const keyboard = queued.texts[0]?.options?.reply_markup;
    assert.ok(keyboard && "inline_keyboard" in keyboard);
    assert.deepEqual(keyboard.inline_keyboard.map((row) => row.map(({ text }) => text)), [["Run it", "Discard"]]);
    assert.equal(await alasio.runPromise(queueHeld(job.id)), true);
    await until("the steer to run, once", () => ran.length === 1);
    assert.deepEqual(ran, ["steer this"]);
  });
});

test("alasio starting lets go of the holds the last one left, and ends each turn it left running, its status message saying how", async () => {
  const { store, conversationId } = await conversation();
  const harness = scriptedHarness((params) => answered(params, "unused"));
  // Left by an alasio killed mid-turn: its hold, and its turn, working, with its status message.
  await run(store.holdConversations([conversationId], "forking", 60_000));
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "9", harness: CODEX_HARNESS }));
  assert.equal(await run(store.setTurnStatusMessage(conversationId, epoch, "status-of-the-killed")), true);
  const killed = store.turnPersistence(conversationId, epoch);
  const partial = await run(killed.createPendingResponse(String(CONVERSATION_CHAT), "9", null));
  await run(killed.updateActiveTurnPendingResponseId(partial));
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => Effect.andThen(turns.reconcilePersistentState, turns.recoverInterruptedTurns)));
    assert.deepEqual(await holdsOf(store), []);
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

test("a turn that fails as it starts, after its status message is queued, ends that message saying so, and lets go of the conversation", async () => {
  const { store, conversationId } = await conversation();
  const failing = scriptedHarness(() => Effect.fail(new StoreError({ cause: new Error("the store is down") })));
  await withTurns(store, failing, async (alasio, queued) => {
    const failed = await alasio.runPromise(Effect.flip(turnOf(conversationId, "go")));
    assert.equal(failed._tag, "StoreError");
    assert.equal(queued.statuses.length, 1);
    assert.deepEqual(queued.edits.map(({ edits, text }) => [edits, text]), [[queued.statuses[0]?.id, "Codex hit an error."]]);
  });
  assert.equal((await run(store.getTurn(conversationId)))?.outcome, "failed");
});

test("a turn whose host is readied before it can hold the conversation shows its status message from the first, saying what it waits on, and takes it on as it starts; one then refused says why on it", async () => {
  const { store, conversationId } = await conversation();
  const outbox = recordingOutbox();
  const bound = Effect.runSync(Deferred.make<void>());
  let refuse = false;
  // As a pooled host's reservation binds ahead: what it waits on told, then held, or refused.
  const readied = (host: HarnessHost["Service"]): HarnessHost["Service"] => ({
    ...host,
    reserve: (id, waiting = () => Effect.void) =>
      Effect.gen(function*() {
        yield* waiting(HARNESS_BINDING);
        yield* Deferred.await(bound);
        yield* waiting(null);
        return refuse ? false : yield* host.reserve(id);
      }),
  });
  const harness = scriptedHarness((params) => answered(params, "Done."));
  const shown = () => outbox.queued.statuses.map(({ text }) => text.replace(/ · started .*$/u, ""));
  await withServices({ store, harnesses: { [CODEX_HARNESS]: harness }, outbox: outbox.layer, host: readied }, async (alasio) => {
    const turn = alasio.runFork(turnOf(conversationId, "first"));
    await until("the status message posted ahead", () => outbox.queued.statuses.length === 1);
    assert.deepEqual(shown(), ["Codex is waiting on its harness, whose pod is being readied"]);
    assert.equal(await run(store.getActiveTurn(conversationId)), null, "a turn claimed before its host held the conversation");
    await alasio.runPromise(Deferred.succeed(bound, undefined));
    assert.equal(await alasio.runPromise(Fiber.join(turn)), true);
    // Taken on by the turn: no other posted, recorded on its row, and edited as it went.
    assert.equal(outbox.queued.statuses.length, 1);
    assert.equal((await run(store.getTurn(conversationId)))?.status_outbox_id, outbox.queued.statuses[0]?.id);
    const edits = outbox.queued.edits.filter(({ edits }) => edits === outbox.queued.statuses[0]?.id).map(({ text }) => text.replace(/ · started .*$/u, ""));
    assert.equal(edits[0], "Codex is working");
    assert.match(edits.at(-1) ?? "", /^Codex worked for /u);
    // Refused once readied, as another turn took the conversation meanwhile: said on its message.
    refuse = true;
    const refused = await alasio.runPromise(Effect.flip(turnOf(conversationId, "second")));
    assert.equal(refused._tag, "ConversationBusy");
    assert.deepEqual(outbox.queued.edits.filter(({ edits }) => edits === outbox.queued.statuses[1]?.id).map(({ text }) => text.replace(/ · started .*$/u, "")), [
      "Codex is working",
      "Codex is already working in this conversation; this message waits for it.",
    ]);
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
  })).pipe(Effect.provide(Layer.mergeAll(outbox.layer, TestClock.layer()).pipe(Layer.provideMerge(Layer.succeed(Store, store))))));
});

test("a held prompt sent to steer is done only once the turn took it, and queued, as it was, where it did not", async () => {
  const { store, conversationId } = await conversation();
  const harness = scriptedHarness((params) => answered(params, "Done."));
  await withTurns(store, harness, async (alasio) => {
    let answer = Effect.runSync(Deferred.make<boolean>());
    const scope = await alasio.runPromise(Scope.make());
    // A turn runs, which answers each steer as the test says.
    await alasio.runPromise(bindHarness(conversationId));
    await alasio.runPromise(Effect.flatMap(ActiveTurns, (turns) =>
      turns.register(conversationId, { stop: () => Effect.void, steer: () => Deferred.await(answer), cliInitiated: false })
    ).pipe(Scope.provide(scope)));
    const steer = async (prompt: string, took: boolean) => {
      answer = Effect.runSync(Deferred.make<boolean>());
      const { job } = await heldPrompt(store, conversationId, prompt);
      const steering = alasio.runPromise(steerWithHeld(conversationId, job.id));
      // Taken from being held as the turn is asked, and not yet done.
      await until("the steer to reach the turn", async () => (await run(store.getPromptJob(job.id)))?.state === "steering");
      await alasio.runPromise(Deferred.succeed(answer, took));
      return { steered: await steering, state: (await run(store.getPromptJob(job.id)))?.state };
    };
    assert.deepEqual(await steer("not taken", false), { steered: { outcome: "queued", reason: "not ready" }, state: "pending" });
    assert.deepEqual(await steer("taken", true), { steered: { outcome: "steered" }, state: "completed" });
    await alasio.runPromise(Scope.close(scope, Exit.void));
  });
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

test("a prompt queued again while a turn runs, as its update is handled again after its handling lock was lost, is queued once and asked about once", async () => {
  const { store, conversationId } = await conversation();
  await withTurns(store, scriptedHarness(() => Effect.never), async (alasio, queued) => {
    await alasio.runPromise(Effect.scoped(Effect.gen(function*() {
      // A turn runs in the conversation.
      yield* bindHarness(conversationId);
      assert.ok(yield* (yield* ActiveTurns).reserve(conversationId));
      const turns = yield* Turns;
      const prompt = { conversationId, chatId: CONVERSATION_CHAT, messageId: 77, prompt: "Is it done?", fileIds: [], visibleText: "Is it done?" };
      yield* turns.submit(prompt);
      yield* turns.submit(prompt);
    })));
    assert.equal(queued.texts.filter(({ text }) => text.startsWith("Codex is currently working.")).length, 1);
  });
  assert.deepEqual((await run(store.listHeldPromptJobs(conversationId))).map(({ message_id: messageId }) => messageId), ["77"]);
});

test("a question's buttons are kept with its reply or not at all: a reply the store refuses keeps none of its buttons' actions", async () => {
  const { store, schema, conversationId } = await conversation();
  await poolOf(store).query(`
    create sequence ${schema}.replies_queued;
    create function ${schema}.refuse_first_reply() returns trigger language plpgsql as $$
      begin
        if nextval('${schema}.replies_queued') = 1 then raise exception 'the store is down'; end if;
        return new;
      end
    $$;
    create trigger refuse_first_reply before insert on ${schema}.telegram_outbox for each row execute function ${schema}.refuse_first_reply();
  `);
  const payload = { jobId: "job-1", prompt: "And the docs?" };
  const ask = store.enqueueOutboxPanel(conversationId, [{ kind: "steer", payload }, { kind: "queue", payload }], (ids) => ({ chatId: CONVERSATION_CHAT, text: `asked ${ids.join(" ")}` }));
  assert.equal((await Effect.runPromise(Effect.flip(ask)))._tag, "StoreError");
  assert.equal(await run(store.hasCallbackActionsOfJob("job-1")), false);
  await run(ask);
  assert.equal(await run(store.hasCallbackActionsOfJob("job-1")), true);
});

test("a prompt held while a turn runs whose question could not be asked, the store failing it, is asked about as its update is handled again", async () => {
  const { store, conversationId } = await conversation();
  await withTurns(store, scriptedHarness(() => Effect.never), async (alasio, queued) => {
    await alasio.runPromise(Effect.scoped(Effect.gen(function*() {
      yield* bindHarness(conversationId);
      assert.ok(yield* (yield* ActiveTurns).reserve(conversationId));
      const turns = yield* Turns;
      const prompt = { conversationId, chatId: CONVERSATION_CHAT, messageId: 78, prompt: "And the docs?", fileIds: [], visibleText: "And the docs?" };
      // The update's handling fails as the question does, and is handled again.
      assert.equal((yield* Effect.flip(turns.submit(prompt)))._tag, "StoreError");
      yield* turns.submit(prompt);
    })));
    assert.equal(queued.texts.filter(({ text }) => text.startsWith("Codex is currently working.")).length, 1);
  }, { failedPanels: 1 });
});

test("what a turn tells besides its reply is queued after it, once a conversation, and again for the next turn where queueing it failed", async () => {
  const { store, conversationId } = await conversation();
  const NOTICE = "This Codex login doesn't offer gpt-5.6-sol, so Codex used gpt-5.6-terra, the login's default (at high effort). Use /model to choose another.";
  const harness = scriptedHarness((params) => Effect.map(answered(params, "Done."), (result) => ({ ...result, notice: NOTICE })));
  let refusals = 1;
  await withTurns(store, harness, async (alasio, queued) => {
    const told = () => queued.texts.filter(({ text }) => text === NOTICE).length;
    await alasio.runPromise(turnOf(conversationId, "first"));
    assert.equal(told(), 0, "the store refused it");
    await alasio.runPromise(turnOf(conversationId, "second"));
    assert.equal(told(), 1);
    await alasio.runPromise(turnOf(conversationId, "third"));
    assert.equal(told(), 1, "told once");
  }, { refusesText: ({ text }) => text === NOTICE && (refusals -= 1) >= 0 });
});

test("a notice one alasio told a conversation is told by no other on its store, as another replica or one restarted would be", async () => {
  const { store, conversationId } = await conversation();
  const NOTICE = "This Codex login doesn't offer gpt-5.6-sol, so Codex used gpt-5.6-terra, the login's default (at high effort). Use /model to choose another.";
  const harness = scriptedHarness((params) => Effect.map(answered(params, "Done."), (result) => ({ ...result, notice: NOTICE })));
  const told = (queued: QueuedToOutbox) => queued.texts.filter(({ text }) => text === NOTICE).length;
  assert.equal(await withTurns(store, harness, async (alasio, queued) => (await alasio.runPromise(turnOf(conversationId, "first")), told(queued))), 1);
  assert.equal(await withTurns(store, harness, async (alasio, queued) => (await alasio.runPromise(turnOf(conversationId, "second")), told(queued))), 0);
});

test("a notice whose reply could not be queued is not recorded as told, the two being one transaction", async () => {
  const { store, schema, conversationId } = await conversation();
  const pool = poolOf(store);
  // A reply the store cannot queue, as its outbox refuses it.
  await pool.query(`alter table ${schema}.telegram_outbox add constraint refused check (text <> 'refused') not valid`);
  const refused = await Effect.runPromise(Effect.flip(store.enqueueOutboxNotice(conversationId, { chatId: CONVERSATION_CHAT, text: "refused" })));
  assert.equal(refused._tag, "StoreError");
  assert.deepEqual((await pool.query(`select notice from ${schema}.conversation_notices`)).rows, []);
  assert.equal(await run(store.enqueueOutboxNotice(conversationId, { chatId: CONVERSATION_CHAT, text: "told" })), true);
  assert.equal(await run(store.enqueueOutboxNotice(conversationId, { chatId: CONVERSATION_CHAT, text: "told" })), false);
  assert.equal((await pool.query(`select 1 from ${schema}.telegram_outbox where text = 'told'`)).rows.length, 1);
});

test("replies a pod tells of, however often, are delivered once at a time, and once more where told of meanwhile, so no pod drives alasio's delivery", async () => {
  const { store, conversationId } = await conversation();
  // Each delivery's read of the replies due takes a while, counted.
  let reads = 0;
  let reading = 0;
  let most = 0;
  const counted: Store["Service"] = {
    ...store,
    getCompletedResponsesPendingDelivery: Effect.acquireUseRelease(
      Effect.sync(() => void ((reads += 1), (most = Math.max(most, ++reading)))),
      () => Effect.andThen(Effect.sleep("100 millis"), store.getCompletedResponsesPendingDelivery),
      () => Effect.sync(() => void (reading -= 1)),
    ),
  };
  await withTurns(counted, scriptedHarness((params) => answered(params, "Done.")), async (alasio) => {
    const before = reads;
    await alasio.runPromise(Effect.flatMap(Pools, (pools) => Effect.forEach(Array.from({ length: 30 }), () => pools.tell("harness.background-response", conversationId), { discard: true })));
    await until("the replies told of to be delivered", () => reads > before);
    await sleep(1_000);
    assert.equal(most, 1, "two deliveries at once");
    assert.ok(reads - before <= 3, `${reads - before} deliveries of 30 tellings`);
  });
});
