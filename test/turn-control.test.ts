/**
 * alasio's turns, as Turns runs them on a harness a test scripts: when a turn may start,
 * what it ends, what a harness's own turn claims, what its status message says, what
 * becomes of the prompts held while a turn ran and of the buttons that settle them, and
 * what a start does with what the last alasio left.
 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { Context, Deferred, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";
import { TestClock } from "effect/testing";

// Before alasio's modules, which make their instruments as they load.
import { added } from "./support/metrics.ts";
import { createCommandEventPolicy } from "../src/codex/command-event-policy.ts";
import { buildLostHarnessText } from "../src/operator/lost-prompt.ts";
import { makeStatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { discardHeld, queueHeld, steerWithHeld, swerveTo } from "../src/harness/commands.ts";
import { HarnessHost } from "../src/harness/host.ts";
import { RECOVERIES } from "../src/harness/recoveries.ts";
import { HARNESS_BINDING } from "../src/harness/pooled-host.ts";
import type { Harness, OwnTurns, TurnParams, TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { CurrentWorkerFence, Store, TurnFenced } from "../src/persistence/store.ts";
import { forgetfulPersistence } from "./support/persistence.ts";
import { bindHarness, holdsOf, OOM_LIMIT, POOLED_SCHEMA, Pools, pooledStore, poolOf } from "./support/pools.ts";
import { run } from "./support/store.ts";
import { type QueuedToOutbox, recordingOutbox, type TestAlasio, withServices } from "./support/turns.ts";
import { eventually } from "./support/wait.ts";

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

/** Waits for `check` to hold, as eventually does, for its outer bound at most, the failure naming `what`. */
async function until(what: string, check: () => Promise<boolean> | boolean): Promise<void> {
  await eventually(what, async () => ((await check()) ? true : undefined));
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

test("a prompt an alasio was handing to a turn to steer as it stopped is asked about, quoted, once, as its conversation is taken up, and run only as the operator says", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  // Left steering: taken from being held, and handed to the turn, which may or may not have taken it, as alasio stopped.
  const { job } = await heldPrompt(store, conversationId, "steer this");
  assert.equal((await run(store.settleHeldPromptJob(job.id, "steering")))?.state, "steering");
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => Effect.andThen(turns.takeUnheld, turns.resumePendingPrompts)));
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

test("alasio taking up the conversations no alasio drives attaches again to the turn one left running as it was killed: one its pod knows nothing of runs again, once, its status message saying so; and it leaves the holds of others' forks", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => (ran.push(params.prompt), answered(params, "Checked.")));
  // Another conversation's fork, which another alasio runs.
  const forked = await run(store.upsertConversation({ chatId: "77", user: { id: 77 } }));
  await run(store.holdConversations([forked], "forking", 60_000));
  // Left by an alasio killed mid-turn: its turn, working, with its status message.
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "9", harness: CODEX_HARNESS, prompt: "check" }));
  assert.equal(await run(store.setTurnStatusMessage(conversationId, epoch, "status-of-the-killed")), true);
  const killed = store.turnPersistence(conversationId, epoch);
  const partial = await run(killed.createPendingResponse(String(CONVERSATION_CHAT), "9", null));
  await run(killed.updateActiveTurnPendingResponseId(partial));
  await withTurns(store, harness, async (alasio) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.takeUnheld));
    assert.deepEqual(await holdsOf(store), [[forked, "forking"]]);
    // Its conversation's pod knows nothing of it, as one bound anew after its own was lost would not.
    await until("the lost turn to run again", () => ran.length === 1);
    assert.deepEqual(ran, [buildLostHarnessText(CODEX_HARNESS)]);
    // Said as the recovery is staged, in its transaction.
    assert.ok((await run(store.getDueOutbox(50))).some(({ text }) => text === "Codex's harness was lost during this turn; it runs again on another."));
    // Its partial response let go of, never to be delivered; the recovery's own answer is.
    assert.deepEqual((await run(store.getCompletedResponsesPendingDelivery)).map(({ id }) => id).filter((id) => id === partial), []);
  });
});

test("alasio taking up a conversation ends a turn whose job the agent finished as its alasio stopped, completed, its status message saying so", async () => {
  const { store, conversationId } = await conversation();
  const harness = scriptedHarness((params) => answered(params, "unused"));
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "5", prompt: "finish" }));
  await run(store.claimNextPromptJob(conversationId));
  await run(store.markPromptJobUpstreamCompleted(job.id, "session-1", "turn-1"));
  const epoch = await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "5", harness: CODEX_HARNESS }));
  await run(store.setTurnStatusMessage(conversationId, epoch, "status-of-the-finished"));
  await withTurns(store, harness, async (alasio, queued) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.takeUnheld));
    await until("the turn to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
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
    createCommandEventPolicy({ persistence: { ...forgetfulPersistence, setWaiting } });
  const recorded = policyOn((waitingOn) => Effect.sync(() => void waits.push(waitingOn)));
  await Effect.runPromise(recorded.inspectCommand("gh run watch 12345678"));
  await Effect.runPromise(recorded.inspectCommand("gh run watch 12345678"));
  await Effect.runPromise(recorded.inspectCommand("ls"));
  assert.deepEqual(waits, ["workflow run 12345678 (watch)", null]);
  const failing = policyOn(() => Effect.fail(new StoreError({ cause: new Error("the store is down") })));
  assert.deepEqual(await Effect.runPromise(failing.inspectCommand("gh run watch 12345678")), { blocked: false });
  const fenced = policyOn(() => Effect.fail(new TurnFenced({ threadKey: "telegram:41", epoch: 1 })));
  const exit = await Effect.runPromiseExit(fenced.inspectCommand("gh run watch 12345678"));
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

/** A Codex harness whose turns each wait for `release`, then answer `text`, its prompts kept in `ran`. */
const waitingHarness = (release: Deferred.Deferred<void>, ran: string[], text = "Done.") =>
  scriptedHarness((params) => Effect.andThen(Effect.sync(() => void ran.push(params.prompt)), Effect.andThen(Deferred.await(release), answered(params, text))));

/**
 * Starts a turn of `prompt` in the conversation through turns of an alasio of their own, over
 * the same store and pods, and once its pod runs it, stops that alasio: the turn left on its
 * pod, for the test's alasio to attach to as the next one would.
 */
async function leftOnItsPod(alasio: TestAlasio, store: Store["Service"], conversationId: string, ran: string[], prompt = "go"): Promise<void> {
  const scope = await alasio.runPromise(Scope.make());
  const stopping = Context.get(await alasio.runPromise(Layer.buildWithScope(Turns.layer({ pod: "alasio-stopping" }), scope)), Turns);
  alasio.runFork(stopping.run({ conversationId, chatId: CONVERSATION_CHAT, messageId: prompt, prompt, traceparent: null }));
  await until("the turn to run on its pod", () => ran.includes(prompt));
  await alasio.runPromise(Scope.close(scope, Exit.void));
  assert.equal((await run(store.getActiveTurn(conversationId)))?.status, "interrupted");
}

/** Takes up the conversations no alasio drives, attaching again to the turns left, as alasio starting does. */
const reattached = (alasio: TestAlasio) => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.takeUnheld));

test("a turn alasio stops driving runs on on its pod, and the next start attaches to it: its reply delivered once, as its pod ends it, and its binding lost after runs nothing again", async () => {
  const { store, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran) }, outbox: outbox.layer }, async (alasio) => {
    await leftOnItsPod(alasio, store, conversationId, ran);
    await reattached(alasio);
    // The pod ends the turn after the start, which waits for it.
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await until("its reply", () => outbox.queued.texts.some(({ text }) => text === "Done."));
    await until("its turn to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
    assert.equal((await run(store.getTurn(conversationId)))?.outcome, "completed");
    // Its binding lost after: nothing of it is run again.
    await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, "lost")));
    await sleep(500);
    assert.deepEqual(ran, ["go"]);
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
    assert.equal(outbox.queued.texts.filter(({ text }) => text === "Done.").length, 1);
  });
});

test("a turn's pod lost as alasio attaches to it again runs the turn again, once, whichever lets go of it first, and delivers nothing of the lost one", async () => {
  for (const reason of ["deleted", "lost"] as const) {
    const { store, conversationId } = await conversation();
    const release = Effect.runSync(Deferred.make<void>());
    const ran: string[] = [];
    const outbox = recordingOutbox();
    await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran, "Recovered.") }, outbox: outbox.layer }, async (alasio) => {
      await leftOnItsPod(alasio, store, conversationId, ran);
      const counted = await added(RECOVERIES, async () => {
        // Attaching, which waits on the pod; the pod lost meanwhile, told of as the gateway tells it.
        await reattached(alasio);
        await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, reason)));
        await until("the turn to run again", () => ran.length === 2);
      });
      // Counted once, as run again, by whichever found it lost first.
      assert.deepEqual(Object.entries(counted).map(([attributes, count]) => `${attributes.replace(/alasio\.harness\.recovery\.by=(lost|reattach),/u, "")} ${count}`), [`alasio.harness=${CODEX_HARNESS},alasio.harness.recovery.outcome=staged 1`], reason);
      assert.deepEqual(ran, ["go", buildLostHarnessText(CODEX_HARNESS)], reason);
      await alasio.runPromise(Deferred.succeed(release, undefined));
      await until("the recovery's reply", () => outbox.queued.texts.some(({ text }) => text === "Recovered."));
      await sleep(500);
      assert.equal(ran.length, 2, `${reason}: run again more than once`);
      assert.equal(outbox.queued.texts.filter(({ text }) => text === "Recovered.").length, 1, reason);
    });
  }
});

test("a turn whose pod is lost as alasio drives it is run again, once, as the drive and the binding's loss both take it as lost", async () => {
  const { store, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran, "Recovered.") }, outbox: outbox.layer }, async (alasio) => {
    const driving = alasio.runFork(turnOf(conversationId, "go"));
    await until("the turn to run on its pod", () => ran.length > 0);
    await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, "broken")));
    await alasio.runPromise(Fiber.await(driving));
    await until("the turn to run again", () => ran.length === 2);
    assert.deepEqual(ran, ["go", buildLostHarnessText(CODEX_HARNESS)]);
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await until("the recovery's reply", () => outbox.queued.texts.some(({ text }) => text === "Recovered."));
    await sleep(500);
    assert.equal(ran.length, 2, "run again more than once");
  });
});

test("a recovery whose pod is lost too is given up, not run again, and the chat is told once to send the message again", async () => {
  const { store, schema, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  const GIVEN_UP = "Codex's harness was lost during this turn, which was itself run again as one was lost before. Send the message again to run it.";
  /** How often the chat was told the turn was given up, on its status message or by a message of its own. */
  const toldGivenUp = async () => Number((await poolOf(store).query(`select count(*) from ${schema}.telegram_outbox where text = $1`, [GIVEN_UP])).rows[0]?.count);
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran, "Recovered.") }, outbox: outbox.layer }, async (alasio) => {
    const counted = await added(RECOVERIES, async () => {
      const driving = alasio.runFork(turnOf(conversationId, "go"));
      await until("the turn to run on its pod", () => ran.length === 1);
      await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, "broken")));
      await alasio.runPromise(Fiber.await(driving));
      await until("the turn to run again", () => ran.length === 2);
      // The recovery's pod lost as well, as the gateway tells it, however often.
      await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, "broken")));
      await until("the chat to be told", async () => (await toldGivenUp()) > 0);
      await until("the recovery to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
    });
    // Each loss counted once, by whichever found it lost first: the turn run again, then its recovery given up.
    assert.deepEqual(
      Object.entries(counted).map(([attributes, count]) => `${attributes.replace(/alasio\.harness\.recovery\.by=(lost|reattach),/u, "")} ${count}`).sort(),
      [`alasio.harness=${CODEX_HARNESS},alasio.harness.recovery.outcome=given-up 1`, `alasio.harness=${CODEX_HARNESS},alasio.harness.recovery.outcome=staged 1`],
    );
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await sleep(500);
    assert.equal(ran.length, 2, "run again more than once");
    assert.equal(await toldGivenUp(), 1);
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
    assert.equal(outbox.queued.texts.filter(({ text }) => text === "Recovered.").length, 0, "the given-up recovery's reply delivered");
  });
});

test("a turn whose pod's host is killed for its memory is ended, not run again, and the chat is told once of the limit it ran out of", async () => {
  const { store, schema, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  const ENDED = `Codex's harness ran out of its memory (${OOM_LIMIT}) during this turn and was stopped. The turn is not run again, as it would run out again; send a message to go on, asking for less at once.`;
  const toldEnded = async () => Number((await poolOf(store).query(`select count(*) from ${schema}.telegram_outbox where text = $1`, [ENDED])).rows[0]?.count);
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran, "Recovered.") }, outbox: outbox.layer }, async (alasio) => {
    const counted = await added(RECOVERIES, async () => {
      const driving = alasio.runFork(turnOf(conversationId, "go"));
      await until("the turn to run on its pod", () => ran.length === 1);
      // Its host killed for its memory, as the gateway tells it.
      await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.lose(conversationId, "oom")));
      await alasio.runPromise(Fiber.await(driving));
      await until("the chat to be told", async () => (await toldEnded()) > 0);
      await until("the turn to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
    });
    assert.deepEqual(
      Object.entries(counted).map(([attributes, count]) => `${attributes.replace(/alasio\.harness\.recovery\.by=(lost|reattach),/u, "")} ${count}`),
      [`alasio.harness=${CODEX_HARNESS},alasio.harness.recovery.outcome=ended 1`],
    );
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await sleep(500);
    assert.deepEqual(ran, ["go"], "run again");
    assert.equal(await toldEnded(), 1);
    assert.equal((await run(store.getTurn(conversationId)))?.outcome, "stopped");
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
  });
});

test("a turn alasio attaches to again, its pod gone as its host ran out of its memory, waits for the release to be heard, which ends it, never run again", { timeout: 60_000 }, async () => {
  const { store, schema, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  const ENDED = `Codex's harness ran out of its memory (${OOM_LIMIT}) during this turn and was stopped. The turn is not run again, as it would run out again; send a message to go on, asking for less at once.`;
  const toldEnded = async () => Number((await poolOf(store).query(`select count(*) from ${schema}.telegram_outbox where text = $1`, [ENDED])).rows[0]?.count);
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran, "Recovered.") }, outbox: outbox.layer, lossHeardWithin: "20 seconds" }, async (alasio) => {
    await leftOnItsPod(alasio, store, conversationId, ran);
    const counted = await added(RECOVERIES, async () => {
      // Its pod gone, its release told of only once alasio, attaching, finds its next pod knows nothing of it.
      const { tell } = await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.loseUntold(conversationId, "oom")));
      const attaching = reattached(alasio);
      await sleep(2_000);
      assert.equal(ran.length, 1, "run again before its release was heard");
      await alasio.runPromise(tell);
      await attaching;
      await until("the chat to be told", async () => (await toldEnded()) > 0);
      await until("the turn to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
      await sleep(500);
    });
    // Ended by its release, which knew why; nothing found it lost as it was attached to.
    assert.deepEqual(
      Object.entries(counted).map(([attributes, count]) => `${attributes} ${count}`),
      [`alasio.harness=${CODEX_HARNESS},alasio.harness.recovery.by=lost,alasio.harness.recovery.outcome=ended 1`],
    );
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await sleep(500);
    assert.deepEqual(ran, ["go"], "run again");
    assert.equal(await toldEnded(), 1);
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
  });
});

test("a binding's loss heard late, once the conversation was bound anew and a turn begun on its new pod, ends nothing of that turn", async () => {
  for (const reason of ["broken", "lost"] as const) {
    const { store, conversationId } = await conversation();
    const ran: string[] = [];
    const release = Effect.runSync(Deferred.make<void>());
    // The first turn answers at once; the next waits for the test.
    const harness = scriptedHarness((params) =>
      Effect.andThen(Effect.sync(() => void ran.push(params.prompt)), Effect.andThen(params.prompt === "next" ? Deferred.await(release) : Effect.void, answered(params, `Done ${params.prompt}.`)))
    );
    const outbox = recordingOutbox();
    await withServices({ store, harnesses: { [CODEX_HARNESS]: harness }, outbox: outbox.layer }, async (alasio) => {
      assert.equal(await alasio.runPromise(turnOf(conversationId, "first")), true);
      // Its pod lost after, as an upgrade --now restarting the cluster loses it; told of only once the next turn runs on another.
      const { tell } = await alasio.runPromise(Effect.flatMap(Pools, (pools) => pools.loseUntold(conversationId, reason)));
      const next = alasio.runFork(turnOf(conversationId, "next"));
      await until("the next turn to run on its pod", () => ran.length === 2);
      await alasio.runPromise(tell);
      await sleep(500);
      assert.equal((await run(store.getActiveTurn(conversationId)))?.thread_ts, "next", reason);
      await alasio.runPromise(Deferred.succeed(release, undefined));
      assert.equal(await alasio.runPromise(Fiber.join(next)), true);
      assert.deepEqual(ran, ["first", "next"], reason);
      assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
      assert.equal(outbox.queued.texts.filter(({ text }) => text === "Done next.").length, 1, reason);
    });
  }
});

test("a pod answering with a session another conversation has, of a turn, a fresh session or a turn attached to again, gets it for its conversation in none, each refused", async () => {
  const { store, conversationId } = await conversation();
  // Another conversation's session, as its own turns named it.
  const other = await run(store.upsertConversation({ chatId: "42", user: { id: 42 } }));
  await run(store.setActiveHarness(other, CODEX_HARNESS));
  await run(store.setSessionId(other, "theirs"));
  const ofIts = async () => (await run(store.getMount(conversationId))).sessionId;
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const answeringTheirs = (params: TurnParams) => Effect.map(answered(params, "Done."), (result) => ({ ...result, sessionId: "theirs" }));
  const harness: Harness = {
    ...scriptedHarness((params) => Effect.andThen(Effect.sync(() => void ran.push(params.prompt)), params.prompt === "left" ? Effect.andThen(Deferred.await(release), answeringTheirs(params)) : answeringTheirs(params))),
    startFreshSession: () => Effect.succeed("theirs"),
  };
  const outbox = recordingOutbox();
  await withServices({ store, harnesses: { [CODEX_HARNESS]: harness }, outbox: outbox.layer }, async (alasio) => {
    // A turn: it fails, its reply never delivered.
    assert.equal((await alasio.runPromise(Effect.flip(turnOf(conversationId, "go"))))._tag, "StoreError");
    assert.equal((await run(store.getTurn(conversationId)))?.outcome, "failed");
    assert.deepEqual(outbox.queued.texts.filter(({ text }) => text === "Done."), []);
    // A fresh session.
    assert.equal((await alasio.runPromise(Effect.flip(Effect.flatMap(Turns, (turns) => turns.startNewSession(conversationId)))))._tag, "StoreError");
    // A turn attached to again: ended failed, its status message saying so.
    await leftOnItsPod(alasio, store, conversationId, ran, "left");
    await reattached(alasio);
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await until("the turn attached to again to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
    assert.equal((await run(store.getTurn(conversationId)))?.outcome, "failed");
    assert.ok(outbox.queued.edits.some(({ text }) => text === "Codex hit an error."));
    assert.equal(await ofIts(), null);
  });
});

/** Turns of another alasio's, over the same store and pods as the test's, let go of as the scope it gives `use` closes. */
async function withAnotherAlasio<T>(alasio: TestAlasio, use: (another: Turns["Service"]) => Promise<T>): Promise<T> {
  const scope = await alasio.runPromise(Scope.make());
  try {
    return await use(Context.get(await alasio.runPromise(Layer.buildWithScope(Turns.layer({ pod: "alasio-another" }), scope)), Turns));
  } finally {
    await alasio.runPromise(Scope.close(scope, Exit.void));
  }
}

test("two alasios on one store, each scheduling a conversation's prompts at once, run each once, by the one worker holding the conversation's claim", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(Effect.sleep("50 millis")), Effect.andThen(answered(params, "Done."))));
  await withTurns(store, harness, (alasio) =>
    withAnotherAlasio(alasio, async (another) => {
      const jobs = [];
      for (const prompt of ["one", "two", "three"]) jobs.push(await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: prompt, prompt })));
      await Promise.all([alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.schedule(conversationId))), alasio.runPromise(another.schedule(conversationId))]);
      // And again as a prompt runs, as one queued through either would.
      await until("a prompt to run", () => ran.length > 0);
      await Promise.all([alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.schedule(conversationId))), alasio.runPromise(another.schedule(conversationId))]);
      await until("every prompt to run", async () => !(await run(store.hasOpenPromptJobs(conversationId))));
      assert.deepEqual(ran, ["one", "two", "three"]);
      // Claimed once each: no worker of the other's took one only to find the conversation busy.
      for (const job of jobs) assert.deepEqual([(await run(store.getPromptJob(job.id)))?.state, (await run(store.getPromptJob(job.id)))?.attempts], ["completed", 1]);
    })
  );
});

test("a turn whose alasio crashed is attached to again by another taking up the conversations no alasio drives once the crashed one's claim lapsed, not before", async () => {
  const { store, conversationId } = await conversation();
  const release = Effect.runSync(Deferred.make<void>());
  const ran: string[] = [];
  const outbox = recordingOutbox();
  await withServices({ store, harnesses: { [CODEX_HARNESS]: waitingHarness(release, ran) }, outbox: outbox.layer }, async (alasio) => {
    await leftOnItsPod(alasio, store, conversationId, ran);
    // Its alasio crashed rather than stopped: its claim held until it lapses.
    assert.ok(await run(store.claimWorker(conversationId, "crashed", 500)));
    await reattached(alasio);
    await alasio.runPromise(Deferred.succeed(release, undefined));
    await sleep(200);
    assert.equal((await run(store.getActiveTurn(conversationId)))?.status, "interrupted");
    assert.equal(outbox.queued.texts.some(({ text }) => text === "Done."), false);
    await sleep(400);
    await reattached(alasio);
    await until("its reply", () => outbox.queued.texts.some(({ text }) => text === "Done."));
    await until("its turn to end", async () => (await run(store.getActiveTurn(conversationId))) === null);
    assert.equal((await run(store.getTurn(conversationId)))?.outcome, "completed");
    assert.deepEqual(ran, ["go"]);
  });
});

test("a prompt job another alasio runs under its claim is left running by one taking up the conversations no alasio drives, and settled, and run again, once that claim lapsed", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  const holder = await run(store.claimWorker(conversationId, "other", 500));
  assert.ok(holder);
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "1", prompt: "never sent" }));
  // Claimed by the other alasio's worker, which has yet to send it to the agent.
  await run(store.claimNextPromptJob(conversationId).pipe(Effect.provideService(CurrentWorkerFence, holder)));
  await withTurns(store, harness, async (alasio) => {
    const takeUnheld = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.takeUnheld));
    await takeUnheld();
    // Nor taken up as a prompt queued through this alasio schedules its worker.
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.schedule(conversationId)));
    await sleep(200);
    assert.deepEqual([(await run(store.getPromptJob(job.id)))?.state, ran], ["running", []]);
    await sleep(400);
    await takeUnheld();
    await until("the job to run again", () => ran.length === 1);
    await until("the job to complete", async () => (await run(store.getPromptJob(job.id)))?.state === "completed");
    assert.equal((await run(store.getPromptJob(job.id)))?.attempts, 2);
  });
});

test("a turn a harness started on its own is its host's: no alasio taking up conversations attaches to it, nor runs it again, even taking its conversation's claim for what else was left", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  // Its own turn, as a pod's harness claims one, with no prompt of alasio's.
  await run(store.claimTurn({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "own", harness: CODEX_HARNESS }));
  await withTurns(store, harness, async (alasio) => {
    const takeUnheld = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.takeUnheld));
    assert.deepEqual(await run(store.unheldConversations), []);
    await takeUnheld();
    // And a job an alasio left running as it crashed, which takes the conversation up.
    const job = await run(store.enqueuePromptJob({ conversationId, chatId: String(CONVERSATION_CHAT), messageId: "1", prompt: "left" }));
    await run(store.claimNextPromptJob(conversationId));
    await takeUnheld();
    await until("the job left to run again", async () => (await run(store.getPromptJob(job.id)))?.state === "completed");
    assert.deepEqual(ran, ["left"]);
  });
});

test("a turn run outside a worker, its conversation's claim held by another alasio's worker as it asks, waits for that claim, and runs once it is let go of", async () => {
  const { store, conversationId } = await conversation();
  const ran: string[] = [];
  const harness = scriptedHarness((params) => Effect.sync(() => ran.push(params.prompt)).pipe(Effect.andThen(answered(params, "Done."))));
  // Another alasio's worker, at a pass that runs nothing, for a second.
  const other = await run(store.claimWorker(conversationId, "other", 60_000));
  assert.ok(other);
  setTimeout(() => void Effect.runPromise(store.releaseWorker(other)), 1_000);
  await withTurns(store, harness, async (alasio) => {
    assert.equal(await alasio.runPromise(turnOf(conversationId, "direct")), true);
    assert.deepEqual(ran, ["direct"]);
  });
});
