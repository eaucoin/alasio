import assert from "node:assert/strict";
import test from "node:test";

import { Effect, Exit, Layer, Schedule } from "effect";

import { CodexAppServer } from "../src/codex/app-server/client.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { holdConversations } from "../src/harness/holds.ts";
import { HarnessHost } from "../src/harness/host.ts";
import { Harnesses } from "../src/harness/index.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { StoreError } from "../src/persistence/sql.ts";
import { Store, TurnFenced } from "../src/persistence/store.ts";
import { holdsIn, newSchema, run, testPool, testStore } from "./support/store.ts";

/** A conversation of Codex's in a new store, in `schema`, and a turn claim of it. */
async function conversation() {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: "7", user: { id: 7 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  const claim = (messageId: string) => run(store.claimTurn({ conversationId, chatId: "7", messageId, harness: CODEX_HARNESS, prompt: messageId }));
  return { store, schema, conversationId, claim };
}

/** That `effect` failed as a write of the turn of `epoch` refused, its conversation claimed since: with TurnFenced, not as the store fails. */
async function refused(effect: Effect.Effect<unknown, unknown>, epoch: number): Promise<void> {
  const exit = await Effect.runPromiseExit(effect);
  assert.ok(Exit.isFailure(exit), "the write was made");
  const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail");
  assert.ok(failure && failure._tag === "Fail");
  assert.ok(failure.error instanceof TurnFenced, String(failure.error));
  assert.equal(failure.error.epoch, epoch);
}

test("each turn claims its conversation under an epoch one more than the turn's before it, working, its outcome cleared, and ends under its own", async () => {
  const { store, conversationId, claim } = await conversation();
  assert.equal(await claim("1"), 1);
  assert.equal(await run(store.endTurn(conversationId, 1, "completed")), true);
  const ended = await run(store.getTurn(conversationId));
  assert.deepEqual([ended?.state, ended?.status, ended?.outcome], ["completed", "ended", "completed"]);
  assert.equal(await run(store.getActiveTurn(conversationId)), null);
  // An ended turn ends no more.
  assert.equal(await run(store.endTurn(conversationId, 1, "failed")), false);
  assert.equal((await run(store.getTurn(conversationId)))?.outcome, "completed");
  assert.equal(await claim("2"), 2);
  const turn = await run(store.getActiveTurn(conversationId));
  assert.deepEqual([turn?.epoch, turn?.status, turn?.outcome, turn?.waiting_on, turn?.status_outbox_id], ["2", "working", null, null, null]);
});

test("a turn's writes, and its harness's, hold until another turn claims the conversation, and are refused after", async () => {
  const { store, conversationId, claim } = await conversation();
  const first = store.turnPersistence(conversationId, await claim("1"));
  const response = await run(first.createPendingResponse("7", "1", null));
  await run(first.updateActiveTurnPendingResponseId(response));
  await run(first.updateActiveTurnSessionId("session-1"));
  await run(first.appendBlocksToPending(response, [{ type: "text", content: "Working.", phase: "commentary" }]));
  await run(first.updatePendingSessionId(response, "session-1"));
  assert.equal((await run(store.getActiveTurn(conversationId)))?.session_id, "session-1");
  assert.equal((await run(store.getMount(conversationId))).sessionId, "session-1");

  // Another turn claims it: the first's every write is refused, and changes nothing.
  const second = store.turnPersistence(conversationId, await claim("2"));
  await refused(first.createPendingResponse("7", "1", null), 1);
  await refused(first.updateActiveTurnPendingResponseId(response), 1);
  await refused(first.updateActiveTurnSessionId("session-stale"), 1);
  await refused(first.appendBlocksToPending(response, [{ type: "text", content: "Stale.", phase: "final_answer" }]), 1);
  await refused(first.markPendingResponseComplete(response), 1);
  await refused(first.updatePendingSessionId(response, "session-stale"), 1);
  await refused(first.markPendingAsPosted(response), 1);
  await refused(first.updateSessionUsage("session-1", { cacheReadInputTokens: 10 }), 1);
  await refused(first.setWaiting("workflow run 1 (watch)"), 1);
  await refused(first.recordRestartEvent({ cause: "self_induced", channel: "7", thread_ts: "1", session_id: null }), 1);
  assert.equal((await run(store.getMount(conversationId))).sessionId, "session-1");
  assert.equal(await run(store.getRestartEvent(conversationId)), null);
  assert.equal(await run(store.getSessionTokens("session-1")), 0);
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  assert.equal((await run(store.getActiveTurn(conversationId)))?.status, "working");

  // The second's hold, its response its own.
  const own = await run(second.createPendingResponse("7", "2", null));
  await run(second.appendBlocksToPending(own, [{ type: "text", content: "Done.", phase: "final_answer" }]));
  await run(second.markPendingResponseComplete(own));
  await run(second.updateSessionUsage("session-2", { cacheReadInputTokens: 10 }));
  assert.deepEqual((await run(store.getCompletedResponsesPendingDelivery)).map(({ id }) => id), [own]);
  assert.equal(await run(store.getSessionTokens("session-2")), 10);
  // Its writes hold on after it ended, until another claims the conversation.
  assert.equal(await run(store.endTurn(conversationId, 2, "completed")), true);
  await run(second.markPendingAsPosted(own));
});

test("what alasio does with a turn holds only under the turn's epoch: a turn superseded ends, interrupts, records and names nothing of the one after it", async () => {
  const { store, conversationId, claim } = await conversation();
  const first = await claim("1");
  const second = await claim("2");
  assert.equal(await run(store.endTurn(conversationId, first, "failed")), false);
  assert.equal(await run(store.setTurnStatus(conversationId, first, "interrupted")), false);
  assert.equal(await run(store.setTurnStatusMessage(conversationId, first, "status-of-the-first")), false);
  await run(store.recordExternalRestartEvent(conversationId, first));
  const turn = await run(store.getActiveTurn(conversationId));
  assert.deepEqual([turn?.epoch, turn?.state, turn?.status, turn?.status_outbox_id], [String(second), "active", "working", null]);
  assert.equal(await run(store.getRestartEvent(conversationId)), null);

  // The second's own: a restart recorded under it is let go of as it ends.
  assert.equal(await run(store.setTurnStatusMessage(conversationId, second, "status-of-the-second")), true);
  await run(store.recordExternalRestartEvent(conversationId, second));
  assert.equal((await run(store.getRestartEvent(conversationId)))?.cause, "external_or_unknown");
  assert.equal(await run(store.endTurn(conversationId, second, "stopped")), true);
  assert.equal(await run(store.getRestartEvent(conversationId)), null);
  const ended = await run(store.getTurn(conversationId));
  assert.deepEqual([ended?.outcome, ended?.status_outbox_id], ["stopped", "status-of-the-second"]);
});

test("a write racing a claim waits for it, and is refused once it commits", async () => {
  const { store, schema, conversationId, claim } = await conversation();
  const first = store.turnPersistence(conversationId, await claim("1"));
  const response = await run(first.createPendingResponse("7", "1", null));
  const client = await (await testPool()).connect();
  try {
    // A claim, in a transaction of its own, not yet committed.
    await client.query("begin");
    await client.query(`update ${schema}.turns set epoch = epoch + 1 where id = $1`, [conversationId]);
    let settled = 0;
    const writes = [
      first.appendBlocksToPending(response, [{ type: "text", content: "Late.", phase: "final_answer" }]),
      first.updateSessionUsage("session-1", { cacheReadInputTokens: 5 }),
      first.recordRestartEvent({ cause: "self_induced", channel: "7", thread_ts: "1", session_id: null }),
    ].map((write) => Effect.runPromiseExit(write).finally(() => void (settled += 1)));
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.equal(settled, 0, "a write did not wait for the claim");
    await client.query("commit");
    for (const exit of await Promise.all(writes)) {
      assert.ok(Exit.isFailure(exit));
      const failure = exit.cause.reasons.find((reason) => reason._tag === "Fail");
      assert.ok(failure?._tag === "Fail" && failure.error instanceof TurnFenced, String(exit));
    }
  } finally {
    client.release();
  }
  assert.equal(await run(store.getSessionTokens("session-1")), 0);
  assert.equal(await run(store.getRestartEvent(conversationId)), null);
});

test("a turn says what it waits on while it works, and works again once it is past it; one left for a restart, or ended, says what it said", async () => {
  const { store, conversationId, claim } = await conversation();
  const epoch = await claim("1");
  const writer = store.turnPersistence(conversationId, epoch);
  await run(writer.setWaiting("workflow run 42 (watch)"));
  assert.deepEqual(await statusOf(store, conversationId), ["waiting", "workflow run 42 (watch)", null]);
  await run(writer.setWaiting(null));
  assert.deepEqual(await statusOf(store, conversationId), ["working", null, null]);
  assert.equal(await run(store.setTurnStatus(conversationId, epoch, "interrupted")), true);
  await run(writer.setWaiting("workflow run 43 (poll)"));
  assert.deepEqual(await statusOf(store, conversationId), ["interrupted", null, null]);
  assert.equal(await run(store.endTurn(conversationId, epoch, "incomplete")), true);
  await run(writer.setWaiting("workflow run 44 (poll)"));
  const ended = await run(store.getTurn(conversationId));
  assert.deepEqual([ended?.status, ended?.waiting_on, ended?.outcome], ["ended", null, "incomplete"]);
  assert.equal(await claim("2"), epoch + 1);
});

/** The conversation's active turn's status, what it waits on, and its outcome. */
async function statusOf(store: Awaited<ReturnType<typeof testStore>>, conversationId: string) {
  const turn = await run(store.getActiveTurn(conversationId));
  return [turn?.status, turn?.waiting_on, turn?.outcome];
}

test("a fork's hold is a row of the store while it lasts, held only where no turn runs, and no turn starts under it", async () => {
  const { store, schema, conversationId } = await conversation();
  const reasons = () => holdsIn(schema);
  await Effect.runPromise(Effect.gen(function*() {
    const activeTurns = yield* ActiveTurns;
    const host = yield* HarnessHost;
    yield* Effect.scoped(Effect.gen(function*() {
      assert.equal(yield* holdConversations(host, store, [conversationId], "forking session fs-a into fs-b"), true);
      assert.deepEqual(yield* Effect.promise(reasons), [[conversationId, "forking session fs-a into fs-b"]]);
      assert.deepEqual([(yield* host.status(conversationId)).busy, (yield* host.status(conversationId)).held], [true, true]);
      assert.equal(yield* host.reserve(conversationId), false);
      // A second hold of it, while the first lasts, is none.
      assert.equal(yield* Effect.scoped(holdConversations(host, store, [conversationId], "forking again")), false);
      assert.deepEqual(yield* Effect.promise(reasons), [[conversationId, "forking session fs-a into fs-b"]]);
    }));
    assert.deepEqual(yield* Effect.promise(reasons), []);
    // A turn runs: no hold, and no row.
    yield* Effect.scoped(Effect.gen(function*() {
      yield* activeTurns.register(conversationId, { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false });
      assert.equal(yield* holdConversations(host, store, [conversationId], "forking"), false);
      assert.deepEqual(yield* Effect.promise(reasons), []);
    }));
  }).pipe(Effect.provide(HarnessHost.layer.pipe(
    Layer.provideMerge(Harnesses.layer({})),
    Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, Layer.succeed(Store, store))),
  ))));
});

/** Runs `body` with the harness host of `store`, in alasio's process, and its running turns. */
const withHost = <A>(store: Store["Service"], body: (host: HarnessHost["Service"]) => Effect.Effect<A, unknown>) =>
  Effect.runPromise(Effect.flatMap(HarnessHost, body).pipe(Effect.provide(HarnessHost.layer.pipe(
    Layer.provideMerge(Harnesses.layer({})),
    Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, Layer.succeed(Store, store))),
  ))));

test("a fork's hold whose letting go fails is let go of once the store takes it, tried until it is through", async () => {
  const { store, schema, conversationId } = await conversation();
  let refusals = 2;
  const refusing: Store["Service"] = {
    ...store,
    releaseHolds: (holder) =>
      Effect.suspend(() => (refusals > 0 ? ((refusals -= 1), Effect.fail(new StoreError({ cause: new Error("Neon is not reached") }))) : store.releaseHolds(holder))),
  };
  await withHost(refusing, (host) =>
    Effect.scoped(holdConversations(host, refusing, [conversationId], "forking", { letGoAgain: Schedule.spaced("20 millis") })).pipe(
      Effect.tap((held) => Effect.sync(() => assert.equal(held, true))),
    ));
  for (let waited = 0; (await holdsIn(schema)).length > 0 && waited < 2000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
  assert.deepEqual([await holdsIn(schema), refusals], [[], 0]);
  assert.equal(await run(store.isConversationHeld(conversationId)), false);
});

test("a fork's hold lasts as its holder renews it, and no longer than it lasts unrenewed, so a holder stopped or stuck pins no conversation", async () => {
  const { store, conversationId } = await conversation();
  await withHost(store, (host) =>
    Effect.gen(function*() {
      // Renewed every 100 ms, it outlasts its 300 ms many times over.
      yield* Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* holdConversations(host, store, [conversationId], "forking", { ttl: "300 millis", renewal: "100 millis" }), true);
        yield* Effect.sleep("900 millis");
        assert.equal((yield* host.status(conversationId)).held, true);
      }));
      // A holder that never lets go, nor renews (one stopped, or stuck): its hold ends as it expires.
      assert.notEqual(yield* store.holdConversations([conversationId], "forking, then stopped", 300), null);
      assert.deepEqual([(yield* host.status(conversationId)).held, yield* Effect.scoped(host.reserve(conversationId))], [true, false]);
      yield* Effect.sleep("500 millis");
      assert.deepEqual([(yield* host.status(conversationId)).busy, yield* Effect.scoped(host.reserve(conversationId))], [false, true]);
      // And another hold takes its place.
      assert.equal(yield* Effect.scoped(holdConversations(host, store, [conversationId], "forking again")), true);
    }));
});

test("holds of conversations some of which are the same, made at once, are each all of their conversations or none, leaving no row of one that held none", async () => {
  const { store, schema } = await conversation();
  const [a, b, c] = await Promise.all(["11", "12", "13"].map((chatId) => run(store.upsertConversation({ chatId, user: { id: Number(chatId) } }))));
  if (!a || !b || !c) throw new Error("the conversations were not made");
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const hold = (ids: readonly string[]): Promise<string | null> => run(store.holdConversations(ids, `forking ${ids.join(" and ")}`, 60_000));
    const held: (string | null)[] = await Promise.all([hold([a, b]), hold([b, c])]);
    const first: string | null = held[0] ?? null;
    const second: string | null = held[1] ?? null;
    const rows = (await holdsIn(schema)).map(([conversationId]) => conversationId).sort();
    // b is one's or the other's: the one that has it holds all of its, and the other none.
    assert.ok((first === null) !== (second === null), `attempt ${attempt}: ${first} and ${second}`);
    assert.deepEqual(rows, (first !== null ? [a, b] : [b, c]).sort(), `attempt ${attempt}`);
    await run(store.releaseHolds(null));
  }
});
