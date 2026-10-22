/**
 * A branch environment's alasio (cli/src/commands/branch.ts): its state made its own as
 * it first starts, and the sessions it inherited forked for it by its parent.
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { setTimeout as sleep } from "node:timers/promises";

import { Deferred, Duration, Effect, Exit, Fiber, Layer, Option, Scope } from "effect";

import { answerSignatureOf, CLOCK_SKEW, FORK_WAIT, ForkAsks, forkAnswered, forkRequestRecord, ForkRequests, parentForks, serveBranchForks } from "../src/branch/fork.ts";
import { forkToken } from "../src/branch/names.ts";
import { CodexAppServer } from "../src/codex/app-server/client.ts";
import { consumeEvents, EnvironmentEvents } from "../src/events/consume.ts";
import { eventRecord, type EventInput, EventNotTold, Events } from "../src/events/events.ts";
import type { TopicLog } from "../src/log/topic.ts";
import { Turns } from "../src/codex/turns.ts";
import { handleTextCommand } from "../src/operator/command-handler.ts";
import { Mounts } from "../src/operator/mounts.ts";
import { buildWorkspacePanel } from "../src/operator/workspace-control.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { HarnessHost } from "../src/harness/host.ts";
import { Harnesses } from "../src/harness/index.ts";
import type { ForkAnswer } from "../src/persistence/fork-repository.ts";
import { Store } from "../src/persistence/store.ts";
import { GatewayError } from "../src/sandbox/gateway-client.ts";
import { type NetMode, SessionSandboxes } from "../src/sandbox/index.ts";
import { takeCallbackQuery } from "../src/telegram/callback-handler.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { type MemoryTopic, memoryTopic } from "./support/topics.ts";
import { recordingOutbox, turnsStub, withServices } from "./support/turns.ts";
import { inProcessEvents } from "./support/host-events.ts";

/** A press taken from the store, then handled, as alasio handles one, its handling run in turn. */
const handleCallbackQuery = (query: Parameters<typeof takeCallbackQuery>[0]) => Effect.flatten(takeCallbackQuery(query));

test("a branch's state, as it first starts, keeps its conversations and loses what was in flight where it was branched from, once", async () => {
  const schema = newSchema();
  const main = await testStore({ schema });
  const conversationId = await run(main.upsertConversation({ chatId: 1001 }));
  await run(main.setActiveHarness(conversationId, "codex"));
  await run(main.setWorkingDirectory(conversationId, "sessionfs:fs-0123456789abcdef"));
  await run(main.recordTelegramUpdate({ update_id: 41, message: { message_id: 7, date: 0, from: { id: 1001, is_bot: false, first_name: "Operator" }, chat: { id: 1001, type: "private", first_name: "Operator" }, text: "queued" } }));
  await run(main.enqueuePromptJob({ conversationId, chatId: 1001, messageId: 7, prompt: "queued", harness: "codex" }));
  const writer = main.turnPersistence(conversationId, await run(main.claimTurn({ conversationId, chatId: "1001", messageId: "8", harness: "codex" })));
  const pending = await run(writer.createPendingResponse(1001, 8));
  await run(writer.updateActiveTurnPendingResponseId(pending));
  await run(writer.markPendingResponseComplete(pending));
  await run(main.enqueueOutboxText({ chatId: 1001, text: "on its way" }));
  const [button] = await run(main.createCallbackActions(conversationId, [{ kind: "refresh" }]));
  await run(main.setCodexLogin('{"tokens":{"refresh_token":"once"}}'));
  await run(main.withCodexLoginLock((login) => Effect.flatMap(login.get, (kept) => (kept ? login.refuse(kept.savedAt) : Effect.void))));
  // As a parent, a request of its branch's it answered; as a branch, an answer its own parent gave it.
  const answer: ForkAnswer = { outcome: "forked", netMode: "none", reason: null };
  await run(main.recordForkRequest("other", "request-answered", "fs-0123456789abcdef", answer));
  await run(main.withEventHandled("00000000-0000-4000-8000-000000000001", "workspace.fork-answered", (writes) => writes.recordForkAnswer("request-heard", "fs-0123456789abcdef", answer)));

  const branch = await testStore({ schema, branch: "try-codex" });
  assert.deepEqual(await run(branch.getMount(conversationId)), { harness: "codex", workingDirectory: "sessionfs:fs-0123456789abcdef", sessionId: null });
  assert.deepEqual(await run(branch.listWaitingPromptConversations), []);
  assert.equal(await run(branch.hasOpenPromptJobs(conversationId)), false);
  assert.deepEqual(await run(branch.getActiveTurns), []);
  assert.deepEqual(await run(branch.getCompletedResponsesPendingDelivery), []);
  assert.equal(await run(branch.getPendingOutboxCount), 0);
  assert.equal(await run(branch.getCodexLogin), null);
  assert.deepEqual((await (await testPool()).query(`select saved_at from ${schema}.codex_login_refusal`)).rows, []);
  assert.equal(await run(branch.consumeCallbackAction(button ?? "")), null);
  assert.equal(await run(branch.getState("branch")), "try-codex");
  assert.equal(await run(branch.forkRequestAnswered("other", "request-answered")), null);
  assert.equal(await run(branch.forkAnswerTo("request-heard")), null);

  // Started once: what the branch does itself is kept when it starts again.
  await run(branch.enqueuePromptJob({ conversationId, chatId: 1001, messageId: 9, prompt: "the branch's own", harness: "codex" }));
  assert.equal(await run((await testStore({ schema, branch: "try-codex" })).hasOpenPromptJobs(conversationId)), true);
  // A branch of the branch is made its own in turn.
  assert.equal(await run((await testStore({ schema, branch: "try-again" })).hasOpenPromptJobs(conversationId)), false);
});

/** A topic of the log, as `EnvironmentEvents.layer` and `ForkRequests.layer` are given one, for a stand-in to join. */
const logOf = (topic: string, group: string): TopicLog => ({ brokers: [], username: "alasio-test", passwordFile: "/nonexistent", topic, group });

/** The branch `branch`'s alasio, as it hears its parent's answers, signed with the token in `tokenFile`: its own store, its events topic, and its handling of them, until `scope` closes. */
async function branchHearing(branch: string, tokenFile: string, scope: Scope.Scope, events = memoryTopic(`events.${branch}`)) {
  const store = await testStore({ branch });
  await Effect.runPromise(Effect.forkIn(
    consumeEvents({ "workspace.fork-answered": forkAnswered({ branch, tokenFile }) }).pipe(
      Effect.provide(Layer.mergeAll(EnvironmentEvents.layer(logOf(`events.${branch}`, `alasio-events.${branch}`), { branch: true }, events.joinTopic), Layer.succeed(Store, store))),
    ),
    scope,
  ));
  return { store, events };
}

/** The record of the answer `answer` to the request `request` of the branch `branch`'s, for its session `volumeId`, signed with `token`: as its parent writes one. */
const answerRecord = (token: string, branch: string, request: string, volumeId: string, answer: ForkAnswer, signature = answerSignatureOf(token, { branch, request, volumeId, ...answer })): string => {
  const event: EventInput = {
    kind: "workspace.fork-answered",
    environment: branch,
    source: "alasio",
    subject: { kind: "workspace", key: `sessionfs:${volumeId}` },
    request,
    volumeId,
    ...answer,
    signature,
  };
  return eventRecord(event, Date.now()).value;
};

test("a branch's alasio hears an answer its parent wrote before the branch first read its events; main's alasio reads its own from the next written", async () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-branch-events-"));
  const scope = Effect.runSync(Scope.make());
  try {
    const tokenFile = join(directory, "token");
    const token = forkToken("key", "try");
    writeFileSync(tokenFile, token);
    const answer: ForkAnswer = { outcome: "forked", netMode: "none", reason: null };
    const branch = await branchHearing("try", tokenFile, scope, memoryTopic("events.try", [answerRecord(token, "try", "request-early", "fs-abc123", answer)]));
    assert.deepEqual(await answerIn(branch.store, "request-early"), answer);

    // Main's, told of none, passes over what was written before it first read.
    const store = await testStore();
    const events = memoryTopic("events.main", [answerRecord(token, "try", "request-before", "fs-abc123", answer)]);
    await Effect.runPromise(Effect.forkIn(
      consumeEvents({ "workspace.fork-answered": forkAnswered({ branch: "try", tokenFile }) }).pipe(
        Effect.provide(Layer.mergeAll(EnvironmentEvents.layer(logOf("events.main", "alasio-events.main"), { branch: false }, events.joinTopic), Layer.succeed(Store, store))),
      ),
      scope,
    ));
    await sleep(500);
    events.write(answerRecord(token, "try", "request-after", "fs-abc123", answer));
    assert.deepEqual(await answerIn(store, "request-after"), answer);
    assert.equal(await run(store.forkAnswerTo("request-before")), null);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("the requests a parent answered and the answers a branch heard are forgotten with what else is kept only in flight", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const answer: ForkAnswer = { outcome: "forked", netMode: "none", reason: null };
  for (const id of ["request-old", "request-new"]) {
    await run(store.recordForkRequest("try", id, "fs-abc123", answer));
    await run(store.withEventHandled(randomUUID(), "workspace.fork-answered", (writes) => writes.recordForkAnswer(id, "fs-abc123", answer)));
  }
  const pool = await testPool();
  await pool.query(`update ${schema}.fork_requests set answered_at = now() - interval '8 days' where id = 'request-old'`);
  await pool.query(`update ${schema}.fork_answers set answered_at = now() - interval '8 days' where request = 'request-old'`);
  await run(store.pruneTransient("7 days"));
  assert.deepEqual(await run(store.forkRequestAnswered("try", "request-old")), null);
  assert.deepEqual(await run(store.forkAnswerTo("request-old")), null);
  assert.deepEqual(await run(store.forkRequestAnswered("try", "request-new")), { volumeId: "fs-abc123", answer });
  assert.deepEqual(await run(store.forkAnswerTo("request-new")), answer);
});

/** Once `topic`'s group has been told of every record written on it. */
async function allTold(topic: MemoryTopic): Promise<void> {
  for (const deadline = Date.now() + 15_000; topic.committed() < topic.written().length; await sleep(20)) {
    if (Date.now() > deadline) assert.fail(`${topic.committed()} of ${topic.written().length} records told`);
  }
}

/** The answer the store `store` has to the request `id`, once it has one. */
async function answerIn(store: Store["Service"], id: string): Promise<ForkAnswer> {
  for (const deadline = Date.now() + 15_000; Date.now() < deadline; await sleep(20)) {
    const answer = await run(store.forkAnswerTo(id));
    if (answer !== null) return answer;
  }
  return assert.fail(`no answer to ${id}`);
}

test("an alasio forks a session one of its conversations knows for the branch whose token asks on the log, into the branch's environment through the gateway, answering on the branch's events the internet it was made with, and none while a turn runs in it", { timeout: 120_000 }, async () => {
  const directory = mkdtempSync(join(tmpdir(), "alasio-branch-forks-"));
  const keyFile = join(directory, "fork-key");
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setWorkingDirectory(conversationId, "sessionfs:fs-abc123"));
  const forked: string[][] = [];
  let outcome: Effect.Effect<void, GatewayError> = Effect.void;
  const sandboxes = SessionSandboxes.of({
    volumes: {
      create: () => Effect.die("the parent makes no session for a branch"),
      fork: (source, volumeId, environment = "main") =>
        Effect.sync(() => forked.push([source, volumeId, environment])).pipe(Effect.andThen(outcome), Effect.as({ volumeId, netMode: "none" as const })),
      destroy: () => Effect.die("the parent deletes no session for a branch"),
      list: Effect.die("the parent lists no workspace for a branch"),
    },
    harnessDirectory: () => directory,
    ensureSession: () => Effect.die("the parent brings no session up for a branch"),
    readFile: () => Effect.die("the parent reads no file for a branch"),
    writeFile: () => Effect.die("the parent writes no file for a branch"),
  });
  const scope = Effect.runSync(Scope.make());
  try {
    // Main's forks topic, which branches write their requests on; and the branches, which main answers on their events topics.
    const forks = memoryTopic("forks.main");
    const tokenFile = join(directory, "token");
    const otherTokenFile = join(directory, "other-token");
    const token = forkToken("key", "try");
    writeFileSync(tokenFile, token);
    writeFileSync(otherTokenFile, forkToken("key", "other"));
    const branches = { try: await branchHearing("try", tokenFile, scope), other: await branchHearing("other", otherTokenFile, scope) };
    let untold = 0;
    const events = Events.of({
      tell: () => Effect.die("the parent tells nothing it may lose"),
      send: (event) =>
        untold-- > 0
          ? Effect.fail(new EventNotTold({ message: "the log is away" }))
          : Effect.sync(() => branches[event.environment as keyof typeof branches].events.write(eventRecord(event, Date.now()).value)),
    });
    // The conversations whose prompts are run again once a fork lets go of them.
    const scheduled: string[] = [];
    const turns = turnsStub({ schedule: (id) => Effect.sync(() => void scheduled.push(id)) });
    const services = await Effect.runPromise(
      Layer.buildWithScope(Layer.mergeAll(Layer.succeed(SessionSandboxes, sandboxes), Layer.succeed(Turns, turns)).pipe(
        Layer.provideMerge(HarnessHost.layer),
        Layer.provideMerge(Harnesses.layer({})),
        Layer.provideMerge(Layer.mergeAll(inProcessEvents, ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir: "/nonexistent-state-dir" }).pipe(Layer.provideMerge(Layer.succeed(Store, store))))),
        Layer.provideMerge(Layer.mergeAll(ForkRequests.layer(logOf("forks.main", "alasio-forks.main"), forks.joinTopic), Layer.succeed(Events, events))),
      ), scope),
    );
    // A request a branch made before its parent first read its forks topic is answered.
    writeFileSync(keyFile, "key\n");
    forks.write(forkRequestRecord(token, { id: "request-early", branch: "try", volumeId: "fs-abc123", at: Date.now() }).value);
    await Effect.runPromise(Effect.forkIn(Effect.provide(serveBranchForks({ keyFile }), services), scope));
    assert.deepEqual(await answerIn(branches.try.store, "request-early"), { outcome: "forked", netMode: "none", reason: null });
    assert.deepEqual(forked, [["fs-abc123", "fs-abc123", "try"]]);
    forked.length = 0;
    const asks = ForkAsks.of({ ask: ({ value }) => Effect.sync(() => forks.write(value)) });
    const asking = (branch: keyof typeof branches, volumeId: string) =>
      Effect.runPromiseExit(parentForks({ branch, tokenFile })(volumeId).pipe(Effect.provideService(ForkAsks, asks), Effect.provideService(Store, branches[branch].store)));
    const refusal = (exit: Exit.Exit<NetMode | null, { readonly message: string }>) => (Exit.isFailure(exit) ? Option.getOrThrow(Exit.findErrorOption(exit)).message : "");

    // Answered on the events of the branch a request names, which whoever wrote it chose: one
    // its parent cannot tell that branch's token on, with no key yet, one another branch
    // signed, and one of an id longer than any a branch gives, are passed over, answered
    // nothing; their branches' waits run out.
    const answersTold = () => [branches.try.events.written().length, branches.other.events.written().length];
    const toldBefore = answersTold();
    rmSync(keyFile);
    forks.write(forkRequestRecord(token, { id: "request-keyless", branch: "try", volumeId: "fs-abc123", at: Date.now() }).value);
    // Signed with the token no key at all would give: still no branch's.
    forks.write(forkRequestRecord(forkToken("", "try"), { id: "request-keyless-signed", branch: "try", volumeId: "fs-abc123", at: Date.now() }).value);
    await allTold(forks);
    writeFileSync(keyFile, "key\n");
    forks.write(forkRequestRecord(token, { id: "request-forged", branch: "other", volumeId: "fs-abc123", at: Date.now() }).value);
    forks.write(forkRequestRecord(token, { id: "r".repeat(65), branch: "try", volumeId: "fs-abc123", at: Date.now() }).value);
    await allTold(forks);
    assert.deepEqual(answersTold(), toldBefore);
    assert.deepEqual(forked, []);

    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed("none"));
    assert.deepEqual(forked, [["fs-abc123", "fs-abc123", "try"]]);
    forked.length = 0;
    // One the parent never had is not its to give; one it made, which no conversation mounts, is, with the internet it was made with.
    assert.deepEqual(await asking("try", "fs-zzz999"), Exit.succeed(null));
    await run(store.recordSessionWorkspace({ volumeId: "fs-made01", forkedFrom: null }));
    assert.deepEqual(await asking("try", "fs-made01"), Exit.succeed(null));
    await run(store.markSessionWorkspaceMade("fs-made01", "full"));
    assert.deepEqual(await asking("try", "fs-made01"), Exit.succeed("full"));
    // Forked already, by an earlier ask.
    outcome = Effect.fail(new GatewayError({ call: "fork fs-abc123", status: 409, reason: "fs-abc123 is there already" }));
    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed("none"));
    outcome = Effect.fail(new GatewayError({ call: "fork fs-abc123", status: 502, reason: "its clone failed" }));
    assert.match(refusal(await asking("try", "fs-abc123")), /did not fork the session fs-abc123: the sandbox gateway did not fork fs-abc123 \(502\): its clone failed$/u);
    outcome = Effect.void;
    forked.length = 0;

    // A request delivered again, or whose answer the log did not take, is answered as it was, from its record, and not forked again.
    const again = forkRequestRecord(token, { id: "request-again", branch: "try", volumeId: "fs-abc123", at: Date.now() });
    untold = 1;
    forks.write(again.value);
    forks.write(again.value);
    assert.deepEqual(await answerIn(branches.try.store, "request-again"), { outcome: "forked", netMode: "none", reason: null });
    await allTold(forks);
    await allTold(branches.try.events);
    assert.deepEqual(forked, [["fs-abc123", "fs-abc123", "try"]]);
    forked.length = 0;

    // A request is answered within the time its branch waits, as the parent's clock says, give or take the skew between the two clocks; refused past it.
    const madeAt = async (id: string, at: number) => {
      forks.write(forkRequestRecord(token, { id, branch: "try", volumeId: "fs-abc123", at }).value);
      return await answerIn(branches.try.store, id);
    };
    const wait = Duration.toMillis(FORK_WAIT);
    const skew = Duration.toMillis(CLOCK_SKEW);
    const late = { outcome: "refused", netMode: null, reason: "the request was not made within the time a branch waits for its answer" };
    assert.deepEqual(await madeAt("request-old", Date.now() - wait - skew - 60_000), late);
    assert.deepEqual(await madeAt("request-ahead", Date.now() + skew + 60_000), late);
    assert.deepEqual(forked, []);
    assert.deepEqual(await madeAt("request-skewed", Date.now() + skew / 2), { outcome: "forked", netMode: "none", reason: null });
    assert.deepEqual(await madeAt("request-waited", Date.now() - wait), { outcome: "forked", netMode: "none", reason: null });
    // A request no branch's, or not of a branch's name, is skipped, and those after it answered.
    forks.write("not a request");
    forks.write(forkRequestRecord(token, { id: "request-unnamed", branch: "Not A Name", volumeId: "fs-abc123", at: Date.now() }).value);
    assert.deepEqual(await madeAt("request-after", Date.now()), { outcome: "forked", netMode: "none", reason: null });
    forked.length = 0;

    // One answered while its branch waited, and delivered again past the wait, is answered as
    // it was, from its record: neither refused nor forked again.
    const lastTold = () => {
      const { request, outcome, reason } = JSON.parse(branches.try.events.written().at(-1) ?? "{}") as { readonly request?: string; readonly outcome?: string; readonly reason?: string | null };
      return { request, outcome, reason };
    };
    const nearlyLate = forkRequestRecord(token, { id: "request-nearly-late", branch: "try", volumeId: "fs-abc123", at: Date.now() - wait - skew + 2_000 });
    forks.write(nearlyLate.value);
    assert.deepEqual(await answerIn(branches.try.store, "request-nearly-late"), { outcome: "forked", netMode: "none", reason: null });
    await sleep(2_500);
    forks.write(nearlyLate.value);
    await allTold(forks);
    assert.deepEqual(lastTold(), { request: "request-nearly-late", outcome: "forked", reason: null });
    assert.deepEqual(forked, [["fs-abc123", "fs-abc123", "try"]]);
    forked.length = 0;
    // One of an id its branch gave a request for another session is refused, that one's answer kept.
    forks.write(forkRequestRecord(token, { id: "request-nearly-late", branch: "try", volumeId: "fs-made01", at: Date.now() }).value);
    await allTold(forks);
    assert.deepEqual(lastTold(), { request: "request-nearly-late", outcome: "refused", reason: "the request request-nearly-late was made for another session, fs-abc123" });
    // A request's id is its branch's own: another's of the same id is answered as its own.
    forks.write(forkRequestRecord(forkToken("key", "other"), { id: "request-nearly-late", branch: "other", volumeId: "fs-made01", at: Date.now() }).value);
    assert.deepEqual(await answerIn(branches.other.store, "request-nearly-late"), { outcome: "forked", netMode: "full", reason: null });
    assert.deepEqual(forked, [["fs-made01", "fs-made01", "other"]]);
    forked.length = 0;

    // An answer its parent did not sign with the branch's token, as the gateways, which write the
    // branch's events too, could write one, is passed over: none is recorded.
    const forged = { outcome: "forked", netMode: "full", reason: null } as const;
    branches.try.events.write(answerRecord(token, "try", "request-forged-answer", "fs-abc123", forged, "not a signature"));
    branches.try.events.write(answerRecord(forkToken("key", "other"), "try", "request-forged-answer", "fs-abc123", forged));
    // One its parent signed, what it says changed after: the internet it was made with, or why it was refused.
    const signedAs = (answer: ForkAnswer, request: string) => answerSignatureOf(token, { branch: "try", request, volumeId: "fs-abc123", ...answer });
    branches.try.events.write(answerRecord(token, "try", "request-netmode-changed", "fs-abc123", forged, signedAs({ ...forged, netMode: "none" }, "request-netmode-changed")));
    const refused = { outcome: "refused", netMode: null, reason: "a turn runs in it" } as const;
    branches.try.events.write(answerRecord(token, "try", "request-reason-changed", "fs-abc123", { ...refused, reason: "forked elsewhere" }, signedAs(refused, "request-reason-changed")));
    branches.try.events.write(answerRecord(token, "try", "request-signed-answer", "fs-abc123", forged));
    assert.deepEqual(await answerIn(branches.try.store, "request-signed-answer"), forged);
    for (const request of ["request-forged-answer", "request-netmode-changed", "request-reason-changed"]) assert.equal(await run(branches.try.store.forkAnswerTo(request)), null, request);

    // While a turn runs in it, which the fork would suspend.
    const turning = await Effect.runPromise(Deferred.make<void>());
    const turn = Effect.runFork(Effect.scoped(Effect.gen(function*() {
      const turns = yield* ActiveTurns;
      yield* turns.register(conversationId, { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false });
      yield* Deferred.await(turning);
    })).pipe(Effect.provide(services)));
    await sleep(50);
    assert.match(refusal(await asking("try", "fs-abc123")), /a turn runs in the session fs-abc123 now/u);
    await Effect.runPromise(Deferred.succeed(turning, undefined));
    await Effect.runPromise(Fiber.join(turn));
    assert.deepEqual(await asking("try", "fs-abc123"), Exit.succeed("none"));

    // No turn starts while it forks, which would suspend the session under it; what was
    // asked of the conversation meanwhile is run once the fork lets go of it.
    const forking = await Effect.runPromise(Deferred.make<void>());
    const forkEnds = await Effect.runPromise(Deferred.make<void>());
    outcome = Effect.andThen(Deferred.succeed(forking, undefined), Deferred.await(forkEnds));
    scheduled.length = 0;
    const asked = asking("try", "fs-abc123");
    await Effect.runPromise(Deferred.await(forking));
    const busy = Effect.flatMap(HarnessHost, (host) => Effect.map(host.status(conversationId), ({ busy: held }) => held)).pipe(Effect.provide(services));
    try {
      assert.equal(await Effect.runPromise(busy), true);
      assert.deepEqual(scheduled, []);
    } finally {
      await Effect.runPromise(Deferred.succeed(forkEnds, undefined));
    }
    assert.deepEqual(await asked, Exit.succeed("none"));
    assert.equal(await Effect.runPromise(busy), false);
    assert.deepEqual(scheduled, [conversationId]);
    outcome = Effect.void;
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a branch environment runs no turn in a folder, which it cannot copy, and says why, failing the prompt", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setActiveHarness(conversationId, "codex"));
  await run(store.setWorkingDirectory(conversationId, "/home/operator/project"));
  const outbox = recordingOutbox();
  await withServices({ store, outbox: outbox.layer, branch: "try" }, async (alasio) => {
    await alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.submit({ conversationId, chatId: 1001, messageId: 7, prompt: "hi", fileIds: [], visibleText: "hi" })));
    for (let waited = 0; outbox.queued.texts.length === 0 && waited < 5000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
    const [{ chatId, text } = { chatId: undefined, text: undefined }] = outbox.queued.texts;
    assert.equal(String(chatId), "1001");
    assert.match(String(text), /^Codex hit an error: This is the branch environment try, .* A folder is this machine's own files, which cannot be copied on write, so a branch never works in one: use \/workspace for a session workspace\.$/u);
    assert.equal(await run(store.hasOpenPromptJobs(conversationId)), false);
  });
});

test("a branch environment mounts and makes no folder, forks no workspace itself, and its panel offers its session workspaces alone", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setWorkingDirectory(conversationId, "sessionfs:fs-abc123"));
  const sandbox = SessionSandboxes.of({
    volumes: { create: () => Effect.die("no session is made"), fork: () => Effect.die("no session is forked"), destroy: () => Effect.die("no session is deleted"), list: Effect.succeed([]) },
    harnessDirectory: () => "/nonexistent",
    ensureSession: () => Effect.die("no session is brought up"),
    readFile: () => Effect.die("no file is read"),
    writeFile: () => Effect.die("no file is written"),
  });
  await withServices({ store, branch: "try", sandbox, workspaceRoot: tmpdir() }, async (alasio) => {
    const refusal = (change: (mounts: Mounts["Service"]) => Effect.Effect<unknown, { readonly message: string }>) =>
      alasio.runPromise(Effect.flip(Effect.flatMap(Mounts, change))).then(({ message }) => message);
    assert.match(await refusal((mounts) => mounts.switchWorkspace(conversationId, tmpdir())), /^This is the branch environment try, .* A folder is this machine's own files/u);
    assert.match(await refusal((mounts) => mounts.createWorkspace(conversationId, "new-folder")), /^This is the branch environment try, .* A folder is this machine's own files/u);
    assert.equal(
      await refusal((mounts) => mounts.forkSessionWorkspace(conversationId)),
      "This is the branch environment try: its session workspaces are forked from alasio's own as it first uses them, and it forks none of them itself.",
    );
  });
  const panel = buildWorkspacePanel({
    current: "sessionfs:fs-abc123",
    workspaceRoot: "/root",
    listing: { candidates: [{ name: "repo", path: "/root/repo", git: true }] },
    working: false,
    sandboxEnabled: true,
    sessionWorkspaces: [{ volumeId: "fs-abc123", forkedFrom: null, netMode: "none", madeAt: new Date(), createdAt: new Date() }],
    branch: "try",
  });
  assert.deepEqual(panel.keyboard.flat().map(({ text }) => text), ["* fs-abc123", "New empty workspace…", "Refresh", "Close"]);
  assert.match(panel.text, /This is the branch environment try\. It works on copy-on-write copies of alasio's session workspaces/u);
});

test("on a branch, a conversation mounted on a folder can still be moved off it: its workspace panel opens and its buttons act", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: 1001 }));
  await run(store.setActiveHarness(conversationId, "codex"));
  await run(store.setWorkingDirectory(conversationId, "/home/operator/project"));
  const telegram = recordingTelegram();
  await withServices({ store, telegram: telegram.layer, branch: "try", allowedUserIds: "1001" }, async (alasio) => {
    assert.equal(await alasio.runPromise(handleTextCommand({ text: "/workspace", fileIds: [], conversationId, chatId: 1001, messageId: 7 })), true);
    assert.match(String(telegram.calls.sendMessage.at(-1)?.[1]), /This is the branch environment try\. It works on copy-on-write copies/u);
    const [refresh = ""] = await run(store.createCallbackActions(conversationId, [{ kind: "workspace:refresh" }]));
    await alasio.runPromise(handleCallbackQuery({
      id: "press",
      from: { id: 1001, is_bot: false, first_name: "Operator" },
      chat_instance: "1",
      data: refresh,
      message: { message_id: 77, date: 0, chat: { id: 1001, type: "private", first_name: "Operator" } },
    }));
    assert.deepEqual(telegram.calls.answerCallbackQuery.map(([, text]) => text), ["Refreshed."]);
  });
});

test("a session a branch inherited is recorded as one it made, keeping what its parent recorded of it", async () => {
  const store = await testStore({ branch: "try" });
  await run(store.recordInheritedSessionWorkspace("fs-legacy1", "none"));
  await run(store.recordSessionWorkspace({ volumeId: "fs-fork001", forkedFrom: "fs-legacy1" }));
  await run(store.markSessionWorkspaceMade("fs-fork001", "full"));
  const before = (await run(store.listSessionWorkspaces)).find(({ volumeId }) => volumeId === "fs-fork001");
  await run(store.recordInheritedSessionWorkspace("fs-fork001", "full"));
  const workspaces = await run(store.listSessionWorkspaces);
  assert.deepEqual(workspaces.find(({ volumeId }) => volumeId === "fs-fork001"), before);
  const legacy = workspaces.find(({ volumeId }) => volumeId === "fs-legacy1");
  assert.deepEqual([legacy?.forkedFrom, legacy?.netMode, legacy?.madeAt instanceof Date], [null, "none", true]);
});

test("main's state is never made a branch's", async () => {
  const schema = newSchema();
  const conversationId = await run((await testStore({ schema })).upsertConversation({ chatId: 1001 }));
  await run((await testStore({ schema })).enqueuePromptJob({ conversationId, chatId: 1001, messageId: 7, prompt: "main's", harness: "codex" }));
  const main = await testStore({ schema });
  assert.equal(await run(main.hasOpenPromptJobs(conversationId)), true);
  assert.equal(await run(main.getState("branch")), null);
});
