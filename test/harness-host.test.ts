/**
 * The harness host (src/harness/host.ts), as alasio's control plane reaches its turns, in
 * its process or elsewhere: given data alone, it runs a conversation's turn on its harness
 * with the claim's writes and its job's progress, and tells, as its events, what the
 * harness does on its own; what fails fails as the same error wherever the host is.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Array as Arr, Context, Deferred, Effect, Fiber, Layer, Logger, Option, PubSub, type Scope } from "effect";

import { CodexAppServer, makeAppServer } from "../src/codex/app-server/client.ts";
import { AppServerRequestTimeout } from "../src/codex/app-server/rpc-client.ts";
import type { ResponseBlock } from "../src/codex/event-projection.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { makeCodexHarness } from "../src/harness/codex.ts";
import { whileHeld } from "../src/harness/holds.ts";
import { HarnessHost, type TurnStart } from "../src/harness/host.ts";
import { HarnessEvents, type HostEvent, HostEvents } from "../src/harness/host-events.ts";
import { type Harness, Harnesses, NoWorkspaceMounted, type TurnParams, type TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { remoteHarnessHost } from "../src/harness/remote-host.ts";
import { serveHost } from "../src/harness/serve.ts";
import { noFolderBayma } from "../src/mcp/bayma.ts";
import { Store, TurnFenced } from "../src/persistence/store.ts";
import { ReceivedFiles } from "../src/telegram/files.ts";
import { holdsIn, newSchema, run, testStore } from "./support/store.ts";

/** What the recording harness tells of each turn besides its reply. */
const NOTICE = "This Codex login doesn't offer gpt-5.6-sol, so Codex used gpt-5.6-terra, the login's default (at high effort). Use /model to choose another.";

/**
 * A Codex harness whose turn hands what it was given to `seen`, writes its answer, and acts
 * as a harness that runs on between prompts would; `onRun` says what it finds as it runs.
 */
function recordingHarness(seen: TurnParams[], onRun: () => void = () => {}): Harness {
  const unused = (name: string) => () => assert.fail(`the recording harness has no ${name}`);
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
      createForkedSession: (sessionId, beforeUuid, { threadKey }) => Effect.succeed(`fork of ${sessionId} before ${beforeUuid} for ${threadKey}`),
    },
    startFreshSession: ({ threadKey }) => Effect.succeed(`fresh-of-${threadKey}`),
    warmSession: () => Effect.succeed(false),
    runTurn: (params) =>
      Effect.gen(function*() {
        seen.push(params);
        onRun();
        yield* params.onPromptDispatched ?? Effect.void;
        const pendingResponseId = yield* params.persistence.createPendingResponse(params.chatId, params.messageId, "session-1");
        const blockSequence: ResponseBlock[] = [{ type: "text", content: "Done." }];
        yield* params.persistence.appendBlocksToPending(pendingResponseId, blockSequence);
        yield* params.persistence.markPendingResponseComplete(pendingResponseId);
        yield* params.onBackgroundResponse ?? Effect.void;
        yield* params.onIdle ?? Effect.void;
        const result: TurnResult = { blockSequence, sessionId: "session-1", pendingResponseId, interrupted: false, responseCompleted: true, notice: NOTICE };
        return result;
      }),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/**
 * Runs `effect` on the host a harness pool's pod serves (HarnessHost.layer, src/harness/program.ts),
 * its Codex `harness`, on `store`, the files a prompt names written under `stateDir`, what it
 * tells heard in the test's process (HostEvents.inProcess).
 */
const onPodHost = <A, E>(
  { store, harness, stateDir = "/nonexistent-state-dir" }: { readonly store: Store["Service"]; readonly harness: Harness; readonly stateDir?: string },
  effect: Effect.Effect<A, E, HarnessHost | HarnessEvents | Scope.Scope>,
): Promise<A> =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.provide(HarnessHost.layer.pipe(
    Layer.provideMerge(Harnesses.layer({ overrides: { [CODEX_HARNESS]: harness } })),
    Layer.provideMerge(HostEvents.inProcess),
    Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir }))),
    Layer.provideMerge(Layer.succeed(Store, store)),
  ))));

/** The host as the control plane reaches it: in its process, or elsewhere, over HTTP (src/harness/serve.ts, src/harness/remote-host.ts), serving `conversationId`. */
const REACHES: Readonly<Record<string, (host: HarnessHost["Service"], conversationId: string) => Effect.Effect<HarnessHost["Service"], unknown, Scope.Scope>>> = {
  "in alasio's process": (host) => Effect.succeed(host),
  "elsewhere, over HTTP": (host, conversationId) =>
    Effect.gen(function*() {
      const { port } = yield* Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token" }), HarnessHost, host);
      const remote = yield* Layer.build(remoteHarnessHost({ url: `http://127.0.0.1:${port}`, token: Effect.succeed("host-token") }));
      return Context.get(remote, HarnessHost);
    }),
};

for (const [reach, reached] of Object.entries(REACHES)) {
  test(`a turn started through the host ${reach} runs on its harness under the turn's claim, its job's progress recorded, what the harness does on its own heard as its host tells it, and what fails failing as it does`, async () => {
    const schema = newSchema();
    const store = await testStore({ schema });
    const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work"));
    const job = await run(store.enqueuePromptJob({ conversationId, chatId: "41", messageId: "10", prompt: "go" }));
    await run(store.claimNextPromptJob(conversationId));
    const claim = (messageId: string) => run(store.claimTurn({ conversationId, chatId: "41", messageId, sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
    const epoch = await claim("10");
    const seen: TurnParams[] = [];
    const start = (messageId: string, claimed: number, jobId: string | null): TurnStart => ({
      conversationId,
      chatId: "41",
      messageId,
      harness: CODEX_HARNESS,
      workingDirectory: "/work",
      prompt: "go",
      fileIds: [],
      resumeSession: null,
      modelChoice: null,
      epoch: claimed,
      jobId,
      attachedTurn: null,
      bayma: null,
      codex: null,
    });
    await onPodHost({ store, harness: recordingHarness(seen) }, Effect.gen(function*() {
      const host = yield* reached(yield* HarnessHost, conversationId);
      const events = yield* (yield* HarnessEvents).subscribe;
      // Held for one turn at a time, until the scope it is held in closes.
      yield* Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* host.reserve(conversationId), true);
        assert.deepEqual(yield* host.status(conversationId), { busy: true, running: true, cliInitiated: false, held: false, idleSince: null });
        assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
      }));
      const { idleSince, ...idle } = yield* host.status(conversationId);
      assert.deepEqual(idle, { busy: false, running: false, cliInitiated: false, held: false });
      assert.ok(idleSince !== null && idleSince <= Date.now());
      // A fork's hold, a row of the store, keeps it busy as no turn, which no turn starts under.
      assert.ok(Option.isSome(yield* whileHeld(host, store, [conversationId], "forking session fs-a into fs-b", () => Effect.gen(function*() {
        assert.deepEqual([(yield* host.status(conversationId)).busy, (yield* host.status(conversationId)).held, (yield* host.status(conversationId)).running], [true, true, false]);
        assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
      }))));
      // And none is held where a turn runs.
      yield* Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* host.reserve(conversationId), true);
        assert.deepEqual(yield* whileHeld(host, store, [conversationId], "forking", () => Effect.void), Option.none());
      }));
      assert.deepEqual(yield* Effect.promise(() => holdsIn(schema)), []);

      const result = yield* host.startTurn(start("10", epoch, job.id));
      assert.deepEqual([result.blockSequence, result.sessionId, result.interrupted, result.responseCompleted, result.notice], [[{ type: "text", content: "Done." }], "session-1", false, true, NOTICE]);
      assert.ok((yield* store.getCompletedResponsesPendingDelivery).some(({ id }) => id === result.pendingResponseId));
      assert.deepEqual([yield* PubSub.take(events), yield* PubSub.take(events)], [
        { kind: "background-response", conversationId },
        { kind: "idle", conversationId },
      ]);
      // A turn of a claim another has overtaken since fails as its write does.
      yield* Effect.promise(() => claim("11"));
      assert.deepEqual(yield* Effect.flip(host.startTurn(start("12", epoch, null))), new TurnFenced({ threadKey: conversationId, epoch }));
      // A fresh session of the harness, through the host; none where no folder is mounted.
      assert.equal(yield* host.startFreshSession({ conversationId, harness: CODEX_HARNESS, workingDirectory: "/work" }), `fresh-of-${conversationId}`);
      assert.deepEqual(yield* Effect.flip(host.startFreshSession({ conversationId, harness: CODEX_HARNESS, workingDirectory: "" })), new NoWorkspaceMounted());
      // No turn of it runs once it has ended, to steer or stop.
      assert.equal((yield* host.status(conversationId)).busy, false);
      assert.deepEqual(yield* host.steer(conversationId, "also", []), Option.none());
      assert.equal(yield* host.interrupt(conversationId, "interrupt"), false);
      // A rewind's fork, and a thread's goals, of the conversation's harness, through the host: of a harness with no goals, none.
      const of = { conversationId, harness: CODEX_HARNESS, workingDirectory: "/work" } as const;
      assert.equal(yield* host.forkSession({ ...of, sessionId: "session-1", beforeUuid: "turn-1" }), `fork of session-1 before turn-1 for ${conversationId}`);
      // Said as the host says it, wherever it is: not as of the host's being reached again.
      assert.equal((yield* Effect.flip(host.goals(of).read({ threadId: "session-1" }))).message, "The conversation's harness could not be reached: Codex has no goals");
    }));
    const [params] = seen;
    assert.ok(params);
    assert.deepEqual(
      [params.threadKey, params.chatId, params.messageId, params.workingDirectory, params.prompt, params.resumeSession, params.persistence.epoch],
      [conversationId, "41", "10", "/work", "go", null, epoch],
    );
    // The job's dispatch, recorded by the host as the harness said it sent the prompt.
    assert.ok((await run(store.getPromptJob(job.id)))?.upstream_dispatched_at);
  });
}

for (const [reach, reached] of Object.entries(REACHES)) {
  test(`the host ${reach} says whether the conversation's harness has work of its own going on between turns, asked apart from its status, which asks no harness`, async () => {
    const store = await testStore();
    const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work"));
    const asked: string[] = [];
    let working = true;
    const harness: Harness = {
      ...recordingHarness([]),
      backgroundWork: (threadKey) => Effect.sync(() => (asked.push(threadKey), working)),
    };
    await onPodHost({ store, harness }, Effect.gen(function*() {
      const host = yield* reached(yield* HarnessHost, conversationId);
      const status = yield* host.status(conversationId);
      assert.deepEqual([status.busy, status.running, asked], [false, false, []]);
      assert.equal(yield* host.backgroundWork(conversationId), true);
      working = false;
      assert.equal(yield* host.backgroundWork(conversationId), false);
    }));
    assert.deepEqual(asked, [conversationId, conversationId]);
  });
}

for (const [reach, reached] of Object.entries(REACHES)) {
  test(`a turn's caller that lets go of it, as alasio does as it stops, attaches to it again through the host ${reach}, the turn asked for again the one started then: its result once it ends, kept however often taken, an attach broken on its way among them, until the next turn starts, and none of a turn it knows not`, async () => {
    const store = await testStore();
    const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work"));
    const claim = (messageId: string) => run(store.claimTurn({ conversationId, chatId: "41", messageId, sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
    const start = (messageId: string, epoch: number): TurnStart => ({ conversationId, chatId: "41", messageId, harness: CODEX_HARNESS, workingDirectory: "/work", prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null, bayma: null, codex: null });
    // Each turn ends as the test lets it, once it is running.
    const seen: TurnParams[] = [];
    const gates: Deferred.Deferred<void>[] = [];
    const recorded = recordingHarness(seen);
    const harness: Harness = {
      ...recorded,
      runTurn: (params) => Effect.gen(function*() {
        const gate = yield* Deferred.make<void>();
        gates.push(gate);
        yield* Deferred.await(gate);
        return yield* recorded.runTurn(params);
      }),
    };
    const first = await claim("10");
    await onPodHost({ store, harness }, Effect.gen(function*() {
      const host = yield* reached(yield* HarnessHost, conversationId);
      // None of a turn it never ran.
      assert.deepEqual(yield* host.attach({ conversationId, epoch: first, messageId: "10" }), Option.none());
      // Its caller lets go of it as it runs: it runs on.
      const caller = yield* Effect.forkChild(host.startTurn(start("10", first)));
      while (gates.length === 0) yield* Effect.sleep("10 millis");
      yield* Fiber.interrupt(caller);
      const attached = yield* Effect.forkChild(host.attach({ conversationId, epoch: first, messageId: "10" }));
      // An attach that lets go as it waits, as one whose connection broke does, takes nothing away.
      const broken = yield* Effect.forkChild(host.attach({ conversationId, epoch: first, messageId: "10" }));
      yield* Effect.sleep("20 millis");
      yield* Fiber.interrupt(broken);
      // Asked for again under its epoch, as a caller that lost its answer may, it is the one started then.
      const again = yield* Effect.forkChild(host.startTurn(start("10", first)));
      yield* Effect.sleep("50 millis");
      assert.equal(attached.pollUnsafe(), undefined, "an attach answered before its turn ended");
      yield* Deferred.succeed(gates[0]!, undefined);
      const result = Option.getOrThrow(yield* Fiber.join(attached));
      assert.deepEqual([result.blockSequence, result.sessionId], [[{ type: "text", content: "Done." }], "session-1"]);
      assert.equal((yield* Fiber.join(again)).pendingResponseId, result.pendingResponseId);
      // Taken so, it is kept still, for an attach whose answer was lost on its way to ask again; and only the one turn ran.
      assert.equal(Option.getOrThrow(yield* host.attach({ conversationId, epoch: first, messageId: "10" })).pendingResponseId, result.pendingResponseId);
      assert.equal(seen.length, 1);
      // A turn whose caller took its answer is kept until the next starts: attached to after it ended, then of no more once another has started.
      const second = yield* Effect.promise(() => claim("11"));
      const ending = yield* Effect.forkChild(host.startTurn(start("11", second)));
      while (gates.length < 2) yield* Effect.sleep("10 millis");
      yield* Deferred.succeed(gates[1]!, undefined);
      yield* Fiber.join(ending);
      const third = yield* Effect.promise(() => claim("12"));
      const next = yield* Effect.forkChild(host.startTurn(start("12", third)));
      while (gates.length < 3) yield* Effect.sleep("10 millis");
      assert.deepEqual(yield* host.attach({ conversationId, epoch: second, messageId: "11" }), Option.none());
      // And of another message, or another epoch, of the turn running: none either.
      assert.deepEqual(yield* host.attach({ conversationId, epoch: third, messageId: "11" }), Option.none());
      assert.deepEqual(yield* host.attach({ conversationId, epoch: second, messageId: "12" }), Option.none());
      yield* Deferred.succeed(gates[2]!, undefined);
      yield* Fiber.join(next);
      assert.ok(Option.isSome(yield* host.attach({ conversationId, epoch: third, messageId: "12" })));
    }));
  });
}

test("Codex has work of its own in a conversation as its app-server says, none where it runs none, and where that cannot be told, in time or at all, has, as alasio's log says", async () => {
  const logged: string[] = [];
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // An app-server that has not started, which runs nothing.
    const appServer = yield* makeAppServer({ spawn: () => Effect.sync(() => assert.fail("no app-server is started to ask")) });
    let work = appServer.backgroundWork;
    const harness = yield* makeCodexHarness({ workingDirectory: "/work", sandbox: null, sessionFsCodex: null, folderBayma: noFolderBayma }).pipe(
      Effect.provideService(CodexAppServer, { ...appServer, backgroundWork: (threadKey) => work(threadKey) }),
      Effect.provide(ActiveTurns.layer),
    );
    const working = (threadKey: string) => harness.backgroundWork?.(threadKey) ?? Effect.succeed(false);
    assert.equal(yield* working("telegram:41"), false);
    const asked: string[] = [];
    work = (threadKey: string) => Effect.sync(() => (asked.push(threadKey), threadKey === "telegram:41"));
    assert.deepEqual([yield* working("telegram:41"), yield* working("telegram:42"), asked], [true, false, ["telegram:41", "telegram:42"]]);
    // Not told in time: taken as work going on, which no release goes on under.
    work = () => Effect.fail(new AppServerRequestTimeout({ method: "thread/backgroundTerminals/list" }));
    assert.equal(yield* working("telegram:41"), true);
  }).pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(Arr.ensure(message).join(" ")))])))));
  assert.ok(logged.some((line) => line.includes("could not tell whether Codex works on its own in telegram:41, so it is taken to")), JSON.stringify(logged));
});

for (const [reach, reached] of Object.entries(REACHES)) {
  test(`the host ${reach}, drained to be let go of, drains at once where nothing goes on in it, its harness's session mirrored, and starts no turn after; where a turn is about to run, or its harness works on its own, it drains nothing and keeps it as it was`, async () => {
    const store = await testStore();
    const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work"));
    await run(store.setSessionId(conversationId, "session-1"));
    const mirrored: string[] = [];
    let working = false;
    const harness: Harness = { ...recordingHarness([]), mirror: (sessionId) => Effect.sync(() => void mirrored.push(sessionId)), backgroundWork: () => Effect.sync(() => working) };
    await onPodHost({ store, harness }, Effect.gen(function*() {
      const host = yield* reached(yield* HarnessHost, conversationId);
      // A turn about to run, its reservation held until it is let go: not drained, and not waited for, so neither waits on the other.
      const letGo = yield* Deferred.make<void>();
      const turn = yield* Effect.forkChild(Effect.scoped(Effect.andThen(host.reserve(conversationId), Deferred.await(letGo))));
      while (!(yield* host.status(conversationId)).running) yield* Effect.sleep("10 millis");
      assert.deepEqual([yield* host.drain(conversationId), mirrored], [false, []]);
      yield* Deferred.succeed(letGo, undefined);
      yield* Fiber.join(turn);
      // Kept as it was: a turn reserves it again, then lets it go.
      assert.equal(yield* Effect.scoped(host.reserve(conversationId)), true);
      // Its harness working on its own: not drained.
      working = true;
      assert.deepEqual([yield* host.drain(conversationId), mirrored], [false, []]);
      working = false;
      // Nothing going on: drained, mirrored, and no turn is started in it again.
      assert.equal(yield* host.drain(conversationId), true);
      assert.deepEqual(mirrored, ["session-1"]);
      assert.equal((yield* host.status(conversationId)).busy, true);
      assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
    }));
  });
}

test("a host tells what its harness does on its own as the HostEvents it is given says, as a harness pool's pod tells it on the log", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  const epoch = await run(store.claimTurn({ conversationId, chatId: "41", messageId: "10", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
  const told: HostEvent[] = [];
  const telling = HostEvents.of({ tell: (event) => Effect.sync(() => void told.push(event)) });
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const host = Context.get(yield* Layer.build(HarnessHost.layer.pipe(
      Layer.provideMerge(Harnesses.layer({ overrides: { [CODEX_HARNESS]: recordingHarness([]) } })),
      Layer.provideMerge(Layer.mergeAll(ActiveTurns.layer, CodexAppServer.layer, ReceivedFiles.layer({ stateDir: "/nonexistent-state-dir" }))),
      Layer.provideMerge(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(HostEvents, telling))),
    )), HarnessHost);
    yield* host.startTurn({ conversationId, chatId: "41", messageId: "10", harness: CODEX_HARNESS, workingDirectory: "/work", prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null, bayma: null, codex: null });
  })));
  assert.deepEqual(told, [{ kind: "background-response", conversationId }, { kind: "idle", conversationId }]);
});

test("a host elsewhere not reached says nothing of its conversation and stops nothing, failing as not reached, so that no hold is made on what it cannot say", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A port no host listens on: a partition, as the control plane sees one.
    const host = Context.get(yield* Layer.build(remoteHarnessHost({ url: "http://127.0.0.1:1", token: Effect.succeed("host-token") })), HarnessHost);
    assert.equal((yield* Effect.flip(host.status(conversationId)))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(host.interrupt(conversationId, "interrupt")))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(Effect.scoped(host.reserve(conversationId))))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(whileHeld(host, store, [conversationId], "forking", () => Effect.void)))._tag, "HarnessHostError");
  })));
  assert.deepEqual(await holdsIn(schema), []);
});

test("the files a turn's prompt names, and a steer's, are written where the harness reads them before it runs, or is steered", async () => {
  const store = await testStore();
  const stateDir = mkdtempSync(join(tmpdir(), "alasio-host-files-"));
  try {
    const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    await run(store.setWorkingDirectory(conversationId, "/work"));
    /** A file a message of the conversation's carried, kept in the store alone: its id. */
    const kept = async (name: string, text: string) => {
      const messageId = await run(store.insertMessage({ conversationId, direction: "in", kind: "file", text: name }));
      const content = Buffer.from(text);
      return await run(store.insertFile({
        conversationId,
        messageId,
        file: { kind: "document", file_id: name, file_unique_id: name, file_name: name },
        name,
        content,
        sha256: createHash("sha256").update(content).digest("hex"),
      }));
    };
    const report = await kept("report.txt", "a report");
    const notes = await kept("notes.txt", "some notes");
    const written = (id: string, name: string) => join(stateDir, "telegram-files", id, name);
    const epoch = await run(store.claimTurn({ conversationId, chatId: "41", messageId: "10", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
    let found: string | null = null;
    const harness = recordingHarness([], () => (found = existsSync(written(report, "report.txt")) ? readFileSync(written(report, "report.txt"), "utf8") : null));
    await onPodHost({ store, stateDir, harness }, Effect.gen(function*() {
      const host = yield* HarnessHost;
      yield* host.startTurn({
        conversationId,
        chatId: "41",
        messageId: "10",
        harness: CODEX_HARNESS,
        workingDirectory: "/work",
        prompt: "go",
        fileIds: [report],
        resumeSession: null,
        modelChoice: null,
        epoch,
        jobId: null,
        attachedTurn: null,
        bayma: null,
        codex: null,
      });
      assert.equal(found, "a report", "the harness found the file as it ran");
      // No turn runs to take the steer, but its file is written all the same, for the turn it is queued for.
      assert.deepEqual(yield* host.steer(conversationId, "and these", [notes]), Option.none());
      assert.equal(readFileSync(written(notes, "notes.txt"), "utf8"), "some notes");
    }));
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});
