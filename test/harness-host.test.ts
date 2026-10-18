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

import { Context, Effect, Layer, Option, PubSub, type Scope } from "effect";

import type { ResponseBlock } from "../src/codex/event-projection.ts";
import { holdConversations } from "../src/harness/holds.ts";
import { HarnessHost, type TurnStart } from "../src/harness/host.ts";
import { type Harness, NoWorkspaceMounted, type TurnParams, type TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { remoteHarnessHost } from "../src/harness/remote-host.ts";
import { serveHost } from "../src/harness/serve.ts";
import { TurnFenced } from "../src/persistence/store.ts";
import { holdsIn, newSchema, run, testStore } from "./support/store.ts";
import { turnsStub, withServices } from "./support/turns.ts";

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
      createForkedSession: unused("createForkedSession"),
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
        const result: TurnResult = { blockSequence, sessionId: "session-1", pendingResponseId, interrupted: false, responseCompleted: true };
        return result;
      }),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

/** The host as the control plane reaches it: in its process, or elsewhere, over HTTP (src/harness/serve.ts, src/harness/remote-host.ts), serving `conversationId`. */
const REACHES: Readonly<Record<string, (host: HarnessHost["Service"], conversationId: string) => Effect.Effect<HarnessHost["Service"], unknown, Scope.Scope>>> = {
  "in alasio's process": (host) => Effect.succeed(host),
  "elsewhere, over HTTP": (host, conversationId) =>
    Effect.gen(function*() {
      const { port } = yield* Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token" }), HarnessHost, host);
      const remote = yield* Layer.build(remoteHarnessHost({ url: `http://127.0.0.1:${port}`, token: "host-token", panels: host.panels }));
      return Context.get(remote, HarnessHost);
    }),
};

for (const [reach, reached] of Object.entries(REACHES)) {
  test(`a turn started through the host ${reach} runs on its harness under the turn's claim, its job's progress recorded, what the harness does on its own told as the host's events, and what fails failing as it does`, async () => {
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
    });
    await withServices({ store, harnesses: { [CODEX_HARNESS]: recordingHarness(seen) }, turns: turnsStub({}) }, (alasio) =>
      alasio.runPromise(Effect.scoped(Effect.gen(function*() {
        const host = yield* reached(yield* HarnessHost, conversationId);
        const events = yield* host.events;
        // Held for one turn at a time, until the scope it is held in closes.
        yield* Effect.scoped(Effect.gen(function*() {
          assert.equal(yield* host.reserve(conversationId), true);
          assert.deepEqual(yield* host.status(conversationId), { busy: true, running: true, cliInitiated: false, held: false, backgroundWork: false, idleSince: null });
          assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
        }));
        const { idleSince, ...idle } = yield* host.status(conversationId);
        assert.deepEqual(idle, { busy: false, running: false, cliInitiated: false, held: false, backgroundWork: false });
        assert.ok(idleSince !== null && idleSince <= Date.now());
        // A fork's hold, a row of the store, keeps it busy as no turn, which no turn starts under.
        yield* Effect.scoped(Effect.gen(function*() {
          assert.equal(yield* holdConversations(host, store, [conversationId], "forking session fs-a into fs-b"), true);
          assert.deepEqual([(yield* host.status(conversationId)).busy, (yield* host.status(conversationId)).held, (yield* host.status(conversationId)).running], [true, true, false]);
          assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
        }));
        // And none is held where a turn runs.
        yield* Effect.scoped(Effect.gen(function*() {
          assert.equal(yield* host.reserve(conversationId), true);
          assert.equal(yield* Effect.scoped(holdConversations(host, store, [conversationId], "forking")), false);
        }));
        assert.deepEqual(yield* Effect.promise(() => holdsIn(schema)), []);

        const result = yield* host.startTurn(start("10", epoch, job.id));
        assert.deepEqual([result.blockSequence, result.sessionId, result.interrupted, result.responseCompleted], [[{ type: "text", content: "Done." }], "session-1", false, true]);
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
        // The panels reach the mount's harness through the host.
        const panels = Option.getOrNull(yield* host.panels(yield* store.getMount(conversationId)));
        assert.equal(panels?.displayName, harnessDisplayName(CODEX_HARNESS));
        assert.throws(() => panels?.listModels(), /the recording harness has no listModels/u);
      })))
    );
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

test("a host elsewhere not reached says nothing of its conversation and stops nothing, failing as not reached, so that no hold is made on what it cannot say", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A port no host listens on: a partition, as the control plane sees one.
    const host = Context.get(yield* Layer.build(remoteHarnessHost({ url: "http://127.0.0.1:1", token: "host-token", panels: () => Effect.succeedNone })), HarnessHost);
    assert.equal((yield* Effect.flip(host.status(conversationId)))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(host.interrupt(conversationId, "interrupt")))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(Effect.scoped(host.reserve(conversationId))))._tag, "HarnessHostError");
    assert.equal((yield* Effect.flip(Effect.scoped(holdConversations(host, store, [conversationId], "forking"))))._tag, "HarnessHostError");
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
    await withServices({ store, stateDir, harnesses: { [CODEX_HARNESS]: harness }, turns: turnsStub({}) }, (alasio) =>
      alasio.runPromise(Effect.gen(function*() {
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
        });
        assert.equal(found, "a report", "the harness found the file as it ran");
        // No turn runs to take the steer, but its file is written all the same, for the turn it is queued for.
        assert.deepEqual(yield* host.steer(conversationId, "and these", [notes]), Option.none());
        assert.equal(readFileSync(written(notes, "notes.txt"), "utf8"), "some notes");
      }))
    );
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});
