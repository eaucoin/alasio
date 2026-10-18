/**
 * The harness host (src/harness/host.ts), as alasio's control plane reaches its turns:
 * given data alone, it runs a conversation's turn on its harness with the claim's writes
 * and its job's progress, and tells, as its events, what the harness does on its own.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Option, PubSub } from "effect";

import { HarnessHost } from "../src/harness/host.ts";
import type { Harness, TurnParams, TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { run, testStore } from "./support/store.ts";
import { turnsStub, withServices } from "./support/turns.ts";

/**
 * A Codex harness whose one turn hands what it was given to `seen`, and acts as a harness
 * that runs on between prompts would; `onRun` says what it finds as it runs.
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
        yield* params.onBackgroundResponse ?? Effect.void;
        yield* params.onIdle ?? Effect.void;
        const result: TurnResult = { blockSequence: [], sessionId: "session-1", pendingResponseId: "pending-1", interrupted: false, responseCompleted: true };
        return result;
      }),
    listModels: unused("listModels"),
    defaultModelChoice: unused("defaultModelChoice"),
  };
}

test("a turn started through the host runs on its harness under the turn's claim, its job's progress recorded, and what the harness does on its own told as the host's events", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: "41", messageId: "10", prompt: "go" }));
  await run(store.claimNextPromptJob(conversationId));
  const epoch = await run(store.claimTurn({ conversationId, chatId: "41", messageId: "10", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
  const seen: TurnParams[] = [];
  await withServices({ store, harnesses: { [CODEX_HARNESS]: recordingHarness(seen) }, turns: turnsStub({}) }, (alasio) =>
    alasio.runPromise(Effect.scoped(Effect.gen(function*() {
      const host = yield* HarnessHost;
      const events = yield* host.events;
      const before = Date.now();
      const result = yield* host.startTurn({
        conversationId,
        chatId: "41",
        messageId: "10",
        harness: CODEX_HARNESS,
        workingDirectory: "/work",
        prompt: "go",
        fileIds: [],
        resumeSession: null,
        modelChoice: null,
        epoch,
        jobId: job.id,
        attachedTurn: null,
      });
      assert.equal(result.pendingResponseId, "pending-1");
      assert.deepEqual([yield* PubSub.take(events), yield* PubSub.take(events)], [
        { kind: "background-response", conversationId },
        { kind: "idle", conversationId },
      ]);
      // A fresh session of the harness, through the host.
      assert.equal(yield* host.startFreshSession({ conversationId, harness: CODEX_HARNESS, workingDirectory: "/work" }), `fresh-of-${conversationId}`);
      // No turn of it runs once it has ended; nor did one start on its own, nor does the harness work on.
      const { idleSince, ...status } = yield* host.status(conversationId);
      assert.deepEqual(status, { busy: false, running: false, cliInitiated: false, held: false, backgroundWork: false });
      assert.ok(idleSince !== null && idleSince <= Date.now());
      // While it is held for a turn, it runs one, of alasio's; idle again since that let go.
      yield* Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* host.reserve(conversationId), true);
        assert.deepEqual(yield* host.status(conversationId), { busy: true, running: true, cliInitiated: false, held: false, backgroundWork: false, idleSince: null });
      }));
      assert.ok(((yield* host.status(conversationId)).idleSince ?? 0) >= before);
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
