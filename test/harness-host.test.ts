/**
 * The harness host (src/harness/host.ts), as alasio's control plane reaches its turns:
 * given data alone, it runs a conversation's turn on its harness with the claim's writes
 * and its job's progress, and tells, as its events, what the harness does on its own.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, PubSub } from "effect";

import { HarnessHost } from "../src/harness/host.ts";
import type { Harness, TurnParams, TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, harnessDisplayName } from "../src/harness/names.ts";
import { run, testStore } from "./support/store.ts";
import { turnsStub, withServices } from "./support/turns.ts";

/** A Codex harness whose one turn hands what it was given to `seen`, and acts as a harness that runs on between prompts would. */
function recordingHarness(seen: TurnParams[]): Harness {
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
      const result = yield* host.startTurn({
        conversationId,
        chatId: "41",
        messageId: "10",
        harness: CODEX_HARNESS,
        workingDirectory: "/work",
        prompt: "go",
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
      // No turn of it runs once it has ended.
      assert.deepEqual(yield* host.status(conversationId), { busy: false });
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
