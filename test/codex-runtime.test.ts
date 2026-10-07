import assert from "node:assert/strict";
import { test } from "node:test";

import type { ThreadEvent } from "@openai/codex-sdk";
import { Effect, Logger } from "effect";

import { makeAppServer } from "../src/codex/app-server/client.ts";
import { executeCodexTurn } from "../src/codex/runtime.ts";
import type { CodexExecClient } from "../src/codex/transport.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { TurnPersistence } from "../src/harness/index.ts";

/** A Codex whose every thread runs one turn that edits three files, answers, and completes. */
function codexFactory(): CodexExecClient {
  const thread = {
    async runStreamed() {
      return {
        events: (async function* (): AsyncGenerator<ThreadEvent> {
          yield { type: "thread.started", thread_id: "thread-1" };
          yield {
            type: "item.completed",
            item: { id: "item-1", type: "file_change", status: "completed", changes: ["a", "b", "c"].map((path) => ({ path, kind: "update" as const })) },
          };
          yield { type: "item.completed", item: { id: "item-2", type: "agent_message", text: "done" } };
          yield { type: "turn.completed", usage: {
            input_tokens: 1,
            cached_input_tokens: 0,
            cache_write_input_tokens: 0,
            output_tokens: 1,
            reasoning_output_tokens: 0,
          } };
        })(),
      };
    },
  };
  return { startThread: () => thread, resumeThread: () => thread };
}

/** alasio's persistence, recording which of its methods are called, in order, and the blocks each store of them is given. */
function recordingPersistence(calls: string[], stored: unknown[][] = []): TurnPersistence {
  // The proxy answers every property with a method that records its call, so it has
  // whichever of the store's methods the turn calls; none returns anything the turn
  // reads but createPendingResponse, which returns the pending response's id.
  return new Proxy({} as TurnPersistence, {
    get: (_target, name) => (...args: unknown[]) =>
      Effect.sync(() => {
        calls.push(String(name));
        if (name === "appendBlocksToPending") stored.push(args[1] as unknown[]);
        return name === "createPendingResponse" ? "pending-1" : undefined;
      }),
  });
}

/** Runs a turn of `codexFactory`'s Codex on `persistence`, its response made complete once `beforeResponseComplete` is done. */
const runTurn = (persistence: TurnPersistence, beforeResponseComplete: (sessionId: string | null | undefined) => Effect.Effect<void> = () => Effect.void) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // The exec transport's turn, which no app-server serves.
    const appServer = yield* makeAppServer({ spawn: () => Effect.sync(() => assert.fail("no app-server runs in this test")) });
    return yield* executeCodexTurn({
      prompt: "hello",
      resumeSession: null,
      threadKey: "conversation-1",
      chatId: "1",
      messageId: "2",
      workingDirectory: process.cwd(),
      modelChoice: null,
      persistence,
      appServer,
      codexFactory,
      folderBayma: () => Effect.succeed({ type: "http", url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} }),
      beforeResponseComplete,
    });
  })).pipe(Effect.provide([ActiveTurns.layer, Logger.layer([])])));

test("a turn's response is marked complete only once beforeResponseComplete has finished", async () => {
  const calls: string[] = [];
  const result = await runTurn(recordingPersistence(calls), (sessionId) =>
    Effect.sleep("20 millis").pipe(Effect.andThen(Effect.sync(() => calls.push(`beforeResponseComplete:${sessionId}`)))));

  assert.equal(result.responseCompleted, true);
  const flushed = calls.indexOf("beforeResponseComplete:thread-1");
  assert.ok(flushed >= 0);
  assert.ok(flushed < calls.indexOf("markPendingResponseComplete"));
});

test("the blocks an event adds to a turn's response are stored together, in one statement", async () => {
  const stored: unknown[][] = [];
  const result = await runTurn(recordingPersistence([], stored));

  assert.deepEqual(stored, [
    [{ type: "tool", name: "Edit" }, { type: "tool", name: "Edit" }, { type: "tool", name: "Edit" }],
    [{ type: "text", content: "done", phase: null }],
  ]);
  assert.deepEqual(result.blockSequence, stored.flat());
});
