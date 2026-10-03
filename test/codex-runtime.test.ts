import assert from "node:assert/strict";
import { test } from "node:test";

import type { ThreadEvent } from "@openai/codex-sdk";

import { executeCodexTurn } from "../src/codex/runtime.ts";
import type { CodexExecClient } from "../src/codex/transport.ts";
import type { SqliteStore } from "../src/persistence/store.ts";

/** A Codex whose every thread runs one turn that answers and completes. */
function codexFactory(): CodexExecClient {
  const thread = {
    async runStreamed() {
      return {
        events: (async function* (): AsyncGenerator<ThreadEvent> {
          yield { type: "thread.started", thread_id: "thread-1" };
          yield { type: "item.completed", item: { id: "item-1", type: "agent_message", text: "done" } };
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

/** alasio's persistence, recording which of its methods are called, in order. */
function recordingPersistence(calls: string[]): SqliteStore {
  // The proxy answers every property with a method that records its call, so it has
  // whichever SqliteStore methods the turn calls; none returns anything the turn reads
  // but createPendingResponse, which returns the pending response's id.
  return new Proxy({} as SqliteStore, {
    get: (_target, name) => () => {
      calls.push(String(name));
      return name === "createPendingResponse" ? "pending-1" : undefined;
    },
  });
}

test("a turn's response is marked complete only once beforeResponseComplete has finished", async () => {
  const calls: string[] = [];
  const result = await executeCodexTurn({
    prompt: "hello",
    resumeSession: null,
    threadKey: "conversation-1",
    chatId: "1",
    messageId: "2",
    workingDirectory: process.cwd(),
    persistence: recordingPersistence(calls),
    activeQueries: new Map(),
    codexFactory,
    folderBayma: async () => ({ type: "http", url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} }),
    beforeResponseComplete: async (sessionId) => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      calls.push(`beforeResponseComplete:${sessionId}`);
    },
  });

  assert.equal(result.responseCompleted, true);
  const flushed = calls.indexOf("beforeResponseComplete:thread-1");
  assert.ok(flushed >= 0);
  assert.ok(flushed < calls.indexOf("markPendingResponseComplete"));
});
