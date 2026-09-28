import assert from "node:assert/strict";
import { test } from "node:test";

import { executeCodexTurn } from "../src/codex/runtime.js";

/** A Codex whose every thread runs one turn that answers and completes. */
function codexFactory() {
  const thread = {
    async runStreamed() {
      return {
        events: (async function* () {
          yield { type: "thread.started", thread_id: "thread-1" };
          yield { type: "item.completed", item: { id: "item-1", type: "agent_message", text: "done" } };
          yield { type: "turn.completed", usage: { input_tokens: 1, cached_input_tokens: 0, output_tokens: 1 } };
        })(),
      };
    },
  };
  return { startThread: () => thread, resumeThread: () => thread };
}

/** alasio's persistence, recording which of its methods are called, in order. */
function recordingPersistence(calls) {
  return new Proxy({}, {
    get: (_target, name) => (...args) => {
      calls.push(String(name));
      return name === "createPendingResponse" ? "pending-1" : undefined;
    },
  });
}

test("a turn's response is marked complete only once beforeResponseComplete has finished", async () => {
  const calls = [];
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
