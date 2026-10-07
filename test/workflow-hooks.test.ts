import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Layer } from "effect";

import { CODEX_HARNESS } from "../src/harness/names.ts";
import { Store } from "../src/persistence/store.ts";
import { WorkflowHooks } from "../src/workflow/hook-server.ts";
import { testStore } from "./support/store.ts";

test("the workflow hook server records a session's wait under its turn's thread, and refuses what is not one", async () => {
  const services = WorkflowHooks.layer(0).pipe(Layer.provideMerge(Layer.succeed(Store, await testStore())));
  await Effect.runPromise(Effect.gen(function*() {
    const store = yield* Store;
    const conversationId = yield* store.upsertConversation({ chatId: "123", user: { id: 123 } });
    yield* store.setActiveHarness(conversationId, CODEX_HARNESS);
    yield* store.upsertActiveTurn({ conversationId, chatId: "123", messageId: "1", sessionId: "session-1", harness: CODEX_HARNESS });
    const [turn] = yield* store.getActiveTurns;
    assert.ok(turn);
    const hooks = yield* WorkflowHooks;
    const post = (path: string, body: string) =>
      fetch(`http://localhost:${hooks.port}${path}`, { method: "POST", headers: { "Content-Type": "application/json" }, body });
    const before = Date.now() / 1000;
    yield* Effect.promise(async () => {
      const recorded = await post("/hook/workflow", JSON.stringify({ session_id: "session-1", run_id: "123456789", wait_type: "watch", command: "gh run watch 123456789" }));
      assert.equal(recorded.status, 200);
      assert.equal(await recorded.text(), "OK");
      const missing = await post("/hook/workflow", JSON.stringify({ session_id: "session-2" }));
      assert.equal(missing.status, 400);
      assert.equal(await missing.text(), "Missing session_id or run_id");
      const elsewhere = await post("/hook/other", "{}");
      assert.equal(elsewhere.status, 404);
      assert.equal(await elsewhere.text(), "Not found");
    });
    const wait = hooks.waits.get("session-1");
    assert.ok(wait);
    assert.deepEqual({ ...wait, startedAt: 0 }, { runId: "123456789", waitType: "watch", command: "gh run watch 123456789", threadKey: turn.thread_key, startedAt: 0 });
    assert.ok(wait.startedAt >= before && wait.startedAt <= Date.now() / 1000);
    assert.equal(hooks.waits.has("session-2"), false);
  }).pipe(Effect.provide(services)));
});
