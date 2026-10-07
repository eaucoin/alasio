/**
 * A branch environment's alasio (cli/src/commands/branch.ts): its state made its own as
 * it first starts.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { newSchema, run, testStore } from "./support/store.ts";

test("a branch's state, as it first starts, keeps its conversations and loses what was in flight where it was branched from, once", async () => {
  const schema = newSchema();
  const main = await testStore({ schema });
  const conversationId = await run(main.upsertConversation({ chatId: 1001 }));
  await run(main.setActiveHarness(conversationId, "codex"));
  await run(main.setWorkingDirectory(conversationId, "sessionfs:fs-0123456789abcdef"));
  await run(main.setTelegramOffset(41));
  await run(main.recordTelegramUpdate({ update_id: 41, message: { message_id: 7, date: 0, from: { id: 1001, is_bot: false, first_name: "Operator" }, chat: { id: 1001, type: "private", first_name: "Operator" }, text: "queued" } }));
  await run(main.enqueuePromptJob({ conversationId, chatId: 1001, messageId: 7, prompt: "queued", harness: "codex" }));
  const pending = await run(main.createPendingResponse(1001, 8));
  await run(main.upsertActiveTurn({ conversationId, chatId: "1001", messageId: "8", harness: "codex", pendingResponseId: pending }));
  await run(main.markPendingResponseComplete(pending));
  await run(main.enqueueOutboxText({ chatId: 1001, text: "on its way" }));
  const [button] = await run(main.createCallbackActions(conversationId, [{ kind: "refresh" }]));
  await run(main.recordRestartEvent({ cause: "operator_induced", thread_key: conversationId, channel: "1001", thread_ts: "8", session_id: null }));
  await run(main.setCodexLogin('{"tokens":{"refresh_token":"once"}}'));

  const branch = await testStore({ schema, branch: "try-codex" });
  assert.deepEqual(await run(branch.getMount(conversationId)), { harness: "codex", workingDirectory: "sessionfs:fs-0123456789abcdef", sessionId: null });
  assert.equal(await run(branch.getTelegramOffset), undefined);
  assert.deepEqual(await run(branch.listPendingPromptConversations), []);
  assert.equal(await run(branch.hasOpenPromptJobs(conversationId)), false);
  assert.deepEqual(await run(branch.getActiveTurns), []);
  assert.deepEqual(await run(branch.getCompletedResponsesPendingDelivery), []);
  assert.equal(await run(branch.getPendingOutboxCount), 0);
  assert.equal(await run(branch.getRestartEvent(conversationId)), null);
  assert.equal(await run(branch.getCodexLogin), null);
  assert.equal(await run(branch.consumeCallbackAction(button ?? "")), null);
  assert.equal(await run(branch.getState("branch")), "try-codex");

  // Started once: what the branch does itself is kept when it starts again.
  await run(branch.setTelegramOffset(3));
  assert.equal(await run((await testStore({ schema, branch: "try-codex" })).getTelegramOffset), 3);
  // A branch of the branch is made its own in turn.
  assert.equal(await run((await testStore({ schema, branch: "try-again" })).getTelegramOffset), undefined);
});

test("main's state is never made a branch's", async () => {
  const schema = newSchema();
  await run((await testStore({ schema })).setTelegramOffset(41));
  const main = await testStore({ schema });
  assert.equal(await run(main.getTelegramOffset), 41);
  assert.equal(await run(main.getState("branch")), null);
});
