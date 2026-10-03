// @ts-nocheck
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { AppServerNotificationQueue } from "../src/codex/app-server/notification-queue.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { AppServerThreadClient } from "../src/codex/app-server/thread-client.ts";
import { finalResponseToMarkdown } from "../src/codex/response-markdown.ts";
import { interruptCodexTurn } from "../src/codex/runtime.ts";
import { RestartRecovery } from "../src/codex/restart-recovery.ts";
import { StatusReporter } from "../src/codex/status-reporter.ts";
import { SqliteStore } from "../src/persistence/store.ts";
import { TelegramCodexApp } from "../src/telegram/app.ts";
import { Client, TelegramApiError } from "../src/telegram/client.ts";

const silentLog = { info() {}, warn() {}, error() {} };

test("stop remains active until transport cleanup finishes", async () => {
  let release;
  const cleanup = new Promise((resolve) => { release = resolve; });
  const activeQueries = new Map();
  activeQueries.set("thread", { abort: async () => {
    await cleanup;
    activeQueries.delete("thread");
  } });
  let finished = false;
  const stopping = interruptCodexTurn(activeQueries, "thread").then(() => { finished = true; });
  await Promise.resolve();
  assert.equal(activeQueries.has("thread"), true);
  assert.equal(finished, false);
  release();
  await stopping;
  assert.equal(finished, true);
});

test("failed app-server interrupt forgets local turn ownership", async () => {
  const infoLogs = [];
  const notifications = new AppServerNotificationQueue({ log: silentLog });
  notifications.rememberTurn("thread", "stale-turn");
  const client = new AppServerThreadClient({
    notifications,
    log: { ...silentLog, info: (message) => infoLogs.push(message) },
    rpc: { request: async () => { throw new Error("no interrupt response"); } },
  });
  await assert.rejects(client.interrupt("thread", "stream-error"), /no interrupt response/);
  assert.equal(notifications.getCurrentTurnId("thread"), undefined);
  assert.match(infoLogs[0], /origin=stream-error/);
});

test("final response uses the upstream final-answer phase", () => {
  const blocks = [
    { type: "text", content: "I am checking the repository.", phase: "commentary" },
    { type: "tool", name: "Bash" },
    { type: "text", content: "The final answer.", phase: "final_answer" },
    { type: "text", content: "Late commentary must not replace it.", phase: "commentary" },
  ];
  assert.equal(finalResponseToMarkdown(blocks), "The final answer.");
});

test("final response rejects phase-less text", () => {
  const blocks = [
    { type: "text", content: "Legacy progress." },
    { type: "tool", name: "Bash" },
    { type: "text", content: "Legacy final answer." },
  ];
  assert.equal(finalResponseToMarkdown(blocks), "");
});

test("final response does not promote commentary when phased output lacks a final answer", () => {
  const blocks = [
    { type: "text", content: "Still investigating.", phase: "commentary" },
  ];
  assert.equal(finalResponseToMarkdown(blocks), "");
});

test("missing phased final answer does not enqueue fabricated completion text", async () => {
  const enqueued = [];
  const posted = [];
  const reporter = new StatusReporter({
    client: {},
    store: { markPendingAsPosted: (id) => posted.push(id) },
    outbox: { enqueueText: (item) => enqueued.push(item) },
    workflowWaits: new Map(),
    workflowWakeEvents: new Map(),
    log: silentLog,
  });

  await reporter.postResponse({ chatId: "123", response: "", pendingResponseId: "pending-1" });

  assert.deepEqual(enqueued, []);
  assert.deepEqual(posted, ["pending-1"]);
});

test("SQLite response recovery exposes only terminal upstream responses", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-response-phase-"));
  try {
    const store = new SqliteStore(root);
    store.setActiveHarness(store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
    const pendingResponseId = store.createPendingResponse("123", "9");
    store.appendBlockToPending(pendingResponseId, {
      type: "text",
      content: "The final answer.",
      phase: "final_answer",
    });

    assert.deepEqual(store.getCompletedResponsesPendingDelivery(), []);
    store.markPendingResponseComplete(pendingResponseId);
    const [completed] = store.getCompletedResponsesPendingDelivery();
    assert.equal(finalResponseToMarkdown(completed.blocks), "The final answer.");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("completed response recovery enqueues one final answer exactly once", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-response-once-"));
  try {
    const store = new SqliteStore(root);
    store.setActiveHarness(store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
    const pendingResponseId = store.createPendingResponse("123", "9");
    store.appendBlockToPending(pendingResponseId, {
      type: "text",
      content: "Still working.",
      phase: "commentary",
    });
    store.appendBlockToPending(pendingResponseId, {
      type: "text",
      content: "The final answer.",
      phase: "final_answer",
    });
    store.markPendingResponseComplete(pendingResponseId);
    const reporter = new StatusReporter({
      client: {},
      store,
      outbox: { enqueueText: (args) => store.enqueueOutboxText(args) },
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      log: silentLog,
    });

    await reporter.flushCompletedResponses();
    await reporter.flushCompletedResponses();

    const due = store.getDueOutbox(10);
    assert.equal(due.length, 1);
    assert.equal(due[0].text, "The final answer.");
    assert.equal(due[0].pending_response_id, pendingResponseId);
    assert.deepEqual(store.getCompletedResponsesPendingDelivery(), []);
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("outbox migration keeps sent evidence over a pending duplicate", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-outbox-migration-"));
  try {
    const first = new SqliteStore(root);
    first.setActiveHarness(first.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
    const pendingResponseId = first.createPendingResponse("123", "9");
    const sentId = first.enqueueOutboxText({ chatId: "123", text: "done", pendingResponseId });
    first.markOutboxSent(sentId);
    first.db.exec("drop index idx_telegram_outbox_pending_response");
    first.db.prepare(`
      insert into telegram_outbox
        (id, conversation_id, chat_id, kind, text, options_json, pending_response_id, state, available_at)
      select 'duplicate-pending', conversation_id, chat_id, kind, text, options_json, pending_response_id, 'pending', available_at
      from telegram_outbox
      where id = ?
    `).run(sentId);
    first.close();

    const migrated = new SqliteStore(root);
    const rows = migrated.db.prepare("select id, state from telegram_outbox where pending_response_id = ?").all(pendingResponseId);
    assert.deepEqual(rows, [{ id: sentId, state: "sent" }]);
    migrated.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("SQLite prompt jobs and outbox survive process boundaries", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-reliability-"));
  try {
    const first = new SqliteStore(root);
    const conversationId = first.upsertConversation({ chatId: "123", user: { id: 123 } });
    first.setActiveHarness(conversationId, CODEX_HARNESS);
    const job = first.enqueuePromptJob({ conversationId, chatId: "123", messageId: "9", prompt: "hello" });
    const claimed = first.claimNextPromptJob(conversationId);
    assert.equal(claimed.id, job.id);
    first.enqueueOutboxText({ chatId: "123", text: "done", pendingResponseId: null });
    first.close();

    const second = new SqliteStore(root);
    second.recoverPromptJobsAfterRestart();
    assert.equal(second.claimNextPromptJob(conversationId).prompt, "hello");
    assert.equal(second.getDueOutbox(10)[0].text, "done");
    second.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Telegram app wires the durable outbox into final response delivery", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-composition-"));
  try {
    const app = new TelegramCodexApp({
      telegramBotToken: "test-token",
      allowedUserIds: "",
      workingDirectory: root,
      workspaceRoot: root,
      stateDir: join(root, ".alasio"),
      dbPath: join(root, ".alasio", "alasio.sqlite"),
      hookPort: 0,
      warmLinkedSessions: false,
    });
    assert.equal(app.turns.status.outbox, app.outbox);
    app.store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("restart reconciliation preserves upstream-completed prompt jobs", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-completed-job-"));
  try {
    const store = new SqliteStore(root);
    const conversationId = store.upsertConversation({ chatId: "123", user: { id: 123 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const job = store.enqueuePromptJob({ conversationId, chatId: "123", messageId: "10", prompt: "finish" });
    store.claimNextPromptJob(conversationId);
    store.markPromptJobUpstreamStarted(job.id, "session", "turn");
    store.markPromptJobUpstreamCompleted(job.id, "session", "turn");
    assert.deepEqual(store.recoverPromptJobsAfterRestart(), [conversationId]);
    assert.equal(store.getPromptJob(job.id).state, "completed");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("self-restart recovery stages a distinct durable continuation", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-restart-continuation-"));
  try {
    const store = new SqliteStore(root);
    const conversationId = store.upsertConversation({ chatId: "123", user: { id: 123 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    const original = store.enqueuePromptJob({
      conversationId,
      chatId: "123",
      messageId: "3334",
      prompt: "Apply the change and restart.",
    });
    store.claimNextPromptJob(conversationId);
    store.markPromptJobUpstreamStarted(original.id, "session-1", "turn-1");
    const partialResponseId = store.createPendingResponse("123", "3334", "session-1");
    store.appendBlockToPending(partialResponseId, {
      type: "text",
      content: "Restarting now.",
      phase: "commentary",
    });
    store.upsertActiveTurn({
      conversationId,
      chatId: "123",
      messageId: "3334",
      sessionId: "session-1",
      pendingResponseId: partialResponseId,
      prompt: original.prompt,
    });
    store.recordRestartEvent({
      cause: "self_induced",
      thread_key: conversationId,
      channel: "123",
      thread_ts: "3334",
      session_id: "session-1",
      timestamp: 1234,
    });

    store.recoverPromptJobsAfterRestart();
    const recovery = new RestartRecovery({ store });
    await recovery.recoverInterruptedTurns();
    await recovery.recoverInterruptedTurns();

    assert.equal(store.getPromptJob(original.id).state, "interrupted");
    assert.deepEqual(store.getActiveTurns(), []);
    assert.equal(store.getRestartEvent(conversationId), null);
    assert.deepEqual(
      store.db.prepare("select distinct posted from response_blocks where pending_response_id = ?").all(partialResponseId),
      [{ posted: 1 }],
    );
    assert.equal(store.db.prepare("select count(*) count from prompt_jobs").get().count, 2);
    const continuation = store.claimNextPromptJob(conversationId);
    assert.notEqual(continuation.message_id, "3334");
    assert.match(continuation.message_id, /^restart:3334:/);
    assert.match(continuation.prompt, /SYSTEM RESTART EVENT/);
    assert.equal(store.getSessionId(conversationId), "session-1");

    const completedResponseId = store.createPendingResponse("123", continuation.message_id, "session-1");
    store.appendBlockToPending(completedResponseId, { type: "text", content: "Recovered commentary.", phase: "commentary" });
    store.appendBlockToPending(completedResponseId, { type: "text", content: "Recovered final answer.", phase: "final_answer" });
    store.markPendingResponseComplete(completedResponseId);
    const reporter = new StatusReporter({
      client: {},
      store,
      outbox: { enqueueText: (args) => store.enqueueOutboxText(args) },
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      log: silentLog,
    });
    await reporter.flushCompletedResponses();
    assert.deepEqual(store.getDueOutbox(10).map((item) => item.text), ["Recovered final answer."]);
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("stale turn completion cannot clear a replacement turn", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-turn-identity-"));
  try {
    const store = new SqliteStore(root);
    const conversationId = store.upsertConversation({ chatId: "123", user: { id: 123 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.upsertActiveTurn({ conversationId, chatId: "123", messageId: "1", pendingResponseId: "old" });
    store.upsertActiveTurn({ conversationId, chatId: "123", messageId: "2", pendingResponseId: "new" });
    store.clearActiveTurn(conversationId, "old");
    assert.equal(store.getActiveTurns()[0].pending_response_id, "new");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("callback actions capture the mounted session generation", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-callback-session-"));
  try {
    const store = new SqliteStore(root);
    const conversationId = store.upsertConversation({ chatId: "123", user: { id: 123 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setSessionId(conversationId, "session-a");
    const id = store.createCallbackAction({ conversationId, kind: "goal:resume", payload: {} });
    assert.equal(store.consumeCallbackAction(id).payload.expectedSessionId, "session-a");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Telegram API error exposes retry_after", () => {
  const error = new TelegramApiError("sendMessage", { status: 429 }, { parameters: { retry_after: 3 } });
  assert.equal(error.retryAfterMs, 3000);
});

test("Telegram client serializes concurrent outbound messages", async () => {
  const client = new Client("test-token");
  const calls = [];
  client.call = async (_method, payload) => {
    calls.push(`start:${payload.text}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
    calls.push(`end:${payload.text}`);
    return { message_id: calls.length };
  };
  await Promise.all([client.sendMessage("1", "first", { format: "plain" }), client.sendMessage("1", "second", { format: "plain" })]);
  assert.deepEqual(calls, ["start:first", "end:first", "start:second", "end:second"]);
});
