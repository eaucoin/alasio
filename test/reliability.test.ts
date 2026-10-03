import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { Array as Arr, Deferred, Effect, Exit, Fiber, Layer, Logger as EffectLogger, Scope } from "effect";
import { makeAppServerNotifications } from "../src/codex/app-server/notification-queue.ts";
import { AppServerRequestTimeout } from "../src/codex/app-server/rpc-client.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { makeAppServerThreads } from "../src/codex/app-server/thread-client.ts";
import { finalResponseToMarkdown } from "../src/codex/response-markdown.ts";
import { recoverInterruptedTurns } from "../src/codex/restart-recovery.ts";
import { makeStatusReporter, type StatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turn-controller.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { SqliteStore, Store } from "../src/persistence/store.ts";
import { type AlasioOptions, alasioServices } from "../src/alasio.ts";
import { TelegramApiError } from "../src/telegram/client.ts";
import { Outbox, type OutboxText } from "../src/telegram/outbox.ts";
import { botApiClient, paramsOf } from "./support/bot-api.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { noWorkflowHooks } from "./support/turns.ts";

/** A Telegram client for a reporter that only queues replies, never sending or editing itself. */
const unusedClient = recordingTelegram({
  sendMessage: () => assert.fail("no message is sent directly"),
  editMessageText: () => assert.fail("no message is edited"),
});

/** The status reporter of `store`, whose replies `enqueue` queues. */
function reporterFor(store: SqliteStore, enqueue: (text: OutboxText) => string): StatusReporter {
  return Effect.runSync(makeStatusReporter().pipe(Effect.provide(Layer.mergeAll(
    Layer.succeed(Store, store),
    unusedClient.layer,
    Layer.succeed(Outbox, Outbox.of({ enqueueText: (text) => Effect.sync(() => enqueue(text)), deliverDue: Effect.void })),
    noWorkflowHooks,
  ))));
}

test("a stop is done only once the turn has let go of its conversation", async () => {
  await Effect.runPromise(Effect.gen(function*() {
    const activeTurns = yield* ActiveTurns;
    const cleanup = yield* Deferred.make<void>();
    const registration = yield* Scope.make();
    // A turn whose stop lets go of the conversation once its transport is cleaned up.
    yield* activeTurns.register("thread", {
      stop: () => Deferred.await(cleanup).pipe(Effect.andThen(Scope.close(registration, Exit.void))),
      steer: () => Effect.succeed(false),
      cliInitiated: false,
    }).pipe(Scope.provide(registration));
    const stopping = yield* Effect.forkChild(activeTurns.stop("thread", "interrupt"));
    yield* Effect.yieldNow;
    assert.equal(yield* activeTurns.isBusy("thread"), true);
    assert.equal(stopping.pollUnsafe(), undefined, "the stop waits for the turn");
    yield* Deferred.succeed(cleanup, undefined);
    assert.equal(yield* Fiber.join(stopping), true);
    assert.equal(yield* activeTurns.isBusy("thread"), false);
    assert.equal(yield* activeTurns.stop("thread", "interrupt"), false, "no turn is left to stop");
  }).pipe(Effect.provide(ActiveTurns.layer)));
});

test("failed app-server interrupt forgets local turn ownership", async () => {
  const infoLogs: string[] = [];
  const logger = EffectLogger.make(({ message }) => {
    infoLogs.push(Arr.ensure(message).join(" "));
  });
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const notifications = yield* makeAppServerNotifications;
    yield* notifications.rememberTurn("thread", "stale-turn");
    const client = makeAppServerThreads({
      start: () => Effect.sync(() => assert.fail("an interrupt starts no app-server")),
      request: () => Effect.fail(new AppServerRequestTimeout({ method: "turn/interrupt" })),
      whenGone: Effect.succeed(Effect.never),
    }, notifications);
    const interrupted = yield* Effect.flip(client.interrupt("thread", "stream-error"));
    assert.equal(interrupted.message, "Codex app-server request timed out: turn/interrupt");
    assert.equal(yield* notifications.currentTurnId("thread"), undefined);
  })).pipe(Effect.provide(EffectLogger.layer([logger]))));
  assert.match(infoLogs[0] ?? "", /origin=stream-error/);
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
  const root = mkdtempSync(join(tmpdir(), "alasio-response-missing-"));
  try {
    const store = new SqliteStore(root);
    store.setActiveHarness(store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
    const pendingResponseId = store.createPendingResponse("123", "9");
    store.appendBlockToPending(pendingResponseId, { type: "text", content: "Still investigating.", phase: "commentary" });
    store.markPendingResponseComplete(pendingResponseId);
    const enqueued: OutboxText[] = [];
    const reporter = reporterFor(store, (item) => {
      enqueued.push(item);
      return "outbox-1";
    });

    await Effect.runPromise(reporter.postResponse({ chatId: "123", response: "", pendingResponseId, status: null }));

    assert.deepEqual(enqueued, []);
    // Posted as it is, with nothing to deliver.
    assert.deepEqual(store.getCompletedResponsesPendingDelivery(), []);
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
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
    assert.ok(completed);
    assert.equal(finalResponseToMarkdown(completed.blocks), "The final answer.");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a block for a pending response that does not exist is dropped with a warning", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-response-unknown-"));
  try {
    const store = new SqliteStore(root);
    store.appendBlockToPending("no-such-response", { type: "text", content: "Lost.", phase: "final_answer" });
    assert.equal(store.db.prepare<[], { n: number }>("select count(*) as n from response_blocks").get()?.n, 0);
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
    const reporter = reporterFor(store, (args) => store.enqueueOutboxText(args));

    await Effect.runPromise(reporter.flushCompletedResponses);
    await Effect.runPromise(reporter.flushCompletedResponses);

    const due = store.getDueOutbox(10);
    assert.equal(due.length, 1);
    assert.equal(due[0]?.text, "The final answer.");
    assert.equal(due[0]?.pending_response_id, pendingResponseId);
    assert.deepEqual(store.getCompletedResponsesPendingDelivery(), []);
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a reply waiting to be retried holds back the replies queued after it to its chat, and no other chat's", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-outbox-order-"));
  try {
    const store = new SqliteStore(root);
    for (const chatId of ["1", "2"]) store.setActiveHarness(store.upsertConversation({ chatId, user: { id: Number(chatId) } }), CODEX_HARNESS);
    const first = store.enqueueOutboxText({ chatId: "1", text: "first" });
    store.enqueueOutboxText({ chatId: "1", text: "second" });
    store.enqueueOutboxText({ chatId: "2", text: "elsewhere" });
    assert.deepEqual(store.getDueOutbox().map((reply) => reply.text), ["first", "elsewhere"]);
    store.rescheduleOutbox(first, new Error("Bad Gateway"), 60_000);
    assert.deepEqual(store.getDueOutbox().map((reply) => reply.text), ["elsewhere"]);
    store.markOutboxSent(first);
    assert.deepEqual(store.getDueOutbox().map((reply) => reply.text), ["second", "elsewhere"]);
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
    assert.equal(claimed?.id, job.id);
    first.enqueueOutboxText({ chatId: "123", text: "done", pendingResponseId: null });
    first.close();

    const second = new SqliteStore(root);
    second.recoverPromptJobsAfterRestart();
    assert.equal(second.claimNextPromptJob(conversationId)?.prompt, "hello");
    assert.equal(second.getDueOutbox(10)[0]?.text, "done");
    second.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("alasio's services wire the durable outbox into final response delivery", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-composition-"));
  // Telegram refuses every connection, so what is queued stays queued.
  const apiRoot = process.env["TELEGRAM_API_ROOT"];
  process.env["TELEGRAM_API_ROOT"] = "http://127.0.0.1:9";
  try {
    const options: AlasioOptions = {
      telegramBotToken: "test-token",
      allowedUserIds: "",
      workingDirectory: root,
      workspaceRoot: root,
      stateDir: join(root, ".alasio"),
      dbPath: join(root, ".alasio", "alasio.sqlite"),
      hookPort: 0,
      warmLinkedSessions: false,
      defaultHarness: null,
    };
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const store = yield* Store;
      store.setActiveHarness(store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
      const pendingResponseId = store.createPendingResponse("123", "9");
      store.appendBlockToPending(pendingResponseId, { type: "text", content: "Delivered durably.", phase: "final_answer" });
      store.markPendingResponseComplete(pendingResponseId);
      yield* (yield* Turns).flushCompletedResponses;
      assert.deepEqual(store.getCompletedResponsesPendingDelivery(), []);
      assert.equal(store.getPendingOutboxCount(), 1);
    }).pipe(Effect.provide(alasioServices(options)))));
  } finally {
    if (apiRoot === undefined) delete process.env["TELEGRAM_API_ROOT"];
    else process.env["TELEGRAM_API_ROOT"] = apiRoot;
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
    assert.equal(store.getPromptJob(job.id)?.state, "completed");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("restart reconciliation runs again only the prompts never sent to the agent", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-dispatched-job-"));
  try {
    const store = new SqliteStore(root);
    const jobs = new Map<string, string>();
    for (const [chatId, dispatched] of [["1", false], ["2", true]] as const) {
      const conversationId = store.upsertConversation({ chatId, user: { id: Number(chatId) } });
      store.setActiveHarness(conversationId, CODEX_HARNESS);
      const job = store.enqueuePromptJob({ conversationId, chatId, messageId: "10", prompt: "do it" });
      store.claimNextPromptJob(conversationId);
      if (dispatched) store.markPromptJobDispatched(job.id);
      jobs.set(chatId, job.id);
    }
    assert.deepEqual(store.recoverPromptJobsAfterRestart(), []);
    assert.equal(store.getPromptJob(jobs.get("1") ?? "")?.state, "pending");
    assert.equal(store.getPromptJob(jobs.get("2") ?? "")?.state, "interrupted");
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
    Effect.runSync(recoverInterruptedTurns(store));
    Effect.runSync(recoverInterruptedTurns(store));

    assert.equal(store.getPromptJob(original.id)?.state, "interrupted");
    assert.deepEqual(store.getActiveTurns(), []);
    assert.equal(store.getRestartEvent(conversationId), null);
    assert.deepEqual(
      store.db.prepare("select distinct posted from response_blocks where pending_response_id = ?").all(partialResponseId),
      [{ posted: 1 }],
    );
    assert.equal(store.db.prepare<[], { count: number }>("select count(*) count from prompt_jobs").get()?.count, 2);
    const continuation = store.claimNextPromptJob(conversationId);
    assert.ok(continuation);
    assert.notEqual(continuation.message_id, "3334");
    assert.match(continuation.message_id, /^restart:3334:/);
    assert.match(continuation.prompt, /SYSTEM RESTART EVENT/);
    assert.equal(store.getSessionId(conversationId), "session-1");

    const completedResponseId = store.createPendingResponse("123", continuation.message_id, "session-1");
    store.appendBlockToPending(completedResponseId, { type: "text", content: "Recovered commentary.", phase: "commentary" });
    store.appendBlockToPending(completedResponseId, { type: "text", content: "Recovered final answer.", phase: "final_answer" });
    store.markPendingResponseComplete(completedResponseId);
    const reporter = reporterFor(store, (args) => store.enqueueOutboxText(args));
    await Effect.runPromise(reporter.flushCompletedResponses);
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
    assert.equal(store.getActiveTurns()[0]?.pending_response_id, "new");
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
    assert.equal(store.consumeCallbackAction(id)?.payload["expectedSessionId"], "session-a");
    store.close();
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Telegram API error exposes retry_after", () => {
  const error = TelegramApiError.fromResponse("sendMessage", 429, {
    ok: false,
    error_code: 429,
    description: "Too Many Requests: retry after 3",
    parameters: { retry_after: 3 },
  });
  assert.equal(error.retryAfterMs, 3000);
  assert.equal(error.message, 'Telegram sendMessage failed: HTTP 429 {"ok":false,"error_code":429,"description":"Too Many Requests: retry after 3","parameters":{"retry_after":3}}');
});

test("Telegram client serializes concurrent outbound messages", async () => {
  const calls: string[] = [];
  const client = await botApiClient(async (call) => {
    const { text } = paramsOf(call, "sendMessage");
    calls.push(`start:${text}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
    calls.push(`end:${text}`);
    return { message_id: calls.length };
  });
  await Effect.runPromise(Effect.all([
    client.sendMessage("1", "first", { format: "plain" }),
    client.sendMessage("1", "second", { format: "plain" }),
  ], { concurrency: "unbounded" }));
  assert.deepEqual(calls, ["start:first", "end:first", "start:second", "end:second"]);
});
