import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { Array as Arr, Deferred, Effect, Exit, Fiber, Layer, Logger as EffectLogger, Scope } from "effect";
import pg from "pg";
import { makeAppServerNotifications } from "../src/codex/app-server/notification-queue.ts";
import { AppServerRequestTimeout } from "../src/codex/app-server/rpc-client.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { makeAppServerThreads } from "../src/codex/app-server/thread-client.ts";
import { finalResponseToMarkdown } from "../src/codex/response-markdown.ts";
import { recoverInterruptedTurns } from "../src/codex/restart-recovery.ts";
import { makeStatusReporter, type StatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turns.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { StoreError } from "../src/persistence/sql.ts";
import { Store } from "../src/persistence/store.ts";
import { type AlasioOptions, alasioServices } from "../src/alasio.ts";
import { TelegramApiError } from "../src/telegram/client.ts";
import { Outbox, type OutboxText } from "../src/telegram/outbox.ts";
import { botApiClient, paramsOf } from "./support/bot-api.ts";
import { newSchema, run, testDatabaseUrl, testPool, testStore } from "./support/store.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { noWorkflowHooks } from "./support/turns.ts";

/** A Telegram client for a reporter that only queues replies, never sending or editing itself. */
const unusedClient = recordingTelegram({
  sendMessage: () => assert.fail("no message is sent directly"),
  editMessageText: () => assert.fail("no message is edited"),
});

/** The status reporter of `store`, whose replies `enqueue` queues. */
function reporterFor(store: Store["Service"], enqueue: (text: OutboxText) => Effect.Effect<string, StoreError>): StatusReporter {
  return Effect.runSync(makeStatusReporter().pipe(Effect.provide(Layer.mergeAll(
    Layer.succeed(Store, store),
    unusedClient.layer,
    Layer.succeed(Outbox, Outbox.of({ enqueueText: enqueue, deliverDue: Effect.void })),
    noWorkflowHooks,
  ))));
}

/** The conversation of chat 123. */
const CONVERSATION = "telegram:123";

/** A store holding the conversation of chat 123, on Codex. */
async function codexConversation(): Promise<Store["Service"]> {
  const store = await testStore();
  await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "123", user: { id: 123 } })), CODEX_HARNESS));
  return store;
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
  const store = await codexConversation();
  const pendingResponseId = await run(store.createPendingResponse("123", "9"));
  await run(store.appendBlocksToPending(pendingResponseId, [{ type: "text", content: "Still investigating.", phase: "commentary" }]));
  await run(store.markPendingResponseComplete(pendingResponseId));
  const enqueued: OutboxText[] = [];
  const reporter = reporterFor(store, (item) => Effect.sync(() => {
    enqueued.push(item);
    return "outbox-1";
  }));

  await run(reporter.postResponse({ chatId: "123", response: "", pendingResponseId, status: null }));

  assert.deepEqual(enqueued, []);
  // Posted as it is, with nothing to deliver.
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
});

test("response recovery exposes only completed responses, their blocks in order", async () => {
  const store = await codexConversation();
  const pendingResponseId = await run(store.createPendingResponse("123", "9"));
  await run(store.appendBlocksToPending(pendingResponseId, [
    { type: "text", content: "Looking.", phase: "commentary" },
    { type: "tool", name: "Bash" },
  ]));
  await run(store.appendBlocksToPending(pendingResponseId, [{ type: "text", content: "The final answer.", phase: "final_answer" }]));

  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  await run(store.markPendingResponseComplete(pendingResponseId));
  const [completed] = await run(store.getCompletedResponsesPendingDelivery);
  assert.ok(completed);
  assert.deepEqual(completed.blocks.map((block) => block["content"] ?? block["name"]), ["Looking.", "Bash", "The final answer."]);
  assert.equal(finalResponseToMarkdown(completed.blocks), "The final answer.");
});

test("a new response to a message lets go of the one before it that was never delivered", async () => {
  const store = await codexConversation();
  const first = await run(store.createPendingResponse("123", "9"));
  await run(store.markPendingResponseComplete(first));
  const second = await run(store.createPendingResponse("123", "9"));
  await run(store.markPendingResponseComplete(second));
  assert.deepEqual((await run(store.getCompletedResponsesPendingDelivery)).map((response) => response.id), [second]);
});

test("completed response recovery enqueues one final answer exactly once", async () => {
  const store = await codexConversation();
  const pendingResponseId = await run(store.createPendingResponse("123", "9"));
  await run(store.appendBlocksToPending(pendingResponseId, [
    { type: "text", content: "Still working.", phase: "commentary" },
    { type: "text", content: "The final answer.", phase: "final_answer" },
  ]));
  await run(store.markPendingResponseComplete(pendingResponseId));
  const reporter = reporterFor(store, (args) => store.enqueueOutboxText(args));

  await run(reporter.flushCompletedResponses);
  await run(reporter.flushCompletedResponses);

  const due = await run(store.getDueOutbox(10));
  assert.equal(due.length, 1);
  assert.equal(due[0]?.text, "The final answer.");
  assert.equal(due[0]?.pending_response_id, pendingResponseId);
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  // A reply queued again for the same response is the one already queued.
  assert.equal(await run(store.enqueueOutboxText({ chatId: "123", text: "again", pendingResponseId })), due[0]?.id);
});

test("a reply waiting to be retried holds back the replies queued after it to its chat, and no other chat's", async () => {
  const store = await testStore();
  for (const chatId of ["1", "2"]) await run(store.setActiveHarness(await run(store.upsertConversation({ chatId, user: { id: Number(chatId) } })), CODEX_HARNESS));
  const first = await run(store.enqueueOutboxText({ chatId: "1", text: "first" }));
  await run(store.enqueueOutboxText({ chatId: "1", text: "second" }));
  await run(store.enqueueOutboxText({ chatId: "2", text: "elsewhere" }));
  const due = async () => (await run(store.getDueOutbox())).map((reply) => reply.text);
  assert.deepEqual(await due(), ["first", "elsewhere"]);
  await run(store.rescheduleOutbox(first, new Error("Bad Gateway"), 60_000));
  assert.deepEqual(await due(), ["elsewhere"]);
  await run(store.markOutboxSent(first));
  assert.deepEqual(await due(), ["second", "elsewhere"]);
});

test("prompt jobs and the outbox outlast the store they were queued through", async () => {
  const schema = newSchema();
  const first = await testStore({ schema });
  const conversationId = await run(first.upsertConversation({ chatId: "123", user: { id: 123 } }));
  await run(first.setActiveHarness(conversationId, CODEX_HARNESS));
  const job = await run(first.enqueuePromptJob({ conversationId, chatId: "123", messageId: "9", prompt: "hello" }));
  const claimed = await run(first.claimNextPromptJob(conversationId));
  assert.equal(claimed?.id, job.id);
  await run(first.enqueueOutboxText({ chatId: "123", text: "done", pendingResponseId: null }));

  const second = await testStore({ schema });
  await run(second.recoverPromptJobsAfterRestart);
  assert.equal((await run(second.claimNextPromptJob(conversationId)))?.prompt, "hello");
  assert.equal((await run(second.getDueOutbox(10)))[0]?.text, "done");
});

test("a prompt job is claimed by one worker however many ask at once", async () => {
  const store = await codexConversation();
  await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "1", prompt: "first" }));
  await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "2", prompt: "second", priority: 1 }));
  const claims = await Promise.all([1, 2, 3].map(() => run(store.claimNextPromptJob(CONVERSATION))));
  assert.deepEqual(claims.map((job) => job?.prompt ?? "none").sort(), ["first", "none", "second"]);
});

test("the store's schema is not one alasio's role finds first, so what the role makes unqualified goes where it always went", async () => {
  // alasio's role makes the store, as it does in alasio's Neon.
  await (await testPool()).query(`
    create role alasio login password 'alasio';
    do $$ begin execute format('grant create on database %I to alasio', current_database()); end $$;
  `);
  const url = new URL(await testDatabaseUrl());
  url.username = "alasio";
  url.password = "alasio";
  const pool = new pg.Pool({ connectionString: url.href, max: 1 });
  try {
    await Effect.runPromise(Effect.provide(Store, Store.layer({ pool })));
    const { rows } = await pool.query<{ schemas: string[] }>("select current_schemas(false)::text[] as schemas");
    assert.deepEqual(rows[0]?.schemas, ["public"]);
  } finally {
    await pool.end();
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
      pool: await testPool(),
      stateSchema: newSchema(),
      keepCodexLogin: false,
      hookPort: 0,
      warmLinkedSessions: false,
      defaultHarness: null,
      branch: null,
      branchForkKeyFile: null,
    };
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const store = yield* Store;
      yield* store.setActiveHarness(yield* store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
      const pendingResponseId = yield* store.createPendingResponse("123", "9");
      yield* store.appendBlocksToPending(pendingResponseId, [{ type: "text", content: "Delivered durably.", phase: "final_answer" }]);
      yield* store.markPendingResponseComplete(pendingResponseId);
      yield* (yield* Turns).flushCompletedResponses;
      assert.deepEqual(yield* store.getCompletedResponsesPendingDelivery, []);
      assert.equal(yield* store.getPendingOutboxCount, 1);
    }).pipe(Effect.provide(alasioServices(options)))));
  } finally {
    if (apiRoot === undefined) delete process.env["TELEGRAM_API_ROOT"];
    else process.env["TELEGRAM_API_ROOT"] = apiRoot;
    rmSync(root, { recursive: true, force: true });
  }
});

test("restart reconciliation preserves upstream-completed prompt jobs", async () => {
  const store = await codexConversation();
  const job = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "10", prompt: "finish" }));
  await run(store.claimNextPromptJob(CONVERSATION));
  await run(store.markPromptJobUpstreamStarted(job.id, "session", "turn"));
  await run(store.markPromptJobUpstreamCompleted(job.id, "session", "turn"));
  assert.deepEqual(await run(store.recoverPromptJobsAfterRestart), [CONVERSATION]);
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "completed");
});

test("restart reconciliation runs again only the prompts never sent to the agent", async () => {
  const store = await testStore();
  const jobs = new Map<string, string>();
  for (const [chatId, dispatched] of [["1", false], ["2", true]] as const) {
    const conversationId = await run(store.upsertConversation({ chatId, user: { id: Number(chatId) } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    const job = await run(store.enqueuePromptJob({ conversationId, chatId, messageId: "10", prompt: "do it" }));
    await run(store.claimNextPromptJob(conversationId));
    if (dispatched) await run(store.markPromptJobDispatched(job.id));
    jobs.set(chatId, job.id);
  }
  assert.deepEqual(await run(store.recoverPromptJobsAfterRestart), []);
  assert.equal((await run(store.getPromptJob(jobs.get("1") ?? "")))?.state, "pending");
  assert.equal((await run(store.getPromptJob(jobs.get("2") ?? "")))?.state, "interrupted");
});

test("self-restart recovery stages a distinct durable continuation", async () => {
  const store = await codexConversation();
  const original = await run(store.enqueuePromptJob({
    conversationId: CONVERSATION,
    chatId: "123",
    messageId: "3334",
    prompt: "Apply the change and restart.",
  }));
  await run(store.claimNextPromptJob(CONVERSATION));
  await run(store.markPromptJobUpstreamStarted(original.id, "session-1", "turn-1"));
  const partialResponseId = await run(store.createPendingResponse("123", "3334", "session-1"));
  await run(store.appendBlocksToPending(partialResponseId, [{ type: "text", content: "Restarting now.", phase: "commentary" }]));
  await run(store.upsertActiveTurn({
    conversationId: CONVERSATION,
    chatId: "123",
    messageId: "3334",
    sessionId: "session-1",
    harness: CODEX_HARNESS,
    pendingResponseId: partialResponseId,
    prompt: original.prompt,
  }));
  await run(store.recordRestartEvent({ cause: "self_induced", thread_key: CONVERSATION, channel: "123", thread_ts: "3334", session_id: "session-1" }));

  await run(store.recoverPromptJobsAfterRestart);
  await run(recoverInterruptedTurns(store));
  await run(recoverInterruptedTurns(store));

  assert.equal((await run(store.getPromptJob(original.id)))?.state, "interrupted");
  assert.deepEqual(await run(store.getActiveTurns), []);
  assert.equal(await run(store.getRestartEvent(CONVERSATION)), null);
  // The partial response was let go of: completed, it is still never delivered.
  await run(store.markPendingResponseComplete(partialResponseId));
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  const continuation = await run(store.claimNextPromptJob(CONVERSATION));
  assert.ok(continuation);
  assert.notEqual(continuation.message_id, "3334");
  assert.match(continuation.message_id, /^restart:3334:/);
  assert.match(continuation.prompt, /SYSTEM RESTART EVENT/);
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null, "staged twice, the continuation is queued once");
  assert.equal((await run(store.getMount(CONVERSATION))).sessionId, "session-1");

  const completedResponseId = await run(store.createPendingResponse("123", continuation.message_id, "session-1"));
  await run(store.appendBlocksToPending(completedResponseId, [
    { type: "text", content: "Recovered commentary.", phase: "commentary" },
    { type: "text", content: "Recovered final answer.", phase: "final_answer" },
  ]));
  await run(store.markPendingResponseComplete(completedResponseId));
  const reporter = reporterFor(store, (args) => store.enqueueOutboxText(args));
  await run(reporter.flushCompletedResponses);
  assert.deepEqual((await run(store.getDueOutbox(10))).map((item) => item.text), ["Recovered final answer."]);
});

test("stale turn completion cannot clear a replacement turn", async () => {
  const store = await codexConversation();
  await run(store.upsertActiveTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "1", harness: CODEX_HARNESS, pendingResponseId: "old" }));
  await run(store.upsertActiveTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "2", harness: CODEX_HARNESS, pendingResponseId: "new" }));
  await run(store.clearActiveTurn(CONVERSATION, "old"));
  assert.equal((await run(store.getActiveTurns))[0]?.pending_response_id, "new");
});

test("callback actions capture the mounted session generation", async () => {
  const store = await codexConversation();
  await run(store.setSessionId(CONVERSATION, "session-a"));
  const [id = ""] = await run(store.createCallbackActions(CONVERSATION, [{ kind: "goal:resume" }]));
  assert.equal((await run(store.consumeCallbackAction(id)))?.payload["expectedSessionId"], "session-a");
  assert.equal(await run(store.consumeCallbackAction(id)), null, "a button acts once");
});

test("what is kept only in flight is pruned once it has landed long enough ago; what is still in flight is not", async () => {
  const store = await codexConversation();
  const sent = await run(store.enqueueOutboxText({ chatId: "123", text: "delivered" }));
  await run(store.markOutboxSent(sent));
  await run(store.enqueueOutboxText({ chatId: "123", text: "waiting" }));
  await run(store.pruneTransient("1 hour"));
  assert.equal(await run(store.getPendingOutboxCount), 1);

  const [button = ""] = await run(store.createCallbackActions(CONVERSATION, [{ kind: "queue" }]));
  await run(store.pruneTransient(0));
  assert.deepEqual((await run(store.getDueOutbox())).map((reply) => reply.text), ["waiting"]);
  assert.notEqual(await run(store.consumeCallbackAction(button)), null, "a button not yet pressed acts however old it is");
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
