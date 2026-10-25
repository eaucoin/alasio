import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import test from "node:test";

import { Array as Arr, Deferred, Effect, Exit, Fiber, Layer, Logger as EffectLogger, Scope } from "effect";
import pg from "pg";
import { makeAppServerNotifications } from "../src/codex/app-server/notification-queue.ts";
import { AppServerRequestTimeout } from "../src/codex/app-server/rpc-client.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { makeAppServerThreads } from "../src/codex/app-server/thread-client.ts";
import { finalResponseToMarkdown } from "../src/codex/response-markdown.ts";
import { makeStatusReporter, type StatusReporter } from "../src/codex/status-reporter.ts";
import { Turns } from "../src/codex/turns.ts";
import { CODEX_LOGIN_ROLE, HARNESS_ROLE } from "../neon/control/secrets.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../src/codex/rollouts/store.ts";
import { EnvironmentEvents } from "../src/events/consume.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import type { StoreError } from "../src/persistence/sql.ts";
import { LOST_PREFIX, type LostRecovery, Store } from "../src/persistence/store.ts";
import { type AlasioOptions, alasioServices } from "../src/alasio.ts";
import { TelegramApiError } from "../src/telegram/client.ts";
import { Outbox, type OutboxText } from "../src/telegram/outbox.ts";
import { botApiClient, paramsOf } from "./support/bot-api.ts";
import { newSchema, run, testDatabaseUrl, testPool, testStore, writerOf } from "./support/store.ts";
import { stubUpdates } from "./support/stub-updates.ts";
import { recordingTelegram } from "./support/telegram-calls.ts";
import { CODEX_LOGIN_URL, pooledDatabase } from "./support/pools.ts";
import { memoryTopic } from "./support/topics.ts";
import { withServices } from "./support/turns.ts";
import { eventually } from "./support/wait.ts";

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
    Layer.succeed(Outbox, Outbox.of({ enqueueText: enqueue, enqueuePanel: () => Effect.die("no panel is queued"), enqueueNotice: () => Effect.die("no notice is queued"), enqueueStatus: () => Effect.succeed("status-1"), enqueueEdit: () => Effect.succeed("edit-1"), deliverDue: Effect.void, deliverChat: () => Effect.void })),
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
      stop: Effect.void,
      whenGone: Effect.succeed(Effect.never),
      running: Effect.succeed(1),
    }, notifications, (work) => Effect.forkDetach(work));
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
  const pendingResponseId = await run((await writerOf(store, CONVERSATION)).createPendingResponse("123", "9"));
  await run((await writerOf(store, CONVERSATION)).appendBlocksToPending(pendingResponseId, [{ type: "text", content: "Still investigating.", phase: "commentary" }]));
  await run((await writerOf(store, CONVERSATION)).markPendingResponseComplete(pendingResponseId));
  const enqueued: OutboxText[] = [];
  const reporter = reporterFor(store, (item) => Effect.sync(() => {
    enqueued.push(item);
    return "outbox-1";
  }));

  await run(reporter.postResponse({ conversationId: CONVERSATION, chatId: "123", response: "", pendingResponseId, status: null }));

  assert.deepEqual(enqueued, []);
  // Posted as it is, with nothing to deliver.
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
});

test("response recovery exposes only completed responses, their blocks in order", async () => {
  const store = await codexConversation();
  const pendingResponseId = await run((await writerOf(store, CONVERSATION)).createPendingResponse("123", "9"));
  await run((await writerOf(store, CONVERSATION)).appendBlocksToPending(pendingResponseId, [
    { type: "text", content: "Looking.", phase: "commentary" },
    { type: "tool", name: "Bash" },
  ]));
  await run((await writerOf(store, CONVERSATION)).appendBlocksToPending(pendingResponseId, [{ type: "text", content: "The final answer.", phase: "final_answer" }]));

  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  await run((await writerOf(store, CONVERSATION)).markPendingResponseComplete(pendingResponseId));
  const [completed] = await run(store.getCompletedResponsesPendingDelivery);
  assert.ok(completed);
  assert.deepEqual(completed.blocks.map((block) => block["content"] ?? block["name"]), ["Looking.", "Bash", "The final answer."]);
  assert.equal(finalResponseToMarkdown(completed.blocks), "The final answer.");
});

test("a new response to a message lets go of the one before it that was never delivered", async () => {
  const store = await codexConversation();
  const first = await run((await writerOf(store, CONVERSATION)).createPendingResponse("123", "9"));
  await run((await writerOf(store, CONVERSATION)).markPendingResponseComplete(first));
  const second = await run((await writerOf(store, CONVERSATION)).createPendingResponse("123", "9"));
  await run((await writerOf(store, CONVERSATION)).markPendingResponseComplete(second));
  assert.deepEqual((await run(store.getCompletedResponsesPendingDelivery)).map((response) => response.id), [second]);
});

test("completed response recovery enqueues one final answer exactly once", async () => {
  const store = await codexConversation();
  const pendingResponseId = await run((await writerOf(store, CONVERSATION)).createPendingResponse("123", "9"));
  await run((await writerOf(store, CONVERSATION)).appendBlocksToPending(pendingResponseId, [
    { type: "text", content: "Still working.", phase: "commentary" },
    { type: "text", content: "The final answer.", phase: "final_answer" },
  ]));
  await run((await writerOf(store, CONVERSATION)).markPendingResponseComplete(pendingResponseId));
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
  const deferred = await run(store.claimOutboxOf("1", "deferring", "1 minute"));
  assert.equal(deferred?.entry.id, first);
  assert.equal(await run(store.rescheduleOutbox(deferred, new Error("Bad Gateway"), 300)), true);
  assert.deepEqual(await due(), ["elsewhere"]);
  await sleep(400);
  assert.deepEqual(await due(), ["first", "elsewhere"]);
  const sent = await run(store.claimOutboxOf("1", "sending", "1 minute"));
  assert.equal(sent?.entry.id, first);
  assert.equal(await run(store.markOutboxSent(sent)), true);
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
  assert.equal(await run(second.recoverLeftPromptJobs(conversationId)), false);
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

test("a prompt queued as its conversation's worker finds no other is still run", async () => {
  const store = await codexConversation();
  // The worker's first claim, which finds nothing, held as it returns until the test lets it go.
  const foundNone = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let claims = 0;
  const held: Store["Service"] = {
    ...store,
    claimNextPromptJob: (conversationId) =>
      store.claimNextPromptJob(conversationId).pipe(Effect.tap((job) => {
        claims += 1;
        return claims === 1 && job === null ? Effect.andThen(Effect.sync(() => foundNone.resolve()), Effect.promise(() => release.promise)) : Effect.void;
      })),
  };
  await withServices({ store: held }, async (alasio) => {
    const schedule = () => alasio.runPromise(Effect.flatMap(Turns, (turns) => turns.schedule(CONVERSATION)));
    await schedule();
    await foundNone.promise;
    // Queued and scheduled while the worker is ending, having found no job.
    const job = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "1", prompt: "queued meanwhile" }));
    await schedule();
    release.resolve();
    await eventually("the job to be claimed", async () => ((await run(store.getPromptJob(job.id)))?.state === "pending" ? undefined : true));
  });
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
    const database = await pooledDatabase();
    const options: AlasioOptions = {
      telegramBotToken: "test-token",
      allowedUserIds: "",
      workingDirectory: root,
      workspaceRoot: root,
      stateDir: join(root, ".alasio"),
      pool: database.pool,
      stateSchema: newSchema(),
      keepCodexLogin: false,
      defaultHarness: null,
      branch: null,
      branchForkKeyFile: null,
      // A gateway that refuses every connection: nothing here binds a harness.
      sessionsGateway: { url: "http://127.0.0.1:9", environment: "main", callerKey: Buffer.from("caller-key") },
      folders: false,
      codexLoginUrl: CODEX_LOGIN_URL,
      pod: "alasio-composition",
      inKubernetes: false,
      harnessRole: HARNESS_ROLE,
      codexLoginRole: CODEX_LOGIN_ROLE,
      sessionStore: new NeonSessionStore(database.pool),
      rolloutStores: { codex: new NeonRolloutStore(database.pool), sessionFs: new NeonRolloutStore(database.pool, { schema: SESSION_FS_SCHEMA }) },
      events: Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({ join: memoryTopic("events.main").join })),
      // No update comes: the stand-in is refused as Telegram is.
      updates: stubUpdates("http://127.0.0.1:9"),
    };
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const store = yield* Store;
      yield* store.setActiveHarness(yield* store.upsertConversation({ chatId: "123", user: { id: 123 } }), CODEX_HARNESS);
      const writer = yield* Effect.promise(() => writerOf(store, CONVERSATION));
      const pendingResponseId = yield* writer.createPendingResponse("123", "9");
      yield* writer.appendBlocksToPending(pendingResponseId, [{ type: "text", content: "Delivered durably.", phase: "final_answer" }]);
      yield* writer.markPendingResponseComplete(pendingResponseId);
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

test("a conversation's recovery of what its last worker left completes a prompt job the agent finished", async () => {
  const store = await codexConversation();
  const job = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "10", prompt: "finish" }));
  await run(store.claimNextPromptJob(CONVERSATION));
  await run(store.markPromptJobUpstreamStarted(job.id, "session", "turn"));
  await run(store.markPromptJobUpstreamCompleted(job.id, "session", "turn"));
  assert.equal(await run(store.recoverLeftPromptJobs(CONVERSATION)), true);
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "completed");
});

test("a conversation's recovery of what its last worker left runs again only the prompts never sent to the agent, and only the conversation's", async () => {
  const store = await testStore();
  const jobs = new Map<string, string>();
  const conversations = new Map<string, string>();
  for (const [chatId, dispatched] of [["1", false], ["2", true]] as const) {
    const conversationId = await run(store.upsertConversation({ chatId, user: { id: Number(chatId) } }));
    await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
    const job = await run(store.enqueuePromptJob({ conversationId, chatId, messageId: "10", prompt: "do it" }));
    await run(store.claimNextPromptJob(conversationId));
    if (dispatched) {
      // Sent by its turn, as its pod records it.
      const epoch = await run(store.claimTurn({ conversationId, chatId, messageId: "10", harness: CODEX_HARNESS, prompt: "do it" }));
      assert.equal(await run(store.markPromptJobDispatched(job.id, epoch)), true);
    }
    jobs.set(chatId, job.id);
    conversations.set(chatId, conversationId);
  }
  assert.equal(await run(store.recoverLeftPromptJobs(conversations.get("1") ?? "")), false);
  assert.deepEqual([(await run(store.getPromptJob(jobs.get("1") ?? "")))?.state, (await run(store.getPromptJob(jobs.get("2") ?? "")))?.state], ["pending", "running"]);
  assert.equal(await run(store.recoverLeftPromptJobs(conversations.get("2") ?? "")), false);
  assert.equal((await run(store.getPromptJob(jobs.get("2") ?? "")))?.state, "interrupted");
});

/** A job of the conversation running, by its turn, claimed, its prompt not yet sent: the job, and the turn's epoch. */
async function unsentJob(store: Store["Service"], conversationId: string) {
  const job = await run(store.enqueuePromptJob({ conversationId, chatId: "123", messageId: "21", prompt: "do it" }));
  await run(store.claimNextPromptJob(conversationId));
  const epoch = await run(store.claimTurn({ conversationId, chatId: "123", messageId: "21", harness: CODEX_HARNESS, prompt: "do it" }));
  return { job, epoch };
}

/** The lost turn's recovery of `epoch`, as alasio attaching again, or a binding's loss heard, asks it. */
const lostRecovery = (conversationId: string, epoch: number): LostRecovery => ({
  conversationId,
  epoch,
  binding: null,
  messageId: `${LOST_PREFIX}${epoch}`,
  prompt: () => "lost",
  said: (outcome) => outcome,
});

test("a turn lost before its job's prompt was sent runs that job again as itself, never also as a prompt of its own, and its pod can then send the prompt no more", async () => {
  const store = await codexConversation();
  const { job, epoch } = await unsentJob(store, CONVERSATION);
  // Its conversation's alasio, taking it over, settles its jobs: this one, its turn left, its pod perhaps holding it yet, is left be.
  assert.equal(await run(store.recoverLeftPromptJobs(CONVERSATION)), false);
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "running");
  const recovered = await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)));
  assert.equal(recovered.outcome, "requeued");
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "pending");
  // Its pod, late, records the prompt sent no more, and sends nothing.
  assert.equal(await run(store.markPromptJobDispatched(job.id, epoch)), false);
  assert.equal((await run(store.getPromptJob(job.id)))?.upstream_dispatched_at, null);
  // What runs next is the job itself, and nothing after it: no prompt of its own for the turn lost.
  assert.equal((await run(store.claimNextPromptJob(CONVERSATION)))?.id, job.id);
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null);
});

test("a turn whose job's prompt its pod sent first is run again as a prompt of its own, its job ended with it, never failed nor run again: completed where its response was, interrupted otherwise; no other job of the conversation's", async () => {
  const sent = async (store: Store["Service"]) => {
    const left = await unsentJob(store, CONVERSATION);
    assert.equal(await run(store.markPromptJobDispatched(left.job.id, left.epoch)), true);
    return left;
  };
  {
    const store = await codexConversation();
    const { job, epoch } = await sent(store);
    assert.equal((await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)))).outcome, "staged");
    assert.equal((await run(store.getPromptJob(job.id)))?.state, "interrupted");
    // Its worker, finding its harness gone, fails it no more.
    assert.equal(await run(store.failPromptJob(job.id, "lost")), false);
  }
  {
    // Its response complete, delivered rather than run again.
    const store = await codexConversation();
    const { job, epoch } = await sent(store);
    const writer = store.turnPersistence(CONVERSATION, epoch);
    const response = await run(writer.createPendingResponse("123", "21", null));
    await run(writer.updateActiveTurnPendingResponseId(response));
    await run(writer.markPendingResponseComplete(response));
    assert.equal((await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)))).outcome, "completed");
    assert.equal((await run(store.getPromptJob(job.id)))?.state, "completed");
  }
  {
    // Another job running: of another message, claimed before the turn; and one of its message claimed after it began, in a millisecond of its own.
    const schema = newSchema();
    const store = await testStore({ schema });
    await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "123", user: { id: 123 } })), CODEX_HARNESS));
    const other = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "30", prompt: "another" }));
    await run(store.claimNextPromptJob(CONVERSATION));
    const epoch = await run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "21", harness: CODEX_HARNESS, prompt: "do it" }));
    const later = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "21", prompt: "do it" }));
    assert.equal((await run(store.claimNextPromptJob(CONVERSATION)))?.id, later.id);
    await (await testPool()).query(`update ${schema}.prompt_jobs j set started_at = date_trunc('milliseconds', t.started_at) + interval '1 millisecond' from ${schema}.turns t where t.id = j.conversation_id and j.id = $1`, [later.id]);
    await run(store.markPromptJobUpstreamStarted(later.id, "session-1", "turn-1"));
    assert.equal((await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)))).outcome, "staged");
    assert.deepEqual([(await run(store.getPromptJob(other.id)))?.state, (await run(store.getPromptJob(later.id)))?.state], ["running", "running"]);
  }
});

test("a turn lost as its host ran out of its memory, not run again, ends its job with it, its prompt never sent: interrupted, never failed nor run again", async () => {
  const store = await codexConversation();
  const { job, epoch } = await unsentJob(store, CONVERSATION);
  assert.equal((await run(store.stageLostRecovery({ ...lostRecovery(CONVERSATION, epoch), rerun: false }))).outcome, "ended");
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "interrupted");
  assert.equal(await run(store.failPromptJob(job.id, "out of memory")), false);
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null);
});

test("a binding's loss heard for a turn whose job's prompt was never sent, as an alasio holding the conversation's claim attaches to it again or drives it, runs the job again once, as itself: what that alasio settles of the job after does nothing", async () => {
  const store = await codexConversation();
  const { job, epoch } = await unsentJob(store, CONVERSATION);
  await run(store.claimWorker(CONVERSATION, "pod-a/x", 60_000));
  const turn = await run(store.getActiveTurn(CONVERSATION));
  assert.equal((await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)))).outcome, "requeued");
  // Its attaching again, given up, settles its turn's job left; its driving ends, failed or settled: none touches the job run again.
  await run(store.recoverLeftPromptJobOf(CONVERSATION, turn!.started_at));
  assert.equal(await run(store.failPromptJob(job.id, "lost")), false);
  assert.equal(await run(store.settleRunningPromptJob(job.id, "cancelled")), false);
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "pending");
  // Nor, once the conversation's worker claims it anew, does its attaching again, ending late.
  assert.equal((await run(store.claimNextPromptJob(CONVERSATION)))?.id, job.id);
  await run(store.recoverLeftPromptJobOf(CONVERSATION, turn!.started_at));
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "running");
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null);
});

test("a pod's record of its job's prompt sent, waiting on the job's row as the job is run again and claimed anew, records it for the turn it was taken from no more", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "123", user: { id: 123 } })), CODEX_HARNESS));
  const { job, epoch } = await unsentJob(store, CONVERSATION);
  const pool = await testPool();
  const anew = await pool.connect();
  try {
    // The job run again and claimed anew, in a transaction the pod's record waits on, its turn read as it was.
    await anew.query("begin");
    await anew.query(`update ${schema}.prompt_jobs set started_at = clock_timestamp(), attempts = attempts + 1 where id = $1`, [job.id]);
    const recorded = run(store.markPromptJobDispatched(job.id, epoch));
    await eventually("the record to wait on the job's row", async () =>
      (await pool.query(`select 1 from pg_stat_activity where pid <> pg_backend_pid() and wait_event_type = 'Lock' and query like '%upstream_dispatched_at = coalesce%'`)).rows.length > 0 ? true : undefined);
    await anew.query("commit");
    assert.equal(await recorded, false);
  } finally {
    // Its transaction, where the test failed within it, ended with the connection, not left to the pool.
    anew.release(true);
  }
  assert.equal((await run(store.getPromptJob(job.id)))?.upstream_dispatched_at, null);
});

test("a turn attached to again, or lost, settles as it ends the job left of it, claimed in the millisecond it started in", async () => {
  for (const ended of ["attached to again", "lost"] as const) {
    const schema = newSchema();
    const store = await testStore({ schema });
    await run(store.setActiveHarness(await run(store.upsertConversation({ chatId: "123", user: { id: 123 } })), CODEX_HARNESS));
    const { job, epoch } = await unsentJob(store, CONVERSATION);
    // Its job claimed, then its turn started, in one millisecond, which the turn's start is read to.
    const pool = await testPool();
    await pool.query(`update ${schema}.turns set started_at = date_trunc('milliseconds', started_at) + interval '900 microseconds' where id = $1`, [CONVERSATION]);
    await pool.query(`update ${schema}.prompt_jobs j set started_at = date_trunc('milliseconds', t.started_at) + interval '400 microseconds' from ${schema}.turns t where t.id = j.conversation_id and j.id = $1`, [job.id]);
    const turn = await run(store.getActiveTurn(CONVERSATION));
    assert.equal(await run(store.markPromptJobDispatched(job.id, epoch)), true);
    if (ended === "lost") {
      assert.equal((await run(store.stageLostRecovery(lostRecovery(CONVERSATION, epoch)))).outcome, "staged");
    } else {
      assert.equal(await run(store.endTurn(CONVERSATION, epoch, "interrupted")), true);
      await run(store.recoverLeftPromptJobOf(CONVERSATION, turn!.started_at));
    }
    assert.equal((await run(store.getPromptJob(job.id)))?.state, "interrupted", ended);
  }
});

test("a turn ended as one not to be run again, as its pod ran out of its memory, has its job, never sent, ended with it, not run again as what its worker left is settled", async () => {
  const store = await codexConversation();
  const job = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "4441", prompt: "Hold it all." }));
  assert.equal((await run(store.claimNextPromptJob(CONVERSATION)))?.id, job.id);
  await run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "4441", sessionId: "session-1", harness: CODEX_HARNESS, prompt: "Hold it all." }));
  const ended = await run(store.stageLostRecovery({ conversationId: CONVERSATION, epoch: null, binding: null, messageId: "lost:oom", prompt: () => "again", rerun: false, said: (outcome) => `said ${outcome}` }));
  assert.equal(ended.outcome, "ended");
  await run(store.recoverLeftPromptJobs(CONVERSATION));
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null, "the ended turn's job was run again");
  assert.equal((await run(store.getPromptJob(job.id)))?.state, "interrupted");
});

test("a turn whose harness instance was lost is run again once, as a prompt of its own; one whose response was complete is delivered instead; a recovery lost again is let go of", async () => {
  const store = await codexConversation();
  const original = await run(store.enqueuePromptJob({ conversationId: CONVERSATION, chatId: "123", messageId: "3334", prompt: "Apply the change." }));
  await run(store.claimNextPromptJob(CONVERSATION));
  await run(store.markPromptJobUpstreamStarted(original.id, "session-1", "turn-1"));
  const claim = (messageId: string) => run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId, sessionId: "session-1", harness: CODEX_HARNESS, prompt: "go" }));
  const epoch = await claim("3334");
  const writer = store.turnPersistence(CONVERSATION, epoch);
  const partial = await run(writer.createPendingResponse("123", "3334", "session-1"));
  await run(writer.updateActiveTurnPendingResponseId(partial));
  await run(writer.appendBlocksToPending(partial, [{ type: "text", content: "Applying.", phase: "commentary" }]));
  await run(store.setTurnStatusMessage(CONVERSATION, epoch, await run(store.enqueueOutboxText({ chatId: "123", text: "Codex is working" }))));
  // Its status message delivered, so an edit of it is the chat's next due.
  const shown = await run(store.claimOutboxOf("123", "delivering", "1 minute"));
  assert.ok(shown);
  await run(store.markOutboxSent(shown));
  const said: string[] = [];
  const lost = (messageId: string, of: number | null = epoch, binding: string | null = null) =>
    run(store.stageLostRecovery({ conversationId: CONVERSATION, epoch: of, binding, messageId, prompt: (turn) => `run ${turn.thread_ts} again`, said: (outcome) => (said.push(outcome), `said ${outcome}`) }));

  // Of another epoch, none, nor of a binding it does not run on, as a loss heard after the conversation was bound anew is;
  // of its own, once, however often it is staged, as by its binding's loss and its reattach both.
  assert.deepEqual(await lost("lost:binding", epoch + 1), { outcome: "none" });
  assert.equal(await run(store.setTurnBinding(CONVERSATION, epoch, "binding-now")), true);
  assert.deepEqual(await lost("lost:binding", null, "binding-before"), { outcome: "none" });
  assert.equal((await lost("lost:binding")).outcome, "staged");
  assert.deepEqual(await lost(`lost:${epoch}`), { outcome: "none" });
  assert.deepEqual(await lost("lost:binding", null), { outcome: "none" });
  assert.deepEqual(await run(store.getActiveTurns), []);
  assert.equal((await run(store.getTurn(CONVERSATION)))?.outcome, "stopped");
  // Its partial response let go of: completed by its pod after, it is still never delivered.
  await run(writer.markPendingResponseComplete(partial));
  assert.deepEqual(await run(store.getCompletedResponsesPendingDelivery), []);
  const recovery = await run(store.claimNextPromptJob(CONVERSATION));
  assert.deepEqual([recovery?.message_id, recovery?.prompt, recovery?.harness], ["lost:binding", "run 3334 again", CODEX_HARNESS]);
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null, "staged twice, the recovery is queued once");
  // Its status message says so.
  assert.deepEqual(said, ["staged"]);
  assert.ok((await run(store.getDueOutbox(10))).some((item) => item.text === "said staged"));

  // The recovery's own turn lost too: let go of, not run again; it has no status message, so the chat is told by one of its own.
  await claim("lost:binding");
  const pending = await run(store.getPendingOutboxCount);
  assert.equal((await lost("lost:again", null)).outcome, "given-up");
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null);
  // Its job, which its worker ran and its pod never sent, not put back to run either as what its worker left is settled,
  // as attaching to its turn again settles it.
  await run(store.recoverLeftPromptJobs(CONVERSATION));
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null, "the given-up recovery's job was run again");
  assert.equal((await run(store.getPromptJob(recovery?.id ?? "")))?.state, "interrupted");
  assert.deepEqual(said, ["staged", "given-up"]);
  assert.equal(await run(store.getPendingOutboxCount), pending + 1);

  // A turn whose pod completed its response before it went: delivered, not run again.
  const finished = await claim("3335");
  const done = store.turnPersistence(CONVERSATION, finished);
  const response = await run(done.createPendingResponse("123", "3335", "session-1"));
  await run(done.updateActiveTurnPendingResponseId(response));
  await run(done.appendBlocksToPending(response, [{ type: "text", content: "Applied.", phase: "final_answer" }]));
  await run(done.markPendingResponseComplete(response));
  const told = await run(store.getPendingOutboxCount);
  assert.equal((await lost("lost:other", null)).outcome, "completed");
  // Its reply tells of it: nothing more is said.
  assert.equal(await run(store.getPendingOutboxCount), told);
  assert.equal((await run(store.getTurn(CONVERSATION)))?.outcome, "completed");
  assert.equal(await run(store.claimNextPromptJob(CONVERSATION)), null);
  assert.deepEqual((await run(store.getCompletedResponsesPendingDelivery)).map(({ id }) => id), [response]);
});

test("a turn its harness starts on its own runs on the binding its pod's host says is its own, and one alasio claims on none until alasio drives it on one", async () => {
  const store = await codexConversation();
  const claim = (messageId: string, binding?: string) => run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId, harness: CODEX_HARNESS, binding }));
  const binding = async () => (await run(store.getTurn(CONVERSATION)))?.binding;
  const driven = await claim("1");
  assert.equal(await binding(), null);
  assert.equal(await run(store.setTurnBinding(CONVERSATION, driven, "binding-a")), true);
  assert.equal(await binding(), "binding-a");
  await run(store.endTurn(CONVERSATION, driven, "completed"));
  await claim("own", "binding-before");
  assert.equal(await binding(), "binding-before");
  await claim("2");
  assert.equal(await binding(), null);
});

test("stale turn completion cannot clear a replacement turn", async () => {
  const store = await codexConversation();
  const old = await run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "1", harness: CODEX_HARNESS, pendingResponseId: "old" }));
  await run(store.claimTurn({ conversationId: CONVERSATION, chatId: "123", messageId: "2", harness: CODEX_HARNESS, pendingResponseId: "new" }));
  assert.equal(await run(store.endTurn(CONVERSATION, old, "completed")), false);
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
  await run(store.enqueueOutboxText({ chatId: "123", text: "delivered" }));
  const claim = await run(store.claimOutboxOf("123", "delivering", "1 minute"));
  assert.ok(claim);
  await run(store.markOutboxSent(claim));
  await run(store.enqueueOutboxText({ chatId: "123", text: "waiting" }));
  await run(store.pruneTransient("1 hour"));
  assert.equal(await run(store.getPendingOutboxCount), 1);

  const [button = ""] = await run(store.createCallbackActions(CONVERSATION, [{ kind: "queue" }]));
  await run(store.pruneTransient(0));
  assert.deepEqual((await run(store.getDueOutbox())).map((reply) => reply.text), ["waiting"]);
  assert.notEqual(await run(store.consumeCallbackAction(button)), null, "a button not yet pressed acts however old it is");
});

test("the store is pruned once a day by whichever alasio looks when it is due, so an alasio that starts often prunes no more often", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const other = await testStore({ schema });
  assert.equal(await run(store.pruneTransientIfDue("7 days", "1 day")), true, "never pruned: due");
  assert.equal(await run(other.pruneTransientIfDue("7 days", "1 day")), false, "pruned within the day, by another");
  assert.equal(await run(store.pruneTransientIfDue("7 days", "1 day")), false);
  await (await testPool()).query(`update ${schema}.bot_state set updated_at = now() - interval '25 hours' where key = 'pruned'`);
  assert.equal(await run(other.pruneTransientIfDue("7 days", "1 day")), true, "due again a day after");
  // Of alasios that look at once, as a day comes due, one alone prunes.
  await (await testPool()).query(`update ${schema}.bot_state set updated_at = now() - interval '25 hours' where key = 'pruned'`);
  const stores = await Promise.all(Array.from({ length: 6 }, () => testStore({ schema })));
  const pruned = await Promise.all(stores.map((each) => run(each.pruneTransientIfDue("7 days", "1 day"))));
  assert.equal(pruned.filter(Boolean).length, 1, String(pruned));
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
