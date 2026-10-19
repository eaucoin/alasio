import assert from "node:assert/strict";
import { test } from "node:test";

import type { FileChangeItem as SdkFileChangeItem } from "@openai/codex-sdk";
import { Array as Arr, Deferred, Effect, Exit, Fiber, Logger, Option, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import type { v2 } from "../.types/codex/index.js";
import { makeAppServer } from "../src/codex/app-server/client.ts";
import { type AppServerNotifications, makeAppServerNotifications } from "../src/codex/app-server/notification-queue.ts";
import {
  type AppServerEvent,
  type AppServerMethod,
  type AppServerNotification,
  type AppServerParams,
  type AppServerResult,
  getNotificationTurnId,
  mapNotificationToSdkEvent,
  notificationMatchesTurn,
} from "../src/codex/app-server/protocol.ts";
import { AppServerRequestFailed, type AppServerRequestError, AppServerRequestTimeout, type AppServerRpc } from "../src/codex/app-server/rpc-client.ts";
import { type AppServerApart, makeAppServerThreads } from "../src/codex/app-server/thread-client.ts";
import { mapItemToBlocks, responseProjection } from "../src/codex/event-projection.ts";
import type { CodexThreadConfig } from "../src/codex/thread-config.ts";
import { appServerProcess } from "./support/app-server-process.ts";
import { agentMessage, codexModel, codexThread, codexTurn } from "./support/codex-protocol.ts";

/** An active goal as Codex reports it. */
function activeGoal(threadId: string, objective: string): v2.ThreadGoal {
  return { threadId, objective, status: "active", tokenBudget: null, tokensUsed: 0, timeUsedSeconds: 0, createdAt: 0, updatedAt: 0 };
}

/** Codex's answer to a thread's resume (or, leaving out the turn pages, its start). */
function threadLoaded(loaded: v2.Thread): v2.ThreadResumeResponse {
  return {
    thread: loaded,
    model: "gpt-5.6-sol",
    modelProvider: "openai",
    serviceTier: null,
    cwd: loaded.cwd,
    runtimeWorkspaceRoots: [],
    instructionSources: [],
    approvalPolicy: "never",
    approvalsReviewer: "user",
    sandbox: { type: "dangerFullAccess" },
    activePermissionProfile: null,
    reasoningEffort: "high",
    multiAgentMode: "explicitRequestOnly",
    initialTurnsPage: null,
    turnsBackwardsCursor: null,
    itemsBackwardsCursor: null,
  };
}

/** Token usage as Codex reports it, all zero. */
function noTokenUsage(): v2.ThreadTokenUsage {
  const none = { totalTokens: 0, inputTokens: 0, cachedInputTokens: 0, cacheWriteInputTokens: 0, outputTokens: 0, reasoningOutputTokens: 0 };
  return { total: none, last: none, modelContextWindow: null };
}

/** How a test's app-server answers each request it expects. */
type AppServerAnswers = {
  readonly [M in AppServerMethod]?: (params: AppServerParams<M>) => Effect.Effect<AppServerResult<M>, AppServerRequestError>;
};

/** An app-server that is already running and answers requests with `answers`; any other request fails the test. */
/** What the threads' work runs apart, here detached. */
const apart: AppServerApart = (work) => Effect.forkDetach(work);

function answeringRpc(answers: AppServerAnswers): Pick<AppServerRpc, "start" | "request" | "stop" | "whenGone" | "running"> {
  return {
    start: () => Effect.void,
    stop: Effect.void,
    request: (method, params) => {
      const answer = answers[method];
      return answer ? answer(params) : Effect.sync(() => assert.fail(`unexpected app-server request ${method}`));
    },
    whenGone: Effect.succeed(Effect.never),
    running: Effect.succeed(1),
  };
}

/** Runs `body` with an app-server's notification routing, made for it, and its log lines dropped. */
function withNotifications<A, E>(body: (notifications: AppServerNotifications) => Effect.Effect<A, E>): Promise<A> {
  return Effect.runPromise(Effect.scoped(Effect.flatMap(makeAppServerNotifications, body)).pipe(Effect.provide(Logger.layer([]))));
}

test("app-server protocol reads turn identity from direct and nested notification shapes", () => {
  assert.equal(getNotificationTurnId({ method: "turn/diff/updated", params: { threadId: "thread-1", turnId: "turn-direct", diff: "" } }), "turn-direct");
  assert.equal(getNotificationTurnId({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("turn-nested") } }), "turn-nested");
  assert.equal(getNotificationTurnId({ method: "thread/realtime/itemAdded", params: { threadId: "thread-1", item: { turnId: "turn-item" } } }), "turn-item");
});

test("app-server protocol rejects stale completed-turn notifications for the active stream", () => {
  const staleCompletion: AppServerNotification = {
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("old-turn", { status: "completed" }),
    },
  };

  assert.equal(notificationMatchesTurn(staleCompletion, "new-turn"), false);
  assert.equal(notificationMatchesTurn(staleCompletion, "old-turn"), true);
});

test("app-server protocol preserves the upstream agent message phase", () => {
  const event = mapNotificationToSdkEvent({
    method: "item/completed",
    params: {
      threadId: "thread-1",
      turnId: "turn-1",
      item: agentMessage("answer-1", "The final answer.", "final_answer"),
      completedAtMs: 0,
    },
  });

  const item = event?.type === "item.completed" ? event.item : null;
  assert.equal(item?.type, "agent_message");
  assert.equal(item.phase, "final_answer");
});

test("app-server notification queue does not forget the active turn when an old completion arrives", () =>
  withNotifications((queue) => Effect.gen(function*() {
    yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("new-turn") } });
    yield* queue.observe({ method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("old-turn", { status: "completed" }) } });

    assert.equal(yield* queue.currentTurnId("thread-1"), "new-turn");
  })));

test("app-server notification queue remembers goal-created turn ids", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const waiting = yield* Effect.forkChild(queue.waitForTurnId("thread-1", 100));
    yield* Effect.yieldNow;
    yield* queue.observe({
      method: "thread/goal/updated",
      params: { threadId: "thread-1", turnId: "goal-turn", goal: activeGoal("thread-1", "Keep working") },
    });

    assert.equal(yield* Fiber.join(waiting), "goal-turn");
    assert.equal(yield* queue.currentTurnId("thread-1"), "goal-turn");
    assert.equal((yield* queue.nextForThread("thread-1")).method, "thread/goal/updated");
  })));

test("app-server goal handoff after completion starts a distinct logical turn", () =>
  withNotifications((queue) => Effect.gen(function*() {
    yield* queue.rememberTurn("thread-1", "completed-turn");
    yield* queue.observe({ method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("completed-turn", { status: "completed" }) } });
    yield* queue.observe({
      method: "thread/goal/updated",
      params: { threadId: "thread-1", turnId: "goal-turn", goal: activeGoal("thread-1", "Keep working") },
    });

    assert.equal(yield* queue.currentTurnId("thread-1"), "goal-turn");
    assert.deepEqual([...yield* queue.turnAliases("thread-1")], ["goal-turn"]);
  })));

/**
 * A turn stopped as it waits for its start, which `answer` answers, once its start is asked
 * (`asked`) and the stop let through (`stop`), `settle` run then, the turns of `others`
 * running on other threads meanwhile: whether the app-server was stopped once the stop is
 * done, and what it was asked; what was logged put in `logged`.
 */
const stoppedAsItStarts = (
  answer: (asked: Deferred.Deferred<void>, stop: Deferred.Deferred<void>) => AppServerAnswers,
  settle: Effect.Effect<void>,
  { others = [], logged = [] }: { readonly others?: readonly { readonly threadId: string; readonly turnId: string }[]; readonly logged?: string[] } = {},
) =>
  withNotifications((queue) => Effect.gen(function*() {
    for (const { threadId, turnId } of others) yield* queue.observe({ method: "turn/started", params: { threadId, turn: codexTurn(turnId) } });
    const asked = yield* Deferred.make<void>();
    const stop = yield* Deferred.make<void>();
    const methods: string[] = [];
    let stopped = false;
    const answers = answer(asked, stop);
    const rpc = answeringRpc(answers);
    const thread = makeAppServerThreads({
      ...rpc,
      request: (method, params, timeout) => Effect.suspend(() => {
        methods.push(method);
        return rpc.request(method, params, timeout);
      }),
      stop: Effect.sync(() => {
        stopped = true;
      }),
    }, queue, apart);
    const starting = yield* Effect.forkChild(thread.startTurn({ threadId: "thread-1", prompt: "go", cwd: "/tmp" }));
    yield* Deferred.await(asked);
    const stopping = yield* Effect.forkChild(Fiber.interrupt(starting));
    yield* Effect.yieldNow;
    yield* Deferred.succeed(stop, undefined);
    yield* settle;
    yield* Fiber.join(stopping);
    return { stopped, methods };
  }).pipe(
    Effect.provide(TestClock.layer()),
    Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(Arr.ensure(message).join(" ")))])),
  ));

test("a turn stopped as it starts, whose start the app-server answers late, is interrupted there once it is", async () => {
  const { stopped, methods } = await stoppedAsItStarts((asked) => ({
    "turn/start": () => Deferred.succeed(asked, undefined).pipe(Effect.andThen(Effect.sleep("10 seconds")), Effect.as({ turn: codexTurn("turn-1") })),
    "turn/interrupt": () => Effect.succeed({}),
  }), TestClock.adjust("10 seconds"));
  assert.deepEqual([stopped, methods], [false, ["turn/start", "turn/interrupt"]]);
});

test("a turn stopped as it starts, whose start or interrupt the app-server gives no answer to in time, takes the app-server with it", async () => {
  const logged: string[] = [];
  const unanswered = await stoppedAsItStarts((asked) => ({
    "turn/start": () => Deferred.succeed(asked, undefined).pipe(Effect.andThen(Effect.never)),
  }), TestClock.adjust("30 seconds"), { others: [{ threadId: "thread-2", turnId: "goal-turn" }], logged });
  assert.deepEqual(unanswered, { stopped: true, methods: ["turn/start"] });
  // Which threads' turns the app-server's stop ends, others' among them, is said.
  assert.ok(logged.some((line) => /stopping the app-server, .* ending=thread-2\/goal-turn$/u.test(line)), logged.join("\n"));
  const refused = await stoppedAsItStarts((asked, stop) => ({
    "turn/start": () => Deferred.succeed(asked, undefined).pipe(Effect.andThen(Deferred.await(stop)), Effect.as({ turn: codexTurn("turn-1") })),
    "turn/interrupt": () => Effect.fail(new AppServerRequestTimeout({ method: "turn/interrupt" })),
  }), Effect.void);
  assert.deepEqual(refused, { stopped: true, methods: ["turn/start", "turn/interrupt"] });
});

test("a turn stopped as it starts, whose interrupt the app-server answers with an error, as one that ended as it went, leaves the app-server running", async () => {
  const answered = await stoppedAsItStarts((asked, stop) => ({
    "turn/start": () => Deferred.succeed(asked, undefined).pipe(Effect.andThen(Deferred.await(stop)), Effect.as({ turn: codexTurn("turn-1") })),
    "turn/interrupt": () => Effect.fail(new AppServerRequestFailed({ error: { code: -32600, message: "no active turn to interrupt" } })),
  }), Effect.void);
  assert.deepEqual(answered, { stopped: false, methods: ["turn/start", "turn/interrupt"] });
});

test("an interrupt of a thread's turn by its id interrupts that turn alone, not the one the app-server went on to after it", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const interrupted: unknown[] = [];
    const thread = makeAppServerThreads({
      ...answeringRpc({ "turn/interrupt": (params) => Effect.sync(() => void interrupted.push(params)).pipe(Effect.as({})) }),
      stop: Effect.void,
    }, queue, apart);
    // The thread's turn-1 ends, and the app-server goes on to turn-2 of its own, a goal's.
    yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("turn-1") } });
    yield* queue.observe({ method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("turn-1", { status: "completed" }) } });
    yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("turn-2") } });
    assert.equal(yield* thread.interrupt("thread-1", "test", "turn-1"), false);
    assert.deepEqual(interrupted, []);
    assert.equal(yield* thread.interrupt("thread-1", "test", "turn-2"), true);
    assert.deepEqual(interrupted, [{ threadId: "thread-1", turnId: "turn-2" }]);
  })));

/** A thread's configuration of nothing alasio adds. */
const NO_CONFIG: CodexThreadConfig = { developer_instructions: "", mcp_servers: {} };

test("an app-server closed while a turn stopped as it started waits on its start interrupts the turn there before it stops", async () => {
  const process = appServerProcess();
  process.answer("thread/list", () => ({ data: [], nextCursor: null }));
  process.answer("turn/interrupt", () => ({}));
  await Effect.runPromise(Effect.gen(function*() {
    const scope = yield* Scope.make();
    const appServer = yield* makeAppServer({ spawn: process.spawn }).pipe(Scope.provide(scope));
    yield* appServer.listThreads({ cwd: "/repo", env: {} });
    const starting = yield* Effect.forkDetach(appServer.startTurn({ threadId: "thread-1", threadKey: "conversation-1", prompt: "go", cwd: "/repo", env: {}, config: NO_CONFIG }));
    // Stopped once its start is asked, the app-server not yet answering it.
    while (!process.written.some(({ method }) => method === "turn/start")) yield* Effect.yieldNow;
    const stopping = yield* Effect.forkDetach(Fiber.interrupt(starting));
    yield* Effect.yieldNow;
    // The app-server's scope closes, as alasio stops, with the stopped start pending still.
    const closing = yield* Effect.forkDetach(Scope.close(scope, Exit.void));
    yield* Effect.sleep("20 millis");
    const start = process.written.find(({ method }) => method === "turn/start");
    process.send({ id: start?.id, result: { turn: codexTurn("turn-1") } });
    yield* Fiber.join(closing);
    yield* Fiber.join(stopping);
  }).pipe(Effect.provide(Logger.layer([]))));
  assert.deepEqual(process.written.filter(({ method }) => method === "turn/interrupt").map(({ params }) => params), [{ threadId: "thread-1", turnId: "turn-1" }]);
});

test("app-server notification waits have no timeout of their own", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const waiting = yield* Effect.forkChild(queue.nextForThread("thread-1"));
    yield* TestClock.adjust("1 day");
    assert.equal(waiting.pollUnsafe(), undefined);
    yield* queue.observe({ method: "thread/queue/changed", params: { threadId: "thread-1" } });
    assert.equal((yield* Fiber.join(waiting)).method, "thread/queue/changed");
  }).pipe(Effect.provide(TestClock.layer()))));

test("an interrupted wait for a thread's notification leaves the next one for the wait after it", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const stopped = yield* Effect.forkChild(queue.nextForThread("thread-1"));
    yield* Effect.yieldNow;
    yield* Fiber.interrupt(stopped);
    yield* queue.observe({ method: "thread/queue/changed", params: { threadId: "thread-1" } });
    assert.equal((yield* queue.nextForThread("thread-1")).method, "thread/queue/changed");
  })));

test("app-server completion accepts every identity for one logical turn", () =>
  withNotifications((queue) => Effect.gen(function*() {
    yield* queue.rememberTurn("thread-1", "response-turn");
    yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("notification-turn") } });

    assert.equal(yield* queue.currentTurnId("thread-1"), "notification-turn");
    assert.deepEqual([...yield* queue.turnAliases("thread-1")].sort(), ["notification-turn", "response-turn"]);

    yield* queue.observe({ method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("response-turn", { status: "completed" }) } });

    assert.equal(yield* queue.currentTurnId("thread-1"), undefined);
    assert.deepEqual([...yield* queue.turnAliases("thread-1")].sort(), ["notification-turn", "response-turn"]);
  })));

test("app-server start sends configured model and keeps observed notification turn id when response handle differs", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const thread = makeAppServerThreads(answeringRpc({
      "turn/start": (params) => Effect.gen(function*() {
        assert.equal(params.model, "gpt-5.6-sol");
        assert.equal(params.effort, "high");
        yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("notification-turn") } });
        return { turn: codexTurn("response-turn") };
      }),
    }), queue, apart);

    const turnId = yield* thread.startTurn({ threadId: "thread-1", prompt: "go", cwd: "/tmp" });

    assert.equal(turnId, "notification-turn");
    assert.equal(yield* queue.currentTurnId("thread-1"), "notification-turn");
    assert.equal((yield* queue.nextForThread("thread-1")).method, "turn/started");
  })));

test("app-server fast completion cannot become a leftover turn after the start response", () =>
  withNotifications((queue) => Effect.gen(function*() {
    let starts = 0;
    let interrupts = 0;
    const thread = makeAppServerThreads(answeringRpc({
      "turn/interrupt": () => Effect.sync(() => {
        interrupts += 1;
        return {};
      }),
      "turn/start": () => Effect.gen(function*() {
        starts += 1;
        if (starts === 1) {
          yield* queue.observe({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("notification-turn-1") } });
          yield* queue.observe({ method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("notification-turn-1", { status: "completed" }) } });
        }
        return { turn: codexTurn(`response-turn-${starts}`) };
      }),
    }), queue, apart);

    assert.equal(yield* thread.startTurn({ threadId: "thread-1", prompt: "first", cwd: "/tmp" }), "notification-turn-1");
    assert.equal(yield* queue.currentTurnId("thread-1"), undefined);
    assert.deepEqual([...yield* queue.turnAliases("thread-1")].sort(), ["notification-turn-1", "response-turn-1"]);
    assert.equal(yield* queue.waitForTurnId("thread-1", 5), null);

    assert.equal(yield* thread.startTurn({ threadId: "thread-1", prompt: "second", cwd: "/tmp" }), "response-turn-2");
    assert.equal(interrupts, 0);
  })));

test("app-server thread start and resume carry alasio model config", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const resumes: AppServerParams<"thread/resume">[] = [];
    const starts: AppServerParams<"thread/start">[] = [];
    const thread = makeAppServerThreads(answeringRpc({
      "thread/loaded/list": () => Effect.succeed({ data: [], nextCursor: null }),
      "thread/resume": (params) => Effect.sync(() => {
        resumes.push(params);
        return threadLoaded(codexThread(params.threadId));
      }),
      "thread/start": (params) => Effect.sync(() => {
        starts.push(params);
        return threadLoaded(codexThread("thread-started"));
      }),
    }), queue, apart);
    const config = { project_doc_max_bytes: 32768, developer_instructions: "", mcp_servers: {} };

    yield* thread.ensureThread({ threadId: "thread-existing", threadKey: "conversation-1", cwd: "/repo", env: {}, config });
    yield* thread.startThread({ threadKey: "conversation-2", cwd: "/repo", env: {}, config });

    for (const params of [resumes[0], starts[0]]) {
      assert.equal(params?.model, "gpt-5.6-sol");
      assert.equal(params.config?.["model_reasoning_effort"], "high");
      assert.equal(params.config?.["project_doc_max_bytes"], 32768);
    }
  })));

/** The log lines `effect` writes. */
function loggedBy<A, E>(effect: Effect.Effect<A, E>): Effect.Effect<{ readonly result: Exit.Exit<A, E>; readonly lines: string[] }> {
  const lines: string[] = [];
  const logger = Logger.make(({ message }) => {
    lines.push(Arr.ensure(message).join(" "));
  });
  return Effect.exit(effect).pipe(Effect.map((result) => ({ result, lines })), Effect.provide(Logger.layer([logger])));
}

/**
 * The events of the turn `turnId` of thread-1, on an app-server process that has sent
 * `notifications` first, as the turn's stream reads them; and what was logged.
 */
function streamed(notifications: readonly AppServerNotification[], turnId: string) {
  const process = appServerProcess();
  return loggedBy(Effect.scoped(Effect.gen(function*() {
    const appServer = yield* makeAppServer({ spawn: process.spawn });
    // Started as any call starts it, with the thread list of an app-server that has none.
    process.answer("thread/list", () => ({ data: [], nextCursor: null }));
    yield* appServer.listThreads({ cwd: "/repo", env: {} });
    for (const notification of notifications) process.send(notification);
    return yield* appServer.eventsForTurn("thread-1", turnId).pipe(Stream.map((event) => event.type), Stream.runCollect);
  })));
}

test("app-server stream adopts notification turn id when response handle differs", async () => {
  const { result } = await Effect.runPromise(streamed([
    { method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("notification-turn") } },
    { method: "item/started", params: { threadId: "thread-1", turnId: "notification-turn", item: agentMessage("item-1", ""), startedAtMs: 0 } },
    { method: "item/completed", params: { threadId: "thread-1", turnId: "notification-turn", item: agentMessage("item-1", "done"), completedAtMs: 0 } },
    { method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("notification-turn", { status: "completed" }) } },
  ], "response-turn"));
  assert.deepEqual(result, Exit.succeed(["turn.started", "item.started", "item.completed", "turn.completed"] satisfies AppServerEvent["type"][]));
});

test("app-server stream interrupted before the turn ends interrupts the turn, as stopped by its control", async () => {
  const process = appServerProcess();
  const { lines } = await Effect.runPromise(loggedBy(Effect.scoped(Effect.gen(function*() {
    const appServer = yield* makeAppServer({ spawn: process.spawn });
    process.answer("thread/list", () => ({ data: [], nextCursor: null }));
    process.answer("turn/interrupt", () => ({}));
    yield* appServer.listThreads({ cwd: "/repo", env: {} });
    process.send({ method: "turn/started", params: { threadId: "thread-1", turn: codexTurn("active-turn") } });
    // Read up to the turn's start, then stopped while it waits for more.
    const started = yield* Deferred.make<void>();
    const reading = yield* appServer.eventsForTurn("thread-1", "active-turn").pipe(
      Stream.tap(() => Deferred.succeed(started, undefined)),
      Stream.runDrain,
      Effect.forkChild,
    );
    yield* Deferred.await(started);
    yield* Fiber.interrupt(reading);
  }))));
  assert.deepEqual(process.written.filter(({ method }) => method === "turn/interrupt").map(({ params }) => params), [{ threadId: "thread-1", turnId: "active-turn" }]);
  assert.ok(lines.some((line) => /interrupting app-server turn thread=thread-1 turn=active-turn origin=abort-signal/.test(line)), lines.join("\n"));
});

test("app-server stream ignores unmapped same-thread notifications without tripping stale guard", async () => {
  const { result } = await Effect.runPromise(streamed([
    ...Array.from({ length: 1005 }, (): AppServerNotification => ({ method: "thread/queue/changed", params: { threadId: "thread-1" } })),
    { method: "turn/completed", params: { threadId: "thread-1", turn: codexTurn("active-turn", { status: "completed" }) } },
  ], "active-turn"));
  assert.deepEqual(result, Exit.succeed(["turn.completed"] satisfies AppServerEvent["type"][]));
});

test("app-server stream still rejects explicitly mismatched lifecycle turn notifications", async () => {
  const { result } = await Effect.runPromise(streamed(
    Array.from({ length: 1000 }, (): AppServerNotification => ({
      method: "thread/tokenUsage/updated",
      params: { threadId: "thread-1", turnId: "old-turn", tokenUsage: noTokenUsage() },
    })),
    "active-turn",
  ));
  const error = Exit.findErrorOption(result);
  assert.ok(Option.isSome(error), "the stream failed");
  assert.match(error.value.message, /Exceeded 1000 skipped app-server notifications/);
});

test("an error notification reports Codex's own message, unless Codex is retrying", () => {
  const error = (willRetry: boolean) => mapNotificationToSdkEvent({
    method: "error",
    params: { error: { message: "Rate limit reached", codexErrorInfo: null, additionalDetails: null, misalignment: null }, willRetry, threadId: "thread-1", turnId: "turn-1" },
  });
  assert.deepEqual(error(false), { type: "error", message: "Rate limit reached" });
  assert.equal(error(true), null);
});

test("a file change shows a deletion as one, from the app-server and from exec", () => {
  const names = (changes: readonly v2.FileUpdateChange[] | SdkFileChangeItem["changes"]) => {
    const projection = responseProjection("pending-1");
    mapItemToBlocks({ type: "file_change", id: "item-1", changes }, projection);
    return projection.blockSequence.map((block) => ("name" in block ? block.name : undefined));
  };
  assert.deepEqual(names([
    { path: "a", kind: { type: "delete" }, diff: "" },
    { path: "b", kind: { type: "update", move_path: null }, diff: "" },
    { path: "c", kind: { type: "add" }, diff: "" },
  ]), ["Delete", "Edit", "Edit"]);
  assert.deepEqual(names([{ path: "a", kind: "delete" }, { path: "b", kind: "update" }]), ["Delete", "Edit"]);
});

test("the login's models are listed hidden ones too, once an app-server, and again for the next app-server", () =>
  withNotifications((queue) => Effect.gen(function*() {
    const asked: unknown[] = [];
    let running = 1;
    const thread = makeAppServerThreads({
      ...answeringRpc({
        "model/list": (params) => Effect.sync(() => (asked.push(params), { data: [codexModel(`gpt-of-${running}`, { hidden: true })], nextCursor: null })),
      }),
      running: Effect.sync(() => running),
    }, queue, apart);
    const scope = { cwd: "/tmp", env: {} };
    const listed = yield* Effect.all([thread.loginModels(scope), thread.loginModels(scope)], { concurrency: "unbounded" });
    assert.deepEqual([listed.map((models) => models.map(({ id }) => id)), asked.length], [[["gpt-of-1"], ["gpt-of-1"]], 1]);
    assert.equal((asked[0] as { readonly includeHidden?: boolean }).includeHidden, true);
    // The app-server replaced: its models listed anew.
    running = 2;
    assert.deepEqual([(yield* thread.loginModels(scope)).map(({ id }) => id), asked.length], [["gpt-of-2"], 2]);
  })));

test("the login's models not listed within ten seconds are none for the turn that asked, which runs on the model chosen, and asked for again by the next", () =>
  withNotifications((queue) => Effect.gen(function*() {
    let asked = 0;
    // The first list never comes; the next does.
    const thread = makeAppServerThreads(answeringRpc({
      "model/list": () => Effect.suspend(() => ((asked += 1) === 1 ? Effect.never : Effect.succeed({ data: [codexModel("gpt-5.6-terra")], nextCursor: null }))),
    }), queue, apart);
    const scope = { cwd: "/tmp", env: {} };
    const listing = yield* Effect.forkChild(thread.loginModels(scope));
    yield* TestClock.adjust("9 seconds");
    assert.equal(listing.pollUnsafe(), undefined);
    yield* TestClock.adjust("1 second");
    assert.deepEqual(yield* Fiber.join(listing), []);
    // Not kept: the next turn asks again, and keeps the list that came.
    assert.deepEqual([(yield* thread.loginModels(scope)).map(({ id }) => id), asked], [["gpt-5.6-terra"], 2]);
    assert.deepEqual([(yield* thread.loginModels(scope)).map(({ id }) => id), asked], [["gpt-5.6-terra"], 2]);
  }).pipe(Effect.provide(TestClock.layer()))));
