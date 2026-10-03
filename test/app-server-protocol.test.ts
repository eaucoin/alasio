import assert from "node:assert/strict";
import { mock, test } from "node:test";

import type { FileChangeItem as SdkFileChangeItem } from "@openai/codex-sdk";

import type { v2 } from "../.types/codex/index.js";
import { AppServerClient } from "../src/codex/app-server/client.ts";
import { AppServerNotificationQueue } from "../src/codex/app-server/notification-queue.ts";
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
import type { AppServerRpcClient } from "../src/codex/app-server/rpc-client.ts";
import { AppServerThreadClient } from "../src/codex/app-server/thread-client.ts";
import { type ResponseBlock, mapItemToBlocks } from "../src/codex/event-projection.ts";
import type { Logger } from "../src/shared/log.ts";
import { agentMessage, codexThread, codexTurn } from "./support/codex-protocol.ts";

const silentLog: Logger = { info: () => undefined, warn: () => undefined, error: () => undefined };

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
  readonly [M in AppServerMethod]?: (params: AppServerParams<M>) => Promise<AppServerResult<M>>;
};

/** An app-server that is already running and answers requests with `answers`; any other request fails the test. */
function answeringRpc(answers: AppServerAnswers): Pick<AppServerRpcClient, "start" | "request"> {
  return {
    start: async () => undefined,
    request: async (method, params) => {
      const answer = answers[method];
      if (!answer) {
        return assert.fail(`unexpected app-server request ${method}`);
      }
      return await answer(params);
    },
  };
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

test("app-server notification queue does not forget the active turn when an old completion arrives", () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });

  queue.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: codexTurn("new-turn"),
    },
  });
  queue.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("old-turn", { status: "completed" }),
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), "new-turn");
});

test("app-server notification queue remembers goal-created turn ids", async () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });

  const turnIdPromise = queue.waitForTurnId("thread-1", { timeoutMs: 100 });
  queue.observe({
    method: "thread/goal/updated",
    params: {
      threadId: "thread-1",
      turnId: "goal-turn",
      goal: activeGoal("thread-1", "Keep working"),
    },
  });

  assert.equal(await turnIdPromise, "goal-turn");
  assert.equal(queue.getCurrentTurnId("thread-1"), "goal-turn");

  const notification = await queue.nextForThread("thread-1");
  assert.equal(notification.method, "thread/goal/updated");
});

test("app-server goal handoff after completion starts a distinct logical turn", () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });
  queue.rememberTurn("thread-1", "completed-turn");
  queue.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("completed-turn", { status: "completed" }),
    },
  });

  queue.observe({
    method: "thread/goal/updated",
    params: {
      threadId: "thread-1",
      turnId: "goal-turn",
      goal: activeGoal("thread-1", "Keep working"),
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), "goal-turn");
  assert.deepEqual([...queue.getTurnAliases("thread-1")], ["goal-turn"]);
});

test("app-server notification waits do not acquire a wall-clock timeout", async () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });
  const setTimeoutSpy = mock.method(globalThis, "setTimeout");
  let notificationPromise: Promise<AppServerNotification> | undefined;
  try {
    notificationPromise = queue.nextForThread("thread-1");
  } finally {
    setTimeoutSpy.mock.restore();
  }

  assert.equal(setTimeoutSpy.mock.callCount(), 0);
  queue.observe({ method: "thread/queue/changed", params: { threadId: "thread-1" } });
  assert.equal((await notificationPromise)?.method, "thread/queue/changed");
});

test("app-server notification waits honor an already-aborted control signal", async () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });
  const controller = new AbortController();
  controller.abort("operator stop");

  await assert.rejects(
    queue.nextForThread("thread-1", controller.signal),
    /operator stop/,
  );
  assert.equal(queue.waiters.length, 0);
});

test("app-server completion accepts every identity for one logical turn", () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });

  queue.rememberTurn("thread-1", "response-turn");
  queue.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: codexTurn("notification-turn"),
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), "notification-turn");
  assert.deepEqual(
    [...queue.getTurnAliases("thread-1")].sort(),
    ["notification-turn", "response-turn"],
  );

  queue.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("response-turn", { status: "completed" }),
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), undefined);
  assert.deepEqual(
    [...queue.getTurnAliases("thread-1")].sort(),
    ["notification-turn", "response-turn"],
  );
});

test("app-server start sends configured model and keeps observed notification turn id when response handle differs", async () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });
  const thread = new AppServerThreadClient({
    notifications: queue,
    log: silentLog,
    rpc: answeringRpc({
      "turn/start": async (params) => {
        assert.equal(params.model, "gpt-5.6-sol");
        assert.equal(params.effort, "high");
        queue.observe({
          method: "turn/started",
          params: {
            threadId: "thread-1",
            turn: codexTurn("notification-turn"),
          },
        });
        return { turn: codexTurn("response-turn") };
      },
    }),
  });

  const turnId = await thread.startTurn({ threadId: "thread-1", prompt: "go", cwd: "/tmp" });

  assert.equal(turnId, "notification-turn");
  assert.equal(queue.getCurrentTurnId("thread-1"), "notification-turn");
  const notification = await queue.nextForThread("thread-1");
  assert.equal(notification.method, "turn/started");
});

test("app-server fast completion cannot become a leftover turn after the start response", async () => {
  const queue = new AppServerNotificationQueue({ log: silentLog });
  let starts = 0;
  let interrupts = 0;
  const thread = new AppServerThreadClient({
    notifications: queue,
    log: silentLog,
    rpc: answeringRpc({
      "turn/interrupt": async () => {
        interrupts += 1;
        return {};
      },
      "turn/start": async () => {
        starts += 1;
        if (starts === 1) {
          queue.observe({
            method: "turn/started",
            params: { threadId: "thread-1", turn: codexTurn("notification-turn-1") },
          });
          queue.observe({
            method: "turn/completed",
            params: {
              threadId: "thread-1",
              turn: codexTurn("notification-turn-1", { status: "completed" }),
            },
          });
        }
        return { turn: codexTurn(`response-turn-${starts}`) };
      },
    }),
  });

  assert.equal(
    await thread.startTurn({ threadId: "thread-1", prompt: "first", cwd: "/tmp" }),
    "notification-turn-1",
  );
  assert.equal(queue.getCurrentTurnId("thread-1"), undefined);
  assert.deepEqual(
    [...queue.getTurnAliases("thread-1")].sort(),
    ["notification-turn-1", "response-turn-1"],
  );
  assert.equal(await queue.waitForTurnId("thread-1", { timeoutMs: 5 }), null);

  assert.equal(
    await thread.startTurn({ threadId: "thread-1", prompt: "second", cwd: "/tmp" }),
    "response-turn-2",
  );
  assert.equal(interrupts, 0);
});

test("app-server thread start and resume carry alasio model config", async () => {
  const resumes: AppServerParams<"thread/resume">[] = [];
  const starts: AppServerParams<"thread/start">[] = [];
  const thread = new AppServerThreadClient({
    notifications: new AppServerNotificationQueue({ log: silentLog }),
    log: silentLog,
    rpc: answeringRpc({
      "thread/loaded/list": async () => ({ data: [], nextCursor: null }),
      "thread/resume": async (params) => {
        resumes.push(params);
        return threadLoaded(codexThread(params.threadId));
      },
      "thread/start": async (params) => {
        starts.push(params);
        return threadLoaded(codexThread("thread-started"));
      },
    }),
  });
  const config = { project_doc_max_bytes: 32768, developer_instructions: "", mcp_servers: {} };

  await thread.ensureThread({
    threadId: "thread-existing",
    threadKey: "conversation-1",
    cwd: "/repo",
    env: {},
    config,
  });
  await thread.startThread({
    threadKey: "conversation-2",
    cwd: "/repo",
    env: {},
    config,
  });

  for (const params of [resumes[0], starts[0]]) {
    assert.equal(params?.model, "gpt-5.6-sol");
    assert.equal(params.config?.["model_reasoning_effort"], "high");
    assert.equal(params.config?.["project_doc_max_bytes"], 32768);
  }
});

test("app-server stream adopts notification turn id when response handle differs", async () => {
  const client = new AppServerClient();

  client.notifications.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: codexTurn("notification-turn"),
    },
  });
  client.notifications.observe({
    method: "item/started",
    params: {
      threadId: "thread-1",
      turnId: "notification-turn",
      item: agentMessage("item-1", ""),
      startedAtMs: 0,
    },
  });
  client.notifications.observe({
    method: "item/completed",
    params: {
      threadId: "thread-1",
      turnId: "notification-turn",
      item: agentMessage("item-1", "done"),
      completedAtMs: 0,
    },
  });
  client.notifications.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("notification-turn", { status: "completed" }),
    },
  });

  const events: AppServerEvent["type"][] = [];
  for await (const event of client.eventsForTurn("thread-1", "response-turn")) {
    events.push(event.type);
  }

  assert.deepEqual(events, ["turn.started", "item.started", "item.completed", "turn.completed"]);
  client.stop();
});

test("app-server stream attributes an in-flight abort to its control signal", async () => {
  const client = new AppServerClient();
  const origins: (string | undefined)[] = [];
  client.notifications.rememberTurn("thread-1", "active-turn");
  client.interrupt = async (_threadId, origin) => {
    origins.push(origin);
    client.notifications.forgetTurn("thread-1");
    return true;
  };
  const controller = new AbortController();
  const iterator = client.eventsForTurn("thread-1", "active-turn", controller.signal);
  const nextEvent = iterator.next();
  await Promise.resolve();
  controller.abort("operator stop");

  await assert.rejects(nextEvent, /operator stop/);
  assert.deepEqual(origins, ["abort-signal"]);
  client.stop();
});

test("app-server stream ignores unmapped same-thread notifications without tripping stale guard", async () => {
  const client = new AppServerClient();

  for (let index = 0; index < 1005; index += 1) {
    client.notifications.observe({
      method: "thread/queue/changed",
      params: {
        threadId: "thread-1",
      },
    });
  }
  client.notifications.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: codexTurn("active-turn", { status: "completed" }),
    },
  });

  const events: AppServerEvent["type"][] = [];
  for await (const event of client.eventsForTurn("thread-1", "active-turn")) {
    events.push(event.type);
  }

  assert.deepEqual(events, ["turn.completed"]);
  client.stop();
});

test("app-server stream still rejects explicitly mismatched lifecycle turn notifications", async () => {
  const client = new AppServerClient();

  for (let index = 0; index < 1000; index += 1) {
    client.notifications.observe({
      method: "thread/tokenUsage/updated",
      params: {
        threadId: "thread-1",
        turnId: "old-turn",
        tokenUsage: noTokenUsage(),
      },
    });
  }

  await assert.rejects(
    async () => {
      for await (const _event of client.eventsForTurn("thread-1", "active-turn")) {
        // The stale guard should fail before yielding any event.
      }
    },
    /Exceeded 1000 skipped app-server notifications/,
  );
  client.stop();
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
    const blockSequence: ResponseBlock[] = [];
    mapItemToBlocks({ type: "file_change", id: "item-1", changes }, { blockSequence, persistence: { appendBlockToPending() {} }, pendingResponseId: "pending-1" });
    return blockSequence.map((block) => ("name" in block ? block.name : undefined));
  };
  assert.deepEqual(names([
    { path: "a", kind: { type: "delete" }, diff: "" },
    { path: "b", kind: { type: "update", move_path: null }, diff: "" },
    { path: "c", kind: { type: "add" }, diff: "" },
  ]), ["Delete", "Edit", "Edit"]);
  assert.deepEqual(names([{ path: "a", kind: "delete" }, { path: "b", kind: "update" }]), ["Delete", "Edit"]);
});
