// @ts-nocheck
import assert from "node:assert/strict";
import { test } from "node:test";

import { AppServerClient } from "../src/codex/app-server/client.ts";
import { AppServerNotificationQueue } from "../src/codex/app-server/notification-queue.ts";
import { AppServerThreadClient } from "../src/codex/app-server/thread-client.ts";
import { getNotificationTurnId, mapNotificationToSdkEvent, notificationMatchesTurn } from "../src/codex/app-server/protocol.ts";
import { mapItemToBlocks } from "../src/codex/event-projection.ts";

test("app-server protocol reads turn identity from direct and nested notification shapes", () => {
  assert.equal(getNotificationTurnId({ params: { turnId: "turn-direct" } }), "turn-direct");
  assert.equal(getNotificationTurnId({ params: { turn: { id: "turn-nested" } } }), "turn-nested");
  assert.equal(getNotificationTurnId({ params: { item: { turnId: "turn-item" } } }), "turn-item");
});

test("app-server protocol rejects stale completed-turn notifications for the active stream", () => {
  const staleCompletion = {
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: { id: "old-turn", status: { type: "completed" } },
    },
  };

  assert.equal(notificationMatchesTurn(staleCompletion, "new-turn"), false);
  assert.equal(notificationMatchesTurn(staleCompletion, "old-turn"), true);
});

test("app-server protocol preserves the upstream agent message phase", () => {
  const event = mapNotificationToSdkEvent({
    method: "item/completed",
    params: {
      item: {
        id: "answer-1",
        type: "agentMessage",
        text: "The final answer.",
        phase: "final_answer",
      },
    },
  });

  assert.equal(event.item.type, "agent_message");
  assert.equal(event.item.phase, "final_answer");
});

test("app-server notification queue does not forget the active turn when an old completion arrives", () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });

  queue.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: { id: "new-turn" },
    },
  });
  queue.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: { id: "old-turn", status: { type: "completed" } },
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), "new-turn");
});

test("app-server notification queue remembers goal-created turn ids", async () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });

  const turnIdPromise = queue.waitForTurnId("thread-1", { timeoutMs: 100 });
  queue.observe({
    method: "thread/goal/updated",
    params: {
      threadId: "thread-1",
      turnId: "goal-turn",
      goal: { objective: "Keep working", status: "active" },
    },
  });

  assert.equal(await turnIdPromise, "goal-turn");
  assert.equal(queue.getCurrentTurnId("thread-1"), "goal-turn");

  const notification = await queue.nextForThread("thread-1");
  assert.equal(notification.method, "thread/goal/updated");
});

test("app-server goal handoff after completion starts a distinct logical turn", () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });
  queue.rememberTurn("thread-1", "completed-turn");
  queue.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: { id: "completed-turn", status: { type: "completed" } },
    },
  });

  queue.observe({
    method: "thread/goal/updated",
    params: {
      threadId: "thread-1",
      turnId: "goal-turn",
      goal: { objective: "Keep working", status: "active" },
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), "goal-turn");
  assert.deepEqual([...queue.getTurnAliases("thread-1")], ["goal-turn"]);
});

test("app-server notification waits do not acquire a wall-clock timeout", async () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });
  const originalSetTimeout = globalThis.setTimeout;
  let timeoutScheduled = false;
  let notificationPromise;
  globalThis.setTimeout = (...args) => {
    timeoutScheduled = true;
    return originalSetTimeout(...args);
  };
  try {
    notificationPromise = queue.nextForThread("thread-1");
  } finally {
    globalThis.setTimeout = originalSetTimeout;
  }

  assert.equal(timeoutScheduled, false);
  queue.observe({ method: "turn/progress", params: { threadId: "thread-1" } });
  assert.equal((await notificationPromise).method, "turn/progress");
});

test("app-server notification waits honor an already-aborted control signal", async () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });
  const controller = new AbortController();
  controller.abort("operator stop");

  await assert.rejects(
    queue.nextForThread("thread-1", controller.signal),
    /operator stop/,
  );
  assert.equal(queue.waiters.length, 0);
});

test("app-server completion accepts every identity for one logical turn", () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });

  queue.rememberTurn("thread-1", "response-turn");
  queue.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: { id: "notification-turn" },
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
      turn: { id: "response-turn", status: { type: "completed" } },
    },
  });

  assert.equal(queue.getCurrentTurnId("thread-1"), undefined);
  assert.deepEqual(
    [...queue.getTurnAliases("thread-1")].sort(),
    ["notification-turn", "response-turn"],
  );
});

test("app-server start sends configured model and keeps observed notification turn id when response handle differs", async () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });
  const thread = new AppServerThreadClient({
    notifications: queue,
    log: { info: () => undefined, warn: () => undefined },
    rpc: {
      request: async (method, params) => {
        assert.equal(method, "turn/start");
        assert.equal(params.model, "gpt-5.6-sol");
        assert.equal(params.effort, "high");
        queue.observe({
          method: "turn/started",
          params: {
            threadId: "thread-1",
            turn: { id: "notification-turn" },
          },
        });
        return { turn: { id: "response-turn" } };
      },
    },
  });

  const turnId = await thread.startTurn({ threadId: "thread-1", prompt: "go", cwd: "/tmp" });

  assert.equal(turnId, "notification-turn");
  assert.equal(queue.getCurrentTurnId("thread-1"), "notification-turn");
  const notification = await queue.nextForThread("thread-1");
  assert.equal(notification.method, "turn/started");
});

test("app-server fast completion cannot become a leftover turn after the start response", async () => {
  const queue = new AppServerNotificationQueue({
    log: { info: () => undefined, warn: () => undefined },
  });
  let starts = 0;
  let interrupts = 0;
  const thread = new AppServerThreadClient({
    notifications: queue,
    log: { info: () => undefined, warn: () => undefined },
    rpc: {
      request: async (method) => {
        if (method === "turn/interrupt") {
          interrupts += 1;
          return {};
        }
        assert.equal(method, "turn/start");
        starts += 1;
        if (starts === 1) {
          queue.observe({
            method: "turn/started",
            params: { threadId: "thread-1", turn: { id: "notification-turn-1" } },
          });
          queue.observe({
            method: "turn/completed",
            params: {
              threadId: "thread-1",
              turn: { id: "notification-turn-1", status: { type: "completed" } },
            },
          });
        }
        return { turn: { id: `response-turn-${starts}` } };
      },
    },
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
  const requests = [];
  const rpc = {
    start: async () => undefined,
    request: async (method, params) => {
      requests.push({ method, params });
      if (method === "thread/loaded/list") {
        return { data: [] };
      }
      if (method === "thread/resume") {
        return { thread: { id: params.threadId, turns: [] } };
      }
      if (method === "thread/start") {
        return { thread: { id: "thread-started" } };
      }
      throw new Error(`unexpected method ${method}`);
    },
  };
  const thread = new AppServerThreadClient({
    notifications: new AppServerNotificationQueue({
      log: { info: () => undefined, warn: () => undefined },
    }),
    log: { info: () => undefined, warn: () => undefined },
    rpc,
  });

  await thread.ensureThread({
    threadId: "thread-existing",
    threadKey: "conversation-1",
    cwd: "/repo",
    env: {},
    config: { project_doc_max_bytes: 32768 },
  });
  await thread.startThread({
    threadKey: "conversation-2",
    cwd: "/repo",
    env: {},
    config: { project_doc_max_bytes: 32768 },
  });

  const resumeRequest = requests.find((request) => request.method === "thread/resume");
  const startRequest = requests.find((request) => request.method === "thread/start");
  for (const request of [resumeRequest, startRequest]) {
    assert.equal(request.params.model, "gpt-5.6-sol");
    assert.equal(request.params.config.model_reasoning_effort, "high");
    assert.equal(request.params.config.project_doc_max_bytes, 32768);
  }
});

test("app-server stream adopts notification turn id when response handle differs", async () => {
  const client = new AppServerClient();

  client.notifications.observe({
    method: "turn/started",
    params: {
      threadId: "thread-1",
      turn: { id: "notification-turn" },
    },
  });
  client.notifications.observe({
    method: "item/started",
    params: {
      threadId: "thread-1",
      item: { id: "item-1", turnId: "notification-turn", type: "agentMessage", text: "" },
    },
  });
  client.notifications.observe({
    method: "item/completed",
    params: {
      threadId: "thread-1",
      item: { id: "item-1", turnId: "notification-turn", type: "agentMessage", text: "done" },
    },
  });
  client.notifications.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: { id: "notification-turn", status: { type: "completed" } },
    },
  });

  const events = [];
  for await (const event of client.eventsForTurn("thread-1", "response-turn")) {
    events.push(event.type);
  }

  assert.deepEqual(events, ["turn.started", "item.started", "item.completed", "turn.completed"]);
  client.stop();
});

test("app-server stream attributes an in-flight abort to its control signal", async () => {
  const client = new AppServerClient();
  const origins = [];
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
      method: "experimental/progress",
      params: {
        threadId: "thread-1",
        data: `line ${index}`,
      },
    });
  }
  client.notifications.observe({
    method: "turn/completed",
    params: {
      threadId: "thread-1",
      turn: { id: "active-turn", status: { type: "completed" } },
    },
  });

  const events = [];
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
        tokenUsage: null,
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
  const error = (willRetry) => mapNotificationToSdkEvent({
    method: "error",
    params: { error: { message: "Rate limit reached", codexErrorInfo: null, additionalDetails: null, misalignment: null }, willRetry, threadId: "thread-1", turnId: "turn-1" },
  });
  assert.deepEqual(error(false), { type: "error", message: "Rate limit reached" });
  assert.equal(error(true), null);
});

test("a file change shows a deletion as one, from the app-server and from exec", () => {
  const names = (changes) => {
    const blockSequence = [];
    mapItemToBlocks({ type: "file_change", id: "item-1", changes }, { blockSequence, persistence: { appendBlockToPending() {} }, pendingResponseId: "pending-1" });
    return blockSequence.map((block) => block.name);
  };
  assert.deepEqual(names([{ path: "a", kind: { type: "delete" } }, { path: "b", kind: { type: "update", move_path: null } }, { path: "c", kind: { type: "add" } }]), ["Delete", "Edit", "Edit"]);
  assert.deepEqual(names([{ path: "a", kind: "delete" }, { path: "b", kind: "update" }]), ["Delete", "Edit"]);
});
