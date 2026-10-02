import assert from "node:assert/strict";
import { test } from "node:test";

import { finalResponseToMarkdown } from "../src/codex/response-markdown.js";
import {
  parseMcpToolName,
  projectAssistantMessageToItems,
  projectResultMessage,
  projectToolUseToItem,
} from "../src/harness/claude/event-projection.js";
import { buildClaudeUserMessage, createPromptChannel } from "../src/harness/claude/prompt-channel.js";
import { createClaudeLiveSessions } from "../src/harness/claude/live-sessions.js";
import { buildClaudeQueryOptions, executeClaudeTurn } from "../src/harness/claude/runtime.js";
import { ALASIO_CLAUDE_EFFORT, ALASIO_CLAUDE_MODEL } from "../src/harness/claude/model.js";
import { createClaudeSessionApi } from "../src/harness/claude/sessions.js";

function createPersistence() {
  const state = {
    blocks: [],
    sessionIds: [],
    activeTurnSessionIds: [],
    completed: [],
    posted: [],
    pending: [],
    usage: [],
  };
  return {
    state,
    createPendingResponse: (chatId, messageId) => {
      state.pending.push(String(messageId));
      return `pending-${state.pending.length}`;
    },
    markPendingAsPosted: (id) => state.posted.push(id),
    updateActiveTurnPendingResponseId: () => undefined,
    updatePendingSessionId: (_id, sessionId) => state.sessionIds.push(sessionId),
    updateActiveTurnSessionId: (_threadKey, sessionId) => state.activeTurnSessionIds.push(sessionId),
    appendBlockToPending: (_id, block) => state.blocks.push(block),
    markPendingResponseComplete: (id) => state.completed.push(id),
    updateSessionUsage: (sessionId, usage) => state.usage.push([sessionId, usage]),
  };
}

const quietSessions = {
  async sessionExists() {
    return false;
  },
};

function assistant(content, extra = {}) {
  return { type: "assistant", message: { role: "assistant", content }, parent_tool_use_id: null, session_id: "s-1", ...extra };
}

/** One turn on a throwaway live-session registry, closed once the turn returns. */
/** A folder conversation's bayma, as the deployment's host profile would give it. */
const folderBayma = async ({ threadKey }) => ({ type: "http", url: `http://bayma-${threadKey.replace(/\W/gu, "")}.alasio-host.svc:7290/mcp`, headers: { Authorization: "Bearer t" } });

async function runClaudeTurn(params) {
  const liveSessions = createClaudeLiveSessions({
    workingDirectory: params.workingDirectory,
    sessions: params.sessions,
    queryFactory: params.queryFactory,
    folderBayma,
  });
  try {
    return await executeClaudeTurn({ ...params, liveSessions });
  } finally {
    await liveSessions.closeAll("test");
  }
}

async function* drainPromptChannel(iterable, seen) {
  for await (const message of iterable) {
    seen.push(message);
  }
}

test("prompt channel delivers pushed messages and completes when ended", async () => {
  const channel = createPromptChannel();
  const seen = [];
  const drained = drainPromptChannel(channel.iterable, seen);
  assert.equal(channel.push(buildClaudeUserMessage("first")), true);
  channel.push(buildClaudeUserMessage("second"));
  channel.end();
  await drained.next();
  assert.deepEqual(seen.map((message) => message.message.content), ["first", "second"]);
  assert.equal(channel.push(buildClaudeUserMessage("late")), false);
  assert.equal(buildClaudeUserMessage("x").origin.kind, "human");
});

test("tool use blocks project into Codex-shaped items", () => {
  assert.deepEqual(projectToolUseToItem({ id: "t1", name: "Bash", input: { command: "ls -la" } }), {
    type: "command_execution",
    id: "t1",
    command: "ls -la",
  });
  assert.equal(projectToolUseToItem({ id: "t2", name: "Edit", input: { file_path: "/x" } }).type, "file_change");
  assert.equal(projectToolUseToItem({ id: "t3", name: "WebSearch", input: {} }).type, "web_search");
  assert.deepEqual(parseMcpToolName("mcp__bayma__exec"), { server: "bayma", tool: "exec" });
  assert.deepEqual(projectToolUseToItem({ id: "t4", name: "mcp__bayma__exec", input: { code: "1" } }), {
    type: "mcp_tool_call",
    id: "t4",
    server: "bayma",
    tool: "exec",
    arguments: { code: "1" },
  });
  const ask = projectToolUseToItem({ id: "t5", name: "AskUserQuestion", input: { questions: [{ header: "Scope" }] } });
  assert.equal(ask.tool, "AskUserQuestion");
  assert.deepEqual(ask.arguments.questions, [{ header: "Scope" }]);
});

test("assistant text stays commentary and subagent messages are ignored", () => {
  const items = projectAssistantMessageToItems(assistant([
    { type: "text", text: "Looking around." },
    { type: "tool_use", id: "t1", name: "Bash", input: { command: "pwd" } },
  ]));
  assert.deepEqual(items.map((item) => item.type), ["agent_message", "command_execution"]);
  assert.equal(items[0].phase, "commentary");
  assert.deepEqual(projectAssistantMessageToItems(assistant([{ type: "text", text: "inner" }], { parent_tool_use_id: "task-1" })), []);
});

test("result projection separates final answers from failures", () => {
  assert.deepEqual(projectResultMessage({ type: "result", subtype: "success", is_error: false, result: "Done.", usage: { cache_read_input_tokens: 5 } }), {
    ok: true,
    text: "Done.",
    usage: { cache_read_input_tokens: 5 },
  });
  const failure = projectResultMessage({ type: "result", subtype: "error_during_execution", is_error: true, errors: ["boom"], usage: null });
  assert.equal(failure.ok, false);
  assert.equal(failure.error, "boom");
  const apiFailure = projectResultMessage({ type: "result", subtype: "success", is_error: true, result: "rate limited" });
  assert.equal(apiFailure.ok, false);
  assert.equal(apiFailure.error, "rate limited");
});

test("query options resume existing sessions and reserve fresh ids", () => {
  const controller = new AbortController();
  const env = { ALASIO_CLAUDE_MODEL: "claude-test", ALASIO_CLAUDE_EFFORT: "high", ALASIO_CLAUDE_BIN: "/opt/claude" };
  const resumed = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: { HOME: "/h" }, mcpServers: {}, resumeSession: "abc", resumeExists: true, controller, hooks: {}, env });
  assert.equal(resumed.resume, "abc");
  assert.equal(resumed.sessionId, undefined);
  assert.equal(resumed.model, "claude-test");
  assert.equal(resumed.effort, "high");
  assert.equal(resumed.pathToClaudeCodeExecutable, "/opt/claude");
  assert.equal(resumed.permissionMode, "bypassPermissions");
  assert.deepEqual(resumed.mcpServers, {});
  assert.equal(resumed.strictMcpConfig, undefined);
  const reserved = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: { a: { type: "stdio", command: "a" } }, resumeSession: "abc", resumeExists: false, controller, hooks: {}, env: {} });
  assert.equal(reserved.sessionId, "abc");
  assert.equal(reserved.resume, undefined);
  // No override in the environment: the harness pins the model itself.
  assert.equal(reserved.model, ALASIO_CLAUDE_MODEL);
  assert.equal(reserved.effort, ALASIO_CLAUDE_EFFORT);
  assert.deepEqual(Object.keys(reserved.mcpServers), ["a"]);
});

test("with a session store, a query mirrors every transcript write as it is written but resumes from the local transcript", async () => {
  const appended = [];
  const store = { append: async (key, entries) => appended.push([key, entries]), load: async () => [{ type: "user" }] };
  const options = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: {}, resumeSession: "abc", resumeExists: true, controller: new AbortController(), hooks: {}, env: {}, sessionStore: store });
  assert.equal(options.persistSession, true);
  assert.equal(options.sessionStoreFlush, "eager");
  await options.sessionStore.append({ projectKey: "p", sessionId: "abc" }, [{ type: "user" }]);
  assert.equal(appended.length, 1);
  assert.equal(await options.sessionStore.load({ projectKey: "p", sessionId: "abc" }), null);
  const withoutStore = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: {}, controller: new AbortController(), hooks: {}, env: {} });
  assert.equal(withoutStore.sessionStore, undefined);
});

test("a result for another turn does not end the prompt channel", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  let promptUuid = null;
  let channelOpenWhenForeignResultSeen = null;
  const queryFactory = ({ prompt }) => {
    const generator = (async function* run() {
      const iterator = prompt[Symbol.asyncIterator]();
      const first = await iterator.next();
      promptUuid = first.value.uuid;
      yield { type: "system", subtype: "init", session_id: "s1", model: "m" };
      // A resumed session re-runs the turn a previous worker left interrupted;
      // its result names that turn's prompt, not ours.
      yield {
        type: "result",
        subtype: "success",
        is_error: false,
        result: "Finished the interrupted turn.",
        session_id: "s1",
        user_message_uuid: "someone-elses-turn",
        user_message_uuids: ["someone-elses-turn"],
        resume_reason: "interrupted_turn",
      };
      // If the channel had been closed on that result, this would never run:
      // the next read would report done, and a pending tool call would be
      // cancelled.
      const pending = iterator.next();
      channelOpenWhenForeignResultSeen = await Promise.race([
        pending.then(() => false),
        new Promise((resolve) => setTimeout(() => resolve(true), 20)),
      ]);
      yield assistant([{ type: "tool_use", id: "t1", name: "Bash", input: { command: "echo hi" } }]);
      yield {
        type: "result",
        subtype: "success",
        is_error: false,
        result: "Ours at last.",
        session_id: "s1",
        user_message_uuid: promptUuid,
        user_message_uuids: [promptUuid],
      };
      const closing = await iterator.next();
      assert.equal(closing.done, true);
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "s1",
    threadKey: "telegram:1",
    chatId: 1,
    messageId: 2,
    workingDirectory: "/work",
    persistence,
    activeQueries,
    sessions: { sessionExists: async () => true },
    queryFactory,
  });
  assert.equal(typeof promptUuid, "string", "the pushed prompt carries a client uuid");
  assert.equal(channelOpenWhenForeignResultSeen, true, "the channel stays open through a foreign result");
  assert.equal(result.responseCompleted, true);
  assert.equal(
    result.blockSequence.filter((block) => block.type === "text").at(-1)?.content,
    "Ours at last.",
    "the turn ends on the result that answers our prompt",
  );
});

test("a steered prompt keeps the channel open until its own result arrives", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const uuids = [];
  let closedBeforeSteerAnswered = null;
  const queryFactory = ({ prompt }) => {
    const generator = (async function* run() {
      const iterator = prompt[Symbol.asyncIterator]();
      uuids.push((await iterator.next()).value.uuid);
      yield { type: "system", subtype: "init", session_id: "s1", model: "m" };
      // Steer arrives while the turn is running.
      await activeQueries.get("telegram:1").steer("also do this");
      uuids.push((await iterator.next()).value.uuid);
      // The CLI answers only the original prompt first.
      yield {
        type: "result",
        subtype: "success",
        is_error: false,
        result: "First answer.",
        session_id: "s1",
        user_message_uuid: uuids[0],
        user_message_uuids: [uuids[0]],
      };
      const pending = iterator.next();
      closedBeforeSteerAnswered = await Promise.race([
        pending.then(() => true),
        new Promise((resolve) => setTimeout(() => resolve(false), 20)),
      ]);
      yield {
        type: "result",
        subtype: "success",
        is_error: false,
        result: "Steered answer.",
        session_id: "s1",
        user_message_uuid: uuids[1],
        user_message_uuids: [uuids[1]],
      };
      assert.equal((await iterator.next()).done, true);
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "s1",
    threadKey: "telegram:1",
    chatId: 1,
    messageId: 2,
    workingDirectory: "/work",
    persistence,
    activeQueries,
    sessions: { sessionExists: async () => true },
    queryFactory,
  });
  assert.equal(closedBeforeSteerAnswered, false, "the channel stays open for the steered prompt");
  assert.equal(
    result.blockSequence.filter((block) => block.type === "text").at(-1)?.content,
    "Steered answer.",
    "the newest matching result is the one posted",
  );
});

test("Claude turn persists session identity, tool blocks, and the result as final answer", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const seenPrompts = [];
  const queryFactory = ({ prompt, options }) => {
    assert.equal(options.cwd, "/work");
    assert.equal(options.sessionId, "reserved-1");
    const generator = (async function* run() {
      const iterator = prompt[Symbol.asyncIterator]();
      const first = await iterator.next();
      seenPrompts.push(first.value.message.content);
      yield { type: "system", subtype: "init", session_id: "reserved-1", model: "m" };
      yield assistant([{ type: "text", text: "Thinking out loud." }, { type: "tool_use", id: "t1", name: "Bash", input: { command: "echo hi" } }]);
      yield { type: "result", subtype: "success", is_error: false, result: "All done.", session_id: "reserved-1", usage: { cache_read_input_tokens: 42 }, user_message_uuids: [first.value.uuid] };
      const closing = await iterator.next();
      assert.equal(closing.done, true);
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "reserved-1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "1",
    workingDirectory: "/work",
    persistence,
    activeQueries,
    sessions: quietSessions,
    queryFactory,
  });
  assert.deepEqual(seenPrompts, ["do it"]);
  assert.equal(result.sessionId, "reserved-1");
  assert.equal(result.responseCompleted, true);
  assert.equal(result.interrupted, false);
  assert.deepEqual(persistence.state.sessionIds, ["reserved-1"]);
  assert.deepEqual(persistence.state.completed, ["pending-1"]);
  assert.deepEqual(persistence.state.usage, [["reserved-1", { cacheReadInputTokens: 42 }]]);
  assert.deepEqual(result.blockSequence.map((block) => block.type), ["text", "tool", "text"]);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "All done.");
  assert.equal(activeQueries.size, 0);
});

test("a result from a turn the CLI started itself does not end the operator's turn", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  let channelOpenAfterNotificationResult = null;
  const queryFactory = ({ prompt }) => {
    const generator = (async function* run() {
      const iterator = prompt[Symbol.asyncIterator]();
      const first = await iterator.next();
      yield { type: "system", subtype: "init", session_id: "s-7" };
      // The CLI answers a background task's stop notice before the operator's prompt.
      yield { type: "result", subtype: "success", is_error: false, result: "Noted the stopped task.", session_id: "s-7" };
      const pending = iterator.next();
      channelOpenAfterNotificationResult = await Promise.race([
        pending.then(() => false),
        new Promise((resolve) => setTimeout(() => resolve(true), 20)),
      ]);
      yield { type: "result", subtype: "success", is_error: false, result: "The real answer.", session_id: "s-7", user_message_uuids: [first.value.uuid] };
      assert.equal((await pending).done, true);
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "check now",
    resumeSession: null,
    threadKey: "telegram:7",
    chatId: "7",
    messageId: "7",
    workingDirectory: "/work",
    persistence,
    activeQueries,
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(channelOpenAfterNotificationResult, true, "the notice's result must not close the prompt channel");
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "The real answer.");
});

test("Claude turn interruption is classified as operator control and steering pushes guidance", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const pushed = [];
  let releaseSteer;
  const steerSeen = new Promise((resolve) => {
    releaseSteer = resolve;
  });
  const queryFactory = ({ prompt, options }) => {
    const generator = (async function* run() {
      const iterator = prompt[Symbol.asyncIterator]();
      await iterator.next();
      yield { type: "system", subtype: "init", session_id: "s-9" };
      const steer = await iterator.next();
      pushed.push(steer.value.message.content);
      releaseSteer();
      await new Promise((_resolve, reject) => {
        options.abortController.signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
      });
    })();
    generator.close = () => undefined;
    return generator;
  };
  const turn = runClaudeTurn({
    prompt: "long task",
    resumeSession: null,
    threadKey: "telegram:2",
    chatId: "2",
    messageId: "2",
    workingDirectory: "/work",
    persistence,
    activeQueries,
    sessions: quietSessions,
    queryFactory,
  });
  while (!activeQueries.has("telegram:2")) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const activeQuery = activeQueries.get("telegram:2");
  assert.equal(await activeQuery.steer("focus on tests"), true);
  await steerSeen;
  await activeQuery.abort("Interrupted from Telegram");
  const result = await turn;
  assert.deepEqual(pushed, ["focus on tests"]);
  assert.equal(result.interrupted, true);
  assert.equal(result.responseCompleted, false);
  assert.deepEqual(result.blockSequence, []);
  assert.equal(activeQueries.size, 0);
});

test("Claude turn surfaces failures and non-operator aborts as errors", async () => {
  const persistence = createPersistence();
  const queryFactory = ({ prompt }) => {
    const generator = (async function* run() {
      const first = await prompt[Symbol.asyncIterator]().next();
      yield { type: "system", subtype: "init", session_id: "s-3" };
      yield { type: "result", subtype: "error_max_turns", is_error: true, errors: ["max turns"], session_id: "s-3", user_message_uuids: [first.value.uuid] };
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "x",
    resumeSession: null,
    threadKey: "telegram:3",
    chatId: "3",
    messageId: "3",
    workingDirectory: "/work",
    persistence,
    activeQueries: new Map(),
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(result.responseCompleted, false);
  assert.deepEqual(result.blockSequence, [{ type: "text", content: "Error: max turns" }]);
});

test("Bash, Monitor, Grep and Glob are removed and bayma exec code passes the database guardrail", async () => {
  const persistence = createPersistence();
  let denied;
  let allowed;
  let disallowed;
  const queryFactory = ({ prompt, options }) => {
    disallowed = options.disallowedTools;
    const generator = (async function* run() {
      const first = await prompt[Symbol.asyncIterator]().next();
      yield { type: "system", subtype: "init", session_id: "s-4" };
      const [matcher] = options.hooks.PreToolUse;
      assert.equal(options.hooks.PreToolUse.length, 1);
      assert.equal(matcher.matcher, "mcp__bayma__exec");
      denied = await matcher.hooks[0]({
        hook_event_name: "PreToolUse",
        tool_name: "mcp__bayma__exec",
        tool_input: { session_id: "b1", code: 'import { $ } from "bun";\nawait $`psql -c "drop table users"`' },
        tool_use_id: "t1",
      });
      allowed = await matcher.hooks[0]({
        hook_event_name: "PreToolUse",
        tool_name: "mcp__bayma__exec",
        tool_input: { session_id: "b1", code: "await $`ls -la`" },
        tool_use_id: "t2",
      });
      yield { type: "result", subtype: "success", is_error: false, result: "Skipped the drop.", session_id: "s-4", user_message_uuids: [first.value.uuid] };
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await runClaudeTurn({
    prompt: "drop it",
    resumeSession: null,
    threadKey: "telegram:4",
    chatId: "4",
    messageId: "4",
    workingDirectory: "/work",
    persistence,
    activeQueries: new Map(),
    sessions: quietSessions,
    queryFactory,
  });
  assert.deepEqual(disallowed, ["Bash", "Monitor", "Grep", "Glob"]);
  assert.equal(denied.hookSpecificOutput.permissionDecision, "deny");
  assert.match(denied.hookSpecificOutput.permissionDecisionReason, /.+/);
  assert.deepEqual(allowed, {});
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "Skipped the drop.");
});

test("a restart through a Bun shell in bayma exec records self-induced provenance once", async () => {
  const persistence = createPersistence();
  const restarts = [];
  persistence.recordRestartEvent = (event) => restarts.push(event);
  const queryFactory = ({ prompt, options }) => {
    const generator = (async function* run() {
      const first = await prompt[Symbol.asyncIterator]().next();
      yield { type: "system", subtype: "init", session_id: "s-5" };
      await options.hooks.PreToolUse[0].hooks[0]({
        hook_event_name: "PreToolUse",
        tool_name: "mcp__bayma__exec",
        tool_input: { session_id: "b1", code: "await $`kubectl rollout restart deployment/alasio`" },
        tool_use_id: "t1",
      });
      yield { type: "result", subtype: "success", is_error: false, result: "Restarting.", session_id: "s-5", user_message_uuids: [first.value.uuid] };
    })();
    generator.close = () => undefined;
    return generator;
  };
  await runClaudeTurn({
    prompt: "restart yourself",
    resumeSession: null,
    threadKey: "telegram:5",
    chatId: "5",
    messageId: "5",
    workingDirectory: "/work",
    persistence,
    activeQueries: new Map(),
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(restarts.length, 1);
  assert.equal(restarts[0].cause, "self_induced");
});

test("Claude session api maps SDK transcripts to alasio session and rewind shapes", async () => {
  const messages = [
    { type: "user", uuid: "u1", parent_tool_use_id: null, timestamp: "2026-09-21T10:00:00Z", message: { role: "user", content: "first ask" } },
    { type: "assistant", uuid: "a1", parent_tool_use_id: null, message: { role: "assistant", content: [{ type: "text", text: "first answer" }] } },
    { type: "user", uuid: "u2", parent_tool_use_id: null, message: { role: "user", content: [{ type: "tool_result", tool_use_id: "t" }] } },
    { type: "user", uuid: "u3", parent_tool_use_id: null, timestamp: "2026-09-21T10:05:00Z", message: { role: "user", content: [{ type: "text", text: "second ask" }] } },
    { type: "assistant", uuid: "a2", parent_tool_use_id: null, message: { role: "assistant", content: [{ type: "text", text: "second answer" }] } },
  ];
  const forks = [];
  const api = createClaudeSessionApi({
    workingDirectory: "/work",
    sdk: {
      async listSessions({ dir }) {
        assert.equal(dir, "/work");
        return [
          { sessionId: "old", lastModified: 1, summary: "Old" },
          { sessionId: "new", lastModified: 2, customTitle: "Newest work", summary: "ignored" },
        ];
      },
      async getSessionMessages(sessionId) {
        return sessionId === "new" ? messages : [];
      },
      async forkSession(sessionId, options) {
        forks.push([sessionId, options]);
        return { sessionId: "forked" };
      },
      async getSessionInfo(sessionId) {
        return sessionId === "new" ? { sessionId } : undefined;
      },
    },
  });
  assert.deepEqual(await api.listSessions(1), [
    { uuid: "new", timestamp: "1970-01-01", label: "Newest work" },
    { uuid: "old", timestamp: "1970-01-01", label: "Old" },
  ]);
  assert.equal(await api.getTotalSessionPages(), 1);
  assert.equal(await api.getSessionByNumber(2), "old");
  assert.equal(await api.getSessionByNumber(3), null);
  assert.equal(await api.getSessionLastMessage("new"), "second answer");
  assert.deepEqual((await api.listSessionMessages("new")).map((message) => [message.index, message.uuid, message.text]), [
    [-1, "u3", "second ask"],
    [-2, "u1", "first ask"],
  ]);
  assert.equal(await api.getTotalRewindPages("new"), 1);
  assert.equal(await api.createForkedSession("new", "u3"), "forked");
  assert.deepEqual(forks, [["new", { dir: "/work", upToMessageId: "u2" }]]);
  assert.match(await api.createForkedSession("new", "u1"), /^[0-9a-f-]{36}$/);
  assert.equal(await api.createForkedSession("new", "missing"), null);
  assert.equal(await api.sessionExists("new"), true);
  assert.equal(await api.sessionExists("old"), false);
});

test("Claude session api reads the session store, and falls back to local transcripts when it cannot", async () => {
  const calls = [];
  let storeUp = true;
  const store = {
    // Holds no transcripts; unreachable once storeUp is false.
    async projectKeyOf() {
      if (!storeUp) throw new Error("store unreachable");
      return null;
    },
  };
  const api = createClaudeSessionApi({
    workingDirectory: "/work",
    store,
    sdk: {
      async listSessions(options) {
        calls.push(["list", Boolean(options.sessionStore)]);
        if (options.sessionStore && !storeUp) throw new Error("store unreachable");
        return [{ sessionId: "kept", lastModified: 1, summary: "Kept" }];
      },
      async getSessionMessages() {
        return [];
      },
      async forkSession() {
        return null;
      },
      async getSessionInfo(sessionId, options) {
        calls.push(["info", Boolean(options.sessionStore)]);
        return sessionId === "local-only" ? { sessionId } : undefined;
      },
    },
  });
  // Listing reads the store.
  assert.equal(await api.getSessionByNumber(1), "kept");
  assert.deepEqual(calls.splice(0), [["list", true]]);
  // With the store unreachable, listing and existence fall back to the local transcripts.
  storeUp = false;
  assert.equal(await api.getSessionByNumber(1), "kept");
  assert.deepEqual(calls.splice(0), [["list", true], ["list", false]]);
  assert.equal(await api.sessionExists("local-only"), true);
  assert.deepEqual(calls.splice(0), [["info", false]]);
});

/**
 * A scriptable stand-in for a long-lived Claude Code process: the test reads
 * the prompts it receives and decides what it streams back.
 */
function createFakeCli() {
  const outbox = [];
  let wake = null;
  const prompts = [];
  const promptWaiters = [];
  const state = { created: 0, closed: 0, interrupts: 0, options: [] };
  function emit(message) {
    outbox.push(message);
    wake?.();
  }
  function nextPrompt() {
    if (prompts.length > state.consumed) {
      return Promise.resolve(prompts[state.consumed++]);
    }
    return new Promise((resolve) => promptWaiters.push(resolve));
  }
  state.consumed = 0;
  const queryFactory = ({ prompt, options }) => {
    state.created += 1;
    state.options.push(options);
    let closed = false;
    (async () => {
      for await (const message of prompt) {
        prompts.push(message);
        const waiter = promptWaiters.shift();
        if (waiter) {
          state.consumed += 1;
          waiter(message);
        }
      }
      closed = true;
      wake?.();
    })();
    options.abortController.signal.addEventListener("abort", () => {
      closed = true;
      wake?.();
    });
    const generator = (async function* run() {
      while (true) {
        while (outbox.length > 0) {
          yield outbox.shift();
        }
        if (closed) {
          return;
        }
        await new Promise((resolve) => {
          wake = resolve;
        });
      }
    })();
    generator.close = () => {
      state.closed += 1;
      closed = true;
      wake?.();
    };
    generator.interrupt = async () => {
      state.interrupts += 1;
    };
    return generator;
  };
  return { queryFactory, emit, nextPrompt, prompts, state };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

function turnParams(persistence, activeQueries, extra = {}) {
  return {
    prompt: "go",
    resumeSession: "s-1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "1",
    workingDirectory: "/work",
    persistence,
    activeQueries,
    ...extra,
  };
}

test("background work keeps running after the answer and its report is delivered as its own reply", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const cli = createFakeCli();
  const liveSessions = createClaudeLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: async () => true }, queryFactory: cli.queryFactory, folderBayma });
  const events = [];
  const turn = executeClaudeTurn({
    ...turnParams(persistence, activeQueries),
    liveSessions,
    onBackgroundResponse: () => events.push("background-response"),
    onIdle: () => events.push("idle"),
  });
  const first = await cli.nextPrompt();
  cli.emit({ type: "system", subtype: "init", session_id: "s-1" });
  cli.emit(assistant([{ type: "tool_use", id: "t1", name: "Bash", input: { command: "bun run test", run_in_background: true } }]));
  cli.emit({ type: "system", subtype: "background_tasks_changed", tasks: [{ task_id: "b1", task_type: "local_bash", description: "bun run test" }] });
  cli.emit({ type: "result", subtype: "success", is_error: false, result: "Tests are running; I'll report back.", session_id: "s-1", user_message_uuids: [first.uuid] });
  const result = await turn;
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "Tests are running; I'll report back.");
  assert.equal(cli.state.closed, 0, "the answer does not end the process");
  assert.equal(activeQueries.size, 0, "the conversation is free once answered");

  // The task settles and Claude Code starts a turn of its own to report it.
  cli.emit({ type: "system", subtype: "background_tasks_changed", tasks: [] });
  cli.emit(assistant([{ type: "text", text: "Checking the log." }]));
  await settle();
  assert.equal(activeQueries.get("telegram:1")?.cliInitiated, true, "the report holds the conversation busy");
  cli.emit({ type: "result", subtype: "success", is_error: false, result: "All 212 tests passed.", session_id: "s-1" });
  await settle();
  assert.equal(activeQueries.size, 0);
  assert.deepEqual(persistence.state.completed, ["pending-1", "pending-2"]);
  assert.match(persistence.state.pending[1], /^claude-cli-turn:/);
  assert.equal(persistence.state.blocks.filter((block) => block.phase === "final_answer").at(-1).content, "All 212 tests passed.");
  assert.deepEqual(events.filter((event) => event === "background-response"), ["background-response"]);
  assert.ok(events.includes("idle"));
  await liveSessions.closeAll("test");
  assert.equal(cli.state.closed, 1);
});

test("later prompts on the same session reuse the live process; a new session or model replaces it", async () => {
  const persistence = createPersistence();
  let model = null;
  persistence.getModelChoice = () => model;
  const activeQueries = new Map();
  const cli = createFakeCli();
  const liveSessions = createClaudeLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: async () => true }, queryFactory: cli.queryFactory, folderBayma });
  const answer = async (text) => {
    const received = await cli.nextPrompt();
    cli.emit({ type: "result", subtype: "success", is_error: false, result: `re: ${received.message.content}`, session_id: "s-1", user_message_uuids: [received.uuid] });
    return received;
  };
  const run = async (prompt, extra = {}) => {
    const turn = executeClaudeTurn({ ...turnParams(persistence, activeQueries, { prompt, ...extra }), liveSessions });
    await answer();
    return await turn;
  };
  cli.emit({ type: "system", subtype: "init", session_id: "s-1" });
  assert.equal(finalResponseToMarkdown((await run("one")).blockSequence), "re: one");
  assert.equal(finalResponseToMarkdown((await run("two")).blockSequence), "re: two");
  assert.equal(cli.state.created, 1, "one process served both prompts");
  assert.equal(cli.state.options[0].resume, "s-1");

  model = { model: "claude-haiku-4-5-20251001", effort: null };
  await run("three");
  assert.equal(cli.state.created, 2, "a model change starts a new process");
  assert.equal(cli.state.closed, 1);

  await run("four", { resumeSession: "s-2" });
  assert.equal(cli.state.created, 3, "a different mounted session starts a new process");
  assert.equal(cli.state.options[2].resume, "s-2");
  await liveSessions.closeAll("test");
});

test("steering a Claude-started turn is answered in that turn's own reply", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const cli = createFakeCli();
  const liveSessions = createClaudeLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: async () => true }, queryFactory: cli.queryFactory, folderBayma });
  const turn = executeClaudeTurn({ ...turnParams(persistence, activeQueries), liveSessions });
  const first = await cli.nextPrompt();
  cli.emit({ type: "result", subtype: "success", is_error: false, result: "Started it.", session_id: "s-1", user_message_uuids: [first.uuid] });
  await turn;
  cli.emit(assistant([{ type: "text", text: "Build finished, reviewing." }]));
  await settle();
  const cliTurn = activeQueries.get("telegram:1");
  assert.equal(cliTurn.cliInitiated, true);
  assert.equal(await cliTurn.steer("also summarize warnings"), true);
  const steered = await cli.nextPrompt();
  assert.equal(steered.message.content, "also summarize warnings");
  cli.emit({ type: "result", subtype: "success", is_error: false, result: "Build is green; 3 warnings.", session_id: "s-1", user_message_uuids: [steered.uuid] });
  await settle();
  assert.equal(activeQueries.size, 0);
  assert.equal(persistence.state.completed.at(-1), "pending-2");
  await liveSessions.closeAll("test");
});

test("/stop interrupts the turn without killing the process, and the interrupted turn's tail is not a new turn", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const cli = createFakeCli();
  const liveSessions = createClaudeLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: async () => true }, queryFactory: cli.queryFactory, folderBayma });
  const turn = executeClaudeTurn({ ...turnParams(persistence, activeQueries), liveSessions });
  const first = await cli.nextPrompt();
  cli.emit(assistant([{ type: "text", text: "Working..." }]));
  await settle();
  await activeQueries.get("telegram:1").abort("Interrupted from Telegram");
  const result = await turn;
  assert.equal(result.interrupted, true);
  assert.equal(cli.state.interrupts, 1);
  assert.equal(cli.state.closed, 0);
  // Output the CLI flushes while stopping belongs to the stopped turn.
  cli.emit(assistant([{ type: "text", text: "(stopping)" }]));
  await settle();
  assert.equal(activeQueries.size, 0, "the tail does not open a Claude-started turn");
  cli.emit({ type: "result", subtype: "error_during_execution", is_error: true, errors: ["interrupted"], session_id: "s-1", user_message_uuids: [first.uuid] });
  await settle();
  assert.deepEqual(persistence.state.pending, ["1"], "no reply was created for the tail");
  await liveSessions.closeAll("test");
});

test("a Claude Code process that exits mid-turn fails that turn and the next prompt starts a fresh one", async () => {
  const persistence = createPersistence();
  const activeQueries = new Map();
  const cli = createFakeCli();
  const liveSessions = createClaudeLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: async () => true }, queryFactory: cli.queryFactory, folderBayma });
  const turn = executeClaudeTurn({ ...turnParams(persistence, activeQueries), liveSessions });
  await cli.nextPrompt();
  cli.state.options[0].abortController.abort("crash");
  const result = await turn;
  assert.equal(result.responseCompleted, false);
  assert.match(result.blockSequence.at(-1).content, /^Error: Claude Code exited before answering/);
  assert.equal(liveSessions.get("telegram:1"), null);
  const next = executeClaudeTurn({ ...turnParams(persistence, activeQueries, { prompt: "again" }), liveSessions });
  const again = await cli.nextPrompt();
  cli.emit({ type: "result", subtype: "success", is_error: false, result: "Back.", session_id: "s-1", user_message_uuids: [again.uuid] });
  assert.equal((await next).responseCompleted, true);
  assert.equal(cli.state.created, 2);
  await liveSessions.closeAll("test");
});
