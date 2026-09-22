import assert from "node:assert/strict";
import { test } from "node:test";

import { finalResponseToMarkdown } from "../src/codex/response-markdown.js";
import {
  parseMcpToolName,
  projectAssistantMessageToItems,
  projectResultMessage,
  projectToolUseToItem,
} from "../src/harness/claude/event-projection.js";
import { toClaudeMcpServers } from "../src/harness/claude/mcp.js";
import { buildClaudeUserMessage, createPromptChannel } from "../src/harness/claude/prompt-channel.js";
import { buildClaudeQueryOptions, executeClaudeTurn } from "../src/harness/claude/runtime.js";
import { ALASIO_CLAUDE_EFFORT, ALASIO_CLAUDE_MODEL } from "../src/harness/claude/model.js";
import { createClaudeSessionApi } from "../src/harness/claude/sessions.js";

function createPersistence() {
  const state = {
    blocks: [],
    sessionIds: [],
    activeTurnSessionIds: [],
    completed: [],
    usage: [],
  };
  return {
    state,
    createPendingResponse: () => "pending-1",
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
  assert.deepEqual(parseMcpToolName("mcp__bayma_python__run"), { server: "bayma_python", tool: "run" });
  assert.deepEqual(projectToolUseToItem({ id: "t4", name: "mcp__bayma_python__run", input: { code: "1" } }), {
    type: "mcp_tool_call",
    id: "t4",
    server: "bayma_python",
    tool: "run",
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

test("Codex MCP server tables convert to Claude SDK server configs", () => {
  assert.deepEqual(toClaudeMcpServers({
    bayma_python: { command: "uv", args: ["run", "bayma"], env: { BAYMA_STATE: "/tmp/x", EMPTY: null } },
    remote: { url: "https://mcp.example.test/sse" },
    disabled: { command: "x", enabled: false },
    broken: { args: ["no-command"] },
  }), {
    bayma_python: { type: "stdio", command: "uv", args: ["run", "bayma"], env: { BAYMA_STATE: "/tmp/x" } },
    remote: { type: "http", url: "https://mcp.example.test/sse" },
  });
  assert.deepEqual(toClaudeMcpServers(null), {});
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
  assert.equal(resumed.mcpServers, undefined);
  const reserved = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: { a: { type: "stdio", command: "a" } }, resumeSession: "abc", resumeExists: false, controller, hooks: {}, env: {} });
  assert.equal(reserved.sessionId, "abc");
  assert.equal(reserved.resume, undefined);
  // No override in the environment: the harness pins the model itself.
  assert.equal(reserved.model, ALASIO_CLAUDE_MODEL);
  assert.equal(reserved.effort, ALASIO_CLAUDE_EFFORT);
  assert.deepEqual(Object.keys(reserved.mcpServers), ["a"]);
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
  const result = await executeClaudeTurn({
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
  const result = await executeClaudeTurn({
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
      yield { type: "result", subtype: "success", is_error: false, result: "All done.", session_id: "reserved-1", usage: { cache_read_input_tokens: 42 } };
      const closing = await iterator.next();
      assert.equal(closing.done, true);
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await executeClaudeTurn({
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
  const turn = executeClaudeTurn({
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
  const queryFactory = () => {
    const generator = (async function* run() {
      yield { type: "system", subtype: "init", session_id: "s-3" };
      yield { type: "result", subtype: "error_max_turns", is_error: true, errors: ["max turns"], session_id: "s-3" };
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await executeClaudeTurn({
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

test("Bash PreToolUse hook denies forbidden database commands with guardrail guidance", async () => {
  const persistence = createPersistence();
  let hookResult;
  const queryFactory = ({ options }) => {
    const generator = (async function* run() {
      yield { type: "system", subtype: "init", session_id: "s-4" };
      const [matcher] = options.hooks.PreToolUse;
      assert.equal(matcher.matcher, "Bash");
      hookResult = await matcher.hooks[0]({
        hook_event_name: "PreToolUse",
        tool_name: "Bash",
        tool_input: { command: "psql -c 'drop table users'" },
        tool_use_id: "t1",
      });
      const allowed = await matcher.hooks[0]({
        hook_event_name: "PreToolUse",
        tool_name: "Bash",
        tool_input: { command: "ls" },
        tool_use_id: "t2",
      });
      assert.deepEqual(allowed, {});
      yield { type: "result", subtype: "success", is_error: false, result: "Skipped the drop.", session_id: "s-4" };
    })();
    generator.close = () => undefined;
    return generator;
  };
  const result = await executeClaudeTurn({
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
  assert.equal(hookResult.hookSpecificOutput.permissionDecision, "deny");
  assert.match(hookResult.hookSpecificOutput.permissionDecisionReason, /.+/);
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "Skipped the drop.");
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
