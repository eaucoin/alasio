import assert from "node:assert/strict";
import { test } from "node:test";

import type {
  ForkSessionOptions,
  HookJSONOutput,
  Options,
  PreToolUseHookInput,
  PreToolUseHookSpecificOutput,
  SDKMessage,
  SDKUserMessage,
  SessionKey,
  SessionMessage,
  SessionStore,
  SessionStoreEntry,
} from "@anthropic-ai/claude-agent-sdk";
import { Deferred, Effect, Exit, Option, Scope } from "effect";

import type { ResponseBlock } from "../src/codex/event-projection.ts";
import { finalResponseToMarkdown } from "../src/codex/response-markdown.ts";
import {
  parseMcpToolName,
  projectAssistantMessageToItems,
  projectResultMessage,
  projectToolUseToItem,
} from "../src/harness/claude/event-projection.ts";
import { buildClaudeUserMessage, makePromptChannel } from "../src/harness/claude/prompt-channel.ts";
import { type ClaudeLiveSessions, type ClaudeLiveSessionsOptions, makeClaudeLiveSessions } from "../src/harness/claude/live-sessions.ts";
import { buildClaudeQueryOptions, type ClaudeQueryFactory } from "../src/harness/claude/runtime.ts";
import { ALASIO_CLAUDE_EFFORT, ALASIO_CLAUDE_MODEL } from "../src/harness/claude/model.ts";
import { type ClaudeSessionApi, type ClaudeTranscriptStore, createClaudeSessionApi } from "../src/harness/claude/sessions.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import type { TurnParams, TurnPersistence, TurnResult } from "../src/harness/index.ts";
import { BaymaNotAnswering } from "../src/kube/sandboxes.ts";
import type { BaymaMcpServer, HostBaymaScope } from "../src/mcp/bayma.ts";
import type { ModelChoice } from "../src/persistence/conversation-repository.ts";
import type { ResponseBlock as StoredBlock } from "../src/persistence/response-repository.ts";
import { StoreError } from "../src/persistence/sql.ts";
import type { NewRestartEvent } from "../src/persistence/restart-repository.ts";
import type { SessionUsage } from "../src/persistence/usage-repository.ts";
import {
  assistantMessage,
  backgroundTasksChanged,
  errorResult,
  fakeQuery,
  initMessage,
  readPrompt,
  type StampedPrompt,
  stamped,
  successResult,
  text,
  toolUse,
  usage,
} from "./support/claude-sdk.ts";
import { activeTurnsHolding, type Hold } from "./support/busy-pods.ts";
import { ownTurnsOn } from "./support/persistence.ts";

type TextResponseBlock = Extract<ResponseBlock, { type: "text" }>;

/** A turn's persistence that records what the turn stores. */
interface RecordingPersistence extends TurnPersistence {
  readonly state: {
    readonly blocks: StoredBlock[];
    readonly sessionIds: (string | null)[];
    readonly activeTurnSessionIds: (string | null)[];
    readonly completed: string[];
    readonly posted: string[];
    readonly pending: string[];
    readonly usage: [string | null | undefined, SessionUsage | null | undefined][];
    readonly restarts: Omit<NewRestartEvent, "thread_key">[];
    readonly waits: (string | null)[];
  };
}

function createPersistence(): RecordingPersistence {
  const state: RecordingPersistence["state"] = {
    blocks: [],
    sessionIds: [],
    activeTurnSessionIds: [],
    completed: [],
    posted: [],
    pending: [],
    usage: [],
    restarts: [],
    waits: [],
  };
  const record = (write: () => void) => Effect.sync(write);
  return {
    state,
    epoch: 1,
    createPendingResponse: (_chatId, messageId) =>
      Effect.sync(() => {
        state.pending.push(String(messageId));
        return `pending-${state.pending.length}`;
      }),
    markPendingAsPosted: (id) => record(() => state.posted.push(id)),
    updateActiveTurnPendingResponseId: () => Effect.void,
    updatePendingSessionId: (_id, sessionId) => record(() => state.sessionIds.push(sessionId)),
    updateActiveTurnSessionId: (sessionId) => record(() => state.activeTurnSessionIds.push(sessionId)),
    appendBlocksToPending: (_id, blocks) => record(() => state.blocks.push(...blocks)),
    markPendingResponseComplete: (id) => record(() => state.completed.push(id)),
    updateSessionUsage: (sessionId, turnUsage) => record(() => state.usage.push([sessionId, turnUsage])),
    setWaiting: (waitingOn) => record(() => state.waits.push(waitingOn)),
    recordRestartEvent: (event) => record(() => state.restarts.push(event)),
  };
}

const quietSessions: Pick<ClaudeSessionApi, "sessionExists"> = {
  sessionExists: () => Effect.succeed(false),
};

/** The text blocks of a response, in order. */
function textBlocks(blocks: readonly ResponseBlock[]): TextResponseBlock[] {
  return blocks.filter((block): block is TextResponseBlock => block.type === "text");
}

/** The running turns, as alasio keeps them, for a test's live sessions to register theirs in. */
function makeActiveTurns(): ActiveTurns["Service"] {
  return Effect.runSync(Effect.provide(ActiveTurns, ActiveTurns.layer));
}

/** Whether a turn runs for `threadKey`. */
const busy = (activeTurns: ActiveTurns["Service"], threadKey: string): boolean => Effect.runSync(activeTurns.isBusy(threadKey));

/** The turn running for `threadKey`, which there must be, as the operator's controls reach it. */
function runningTurnOf(activeTurns: ActiveTurns["Service"], threadKey: string) {
  const running = Effect.runSync(activeTurns.get(threadKey));
  assert.ok(Option.isSome(running), `a turn is running for ${threadKey}`);
  const turn = running.value;
  return {
    cliInitiated: turn.cliInitiated,
    steer: (prompt: string) => Effect.runPromise(turn.steer(prompt)),
    stop: () => Effect.runPromise(turn.stop("interrupt")),
  };
}

/** What a query's abort controller signals; alasio gives every query one. */
function abortSignalOf(options: Options): AbortSignal {
  assert.ok(options.abortController, "the query's abort controller");
  return options.abortController.signal;
}

/** A folder conversation's bayma, as the deployment's host profile would give it. */
const folderBayma = ({ threadKey }: Pick<HostBaymaScope, "threadKey">) =>
  Effect.succeed<BaymaMcpServer>({
    type: "http",
    url: `http://bayma-${threadKey.replace(/\W/gu, "")}.alasio-host.svc:7290/mcp`,
    headers: { Authorization: "Bearer t" },
  });

/** A turn for runClaudeTurn: the turn, what its live sessions are made with, and the running turns they register in. */
interface ClaudeTurnRun extends Omit<TurnParams, "modelChoice" | "ownTurns"> {
  readonly sessions: Pick<ClaudeSessionApi, "sessionExists">;
  readonly queryFactory: ClaudeQueryFactory;
  readonly activeTurns?: ActiveTurns["Service"];
}

/** One turn on a throwaway live-session registry, closed once the turn returns. */
async function runClaudeTurn({ sessions, queryFactory, activeTurns = makeActiveTurns(), ...params }: ClaudeTurnRun): Promise<TurnResult> {
  return await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const liveSessions = yield* makeClaudeLiveSessions({ workingDirectory: params.workingDirectory, sessions, queryFactory, folderBayma });
    return yield* liveSessions.runTurn({ modelChoice: null, ownTurns: ownTurnsOn(params.persistence), ...params });
  })).pipe(Effect.provideService(ActiveTurns, activeTurns)));
}

/** Live sessions kept open until `closeAll`, as a harness keeps them from its making to its shutdown. */
function openLiveSessions(options: ClaudeLiveSessionsOptions, activeTurns: ActiveTurns["Service"]): { readonly liveSessions: ClaudeLiveSessions; readonly closeAll: () => Promise<void> } {
  const scope = Scope.makeUnsafe();
  const liveSessions = Effect.runSync(makeClaudeLiveSessions(options).pipe(Scope.provide(scope), Effect.provideService(ActiveTurns, activeTurns)));
  return { liveSessions, closeAll: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

/** A turn on live sessions, as the harness runs one. */
const runTurn = (liveSessions: ClaudeLiveSessions, params: TurnParams): Promise<TurnResult> => Effect.runPromise(liveSessions.runTurn(params));

async function* drainPromptChannel(iterable: AsyncIterable<SDKUserMessage>, seen: SDKUserMessage[]) {
  for await (const message of iterable) {
    seen.push(message);
  }
}

/** The bayma exec call Claude Code is about to make, as its PreToolUse hook sees it. */
function execHookInput(code: string, toolUseId: string): PreToolUseHookInput {
  return {
    hook_event_name: "PreToolUse",
    session_id: "s-1",
    transcript_path: "/home/op/.claude/projects/-work/s-1.jsonl",
    cwd: "/work",
    tool_name: "mcp__bayma__exec",
    tool_input: { session_id: "b1", code },
    tool_use_id: toolUseId,
  };
}

/** What a PreToolUse hook decided, if it decided anything. */
function preToolUseDecision(output: HookJSONOutput | undefined): PreToolUseHookSpecificOutput | undefined {
  const specific = output && "hookSpecificOutput" in output ? output.hookSpecificOutput : undefined;
  return specific?.hookEventName === "PreToolUse" ? specific : undefined;
}

/** Whether `pending` settles within 20ms. */
async function settlesSoon(pending: Promise<unknown>): Promise<boolean> {
  return await Promise.race([
    pending.then(() => true),
    new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 20)),
  ]);
}

test("prompt channel delivers pushed messages and completes when ended", async () => {
  const channel = await Effect.runPromise(makePromptChannel);
  const seen: SDKUserMessage[] = [];
  const drained = drainPromptChannel(channel.prompts, seen);
  assert.equal(await Effect.runPromise(channel.push(buildClaudeUserMessage("first"))), true);
  await Effect.runPromise(channel.push(buildClaudeUserMessage("second")));
  assert.equal(await Effect.runPromise(channel.end), true);
  await drained.next();
  assert.deepEqual(seen.map((message) => message.message.content), ["first", "second"]);
  assert.equal(await Effect.runPromise(channel.push(buildClaudeUserMessage("late"))), false);
  assert.equal(await Effect.runPromise(channel.end), false, "a channel ends once");
  assert.equal(buildClaudeUserMessage("x").origin?.kind, "human");
});

test("tool use blocks project into Codex-shaped items", () => {
  assert.deepEqual(projectToolUseToItem(toolUse("t1", "Bash", { command: "ls -la" })), {
    type: "command_execution",
    id: "t1",
    command: "ls -la",
  });
  assert.equal(projectToolUseToItem(toolUse("t2", "Edit", { file_path: "/x" }))?.type, "file_change");
  assert.equal(projectToolUseToItem(toolUse("t3", "WebSearch", {}))?.type, "web_search");
  assert.deepEqual(parseMcpToolName("mcp__bayma__exec"), { server: "bayma", tool: "exec" });
  assert.deepEqual(projectToolUseToItem(toolUse("t4", "mcp__bayma__exec", { code: "1" })), {
    type: "mcp_tool_call",
    id: "t4",
    server: "bayma",
    tool: "exec",
    arguments: { code: "1" },
  });
  const ask = projectToolUseToItem(toolUse("t5", "AskUserQuestion", { questions: [{ header: "Scope" }] }));
  assert.ok(ask?.type === "mcp_tool_call");
  assert.equal(ask.tool, "AskUserQuestion");
  assert.deepEqual(ask.arguments, { questions: [{ header: "Scope" }] });
});

test("assistant text stays commentary and subagent messages are ignored", () => {
  const items = projectAssistantMessageToItems(assistantMessage([
    text("Looking around."),
    toolUse("t1", "Bash", { command: "pwd" }),
  ]));
  assert.deepEqual(items.map((item) => item.type), ["agent_message", "command_execution"]);
  const [commentary] = items;
  assert.ok(commentary?.type === "agent_message");
  assert.equal(commentary.phase, "commentary");
  assert.deepEqual(projectAssistantMessageToItems(assistantMessage([text("inner")], { parent_tool_use_id: "task-1" })), []);
});

test("result projection separates final answers from failures", () => {
  const answered = successResult({ result: "Done.", session_id: "s-1", usage: usage({ cache_read_input_tokens: 5 }) });
  assert.deepEqual(projectResultMessage(answered), {
    ok: true,
    text: "Done.",
    usage: answered.usage,
  });
  const failure = projectResultMessage(errorResult({ subtype: "error_during_execution", errors: ["boom"], session_id: "s-1" }));
  assert.ok(failure?.ok === false);
  assert.equal(failure.error, "boom");
  const apiFailure = projectResultMessage(successResult({ result: "rate limited", is_error: true, session_id: "s-1" }));
  assert.ok(apiFailure?.ok === false);
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
  assert.deepEqual(Object.keys(reserved.mcpServers ?? {}), ["a"]);
});

test("with a session store, a query mirrors every transcript write as it is written but resumes from the local transcript", async () => {
  const appended: [SessionKey, SessionStoreEntry[]][] = [];
  const store: SessionStore = {
    append: async (key, entries) => {
      appended.push([key, entries]);
    },
    load: async () => [{ type: "user" }],
  };
  const options = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: {}, resumeSession: "abc", resumeExists: true, controller: new AbortController(), hooks: {}, env: {}, sessionStore: store });
  assert.equal(options.persistSession, true);
  assert.equal(options.sessionStoreFlush, "eager");
  const mirror = options.sessionStore;
  assert.ok(mirror, "the query mirrors to a store");
  await mirror.append({ projectKey: "p", sessionId: "abc" }, [{ type: "user" }]);
  assert.equal(appended.length, 1);
  assert.equal(await mirror.load({ projectKey: "p", sessionId: "abc" }), null);
  const withoutStore = buildClaudeQueryOptions({ workingDirectory: "/w", claudeEnv: {}, mcpServers: {}, controller: new AbortController(), hooks: {}, env: {} });
  assert.equal(withoutStore.sessionStore, undefined);
});

test("a result for another turn does not end the prompt channel", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  let promptUuid: string | null = null;
  let channelOpenWhenForeignResultSeen: boolean | null = null;
  const queryFactory: ClaudeQueryFactory = ({ prompt }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const iterator = prompt[Symbol.asyncIterator]();
    const ours = (await readPrompt(iterator)).uuid;
    promptUuid = ours;
    yield initMessage("s1");
    // A resumed session re-runs the turn a previous worker left interrupted;
    // its result names that turn's prompt, not ours.
    yield successResult({
      result: "Finished the interrupted turn.",
      session_id: "s1",
      user_message_uuid: "someone-elses-turn",
      user_message_uuids: ["someone-elses-turn"],
      resume_reason: "interrupted_turn",
    });
    // If the channel had been closed on that result, this would never run:
    // the next read would report done, and a pending tool call would be
    // cancelled.
    const pending = iterator.next();
    channelOpenWhenForeignResultSeen = !(await settlesSoon(pending));
    yield assistantMessage([toolUse("t1", "Bash", { command: "echo hi" })]);
    yield successResult({
      result: "Ours at last.",
      session_id: "s1",
      user_message_uuid: ours,
      user_message_uuids: [ours],
    });
    const closing = await iterator.next();
    assert.equal(closing.done, true);
  })());
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "s1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "2",
    workingDirectory: "/work",
    persistence,
    activeTurns,
    sessions: { sessionExists: () => Effect.succeed(true) },
    queryFactory,
  });
  assert.equal(typeof promptUuid, "string", "the pushed prompt carries a client uuid");
  assert.equal(channelOpenWhenForeignResultSeen, true, "the channel stays open through a foreign result");
  assert.equal(result.responseCompleted, true);
  assert.equal(
    textBlocks(result.blockSequence).at(-1)?.content,
    "Ours at last.",
    "the turn ends on the result that answers our prompt",
  );
});

test("a steered prompt keeps the channel open until its own result arrives", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  let closedBeforeSteerAnswered: boolean | null = null;
  const queryFactory: ClaudeQueryFactory = ({ prompt }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const iterator = prompt[Symbol.asyncIterator]();
    const own = (await readPrompt(iterator)).uuid;
    yield initMessage("s1");
    // Steer arrives while the turn is running.
    await runningTurnOf(activeTurns, "telegram:1").steer("also do this");
    const steered = (await readPrompt(iterator)).uuid;
    // The CLI answers only the original prompt first.
    yield successResult({
      result: "First answer.",
      session_id: "s1",
      user_message_uuid: own,
      user_message_uuids: [own],
    });
    const pending = iterator.next();
    closedBeforeSteerAnswered = await settlesSoon(pending);
    yield successResult({
      result: "Steered answer.",
      session_id: "s1",
      user_message_uuid: steered,
      user_message_uuids: [steered],
    });
    assert.equal((await iterator.next()).done, true);
  })());
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "s1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "2",
    workingDirectory: "/work",
    persistence,
    activeTurns,
    sessions: { sessionExists: () => Effect.succeed(true) },
    queryFactory,
  });
  assert.equal(closedBeforeSteerAnswered, false, "the channel stays open for the steered prompt");
  assert.equal(
    textBlocks(result.blockSequence).at(-1)?.content,
    "Steered answer.",
    "the newest matching result is the one posted",
  );
});

test("Claude turn persists session identity, tool blocks, and the result as final answer", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const seenPrompts: SDKUserMessage["message"]["content"][] = [];
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => {
    assert.equal(options.cwd, "/work");
    assert.equal(options.sessionId, "reserved-1");
    return fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
      const iterator = prompt[Symbol.asyncIterator]();
      const first = await readPrompt(iterator);
      seenPrompts.push(first.message.content);
      yield initMessage("reserved-1");
      yield assistantMessage([text("Thinking out loud."), toolUse("t1", "Bash", { command: "echo hi" })]);
      yield successResult({ result: "All done.", session_id: "reserved-1", usage: usage({ cache_read_input_tokens: 42 }), user_message_uuids: [first.uuid] });
      const closing = await iterator.next();
      assert.equal(closing.done, true);
    })());
  };
  const result = await runClaudeTurn({
    prompt: "do it",
    resumeSession: "reserved-1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "1",
    workingDirectory: "/work",
    persistence,
    activeTurns,
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
  assert.equal(busy(activeTurns, "telegram:1"), false);
});

test("a result from a turn the CLI started itself does not end the operator's turn", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  let channelOpenAfterNotificationResult: boolean | null = null;
  const queryFactory: ClaudeQueryFactory = ({ prompt }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const iterator = prompt[Symbol.asyncIterator]();
    const first = await readPrompt(iterator);
    yield initMessage("s-7");
    // The CLI answers a background task's stop notice before the operator's prompt.
    yield successResult({ result: "Noted the stopped task.", session_id: "s-7" });
    const pending = iterator.next();
    channelOpenAfterNotificationResult = !(await settlesSoon(pending));
    yield successResult({ result: "The real answer.", session_id: "s-7", user_message_uuids: [first.uuid] });
    assert.equal((await pending).done, true);
  })());
  const result = await runClaudeTurn({
    prompt: "check now",
    resumeSession: null,
    threadKey: "telegram:7",
    chatId: "7",
    messageId: "7",
    workingDirectory: "/work",
    persistence,
    activeTurns,
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(channelOpenAfterNotificationResult, true, "the notice's result must not close the prompt channel");
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "The real answer.");
});

test("Claude turn interruption is classified as operator control and steering pushes guidance", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const pushed: SDKUserMessage["message"]["content"][] = [];
  const { promise: steerSeen, resolve: releaseSteer } = Promise.withResolvers<void>();
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const iterator = prompt[Symbol.asyncIterator]();
    await iterator.next();
    yield initMessage("s-9");
    const steer = await readPrompt(iterator);
    pushed.push(steer.message.content);
    releaseSteer();
    await new Promise((_resolve, reject) => {
      abortSignalOf(options).addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    });
  })());
  const turn = runClaudeTurn({
    prompt: "long task",
    resumeSession: null,
    threadKey: "telegram:2",
    chatId: "2",
    messageId: "2",
    workingDirectory: "/work",
    persistence,
    activeTurns,
    sessions: quietSessions,
    queryFactory,
  });
  while (!busy(activeTurns, "telegram:2")) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const running = runningTurnOf(activeTurns, "telegram:2");
  assert.equal(await running.steer("focus on tests"), true);
  await steerSeen;
  await running.stop();
  const result = await turn;
  assert.deepEqual(pushed, ["focus on tests"]);
  assert.equal(result.interrupted, true);
  assert.equal(result.responseCompleted, false);
  assert.deepEqual(result.blockSequence, []);
  assert.equal(busy(activeTurns, "telegram:2"), false);
});

test("Claude turn surfaces failures and non-operator aborts as errors", async () => {
  const persistence = createPersistence();
  const queryFactory: ClaudeQueryFactory = ({ prompt }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const first = await readPrompt(prompt[Symbol.asyncIterator]());
    yield initMessage("s-3");
    yield errorResult({ subtype: "error_max_turns", errors: ["max turns"], session_id: "s-3", user_message_uuids: [first.uuid] });
  })());
  const result = await runClaudeTurn({
    prompt: "x",
    resumeSession: null,
    threadKey: "telegram:3",
    chatId: "3",
    messageId: "3",
    workingDirectory: "/work",
    persistence,
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(result.responseCompleted, false);
  assert.deepEqual(result.blockSequence, [{ type: "text", content: "Error: max turns" }]);
});

test("Bash, Monitor, Grep and Glob are removed and bayma exec code passes the database guardrail", async () => {
  const persistence = createPersistence();
  let denied: HookJSONOutput | undefined;
  let allowed: HookJSONOutput | undefined;
  let disallowed: string[] | undefined;
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => {
    disallowed = options.disallowedTools;
    return fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
      const first = await readPrompt(prompt[Symbol.asyncIterator]());
      yield initMessage("s-4");
      const preToolUse = options.hooks?.PreToolUse;
      assert.ok(preToolUse, "a PreToolUse hook");
      const [matcher] = preToolUse;
      assert.equal(preToolUse.length, 1);
      assert.equal(matcher?.matcher, "mcp__bayma__exec");
      const [hook] = matcher.hooks;
      assert.ok(hook, "the hook's callback");
      const { signal } = new AbortController();
      denied = await hook(execHookInput('import { $ } from "bun";\nawait $`psql -c "drop table users"`', "t1"), "t1", { signal });
      allowed = await hook(execHookInput("await $`ls -la`", "t2"), "t2", { signal });
      yield successResult({ result: "Skipped the drop.", session_id: "s-4", user_message_uuids: [first.uuid] });
    })());
  };
  const result = await runClaudeTurn({
    prompt: "drop it",
    resumeSession: null,
    threadKey: "telegram:4",
    chatId: "4",
    messageId: "4",
    workingDirectory: "/work",
    persistence,
    sessions: quietSessions,
    queryFactory,
  });
  assert.deepEqual(disallowed, ["Bash", "Monitor", "Grep", "Glob"]);
  const decision = preToolUseDecision(denied);
  assert.equal(decision?.permissionDecision, "deny");
  assert.match(decision?.permissionDecisionReason ?? "", /.+/);
  assert.deepEqual(allowed, {});
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "Skipped the drop.");
});

test("a restart through a Bun shell in bayma exec records self-induced provenance once", async () => {
  const persistence = createPersistence();
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
    const first = await readPrompt(prompt[Symbol.asyncIterator]());
    yield initMessage("s-5");
    const hook = options.hooks?.PreToolUse?.[0]?.hooks[0];
    assert.ok(hook, "the bayma exec hook");
    await hook(execHookInput("await $`kubectl rollout restart deployment/alasio`", "t1"), "t1", { signal: new AbortController().signal });
    yield successResult({ result: "Restarting.", session_id: "s-5", user_message_uuids: [first.uuid] });
  })());
  await runClaudeTurn({
    prompt: "restart yourself",
    resumeSession: null,
    threadKey: "telegram:5",
    chatId: "5",
    messageId: "5",
    workingDirectory: "/work",
    persistence,
    sessions: quietSessions,
    queryFactory,
  });
  assert.equal(persistence.state.restarts.length, 1);
  assert.equal(persistence.state.restarts[0]?.cause, "self_induced");
});

test("Claude session api maps SDK transcripts to alasio session and rewind shapes", async () => {
  // Transcript entries carry the time they were written, which the SDK's type leaves out.
  const messages: (SessionMessage & { readonly timestamp?: string })[] = [
    { type: "user", uuid: "u1", session_id: "new", parent_tool_use_id: null, parent_agent_id: null, timestamp: "2026-09-21T10:00:00Z", message: { role: "user", content: "first ask" } },
    { type: "assistant", uuid: "a1", session_id: "new", parent_tool_use_id: null, parent_agent_id: null, message: { role: "assistant", content: [{ type: "text", text: "first answer" }] } },
    { type: "user", uuid: "u2", session_id: "new", parent_tool_use_id: null, parent_agent_id: null, message: { role: "user", content: [{ type: "tool_result", tool_use_id: "t" }] } },
    { type: "user", uuid: "u3", session_id: "new", parent_tool_use_id: null, parent_agent_id: null, timestamp: "2026-09-21T10:05:00Z", message: { role: "user", content: [{ type: "text", text: "second ask" }] } },
    { type: "assistant", uuid: "a2", session_id: "new", parent_tool_use_id: null, parent_agent_id: null, message: { role: "assistant", content: [{ type: "text", text: "second answer" }] } },
  ];
  const forks: [string, ForkSessionOptions | undefined][] = [];
  const api = createClaudeSessionApi({
    workingDirectory: "/work",
    sdk: {
      async listSessions(options) {
        assert.equal(options?.dir, "/work");
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
        return sessionId === "new" ? { sessionId, summary: "", lastModified: 0 } : undefined;
      },
    },
  });
  assert.deepEqual(await Effect.runPromise(api.listSessions(1)), [
    { uuid: "new", timestamp: "1970-01-01", label: "Newest work" },
    { uuid: "old", timestamp: "1970-01-01", label: "Old" },
  ]);
  assert.equal(await Effect.runPromise(api.getTotalSessionPages()), 1);
  assert.equal(await Effect.runPromise(api.getSessionByNumber(2)), "old");
  assert.equal(await Effect.runPromise(api.getSessionByNumber(3)), null);
  assert.equal(await Effect.runPromise(api.getSessionLastMessage("new")), "second answer");
  assert.deepEqual((await Effect.runPromise(api.listSessionMessages("new"))).map((message) => [message.index, message.uuid, message.text]), [
    [-1, "u3", "second ask"],
    [-2, "u1", "first ask"],
  ]);
  assert.equal(await Effect.runPromise(api.getTotalRewindPages("new")), 1);
  assert.equal(await Effect.runPromise(api.createForkedSession("new", "u3")), "forked");
  assert.deepEqual(forks, [["new", { dir: "/work", upToMessageId: "u2" }]]);
  assert.match(await Effect.runPromise(api.createForkedSession("new", "u1")) ?? "", /^[0-9a-f-]{36}$/);
  assert.equal(await Effect.runPromise(api.createForkedSession("new", "missing")), null);
  assert.equal(await Effect.runPromise(api.sessionExists("new")), true);
  assert.equal(await Effect.runPromise(api.sessionExists("old")), false);
});

test("Claude session api reads the session store, and falls back to local transcripts when it cannot", async () => {
  const calls: [string, boolean][] = [];
  let storeUp = true;
  const unread = () => assert.fail("a store holding no transcripts is never read");
  const store: ClaudeTranscriptStore = {
    // Holds no transcripts; unreachable once storeUp is false.
    async projectKeyOf() {
      if (!storeUp) throw new Error("store unreachable");
      return null;
    },
    append: unread,
    load: unread,
    listSubkeys: unread,
  };
  const api = createClaudeSessionApi({
    workingDirectory: "/work",
    store,
    sdk: {
      async listSessions(options) {
        calls.push(["list", Boolean(options?.sessionStore)]);
        if (options?.sessionStore && !storeUp) throw new Error("store unreachable");
        return [{ sessionId: "kept", lastModified: 1, summary: "Kept" }];
      },
      async getSessionMessages() {
        return [];
      },
      forkSession: () => assert.fail("nothing is forked here"),
      async getSessionInfo(sessionId, options) {
        calls.push(["info", Boolean(options?.sessionStore)]);
        return sessionId === "local-only" ? { sessionId, summary: "", lastModified: 0 } : undefined;
      },
    },
  });
  // Listing reads the store.
  assert.equal(await Effect.runPromise(api.getSessionByNumber(1)), "kept");
  assert.deepEqual(calls.splice(0), [["list", true]]);
  // With the store unreachable, listing and existence fall back to the local transcripts.
  storeUp = false;
  assert.equal(await Effect.runPromise(api.getSessionByNumber(1)), "kept");
  assert.deepEqual(calls.splice(0), [["list", true], ["list", false]]);
  assert.equal(await Effect.runPromise(api.sessionExists("local-only")), true);
  assert.deepEqual(calls.splice(0), [["info", false]]);
});

/** What a fake CLI has done: processes started and closed, interrupts, and each process's options. */
interface FakeCliState {
  created: number;
  closed: number;
  interrupts: number;
  /** How many of the prompts it received the test has read. */
  consumed: number;
  readonly options: Options[];
}

/**
 * A scriptable stand-in for a long-lived Claude Code process: the test reads
 * the prompts it receives and decides what it streams back.
 */
function createFakeCli() {
  const outbox: SDKMessage[] = [];
  let wake: (() => void) | null = null;
  const prompts: SDKUserMessage[] = [];
  const promptWaiters: ((message: SDKUserMessage) => void)[] = [];
  const state: FakeCliState = { created: 0, closed: 0, interrupts: 0, consumed: 0, options: [] };
  function emit(message: SDKMessage): void {
    outbox.push(message);
    wake?.();
  }
  function nextPrompt(): Promise<StampedPrompt> {
    if (prompts.length > state.consumed) {
      return Promise.resolve(stamped(prompts[state.consumed++]));
    }
    return new Promise<SDKUserMessage>((resolve) => promptWaiters.push(resolve)).then(stamped);
  }
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => {
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
    abortSignalOf(options).addEventListener("abort", () => {
      closed = true;
      wake?.();
    });
    const generator = (async function* run(): AsyncGenerator<SDKMessage, void> {
      while (true) {
        while (outbox.length > 0) {
          const message = outbox.shift();
          if (message) {
            yield message;
          }
        }
        if (closed) {
          return;
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    })();
    return fakeQuery(generator, {
      close: () => {
        state.closed += 1;
        closed = true;
        wake?.();
      },
      interrupt: async () => {
        state.interrupts += 1;
        return undefined;
      },
    });
  };
  return { queryFactory, emit, nextPrompt, prompts, state };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 5));

function turnParams(persistence: TurnPersistence, extra: Partial<TurnParams> = {}): TurnParams {
  return {
    prompt: "go",
    resumeSession: "s-1",
    threadKey: "telegram:1",
    chatId: "1",
    messageId: "1",
    workingDirectory: "/work",
    modelChoice: null,
    persistence,
    ownTurns: ownTurnsOn(persistence),
    ...extra,
  };
}

test("Claude Code's background tasks are work of its own between turns, but its ambient ones, which are no activity", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, turnParams(persistence));
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Done.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;
  assert.equal(liveSessions.backgroundWork("telegram:1"), false);
  // A live-update watcher alone: ambient, no activity.
  cli.emit(backgroundTasksChanged([{ task_id: "w1", task_type: "monitor", description: "watch the PR", ambient: true }]));
  await settle();
  assert.equal(liveSessions.backgroundWork("telegram:1"), false);
  cli.emit(backgroundTasksChanged([{ task_id: "w1", task_type: "monitor", description: "watch the PR", ambient: true }, { task_id: "b1", task_type: "local_bash", description: "bun run test" }]));
  await settle();
  assert.equal(liveSessions.backgroundWork("telegram:1"), true);
  assert.equal(liveSessions.backgroundWork("telegram:2"), false);
  await closeAll();
});

test("background work keeps running after the answer and its report is delivered as its own reply", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const events: string[] = [];
  const turn = runTurn(liveSessions, {
    ...turnParams(persistence),
    onBackgroundResponse: Effect.sync(() => events.push("background-response")),
    onIdle: Effect.sync(() => events.push("idle")),
  });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(assistantMessage([toolUse("t1", "Bash", { command: "bun run test", run_in_background: true })]));
  cli.emit(backgroundTasksChanged([{ task_id: "b1", task_type: "local_bash", description: "bun run test" }]));
  cli.emit(successResult({ result: "Tests are running; I'll report back.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  const result = await turn;
  assert.equal(result.responseCompleted, true);
  assert.equal(finalResponseToMarkdown(result.blockSequence), "Tests are running; I'll report back.");
  assert.equal(cli.state.closed, 0, "the answer does not end the process");
  assert.equal(busy(activeTurns, "telegram:1"), false, "the conversation is free once answered");

  // The task settles and Claude Code starts a turn of its own to report it.
  cli.emit(backgroundTasksChanged([]));
  cli.emit(assistantMessage([text("Checking the log.")]));
  await settle();
  assert.equal(runningTurnOf(activeTurns, "telegram:1").cliInitiated, true, "the report holds the conversation busy");
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  assert.equal(busy(activeTurns, "telegram:1"), false);
  assert.deepEqual(persistence.state.completed, ["pending-1", "pending-2"]);
  assert.match(persistence.state.pending[1] ?? "", /^claude-cli-turn:/);
  assert.equal(persistence.state.blocks.filter((block) => block["phase"] === "final_answer").at(-1)?.["content"], "All 212 tests passed.");
  assert.deepEqual(events.filter((event) => event === "background-response"), ["background-response"]);
  assert.ok(events.includes("idle"));
  await closeAll();
  assert.equal(cli.state.closed, 1);
});

test("a report Claude Code starts while another turn holds the conversation waits for it to let go, then claims the conversation itself and is delivered", async () => {
  const persistence = createPersistence();
  // The turns Claude Code starts on its own write under claims of their own.
  const own = createPersistence();
  const ended: string[] = [];
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const events: string[] = [];
  const turn = runTurn(liveSessions, {
    ...turnParams(persistence),
    ownTurns: ownTurnsOn(own, ended),
    onBackgroundResponse: Effect.sync(() => events.push("background-response")),
  });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Tests are running; I'll report back.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  // Another turn holds the conversation as Claude Code reports.
  const elsewhere = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(elsewhere)));
  cli.emit(assistantMessage([text("Checking the log.")]));
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  assert.deepEqual([own.state.pending, ended, events], [[], [], []], "the report claimed a conversation another turn holds");
  assert.equal(runningTurnOf(activeTurns, "telegram:1").cliInitiated, false);

  // It lets go: the report claims the conversation, and is delivered under its claim alone.
  await Effect.runPromise(Scope.close(elsewhere, Exit.void));
  await new Promise((resolve) => setTimeout(resolve, 1500));
  assert.equal(own.state.pending.length, 1);
  assert.match(own.state.pending[0] ?? "", /^claude-cli-turn:/);
  assert.deepEqual(own.state.completed, ["pending-1"]);
  assert.deepEqual(textBlocks(own.state.blocks as ResponseBlock[]).map(({ content }) => content), ["Checking the log.", "All 212 tests passed."]);
  assert.equal(persistence.state.pending.length, 1, "the report wrote under the operator's turn");
  assert.deepEqual(ended, ["completed"]);
  assert.deepEqual(events, ["background-response"]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a turn Claude Code starts while the turn before it still lets go of the conversation holds it once that turn has, while it runs, so a prompt then finds it busy", async () => {
  const persistence = createPersistence();
  const own = createPersistence();
  const ended: string[] = [];
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(own, ended) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Tests are running; I'll report back.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  // The operator's turn, its answer given, still holds the conversation as alasio lets go
  // of it, as Claude Code begins a turn of its own.
  const lettingGo = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(lettingGo)));
  cli.emit(assistantMessage([text("Checking the log.")]));
  await settle();
  assert.equal(runningTurnOf(activeTurns, "telegram:1").cliInitiated, false);
  assert.deepEqual(own.state.pending, []);

  // Once it has let go, Claude Code's turn, running still, holds the conversation and has claimed it.
  await Effect.runPromise(Scope.close(lettingGo, Exit.void));
  await settle();
  assert.equal(busy(activeTurns, "telegram:1"), true, "a prompt now finds the conversation busy");
  assert.equal(runningTurnOf(activeTurns, "telegram:1").cliInitiated, true);
  assert.equal(own.state.pending.length, 1);

  // It ends, delivered under its claim, and lets go.
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  await settle();
  assert.deepEqual(own.state.completed, ["pending-1"]);
  assert.deepEqual(ended, ["completed"]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a turn Claude Code starts whose claim's first write of its blocks is slow delivers its reply after that write, each block once and in order", async () => {
  const persistence = createPersistence();
  const own = createPersistence();
  const ended: string[] = [];
  // Its first write of blocks, its claim's, waits until the test lets it go on.
  const slowWrite = Effect.runSync(Deferred.make<void>());
  let writes = 0;
  const slow: TurnPersistence = {
    ...own,
    appendBlocksToPending: (id, blocks) => ((writes += 1) === 1 ? Deferred.await(slowWrite) : Effect.void).pipe(Effect.andThen(own.appendBlocksToPending(id, blocks))),
  };
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(slow, ended) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Tests are running.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  const lettingGo = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(lettingGo)));
  cli.emit(assistantMessage([text("Checking the log.")]));
  await settle();
  // The turn before lets go: Claude Code's claims the conversation, its first write held.
  await Effect.runPromise(Scope.close(lettingGo, Exit.void));
  await settle();
  assert.equal(writes, 1);
  // Meanwhile it says more, and ends.
  cli.emit(assistantMessage([text("All green.")]));
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  assert.deepEqual(own.state.completed, [], "nothing is delivered while its claim's first write is in flight");
  Effect.runSync(Deferred.succeed(slowWrite, undefined));
  for (let waited = 0; ended.length === 0 && waited < 2000; waited += 20) await settle();
  assert.deepEqual(textBlocks(own.state.blocks as ResponseBlock[]).map(({ content }) => content), ["Checking the log.", "All green.", "All 212 tests passed."]);
  assert.deepEqual([own.state.completed, ended], [["pending-1"], ["completed"]]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a turn Claude Code starts while a fork holds its conversation waits, unclaimed, until the hold is let go of, then claims it and delivers its reply", async () => {
  const persistence = createPersistence();
  const own = createPersistence();
  const ended: string[] = [];
  // A fork's hold, a row of the store, which the CLI turn asks of through its own turns.
  let held = true;
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(own, ended, Effect.sync(() => held)) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Tests are running.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  cli.emit(assistantMessage([text("Checking the log.")]));
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  // Neither running in the conversation nor claiming it, under the hold.
  assert.deepEqual([busy(activeTurns, "telegram:1"), own.state.blocks, ended], [false, [], []]);
  held = false;
  for (let waited = 0; ended.length === 0 && waited < 4000; waited += 20) await settle();
  assert.deepEqual(textBlocks(own.state.blocks as ResponseBlock[]).map(({ content }) => content), ["Checking the log.", "All 212 tests passed."]);
  assert.deepEqual([own.state.completed, ended], [["pending-1"], ["completed"]]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a turn Claude Code starts that is stopped as it claims its conversation ends that claim", async () => {
  const persistence = createPersistence();
  const own = createPersistence();
  const ended: string[] = [];
  // Its claim of the conversation is made once the test lets it.
  const slowClaim = Effect.runSync(Deferred.make<void>());
  const claiming = Effect.runSync(Deferred.make<void>());
  const ownTurns = ownTurnsOn(own, ended);
  const slowly: typeof ownTurns = { ...ownTurns, claim: (messageId, sessionId) => Deferred.succeed(claiming, undefined).pipe(Effect.andThen(Deferred.await(slowClaim)), Effect.andThen(ownTurns.claim(messageId, sessionId))) };
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: slowly });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Tests are running.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  const lettingGo = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(lettingGo)));
  cli.emit(assistantMessage([text("Checking the log.")]));
  await settle();
  await Effect.runPromise(Scope.close(lettingGo, Exit.void));
  await Effect.runPromise(Deferred.await(claiming));
  // Stopped as it claims: the claim is made, and ended.
  const stopping = runningTurnOf(activeTurns, "telegram:1").stop();
  await settle();
  Effect.runSync(Deferred.succeed(slowClaim, undefined));
  await stopping;
  for (let waited = 0; ended.length === 0 && waited < 2000; waited += 20) await settle();
  assert.deepEqual(ended, ["interrupted"]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("reports Claude Code starts while another turn holds the conversation are delivered in the order it made them", async () => {
  const persistence = createPersistence();
  const own = createPersistence();
  const ended: string[] = [];
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(own, ended) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Started both.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;

  const elsewhere = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(elsewhere)));
  cli.emit(assistantMessage([text("The build is done.")]));
  cli.emit(successResult({ result: "Build passed.", session_id: "s-1" }));
  await settle();
  cli.emit(assistantMessage([text("The tests are done.")]));
  cli.emit(successResult({ result: "Tests passed.", session_id: "s-1" }));
  await settle();
  assert.deepEqual(own.state.pending, []);

  await Effect.runPromise(Scope.close(elsewhere, Exit.void));
  for (let waited = 0; ended.length < 2 && waited < 2000; waited += 20) await settle();
  assert.deepEqual(textBlocks(own.state.blocks as ResponseBlock[]).map(({ content }) => content), ["The build is done.", "Build passed.", "The tests are done.", "Tests passed."]);
  assert.deepEqual([own.state.completed, ended], [["pending-1", "pending-2"], ["completed", "completed"]]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a report whose claim is made but whose response cannot be begun ends its claim, and lets go of the conversation", async () => {
  const persistence = createPersistence();
  const ended: string[] = [];
  const unwritable: TurnPersistence = { ...createPersistence(), createPendingResponse: () => Effect.fail(new StoreError({ cause: new Error("the store is down") })) };
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(unwritable, ended) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Started it.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;
  cli.emit(assistantMessage([text("Reporting.")]));
  for (let waited = 0; ended.length === 0 && waited < 2000; waited += 20) await settle();
  assert.deepEqual(ended, ["failed"]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a report whose claim is made but whose blocks so far cannot be stored ends its claim, and lets go of the conversation", async () => {
  const persistence = createPersistence();
  const ended: string[] = [];
  const unwritable: TurnPersistence = { ...createPersistence(), appendBlocksToPending: () => Effect.fail(new StoreError({ cause: new Error("the store is down") })) };
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, { ...turnParams(persistence), ownTurns: ownTurnsOn(unwritable, ended) });
  const first = await cli.nextPrompt();
  cli.emit(initMessage("s-1"));
  cli.emit(successResult({ result: "Started it.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;
  // It ends while another turn holds the conversation, so it claims it with its blocks once it is let go.
  const elsewhere = Effect.runSync(Scope.make());
  Effect.runSync(activeTurns.register("telegram:1", { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false }).pipe(Scope.provide(elsewhere)));
  cli.emit(assistantMessage([text("Reporting.")]));
  cli.emit(successResult({ result: "Reported.", session_id: "s-1" }));
  await settle();
  await Effect.runPromise(Scope.close(elsewhere, Exit.void));
  for (let waited = 0; ended.length === 0 && waited < 2000; waited += 20) await settle();
  await settle();
  // Ended once, as it failed.
  assert.deepEqual(ended, ["failed"]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a turn holds the pod it works in busy while it runs: an operator's, and one Claude Code starts itself to report", async () => {
  const persistence = createPersistence();
  const holds: Hold[] = [];
  const activeTurns = activeTurnsHolding(holds);
  const cli = createFakeCli();
  const pod = { namespace: "alasio-host", name: "bayma-telegram-1" };
  const workspacePod = (threadKey: string) => (threadKey === "telegram:1" ? pod : null);
  const { liveSessions, closeAll } = openLiveSessions(
    { workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma, workspacePod },
    activeTurns,
  );
  const turn = runTurn(liveSessions, turnParams(persistence));
  const first = await cli.nextPrompt();
  assert.deepEqual(holds, [{ workspace: pod, released: false }]);
  cli.emit(initMessage("s-1"));
  cli.emit(assistantMessage([toolUse("t1", "Bash", { command: "bun run test", run_in_background: true })]));
  cli.emit(backgroundTasksChanged([{ task_id: "b1", task_type: "local_bash", description: "bun run test" }]));
  cli.emit(successResult({ result: "Tests are running.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;
  assert.deepEqual(holds, [{ workspace: pod, released: true }]);

  cli.emit(backgroundTasksChanged([]));
  cli.emit(assistantMessage([text("Checking the log.")]));
  await settle();
  assert.deepEqual(holds.at(-1), { workspace: pod, released: false });
  cli.emit(successResult({ result: "All 212 tests passed.", session_id: "s-1" }));
  await settle();
  assert.deepEqual(holds, [{ workspace: pod, released: true }, { workspace: pod, released: true }]);
  await closeAll();
});

test("later prompts on the same session reuse the live process; a new session or model replaces it", async () => {
  const persistence = createPersistence();
  let model: ModelChoice | null = null;
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const answer = async () => {
    const received = await cli.nextPrompt();
    cli.emit(successResult({ result: `re: ${received.message.content}`, session_id: "s-1", user_message_uuids: [received.uuid] }));
    return received;
  };
  const run = async (prompt: string, extra: Partial<TurnParams> = {}) => {
    const turn = runTurn(liveSessions, turnParams(persistence, { prompt, modelChoice: model, ...extra }));
    await answer();
    return await turn;
  };
  cli.emit(initMessage("s-1"));
  assert.equal(finalResponseToMarkdown((await run("one")).blockSequence), "re: one");
  assert.equal(finalResponseToMarkdown((await run("two")).blockSequence), "re: two");
  assert.equal(cli.state.created, 1, "one process served both prompts");
  assert.equal(cli.state.options[0]?.resume, "s-1");

  model = { model: "claude-haiku-4-5-20251001", effort: null };
  await run("three");
  assert.equal(cli.state.created, 2, "a model change starts a new process");
  assert.equal(cli.state.closed, 1);

  await run("four", { resumeSession: "s-2" });
  assert.equal(cli.state.created, 3, "a different mounted session starts a new process");
  assert.equal(cli.state.options[2]?.resume, "s-2");
  await closeAll();
});

test("steering a Claude-started turn is answered in that turn's own reply", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, turnParams(persistence));
  const first = await cli.nextPrompt();
  cli.emit(successResult({ result: "Started it.", session_id: "s-1", user_message_uuids: [first.uuid] }));
  await turn;
  cli.emit(assistantMessage([text("Build finished, reviewing.")]));
  await settle();
  const cliTurn = runningTurnOf(activeTurns, "telegram:1");
  assert.equal(cliTurn.cliInitiated, true);
  assert.equal(await cliTurn.steer("also summarize warnings"), true);
  const steered = await cli.nextPrompt();
  assert.equal(steered.message.content, "also summarize warnings");
  cli.emit(successResult({ result: "Build is green; 3 warnings.", session_id: "s-1", user_message_uuids: [steered.uuid] }));
  await settle();
  assert.equal(busy(activeTurns, "telegram:1"), false);
  assert.equal(persistence.state.completed.at(-1), "pending-2");
  await closeAll();
});

test("/stop interrupts the turn without killing the process, and the interrupted turn's tail is not a new turn", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, turnParams(persistence));
  const first = await cli.nextPrompt();
  cli.emit(assistantMessage([text("Working...")]));
  await settle();
  await runningTurnOf(activeTurns, "telegram:1").stop();
  const result = await turn;
  assert.equal(result.interrupted, true);
  assert.equal(cli.state.interrupts, 1);
  assert.equal(cli.state.closed, 0);
  // Output the CLI flushes while stopping belongs to the stopped turn.
  cli.emit(assistantMessage([text("(stopping)")]));
  await settle();
  assert.equal(busy(activeTurns, "telegram:1"), false, "the tail does not open a Claude-started turn");
  cli.emit(errorResult({ subtype: "error_during_execution", errors: ["interrupted"], session_id: "s-1", user_message_uuids: [first.uuid] }));
  await settle();
  assert.deepEqual(persistence.state.pending, ["1"], "no reply was created for the tail");
  await closeAll();
});

test("a Claude Code process that exits mid-turn fails that turn and the next prompt starts a fresh one", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: { sessionExists: () => Effect.succeed(true) }, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  const turn = runTurn(liveSessions, turnParams(persistence));
  await cli.nextPrompt();
  const [options] = cli.state.options;
  assert.ok(options?.abortController, "the process's abort controller");
  options.abortController.abort("crash");
  const result = await turn;
  assert.equal(result.responseCompleted, false);
  const last = result.blockSequence.at(-1);
  assert.ok(last?.type === "text");
  assert.match(last.content, /^Error: Claude Code exited before answering/);
  assert.equal(liveSessions.get("telegram:1"), null);
  const next = runTurn(liveSessions, turnParams(persistence, { prompt: "again" }));
  const again = await cli.nextPrompt();
  cli.emit(successResult({ result: "Back.", session_id: "s-1", user_message_uuids: [again.uuid] }));
  assert.equal((await next).responseCompleted, true);
  assert.equal(cli.state.created, 2);
  await closeAll();
});

test("a stop asked while Claude Code is still starting is done once the turn ends, though the start fails", async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const { promise: bayma, reject: failBayma } = Promise.withResolvers<BaymaMcpServer>();
  const { liveSessions, closeAll } = openLiveSessions({
    workingDirectory: "/work",
    sessions: quietSessions,
    queryFactory: () => assert.fail("Claude Code never starts"),
    // Starting the process waits on the conversation's bayma.
    folderBayma: () => Effect.tryPromise({ try: () => bayma, catch: () => new BaymaNotAnswering({ url: "http://bayma:7290/mcp", reason: "unreachable" }) }),
  }, activeTurns);
  const turn = runTurn(liveSessions, turnParams(persistence));
  while (!busy(activeTurns, "telegram:1")) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const stopping = runningTurnOf(activeTurns, "telegram:1").stop();
  assert.equal(await settlesSoon(stopping), false, "the stop waits for the turn to end");
  failBayma(new Error("unreachable"));
  const result = await turn;
  assert.equal(await settlesSoon(stopping), true, "the stop is done once the turn ends");
  assert.equal(result.responseCompleted, false);
  assert.deepEqual(result.blockSequence, [{ type: "text", content: "Error: bayma in http://bayma:7290/mcp did not answer: unreachable" }]);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a stop asked while Claude Code is still starting stops the turn as it starts: its prompt is neither dispatched nor sent", { timeout: 10_000 }, async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { promise: bayma, resolve: answerBayma } = Promise.withResolvers<BaymaMcpServer>();
  const { liveSessions, closeAll } = openLiveSessions({
    workingDirectory: "/work",
    sessions: quietSessions,
    queryFactory: cli.queryFactory,
    // Starting the process waits on the conversation's bayma.
    folderBayma: () => Effect.promise(() => bayma),
  }, activeTurns);
  let dispatched = false;
  let idle = 0;
  const turn = runTurn(liveSessions, turnParams(persistence, { onPromptDispatched: Effect.sync(() => void (dispatched = true)), onIdle: Effect.sync(() => void (idle += 1)) }));
  while (!busy(activeTurns, "telegram:1")) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const stopping = runningTurnOf(activeTurns, "telegram:1").stop();
  assert.equal(await settlesSoon(stopping), false, "the stop waits for the turn to end");
  answerBayma(Effect.runSync(folderBayma({ threadKey: "telegram:1" })));
  const result = await turn;
  assert.equal(await settlesSoon(stopping), true, "the stop is done once the turn ends");
  assert.equal(result.interrupted, true);
  assert.equal(result.responseCompleted, false);
  await settle();
  assert.deepEqual(cli.prompts, [], "the prompt was never sent");
  assert.equal(dispatched, false, "nor recorded as dispatched, for a restart to take as sent");
  assert.equal(idle, 1, "the conversation's idle hook runs, as for any turn's end");
  assert.equal(cli.state.interrupts, 0);
  assert.equal(busy(activeTurns, "telegram:1"), false);
  await closeAll();
});

test("a stop asked as the turn's prompt is dispatched, its process running, stops the turn there: its prompt is never sent", { timeout: 10_000 }, async () => {
  const persistence = createPersistence();
  const activeTurns = makeActiveTurns();
  const cli = createFakeCli();
  const { liveSessions, closeAll } = openLiveSessions({ workingDirectory: "/work", sessions: quietSessions, queryFactory: cli.queryFactory, folderBayma }, activeTurns);
  // The prompt's dispatch takes its while to record.
  const { promise: recorded, resolve: record } = Promise.withResolvers<void>();
  let dispatching = false;
  let idle = 0;
  const turn = runTurn(liveSessions, turnParams(persistence, {
    onIdle: Effect.sync(() => void (idle += 1)),
    onPromptDispatched: Effect.suspend(() => {
      dispatching = true;
      return Effect.promise(() => recorded);
    }),
  }));
  while (!dispatching) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  const stopping = runningTurnOf(activeTurns, "telegram:1").stop();
  assert.equal(await settlesSoon(stopping), false, "the stop waits for the turn to end");
  record();
  const result = await turn;
  assert.equal(await settlesSoon(stopping), true, "the stop is done once the turn ends");
  assert.equal(result.interrupted, true);
  await settle();
  assert.deepEqual(cli.prompts, [], "the prompt was never sent");
  assert.equal(cli.state.interrupts, 0, "the idle process is not interrupted");
  assert.equal(idle, 1, "the conversation's idle hook runs, as for any turn's end");
  assert.equal(busy(activeTurns, "telegram:1"), false);
  // The process serves the conversation's next turn.
  const next = runTurn(liveSessions, turnParams(persistence, { prompt: "again" }));
  const again = await cli.nextPrompt();
  assert.equal(again.message.content, "again");
  cli.emit(successResult({ result: "Done.", session_id: "s-1", user_message_uuids: [again.uuid] }));
  assert.equal((await next).responseCompleted, true);
  assert.equal(cli.state.created, 1);
  await closeAll();
});
