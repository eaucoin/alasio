import assert from "node:assert/strict";
import { test } from "node:test";

import type { Options, SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { Effect, Exit, Layer, Scope } from "effect";

import type { v2 } from "../.types/codex/index.js";
import { type AppServer, CodexAppServer, makeAppServer } from "../src/codex/app-server/client.ts";
import type { AppServerScope } from "../src/codex/app-server/rpc-client.ts";
import type { ThreadOptions, ThreadScope } from "../src/codex/app-server/thread-client.ts";
import { SessionFsCodex } from "../src/codex/sessionfs.ts";
import { codexMcpServer } from "../src/codex/thread-config.ts";
import { ActiveTurns } from "../src/harness/active-turns.ts";
import { makeCodexHarness } from "../src/harness/codex.ts";
import { makeClaudeHarness } from "../src/harness/claude/index.ts";
import type { ClaudeQueryFactory } from "../src/harness/claude/runtime.ts";
import { SESSION_FS_CLAUDE_TOOLS } from "../src/harness/claude/sessionfs.ts";
import { createClaudeSessionApi } from "../src/harness/claude/sessions.ts";
import type { TurnPersistence } from "../src/harness/index.ts";
import type { BaymaEndpoint } from "../src/kube/sandboxes.ts";
import { noFolderBayma } from "../src/mcp/bayma.ts";
import type { SessionSandboxes } from "../src/sandbox/index.ts";
import { fakeQuery, initMessage, stamped, successResult } from "./support/claude-sdk.ts";

// Both harnesses on a session filesystem, over a fake sandbox: what they run in, what they
// are given, and which calls start the session's host.
const WORKSPACE = "sessionfs:fs-abc123";
const DIRECTORY = "/state/sessionfs/workspaces/fs-abc123";
const BAYMA: BaymaEndpoint = { url: "http://127.0.0.1:40000/mcp", headers: { Authorization: "Bearer forward-token" } };
const CODEX_ENV = { CODEX_HOME: "/state/sessionfs/codex" };

function fakeSandbox(): SessionSandboxes["Service"] & { readonly ensured: string[] } {
  const ensured: string[] = [];
  const unused = () => assert.fail("a harness only finds a session's directory and starts its host");
  return {
    ensured,
    volumes: { create: unused, fork: unused, destroy: unused },
    harnessDirectory: (volumeId) => `/state/sessionfs/workspaces/${volumeId}`,
    ensureSession: (volumeId) =>
      Effect.sync(() => {
        ensured.push(volumeId);
        return { bayma: BAYMA };
      }),
    readFile: unused,
  };
}

/** An app-server call a harness made: its method, and what it named. */
type AppServerCall =
  | { readonly method: "listThreads" | "listModels"; readonly args: AppServerScope }
  | { readonly method: "getGoal"; readonly args: ThreadScope }
  | { readonly method: "startThread"; readonly args: ThreadOptions };

const THREAD: v2.Thread = {
  id: "thread-1",
  extra: null,
  sessionId: "thread-1",
  forkedFromId: null,
  parentThreadId: null,
  preview: "",
  ephemeral: false,
  section: null,
  sectionEnteredAt: null,
  projectId: null,
  historyMode: "paginated",
  modelProvider: "openai",
  model: null,
  reasoningEffort: null,
  createdAt: 1_790_000_000,
  updatedAt: 1_790_000_000,
  recencyAt: null,
  status: { type: "notLoaded" },
  path: null,
  cwd: DIRECTORY,
  cliVersion: "0.0.0",
  source: "appServer",
  canAcceptDirectInput: null,
  threadSource: null,
  agentNickname: null,
  agentRole: null,
  gitInfo: null,
  name: "a thread",
  turns: [],
};

const GOAL: v2.ThreadGoal = {
  threadId: "thread-1",
  objective: "ship it",
  status: "active",
  tokenBudget: null,
  tokensUsed: 0,
  timeUsedSeconds: 0,
  createdAt: 1_790_000_000,
  updatedAt: 1_790_000_000,
};

const MODEL: v2.Model = {
  id: "m",
  model: "m",
  upgrade: null,
  upgradeInfo: null,
  availabilityNux: null,
  displayName: "M",
  description: "",
  modelSpecialty: null,
  hidden: false,
  supportedReasoningEfforts: [],
  defaultReasoningEffort: "medium",
  inputModalities: [],
  supportsPersonality: false,
  multiAgentVersion: null,
  additionalSpeedTiers: [],
  serviceTiers: [],
  defaultServiceTier: null,
  isDefault: false,
};

/** An app-server no process runs for: any call that would start one fails the test. */
const idleAppServer = makeAppServer({ spawn: () => Effect.sync(() => assert.fail("no app-server runs in this test")) });

/** The session filesystems' app-server, answering from fixtures and recording every call. */
function recordingAppServer(idle: AppServer): AppServer & { readonly calls: readonly AppServerCall[] } {
  const calls: AppServerCall[] = [];
  return {
    ...idle,
    calls,
    listThreads: (args) => Effect.sync(() => {
      calls.push({ method: "listThreads", args });
      return [THREAD];
    }),
    getGoal: (args) => Effect.sync(() => {
      calls.push({ method: "getGoal", args });
      return { goal: GOAL };
    }),
    listModels: (args) => Effect.sync(() => {
      calls.push({ method: "listModels", args });
      return [MODEL];
    }),
    startThread: (args) => Effect.sync(() => {
      calls.push({ method: "startThread", args });
      return "thread-2";
    }),
  };
}

test("neither harness serves a session filesystem where the deployment does not enable them", async () => {
  const options = { workingDirectory: WORKSPACE, sandbox: null, folderBayma: noFolderBayma };
  const refusals = await Effect.runPromise(Effect.scoped(Effect.all([
    Effect.flip(makeCodexHarness({ ...options, sessionFsCodex: null })),
    Effect.flip(makeClaudeHarness(options)),
  ])).pipe(Effect.provide([ActiveTurns.layer, Layer.effect(CodexAppServer, idleAppServer)])));
  for (const refusal of refusals) {
    assert.match(refusal.message, /does not enable/);
  }
});

test("Codex on a session filesystem runs on its own app-server, in the harness directory, with the session's bayma", async (t) => {
  const sandbox = fakeSandbox();
  const scope = Effect.runSync(Scope.make());
  t.after(() => Effect.runPromise(Scope.close(scope, Exit.void)));
  // The operator's app-server is there, and never used; the session filesystems' answers.
  const operatorAppServer = await Effect.runPromise(idleAppServer.pipe(Scope.provide(scope)));
  const client = recordingAppServer(await Effect.runPromise(idleAppServer.pipe(Scope.provide(scope))));
  const scopes: { directory: string; bayma: BaymaEndpoint }[] = [];
  const sessionFsCodex = SessionFsCodex.of({
    home: "/state/sessionfs/codex",
    scope: ({ directory, bayma }) => Effect.sync(() => {
      scopes.push({ directory, bayma });
      return { cwd: directory, codexEnv: CODEX_ENV, codexConfig: { developer_instructions: "", mcp_servers: { bayma: codexMcpServer(bayma) } }, appServer: client };
    }),
    listingScope: ({ directory }) => Effect.succeed({ cwd: directory, codexEnv: CODEX_ENV, appServer: client }),
    stop: Effect.void,
  });
  const harness = await Effect.runPromise(makeCodexHarness({ workingDirectory: WORKSPACE, sandbox, sessionFsCodex, folderBayma: noFolderBayma }).pipe(
    Effect.provideService(CodexAppServer, operatorAppServer),
    Effect.provide(ActiveTurns.layer),
  ));
  const { goals } = harness;
  assert.ok(goals, "Codex has goals");

  // Lists, goals, and models need no session host.
  assert.equal((await Effect.runPromise(harness.sessions.listSessions(1)))[0]?.uuid, "thread-1");
  assert.deepEqual(await Effect.runPromise(goals.read({ threadId: "thread-1" })), GOAL);
  assert.equal((await Effect.runPromise(harness.listModels()))[0]?.id, "m");
  assert.deepEqual(sandbox.ensured, []);
  for (const { args } of client.calls) {
    assert.equal(args.cwd, DIRECTORY); // never the sentinel, never the operator's folder
    assert.equal(args.env["CODEX_HOME"], "/state/sessionfs/codex"); // never the operator's Codex home
  }

  // A thread's work starts the session's host and reaches it through its bayma.
  assert.equal(await Effect.runPromise(harness.startFreshSession({ threadKey: "telegram:1" })), "thread-2");
  assert.deepEqual(sandbox.ensured, ["fs-abc123"]);
  assert.deepEqual(scopes, [{ directory: DIRECTORY, bayma: BAYMA }]);
  const started = client.calls.find((call) => call.method === "startThread");
  assert.ok(started?.method === "startThread", "a thread was started");
  assert.equal(started.args.cwd, DIRECTORY);
  assert.deepEqual(started.args.config.mcp_servers["bayma"], codexMcpServer(BAYMA));
});

test("Claude Code on a session filesystem runs in the harness directory, confined to the session's bayma", async () => {
  const sandbox = fakeSandbox();
  const seen: { options: Options | null; resumeAsked: string | null; processes: number } = { options: null, resumeAsked: null, processes: 0 };
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => {
    seen.options = options;
    seen.processes += 1;
    return fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
      yield initMessage("s1");
      for await (const message of prompt) {
        yield successResult({ result: "done", session_id: "s1", user_message_uuids: [stamped(message).uuid] });
      }
    })());
  };
  const sessionApi = {
    ...createClaudeSessionApi({ workingDirectory: DIRECTORY }),
    sessionExists: (id: string) =>
      Effect.sync(() => {
        seen.resumeAsked = id;
        return true;
      }),
  };
  // The harness lasts as long as the scope it is made in, as alasio's last as long as alasio.
  const scope = Effect.runSync(Scope.make());
  const harness = await Effect.runPromise(makeClaudeHarness({ workingDirectory: WORKSPACE, sandbox, sessionApi, claudeQueryFactory: queryFactory, folderBayma: noFolderBayma }).pipe(
    Effect.provide(ActiveTurns.layer),
    Scope.provide(scope),
  ));
  const persistence: TurnPersistence = {
    createPendingResponse: () => "pending-1",
    markPendingAsPosted: () => undefined,
    updateActiveTurnPendingResponseId: () => undefined,
    updatePendingSessionId: () => undefined,
    updateActiveTurnSessionId: () => undefined,
    appendBlockToPending: () => undefined,
    markPendingResponseComplete: () => undefined,
    updateSessionUsage: () => undefined,
    recordRestartEvent: () => undefined,
  };
  try {
    for (const prompt of ["hi", "again"]) {
      const result = await Effect.runPromise(harness.runTurn({
        prompt, resumeSession: "s1", threadKey: "telegram:1", chatId: "1", messageId: "2", workingDirectory: WORKSPACE, persistence,
      }));
      assert.equal(result.responseCompleted, true);
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
  // One live process serves both turns, and the session is made sure of before each: its
  // host may have stopped between them.
  assert.equal(seen.processes, 1);
  assert.deepEqual(sandbox.ensured, ["fs-abc123", "fs-abc123"]);
  assert.equal(seen.resumeAsked, "s1"); // resume is decided from its transcripts on this machine
  const { options } = seen;
  assert.ok(options, "Claude Code was started");
  assert.equal(options.cwd, DIRECTORY);
  assert.equal(options.resume, "s1");
  assert.deepEqual(options.tools, [...SESSION_FS_CLAUDE_TOOLS]);
  assert.deepEqual(options.settingSources, []);
  assert.equal(options.strictMcpConfig, true);
  assert.deepEqual(options.mcpServers, { bayma: { type: "http", ...BAYMA } });
  assert.equal(options.env?.["HOME"], process.env["HOME"]); // the operator's own login and Claude home
});
