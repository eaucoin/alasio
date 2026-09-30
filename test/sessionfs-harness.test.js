import assert from "node:assert/strict";
import { test } from "node:test";

import { createCodexHarness } from "../src/harness/codex.js";
import { createClaudeHarness } from "../src/harness/claude/index.js";
import { SESSION_FS_CLAUDE_TOOLS } from "../src/harness/claude/sessionfs.js";

// Both harnesses on a session filesystem, over a fake sandbox: what they run in, what they
// are given, and which calls start the session's host.
const WORKSPACE = "sessionfs:fs-abc123";
const DIRECTORY = "/state/sessionfs/workspaces/fs-abc123";
const BAYMA = { url: "http://127.0.0.1:40000/mcp", headers: { Authorization: "Bearer forward-token" } };

function fakeSandbox() {
  const ensured = [];
  return {
    ensured,
    enabled: true,
    harnessDirectory: (volumeId) => `/state/sessionfs/workspaces/${volumeId}`,
    ensureSession: async (volumeId) => {
      ensured.push(volumeId);
      return { bayma: BAYMA };
    },
  };
}

test("neither harness serves a session filesystem where the deployment does not enable them", () => {
  assert.throws(() => createCodexHarness({ workingDirectory: WORKSPACE }), /does not enable/);
  assert.throws(() => createClaudeHarness({ workingDirectory: WORKSPACE }), /does not enable/);
});

test("Codex on a session filesystem runs on its own app-server, in the harness directory, with the session's bayma", async () => {
  const sandbox = fakeSandbox();
  const calls = [];
  const client = new Proxy({}, {
    get: (_target, method) => async (args) => {
      calls.push({ method, args });
      if (method === "listThreads") return [{ id: "thread-1", name: "a thread", updatedAt: 1_790_000_000 }];
      if (method === "getGoal") return { goal: { objective: "ship it", status: "active" } };
      if (method === "listModels") return [{ id: "m", model: "m", displayName: "M", hidden: false }];
      if (method === "startThread") return "thread-2";
      return null;
    },
  });
  const scopes = [];
  const sessionFsCodex = {
    scope: async ({ directory, bayma }) => {
      scopes.push({ directory, bayma });
      return { cwd: directory, codexEnv: { CODEX_HOME: "/state/sessionfs/codex" }, codexConfig: { mcp_servers: { bayma } }, client };
    },
    listingScope: async ({ directory }) => ({ cwd: directory, codexEnv: { CODEX_HOME: "/state/sessionfs/codex" }, client }),
  };
  const harness = createCodexHarness({ workingDirectory: WORKSPACE, sandbox, sessionFsCodex });

  // Lists, goals, and models need no session host.
  assert.equal((await harness.sessions.listSessions(1))[0].uuid, "thread-1");
  assert.deepEqual(await harness.goals.read({ threadId: "thread-1" }), { objective: "ship it", status: "active" });
  assert.equal((await harness.listModels())[0].id, "m");
  assert.deepEqual(sandbox.ensured, []);
  for (const { args } of calls) {
    assert.equal(args.cwd, DIRECTORY); // never the sentinel, never the operator's folder
    assert.equal(args.env.CODEX_HOME, "/state/sessionfs/codex"); // never the operator's Codex home
  }

  // A thread's work starts the session's host and reaches it through its bayma.
  assert.equal(await harness.startFreshSession({ threadKey: "telegram:1" }), "thread-2");
  assert.deepEqual(sandbox.ensured, ["fs-abc123"]);
  assert.deepEqual(scopes, [{ directory: DIRECTORY, bayma: BAYMA }]);
  const started = calls.find(({ method }) => method === "startThread").args;
  assert.equal(started.cwd, DIRECTORY);
  assert.deepEqual(started.config.mcp_servers.bayma, BAYMA);
});

test("Claude Code on a session filesystem runs in the harness directory, confined to the session's bayma", async () => {
  const sandbox = fakeSandbox();
  const seen = { options: null, resumeAsked: null, processes: 0 };
  const queryFactory = ({ prompt, options }) => {
    seen.options = options;
    seen.processes += 1;
    const generator = (async function* run() {
      yield { type: "system", subtype: "init", session_id: "s1", model: "m" };
      for await (const message of prompt) {
        yield { type: "result", subtype: "success", is_error: false, result: "done", session_id: "s1", user_message_uuids: [message.uuid] };
      }
    })();
    generator.close = () => undefined;
    return generator;
  };
  const sessionApi = { sessionExists: async (id) => { seen.resumeAsked = id; return true; } };
  const harness = createClaudeHarness({ workingDirectory: WORKSPACE, sandbox, sessionApi, queryFactory });
  const persistence = {
    createPendingResponse: () => "pending-1",
    markPendingAsPosted: () => undefined,
    updateActiveTurnPendingResponseId: () => undefined,
    updatePendingSessionId: () => undefined,
    updateActiveTurnSessionId: () => undefined,
    appendBlockToPending: () => undefined,
    markPendingResponseComplete: () => undefined,
    updateSessionUsage: () => undefined,
  };
  try {
    for (const prompt of ["hi", "again"]) {
      const result = await harness.executeTurn({
        prompt, resumeSession: "s1", threadKey: "telegram:1", chatId: 1, messageId: 2,
        persistence, activeQueries: new Map(),
      });
      assert.equal(result.responseCompleted, true);
    }
  } finally {
    await harness.shutdown();
  }
  // One live process serves both turns, and the session is made sure of before each: its
  // host may have stopped between them.
  assert.equal(seen.processes, 1);
  assert.deepEqual(sandbox.ensured, ["fs-abc123", "fs-abc123"]);
  assert.equal(seen.resumeAsked, "s1"); // resume is decided from its transcripts on this machine
  const { options } = seen;
  assert.equal(options.cwd, DIRECTORY);
  assert.equal(options.resume, "s1");
  assert.deepEqual(options.tools, [...SESSION_FS_CLAUDE_TOOLS]);
  assert.deepEqual(options.settingSources, []);
  assert.equal(options.strictMcpConfig, true);
  assert.deepEqual(options.mcpServers, { bayma: { type: "http", ...BAYMA } });
  assert.equal(options.env.HOME, process.env.HOME); // the operator's own login and Claude home
});
