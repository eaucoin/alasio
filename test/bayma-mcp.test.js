import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

import { buildCodexThreadConfig } from "../src/codex/thread-config.js";
import { claudeMcpServers } from "../src/harness/claude/mcp.js";
import { baymaLaunch, ensureBaymaReady } from "../src/mcp/bayma.js";

const env = { ALASIO_STATE_DIR: "/state" };

test("bayma launches from the pinned package with alasio's own Node", () => {
  const { command, args } = baymaLaunch({ harness: "claude", threadKey: "telegram:1", env });
  assert.equal(command, process.execPath);
  assert.match(args[0], /node_modules\/@bayma-repl\/bayma\/dist\/bayma\.js$/u);
  assert.ok(existsSync(args[0]));
  assert.deepEqual(args.slice(1), [
    "mcp-stdio",
    "--default-durability",
    "checkpointed",
    "--state-dir",
    "/state/bayma/claude/telegram-1",
  ]);
});

test("a session outlives its bayma server and resumes with what it checkpointed", async () => {
  const stateEnv = { ...process.env, ALASIO_STATE_DIR: await mkdtemp(join(tmpdir(), "alasio-bayma-durability-")) };
  let client;
  const connect = async () => {
    client = new Client({ name: "alasio-bayma-durability", version: "1.0.0" });
    const launch = baymaLaunch({ harness: "claude", threadKey: "telegram:1", env: stateEnv });
    await client.connect(new StdioClientTransport({ ...launch, env: stateEnv, stderr: "ignore" }));
  };
  try {
    await connect();
    const created = await client.callTool({
      name: "session.create",
      arguments: { runtime: "bun", title: "survivor", cwd: stateEnv.ALASIO_STATE_DIR },
    });
    const sessionId = created.structuredContent.session.session_id;
    await client.callTool({
      name: "exec",
      arguments: { session_id: sessionId, code: "$checkpoint = { kept: 42 }; globalThis.lost = 1;" },
    });
    await client.close();

    await connect();
    const acquired = await client.callTool({ name: "session.acquire_controller", arguments: { session_id: sessionId } });
    assert.equal(acquired.structuredContent.session.status, "suspended");
    const resumed = await client.callTool({
      name: "exec",
      arguments: { session_id: sessionId, code: "JSON.stringify([$checkpoint.kept, typeof lost])" },
    });
    assert.equal(resumed.structuredContent.result_text, JSON.stringify(JSON.stringify([42, "undefined"])));
  } finally {
    await client?.close();
    await rm(stateEnv.ALASIO_STATE_DIR, { recursive: true, force: true });
  }
});

test("each harness and conversation gets its own bayma state directory", () => {
  const stateDir = (harness, threadKey) => baymaLaunch({ harness, threadKey, env }).args.at(-1);
  assert.notEqual(stateDir("claude", "telegram:1"), stateDir("codex", "telegram:1"));
  assert.notEqual(stateDir("codex", "telegram:1"), stateDir("codex", "telegram:2"));
  assert.equal(stateDir("codex", ".."), "/state/bayma/codex/default");
});

test("alasio adds bayma to Claude Code's servers", () => {
  const servers = claudeMcpServers({ threadKey: "telegram:1", env });
  assert.deepEqual(Object.keys(servers), ["bayma"]);
  assert.equal(servers.bayma.type, "stdio");
  assert.equal(servers.bayma.command, process.execPath);
});

test("Codex threads add bayma and leave the operator's servers and apps connector on", () => {
  const config = buildCodexThreadConfig({ codexEnv: env, threadKey: "telegram:1" });
  assert.deepEqual(config.mcp_servers, {
    bayma: {
      ...baymaLaunch({ harness: "codex", threadKey: "telegram:1", env }),
      startup_timeout_sec: 60,
    },
  });
  assert.equal(config.features, undefined);
});

test("the installed bayma starts and serves its tools", async () => {
  await ensureBaymaReady(process.env);
});
