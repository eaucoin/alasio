import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

import { buildCodexThreadConfig } from "../src/codex/thread-config.js";
import { claudeMcpServers } from "../src/harness/claude/mcp.js";
import { BAYMA_IMAGE, baymaLaunch, ensureBaymaReady } from "../src/mcp/bayma.js";

const env = { ALASIO_STATE_DIR: "/state" };

test("bayma runs from its pinned image, after stopping any bayma still on its state directory", () => {
  const { command, args } = baymaLaunch({ harness: "claude", threadKey: "telegram:1", env });
  assert.equal(command, "/bin/sh");
  assert.equal(args[0], "-c");
  assert.match(args[1], /docker stop .*exec docker run --label "alasio\.bayma\.state-dir=\$0" "\$@"$/u);
  assert.doesNotMatch(args[1], /docker wait/u);
  assert.equal(args[2], "/state/bayma/claude/telegram-1");
  const run = args.slice(3);
  assert.match(BAYMA_IMAGE, /^ghcr\.io\/eaucoin\/bayma:[\d.]+@sha256:[0-9a-f]{64}$/u);
  assert.deepEqual(run.slice(run.indexOf(BAYMA_IMAGE)), [
    BAYMA_IMAGE,
    "mcp-stdio",
    "--default-durability",
    "checkpointed",
    "--state-dir",
    "/state/bayma/claude/telegram-1",
  ]);
  for (const flag of [
    ["--user", `${process.getuid()}:${process.getgid()}`],
    ["--network", "host"],
    ["--cap-add", "CHECKPOINT_RESTORE"],
    ["--cap-add", "SYS_PTRACE"],
    ["--security-opt", "seccomp=unconfined"],
    ["--security-opt", "apparmor=unconfined"],
    ["--volume", "/run/dbus/system_bus_socket:/run/dbus/system_bus_socket"],
    ["--volume", "/home:/home"],
    ["--volume", "/tmp:/tmp"],
    ["--volume", "/var/run/docker.sock:/var/run/docker.sock"],
  ]) {
    assert.ok(
      run.some((value, index) => value === flag[0] && run[index + 1] === flag[1]),
      `${flag.join(" ")} is passed`,
    );
  }
  // Its sessions keep alasio's environment, but not the image's own settings.
  assert.ok(run.some((value, index) => value === "--env" && run[index + 1] === "ALASIO_STATE_DIR"));
  const passed = baymaLaunch({ harness: "claude", threadKey: "telegram:1", env: { ...env, PATH: "/x", BAYMA_PAYLOAD_DIR: "/y" } }).args;
  assert.ok(!passed.includes("PATH") && !passed.includes("BAYMA_PAYLOAD_DIR"));
});

test("a session outlives its bayma server and comes back whole in the next one", async () => {
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
    // Its process was snapshotted as the server stopped, so even what it never
    // checkpointed is still there.
    assert.equal(resumed.structuredContent.result_text, JSON.stringify(JSON.stringify([42, "number"])));
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
  assert.equal(servers.bayma.command, "/bin/sh");
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

test("bayma's image starts and serves its tools", async () => {
  await ensureBaymaReady(process.env);
});

test("a bayma left running on the state directory is stopped, not waited on, before the next starts", async () => {
  const bin = await mkdtemp(join(tmpdir(), "alasio-fake-docker-"));
  try {
    const calls = join(bin, "calls");
    const { writeFile, readFile, chmod } = await import("node:fs/promises");
    // ps reports an orphan on this state directory; wait would hang forever.
    await writeFile(join(bin, "docker"), `#!/bin/sh
echo "$*" >> "${calls}"
case "$1" in
  ps) echo orphan-1 ;;
  wait) sleep 3600 ;;
esac
`);
    await chmod(join(bin, "docker"), 0o755);
    const { command, args } = baymaLaunch({ harness: "claude", threadKey: "telegram:1", env });
    const { execFileSync } = await import("node:child_process");
    execFileSync(command, args, { env: { PATH: `${bin}:/usr/bin:/bin` }, timeout: 10_000 });
    const lines = (await readFile(calls, "utf8")).trim().split("\n");
    assert.match(lines[0], /^ps --quiet --filter label=alasio\.bayma\.state-dir=\/state\/bayma\/claude\/telegram-1$/u);
    assert.equal(lines[1], "stop orphan-1");
    assert.match(lines[2], /^run --label alasio\.bayma\.state-dir=\/state\/bayma\/claude\/telegram-1 /u);
    assert.equal(lines.length, 3);
  } finally {
    await rm(bin, { recursive: true, force: true });
  }
});
