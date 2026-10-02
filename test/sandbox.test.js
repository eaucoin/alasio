import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.js";
import { SESSION_FS_CLAUDE_TOOLS } from "../src/harness/claude/sessionfs.js";
import { SESSION_FS_INSTRUCTIONS } from "../src/harness/workspace-instructions.js";
import {
  createSessionFsCodex,
  SESSION_FS_ENVIRONMENTS_TOML,
  sessionFsCodexConfigToml,
  sessionFsThreadConfig,
} from "../src/codex/sessionfs.js";
import { createSandbox } from "../src/sandbox/index.js";
import { assertValidVolumeId, isValidVolumeId, newVolumeId } from "../src/sandbox/names.js";
import { isSessionFs, parseWorkspace, sessionFsWorkspace } from "../src/workspace/kind.js";

const dirs = [];
const tempDir = () => {
  const dir = mkdtempSync(join(tmpdir(), "alasio-sandbox-"));
  dirs.push(dir);
  return dir;
};
after(() => { for (const dir of dirs) rmSync(dir, { recursive: true, force: true }); });

const BAYMA = { url: "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp", headers: { Authorization: "Bearer fs-abc123.token" } };

test("volume ids are DNS labels, as the names of their Sandboxes must be", () => {
  assert.equal(isValidVolumeId("fs-1a2b3c4d5e"), true);
  assert.equal(isValidVolumeId("abc"), true);
  assert.equal(isValidVolumeId(`${"a".repeat(63)}`), true);
  assert.equal(isValidVolumeId("ab"), false); // too short
  assert.equal(isValidVolumeId(`${"a".repeat(64)}`), false); // too long
  assert.equal(isValidVolumeId("has_underscore"), false);
  assert.equal(isValidVolumeId("has.dot"), false);
  assert.equal(isValidVolumeId("fs-Foo1"), false); // no uppercase
  assert.equal(isValidVolumeId("fs-abc-"), false); // and no trailing hyphen
  assert.equal(isValidVolumeId("-abc"), false);
  assert.throws(() => assertValidVolumeId("no"), /invalid session volume id/);
  const id = newVolumeId(() => "1a2b3c4d-5e6f-7a8b-9c0d-e1f2a3b4c5d6");
  assert.ok(isValidVolumeId(id), id);
});

test("a workspace parses as a folder or a session filesystem, both from one string", () => {
  assert.deepEqual(parseWorkspace("/home/operator/proj"), { kind: "folder", path: "/home/operator/proj" });
  assert.deepEqual(parseWorkspace(sessionFsWorkspace("fs-abc123")), { kind: "sessionfs", volumeId: "fs-abc123" });
  assert.equal(isSessionFs("/home/operator/proj"), false);
  assert.equal(isSessionFs("sessionfs:fs-abc123"), true);
  assert.equal(parseWorkspace(""), null);
  assert.equal(parseWorkspace(null), null);
  assert.throws(() => parseWorkspace("sessionfs:no"), /malformed session-filesystem workspace/);
});

test("a session filesystem's Claude Code keeps only tools that stay off this machine, and reaches bayma alone", () => {
  const opts = buildClaudeQueryOptions({
    workingDirectory: "/state/sessionfs/workspaces/fs-abc123", claudeEnv: { HOME: "/home/op" }, controller: new AbortController(),
    hooks: {}, sessionFsBayma: BAYMA,
  });
  assert.equal(opts.cwd, "/state/sessionfs/workspaces/fs-abc123"); // its harness directory, never the sentinel
  assert.deepEqual(opts.tools, [...SESSION_FS_CLAUDE_TOOLS]);
  for (const tool of ["Bash", "Read", "Write", "Edit", "Glob", "Grep", "NotebookEdit", "WebFetch", "LSP", "Monitor"]) {
    assert.ok(!opts.tools.includes(tool), tool);
  }
  assert.equal(opts.disallowedTools, undefined); // the allowlist says it all
  assert.deepEqual(opts.settingSources, []); // none of the operator's settings, hooks, or skills
  assert.equal(opts.strictMcpConfig, true); // none of the operator's MCP servers
  assert.deepEqual(opts.mcpServers, { bayma: { type: "http", url: BAYMA.url, headers: BAYMA.headers } });
  assert.match(opts.systemPrompt.append, new RegExp(SESSION_FS_INSTRUCTIONS.slice(0, 40)));
  assert.equal(opts.spawnClaudeCodeProcess, undefined); // the CLI runs here, not in the sandbox

  // A folder workspace is as before: its own folder, the operator's settings, the usual tools.
  const folder = buildClaudeQueryOptions({ workingDirectory: "/home/op/proj", claudeEnv: {}, mcpServers: { bayma: {} }, controller: new AbortController(), hooks: {} });
  assert.equal(folder.cwd, "/home/op/proj");
  assert.equal(folder.tools, undefined);
  assert.equal(folder.settingSources, undefined);
  assert.ok(folder.disallowedTools.includes("Bash"));
});

test("the session-filesystem Codex home has no environment and the login relay as its one model provider", () => {
  const toml = sessionFsCodexConfigToml("http://127.0.0.1:41000/v1");
  assert.match(toml, /^model_provider = "alasio_login_relay"$/m);
  assert.match(toml, /^base_url = "http:\/\/127\.0\.0\.1:41000\/v1"$/m);
  assert.match(toml, /^env_key = "ALASIO_CODEX_LOGIN"$/m);
  assert.match(toml, /^wire_api = "responses"$/m);
  assert.equal(SESSION_FS_ENVIRONMENTS_TOML, "include_local = false\n"); // no shell, apply_patch, or view_image
  const config = sessionFsThreadConfig(BAYMA);
  assert.deepEqual(config.mcp_servers, { bayma: { url: BAYMA.url, http_headers: BAYMA.headers, startup_timeout_sec: 60 } });
  assert.match(config.developer_instructions, new RegExp(SESSION_FS_INSTRUCTIONS.slice(0, 40)));
});

test("the session-filesystem Codex writes its own home, carries none of the operator's env, and stops cleanly", async () => {
  const home = join(tempDir(), "codex");
  const relays = [];
  const spawned = [];
  process.env.OPENAI_API_KEY_FOR_THIS_TEST = "operator-secret";
  try {
    const codex = createSessionFsCodex({
      home,
      authFile: "/operator/.codex/auth.json",
      startRelay: async ({ authFile }) => {
        const relay = { authFile, url: "http://127.0.0.1:41000/v1", bearer: "relay-bearer", closed: false, close: async () => { relay.closed = true; } };
        relays.push(relay);
        return relay;
      },
      spawnProcess: (options) => { spawned.push(options); throw new Error("not spawned in this test"); },
    });
    const scope = await codex.scope({ directory: "/state/sessionfs/workspaces/fs-abc123", bayma: BAYMA });
    const listing = await codex.listingScope({ directory: "/state/sessionfs/workspaces/fs-abc123" });
    assert.equal(relays.length, 1); // one relay and app-server for every workspace
    assert.equal(relays[0].authFile, "/operator/.codex/auth.json");
    assert.equal(scope.client, listing.client);
    assert.equal(scope.cwd, "/state/sessionfs/workspaces/fs-abc123");
    assert.deepEqual(scope.codexConfig, sessionFsThreadConfig(BAYMA));
    assert.equal(listing.codexConfig, undefined);
    assert.deepEqual(Object.keys(scope.codexEnv).sort(), ["CODEX_HOME", "HOME", "PATH", "ALASIO_CODEX_LOGIN"]);
    assert.equal(scope.codexEnv.CODEX_HOME, home);
    assert.equal(scope.codexEnv.HOME, join(home, "home")); // not the operator's home
    assert.equal(scope.codexEnv.ALASIO_CODEX_LOGIN, "relay-bearer");
    assert.equal(readFileSync(join(home, "config.toml"), "utf8"), sessionFsCodexConfigToml("http://127.0.0.1:41000/v1"));
    assert.equal(readFileSync(join(home, "environments.toml"), "utf8"), SESSION_FS_ENVIRONMENTS_TOML);

    // The app-server process runs in the home, not in the directory of whichever workspace asked first.
    await assert.rejects(scope.client.listModels({ env: scope.codexEnv, cwd: scope.cwd }), /not spawned in this test/);
    assert.equal(spawned[0].cwd, home);

    await codex.stop();
    assert.equal(relays[0].closed, true);
  } finally {
    delete process.env.OPENAI_API_KEY_FOR_THIS_TEST;
  }
});

test("a session's harness directory is its own, under the state directory, outside the sandbox", () => {
  const stateDir = tempDir();
  const profile = { namespace: "alasio-sessions", port: 7290, workspaceDir: "/workspace", podTemplate: { spec: { containers: [{ name: "bayma" }] } } };
  const sandbox = createSandbox({ templates: { sessions: profile }, stateDir, env: {}, kube: {} });
  const directory = sandbox.harnessDirectory("fs-abc123");
  assert.equal(directory, join(stateDir, "sessionfs", "workspaces", "fs-abc123"));
  assert.ok(existsSync(directory));
  assert.throws(() => sandbox.harnessDirectory("../escape"), /invalid session volume id/);
});
