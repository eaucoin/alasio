import assert from "node:assert/strict";
import { createServer } from "node:http";
import { PassThrough } from "node:stream";
import { after, before, test } from "node:test";

import { buildClaudeQueryOptions } from "../src/harness/claude/runtime.js";
import { sandboxClaudeEnv, sandboxMcpServers } from "../src/harness/claude/sandbox.js";
import { sandboxCodexConfig, sandboxCodexEnv, sandboxCodexSpawnProcess } from "../src/codex/sandbox.js";
import { startCodexTransportThread } from "../src/codex/transport.js";
import { SessionGateway } from "../src/sandbox/gateway.js";
import { createSandbox } from "../src/sandbox/index.js";
import { createMetadataEngine } from "../src/sandbox/metadata-engine.js";
import { assertValidVolumeId, isValidVolumeId, newVolumeId, sessionHostName, volumeS3Prefix } from "../src/sandbox/names.js";
import { isSessionFs, parseWorkspace, sessionFsWorkspace } from "../src/workspace/kind.js";

test("volume ids are validated to JuiceFS's 3-63 char rule", () => {
  assert.equal(isValidVolumeId("fs-1a2b3c4d5e"), true);
  assert.equal(isValidVolumeId("abc"), true);
  assert.equal(isValidVolumeId("ab"), false); // too short
  assert.equal(isValidVolumeId("has_underscore"), false);
  assert.equal(isValidVolumeId("has.dot"), false);
  assert.equal(isValidVolumeId(`${"a".repeat(64)}`), false); // too long
  assert.throws(() => assertValidVolumeId("no"), /invalid session volume id/);
  const id = newVolumeId(() => "1a2b3c4d-5e6f-7a8b-9c0d-e1f2a3b4c5d6");
  assert.ok(isValidVolumeId(id), id);
  assert.equal(sessionHostName("fs-abc123"), "alasio-session-fs-abc123");
  assert.equal(volumeS3Prefix("fs-abc123"), "fs-abc123/");
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

test("the redis metadata engine builds per-volume URLs and rejects others", () => {
  const engine = createMetadataEngine({ url: "redis://valkey:6379", databases: 4096 });
  assert.equal(engine.kind, "redis");
  assert.equal(engine.namespaceCount, 4095);
  assert.equal(engine.firstNamespace, 1);
  assert.equal(engine.metaUrl(7), "redis://valkey:6379/7");
  assert.equal(engine.metaUrl(4095), "redis://valkey:6379/4095");
  assert.throws(() => engine.metaUrl(0), /out of range/); // DB 0 reserved
  assert.throws(() => engine.metaUrl(4096), /out of range/);
  assert.throws(() => createMetadataEngine({ url: "postgres://pg/db", databases: 16 }), /unsupported/);
  assert.throws(() => createMetadataEngine({ url: "redis://x", databases: 1 }), /databases count/);
});

test("Claude runs in /workspace inside the sandbox, on the gateway, with bayma over http", () => {
  const env = sandboxClaudeEnv({ bearer: "bearer-1", gatewayUrl: "http://10.0.0.9:8080" });
  assert.equal(env.ANTHROPIC_BASE_URL, "http://10.0.0.9:8080");
  assert.equal(env.ANTHROPIC_AUTH_TOKEN, "bearer-1");
  assert.equal(env.CLAUDE_CONFIG_DIR, "/home/agent/.claude");
  assert.equal(env.CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC, "1");
  assert.ok(!("ANTHROPIC_API_KEY" in env)); // no host secret carried in
  assert.deepEqual(sandboxMcpServers("http://127.0.0.1:7290/mcp"), { bayma: { type: "http", url: "http://127.0.0.1:7290/mcp" } });

  const spawned = [];
  const session = { spawn: (argv, e) => { spawned.push({ argv, e }); return { pid: 1 }; }, baymaHttpUrl: "http://127.0.0.1:7290/mcp" };
  const opts = buildClaudeQueryOptions({
    workingDirectory: "sessionfs:fs-abc123", claudeEnv: env, mcpServers: sandboxMcpServers(session.baymaHttpUrl),
    controller: new AbortController(), hooks: {}, sandboxSession: session,
  });
  assert.equal(opts.cwd, "/workspace"); // not the sentinel, not the host
  assert.equal(typeof opts.spawnClaudeCodeProcess, "function");
  opts.spawnClaudeCodeProcess({ args: ["--print", "hi"], env: { CLAUDE_CODE_ENTRYPOINT: "sdk" } });
  assert.deepEqual(spawned[0].argv, ["claude", "--print", "hi"]); // host binary path dropped, runs `claude` inside
  assert.equal(spawned[0].e.CLAUDE_CODE_ENTRYPOINT, "sdk"); // per-spawn env forwarded

  // A folder workspace keeps the host path and no in-sandbox spawn.
  const folderOpts = buildClaudeQueryOptions({ workingDirectory: "/home/operator/proj", claudeEnv: {}, mcpServers: {}, controller: new AbortController(), hooks: {} });
  assert.equal(folderOpts.cwd, "/home/operator/proj");
  assert.equal(folderOpts.spawnClaudeCodeProcess, undefined);
});

test("Codex runs its own app-server inside the sandbox, on the gateway, with bayma over http", async () => {
  const env = sandboxCodexEnv({ bearer: "bearer-2" });
  assert.equal(env.CODEX_HOME, "/home/agent/.codex");
  assert.equal(env.HOME, "/home/agent");
  assert.equal(env.ALASIO_GATEWAY_TOKEN, "bearer-2"); // the bearer, under the provider's env_key
  assert.ok(!("ANTHROPIC_API_KEY" in env) && !("OPENAI_API_KEY" in env)); // no host secret carried in

  const config = sandboxCodexConfig({ gatewayUrl: "http://10.0.0.9:8080", baymaHttpUrl: "http://127.0.0.1:7290/mcp" });
  assert.equal(config.model_provider, "gateway");
  assert.equal(config.model_providers.gateway.base_url, "http://10.0.0.9:8080/v1");
  assert.equal(config.model_providers.gateway.env_key, "ALASIO_GATEWAY_TOKEN");
  assert.equal(config.model_providers.gateway.wire_api, "responses");
  assert.deepEqual(config.mcp_servers.bayma, { url: "http://127.0.0.1:7290/mcp" });

  // The spawn runs `codex app-server` inside the sandbox over the session's own spawn,
  // wiring the app-server's stdout to readline; the host binary path is never used.
  const stdout = new PassThrough();
  const spawned = [];
  const child = { stdout, stderr: new PassThrough(), on() {}, kill() {} };
  const session = { spawn: (argv, e) => { spawned.push({ argv, e }); return child; } };
  const lines = [];
  const proc = sandboxCodexSpawnProcess(session)({ cwd: "/workspace", env, onLine: (l) => lines.push(l), onExit: () => {}, onError: () => {} });
  assert.deepEqual(spawned[0].argv, ["codex", "app-server", "--disable", "plugins", "--listen", "stdio://"]);
  assert.equal(spawned[0].e.ALASIO_GATEWAY_TOKEN, "bearer-2");
  stdout.write("hello\n");
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(lines, ["hello"]); // the app-server's stdout is read line by line
  assert.equal(typeof proc.stop, "function");

  // The transport routes to the per-session client and its /workspace cwd, not the singleton.
  const calls = [];
  const fakeClient = { startThread: async (a) => { calls.push(a); return "thread-9"; } };
  const threadId = await startCodexTransportThread({ threadKey: "k", workingDirectory: "/workspace", codexEnv: env, codexConfig: config, client: fakeClient });
  assert.equal(threadId, "thread-9");
  assert.equal(calls[0].cwd, "/workspace");
  assert.equal(calls[0].env.ALASIO_GATEWAY_TOKEN, "bearer-2");
});

// A stand-in upstream that records the credential each request arrived with, so the
// gateway's swap and path filter are checked as in session-fs-research E8.
let upstream;
let upstreamPort;
const seen = [];
before(async () => {
  upstream = createServer((req, res) => {
    seen.push({
      path: req.url,
      apiKey: req.headers["x-api-key"] ?? null,
      auth: req.headers.authorization ?? null,
      accountId: req.headers["chatgpt-account-id"] ?? null,
    });
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise((r) => upstream.listen(0, "127.0.0.1", r));
  upstreamPort = upstream.address().port;
});
after(() => new Promise((r) => upstream.close(r)));

async function call(gw, { path, bearer, method = "POST" }) {
  const { port, close } = await gw.listen(0, "127.0.0.1");
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      method,
      headers: bearer ? { authorization: `Bearer ${bearer}` } : {},
      body: method === "POST" ? "{}" : undefined,
    });
    return { status: res.status, body: await res.text() };
  } finally {
    await close();
  }
}

test("the gateway swaps a per-session bearer for the real credential and gates paths", async () => {
  const gw = new SessionGateway({
    providers: {
      anthropic: { upstream: `http://127.0.0.1:${upstreamPort}`, credentialSource: () => "REAL-ANTHROPIC-KEY" },
      openai: { upstream: `http://127.0.0.1:${upstreamPort}`, credentialSource: () => "REAL-OPENAI-KEY" },
    },
    newBearer: () => "bearer-fixed",
  });
  const bearer = gw.issueBearer("session-1");
  assert.equal(bearer, "bearer-fixed");

  seen.length = 0;
  assert.equal((await call(gw, { path: "/v1/messages?beta=true", bearer })).status, 200);
  assert.equal(seen.at(-1).apiKey, "REAL-ANTHROPIC-KEY"); // the real key reached upstream
  assert.equal(seen.at(-1).auth, null); // the bearer did not

  assert.equal((await call(gw, { path: "/v1/messages", bearer: "guessed" })).status, 401);
  assert.equal((await call(gw, { path: "/v1/oauth/token", bearer })).status, 403);
  assert.equal((await call(gw, { path: "/api/hello", bearer, method: "GET" })).status, 200);

  // The same gateway serves Codex by path, injecting the other provider's credential.
  seen.length = 0;
  assert.equal((await call(gw, { path: "/v1/responses", bearer })).status, 200);
  assert.equal(seen.at(-1).auth, "Bearer REAL-OPENAI-KEY");
  assert.equal(seen.at(-1).apiKey, null);

  gw.revokeSession("session-1");
  assert.equal((await call(gw, { path: "/v1/messages", bearer })).status, 401); // revoked
});

test("the gateway injects a subscription OAuth login as a Bearer, not an x-api-key", async () => {
  const gw = new SessionGateway({
    providers: {
      anthropic: { upstream: `http://127.0.0.1:${upstreamPort}`, credentialSource: () => "oauth-access-token", oauth: true },
    },
    newBearer: () => "bearer-oauth",
  });
  const bearer = gw.issueBearer("session-oauth");
  seen.length = 0;
  assert.equal((await call(gw, { path: "/v1/messages", bearer })).status, 200);
  assert.equal(seen.at(-1).auth, "Bearer oauth-access-token"); // the OAuth token as a Bearer
  assert.equal(seen.at(-1).apiKey, null); // never as x-api-key
});

test("the gateway sends a ChatGPT login to the Codex backend path with its account id", async () => {
  const gw = new SessionGateway({
    providers: {
      openai: {
        upstream: `http://127.0.0.1:${upstreamPort}`,
        credentialSource: () => ({ credential: "chatgpt-access-token", headers: { "chatgpt-account-id": "acct-1" } }),
        rewritePath: (path) => path.replace(/^\/v1(?=\/|$)/, "/backend-api/codex"),
      },
    },
    newBearer: () => "bearer-chatgpt",
  });
  const bearer = gw.issueBearer("session-chatgpt");
  seen.length = 0;
  assert.equal((await call(gw, { path: "/v1/responses", bearer })).status, 200);
  assert.equal(seen.at(-1).path, "/backend-api/codex/responses"); // the backend's own path
  assert.equal(seen.at(-1).auth, "Bearer chatgpt-access-token"); // the real login, not the bearer
  assert.equal(seen.at(-1).accountId, "acct-1");
});

test("without a configured address, session hosts reach the gateway at their network's own gateway", async () => {
  const calls = [];
  const docker = {
    cli: async (args) => {
      calls.push(args);
      return { stdout: "172.31.0.1 \n", stderr: "" };
    },
  };
  const sandbox = createSandbox({
    config: {
      metadata: { url: "redis://valkey:6379", databases: 16, passwordFile: "/dev/null" },
      s3: { endpoint: "http://seaweedfs:8333", bucket: "sessions", accessKey: "k", secretKey: "s" },
      host: { network: "alasio-neon_default", agentImage: "a", sessionHostImage: "h", memoryMb: 1, cpus: 1, pidsLimit: 1, cacheMb: 1, hostPublicIp: null },
      gateway: { ip: null, port: 0, anthropicUpstream: "http://127.0.0.1:1", openaiUpstream: "http://127.0.0.1:1" },
    },
    store: { sessionVolumes: {} },
    docker,
  });
  assert.throws(() => sandbox.gatewayUrl, /not known until startGateway/);
  await sandbox.startGateway();
  try {
    assert.deepEqual(calls[0].slice(0, 3), ["network", "inspect", "alasio-neon_default"]);
    assert.equal(sandbox.gatewayUrl, "http://172.31.0.1:0");
    assert.equal(sandbox.host.config.gateway.ip, "172.31.0.1"); // what a session host's firewall allows
  } finally {
    await sandbox.stopGateway();
  }
});

test("the gateway returns 503 until a model login is configured, mechanism still live", async () => {
  const gw = new SessionGateway({
    providers: { openai: { upstream: `http://127.0.0.1:${upstreamPort}`, credentialSource: () => null } }, // login not configured yet (stubbed)
  });
  const bearer = gw.issueBearer("session-2");
  assert.equal((await call(gw, { path: "/v1/responses", bearer })).status, 503);
  assert.equal((await call(gw, { path: "/v1/responses", bearer: "nope" })).status, 401); // still validates
  assert.equal((await call(gw, { path: "/v1/oauth", bearer })).status, 403); // still gates
});
