import assert from "node:assert/strict";
import { test } from "node:test";

import { buildCodexThreadConfig, codexMcpServer } from "../src/codex/thread-config.ts";
import { claudeMcpServers } from "../src/harness/claude/mcp.ts";
import type { HostProfile, SessionsProfile } from "../src/kube/config.ts";
import { type BaymaMcpServer, createHostBayma, hostBaymaManifest, hostBaymaName, hostBaymaStateDir } from "../src/mcp/bayma.ts";

const host: HostProfile = {
  namespace: "alasio-host",
  port: 7290,
  stateRoot: "/home/op/.alasio/bayma",
  podTemplate: { spec: { containers: [{ name: "bayma", image: "bayma@sha256:1", args: ["mcp-http", "--port", "7290"] }] } },
};
const sessions: SessionsProfile = {
  namespace: "alasio-sessions",
  port: 7290,
  workspaceDir: "/workspace",
  podTemplate: { spec: { containers: [{ name: "bayma" }] } },
};
const BAYMA: BaymaMcpServer = { type: "http", url: "http://bayma-1.alasio-host.svc.cluster.local:7290/mcp", headers: { Authorization: "Bearer bayma-1.t" } };

test("a folder conversation's bayma is a host Sandbox with its own state directory and telemetry", () => {
  const sandbox = hostBaymaManifest({
    harness: "claude",
    threadKey: "telegram:42",
    profile: host,
    env: { OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318" },
  });
  assert.equal(sandbox.metadata.name, hostBaymaName("claude", "telegram:42"));
  assert.match(sandbox.metadata.name, /^bayma-[0-9a-f]{20}$/u);
  assert.equal(sandbox.metadata.namespace, "alasio-host");
  assert.equal(sandbox.metadata.annotations?.["alasio.dev/conversation"], "telegram:42");
  assert.equal(sandbox.metadata.labels?.["alasio.dev/workload"], "folder");
  const [bayma] = sandbox.spec.podTemplate.spec?.containers ?? [];
  assert.ok(bayma, "the pod runs bayma");
  assert.deepEqual(bayma.args?.slice(-4), ["--token-file", "/run/alasio/bayma/token", "--state-dir", "/home/op/.alasio/bayma/claude/telegram-42"]);
  const env = Object.fromEntries((bayma.env ?? []).map(({ name, value }) => [name, value]));
  assert.equal(env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"], "http://collector:4318/v1/traces");
  assert.match(env["OTEL_RESOURCE_ATTRIBUTES"] ?? "", /alasio\.conversation\.id=telegram%3A42/u);
});

test("each harness and conversation gets its own bayma and state directory", () => {
  assert.notEqual(hostBaymaName("codex", "telegram:1"), hostBaymaName("claude", "telegram:1"));
  assert.notEqual(hostBaymaName("claude", "telegram:2"), hostBaymaName("claude", "telegram:1"));
  assert.equal(hostBaymaStateDir(host, "codex", "telegram:1"), "/home/op/.alasio/bayma/codex/telegram-1");
  assert.equal(hostBaymaStateDir(host, "claude", "../x"), "/home/op/.alasio/bayma/claude/x");
});

test("a deployment without the host profile offers no folder bayma", () => {
  assert.equal(createHostBayma({ templates: { sessions, host: null } }), null);
});

test("alasio adds bayma to Claude Code's servers, in its shape", () => {
  assert.deepEqual(claudeMcpServers(BAYMA), { bayma: BAYMA });
});

test("Codex threads add bayma over HTTP and leave the operator's servers and apps connector on", () => {
  const config = buildCodexThreadConfig({ codexEnv: {}, bayma: BAYMA });
  assert.deepEqual(config.mcp_servers, { bayma: codexMcpServer(BAYMA) });
  assert.deepEqual(codexMcpServer(BAYMA), { url: BAYMA.url, http_headers: BAYMA.headers, startup_timeout_sec: 60 });
  // The config's type has no `features`; this pins that none is sent at all.
  assert.equal(Reflect.get(config, "features"), undefined);
});
