import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";

import { buildCodexThreadConfig, codexMcpServer } from "../src/codex/thread-config.ts";
import { claudeMcpServers } from "../src/harness/claude/mcp.ts";
import { KubeClient } from "../src/kube/client.ts";
import type { HostProfile } from "../src/kube/config.ts";
import {
  type BaymaMcpServer,
  folderBayma,
  HostBayma,
  hostBaymaManifest,
  hostBaymaName,
  hostBaymaStateDir,
  noFolderBayma,
} from "../src/mcp/bayma.ts";

const host: HostProfile = {
  namespace: "alasio-host",
  port: 7290,
  stateRoot: "/home/op/.alasio/bayma",
  podTemplate: { spec: { containers: [{ name: "bayma", image: "bayma@sha256:1", args: ["mcp-http", "--port", "7290"] }] } },
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

test("a deployment without the host profile offers no folder bayma", async () => {
  const none = Effect.runSync(folderBayma);
  assert.equal(none, noFolderBayma);
  const refused = await Effect.runPromise(Effect.flip(none({ harness: "claude", threadKey: "telegram:42" })));
  assert.equal(refused.message, "this deployment offers no folder workspaces: its templates have no host profile");
});

test("a folder conversation's bayma is its host Sandbox's endpoint, in Claude Code's shape", async () => {
  const made: string[] = [];
  const kube = KubeClient.of({
    read: () => Effect.succeed(null),
    create: (object) =>
      Effect.sync(() => {
        made.push(`${object.kind}/${object.metadata?.name}`);
        // Made as asked, ready at once, as agent-sandbox would have it by the next look.
        return { ...object, metadata: { ...object.metadata, uid: "u1" }, status: { conditions: [{ type: "Ready", status: "True" }] } };
      }),
    replace: () => Effect.die("alasio replaces no object"),
    patch: () => Effect.die("nothing is patched"),
    remove: () => Effect.die("nothing is removed"),
    exec: () => Effect.die("nothing is run"),
  });
  const layer = HostBayma.layer(host, {}).pipe(
    Layer.provide(Layer.succeed(KubeClient, kube)),
    Layer.provide(Layer.succeed(FetchHttpClient.Fetch, async () => new Response(null, { status: 400 }))),
  );
  const server = await Effect.runPromise(Effect.scoped(Layer.build(layer).pipe(
    Effect.flatMap((services) => folderBayma.pipe(Effect.flatMap((ensure) => ensure({ harness: "claude", threadKey: "telegram:42" })), Effect.provideContext(services))),
  )));
  const name = hostBaymaName("claude", "telegram:42");
  assert.deepEqual(made, [`Sandbox/${name}`, `Secret/${name}-bayma-token`]);
  assert.equal(server.type, "http");
  assert.equal(server.url, `http://${name}.alasio-host.svc:7290/mcp`);
  assert.match(server.headers["Authorization"] ?? "", new RegExp(`^Bearer ${name}\\.`, "u"));
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
