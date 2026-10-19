import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { test } from "node:test";

import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Effect, Layer } from "effect";

import { type Held, LiveBindings } from "../src/binder/live.ts";
import { bindingToken, mintToken } from "../src/binder/tokens.ts";
import { authenticateBinding, sessionResource } from "../src/receiver/receiver.ts";
import { sessionKey } from "../src/sandbox/names.ts";
import { sandboxBaymaTelemetryEnv } from "../src/sandbox/telemetry.ts";
import { makeRateLimiter, serveTelemetryReceiver } from "../src/sandbox/telemetry-receiver.ts";
import type { ForwardResult } from "../src/telemetry/forward.ts";

const bindingKey = randomBytes(32);

function traceRequest(): Buffer {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "bayma" }), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan("work").end();
  const request = ProtobufTraceSerializer.serializeRequest(exporter.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

test("the receiver takes an instance's telemetry with its binding's token while its binding is held, stamped with its workspace, and asks again while the bindings are not known", async () => {
  const bindings = new Map<string, Held>([["held-uid", "held"], ["gone-uid", "gone"], ["unknown-uid", "unknown"]]);
  // The bindings of main's sessions' namespace, and of the folder namespace; another is of no environment's.
  const live = Layer.succeed(LiveBindings, LiveBindings.of({ holds: (_key, binding, namespace) => Effect.succeed(namespace.endsWith("sessions") || namespace === "alasio-host" ? (bindings.get(binding) ?? "gone") : "gone") }));
  const exported: Buffer[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(_signal: string, _encoding: string, body: Buffer): Promise<ForwardResult> {
      exported.push(body);
      return { ok: true };
    },
  };
  const authenticate = authenticateBinding(bindingKey, (volumeId, namespace) =>
    Effect.succeed(sessionResource(volumeId, namespace === "alasio-branch-try-sessions" ? { name: "try", namespace, parent: "main" } : { name: "main", namespace, parent: null }, {})));
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      authenticate: (token) => Effect.provide(authenticate(token), live),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000_000 }),
    });
    const post = (token: string) =>
      fetch(`http://127.0.0.1:${receiver.port}/v1/traces`, { method: "POST", headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${token}` }, body: traceRequest() });
    yield* Effect.promise(async () => {
      const key = sessionKey("fs-abc123");
      const binding = (uid: string, namespace = "alasio-sessions") => bindingToken(bindingKey, { key, binding: uid, namespace });
      assert.equal((await post(binding("held-uid"))).status, 200);
      assert.match(exported[0]?.toString("latin1") ?? "", /fs-abc123/u);
      assert.doesNotMatch(exported[0]?.toString("latin1") ?? "", /alasio\.branch/u);
      // A branch environment's session's, tagged with its branch.
      assert.equal((await post(binding("held-uid", "alasio-branch-try-sessions"))).status, 200);
      assert.match(exported[1]?.toString("latin1") ?? "", /alasio\.branch[^]*try/u);
      // A folder conversation's instance's, stamped with its conversation and its harness.
      assert.equal((await post(bindingToken(bindingKey, { key: "folder:claude:telegram:42", binding: "held-uid", namespace: "alasio-host" }))).status, 200);
      assert.match(exported[2]?.toString("latin1") ?? "", /alasio\.conversation\.id[^]*telegram:42/u);
      assert.match(exported[2]?.toString("latin1") ?? "", /alasio\.harness[^]*claude/u);
      const refused = [
        binding("gone-uid"),
        // One of a namespace of no environment's; one that names no namespace.
        binding("held-uid", "elsewhere"),
        mintToken(bindingKey, { key, binding: "held-uid" }),
        // A caller's, of no binding; one another key signed; one of no session's workspace.
        mintToken(bindingKey, { key, expires: Date.now() + 60_000 }),
        bindingToken(randomBytes(32), { key, binding: "held-uid", namespace: "alasio-sessions" }),
        bindingToken(bindingKey, { key: "folder:claude:conversation", binding: "gone-uid", namespace: "alasio-host" }),
        bindingToken(bindingKey, { key: "workspace:somewhere", binding: "held-uid", namespace: "alasio-sessions" }),
      ];
      for (const token of refused) assert.equal((await post(token)).status, 401, token);
      const unknown = await post(binding("unknown-uid"));
      assert.deepEqual([unknown.status, unknown.headers.get("retry-after")], [503, "1"]);
      assert.equal(exported.length, 3);
    });
  })));
});

test("a bound pod's bayma is told to export each signal the gateway exports, in its protocol, and none where the gateway exports none", () => {
  assert.deepEqual(sandboxBaymaTelemetryEnv({}), {});
  const settings = sandboxBaymaTelemetryEnv({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318", OTEL_METRICS_EXPORTER: "none" });
  assert.equal(settings["OTEL_TRACES_EXPORTER"], "otlp");
  assert.equal(settings["OTEL_METRICS_EXPORTER"], "none");
  assert.ok(settings["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"]);
  // Its endpoint, and its token, the sandbox's own.
  assert.ok(!Object.keys(settings).some((name) => name === "OTEL_EXPORTER_OTLP_ENDPOINT" || name.includes("HEADERS")), JSON.stringify(settings));
});

test("a session's rate is its own in each environment, as a branch's fork of main's session keeps its volume id", async () => {
  const live = Layer.succeed(LiveBindings, LiveBindings.of({ holds: () => Effect.succeed("held") }));
  const forwarder = { protocols: { traces: "http/protobuf" }, export: async (): Promise<ForwardResult> => ({ ok: true }) };
  const authenticate = authenticateBinding(bindingKey, (volumeId) => Effect.succeed(sessionResource(volumeId, null, {})));
  const body = traceRequest();
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    // A budget of one request, refilled slower than the test runs.
    const receiver = yield* serveTelemetryReceiver({ port: 0, host: "127.0.0.1", forwarder, authenticate: (token) => Effect.provide(authenticate(token), live), limiter: yield* makeRateLimiter({ rate: 1, burst: body.length }) });
    yield* Effect.promise(async () => {
      const key = sessionKey("fs-shared");
      const post = (namespace: string) =>
        fetch(`http://127.0.0.1:${receiver.port}/v1/traces`, { method: "POST", headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${bindingToken(bindingKey, { key, binding: "uid", namespace })}` }, body });
      assert.equal((await post("alasio-sessions")).status, 200);
      assert.equal((await post("alasio-sessions")).status, 429);
      // The branch's, of the same volume id, on its own budget.
      assert.equal((await post("alasio-branch-try-sessions")).status, 200);
    });
  })));
});
