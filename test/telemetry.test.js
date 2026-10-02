import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { InMemoryLogRecordExporter, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { MetricReader } from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

// alasio records through the OpenTelemetry API; an SDK registered before alasio's modules
// load (as src/index.js does) receives it. Here it keeps everything in memory.
// The tests set the telemetry variables they mean; none come from the environment that
// runs them.
for (const name of Object.keys(process.env)) {
  if (/^(OTEL_.*|TRACEPARENT|TRACESTATE)$/u.test(name)) delete process.env[name];
}
class CollectingReader extends MetricReader {
  async onForceFlush() {}
  async onShutdown() {}
}
const spans = new InMemorySpanExporter();
const logRecords = new InMemoryLogRecordExporter();
const metricReader = new CollectingReader();
new NodeSDK({
  autoDetectResources: false,
  spanProcessors: [new SimpleSpanProcessor(spans)],
  logRecordProcessors: [new SimpleLogRecordProcessor({ exporter: logRecords })],
  metricReaders: [metricReader],
  instrumentations: [],
}).start();

const { claudeTelemetryEnv } = await import("../src/harness/claude/telemetry.js");
const { buildClaudeEnv } = await import("../src/harness/claude/env.js");
const { buildCodexEnv } = await import("../src/codex/env.js");
const { codexTelemetryArgs, codexTelemetryEnv } = await import("../src/codex/app-server/telemetry.js");
const { AppServerRpcClient } = await import("../src/codex/app-server/rpc-client.js");
const { hostBaymaManifest } = await import("../src/mcp/bayma.js");
const { TurnController } = await import("../src/codex/turn-controller.js");
const { createHarnessRegistry } = await import("../src/harness/index.js");
const { CLAUDE_HARNESS, CODEX_HARNESS } = await import("../src/harness/names.js");
const { SqliteStore } = await import("../src/persistence/store.js");
const { createLogger } = await import("../src/shared/log.js");
const { inSpan, resolveTelemetry, sharedResourceAttributes, withoutTelemetry } = await import("../src/telemetry/index.js");
const { Client } = await import("../src/telegram/client.js");
const { TelegramOutbox } = await import("../src/telegram/outbox.js");

const ENDPOINT = { OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318/" };

function finishedSpan(name) {
  const span = spans.getFinishedSpans().findLast((candidate) => candidate.name === name);
  assert.ok(span, `no finished span ${name}`);
  return span;
}

async function metricPoints(name) {
  const { resourceMetrics } = await metricReader.collect();
  return resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics)
    .filter((metric) => metric.descriptor.name === name)
    .flatMap((metric) => metric.dataPoints);
}

function withEnv(overrides, run) {
  const saved = Object.fromEntries(Object.keys(overrides).map((name) => [name, process.env[name]]));
  Object.assign(process.env, overrides);
  try {
    return run();
  } finally {
    for (const [name, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

test("telemetry is off without an endpoint and otherwise follows the standard variables", () => {
  assert.deepEqual(resolveTelemetry({}), { traces: null, metrics: null, logs: null });

  const shared = resolveTelemetry({ ...ENDPOINT, OTEL_EXPORTER_OTLP_HEADERS: "authorization=Bearer%20x" });
  assert.deepEqual(shared.traces, { endpoint: "http://collector:4318/v1/traces", protocol: "http/protobuf", headers: "authorization=Bearer%20x" });
  assert.equal(shared.logs.endpoint, "http://collector:4318/v1/logs");

  const own = resolveTelemetry({ OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "http://metrics:9090/otlp/v1/metrics" });
  assert.deepEqual(own, { traces: null, metrics: { endpoint: "http://metrics:9090/otlp/v1/metrics", protocol: "http/protobuf", headers: null }, logs: null });

  const grpc = resolveTelemetry({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4317", OTEL_EXPORTER_OTLP_PROTOCOL: "grpc" });
  assert.equal(grpc.traces.endpoint, "http://collector:4317");

  assert.deepEqual(resolveTelemetry({ ...ENDPOINT, OTEL_SDK_DISABLED: "true" }), { traces: null, metrics: null, logs: null });
  const someOff = resolveTelemetry({ ...ENDPOINT, OTEL_TRACES_EXPORTER: "none", OTEL_LOGS_EXPORTER: "console" });
  assert.equal(someOff.traces, null);
  assert.equal(someOff.logs, null);
  assert.ok(someOff.metrics);
});

test("children get no telemetry setting of alasio's as it is", () => {
  assert.deepEqual(
    withoutTelemetry({ PATH: "/bin", OTEL_SERVICE_NAME: "alasio", OTEL_EXPORTER_OTLP_HEADERS: "secret", TRACEPARENT: "00-1-2-01" }),
    { PATH: "/bin" },
  );
  assert.equal(sharedResourceAttributes({ OTEL_RESOURCE_ATTRIBUTES: "service.name=alasio,deployment.environment.name=prod" }), "deployment.environment.name=prod");
  assert.equal(sharedResourceAttributes({ OTEL_RESOURCE_ATTRIBUTES: "service.name=alasio" }), null);
  withEnv({ ...ENDPOINT, OTEL_SERVICE_NAME: "alasio", TRACEPARENT: "00-1-2-01" }, () => {
    for (const env of [buildClaudeEnv(), buildCodexEnv()]) {
      assert.equal(Object.keys(env).some((name) => name.startsWith("OTEL_") || name === "TRACEPARENT"), false);
    }
  });
});

test("Claude Code exports each signal alasio exports, labelled with its conversation", () => {
  assert.deepEqual(claudeTelemetryEnv({ conversationId: "telegram:7" }, {}), {});
  const env = claudeTelemetryEnv({ conversationId: "telegram:7" }, {
    ...ENDPOINT,
    OTEL_LOGS_EXPORTER: "none",
    OTEL_RESOURCE_ATTRIBUTES: "service.name=alasio,deployment.environment.name=prod",
    OTEL_LOG_USER_PROMPTS: "1",
  });
  assert.deepEqual(env, {
    CLAUDE_CODE_ENABLE_TELEMETRY: "1",
    CLAUDE_CODE_ENHANCED_TELEMETRY_BETA: "1",
    OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE: "cumulative",
    OTEL_RESOURCE_ATTRIBUTES: "deployment.environment.name=prod,alasio.conversation.id=telegram%3A7",
    OTEL_TRACES_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "http://collector:4318/v1/traces",
    OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: "http/protobuf",
    OTEL_METRICS_EXPORTER: "otlp",
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "http://collector:4318/v1/metrics",
    OTEL_EXPORTER_OTLP_METRICS_PROTOCOL: "http/protobuf",
    OTEL_LOGS_EXPORTER: "none",
    OTEL_LOG_USER_PROMPTS: "1",
  });
});

test("a folder's bayma exports where alasio does, labelled with its conversation", () => {
  const profile = { namespace: "alasio-host", port: 7290, stateRoot: "/s", podTemplate: { spec: { containers: [{ name: "bayma", args: [] }] } } };
  const envOf = (env) => Object.fromEntries(
    hostBaymaManifest({ harness: CLAUDE_HARNESS, threadKey: "telegram:7", profile, env }).spec.podTemplate.spec.containers[0].env.map(({ name, value }) => [name, value]),
  );
  assert.deepEqual(envOf({ PATH: "/bin" }), {});
  const env = envOf({ ...ENDPOINT, OTEL_METRICS_EXPORTER: "none" });
  assert.equal(env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "http://collector:4318/v1/traces");
  assert.equal(env.OTEL_METRICS_EXPORTER, "none");
  assert.equal(env.OTEL_RESOURCE_ATTRIBUTES, "alasio.conversation.id=telegram%3A7");
});

test("Codex's app-server exports each signal alasio exports through its own config", () => {
  assert.deepEqual(codexTelemetryArgs({}), []);
  assert.deepEqual(codexTelemetryArgs({ ...ENDPOINT, OTEL_METRICS_EXPORTER: "none", OTEL_EXPORTER_OTLP_HEADERS: "x-scope=a%2Cb" }), [
    "-c", 'otel.exporter={"otlp-http"={"endpoint"="http://collector:4318/v1/logs","headers"={"x-scope"="a,b"},"protocol"="binary"}}',
    "-c", 'otel.trace_exporter={"otlp-http"={"endpoint"="http://collector:4318/v1/traces","headers"={"x-scope"="a,b"},"protocol"="binary"}}',
  ]);
  assert.deepEqual(codexTelemetryArgs({ OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: "http://collector:4317", OTEL_EXPORTER_OTLP_PROTOCOL: "grpc" }), [
    "-c", 'otel.trace_exporter={"otlp-grpc"={"endpoint"="http://collector:4317","headers"={}}}',
  ]);
  assert.deepEqual(codexTelemetryEnv({ OTEL_RESOURCE_ATTRIBUTES: "deployment.environment.name=prod" }), { OTEL_RESOURCE_ATTRIBUTES: "deployment.environment.name=prod" });
});

test("log lines become records of the trace they are written in", async () => {
  const log = createLogger("telemetry-test");
  const span = await inSpan("test.logging", {}, async (active) => {
    log.warn("something to see", { "alasio.conversation.id": "telegram:1" });
    return active;
  });
  const record = logRecords.getFinishedLogRecords().findLast((candidate) => candidate.body === "something to see");
  assert.equal(record.severityText, "WARN");
  assert.equal(record.instrumentationScope.name, "telemetry-test");
  assert.equal(record.attributes["alasio.conversation.id"], "telegram:1");
  assert.equal(record.spanContext.traceId, span.spanContext().traceId);
});

test("Bot API calls are client spans that never carry the bot token", async (t) => {
  const responses = [
    { status: 429, body: { ok: false, parameters: { retry_after: 0.001 } } },
    { status: 200, body: { ok: true, result: { message_id: 5 } } },
    { status: 200, body: { ok: true, result: [] } },
  ];
  t.mock.method(globalThis, "fetch", async () => {
    const { status, body } = responses.shift();
    return new Response(JSON.stringify(body), { status });
  });
  const client = new Client("123:SECRET-TOKEN");
  spans.reset();
  await client.sendMessage(1, "hi", { format: "plain" });
  await client.getUpdates({ offset: 1 });
  const [call] = spans.getFinishedSpans();
  assert.equal(spans.getFinishedSpans().length, 1, "the long poll is not a span");
  assert.equal(call.name, "telegram/sendMessage");
  assert.equal(call.kind, SpanKind.CLIENT);
  assert.equal(call.attributes["rpc.method"], "sendMessage");
  assert.equal(call.attributes["http.response.status_code"], 200);
  assert.deepEqual(call.events.map((event) => event.name), ["rate_limited"]);
  assert.equal(JSON.stringify({ attributes: call.attributes, events: call.events }).includes("SECRET"), false);
  const [duration] = (await metricPoints("rpc.client.call.duration")).filter((point) => point.attributes["rpc.method"] === "sendMessage");
  assert.equal(duration.value.count, 1);
});

test("Codex app-server requests carry their span's trace context", async () => {
  const written = [];
  const client = new AppServerRpcClient({ log: createLogger("test"), onNotification: () => undefined, onFailure: () => undefined });
  client.process = { child: { stdin: { write: (line) => written.push(JSON.parse(line)) } }, stop: () => undefined };
  const result = await inSpan("test.codex", {}, async () => {
    const pending = client.request("thread/start", { cwd: "/tmp" });
    client.handleLine(JSON.stringify({ id: written[0].id, result: { thread: { id: "t-1" } } }));
    return await pending;
  });
  assert.deepEqual(result, { thread: { id: "t-1" } });
  const request = finishedSpan("codex/thread/start");
  assert.equal(request.attributes["rpc.system.name"], "jsonrpc");
  assert.equal(request.attributes["rpc.jsonrpc.request_id"], String(written[0].id));
  assert.equal(written[0].trace.traceparent, `00-${request.spanContext().traceId}-${request.spanContext().spanId}-01`);
});

test("a prompt's update, turn, and reply delivery are one trace, however long the prompt waits", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-telemetry-"));
  const folder = join(root, "repo");
  mkdirSync(folder);
  execFileSync("git", ["init", "-q"], { cwd: folder });
  const store = new SqliteStore(root, join(root, "alasio.sqlite"));
  try {
    const conversationId = store.upsertConversation({ chatId: "42", user: { id: 42 } });
    store.setActiveHarness(conversationId, CODEX_HARNESS);
    store.setWorkingDirectory(conversationId, folder);
    const sent = [];
    const telegram = {
      async sendMessage(...args) {
        sent.push(args);
        return [{ message_id: sent.length }];
      },
      async editMessageText() {},
    };
    const harness = (name) => ({
      name,
      displayName: name,
      supportsGoals: false,
      supportsWarmup: false,
      supportsSteer: false,
      sessions: {},
      async executeTurn() {
        return {
          blockSequence: [{ type: "text", phase: "final_answer", content: "done" }],
          sessionId: "thread-1",
          pendingResponseId: "pending-1",
          interrupted: false,
          responseCompleted: true,
        };
      },
      shutdown() {},
    });
    const outbox = new TelegramOutbox({ client: telegram, store, log: createLogger("test") });
    const turns = new TurnController({
      config: { workspaceRoot: root },
      client: telegram,
      store,
      outbox,
      activeQueries: new Map(),
      workflowWaits: new Map(),
      workflowWakeEvents: new Map(),
      isStopping: () => false,
      harnesses: createHarnessRegistry({ config: {}, overrides: { [CODEX_HARNESS]: harness(CODEX_HARNESS), [CLAUDE_HARNESS]: harness(CLAUDE_HARNESS) } }),
    });
    spans.reset();
    await inSpan("alasio.update", { parent: null }, () => turns.processPrompt({ conversationId, chatId: "42", messageId: "9", text: "hello", filePaths: [] }));
    await turns.promptWorkers.get(conversationId);
    await outbox.flushDue();

    const update = finishedSpan("alasio.update");
    const turn = finishedSpan("alasio.turn");
    const delivery = finishedSpan("alasio.delivery");
    const traceId = update.spanContext().traceId;
    assert.equal(turn.spanContext().traceId, traceId);
    assert.equal(turn.parentSpanContext.spanId, update.spanContext().spanId);
    assert.equal(turn.attributes["alasio.harness"], CODEX_HARNESS);
    assert.equal(turn.attributes["alasio.turn.outcome"], "completed");
    assert.equal(turn.attributes["alasio.session.id"], "thread-1");
    assert.equal(delivery.spanContext().traceId, traceId);
    assert.equal(delivery.parentSpanContext.spanId, turn.spanContext().spanId);
    assert.ok(sent.some(([, text]) => text === "done"));

    const [duration] = (await metricPoints("alasio.turn.duration")).filter((point) => point.attributes["alasio.turn.outcome"] === "completed");
    assert.equal(duration.attributes["alasio.harness"], CODEX_HARNESS);
    assert.equal((await metricPoints("alasio.prompt.wait")).length, 1);
    assert.equal((await metricPoints("alasio.delivery.lag")).length, 1);
    assert.equal((await metricPoints("alasio.turn.active"))[0].value, 0);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("a deferred delivery records what stopped it and keeps its trace", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-telemetry-"));
  const store = new SqliteStore(root, join(root, "alasio.sqlite"));
  try {
    store.upsertConversation({ chatId: "43", user: { id: 43 } });
    const failing = { async sendMessage() { throw new Error("Telegram is down"); } };
    const outbox = new TelegramOutbox({ client: failing, store, log: createLogger("test") });
    spans.reset();
    const turn = await inSpan("test.turn", { parent: null }, async (span) => {
      outbox.enqueueText({ chatId: "43", text: "reply" });
      await outbox.flushDue();
      return span;
    });
    const delivery = finishedSpan("alasio.delivery");
    assert.equal(delivery.parentSpanContext.spanId, turn.spanContext().spanId);
    assert.equal(delivery.status.code, SpanStatusCode.ERROR);
    assert.equal(delivery.attributes["alasio.delivery.attempt"], 1);
    assert.equal(store.getPendingOutboxCount(), 1);
    const [pending] = (await metricPoints("alasio.outbox.pending")).filter((point) => point.value === 1);
    assert.ok(pending);
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});
