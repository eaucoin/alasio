import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { Message } from "@grammyjs/types";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import { InMemoryLogRecordExporter, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { DataPointType, type MetricData, MetricReader } from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import type { RequestId } from "../.types/codex/index.js";
import type { Harness } from "../src/harness/index.ts";
import type { HarnessName } from "../src/harness/names.ts";
import type { HostProfile } from "../src/kube/config.ts";

// alasio records through the OpenTelemetry API; an SDK registered before alasio's modules
// load (as src/index.ts does) receives it. Here it keeps everything in memory.
// The tests set the telemetry variables they mean; none come from the environment that
// runs them.
for (const name of Object.keys(process.env)) {
  if (/^(OTEL_.*|TRACEPARENT|TRACESTATE)$/u.test(name)) delete process.env[name];
}
class CollectingReader extends MetricReader {
  protected override async onForceFlush(): Promise<void> {}
  protected override async onShutdown(): Promise<void> {}
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

const { claudeTelemetryEnv } = await import("../src/harness/claude/telemetry.ts");
const { buildClaudeEnv } = await import("../src/harness/claude/env.ts");
const { buildCodexEnv } = await import("../src/codex/env.ts");
const { codexTelemetryArgs, codexTelemetryEnv } = await import("../src/codex/app-server/telemetry.ts");
const { AppServerRpcClient } = await import("../src/codex/app-server/rpc-client.ts");
const { hostBaymaManifest } = await import("../src/mcp/bayma.ts");
const { TurnController } = await import("../src/codex/turn-controller.ts");
const { createHarnessRegistry } = await import("../src/harness/index.ts");
const { CLAUDE_HARNESS, CODEX_HARNESS } = await import("../src/harness/names.ts");
const { SqliteStore } = await import("../src/persistence/store.ts");
const { createLogger } = await import("../src/shared/log.ts");
const { inSpan, resolveTelemetry, sharedResourceAttributes, TracingLayer, withoutTelemetry, withAlasioSpan, withRpcCall } = await import("../src/telemetry/index.ts");
const { Effect, Schema } = await import("effect");
const { Client } = await import("../src/telegram/client.ts");
const { TelegramOutbox } = await import("../src/telegram/outbox.ts");

type SendMessageArgs = Parameters<InstanceType<typeof Client>["sendMessage"]>;

const ENDPOINT = { OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318/" };

function finishedSpan(name: string): ReadableSpan {
  const span = spans.getFinishedSpans().findLast((candidate) => candidate.name === name);
  assert.ok(span, `no finished span ${name}`);
  return span;
}

/** A point of a metric, of whichever kind the metric is. */
type MetricPoint = MetricData["dataPoints"][number];

async function metricData(name: string): Promise<MetricData[]> {
  const { resourceMetrics } = await metricReader.collect();
  return resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics)
    .filter((metric) => metric.descriptor.name === name);
}

async function metricPoints(name: string): Promise<MetricPoint[]> {
  return (await metricData(name)).flatMap((metric): MetricPoint[] => metric.dataPoints);
}

/** The points of a histogram metric. */
async function histogramPoints(name: string) {
  return (await metricData(name)).flatMap((metric) => (metric.dataPointType === DataPointType.HISTOGRAM ? metric.dataPoints : []));
}

function withEnv<T>(overrides: Readonly<Record<string, string>>, run: () => T): T {
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
  assert.equal(shared.logs?.endpoint,"http://collector:4318/v1/logs");

  const own = resolveTelemetry({ OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: "http://metrics:9090/otlp/v1/metrics" });
  assert.deepEqual(own, { traces: null, metrics: { endpoint: "http://metrics:9090/otlp/v1/metrics", protocol: "http/protobuf", headers: null }, logs: null });

  const grpc = resolveTelemetry({ OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4317", OTEL_EXPORTER_OTLP_PROTOCOL: "grpc" });
  assert.equal(grpc.traces?.endpoint,"http://collector:4317");

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
  const profile: HostProfile = { namespace: "alasio-host", port: 7290, stateRoot: "/s", podTemplate: { spec: { containers: [{ name: "bayma", args: [] }] } } };
  const envOf = (env: NodeJS.ProcessEnv) => {
    const [bayma] = hostBaymaManifest({ harness: CLAUDE_HARNESS, threadKey: "telegram:7", profile, env }).spec.podTemplate.spec?.containers ?? [];
    assert.ok(bayma?.env);
    return Object.fromEntries(bayma.env.map(({ name, value }) => [name, value]));
  };
  assert.deepEqual(envOf({ PATH: "/bin" }), {});
  const env = envOf({ ...ENDPOINT, OTEL_METRICS_EXPORTER: "none" });
  assert.equal(env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"], "http://collector:4318/v1/traces");
  assert.equal(env["OTEL_METRICS_EXPORTER"], "none");
  assert.equal(env["OTEL_RESOURCE_ATTRIBUTES"], "alasio.conversation.id=telegram%3A7");
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
  assert.ok(record);
  assert.equal(record.severityText, "WARN");
  assert.equal(record.instrumentationScope.name, "telemetry-test");
  assert.equal(record.attributes["alasio.conversation.id"], "telegram:1");
  assert.equal(record.spanContext?.traceId, span.spanContext().traceId);
});

test("Bot API calls are client spans that never carry the bot token", async (t) => {
  const responses: { status: number; body: object }[] = [
    { status: 429, body: { ok: false, parameters: { retry_after: 0.001 } } },
    { status: 200, body: { ok: true, result: { message_id: 5 } } },
    { status: 200, body: { ok: true, result: [] } },
  ];
  t.mock.method(globalThis, "fetch", async () => {
    const next = responses.shift();
    assert.ok(next, "a response for every call");
    return new Response(JSON.stringify(next.body), { status: next.status });
  });
  const client = new Client("123:SECRET-TOKEN");
  spans.reset();
  await client.sendMessage(1, "hi", { format: "plain" });
  await client.getUpdates({ offset: 1 });
  const [call] = spans.getFinishedSpans();
  assert.equal(spans.getFinishedSpans().length, 1, "the long poll is not a span");
  assert.ok(call);
  assert.equal(call.name, "telegram/sendMessage");
  assert.equal(call.kind, SpanKind.CLIENT);
  assert.equal(call.attributes["rpc.method"], "sendMessage");
  assert.equal(call.attributes["http.response.status_code"], 200);
  assert.deepEqual(call.events.map((event) => event.name), ["rate_limited"]);
  assert.equal(JSON.stringify({ attributes: call.attributes, events: call.events }).includes("SECRET"), false);
  const [duration] = (await histogramPoints("rpc.client.call.duration")).filter((point) => point.attributes["rpc.method"] === "sendMessage");
  assert.equal(duration?.value.count, 1);
});

/** A request the client wrote to the app-server, as far as this test reads it. */
interface WrittenRequest {
  readonly id: RequestId;
  readonly trace?: { readonly traceparent: string };
}

test("Codex app-server requests carry their span's trace context", async () => {
  const written: WrittenRequest[] = [];
  const client = new AppServerRpcClient({ log: createLogger("test"), onNotification: () => undefined, onFailure: () => undefined });
  client.process = {
    child: {
      killed: false,
      stdin: {
        write: (line: string) => {
          written.push(JSON.parse(line));
          return true;
        },
      },
    },
    stop: () => undefined,
  };
  const result = await inSpan("test.codex", {}, async () => {
    const pending = client.request("thread/start", { cwd: "/tmp" });
    const [start] = written;
    assert.ok(start);
    client.handleLine(JSON.stringify({ id: start.id, result: { thread: { id: "t-1" } } }));
    return await pending;
  });
  assert.deepEqual(result, { thread: { id: "t-1" } });
  const request = finishedSpan("codex/thread/start");
  const [sent] = written;
  assert.ok(sent);
  assert.equal(request.attributes["rpc.system.name"], "jsonrpc");
  assert.equal(request.attributes["rpc.jsonrpc.request_id"], String(sent.id));
  assert.equal(sent.trace?.traceparent, `00-${request.spanContext().traceId}-${request.spanContext().spanId}-01`);
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
    const sent: SendMessageArgs[] = [];
    const telegram = {
      async sendMessage(...args: SendMessageArgs): Promise<Message[]> {
        sent.push(args);
        return [{ message_id: sent.length, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } }];
      },
      // What the Bot API answers for an edit it makes without returning the message.
      async editMessageText(): Promise<true> {
        return true;
      },
    };
    // A harness that only runs turns: what else a harness does, a turn does not use.
    const harness = (name: HarnessName): Harness => {
      const unused = (): never => {
        throw new Error(`the test's ${name} harness only runs turns`);
      };
      return {
        name,
        displayName: name,
        supportsGoals: false,
        supportsWarmup: false,
        supportsSteer: false,
        sessions: {
          listSessions: unused,
          getTotalSessionPages: unused,
          getSessionByNumber: unused,
          getSessionLastMessage: unused,
          listSessionMessages: unused,
          getTotalRewindPages: unused,
          createForkedSession: unused,
        },
        startFreshSession: unused,
        warmSession: unused,
        listModels: unused,
        defaultModelChoice: unused,
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
      };
    };
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
    await inSpan("alasio.update", { parent: null }, () => turns.processPrompt({ conversationId, chatId: "42", messageId: 9, text: "hello", filePaths: [] }));
    // The conversation's prompt worker, already running: it resolves once the prompt is done.
    await turns.scheduleConversation(conversationId);
    await outbox.flushDue();

    const update = finishedSpan("alasio.update");
    const turn = finishedSpan("alasio.turn");
    const delivery = finishedSpan("alasio.delivery");
    const traceId = update.spanContext().traceId;
    assert.equal(turn.spanContext().traceId, traceId);
    assert.equal(turn.parentSpanContext?.spanId, update.spanContext().spanId);
    assert.equal(turn.attributes["alasio.harness"], CODEX_HARNESS);
    assert.equal(turn.attributes["alasio.turn.outcome"], "completed");
    assert.equal(turn.attributes["alasio.session.id"], "thread-1");
    assert.equal(delivery.spanContext().traceId, traceId);
    assert.equal(delivery.parentSpanContext?.spanId, turn.spanContext().spanId);
    assert.ok(sent.some(([, text]) => text === "done"));

    const [duration] = (await metricPoints("alasio.turn.duration")).filter((point) => point.attributes["alasio.turn.outcome"] === "completed");
    assert.equal(duration?.attributes["alasio.harness"], CODEX_HARNESS);
    assert.equal((await metricPoints("alasio.prompt.wait")).length, 1);
    assert.equal((await metricPoints("alasio.delivery.lag")).length, 1);
    const [active] = await metricPoints("alasio.turn.active");
    assert.ok(active);
    assert.equal(active.value, 0);
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
    assert.equal(delivery.parentSpanContext?.spanId, turn.spanContext().spanId);
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

class EffectProbeError extends Schema.TaggedError<EffectProbeError>()("EffectProbeError", { message: Schema.String }) {}

test("an effect's span is a span of alasio's, as inSpan makes one: named, kinded, attributed, and labelled with its failure", async () => {
  await Effect.runPromise(Effect.void.pipe(
    withAlasioSpan("alasio.effect.ok", { kind: SpanKind.CONSUMER, attributes: { "alasio.probe": "yes" } }),
    Effect.provide(TracingLayer),
  ));
  const ok = finishedSpan("alasio.effect.ok");
  assert.equal(ok.kind, SpanKind.CONSUMER);
  assert.equal(ok.attributes["alasio.probe"], "yes");
  assert.notEqual(ok.status.code, SpanStatusCode.ERROR);

  const failed = await Effect.runPromise(Effect.fail(new EffectProbeError({ message: "broke" })).pipe(
    withAlasioSpan("alasio.effect.failed"),
    Effect.flip,
    Effect.provide(TracingLayer),
  ));
  assert.equal(failed.message, "broke");
  const span = finishedSpan("alasio.effect.failed");
  assert.equal(span.status.code, SpanStatusCode.ERROR);
  assert.equal(span.attributes["error.type"], "EffectProbeError");
});

test("an effect's span continues a traceparent, starts a trace of its own for null, and is the parent of spans made inside it", async () => {
  const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
  await Effect.runPromise(Effect.void.pipe(withAlasioSpan("alasio.effect.continued", { parent: traceparent }), Effect.provide(TracingLayer)));
  const continued = finishedSpan("alasio.effect.continued");
  assert.equal(continued.spanContext().traceId, "0af7651916cd43dd8448eb211c80319c");
  assert.equal(continued.parentSpanContext?.spanId, "b7ad6b7169203331");

  await inSpan("alasio.outer", {}, async () => {
    await Effect.runPromise(Effect.void.pipe(withAlasioSpan("alasio.effect.root", { parent: null }), Effect.provide(TracingLayer)));
    await Effect.runPromise(Effect.promise(() => inSpan("alasio.effect.inner", {}, () => undefined)).pipe(
      withAlasioSpan("alasio.effect.nested"),
      Effect.provide(TracingLayer),
    ));
  });
  const outer = finishedSpan("alasio.outer");
  assert.notEqual(finishedSpan("alasio.effect.root").spanContext().traceId, outer.spanContext().traceId);
  const nested = finishedSpan("alasio.effect.nested");
  assert.equal(nested.parentSpanContext?.spanId, outer.spanContext().spanId);
  assert.equal(finishedSpan("alasio.effect.inner").parentSpanContext?.spanId, nested.spanContext().spanId);
});

test("an effect's call is a call of alasio's, as rpcCall records one: a client span and a duration labelled with its failure", async () => {
  await Effect.runPromise(Effect.fail(new EffectProbeError({ message: "refused" })).pipe(
    withRpcCall({ system: "telegram", service: "telegram", method: "effectProbe" }),
    Effect.ignore,
    Effect.provide(TracingLayer),
  ));
  const span = finishedSpan("telegram/effectProbe");
  assert.equal(span.kind, SpanKind.CLIENT);
  assert.deepEqual([span.attributes["rpc.system.name"], span.attributes["rpc.service"], span.attributes["rpc.method"]], ["telegram", "telegram", "effectProbe"]);
  assert.equal(span.attributes["error.type"], "EffectProbeError");
  const points = (await histogramPoints("rpc.client.call.duration")).filter((point) => point.attributes["rpc.method"] === "effectProbe");
  assert.equal(points.length, 1);
  assert.equal(points[0]?.attributes["error.type"], "EffectProbeError");
});
