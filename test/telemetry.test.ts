import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type TestContext, test } from "node:test";

import type { HookCallback, SDKMessage } from "@anthropic-ai/claude-agent-sdk";
import { context, type Context, propagation, ROOT_CONTEXT, type Span, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { InMemoryLogRecordExporter, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { DataPointType, type MetricData, MetricReader } from "@opentelemetry/sdk-metrics";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import type { ClaudeQueryFactory } from "../src/harness/claude/runtime.ts";
import type { Harness, TurnPersistence } from "../src/harness/index.ts";
import type { HarnessName } from "../src/harness/names.ts";
import type { HostProfile } from "../src/kube/config.ts";
// Types only: effect itself is loaded once the SDK is registered, below.
import type { Effect as EffectTypes } from "effect";
import type { Outbox as OutboxService } from "../src/telegram/outbox.ts";
import type { BotAnswer } from "./support/bot-api.ts";

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
const { makeAppServerRpc } = await import("../src/codex/app-server/rpc-client.ts");
const { appServerProcess } = await import("./support/app-server-process.ts");
const { hostBaymaManifest } = await import("../src/mcp/bayma.ts");
const { withServices } = await import("./support/turns.ts");
const { recordingTelegram, sentMessage } = await import("./support/telegram-calls.ts");
const { processPrompt } = await import("../src/operator/prompts.ts");
const { eventually } = await import("./support/wait.ts");
const { CLAUDE_HARNESS, CODEX_HARNESS } = await import("../src/harness/names.ts");
const { SqliteStore } = await import("../src/persistence/store.ts");
const { AlasioLoggerLayer, withLogScope } = await import("../src/shared/log.ts");
const { outsideTraces, resolveTelemetry, sharedResourceAttributes, TracingLayer, withoutTelemetry, withAlasioSpan, withRpcCall } = await import("../src/telemetry/index.ts");
const { makeClaudeLiveSessions } = await import("../src/harness/claude/live-sessions.ts");
const { ActiveTurns } = await import("../src/harness/active-turns.ts");
const { fakeQuery, initMessage, stamped, successResult } = await import("./support/claude-sdk.ts");
const { Deferred, Effect, Layer, ManagedRuntime, Schema } = await import("effect");
const { FetchHttpClient } = await import("effect/http");
const { Store } = await import("../src/persistence/store.ts");
const { TelegramClient } = await import("../src/telegram/client.ts");
const { Outbox } = await import("../src/telegram/outbox.ts");
const { botApiLayer, paramsOf } = await import("./support/bot-api.ts");

/**
 * The Outbox over `store` and a Bot API `answer` answers, made with the tracing alasio
 * runs with (its delivery loop runs with it too), until `t` ends: what runs effects on it.
 */
function outboxFor(t: TestContext, store: InstanceType<typeof SqliteStore>, answer: BotAnswer): <A, E>(effect: EffectTypes.Effect<A, E, OutboxService>) => Promise<A> {
  const outbox = ManagedRuntime.make(Outbox.layer.pipe(Layer.provide([botApiLayer(answer), Layer.succeed(Store, store)]), Layer.provideMerge(TracingLayer)));
  t.after(() => outbox.dispose());
  return (effect) => outbox.runPromise(effect);
}

const ENDPOINT = { OTEL_EXPORTER_OTLP_ENDPOINT: "http://collector:4318/" };

const otelTracer = trace.getTracer("telemetry-test");

/** `fn` in a span of OpenTelemetry's own, as an instrumented module (pg, http) makes one: the active span's child, or `parent`'s. */
async function inOtelSpan<T>(name: string, fn: (span: Span) => T | Promise<T>, parent: Context = context.active()): Promise<T> {
  return await otelTracer.startActiveSpan(name, {}, parent, async (span) => {
    try {
      return await fn(span);
    } finally {
      span.end();
    }
  });
}

/** The traceparent what runs now would hand on, as the Agent SDK hands one to Claude Code's process. */
function traceparentHere(): string | undefined {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier["traceparent"];
}

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
  await Effect.runPromise(Effect.logWarning("something to see").pipe(
    Effect.annotateLogs("alasio.conversation.id", "telegram:1"),
    withLogScope("telemetry-test"),
    withAlasioSpan("test.logging"),
    Effect.provide([AlasioLoggerLayer, TracingLayer]),
  ));
  const span = finishedSpan("test.logging");
  const record = logRecords.getFinishedLogRecords().findLast((candidate) => candidate.body === "something to see");
  assert.ok(record);
  assert.equal(record.severityText, "WARN");
  assert.equal(record.instrumentationScope.name, "telemetry-test");
  assert.equal(record.attributes["alasio.conversation.id"], "telegram:1");
  assert.equal(record.spanContext?.traceId, span.spanContext().traceId);
});

test("Bot API calls are client spans that never carry the bot token", async () => {
  const responses: { status: number; body: object }[] = [
    { status: 429, body: { ok: false, parameters: { retry_after: 0.001 } } },
    { status: 200, body: { ok: true, result: { message_id: 5 } } },
    { status: 200, body: { ok: true, result: [] } },
  ];
  const fetch = async (): Promise<Response> => {
    const next = responses.shift();
    assert.ok(next, "a response for every call");
    return new Response(JSON.stringify(next.body), { status: next.status });
  };
  spans.reset();
  await Effect.runPromise(Effect.gen(function*() {
    const client = yield* TelegramClient;
    yield* client.sendMessage(1, "hi", { format: "plain" });
    yield* client.getUpdates({ offset: 1 });
  }).pipe(
    Effect.provide(TelegramClient.layer("123:SECRET-TOKEN").pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)))),
    Effect.provide(TracingLayer),
  ));
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

test("Codex app-server requests carry their span's trace context", async () => {
  const appServer = appServerProcess();
  appServer.answer("thread/start", () => ({ thread: { id: "t-1" } }));
  const result = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const rpc = yield* makeAppServerRpc({ spawn: appServer.spawn, onNotification: () => Effect.void });
    yield* rpc.start({ cwd: "/tmp", env: {} });
    return yield* rpc.request("thread/start", { cwd: "/tmp" });
  }).pipe(withAlasioSpan("test.codex"))).pipe(Effect.provide(TracingLayer)));
  assert.deepEqual(result, { thread: { id: "t-1" } });
  const request = finishedSpan("codex/thread/start");
  const sent = appServer.written.find(({ method }) => method === "thread/start");
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
    const telegram = recordingTelegram({
      sendMessage: () => Effect.sync(() => [sentMessage(42, telegram.calls.sendMessage.length)]),
    });
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
        runTurn: () =>
          Effect.succeed({
            blockSequence: [{ type: "text", phase: "final_answer", content: "done" }],
            sessionId: "thread-1",
            pendingResponseId: "pending-1",
            interrupted: false,
            responseCompleted: true,
          }),
      };
    };
    const delivered: string[] = [];
    const outbox = Outbox.layer.pipe(Layer.provide([
      botApiLayer(async (call) => {
        delivered.push(paramsOf(call, "sendRichMessage").rich_message.markdown ?? "");
        return { message_id: 100 + delivered.length, date: 0, chat: { id: 42, type: "private", first_name: "Operator" } };
      }),
      Layer.succeed(Store, store),
    ]));
    const harnesses = { [CODEX_HARNESS]: harness(CODEX_HARNESS), [CLAUDE_HARNESS]: harness(CLAUDE_HARNESS) };
    spans.reset();
    await withServices({ store, telegram: telegram.layer, harnesses, workspaceRoot: root, outbox }, async (alasio) => {
      await alasio.runPromise(processPrompt({ conversationId, chatId: "42", messageId: 9, text: "hello", filePaths: [] }).pipe(
        withAlasioSpan("alasio.update", { kind: SpanKind.CONSUMER, parent: null }),
      ));
      // The conversation's prompt worker runs the turn, whose reply the outbox delivers.
      await eventually("the turn to end", () => spans.getFinishedSpans().find((span) => span.name === "alasio.turn"));
      await alasio.runPromise(Effect.flatMap(Outbox, (delivery) => delivery.deliverDue));
    });

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
    assert.deepEqual(delivered, ["done"]);

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

test("a deferred delivery records what stopped it and keeps its trace", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "alasio-telemetry-"));
  const store = new SqliteStore(root, join(root, "alasio.sqlite"));
  try {
    store.upsertConversation({ chatId: "43", user: { id: 43 } });
    const run = outboxFor(t, store, async () => {
      throw new Error("Telegram is down");
    });
    spans.reset();
    const turn = await inOtelSpan("test.turn", async (span) => {
      await run(Effect.flatMap(Outbox, (outbox) => outbox.enqueueText({ chatId: "43", text: "reply" })));
      await run(Effect.flatMap(Outbox, (delivery) => delivery.deliverDue));
      return span;
    }, ROOT_CONTEXT);
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

test("an effect's span is a span of alasio's: named, kinded, attributed, and labelled with its failure", async () => {
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

  await inOtelSpan("alasio.outer", async () => {
    await Effect.runPromise(Effect.void.pipe(withAlasioSpan("alasio.effect.root", { parent: null }), Effect.provide(TracingLayer)));
    await Effect.runPromise(Effect.promise(() => inOtelSpan("alasio.effect.inner", () => undefined)).pipe(
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

test("an effect's call is a call of alasio's: a client span and a duration labelled with its failure", async () => {
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

/** A turn's persistence that keeps nothing: what the turn stores is not what this test looks at. */
const forgetful: TurnPersistence = {
  createPendingResponse: () => "pending-1",
  updateActiveTurnPendingResponseId: () => undefined,
  updatePendingSessionId: () => undefined,
  updateActiveTurnSessionId: () => undefined,
  appendBlockToPending: () => undefined,
  markPendingResponseComplete: () => undefined,
  markPendingAsPosted: () => undefined,
  updateSessionUsage: () => undefined,
  recordRestartEvent: () => undefined,
};

test("Claude Code's process does not join the turn's trace, and its hooks and replies still run on alasio's logger and tracer", async () => {
  const handedOn: { atStart?: string | undefined; whileRead?: string | undefined } = {};
  const queryFactory: ClaudeQueryFactory = ({ prompt, options }) => {
    handedOn.atStart = traceparentHere();
    const hook: HookCallback | undefined = options.hooks?.PreToolUse?.[0]?.hooks[0];
    return fakeQuery((async function* run(): AsyncGenerator<SDKMessage, void> {
      yield initMessage("s-1");
      for await (const message of prompt) {
        handedOn.whileRead = traceparentHere();
        // Claude Code runs bayma exec, which the hook sees first.
        await hook?.({
          hook_event_name: "PreToolUse",
          session_id: "s-1",
          transcript_path: "/home/op/.claude/projects/-work/s-1.jsonl",
          cwd: "/work",
          tool_name: "mcp__bayma__exec",
          tool_input: { session_id: "b1", code: "await $`echo traced`" },
          tool_use_id: "t1",
        }, "t1", { signal: new AbortController().signal });
        yield successResult({ result: "done", session_id: "s-1", user_message_uuids: [stamped(message).uuid] });
        // Then reports, unasked, on work it left running: a reply of its own.
        yield successResult({ result: "the background task finished", session_id: "s-1" });
      }
    })());
  };
  spans.reset();
  logRecords.reset();
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const replied = yield* Deferred.make<void>();
    // The live sessions are made in the turn, as the first turn of a folder makes its harness.
    yield* Effect.gen(function*() {
      const liveSessions = yield* makeClaudeLiveSessions({
        workingDirectory: "/work",
        sessions: { sessionExists: () => Effect.succeed(false) },
        queryFactory,
        folderBayma: () => Effect.succeed({ type: "http", url: "http://bayma.alasio-host.svc:7290/mcp", headers: {} }),
      });
      const result = yield* liveSessions.runTurn({
        prompt: "hello",
        resumeSession: null,
        threadKey: "telegram:1",
        chatId: "1",
        messageId: "9",
        workingDirectory: "/work",
        persistence: forgetful,
        onBackgroundResponse: Deferred.succeed(replied, undefined).pipe(Effect.asVoid, withAlasioSpan("test.background-reply")),
      });
      assert.equal(result.responseCompleted, true);
    }).pipe(withAlasioSpan("alasio.turn"));
    yield* Deferred.await(replied);
  })).pipe(Effect.provide(ActiveTurns.layer), Effect.provide([AlasioLoggerLayer, TracingLayer])));

  const turn = finishedSpan("alasio.turn");
  assert.equal(handedOn.atStart, undefined, "the process is started with no trace to continue");
  assert.equal(handedOn.whileRead, undefined, "the process is read with no trace to continue");
  const hookLine = logRecords.getFinishedLogRecords().find((record) => String(record.body).startsWith("exec-hook seen thread=telegram:1"));
  assert.ok(hookLine, "the hook's line is a log record of alasio's");
  assert.equal(hookLine.instrumentationScope.name, "claude-live");
  assert.notEqual(hookLine.spanContext?.traceId, turn.spanContext().traceId);
  const reply = finishedSpan("test.background-reply");
  assert.notEqual(reply.spanContext().traceId, turn.spanContext().traceId, "a reply of Claude Code's own is a trace of its own");
  assert.equal(reply.parentSpanContext, undefined);
  assert.equal(spans.getFinishedSpans().some((span) => span.name === "alasio.outside-traces"), false, "nothing records the span outside traces");
});

test("an effect outside every trace runs with no span, so instrumentation that needs one records nothing, and its own spans start traces", async () => {
  const seen = await Effect.runPromise(Effect.gen(function*() {
    const outside = trace.getSpan(context.active());
    const inner = yield* Effect.sync(() => trace.getSpan(context.active())).pipe(withAlasioSpan("alasio.effect.outside"));
    return { outside, inner };
  }).pipe(outsideTraces, withAlasioSpan("alasio.effect.traced"), Effect.provide(TracingLayer)));
  // As the pg instrumentation's requireParentSpan asks: is there a span at all.
  assert.equal(seen.outside, undefined);
  const outside = finishedSpan("alasio.effect.outside");
  assert.equal(seen.inner?.spanContext().spanId, outside.spanContext().spanId);
  assert.equal(outside.parentSpanContext, undefined);
  assert.notEqual(outside.spanContext().traceId, finishedSpan("alasio.effect.traced").spanContext().traceId);
});

test("an effect's span that has ended is the parent of nothing made after it, though its promise is what the effect resumes from", async () => {
  await Effect.runPromise(Effect.gen(function*() {
    yield* Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 1))).pipe(withAlasioSpan("alasio.effect.earlier"));
    yield* Effect.void.pipe(withAlasioSpan("alasio.effect.later"));
  }).pipe(Effect.provide(TracingLayer)));
  const later = finishedSpan("alasio.effect.later");
  assert.equal(later.parentSpanContext, undefined);
  assert.notEqual(later.spanContext().traceId, finishedSpan("alasio.effect.earlier").spanContext().traceId);
});

test("an effect's span is a span of alasio's tracer, which OTLP encodes", async () => {
  await Effect.runPromise(Effect.void.pipe(withAlasioSpan("alasio.effect.encoded"), Effect.provide(TracingLayer)));
  const span = finishedSpan("alasio.effect.encoded");
  assert.equal(span.instrumentationScope.name, "alasio");
  // As the OTLP exporter encodes a batch: a tracer without a name throws here, and the
  // batch, every span in it, is dropped.
  assert.ok(ProtobufTraceSerializer.serializeRequest([span]));
});
