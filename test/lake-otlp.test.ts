import assert from "node:assert/strict";
import { test } from "node:test";

import { DuckDBListValue, DuckDBMapValue, DuckDBStructValue, DuckDBTimestampNanosecondsValue, type DuckDBValue } from "@duckdb/node-api";
import { ROOT_CONTEXT, SpanKind, SpanStatusCode, trace, TraceFlags } from "@opentelemetry/api";
import { SeverityNumber } from "@opentelemetry/api-logs";
import {
  JsonLogsSerializer,
  JsonMetricsSerializer,
  JsonTraceSerializer,
  ProtobufLogsSerializer,
  ProtobufMetricsSerializer,
  ProtobufTraceSerializer,
} from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { InMemoryLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { AggregationType, MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import { type TelemetryRows, telemetryRows } from "../neon/lake/src/otel.ts";
import { decodeRequest, type Encoding, MalformedRequest, type Signal } from "../neon/lake/src/otlp.ts";

// Requests as OpenTelemetry's own SDK and serializers make them, in both encodings, are
// decoded into the `otel` schema's rows (neon/lake/src/otel.ts), which must be alike
// whichever encoding a request came in.

const RESOURCE = resourceFromAttributes({ "service.name": "alasio", "alasio.volume.id": "fs-abc123" });
const TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
const PARENT_ID = "b7ad6b7169203331";
const START = 1_791_300_000_123_456_789n; // 2026-10-06T…, with nanoseconds
const at = (nanos: bigint): [number, number] => [Number(nanos / 1_000_000_000n), Number(nanos % 1_000_000_000n)];

/** A value as plain JavaScript, so rows compare: a map an object, a list an array, a time its nanoseconds. */
function plain(value: DuckDBValue): unknown {
  if (value instanceof DuckDBMapValue) return Object.fromEntries(value.entries.map(({ key, value: entry }) => [String(key), plain(entry)]));
  if (value instanceof DuckDBListValue) return value.items.map(plain);
  if (value instanceof DuckDBStructValue) return Object.fromEntries(Object.entries(value.entries).map(([key, entry]) => [key, plain(entry)]));
  if (value instanceof DuckDBTimestampNanosecondsValue) return value.nanos;
  return value;
}

function plainRows(rows: TelemetryRows): Record<string, Record<string, unknown>[]> {
  return Object.fromEntries(Object.entries(rows).map(([table, list]) => [table, list.map((row) => Object.fromEntries(Object.entries(row).map(([column, value]) => [column, plain(value)])))]));
}

/** The rows of `body`, a request for `signal` in `encoding`. */
const rowsOf = (signal: Signal, encoding: Encoding, body: Uint8Array | undefined) => {
  assert.ok(body, `a ${encoding} request`);
  return plainRows(telemetryRows(signal, decodeRequest(signal, encoding, body)));
};

/** Rows from a request serialized both ways, which must be alike: those rows. */
function bothWays<T>(signal: Signal, serializers: { protobuf: { serializeRequest(data: T): Uint8Array | undefined }; json: { serializeRequest(data: T): Uint8Array | undefined } }, data: T) {
  const fromProtobuf = rowsOf(signal, "protobuf", serializers.protobuf.serializeRequest(data));
  assert.deepEqual(rowsOf(signal, "json", serializers.json.serializeRequest(data)), fromProtobuf);
  return fromProtobuf;
}

test("spans are a row of otel.traces each, alike from protobuf and from JSON", () => {
  const spans = new InMemorySpanExporter();
  const tracer = new BasicTracerProvider({ resource: RESOURCE, spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer("alasio", "4.2.1");
  const parent = trace.setSpanContext(ROOT_CONTEXT, { traceId: TRACE_ID, spanId: PARENT_ID, traceFlags: TraceFlags.SAMPLED });
  const span = tracer.startSpan("alasio.turn", {
    kind: SpanKind.SERVER,
    startTime: at(START),
    attributes: { "alasio.conversation.id": "telegram:42", "alasio.turn.count": 3, "alasio.turn.done": true, ratio: 0.5, tools: ["Bash", "Read"] },
    links: [{ context: { traceId: "1".repeat(32), spanId: "2".repeat(16), traceFlags: TraceFlags.SAMPLED }, attributes: { why: "queued" } }],
  }, parent);
  span.addEvent("exception", { "exception.message": "boom" }, at(START + 1_000n));
  span.setStatus({ code: SpanStatusCode.ERROR, message: "the harness failed" });
  span.end(at(START + 2_500_000_000n));

  const { traces } = bothWays("traces", { protobuf: ProtobufTraceSerializer, json: JsonTraceSerializer }, spans.getFinishedSpans());
  assert.equal(traces?.length, 1);
  const [row] = traces ?? [];
  assert.ok(row);
  assert.equal(row["Timestamp"], START);
  assert.deepEqual([row["TraceId"], row["ParentSpanId"]], [TRACE_ID, PARENT_ID]);
  assert.match(String(row["SpanId"]), /^[0-9a-f]{16}$/u);
  assert.deepEqual(
    [row["SpanName"], row["SpanKind"], row["ServiceName"], row["ScopeName"], row["ScopeVersion"], row["Duration"], row["StatusCode"], row["StatusMessage"]],
    ["alasio.turn", "Server", "alasio", "alasio", "4.2.1", 2_500_000_000n, "Error", "the harness failed"],
  );
  assert.deepEqual(row["ResourceAttributes"], { "service.name": "alasio", "alasio.volume.id": "fs-abc123" });
  // As the ClickHouse exporter keeps them: every value as text, a list as JSON.
  assert.deepEqual(row["SpanAttributes"], {
    "alasio.conversation.id": "telegram:42",
    "alasio.turn.count": "3",
    "alasio.turn.done": "true",
    ratio: "0.5",
    tools: '["Bash","Read"]',
  });
  assert.deepEqual(row["Events"], [{ Timestamp: START + 1_000n, Name: "exception", Attributes: { "exception.message": "boom" } }]);
  assert.deepEqual(row["Links"], [{ TraceId: "1".repeat(32), SpanId: "2".repeat(16), TraceState: "", Attributes: { why: "queued" } }]);
});

test("log records are a row of otel.logs each, alike from protobuf and from JSON", async () => {
  const exporter = new InMemoryLogRecordExporter();
  const provider = new LoggerProvider({ resource: RESOURCE, processors: [new SimpleLogRecordProcessor({ exporter })] });
  const logger = provider.getLogger("alasio-lake");
  logger.emit({
    timestamp: at(START),
    severityNumber: SeverityNumber.WARN,
    severityText: "WARN",
    body: "load failed",
    attributes: { error: "permission denied" },
    eventName: "lake.load",
  });
  logger.emit({ body: { nested: [1, "two"] } });
  await provider.forceFlush();

  const { logs } = bothWays("logs", { protobuf: ProtobufLogsSerializer, json: JsonLogsSerializer }, exporter.getFinishedLogRecords());
  const [warned, structured] = logs ?? [];
  assert.ok(warned && structured);
  assert.deepEqual(
    [warned["Timestamp"], warned["SeverityText"], warned["SeverityNumber"], warned["ServiceName"], warned["Body"], warned["ScopeName"], warned["EventName"], warned["TraceId"], warned["TraceFlags"]],
    [START, "WARN", SeverityNumber.WARN, "alasio", "load failed", "alasio-lake", "lake.load", "", 0],
  );
  assert.deepEqual(warned["LogAttributes"], { error: "permission denied" });
  assert.equal(structured["Body"], '{"nested":[1,"two"]}');
});

class CollectingReader extends MetricReader {
  protected override async onForceFlush(): Promise<void> {}
  protected override async onShutdown(): Promise<void> {}
}

test("metric points are a row each of their kind's table", async () => {
  const reader = new CollectingReader();
  const provider = new MeterProvider({
    resource: RESOURCE,
    readers: [reader],
    views: [{ instrumentName: "alasio.turn.tokens", aggregation: { type: AggregationType.EXPONENTIAL_HISTOGRAM } }],
  });
  const meter = provider.getMeter("alasio");
  meter.createCounter("alasio.turns", { unit: "{turn}", description: "Turns run" }).add(3, { "alasio.harness": "claude" });
  meter.createUpDownCounter("alasio.turn.active").add(-1);
  meter.createObservableGauge("alasio.memory").addCallback((result) => result.observe(0.25, { kind: "heap" }));
  meter.createHistogram("alasio.turn.duration", { advice: { explicitBucketBoundaries: [1, 10] } }).record(4.5);
  meter.createHistogram("alasio.turn.tokens").record(1200);
  const { resourceMetrics } = await reader.collect();

  const rows = bothWays("metrics", { protobuf: ProtobufMetricsSerializer, json: JsonMetricsSerializer }, resourceMetrics);
  const one = (table: string, name: string) => {
    const found = rows[table]?.find((row) => row["MetricName"] === name);
    assert.ok(found, `${name} in ${table}`);
    return found;
  };
  const turns = one("metrics_sum", "alasio.turns");
  assert.deepEqual(
    [turns["Value"], turns["IsMonotonic"], turns["AggregationTemporality"], turns["MetricUnit"], turns["MetricDescription"], turns["ServiceName"], turns["ScopeName"]],
    [3, true, 2, "{turn}", "Turns run", "alasio", "alasio"],
  );
  assert.deepEqual(turns["Attributes"], { "alasio.harness": "claude" });
  assert.equal(typeof turns["TimeUnix"], "bigint");
  assert.equal(one("metrics_sum", "alasio.turn.active")["IsMonotonic"], false);
  assert.equal(one("metrics_gauge", "alasio.memory")["Value"], 0.25);
  const duration = one("metrics_histogram", "alasio.turn.duration");
  assert.deepEqual(
    [duration["Count"], duration["Sum"], duration["BucketCounts"], duration["ExplicitBounds"], duration["Min"], duration["Max"]],
    [1n, 4.5, [0n, 1n, 0n], [1, 10], 4.5, 4.5],
  );
  const tokens = one("metrics_exponential_histogram", "alasio.turn.tokens");
  assert.deepEqual([tokens["Count"], tokens["Sum"], tokens["ZeroCount"]], [1n, 1200, 0n]);
  assert.deepEqual(tokens["PositiveBucketCounts"], [1n]);
  assert.equal(rows["metrics_summary"]?.length, 0);
});

test("a summary's quantiles and a point's exemplars are kept, as JSON gives them", () => {
  const point = { timeUnixNano: String(START), startTimeUnixNano: "0" };
  const { metrics_summary: [summary] = [], metrics_gauge: [gauge] = [] } = rowsOf("metrics", "json", Buffer.from(JSON.stringify({
    resourceMetrics: [{
      resource: { attributes: [{ key: "service.name", value: { stringValue: "compute_ctl" } }] },
      scopeMetrics: [{
        metrics: [
          { name: "pg.latency", summary: { dataPoints: [{ ...point, count: "4", sum: 2.5, quantileValues: [{ quantile: 0.5, value: 0.4 }, { quantile: 1, value: 1.2 }] }] } },
          {
            name: "pg.connections",
            gauge: {
              dataPoints: [{
                ...point,
                asInt: "9007199254740993",
                exemplars: [{ timeUnixNano: String(START), asDouble: 7, traceId: TRACE_ID.toUpperCase(), spanId: PARENT_ID, filteredAttributes: [{ key: "db", value: { stringValue: "alasio" } }] }],
              }],
            },
          },
        ],
      }],
    }],
  })));
  assert.ok(summary && gauge);
  assert.deepEqual([summary["Count"], summary["Sum"], summary["ServiceName"]], [4n, 2.5, "compute_ctl"]);
  assert.deepEqual(summary["ValueAtQuantiles"], [{ Quantile: 0.5, Value: 0.4 }, { Quantile: 1, Value: 1.2 }]);
  assert.equal(gauge["Value"], 9007199254740992); // a double, as the exporter keeps every value
  assert.deepEqual(gauge["Exemplars"], [{ FilteredAttributes: { db: "alasio" }, TimeUnix: START, Value: 7, SpanId: PARENT_ID, TraceId: TRACE_ID }]);
});

test("attribute values read as the ClickHouse exporter writes them, the last of a repeated key kept, and a record without a time of its own is timed when seen", () => {
  const { logs: [row] = [] } = rowsOf("logs", "json", Buffer.from(JSON.stringify({
    resourceLogs: [{
      scopeLogs: [{
        logRecords: [{
          observedTimeUnixNano: String(START),
          body: { kvlistValue: { values: [{ key: "big", value: { intValue: "9007199254740993" } }, { key: "list", value: { arrayValue: { values: [{ boolValue: false }, { doubleValue: 1.5 }] } } }] } },
          attributes: [
            { key: "bytes", value: { bytesValue: "AAEC" } },
            { key: "twice", value: { stringValue: "first" } },
            { key: "twice", value: { stringValue: "last" } },
            { key: "empty", value: {} },
          ],
        }],
      }],
    }],
  })));
  assert.ok(row);
  assert.equal(row["Body"], '{"big":9007199254740993,"list":[false,1.5]}');
  assert.deepEqual(row["LogAttributes"], { bytes: "AAEC", twice: "last", empty: "" });
  assert.equal(row["ServiceName"], "");
  assert.equal(row["Timestamp"], START);
});

test("a request that is not its signal's OTLP is malformed", () => {
  const malformed = (signal: Signal, encoding: Encoding, body: string | Uint8Array) =>
    assert.throws(() => telemetryRows(signal, decodeRequest(signal, encoding, typeof body === "string" ? Buffer.from(body) : body)), MalformedRequest);
  malformed("traces", "protobuf", Uint8Array.from([0x0a, 0x05, 0x01]));
  malformed("traces", "json", "not json");
  malformed("logs", "json", "[]");
  malformed("metrics", "json", JSON.stringify({ resourceMetrics: 5 }));
  malformed("traces", "json", JSON.stringify({ resourceSpans: [{ scopeSpans: [{ spans: [{ startTimeUnixNano: "soon" }] }] }] }));
  malformed("metrics", "json", JSON.stringify({ resourceMetrics: [{ scopeMetrics: [{ metrics: [{ histogram: { dataPoints: [{ count: "-1" }] } }] }] }] }));
});
