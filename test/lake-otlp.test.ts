import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { pathToFileURL } from "node:url";

import type { DuckDBValue } from "@duckdb/node-api";
import { ROOT_CONTEXT, SpanKind, SpanStatusCode, trace, TraceFlags } from "@opentelemetry/api";
import { SeverityNumber } from "@opentelemetry/api-logs";
import { ProtobufLogsSerializer, ProtobufMetricsSerializer, ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { InMemoryLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { AggregationType, MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import { type TelemetryRows, telemetryRows } from "../neon/lake/src/otel.ts";
import { encodeRequest, MalformedRequest, PART_ITEMS, requestParts, type Signal } from "../neon/lake/src/otlp.ts";
import { startTelemetry } from "../neon/lake/src/telemetry.ts";

// The lake is a package of its own (neon/lake): its modules use the DuckDB client it
// resolves, its own copy where it is installed, the root's otherwise. The values its rows hold
// are that copy's classes, so the rows are read through that copy, not the root's.
const { DuckDBListValue, DuckDBMapValue, DuckDBStructValue, DuckDBTimestampNanosecondsValue }: typeof import("@duckdb/node-api") = await import(
  pathToFileURL(createRequire(new URL("../neon/lake/src/otel.ts", import.meta.url)).resolve("@duckdb/node-api")).href
);

// Requests as OpenTelemetry's own SDK and serializers make them, in binary protobuf as the
// collector writes each to the log, are decoded into the `otel` schema's rows
// (neon/lake/src/otel.ts).

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

/** The rows of `body`, a request for `signal` in binary protobuf, as the intake takes them, its parts' in turn. */
function decodedRows(signal: Signal, body: Uint8Array): TelemetryRows {
  const rows: TelemetryRows = {};
  for (const part of requestParts(signal, body)) {
    for (const [table, tableRows] of Object.entries(telemetryRows(signal, part)) as [keyof TelemetryRows, NonNullable<TelemetryRows[keyof TelemetryRows]>][]) (rows[table] ??= []).push(...tableRows);
  }
  return rows;
}

/** The rows of `body`, a request for `signal` in binary protobuf, as plain values. */
const rowsOf = (signal: Signal, body: Uint8Array | undefined) => {
  assert.ok(body, "a request");
  return plainRows(decodedRows(signal, body));
};

/** A log record's attributes but the id the intake makes it, of what it is, where it has none of its own: checked to be one. */
function ownAttributes(attributes: unknown): Record<string, unknown> {
  const { "log.record.uid": id, ...own } = attributes as Record<string, unknown>;
  assert.match(String(id), /^[0-9a-f]{32}$/u);
  return own;
}

/** An id, given as hex, as protobuf carries it. */
const id = (hex: string) => Buffer.from(hex, "hex");

test("spans are a row of otel.traces each", () => {
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

  const { traces } = rowsOf("traces", ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans()));
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

test("log records are a row of otel.logs each", async () => {
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

  const { logs } = rowsOf("logs", ProtobufLogsSerializer.serializeRequest(exporter.getFinishedLogRecords()));
  const [warned, structured] = logs ?? [];
  assert.ok(warned && structured);
  assert.deepEqual(
    [warned["Timestamp"], warned["SeverityText"], warned["SeverityNumber"], warned["ServiceName"], warned["Body"], warned["ScopeName"], warned["EventName"], warned["TraceId"], warned["TraceFlags"]],
    [START, "WARN", SeverityNumber.WARN, "alasio", "load failed", "alasio-lake", "lake.load", "", 0],
  );
  assert.deepEqual(ownAttributes(warned["LogAttributes"]), { error: "permission denied" });
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

  const rows = rowsOf("metrics", ProtobufMetricsSerializer.serializeRequest(resourceMetrics));
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

test("a summary's quantiles and a point's exemplars are kept", () => {
  const point = { timeUnixNano: String(START), startTimeUnixNano: "0" };
  const { metrics_summary: [summary] = [], metrics_gauge: [gauge] = [] } = rowsOf("metrics", encodeRequest("metrics", {
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
                exemplars: [{ timeUnixNano: String(START), asDouble: 7, traceId: id(TRACE_ID), spanId: id(PARENT_ID), filteredAttributes: [{ key: "db", value: { stringValue: "alasio" } }] }],
              }],
            },
          },
        ],
      }],
    }],
  }));
  assert.ok(summary && gauge);
  assert.deepEqual([summary["Count"], summary["Sum"], summary["ServiceName"]], [4n, 2.5, "compute_ctl"]);
  assert.deepEqual(summary["ValueAtQuantiles"], [{ Quantile: 0.5, Value: 0.4 }, { Quantile: 1, Value: 1.2 }]);
  assert.equal(gauge["Value"], 9007199254740992); // a double, as the exporter keeps every value
  assert.deepEqual(gauge["Exemplars"], [{ FilteredAttributes: { db: "alasio" }, TimeUnix: START, Value: 7, SpanId: PARENT_ID, TraceId: TRACE_ID }]);
});

test("attribute values read as the ClickHouse exporter writes them, the last of a repeated key kept, and a record without a time of its own is timed when seen", () => {
  const { logs: [row] = [] } = rowsOf("logs", encodeRequest("logs", {
    resourceLogs: [{
      scopeLogs: [{
        logRecords: [{
          observedTimeUnixNano: String(START),
          body: { kvlistValue: { values: [{ key: "big", value: { intValue: "9007199254740993" } }, { key: "list", value: { arrayValue: { values: [{ boolValue: false }, { doubleValue: 1.5 }] } } }] } },
          attributes: [
            { key: "bytes", value: { bytesValue: Uint8Array.from([0, 1, 2]) } },
            { key: "twice", value: { stringValue: "first" } },
            { key: "twice", value: { stringValue: "last" } },
            { key: "empty", value: {} },
          ],
        }],
      }],
    }],
  }));
  assert.ok(row);
  assert.equal(row["Body"], '{"big":9007199254740993,"list":[false,1.5]}');
  assert.deepEqual(ownAttributes(row["LogAttributes"]), { bytes: "AAEC", twice: "last", empty: "" });
  assert.equal(row["ServiceName"], "");
  assert.equal(row["Timestamp"], START);
});

test("a request comes a part at a time, each of one resource's one scope's items, of one metric's points, as many as PART_ITEMS, whose rows are the request's", () => {
  const items = 2 * PART_ITEMS + 5;
  const resource = (service: string) => ({ attributes: [{ key: "service.name", value: { stringValue: service } }] });
  const scope = { name: "scope", version: "1", attributes: [{ key: "scope.kind", value: { stringValue: "test" } }] };
  const spans = Array.from({ length: items }, (_, index) => ({
    traceId: id(TRACE_ID),
    spanId: Buffer.alloc(8, index + 1),
    name: `span ${index}`,
    startTimeUnixNano: String(START + BigInt(index)),
    endTimeUnixNano: String(START + BigInt(index) + 1n),
  }));
  const requests = {
    traces: { resourceSpans: [{ resource: resource("one"), scopeSpans: [{ scope, spans, schemaUrl: "https://scope" }], schemaUrl: "https://one" }, { resource: resource("two"), scopeSpans: [{ scope, spans: spans.slice(0, 3) }] }] },
    logs: {
      resourceLogs: [{
        resource: resource("logged"),
        scopeLogs: [{ scope, logRecords: Array.from({ length: items }, (_, index) => ({ timeUnixNano: String(START + BigInt(index)), body: { stringValue: `event ${index}` } })), schemaUrl: "https://scope" }],
        schemaUrl: "https://logged",
      }],
    },
    metrics: {
      resourceMetrics: [{
        resource: resource("measured"),
        scopeMetrics: [{
          scope,
          metrics: [
            { name: "counted", unit: "1", sum: { dataPoints: Array.from({ length: items }, (_, index) => ({ timeUnixNano: String(START + BigInt(index)), asInt: String(index) })), aggregationTemporality: 2, isMonotonic: true } },
            { name: "sampled", gauge: { dataPoints: [{ timeUnixNano: String(START), asDouble: 1.5 }] } },
          ],
        }],
        schemaUrl: "https://measured",
      }],
    },
  } as const;
  for (const signal of ["traces", "logs", "metrics"] as const) {
    const body = encodeRequest(signal, requests[signal]);
    const parts = [...requestParts(signal, body)];
    // The many items in three parts, and the other resource's spans, or the other metric's points, a part of their own.
    assert.equal(parts.length, signal === "logs" ? 3 : 4, signal);
    for (const part of parts) assert.ok(Object.values(telemetryRows(signal, part)).every((rows) => rows.length <= PART_ITEMS), signal);
    assert.deepEqual(rowsOf(signal, body), plainRows(telemetryRows(signal, requests[signal])), signal);
  }
});

test("a record that is not its signal's OTLP is malformed", () => {
  const malformed = (signal: Signal, body: Uint8Array) => assert.throws(() => decodedRows(signal, body), MalformedRequest);
  // A length past the end, and a field of the wrong wire type.
  malformed("traces", Uint8Array.from([0x0a, 0x05, 0x01]));
  malformed("logs", Uint8Array.from([0x0b, 0x01]));
});

test("the lake's events are exported as log records where the standard variables say, and nowhere without them", async () => {
  const received: Uint8Array[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    // With no encoding set, a request reads as Buffers.
    for await (const chunk of request as AsyncIterable<Buffer>) chunks.push(chunk);
    if (request.url === "/v1/logs") received.push(Buffer.concat(chunks));
    response.writeHead(200, { "content-type": "application/x-protobuf" }).end();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const variables = ["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_RESOURCE_ATTRIBUTES"] as const;
  const saved = variables.map((name) => process.env[name]);
  try {
    for (const name of variables) delete process.env[name];
    assert.equal(startTelemetry(), null);
    // A server listening on a TCP port has an address of its own.
    process.env["OTEL_EXPORTER_OTLP_ENDPOINT"] = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    process.env["OTEL_RESOURCE_ATTRIBUTES"] = "deployment.environment.name=test";
    const telemetry = startTelemetry();
    assert.ok(telemetry);
    telemetry.emit("load failed", { error: "permission denied", claude: { inserted: 1 } });
    await telemetry.shutdown();
  } finally {
    variables.forEach((name, index) => {
      const value = saved[index];
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    });
    server.close();
  }
  const logs = received.flatMap((body) => rowsOf("logs", body)["logs"] ?? []);
  assert.equal(logs.length, 1);
  const [event] = logs;
  assert.ok(event);
  assert.deepEqual([event["ServiceName"], event["Body"], event["ScopeName"]], ["alasio-lake", "load failed", "alasio-lake"]);
  assert.deepEqual(ownAttributes(event["LogAttributes"]), { error: "permission denied", claude: '{"inserted":1}' });
  assert.equal((event["ResourceAttributes"] as Record<string, string>)["deployment.environment.name"], "test");
});
