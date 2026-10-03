import assert from "node:assert/strict";
import { test } from "node:test";

import type { Attributes } from "@opentelemetry/api";
import { SeverityNumber } from "@opentelemetry/api-logs";
import {
  type ISerializer,
  JsonLogsSerializer,
  JsonMetricsSerializer,
  JsonTraceSerializer,
  ProtobufLogsSerializer,
  ProtobufMetricsSerializer,
  ProtobufTraceSerializer,
} from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { InMemoryLogRecordExporter, LoggerProvider, SimpleLogRecordProcessor } from "@opentelemetry/sdk-logs";
import { MeterProvider, MetricReader } from "@opentelemetry/sdk-metrics";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import protobuf, { type Long } from "protobufjs";

import { MalformedRequest, stampResources } from "../src/sandbox/otlp-resource.ts";
import { SIGNALS, type Signal } from "../src/telemetry/config.ts";
import type { OtlpEncoding } from "../src/telemetry/forward.ts";

// Requests as OpenTelemetry's own SDK and serializers make them, from a resource that
// claims to be something it is not, are stamped, and their resources read back with an
// independent protobuf decoder, protobufjs, over the levels the stamp touches.

const FORGED = {
  "service.name": "alasio",
  "alasio.volume.id": "fs-other",
  "alasio.conversation.id": "someone-else",
  "deployment.environment.name": "forged",
  "host.name": "session",
  "process.pid": 7,
};
const STAMP = { "deployment.environment.name": "production", "service.name": "bayma", "alasio.volume.id": "fs-abc123" };
const STAMPED = { "host.name": "session", "process.pid": 7, ...STAMP };

const { root } = protobuf.parse(`
  syntax = "proto3";
  message AnyValue { oneof value { string string_value = 1; bool bool_value = 2; int64 int_value = 3; double double_value = 4; } }
  message KeyValue { string key = 1; AnyValue value = 2; }
  message Resource { repeated KeyValue attributes = 1; uint32 dropped_attributes_count = 2; }
  message Part { Resource resource = 1; repeated bytes scopes = 2; string schema_url = 3; }
  message Request { repeated Part parts = 1; }
`);
const Request = root.lookupType("Request");

/** An attribute's value, as either decoder gives it: an int64 is a Long from protobufjs, a string or number in JSON. */
interface AnyValue {
  readonly stringValue?: string;
  readonly boolValue?: boolean;
  readonly intValue?: Long | string | number;
  readonly doubleValue?: number;
}

interface Resource {
  readonly attributes: readonly { readonly key: string; readonly value: AnyValue }[];
}

/** The `Request` above as protobufjs's toObject gives it, defaults included. */
interface DecodedRequest {
  readonly parts: readonly { readonly resource: Resource | null; readonly scopes: readonly Uint8Array[]; readonly schemaUrl: string }[];
}

/** A part of an OTLP JSON request: its resource, and its scopes under the signal's name for them. */
interface JsonPart {
  readonly resource: Resource;
  readonly [field: string]: unknown;
}

/** An OTLP JSON request: its parts under the signal's name for them. */
type JsonRequest = Readonly<Partial<Record<string, readonly JsonPart[]>>>;

const anyValue = (value: AnyValue) =>
  value.stringValue ?? value.boolValue ?? (value.intValue === undefined ? value.doubleValue : Number(value.intValue));

/** Each part of a binary request: its resource's attributes, its scopes' bytes, its schema URL. */
function decode(bytes: Uint8Array) {
  // toObject's result has the shape of the message the schema above declares.
  const request = Request.toObject(Request.decode(bytes), { defaults: true }) as DecodedRequest;
  return request.parts.map((part) => ({
    attributes: Object.fromEntries((part.resource?.attributes ?? []).map(({ key, value }) => [key, anyValue(value)])),
    scopes: part.scopes.map((scope) => Buffer.from(scope).toString("hex")),
    schemaUrl: part.schemaUrl,
  }));
}

function attributesOf(jsonResource: Resource) {
  return Object.fromEntries(jsonResource.attributes.map(({ key, value }) => [key, anyValue(value)]));
}

class CollectingReader extends MetricReader {
  protected override async onForceFlush(): Promise<void> {}
  protected override async onShutdown(): Promise<void> {}
}

/** A signal's request, serialized either way, and the JSON names of its parts and of the scopes within. */
interface SignalRequest {
  serialize(encoding: OtlpEncoding): Uint8Array | undefined;
  readonly list: string;
  readonly scopes: string;
}

function signalRequest<T>(
  serializers: Readonly<Record<OtlpEncoding, ISerializer<T, unknown>>>,
  data: T,
  list: string,
  scopes: string,
): SignalRequest {
  return { serialize: (encoding) => serializers[encoding].serializeRequest(data), list, scopes };
}

/** One request of each signal, from a resource with `attributes`. */
async function requests(attributes: Attributes): Promise<Record<Signal, SignalRequest>> {
  const resource = resourceFromAttributes(attributes);
  const spans = new InMemorySpanExporter();
  const tracer = new BasicTracerProvider({ resource, spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer("bayma");
  tracer.startSpan("tools/call exec", { attributes: { "bayma.session.id": "sess_1" } }).end();
  tracer.startSpan("bayma.exec").end();

  const reader = new CollectingReader();
  new MeterProvider({ resource, readers: [reader] }).getMeter("bayma").createCounter("bayma.exec.count").add(3, { "bayma.runtime": "bun" });
  const { resourceMetrics } = await reader.collect();

  const logs = new InMemoryLogRecordExporter();
  const loggerProvider = new LoggerProvider({ resource, processors: [new SimpleLogRecordProcessor({ exporter: logs })] });
  loggerProvider.getLogger("bayma").emit({ body: "session sess_1 started", severityNumber: SeverityNumber.INFO });
  await loggerProvider.forceFlush();

  return {
    traces: signalRequest({ protobuf: ProtobufTraceSerializer, json: JsonTraceSerializer }, spans.getFinishedSpans(), "resourceSpans", "scopeSpans"),
    metrics: signalRequest({ protobuf: ProtobufMetricsSerializer, json: JsonMetricsSerializer }, resourceMetrics, "resourceMetrics", "scopeMetrics"),
    logs: signalRequest({ protobuf: ProtobufLogsSerializer, json: JsonLogsSerializer }, logs.getFinishedLogRecords(), "resourceLogs", "scopeLogs"),
  };
}

test("a binary request's resources are stamped and every other byte of it is kept", async () => {
  const all = await requests(FORGED);
  for (const signal of SIGNALS) {
    const serialized = all[signal].serialize("protobuf");
    assert.ok(serialized, signal);
    const body = Buffer.from(serialized);
    const stamped = stampResources(signal, "protobuf", body, STAMP);
    const [before] = decode(body);
    const [after] = decode(stamped);
    assert.ok(before && after, signal);
    assert.deepEqual(after.attributes, STAMPED, signal);
    assert.deepEqual(after.scopes, before.scopes, signal);
    assert.ok(after.scopes.length > 0, signal);
    assert.equal(after.schemaUrl, before.schemaUrl, signal);
  }
});

test("a JSON request's resources are stamped and its scopes kept", async () => {
  const all = await requests(FORGED);
  for (const signal of SIGNALS) {
    const request = all[signal];
    const serialized = request.serialize("json");
    assert.ok(serialized, signal);
    const body = Buffer.from(serialized);
    const original: JsonRequest = JSON.parse(body.toString("utf8"));
    const stamped: JsonRequest = JSON.parse(stampResources(signal, "json", body, STAMP).toString("utf8"));
    assert.deepEqual(Object.keys(stamped), [request.list], signal);
    const [part] = stamped[request.list] ?? [];
    const [originalPart] = original[request.list] ?? [];
    assert.ok(part && originalPart, signal);
    assert.deepEqual(attributesOf(part.resource), STAMPED, signal);
    assert.deepEqual(part[request.scopes], originalPart[request.scopes], signal);
  }
});

test("a stamp replaces every resource a request repeats and gives one to a part without", () => {
  // Field 1, length-delimited, at each level: a part, a resource, an attribute (all short).
  const field1 = (...bytes: Buffer[]) => Buffer.concat([Buffer.from([0x0a, Buffer.concat(bytes).length]), ...bytes]);
  const attribute = (key: string, value: string) =>
    field1(Buffer.from(root.lookupType("KeyValue").encode({ key, value: { stringValue: value } }).finish()));
  const scope = Buffer.from([0x12, 0x01, 0x00]);
  // Protobuf merges a message field given twice, so a second resource would otherwise
  // add attributes the first was stripped of.
  const body = Buffer.concat([
    field1(
      field1(attribute("host.name", "session")),
      scope,
      field1(attribute("service.name", "alasio"), attribute("alasio.volume.id", "fs-other")),
    ),
    field1(scope),
  ]);
  const parts = decode(stampResources("traces", "protobuf", body, STAMP));
  assert.deepEqual(parts.map((part) => part.attributes), [{ "host.name": "session", ...STAMP }, STAMP]);
  assert.deepEqual(parts.map((part) => part.scopes), [["00"], ["00"]]);
});

test("a JSON request keeps no second spelling of a resource", () => {
  const body = Buffer.from(JSON.stringify({
    resourceSpans: [{
      resource: { attributes: [{ key: "alasio.volume.id", value: { stringValue: "fs-other" } }], dropped_attributes_count: 1 },
      resource_spans: [],
      scopeSpans: [{ spans: [] }],
    }],
    resource_spans: [{ resource: { attributes: [{ key: "service.name", value: { stringValue: "alasio" } }] } }],
  }));
  assert.deepEqual(JSON.parse(stampResources("traces", "json", body, STAMP).toString("utf8")), {
    resourceSpans: [{
      resource: { attributes: Object.entries(STAMP).map(([key, value]) => ({ key, value: { stringValue: value } })) },
      scopeSpans: [{ spans: [] }],
    }],
  });
});

test("a request that does not parse is refused", () => {
  const malformed: [OtlpEncoding, Buffer][] = [
    ["protobuf", Buffer.from([0x0a, 0x05, 0x01])], // a part longer than the request
    ["protobuf", Buffer.from([0x0b])], // a group, which OTLP never uses
    ["protobuf", Buffer.from([0x08, 0x01])], // a part that is not a message
    ["protobuf", Buffer.from([0x0a, 0x02, 0x0a, 0x80])], // a truncated length
    ["protobuf", Buffer.from([0x00])], // field number 0
    ["json", Buffer.from("{")],
    ["json", Buffer.from("[]")],
    ["json", Buffer.from(JSON.stringify({ resourceSpans: {} }))],
    ["json", Buffer.from(JSON.stringify({ resourceSpans: [{ resource: { attributes: {} } }] }))],
  ];
  for (const [encoding, body] of malformed) {
    assert.throws(() => stampResources("traces", encoding, body, STAMP), MalformedRequest, `${encoding} ${body.toString("hex")}`);
  }
  // An empty request is a valid one, with nothing to stamp.
  assert.equal(stampResources("traces", "protobuf", Buffer.alloc(0), STAMP).length, 0);
});
