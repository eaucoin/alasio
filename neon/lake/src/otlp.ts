/**
 * OTLP export requests, as the telemetry intake receives them over HTTP (./intake.ts):
 * binary protobuf, decoded by protobufjs from the messages below, or JSON, parsed. Both
 * come out as one shape, OTLP's JSON one, but for what JSON spells as text and protobuf
 * does not: an id is hex in JSON and bytes in protobuf, a 64-bit integer a string (or a
 * number) in JSON and a string here, and bytes base64 in JSON and bytes in protobuf.
 * Whoever reads a request reads those through `hexId`, `integer` and `base64`.
 *
 * The messages are opentelemetry-proto's for the three signals' export requests, as of
 * its 1.5, which gave a log record its event name: enums as the integers they are on
 * the wire, and what the intake does not keep (a resource's entity references) left
 * out, which protobuf skips.
 */
import protobuf from "protobufjs";

/** A signal the intake takes. */
export type Signal = "traces" | "metrics" | "logs";

export const SIGNALS: readonly Signal[] = ["traces", "metrics", "logs"];

/** How a request is encoded. */
export type Encoding = "protobuf" | "json";

/** Thrown for a request that does not decode as its signal's. */
export class MalformedRequest extends Error {
  override name = "MalformedRequest";
}

/** An id: hex text from JSON, bytes from protobuf. */
export type Id = string | Uint8Array;
/** A 64-bit integer: text, or a number, from JSON; text from protobuf. */
export type Integer = string | number;

export interface AnyValue {
  readonly stringValue?: string;
  readonly boolValue?: boolean;
  readonly intValue?: Integer;
  readonly doubleValue?: number | string;
  readonly arrayValue?: { readonly values?: readonly AnyValue[] };
  readonly kvlistValue?: { readonly values?: readonly KeyValue[] };
  readonly bytesValue?: string | Uint8Array;
}

export interface KeyValue {
  readonly key?: string;
  readonly value?: AnyValue;
}

export interface Resource {
  readonly attributes?: readonly KeyValue[];
}

export interface Scope {
  readonly name?: string;
  readonly version?: string;
  readonly attributes?: readonly KeyValue[];
  readonly droppedAttributesCount?: number;
}

/** What a request's resource parts and their scope parts have alike. */
interface ScopePart {
  readonly scope?: Scope;
  readonly schemaUrl?: string;
}

interface ResourcePart {
  readonly resource?: Resource;
  readonly schemaUrl?: string;
}

export interface Span {
  readonly traceId?: Id;
  readonly spanId?: Id;
  readonly traceState?: string;
  readonly parentSpanId?: Id;
  readonly name?: string;
  readonly kind?: number;
  readonly startTimeUnixNano?: Integer;
  readonly endTimeUnixNano?: Integer;
  readonly attributes?: readonly KeyValue[];
  readonly events?: readonly {
    readonly timeUnixNano?: Integer;
    readonly name?: string;
    readonly attributes?: readonly KeyValue[];
  }[];
  readonly links?: readonly {
    readonly traceId?: Id;
    readonly spanId?: Id;
    readonly traceState?: string;
    readonly attributes?: readonly KeyValue[];
  }[];
  readonly status?: { readonly message?: string; readonly code?: number };
}

export interface TracesRequest {
  readonly resourceSpans?: readonly (ResourcePart & { readonly scopeSpans?: readonly (ScopePart & { readonly spans?: readonly Span[] })[] })[];
}

export interface Exemplar {
  readonly filteredAttributes?: readonly KeyValue[];
  readonly timeUnixNano?: Integer;
  readonly asDouble?: number | string;
  readonly asInt?: Integer;
  readonly spanId?: Id;
  readonly traceId?: Id;
}

/** What every data point has. */
export interface DataPoint {
  readonly attributes?: readonly KeyValue[];
  readonly startTimeUnixNano?: Integer;
  readonly timeUnixNano?: Integer;
  readonly flags?: number;
}

export interface NumberDataPoint extends DataPoint {
  readonly asDouble?: number | string;
  readonly asInt?: Integer;
  readonly exemplars?: readonly Exemplar[];
}

export interface HistogramDataPoint extends DataPoint {
  readonly count?: Integer;
  readonly sum?: number | string;
  readonly bucketCounts?: readonly Integer[];
  readonly explicitBounds?: readonly (number | string)[];
  readonly exemplars?: readonly Exemplar[];
  readonly min?: number | string;
  readonly max?: number | string;
}

export interface Buckets {
  readonly offset?: number;
  readonly bucketCounts?: readonly Integer[];
}

export interface ExponentialHistogramDataPoint extends DataPoint {
  readonly count?: Integer;
  readonly sum?: number | string;
  readonly scale?: number;
  readonly zeroCount?: Integer;
  readonly positive?: Buckets;
  readonly negative?: Buckets;
  readonly exemplars?: readonly Exemplar[];
  readonly min?: number | string;
  readonly max?: number | string;
}

export interface SummaryDataPoint extends DataPoint {
  readonly count?: Integer;
  readonly sum?: number | string;
  readonly quantileValues?: readonly { readonly quantile?: number | string; readonly value?: number | string }[];
}

export interface Metric {
  readonly name?: string;
  readonly description?: string;
  readonly unit?: string;
  readonly gauge?: { readonly dataPoints?: readonly NumberDataPoint[] };
  readonly sum?: { readonly dataPoints?: readonly NumberDataPoint[]; readonly aggregationTemporality?: number; readonly isMonotonic?: boolean };
  readonly histogram?: { readonly dataPoints?: readonly HistogramDataPoint[]; readonly aggregationTemporality?: number };
  readonly exponentialHistogram?: { readonly dataPoints?: readonly ExponentialHistogramDataPoint[]; readonly aggregationTemporality?: number };
  readonly summary?: { readonly dataPoints?: readonly SummaryDataPoint[] };
}

export interface MetricsRequest {
  readonly resourceMetrics?: readonly (ResourcePart & { readonly scopeMetrics?: readonly (ScopePart & { readonly metrics?: readonly Metric[] })[] })[];
}

export interface LogRecord {
  readonly timeUnixNano?: Integer;
  readonly observedTimeUnixNano?: Integer;
  readonly severityNumber?: number;
  readonly severityText?: string;
  readonly body?: AnyValue;
  readonly attributes?: readonly KeyValue[];
  readonly flags?: number;
  readonly traceId?: Id;
  readonly spanId?: Id;
  readonly eventName?: string;
}

export interface LogsRequest {
  readonly resourceLogs?: readonly (ResourcePart & { readonly scopeLogs?: readonly (ScopePart & { readonly logRecords?: readonly LogRecord[] })[] })[];
}

/** A request of each signal. */
export interface Requests {
  traces: TracesRequest;
  metrics: MetricsRequest;
  logs: LogsRequest;
}

const { root } = protobuf.parse(`
  syntax = "proto3";

  message AnyValue {
    oneof value {
      string string_value = 1;
      bool bool_value = 2;
      int64 int_value = 3;
      double double_value = 4;
      ArrayValue array_value = 5;
      KeyValueList kvlist_value = 6;
      bytes bytes_value = 7;
    }
  }
  message ArrayValue { repeated AnyValue values = 1; }
  message KeyValueList { repeated KeyValue values = 1; }
  message KeyValue { string key = 1; AnyValue value = 2; }
  message InstrumentationScope { string name = 1; string version = 2; repeated KeyValue attributes = 3; uint32 dropped_attributes_count = 4; }
  message Resource { repeated KeyValue attributes = 1; uint32 dropped_attributes_count = 2; }

  message ExportTraceServiceRequest { repeated ResourceSpans resource_spans = 1; }
  message ResourceSpans { Resource resource = 1; repeated ScopeSpans scope_spans = 2; string schema_url = 3; }
  message ScopeSpans { InstrumentationScope scope = 1; repeated Span spans = 2; string schema_url = 3; }
  message Span {
    bytes trace_id = 1;
    bytes span_id = 2;
    string trace_state = 3;
    bytes parent_span_id = 4;
    fixed32 flags = 16;
    string name = 5;
    int32 kind = 6;
    fixed64 start_time_unix_nano = 7;
    fixed64 end_time_unix_nano = 8;
    repeated KeyValue attributes = 9;
    uint32 dropped_attributes_count = 10;
    repeated Event events = 11;
    uint32 dropped_events_count = 12;
    repeated Link links = 13;
    uint32 dropped_links_count = 14;
    Status status = 15;
    message Event { fixed64 time_unix_nano = 1; string name = 2; repeated KeyValue attributes = 3; uint32 dropped_attributes_count = 4; }
    message Link { bytes trace_id = 1; bytes span_id = 2; string trace_state = 3; repeated KeyValue attributes = 4; uint32 dropped_attributes_count = 5; fixed32 flags = 6; }
  }
  message Status { string message = 2; int32 code = 3; }

  message ExportMetricsServiceRequest { repeated ResourceMetrics resource_metrics = 1; }
  message ResourceMetrics { Resource resource = 1; repeated ScopeMetrics scope_metrics = 2; string schema_url = 3; }
  message ScopeMetrics { InstrumentationScope scope = 1; repeated Metric metrics = 2; string schema_url = 3; }
  message Metric {
    string name = 1;
    string description = 2;
    string unit = 3;
    oneof data {
      Gauge gauge = 5;
      Sum sum = 7;
      Histogram histogram = 9;
      ExponentialHistogram exponential_histogram = 10;
      Summary summary = 11;
    }
    repeated KeyValue metadata = 12;
  }
  message Gauge { repeated NumberDataPoint data_points = 1; }
  message Sum { repeated NumberDataPoint data_points = 1; int32 aggregation_temporality = 2; bool is_monotonic = 3; }
  message Histogram { repeated HistogramDataPoint data_points = 1; int32 aggregation_temporality = 2; }
  message ExponentialHistogram { repeated ExponentialHistogramDataPoint data_points = 1; int32 aggregation_temporality = 2; }
  message Summary { repeated SummaryDataPoint data_points = 1; }
  message NumberDataPoint {
    repeated KeyValue attributes = 7;
    fixed64 start_time_unix_nano = 2;
    fixed64 time_unix_nano = 3;
    oneof value { double as_double = 4; sfixed64 as_int = 6; }
    repeated Exemplar exemplars = 5;
    uint32 flags = 8;
  }
  message HistogramDataPoint {
    repeated KeyValue attributes = 9;
    fixed64 start_time_unix_nano = 2;
    fixed64 time_unix_nano = 3;
    fixed64 count = 4;
    optional double sum = 5;
    repeated fixed64 bucket_counts = 6;
    repeated double explicit_bounds = 7;
    repeated Exemplar exemplars = 8;
    uint32 flags = 10;
    optional double min = 11;
    optional double max = 12;
  }
  message ExponentialHistogramDataPoint {
    repeated KeyValue attributes = 1;
    fixed64 start_time_unix_nano = 2;
    fixed64 time_unix_nano = 3;
    fixed64 count = 4;
    optional double sum = 5;
    sint32 scale = 6;
    fixed64 zero_count = 7;
    Buckets positive = 8;
    Buckets negative = 9;
    uint32 flags = 10;
    repeated Exemplar exemplars = 11;
    optional double min = 12;
    optional double max = 13;
    double zero_threshold = 14;
    message Buckets { sint32 offset = 1; repeated uint64 bucket_counts = 2; }
  }
  message SummaryDataPoint {
    repeated KeyValue attributes = 7;
    fixed64 start_time_unix_nano = 2;
    fixed64 time_unix_nano = 3;
    fixed64 count = 4;
    double sum = 5;
    repeated ValueAtQuantile quantile_values = 6;
    uint32 flags = 8;
    message ValueAtQuantile { double quantile = 1; double value = 2; }
  }
  message Exemplar {
    repeated KeyValue filtered_attributes = 7;
    fixed64 time_unix_nano = 2;
    oneof value { double as_double = 3; sfixed64 as_int = 6; }
    bytes span_id = 4;
    bytes trace_id = 5;
  }

  message ExportLogsServiceRequest { repeated ResourceLogs resource_logs = 1; }
  message ResourceLogs { Resource resource = 1; repeated ScopeLogs scope_logs = 2; string schema_url = 3; }
  message ScopeLogs { InstrumentationScope scope = 1; repeated LogRecord log_records = 2; string schema_url = 3; }
  message LogRecord {
    fixed64 time_unix_nano = 1;
    fixed64 observed_time_unix_nano = 11;
    int32 severity_number = 2;
    string severity_text = 3;
    AnyValue body = 5;
    repeated KeyValue attributes = 6;
    uint32 dropped_attributes_count = 7;
    fixed32 flags = 8;
    bytes trace_id = 9;
    bytes span_id = 10;
    string event_name = 12;
  }
`);

const REQUESTS: Readonly<Record<Signal, protobuf.Type>> = {
  traces: root.lookupType("ExportTraceServiceRequest"),
  metrics: root.lookupType("ExportMetricsServiceRequest"),
  logs: root.lookupType("ExportLogsServiceRequest"),
};

/**
 * `body`, an export request for `signal` in `encoding`, decoded. Throws
 * MalformedRequest for one that does not decode.
 */
export function decodeRequest<S extends Signal>(signal: S, encoding: Encoding, body: Uint8Array): Requests[S] {
  try {
    if (encoding === "json") {
      const request: unknown = JSON.parse(Buffer.from(body).toString("utf8"));
      if (typeof request !== "object" || request === null || Array.isArray(request)) throw new MalformedRequest("the request is not an object");
      // OTLP's JSON is the shape Requests describes; what a field holds is checked as it is read.
      return request as Requests[S];
    }
    const type = REQUESTS[signal];
    // The messages above are the shape Requests describes, 64-bit integers as text.
    return type.toObject(type.decode(body), { longs: String }) as Requests[S];
  } catch (error) {
    if (error instanceof MalformedRequest) throw error;
    throw new MalformedRequest(error instanceof Error ? error.message : String(error));
  }
}

/** An id as lowercase hex, "" for none or for one of zeros, as the ClickHouse exporter writes it. */
export function hexId(id: Id | undefined): string {
  const hex = typeof id === "string" ? id.toLowerCase() : Buffer.from(id ?? []).toString("hex");
  return /^0*$/u.test(hex) ? "" : hex;
}

/** A 64-bit integer, 0n for none. */
export function integer(value: Integer | undefined): bigint {
  if (value === undefined) return 0n;
  try {
    return BigInt(value);
  } catch {
    throw new MalformedRequest(`${JSON.stringify(value)} is not an integer`);
  }
}

/** A double, which JSON may give as text ("NaN", "Infinity"); null for none. */
export function double(value: number | string | undefined): number | null {
  return value === undefined ? null : Number(value);
}

/** Bytes as base64. */
export function base64(bytes: string | Uint8Array): string {
  return typeof bytes === "string" ? bytes : Buffer.from(bytes).toString("base64");
}
