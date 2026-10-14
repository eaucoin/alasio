/**
 * OTLP export requests, as the telemetry intake consumes them from the log (./intake.ts),
 * a record each: binary protobuf, decoded by protobufjs from the messages below into
 * OTLP's JSON shape, but for what JSON spells as text and protobuf does not: an id is
 * bytes, where JSON has hex, a 64-bit integer a string, and bytes bytes, where JSON has
 * base64. Whoever reads a request reads those through `hexId`, `integer` and `base64`,
 * which take either spelling.
 *
 * The messages are opentelemetry-proto's for the three signals' export requests, as of
 * its 1.5, which gave a log record its event name: enums as the integers they are on
 * the wire, and what the intake does not keep (a resource's entity references) left
 * out, which protobuf skips.
 */
import protobuf from "protobufjs";

/** A signal of the telemetry's. */
export type Signal = "traces" | "metrics" | "logs";

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

/** `request`, an export request for `signal`, in binary protobuf, as the collector writes each to the log. */
export function encodeRequest<S extends Signal>(signal: S, request: Requests[S]): Uint8Array {
  const type = REQUESTS[signal];
  return type.encode(type.fromObject(request)).finish();
}

/**
 * The fields of each level of a signal's request, down to its items: a request's resources,
 * a resource's scopes, a scope's spans, log records or metrics, and of a metric, its kind's
 * message (whichever of the oneof it has) and that message's points. A part is a request
 * of one item's way down, with as many of its items as PART_ITEMS.
 */
const LEVELS: Readonly<Record<Signal, readonly (readonly string[])[]>> = {
  traces: [["resourceSpans"], ["scopeSpans"], ["spans"]],
  logs: [["resourceLogs"], ["scopeLogs"], ["logRecords"]],
  metrics: [["resourceMetrics"], ["scopeMetrics"], ["metrics"], ["gauge", "sum", "histogram", "exponentialHistogram", "summary"], ["dataPoints"]],
};

/**
 * The most items a part has: a request's rows come a few thousand at a time at most,
 * however large the request, as a record of the log's may be as large as the log takes.
 */
export const PART_ITEMS = 1024;

/**
 * The most rows a request makes, and the most bytes its rows come to, each with what it
 * repeats of the levels above it (its resource's attributes, its scope's, its metric's
 * name): past either, a request is refused (MalformedRequest), as one record's rows would
 * outgrow what the lake stages of a batch. The largest record of a scrape's points makes
 * some 53,000 rows of some 16 MB; a resource's attributes of a MB on a thousand spans,
 * a GB.
 */
export const REQUEST_ROWS = 200_000;
export const REQUEST_ROW_BYTES = 64 * 1024 * 1024;

/** A message of a level, decoded as Requests spells it, 64-bit integers as text. */
const plain = (type: protobuf.Type, bytes: Uint8Array): Record<string, unknown> => type.toObject(type.decode(bytes), { longs: String });

/**
 * The parts of `bytes`, a message of `type` at level `depth` of `levels`, each given to
 * `within`, which puts it in the levels above, whose own fields come to `above` bytes. The
 * message is read for where its fields are, nothing decoded: of the level below, each one's
 * bytes, taken one at a time; of the rest, the message's own fields, decoded alone, which a
 * part repeats. What its items' rows come to is counted in `made`, against REQUEST_ROWS and
 * REQUEST_ROW_BYTES, before they are decoded.
 */
/**
 * Where the fields of `bytes`, a message of `type` at level `depth` of `levels`, are, nothing
 * decoded: its own fields' bytes, and of the level below, each one's field and span.
 */
function fieldsOf(type: protobuf.Type, bytes: Uint8Array, levels: readonly (readonly string[])[], depth: number) {
  const below = new Map((levels[depth] ?? []).map((name) => [type.fields[name]!.id, type.fields[name]!]));
  const reader = protobuf.Reader.create(bytes);
  const own: Uint8Array[] = [];
  const children: { field: protobuf.Field; start: number; end: number }[] = [];
  while (reader.pos < reader.len) {
    const start = reader.pos;
    const tag = reader.uint32();
    const field = below.get(tag >>> 3);
    if (field && (tag & 7) === 2) {
      const length = reader.uint32();
      children.push({ field, start: reader.pos, end: reader.pos + length });
      reader.skip(length);
    } else {
      reader.skipType(tag & 7);
      own.push(bytes.subarray(start, reader.pos));
    }
  }
  return { own, children };
}

function* partsOf(
  type: protobuf.Type,
  bytes: Uint8Array,
  levels: readonly (readonly string[])[],
  depth: number,
  within: (part: object) => object,
  above: number,
  made: { rows: number; bytes: number },
): Generator<object> {
  const { own, children } = fieldsOf(type, bytes, levels, depth);
  const header = plain(type, Buffer.concat(own));
  const repeated = above + own.reduce((sum, field) => sum + field.length, 0);
  const of = (field: protobuf.Field, value: object) => within({ ...header, [field.name]: field.repeated ? [value] : value });
  if (depth < levels.length - 1) {
    for (const { field, start, end } of children) yield* partsOf(field.resolvedType as protobuf.Type, bytes.subarray(start, end), levels, depth + 1, (part) => of(field, part), repeated, made);
    return;
  }
  // The items, PART_ITEMS at a time, each decoded as its part is made.
  for (let at = 0; at < children.length; at += PART_ITEMS) {
    const items = children.slice(at, at + PART_ITEMS);
    made.rows += items.length;
    made.bytes += items.reduce((sum, { start, end }) => sum + end - start + repeated, 0);
    if (made.rows > REQUEST_ROWS) throw new MalformedRequest(`the request makes more than ${REQUEST_ROWS} rows`);
    if (made.bytes > REQUEST_ROW_BYTES) throw new MalformedRequest(`the request's rows come to more than ${REQUEST_ROW_BYTES} bytes`);
    const field = items[0]!.field;
    yield within({ ...header, [field.name]: items.map(({ start, end }) => plain(field.resolvedType as protobuf.Type, bytes.subarray(start, end))) });
  }
}

/**
 * `body`, an export request for `signal` in binary protobuf, a part at a time: each a request
 * of its own of one resource's one scope's items (of a metric's points, one metric's), as
 * many as PART_ITEMS, decoded only as it is taken, so that what a request holds decoded at
 * once is a part's, however large the request. Their rows are the request's (telemetryRows,
 * ./otel.ts). Throws MalformedRequest, as it reaches it, where the request does not decode,
 * or its rows would be more than REQUEST_ROWS, or come to more than REQUEST_ROW_BYTES.
 */
export function* requestParts<S extends Signal>(signal: S, body: Uint8Array): Generator<Requests[S]> {
  try {
    // The messages above are the shape Requests describes.
    yield* partsOf(REQUESTS[signal], body, LEVELS[signal], 0, (part) => part, 0, { rows: 0, bytes: 0 }) as Generator<Requests[S]>;
  } catch (error) {
    if (error instanceof MalformedRequest) throw error;
    throw new MalformedRequest(error instanceof Error ? error.message : String(error));
  }
}

/** What the rows of `bytes` come to, as partsOf counts them, read for where its fields are alone. */
function rowBytesOf(type: protobuf.Type, bytes: Uint8Array, levels: readonly (readonly string[])[], depth: number, above: number): number {
  const { own, children } = fieldsOf(type, bytes, levels, depth);
  const repeated = above + own.reduce((sum, field) => sum + field.length, 0);
  if (depth < levels.length - 1) return children.reduce((sum, { field, start, end }) => sum + rowBytesOf(field.resolvedType as protobuf.Type, bytes.subarray(start, end), levels, depth + 1, repeated), 0);
  return children.reduce((sum, { start, end }) => sum + end - start + repeated, 0);
}

/**
 * What the rows of `body`, an export request for `signal` in binary protobuf, come to in
 * bytes, as requestParts counts them against REQUEST_ROW_BYTES, nothing decoded: what
 * writing it stages; 0 where it does not read as one, as it is refused as it is written.
 */
export function requestRowBytes(signal: Signal, body: Uint8Array): number {
  try {
    return rowBytesOf(REQUESTS[signal], body, LEVELS[signal], 0, 0);
  } catch {
    return 0;
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
