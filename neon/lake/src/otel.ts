/**
 * The lake's `otel` schema: alasio's traces, metrics and logs, as the telemetry intake
 * receives them from the stack's collector (./intake.ts). Its tables are those of
 * OpenTelemetry's ClickHouse exporter, by name and column, so its queries carry over:
 *
 *   otel.traces                          a span a row
 *   otel.logs                            a log record a row
 *   otel.metrics_gauge, metrics_sum,     a data point a row, a table for each kind of
 *   metrics_histogram,                   metric
 *   metrics_exponential_histogram,
 *   metrics_summary
 *
 * As there, every attribute is kept as text in a MAP(VARCHAR, VARCHAR) (a number or a
 * boolean as it reads, a list or a map as JSON), each row names its service
 * (`ServiceName`, its resource's `service.name`, "" without one), ids are lowercase hex
 * ("" for none), and a span's events and links are lists of structs. Unlike there, a
 * metric's times keep their nanoseconds, and a histogram's sum, min and max are null
 * where its point has none.
 *
 * Telemetry is kept nowhere else, so this schema is not the derived model's
 * (./model.ts): a model rebuild never touches it. Its tables are made where missing and
 * never dropped; a change to them can only add to them. Each is partitioned by day, so
 * retention deletes whole days (deleteExpiredTelemetry) and maintenance then deletes
 * their files.
 *
 * Which telemetry is whose: alasio's resources and spans carry these attributes, the
 * keys a query joins on, to transcripts and to each other.
 *
 *   alasio.conversation.id  the conversation (Telegram's chat, as alasio keys it): on
 *                           alasio's `alasio.turn` span, and on the resource of each
 *                           Claude Code process and folder workspace's bayma, each of
 *                           which serves one conversation
 *   alasio.session.id       the harness's session (Claude Code's session id, Codex's
 *                           thread id; claude.entries.session_id, codex.lines.thread_id):
 *                           on `alasio.turn`
 *   alasio.volume.id        a session workspace's volume: on `alasio.turn` when the turn
 *                           runs in one, and on the resource of its bayma's telemetry,
 *                           which alasio stamps as it receives it
 *   alasio.branch           the branch environment, reserved: none is set before
 *                           branches are
 *
 * The rest join through these: alasio's logs within a turn carry its span's TraceId,
 * and Claude Code's own telemetry its `session.id`, the session's id.
 */
import { type DuckDBAppender, type DuckDBConnection, type DuckDBValue, listValue, mapValue, structValue, timestampNanosValue } from "@duckdb/node-api";

import { LAKE, serially, transaction } from "./lake.ts";
import {
  type AnyValue,
  base64,
  type Buckets,
  double,
  type Exemplar,
  hexId,
  integer,
  type Integer,
  type KeyValue,
  type LogsRequest,
  MalformedRequest,
  type MetricsRequest,
  type Requests,
  type Resource,
  type Scope,
  type Signal,
  type TracesRequest,
} from "./otlp.ts";

const MAP = "MAP(VARCHAR, VARCHAR)";
const EXEMPLARS = `STRUCT(FilteredAttributes ${MAP}, TimeUnix TIMESTAMP_NS, Value DOUBLE, SpanId VARCHAR, TraceId VARCHAR)[]`;
const METRIC = `
  ResourceAttributes ${MAP}, ResourceSchemaUrl VARCHAR, ScopeName VARCHAR, ScopeVersion VARCHAR,
  ScopeAttributes ${MAP}, ScopeDroppedAttrCount UINTEGER, ScopeSchemaUrl VARCHAR, ServiceName VARCHAR,
  MetricName VARCHAR, MetricDescription VARCHAR, MetricUnit VARCHAR, Attributes ${MAP},
  StartTimeUnix TIMESTAMP_NS, TimeUnix TIMESTAMP_NS`;

/**
 * Each table: its columns, the time it is partitioned and retained by, and whether an
 * insert into it is inlined in the catalog (INLINED_ROWS).
 */
const TABLES = {
  traces: {
    columns: `
      Timestamp TIMESTAMP_NS, TraceId VARCHAR, SpanId VARCHAR, ParentSpanId VARCHAR, TraceState VARCHAR,
      SpanName VARCHAR, SpanKind VARCHAR, ServiceName VARCHAR, ResourceAttributes ${MAP},
      ScopeName VARCHAR, ScopeVersion VARCHAR, SpanAttributes ${MAP}, Duration UBIGINT,
      StatusCode VARCHAR, StatusMessage VARCHAR,
      Events STRUCT(Timestamp TIMESTAMP_NS, Name VARCHAR, Attributes ${MAP})[],
      Links STRUCT(TraceId VARCHAR, SpanId VARCHAR, TraceState VARCHAR, Attributes ${MAP})[]`,
    time: "Timestamp",
    inlined: true,
  },
  logs: {
    columns: `
      Timestamp TIMESTAMP_NS, TraceId VARCHAR, SpanId VARCHAR, TraceFlags UTINYINT, SeverityText VARCHAR,
      SeverityNumber UTINYINT, ServiceName VARCHAR, Body VARCHAR, ResourceSchemaUrl VARCHAR,
      ResourceAttributes ${MAP}, ScopeSchemaUrl VARCHAR, ScopeName VARCHAR, ScopeVersion VARCHAR,
      ScopeAttributes ${MAP}, LogAttributes ${MAP}, EventName VARCHAR`,
    time: "Timestamp",
    inlined: true,
  },
  metrics_gauge: { columns: `${METRIC}, Value DOUBLE, Flags UINTEGER, Exemplars ${EXEMPLARS}`, time: "TimeUnix", inlined: false },
  metrics_sum: {
    columns: `${METRIC}, Value DOUBLE, Flags UINTEGER, Exemplars ${EXEMPLARS}, AggregationTemporality INTEGER, IsMonotonic BOOLEAN`,
    time: "TimeUnix",
    inlined: false,
  },
  metrics_histogram: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, BucketCounts UBIGINT[], ExplicitBounds DOUBLE[], Exemplars ${EXEMPLARS},
      Flags UINTEGER, Min DOUBLE, Max DOUBLE, AggregationTemporality INTEGER`,
    time: "TimeUnix",
    inlined: false,
  },
  metrics_exponential_histogram: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, Scale INTEGER, ZeroCount UBIGINT,
      PositiveOffset INTEGER, PositiveBucketCounts UBIGINT[], NegativeOffset INTEGER, NegativeBucketCounts UBIGINT[],
      Exemplars ${EXEMPLARS}, Flags UINTEGER, Min DOUBLE, Max DOUBLE, AggregationTemporality INTEGER`,
    time: "TimeUnix",
    inlined: false,
  },
  metrics_summary: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, ValueAtQuantiles STRUCT(Quantile DOUBLE, Value DOUBLE)[], Flags UINTEGER`,
    time: "TimeUnix",
    inlined: false,
  },
} as const;

/** A table of the `otel` schema. */
export type OtelTable = keyof typeof TABLES;

export const OTEL_TABLES = Object.keys(TABLES) as OtelTable[];

/** A row: its values by column, null for none. */
export type TelemetryRow = Readonly<Record<string, DuckDBValue>>;

/** The rows of a request, by table. */
export type TelemetryRows = Partial<Record<OtelTable, TelemetryRow[]>>;

/**
 * The most rows one insert into a table of spans or log records inlines, into the
 * catalog, rather than writes as a Parquet file of its own: as many as the collector
 * batches them in at most, every 5 seconds, so they make no small files and are queried
 * at once. The intake flushes what is inlined to Parquet now and then (flushTelemetry),
 * so the catalog holds at most that long's telemetry. Metric points are never inlined:
 * they are nearly all of it, and measured on the stack, inlining them cost Neon some 25
 * to 45 KB of WAL a second, and the catalog 70 to 130 MB an hour until flushed; the
 * collector sends them a minute's at a time, which the lake writes as a file a kind.
 */
export const INLINED_ROWS = 1000;

/**
 * Makes the `otel` schema's tables where they are missing, each partitioned by its
 * day, and sets how many rows of an insert into each it inlines.
 */
export async function ensureOtel(db: DuckDBConnection): Promise<void> {
  await transaction(db, async () => {
    await db.run(`create schema if not exists ${LAKE}.otel`);
    for (const [table, { columns, time }] of Object.entries(TABLES)) {
      const [exists] = (await db.runAndReadAll(
        `select 1 from duckdb_tables() where database_name = '${LAKE}' and schema_name = 'otel' and table_name = $1`,
        [table],
      )).getRows();
      if (exists) continue;
      await db.run(`create table ${LAKE}.otel.${table} (${columns})`);
      await db.run(`alter table ${LAKE}.otel.${table} set partitioned by (year(${time}), month(${time}), day(${time}))`);
    }
  });
  // Once the schema is committed: DuckLake sets no option of a schema made in the same transaction.
  await serially(async () => {
    for (const [table, { inlined }] of Object.entries(TABLES)) {
      await db.run(`call ${LAKE}.set_option('data_inlining_row_limit', ${inlined ? INLINED_ROWS : 0}, schema => 'otel', table_name => '${table}')`);
    }
  });
}

// --- Rows ---------------------------------------------------------------------------

/** An attribute's value as text, as the ClickHouse exporter keeps it (pdata's AsString). */
function text(value: AnyValue | undefined): string {
  if (value === undefined) return "";
  if (value.stringValue !== undefined) return value.stringValue;
  if (value.boolValue !== undefined) return String(value.boolValue);
  if (value.intValue !== undefined) return String(integer(value.intValue));
  if (value.doubleValue !== undefined) return String(Number(value.doubleValue));
  if (value.bytesValue !== undefined) return base64(value.bytesValue);
  if (value.arrayValue !== undefined || value.kvlistValue !== undefined) return json(value);
  return "";
}

/** A list's or a map's value as JSON, its integers whole, however large. */
function json(value: AnyValue | undefined): string {
  if (value === undefined) return "null";
  if (value.stringValue !== undefined) return JSON.stringify(value.stringValue);
  if (value.boolValue !== undefined) return String(value.boolValue);
  if (value.intValue !== undefined) return String(integer(value.intValue));
  if (value.doubleValue !== undefined) {
    const number = Number(value.doubleValue);
    return Number.isFinite(number) ? String(number) : JSON.stringify(String(number));
  }
  if (value.bytesValue !== undefined) return JSON.stringify(base64(value.bytesValue));
  if (value.arrayValue !== undefined) return `[${(value.arrayValue.values ?? []).map(json).join(",")}]`;
  if (value.kvlistValue !== undefined) return `{${(value.kvlistValue.values ?? []).map(({ key, value }) => `${JSON.stringify(key ?? "")}:${json(value)}`).join(",")}}`;
  return "null";
}

/** Attributes as a map of text, the last of a key repeated winning, as pdata reads them. */
function attributes(list: readonly KeyValue[] | undefined): DuckDBValue {
  const byKey = new Map((list ?? []).map(({ key, value }) => [key ?? "", text(value)]));
  return mapValue([...byKey].map(([key, value]) => ({ key, value })));
}

/** The resource's service, "" for none. */
function serviceName(resource: Resource | undefined): string {
  return text(resource?.attributes?.findLast(({ key }) => key === "service.name")?.value);
}

/** A time, in nanoseconds since the epoch. */
const time = (nanos: Integer | undefined): DuckDBValue => timestampNanosValue(integer(nanos));

const unsigned = (value: Integer | undefined): bigint => {
  const whole = integer(value);
  if (whole < 0n) throw new MalformedRequest(`${whole} is negative`);
  return whole;
};

/** A field whose type the request's own decoding already checked, or JSON gave as asked: a list, or none. */
function list<T>(value: readonly T[] | undefined, what: string): readonly T[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new MalformedRequest(`${what} is not a list`);
  return value;
}

const SPAN_KINDS = ["Unspecified", "Internal", "Server", "Client", "Producer", "Consumer"];
const STATUS_CODES = ["Unset", "Ok", "Error"];

/** A resource's and a scope's columns, as every metric's row has them. */
function metricOrigin(resource: Resource | undefined, resourceSchemaUrl: string | undefined, scope: Scope | undefined, scopeSchemaUrl: string | undefined): TelemetryRow {
  return {
    ResourceAttributes: attributes(resource?.attributes),
    ResourceSchemaUrl: resourceSchemaUrl ?? "",
    ScopeName: scope?.name ?? "",
    ScopeVersion: scope?.version ?? "",
    ScopeAttributes: attributes(scope?.attributes),
    ScopeDroppedAttrCount: scope?.droppedAttributesCount ?? 0,
    ScopeSchemaUrl: scopeSchemaUrl ?? "",
    ServiceName: serviceName(resource),
  };
}

function exemplars(given: readonly Exemplar[] | undefined): DuckDBValue {
  return listValue(list(given, "exemplars").map((exemplar) =>
    structValue({
      FilteredAttributes: attributes(exemplar.filteredAttributes),
      TimeUnix: time(exemplar.timeUnixNano),
      Value: double(exemplar.asDouble) ?? Number(integer(exemplar.asInt)),
      // As the exporter writes an exemplar's ids: all zeros, not "", for none.
      SpanId: hexId(exemplar.spanId) || "0000000000000000",
      TraceId: hexId(exemplar.traceId) || "00000000000000000000000000000000",
    })
  ));
}

const counts = (values: readonly Integer[] | undefined): DuckDBValue => listValue(list(values, "bucket counts").map(unsigned));

function bucketColumns(prefix: "Positive" | "Negative", buckets: Buckets | undefined): TelemetryRow {
  return { [`${prefix}Offset`]: buckets?.offset ?? 0, [`${prefix}BucketCounts`]: counts(buckets?.bucketCounts) };
}

function traceRows(request: TracesRequest): TelemetryRows {
  const traces: TelemetryRow[] = [];
  for (const part of list(request.resourceSpans, "resourceSpans")) {
    const resource = { ServiceName: serviceName(part.resource), ResourceAttributes: attributes(part.resource?.attributes) };
    for (const { scope, spans } of list(part.scopeSpans, "scopeSpans")) {
      for (const span of list(spans, "spans")) {
        const start = integer(span.startTimeUnixNano);
        const end = integer(span.endTimeUnixNano);
        traces.push({
          Timestamp: timestampNanosValue(start),
          TraceId: hexId(span.traceId),
          SpanId: hexId(span.spanId),
          ParentSpanId: hexId(span.parentSpanId),
          TraceState: span.traceState ?? "",
          SpanName: span.name ?? "",
          SpanKind: SPAN_KINDS[span.kind ?? 0] ?? "",
          ...resource,
          ScopeName: scope?.name ?? "",
          ScopeVersion: scope?.version ?? "",
          SpanAttributes: attributes(span.attributes),
          Duration: end > start ? end - start : 0n,
          StatusCode: STATUS_CODES[span.status?.code ?? 0] ?? "",
          StatusMessage: span.status?.message ?? "",
          Events: listValue(list(span.events, "events").map((event) =>
            structValue({ Timestamp: time(event.timeUnixNano), Name: event.name ?? "", Attributes: attributes(event.attributes) })
          )),
          Links: listValue(list(span.links, "links").map((link) =>
            structValue({ TraceId: hexId(link.traceId), SpanId: hexId(link.spanId), TraceState: link.traceState ?? "", Attributes: attributes(link.attributes) })
          )),
        });
      }
    }
  }
  return { traces };
}

function logRows(request: LogsRequest): TelemetryRows {
  const logs: TelemetryRow[] = [];
  for (const part of list(request.resourceLogs, "resourceLogs")) {
    const resource = { ServiceName: serviceName(part.resource), ResourceSchemaUrl: part.schemaUrl ?? "", ResourceAttributes: attributes(part.resource?.attributes) };
    for (const { scope, schemaUrl, logRecords } of list(part.scopeLogs, "scopeLogs")) {
      const origin = { ScopeSchemaUrl: schemaUrl ?? "", ScopeName: scope?.name ?? "", ScopeVersion: scope?.version ?? "", ScopeAttributes: attributes(scope?.attributes) };
      for (const record of list(logRecords, "logRecords")) {
        const at = integer(record.timeUnixNano);
        logs.push({
          // Where a record has no time of its own, the time it was seen.
          Timestamp: timestampNanosValue(at === 0n ? integer(record.observedTimeUnixNano) : at),
          TraceId: hexId(record.traceId),
          SpanId: hexId(record.spanId),
          TraceFlags: (record.flags ?? 0) & 0xff,
          SeverityText: record.severityText ?? "",
          SeverityNumber: Math.min(Math.max(record.severityNumber ?? 0, 0), 255),
          Body: text(record.body),
          ...resource,
          ...origin,
          LogAttributes: attributes(record.attributes),
          EventName: record.eventName ?? "",
        });
      }
    }
  }
  return { logs };
}

function metricRows(request: MetricsRequest): TelemetryRows {
  const rows: Required<Pick<TelemetryRows, Exclude<OtelTable, "traces" | "logs">>> = {
    metrics_gauge: [],
    metrics_sum: [],
    metrics_histogram: [],
    metrics_exponential_histogram: [],
    metrics_summary: [],
  };
  for (const part of list(request.resourceMetrics, "resourceMetrics")) {
    for (const { scope, schemaUrl, metrics } of list(part.scopeMetrics, "scopeMetrics")) {
      const origin = metricOrigin(part.resource, part.schemaUrl, scope, schemaUrl);
      for (const metric of list(metrics, "metrics")) {
        const named = { ...origin, MetricName: metric.name ?? "", MetricDescription: metric.description ?? "", MetricUnit: metric.unit ?? "" };
        const point = (dataPoint: { attributes?: readonly KeyValue[]; startTimeUnixNano?: Integer; timeUnixNano?: Integer; flags?: number }) => ({
          ...named,
          Attributes: attributes(dataPoint.attributes),
          StartTimeUnix: time(dataPoint.startTimeUnixNano),
          TimeUnix: time(dataPoint.timeUnixNano),
          Flags: (dataPoint.flags ?? 0) >>> 0,
        });
        const value = (dataPoint: { asDouble?: number | string; asInt?: Integer }) => double(dataPoint.asDouble) ?? Number(integer(dataPoint.asInt));
        if (metric.gauge) {
          for (const dataPoint of list(metric.gauge.dataPoints, "dataPoints")) {
            rows.metrics_gauge.push({ ...point(dataPoint), Value: value(dataPoint), Exemplars: exemplars(dataPoint.exemplars) });
          }
        } else if (metric.sum) {
          const { aggregationTemporality = 0, isMonotonic = false } = metric.sum;
          for (const dataPoint of list(metric.sum.dataPoints, "dataPoints")) {
            rows.metrics_sum.push({
              ...point(dataPoint),
              Value: value(dataPoint),
              Exemplars: exemplars(dataPoint.exemplars),
              AggregationTemporality: aggregationTemporality,
              IsMonotonic: isMonotonic,
            });
          }
        } else if (metric.histogram) {
          for (const dataPoint of list(metric.histogram.dataPoints, "dataPoints")) {
            rows.metrics_histogram.push({
              ...point(dataPoint),
              Count: unsigned(dataPoint.count),
              Sum: double(dataPoint.sum),
              BucketCounts: counts(dataPoint.bucketCounts),
              ExplicitBounds: listValue(list(dataPoint.explicitBounds, "explicitBounds").map(Number)),
              Exemplars: exemplars(dataPoint.exemplars),
              Min: double(dataPoint.min),
              Max: double(dataPoint.max),
              AggregationTemporality: metric.histogram.aggregationTemporality ?? 0,
            });
          }
        } else if (metric.exponentialHistogram) {
          for (const dataPoint of list(metric.exponentialHistogram.dataPoints, "dataPoints")) {
            rows.metrics_exponential_histogram.push({
              ...point(dataPoint),
              Count: unsigned(dataPoint.count),
              Sum: double(dataPoint.sum),
              Scale: dataPoint.scale ?? 0,
              ZeroCount: unsigned(dataPoint.zeroCount),
              ...bucketColumns("Positive", dataPoint.positive),
              ...bucketColumns("Negative", dataPoint.negative),
              Exemplars: exemplars(dataPoint.exemplars),
              Min: double(dataPoint.min),
              Max: double(dataPoint.max),
              AggregationTemporality: metric.exponentialHistogram.aggregationTemporality ?? 0,
            });
          }
        } else if (metric.summary) {
          for (const dataPoint of list(metric.summary.dataPoints, "dataPoints")) {
            rows.metrics_summary.push({
              ...point(dataPoint),
              Count: unsigned(dataPoint.count),
              Sum: double(dataPoint.sum) ?? 0,
              ValueAtQuantiles: listValue(list(dataPoint.quantileValues, "quantileValues").map((quantile) =>
                structValue({ Quantile: double(quantile.quantile) ?? 0, Value: double(quantile.value) ?? 0 })
              )),
            });
          }
        }
      }
    }
  }
  return rows;
}

const ROWS: { readonly [S in Signal]: (request: Requests[S]) => TelemetryRows } = { traces: traceRows, metrics: metricRows, logs: logRows };

/**
 * The rows of `request`, a decoded export request for `signal` (./otlp.ts), by table.
 * Throws MalformedRequest for a request whose fields are not what OTLP's are.
 */
export function telemetryRows<S extends Signal>(signal: S, request: Requests[S]): TelemetryRows {
  try {
    return ROWS[signal](request);
  } catch (error) {
    if (error instanceof MalformedRequest) throw error;
    // A field of another type than OTLP's, as JSON can give one.
    throw new MalformedRequest(error instanceof Error ? error.message : String(error));
  }
}

// --- Writing ------------------------------------------------------------------------

/** The temporary table a table's rows are staged in, in DuckDB's own memory. */
const staging = (table: OtelTable) => `otel_${table}`;

/** Appends `row` with the appender, in its table's column order. */
function append(appender: DuckDBAppender, columns: readonly string[], row: TelemetryRow): void {
  columns.forEach((column, index) => {
    const value = row[column];
    if (value === null || value === undefined) appender.appendNull();
    else appender.appendValue(value, appender.columnType(index));
  });
  appender.endRow();
}

/**
 * Writes `rows` (telemetryRows's) to the lake in one transaction, staged in DuckDB's
 * memory first, so the transaction is no longer than its inserts. Returns how many
 * rows each table was given.
 */
export async function writeTelemetry(db: DuckDBConnection, rows: TelemetryRows): Promise<Partial<Record<OtelTable, number>>> {
  const written: Partial<Record<OtelTable, number>> = {};
  for (const table of OTEL_TABLES) {
    const tableRows = rows[table] ?? [];
    if (tableRows.length === 0) continue;
    await db.run(`create or replace temp table ${staging(table)} (${TABLES[table].columns})`);
    const columns = (await db.runAndReadAll(`from temp.${staging(table)} limit 0`)).columnNames();
    const appender = await db.createAppender(staging(table), "main", "temp");
    try {
      for (const row of tableRows) append(appender, columns, row);
      appender.flushSync();
    } finally {
      appender.closeSync();
    }
    written[table] = tableRows.length;
  }
  await transaction(db, async () => {
    for (const table of Object.keys(written) as OtelTable[]) await db.run(`insert into ${LAKE}.otel.${table} from temp.${staging(table)}`);
  });
  return written;
}

/** Moves the telemetry the catalog holds inlined to Parquet files. */
export function flushTelemetry(db: DuckDBConnection): Promise<unknown> {
  return serially(() => db.run(`call ducklake_flush_inlined_data('${LAKE}', schema_name => 'otel')`));
}

/**
 * Deletes the telemetry of the days before the last `retentionDays` whole days (UTC)
 * before `now`'s, every table's in one transaction: each day is a partition of its
 * own, so its files are deleted whole, by the maintenance that follows. Returns how many
 * rows it deleted.
 */
export async function deleteExpiredTelemetry(db: DuckDBConnection, retentionDays: number, now = new Date()): Promise<number> {
  const cutoff = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - retentionDays)).toISOString().slice(0, 10);
  return transaction(db, async () => {
    let deleted = 0;
    for (const [table, { time }] of Object.entries(TABLES)) {
      deleted += Number((await db.run(`delete from ${LAKE}.otel.${table} where ${time} < '${cutoff}'::TIMESTAMP_NS`)).rowsChanged);
    }
    return deleted;
  });
}
