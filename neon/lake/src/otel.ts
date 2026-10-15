/**
 * The lake's `otel` schema: alasio's traces, metrics and logs, as the telemetry intake
 * consumes them from the log's topics, which the stack's collector writes (./intake.ts). Its tables are those of
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
import { createHash } from "node:crypto";

import { type DuckDBAppender, type DuckDBConnection, DuckDBDataChunk, type DuckDBType, type DuckDBValue, listValue, mapValue, structValue, timestampNanosValue } from "@duckdb/node-api";

import { LAKE, rows, transaction } from "./lake.ts";
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
  type LogRecord,
  type LogsRequest,
  MalformedRequest,
  type MetricsRequest,
  type Requests,
  type Resource,
  type ResourcePart,
  type Scope,
  type ScopePart,
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

/** Each table: its columns, and the time it is partitioned and retained by. */
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
  },
  logs: {
    columns: `
      Timestamp TIMESTAMP_NS, TraceId VARCHAR, SpanId VARCHAR, TraceFlags UTINYINT, SeverityText VARCHAR,
      SeverityNumber UTINYINT, ServiceName VARCHAR, Body VARCHAR, ResourceSchemaUrl VARCHAR,
      ResourceAttributes ${MAP}, ScopeSchemaUrl VARCHAR, ScopeName VARCHAR, ScopeVersion VARCHAR,
      ScopeAttributes ${MAP}, LogAttributes ${MAP}, EventName VARCHAR`,
    time: "Timestamp",
  },
  metrics_gauge: { columns: `${METRIC}, Value DOUBLE, Flags UINTEGER, Exemplars ${EXEMPLARS}`, time: "TimeUnix" },
  metrics_sum: {
    columns: `${METRIC}, Value DOUBLE, Flags UINTEGER, Exemplars ${EXEMPLARS}, AggregationTemporality INTEGER, IsMonotonic BOOLEAN`,
    time: "TimeUnix",
  },
  metrics_histogram: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, BucketCounts UBIGINT[], ExplicitBounds DOUBLE[], Exemplars ${EXEMPLARS},
      Flags UINTEGER, Min DOUBLE, Max DOUBLE, AggregationTemporality INTEGER`,
    time: "TimeUnix",
  },
  metrics_exponential_histogram: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, Scale INTEGER, ZeroCount UBIGINT,
      PositiveOffset INTEGER, PositiveBucketCounts UBIGINT[], NegativeOffset INTEGER, NegativeBucketCounts UBIGINT[],
      Exemplars ${EXEMPLARS}, Flags UINTEGER, Min DOUBLE, Max DOUBLE, AggregationTemporality INTEGER`,
    time: "TimeUnix",
  },
  metrics_summary: {
    columns: `${METRIC}, Count UBIGINT, Sum DOUBLE, ValueAtQuantiles STRUCT(Quantile DOUBLE, Value DOUBLE)[], Flags UINTEGER`,
    time: "TimeUnix",
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
 * The size up to which the `otel` tables' files are merged, the lake's (TARGET_FILE_SIZE
 * in ./sync.ts) being for transcripts: a file this large is left as it is, so the
 * hourly merge (mergeTelemetry) takes the files written since, not the whole day's again. At the
 * lake's 64 MB, a day's file of points (some 1.5 million, 14 MB, on the stack) was
 * rewritten every hour: measured on a day of the stack's sum points, the merge took
 * 0.6 s and 170 MB at 02:00, 4.8 s and 350 MB at 22:00; with this, 0.4-0.5 s and some
 * 180 MB at any hour, a day's points in a dozen files.
 */
export const TELEMETRY_FILE_SIZE = "1MB";

/**
 * Where the intake has consumed each partition of the log's telemetry topics to: the
 * offset of the next record it is to write, committed with the rows of those before it,
 * so a batch written is never written again, whatever the log's consumer group says
 * (./intake.ts); of the topic by its id, as a topic made anew (the log's, on volumes made
 * anew under a lake that was kept) numbers its records from its start again. Its few rows,
 * rewritten at each commit, are inlined in the catalog, as no telemetry is: they would
 * make two files a commit otherwise.
 */
const OFFSETS = "intake_offsets";
const OFFSETS_COLUMNS = "Topic VARCHAR, TopicId VARCHAR, PartitionId INTEGER, NextOffset BIGINT";

/**
 * What the intake is asked to read again from the log (askReread): of a signal's topic, its
 * records from `Since` on, the earliest time of the rows the lake lost of it. Inlined in the
 * catalog, as the offsets are.
 */
const REREADS = "intake_rereads";
const REREADS_COLUMNS = "Signal VARCHAR, Since TIMESTAMP_NS";

/**
 * What the intake, taking up a reread, found the log no longer holds (RereadGap): of a
 * signal, the time from which it was asked to read again to that of the first record the log
 * still holds, and when it found that. Inlined in the catalog, as the rereads are.
 */
const REREAD_GAPS = "intake_reread_gaps";
const REREAD_GAPS_COLUMNS = "Signal VARCHAR, FromTime TIMESTAMP_NS, ToTime TIMESTAMP_NS, FoundAt TIMESTAMP_NS";

/**
 * Makes the `otel` schema's tables where they are missing, each partitioned by its
 * day, and the intake's offsets, what it is asked to read again and what it found it could
 * not; and sets that nothing of the telemetry is inlined in the catalog, those three alone,
 * and the size up to which the telemetry's files are merged.
 */
export async function ensureOtel(db: DuckDBConnection): Promise<void> {
  await transaction(db, async () => {
    await db.run(`create schema if not exists ${LAKE}.otel`);
    await db.run(`create table if not exists ${LAKE}.otel.${OFFSETS} (${OFFSETS_COLUMNS})`);
    await db.run(`create table if not exists ${LAKE}.otel.${REREADS} (${REREADS_COLUMNS})`);
    await db.run(`create table if not exists ${LAKE}.otel.${REREAD_GAPS} (${REREAD_GAPS_COLUMNS})`);
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
  await transaction(db, async () => {
    for (const table of OTEL_TABLES) {
      await db.run(`call ${LAKE}.set_option('data_inlining_row_limit', 0, schema => 'otel', table_name => '${table}')`);
      await db.run(`call ${LAKE}.set_option('target_file_size', '${TELEMETRY_FILE_SIZE}', schema => 'otel', table_name => '${table}')`);
    }
    for (const table of [OFFSETS, REREADS, REREAD_GAPS]) await db.run(`call ${LAKE}.set_option('data_inlining_row_limit', 1000, schema => 'otel', table_name => '${table}')`);
  });
}

/** The signal each table's rows are of, whose topic the intake reads them from. */
const SIGNALS: Readonly<Record<OtelTable, Signal>> = {
  traces: "traces",
  logs: "logs",
  metrics_gauge: "metrics",
  metrics_sum: "metrics",
  metrics_histogram: "metrics",
  metrics_exponential_histogram: "metrics",
  metrics_summary: "metrics",
};

/** The table of the telemetry's named `table` in the lake, as "otel.traces"; null for another. */
const telemetryTable = (table: string): OtelTable | null => {
  const name = /^otel\.(\w+)$/u.exec(table)?.[1];
  return name !== undefined && Object.hasOwn(TABLES, name) ? name as OtelTable : null;
};

/** The column of the time of each row of `table`, a table of the lake's as "otel.traces": a lost file's rows' earliest is its range's start (askReread). Null for a table not of the telemetry's. */
export function telemetryTime(table: string): string | null {
  const known = telemetryTable(table);
  return known === null ? null : TABLES[known].time;
}

/** Rows a table of the telemetry's lost, from `from` on: the earliest of its time column (telemetryTime) they held, as text; null where that is not known, for all the log holds. */
export interface TelemetryLoss {
  readonly table: string;
  readonly from: string | null;
}

/** The time a loss whose start is not known is read again from: the log's first record's on. */
const ALL_OF_IT = "1970-01-01 00:00:00";

/**
 * Asks the intake to read again from the log what `losses` lost, as the lake's catalog,
 * set right with its store, forgets files the store no longer holds: of each signal's topic,
 * its records from the earliest of its tables' losses on. The intake takes it up at its next
 * write (writeConsumed), where it sets its offsets back to where the log has those records
 * and reads from there, which writes each row the lake no longer has, once. Asked again
 * before then, it reads from the earlier.
 */
export async function askReread(db: DuckDBConnection, losses: readonly TelemetryLoss[]): Promise<void> {
  const known = losses.map((loss) => {
    const table = telemetryTable(loss.table);
    if (table === null) throw new Error(`${loss.table} is no table of the telemetry's`);
    return { ...loss, signal: SIGNALS[table] };
  });
  await transaction(db, async () => {
    for (const { signal, from } of known) await db.run(`insert into ${LAKE}.otel.${REREADS} values ($1, $2::TIMESTAMP_NS)`, [signal, from ?? ALL_OF_IT]);
  });
}

/** What the intake is asked to read again of a signal's topic: from `since`, as text, and in milliseconds since the epoch, on. */
export interface Reread {
  readonly signal: Signal;
  readonly since: string;
  readonly sinceMs: number;
}

/** What the intake is asked to read again and has not yet taken up, of each signal the earliest asked. */
export async function rereadsAsked(db: DuckDBConnection): Promise<Reread[]> {
  return (await rows<{ Signal: Signal; since: string; ms: bigint }>(
    db,
    `select Signal, min(Since)::varchar as since, epoch_ms(min(Since)::TIMESTAMP) as ms from ${LAKE}.otel.${REREADS} group by Signal order by Signal`,
  )).map(({ Signal, since, ms }) => ({ signal: Signal, since, sinceMs: Number(ms) }));
}

/** The intake was asked to read records again (askReread): a write takes that up before any. */
export class RereadAsked extends Error {
  override readonly name = "RereadAsked";
  readonly asked: readonly Reread[];
  constructor(asked: readonly Reread[]) {
    super(`the intake is asked to read again ${asked.map(({ signal, since }) => `${signal} from ${since}`).join(", ")}`);
    this.asked = asked;
  }
}

/**
 * What a reread cannot refill: of `signal`, the rows of `from` to `to`, ISO 8601 times, as
 * the log no longer holds the records of that time, its retention past them.
 */
export interface RereadGap {
  readonly signal: Signal;
  readonly from: string;
  readonly to: string;
}

/** The gaps the intake found taking up rereads (readAgainFrom) at `since`, an ISO 8601 time, or later, in the order found. */
export async function rereadGaps(db: DuckDBConnection, since: string): Promise<RereadGap[]> {
  return (await rows<{ Signal: Signal; from: bigint; to: bigint }>(
    db,
    `select Signal, epoch_ms(FromTime::TIMESTAMP) as "from", epoch_ms(ToTime::TIMESTAMP) as "to" from ${LAKE}.otel.${REREAD_GAPS}
      where FoundAt >= epoch_ms($1::BIGINT)::TIMESTAMP_NS order by FoundAt, Signal`,
    [Date.parse(since)],
  )).map(({ Signal, from, to }) => ({ signal: Signal, from: new Date(Number(from)).toISOString(), to: new Date(Number(to)).toISOString() }));
}

/**
 * Takes up `asked` (RereadAsked): sets each partition's offset back to `from`'s, where the
 * log has the records of their time, where the lake holds it past that, records `gaps`, what
 * it cannot refill (rereadGaps), and clears what was asked, in one transaction. Returns the
 * offsets set back.
 */
export function readAgainFrom(db: DuckDBConnection, asked: readonly Reread[], from: readonly Position[], gaps: readonly RereadGap[] = []): Promise<Position[]> {
  return transaction(db, async () => {
    for (const { signal, from: start, to } of gaps) {
      await db.run(`insert into ${LAKE}.otel.${REREAD_GAPS} values ($1, epoch_ms($2::BIGINT)::TIMESTAMP_NS, epoch_ms($3::BIGINT)::TIMESTAMP_NS, epoch_ms($4::BIGINT)::TIMESTAMP_NS)`, [
        signal,
        Date.parse(start),
        Date.parse(to),
        Date.now(),
      ]);
    }
    const held = new Map((await heldOffsets(db)).map((offset) => [partitionKey(offset), offset]));
    const back: Position[] = [];
    for (const position of from) {
      const kept = held.get(partitionKey(position));
      if (!kept || kept.offset <= position.offset) continue;
      await db.run(`delete from ${LAKE}.otel.${OFFSETS} where Topic = $1 and PartitionId = $2`, [kept.topic, kept.partition]);
      await db.run(`insert into ${LAKE}.otel.${OFFSETS} values ($1, $2, $3, $4)`, [kept.topic, kept.topicId, kept.partition, position.offset]);
      back.push(position);
    }
    // What was asked since from a time as late or later is taken up with it.
    for (const { signal, since } of asked) await db.run(`delete from ${LAKE}.otel.${REREADS} where Signal = $1 and Since >= $2::TIMESTAMP_NS`, [signal, since]);
    return back;
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

/** The latest time a TIMESTAMP_NS holds, 2^63 - 1 nanoseconds since the epoch, in 2262; and the earliest. */
const LATEST_NANOS = 2n ** 63n - 1n;
const EARLIEST_NANOS = -(2n ** 63n);

/** `nanos`, nanoseconds since the epoch, as a time; MalformedRequest for one no TIMESTAMP_NS holds, as a fixed64 may be. */
function at(nanos: bigint): DuckDBValue {
  if (nanos > LATEST_NANOS || nanos < EARLIEST_NANOS) throw new MalformedRequest(`${nanos} ns since the epoch is no time the lake keeps`);
  return timestampNanosValue(nanos);
}

/** A time, in nanoseconds since the epoch. */
const time = (nanos: Integer | undefined): DuckDBValue => at(integer(nanos));

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
          Timestamp: at(start),
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

/** The attribute of a log record's own id, OpenTelemetry's, which logRecordId gives a record without one. */
export const LOG_RECORD_ID = "log.record.uid";

/**
 * The id of `record`, of `resource` and `scope`, where its producer gave it none: what it
 * is, hashed, its times, ids, severity, body and attributes, and its resource's and scope's,
 * so that a record sent twice, as an SDK sends an export again whose answer it did not have,
 * is known for one (insertNew), where an id given as each try was taken would tell the two
 * apart.
 */
function logRecordId(resource: ResourcePart, scope: ScopePart, record: LogRecord): string {
  const hash = createHash("sha256");
  const add = (...values: readonly string[]) => {
    for (const value of values) hash.update(value).update("\u0000");
  };
  const pairs = (given: readonly KeyValue[] | undefined) => {
    add(String(given?.length ?? 0));
    for (const { key, value } of given ?? []) add(key ?? "", text(value));
  };
  pairs(resource.resource?.attributes);
  add(resource.schemaUrl ?? "", scope.scope?.name ?? "", scope.scope?.version ?? "");
  pairs(scope.scope?.attributes);
  add(scope.schemaUrl ?? "", String(integer(record.timeUnixNano)), String(integer(record.observedTimeUnixNano)), hexId(record.traceId), hexId(record.spanId));
  add(String(record.flags ?? 0), String(record.severityNumber ?? 0), record.severityText ?? "", text(record.body), record.eventName ?? "");
  pairs(record.attributes);
  return hash.digest("hex").slice(0, 32);
}

function logRows(request: LogsRequest): TelemetryRows {
  const logs: TelemetryRow[] = [];
  for (const part of list(request.resourceLogs, "resourceLogs")) {
    const resource = { ServiceName: serviceName(part.resource), ResourceSchemaUrl: part.schemaUrl ?? "", ResourceAttributes: attributes(part.resource?.attributes) };
    for (const scopePart of list(part.scopeLogs, "scopeLogs")) {
      const { scope, schemaUrl, logRecords } = scopePart;
      const origin = { ScopeSchemaUrl: schemaUrl ?? "", ScopeName: scope?.name ?? "", ScopeVersion: scope?.version ?? "", ScopeAttributes: attributes(scope?.attributes) };
      for (const record of list(logRecords, "logRecords")) {
        const own = list(record.attributes, "attributes");
        const id = own.some(({ key }) => key === LOG_RECORD_ID) ? [] : [{ key: LOG_RECORD_ID, value: { stringValue: logRecordId(part, scopePart, record) } }];
        const when = integer(record.timeUnixNano);
        logs.push({
          // Where a record has no time of its own, the time it was seen.
          Timestamp: at(when === 0n ? integer(record.observedTimeUnixNano) : when),
          TraceId: hexId(record.traceId),
          SpanId: hexId(record.spanId),
          TraceFlags: (record.flags ?? 0) & 0xff,
          SeverityText: record.severityText ?? "",
          SeverityNumber: Math.min(Math.max(record.severityNumber ?? 0, 0), 255),
          Body: text(record.body),
          ...resource,
          ...origin,
          LogAttributes: attributes([...own, ...id]),
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

/** The temporary table a batch's rows of a table are staged in, in DuckDB's own memory. */
const batchStaging = (table: OtelTable) => `otel_${table}`;


/**
 * What a row is, whatever the log it came through wrote of it: a span, by its trace's id
 * and its own; a log record, by its id, its producer's or logRecordId's; a metric's
 * point, by its series (its service, scope, name and attributes, and its resource's) and
 * its times. A row the lake has, or the batch has before it, as a producer's retry, or a
 * sender's, writes a record twice, is written once.
 */
const pointIdentity = (row: string) => ["ServiceName", "ScopeName", "MetricName", "Attributes", "ResourceAttributes", "StartTimeUnix", "TimeUnix"].map((column) => `${row}.${column}`);
const IDENTITY: Readonly<Record<OtelTable, (row: string) => readonly string[]>> = {
  traces: (row) => [`${row}.TraceId`, `${row}.SpanId`],
  logs: (row) => [`${row}.LogAttributes['${LOG_RECORD_ID}']`],
  metrics_gauge: pointIdentity,
  metrics_sum: pointIdentity,
  metrics_histogram: pointIdentity,
  metrics_exponential_histogram: pointIdentity,
  metrics_summary: pointIdentity,
};

/**
 * Inserts the rows staged for `table` that are not there already, nor before them in the
 * batch, by what each is (IDENTITY), among the table's rows of the batch's time: the
 * number inserted.
 */
async function insertNew(db: DuckDBConnection, table: OtelTable): Promise<number> {
  const { time } = TABLES[table];
  const [range] = await rows<{ low: string | null; high: string | null }>(db, `select min(${time})::varchar as low, max(${time})::varchar as high from temp.${batchStaging(table)}`);
  const ours = IDENTITY[table]("s");
  const theirs = IDENTITY[table]("t");
  const held = range?.low && range.high
    ? `not exists (select 1 from ${LAKE}.otel.${table} t where t.${time} between '${range.low}'::TIMESTAMP_NS and '${range.high}'::TIMESTAMP_NS and ` +
      `${ours.map((column, index) => `${theirs[index]} is not distinct from ${column}`).join(" and ")})`
    : "true";
  const inserted = await db.run(
    `insert into ${LAKE}.otel.${table} select * from temp.${batchStaging(table)} s where ${held} ` +
      `qualify row_number() over (partition by ${ours.join(", ")}) = 1`,
  );
  return Number(inserted.rowsChanged);
}

/** The rows a staging table is given at once: a vector's, DuckDB's own size of a data chunk. */
const CHUNK_ROWS = 2048;

/** The temporary table a record's rows of a table are staged in, before they join the batch's. */
const recordStaging = (table: OtelTable) => `otel_record_${table}`;

/**
 * Rows that DuckDB would not take, as they were staged or written: their values, or their
 * conversion, refused. Those of the records written, not the lake's to fail.
 */
export class UnwritableRows extends Error {
  override readonly name = "UnwritableRows";
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
  }
}

/** DuckDB's refusals of values, as it writes them: their conversion, a constraint, or their range. */
const VALUE_REFUSED = /^(Conversion|Constraint|Invalid Input|Out of Range|Mismatch Type) Error: /u;

/** An error of DuckDB's own, its kind first, as "Out of Memory Error: ...". */
export const DUCKDB_ERROR = /^[A-Z][A-Za-z]*( [A-Za-z]+)* Error: /u;

/**
 * A file the lake's catalog names that the object store did not give back, as a write read
 * the rows it holds to write none twice (insertNew): where that goes on, the store lost it,
 * as a restore of its filer store loses what was written since its backup, and every write
 * of the rows of its time fails on it until the catalog is set right with the store.
 */
export class LakeFileUnreadable extends Error {
  override readonly name = "LakeFileUnreadable";
  readonly file: string;
  constructor(file: string, table: OtelTable, cause: Error) {
    super(
      `the object store did not give back ${file}, a file of otel.${table} the lake's catalog names (${cause.message}): ` +
        "where this goes on, the store lost it, and run alasio lake reconcile to set the lake right",
      { cause },
    );
    this.file = file;
  }
}

/** DuckDB failing to read a Parquet file, as it names it: the store's answer to a GET of it, or its file not opened. */
const FILE_UNREAD = /^(?:HTTP|IO) Error: .*?["']([^"']+\.parquet)["']/u;

/** `error`, raised reading `table`'s rows, as LakeFileUnreadable where it is DuckDB's failing to read a file of them; as it is otherwise. */
function unreadable(error: unknown, table: OtelTable): unknown {
  if (!(error instanceof Error)) return error;
  const file = FILE_UNREAD.exec(error.message)?.[1];
  if (file === undefined) return error;
  let named = file;
  try {
    named = decodeURIComponent(file);
  } catch {
    // Named as DuckDB gave it.
  }
  return new LakeFileUnreadable(named, table, error);
}

/**
 * `error`, raised staging or writing rows, as the rows' (UnwritableRows) where it is theirs:
 * DuckDB's refusal of their values, or the Node API's of them before DuckDB has them; as it
 * is otherwise, DuckDB's own failing, as of its memory or its disk, being the lake's.
 */
const ofRows = (error: unknown): unknown =>
  error instanceof Error && (VALUE_REFUSED.test(error.message) || !DUCKDB_ERROR.test(error.message)) ? new UnwritableRows(error) : error;

/**
 * A batch's staging, a record at a time (writeConsumed): each record's rows, as its parts
 * come (./otlp.ts requestParts), staged a data chunk of CHUNK_ROWS at a time, written into
 * DuckDB's own vectors, in a table of the record's own, which joins the batch's once the
 * record is whole, or is emptied where the record turns out not to decode, so that none of
 * its rows is written. What the intake holds of a batch is then a part's rows and a chunk's
 * of each table, however large its records: the batch itself is in DuckDB's memory, which
 * spills past its limit. A value made a DuckDB value of its own, as the appender's
 * appendValue makes it, is freed only once JavaScript's collector finalizes it, which a
 * batch's thousands of points, each with its maps' entries, outran: 40,000 of a scrape's
 * points took 1.8 GB and 13 s so, 0.5 GB and 0.8 s a chunk at a time (measured).
 */
function staging(db: DuckDBConnection) {
  const tables = new Map<OtelTable, { readonly columns: readonly string[]; readonly types: readonly DuckDBType[]; rows: number }>();
  const opened = async (table: OtelTable) => {
    const known = tables.get(table);
    if (known) return known;
    await db.run(`create or replace temp table ${batchStaging(table)} (${TABLES[table].columns})`);
    await db.run(`create or replace temp table ${recordStaging(table)} (${TABLES[table].columns})`);
    const empty = await db.runAndReadAll(`from temp.${recordStaging(table)} limit 0`);
    const made = { columns: empty.columnNames(), types: empty.columnTypes(), rows: 0 };
    tables.set(table, made);
    return made;
  };
  return {
    /** The batch's tables that rows were staged for, with how many. */
    tables: () => [...tables].filter(([, { rows }]) => rows > 0).map(([table, { rows }]) => ({ table, rows })),
    /** Stages `parts`' rows, a record's, as a whole: false where they do not decode, and none of them is staged. */
    async record(parts: Iterable<TelemetryRows>): Promise<boolean> {
      const pending = new Map<OtelTable, TelemetryRow[]>();
      const appenders = new Map<OtelTable, DuckDBAppender>();
      const append = async (table: OtelTable, chunkRows: readonly TelemetryRow[]) => {
        const { columns, types } = await opened(table);
        const appender = appenders.get(table) ?? await db.createAppender(recordStaging(table), "main", "temp");
        appenders.set(table, appender);
        const chunk = DuckDBDataChunk.create(types, chunkRows.length);
        chunk.setRows(chunkRows.map((row) => columns.map((column) => row[column] ?? null)));
        appender.appendDataChunk(chunk);
      };
      let whole = true;
      try {
        for (const part of parts) {
          for (const [table, tableRows] of Object.entries(part) as [OtelTable, TelemetryRow[]][]) {
            const waiting = [...(pending.get(table) ?? []), ...tableRows];
            for (; waiting.length >= CHUNK_ROWS; waiting.splice(0, CHUNK_ROWS)) await append(table, waiting.slice(0, CHUNK_ROWS));
            pending.set(table, waiting);
          }
        }
        for (const [table, waiting] of pending) if (waiting.length > 0) await append(table, waiting);
      } catch (error) {
        if (!(error instanceof MalformedRequest)) throw ofRows(error);
        whole = false;
      } finally {
        for (const appender of appenders.values()) appender.closeSync();
      }
      for (const table of appenders.keys()) {
        if (whole) {
          const moved = await db.run(`insert into temp.${batchStaging(table)} from temp.${recordStaging(table)}`);
          tables.get(table)!.rows += Number(moved.rowsChanged);
        }
        await db.run(`delete from temp.${recordStaging(table)}`);
      }
      return whole;
    },
  };
}

/** A record of the log's, at its offset in its topic's partition. */
export interface Position {
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
}

/**
 * A record the intake consumed: where it is in the log, of its topic by the topic's id, and
 * its rows, a part's at a time, as they are taken, which throw MalformedRequest for a record
 * that does not decode, which is skipped.
 */
export interface Consumed extends Position {
  readonly topicId: string;
  readonly rows: Iterable<TelemetryRows>;
}

/**
 * What writeConsumed did: the rows each table was given, and those it was not as it has
 * them (insertNew), the records skipped as written before or undecodable, and where each
 * partition is written to now.
 */
export interface ConsumedWrite {
  readonly written: Partial<Record<OtelTable, number>>;
  readonly duplicates: Partial<Record<OtelTable, number>>;
  readonly repeated: number;
  /** The records skipped as they do not decode. */
  readonly malformed: readonly Position[];
  /** For each partition of the batch, the offset of the next record to write. */
  readonly next: readonly Position[];
}

/** Where the lake is written to of a partition of a topic of the log's, of the topic by its id: the offset of the next record. */
export interface Held extends Position {
  readonly topicId: string;
}

/** Where the lake is written to of each partition it has written of. */
export async function heldOffsets(db: DuckDBConnection): Promise<Held[]> {
  return (await rows<{ Topic: string; TopicId: string; PartitionId: number; NextOffset: bigint }>(db, `from ${LAKE}.otel.${OFFSETS}`))
    .map((row) => ({ topic: row.Topic, topicId: row.TopicId, partition: row.PartitionId, offset: BigInt(row.NextOffset) }));
}

const partitionKey = ({ topic, partition }: Pick<Position, "topic" | "partition">) => `${topic}\u0000${partition}`;

/** The offset of the first record a partition of the log's still holds, as the log says. */
export type LogStart = (topic: string, partition: number) => Promise<bigint>;

/**
 * A batch read past the offsets the lake holds (writeConsumed): its member was not told of
 * the lake's being set back beneath it, and joins again to read from `held`, the lake's.
 */
export class LakeSetBack extends Error {
  override readonly name = "LakeSetBack";
  readonly held: readonly Position[];
  constructor(held: readonly Position[]) {
    super(`the lake holds ${held.map(({ topic, partition, offset }) => `${topic}/${partition} to ${offset}`).join(", ")}, before what the batch was read from`);
    this.held = held;
  }
}

/**
 * Writes `consumed`, a batch the intake read from the log, in one transaction with the
 * offset each of its partitions is written to: of each partition, the records at or past
 * the offset the lake holds alone, so a batch written before, read again after a crash or
 * a rebalance, is written once; every record of a topic other than the one the lake holds
 * an offset of, by its id, as one made anew is. Of those, each row the lake has already is
 * not written again (insertNew): what the log holds twice, at two offsets, is in the lake
 * once. Each record's rows are staged in DuckDB's memory first, a record at a time (staging).
 *
 * A partition's records that begin past the offset the lake holds, and past the first record
 * the log still holds (`logStart`), were read by a member the lake was set back beneath, as
 * another replica's taking up a reread sets back every partition's, or a restore of the
 * catalog does: the write is refused (LakeSetBack), so that the member joins again and reads
 * from the lake's offsets, rather than set them past what the lake no longer has.
 */
export function writeConsumed(db: DuckDBConnection, consumed: readonly Consumed[], logStart: LogStart): Promise<ConsumedWrite> {
  return transaction(db, async () => {
    // Asked to read records again, the intake sets its offsets back first (readAgainFrom), or
    // this write would set them past what it is to read again.
    const asked = await rereadsAsked(db);
    if (asked.length > 0) throw new RereadAsked(asked);
    const held = new Map((await heldOffsets(db)).map((offset) => [partitionKey(offset), { topicId: offset.topicId, next: offset.offset }]));
    const heldOf = (record: Consumed) => {
      const offset = held.get(partitionKey(record));
      return offset?.topicId === record.topicId ? offset.next : 0n;
    };
    const firsts = new Map<string, Consumed>();
    for (const record of consumed) {
      const first = firsts.get(partitionKey(record));
      if (!first || record.offset < first.offset) firsts.set(partitionKey(record), record);
    }
    const beneath: Position[] = [];
    for (const first of firsts.values()) {
      const kept = heldOf(first);
      // The log is asked its start of a partition read past the lake's offset alone, the one the
      // fence needs it of: a batch that follows on from the lake's, as nearly every batch does,
      // asks it nothing within the write lock.
      if (first.offset <= kept) continue;
      if (first.offset > await logStart(first.topic, first.partition)) beneath.push({ topic: first.topic, partition: first.partition, offset: kept });
    }
    if (beneath.length > 0) throw new LakeSetBack(beneath);
    const fresh = consumed.filter((record) => record.offset >= heldOf(record));
    const staged = staging(db);
    const malformed: Position[] = [];
    for (const record of fresh) if (!(await staged.record(record.rows))) malformed.push(record);
    const written: Partial<Record<OtelTable, number>> = {};
    const duplicates: Partial<Record<OtelTable, number>> = {};
    for (const { table, rows: stagedRows } of staged.tables()) {
      const inserted = await insertNew(db, table).catch((error: unknown) =>
        Promise.reject(error instanceof Error && VALUE_REFUSED.test(error.message) ? new UnwritableRows(error) : unreadable(error, table))
      );
      written[table] = inserted;
      if (inserted < stagedRows) duplicates[table] = stagedRows - inserted;
    }
    const next = new Map<string, Position & { readonly topicId: string }>();
    for (const record of fresh) {
      const after = record.offset + 1n;
      if ((next.get(partitionKey(record))?.offset ?? -1n) < after) next.set(partitionKey(record), { topic: record.topic, topicId: record.topicId, partition: record.partition, offset: after });
    }
    for (const { topic, topicId, partition, offset } of next.values()) {
      await db.run(`delete from ${LAKE}.otel.${OFFSETS} where Topic = $1 and PartitionId = $2`, [topic, partition]);
      await db.run(`insert into ${LAKE}.otel.${OFFSETS} values ($1, $2, $3, $4)`, [topic, topicId, partition, offset]);
    }
    const positions = new Map<string, Position>();
    for (const record of consumed) {
      const known = heldOf(record);
      const reached = next.get(partitionKey(record))?.offset ?? known;
      positions.set(partitionKey(record), { topic: record.topic, partition: record.partition, offset: reached > known ? reached : known });
    }
    return {
      written,
      duplicates,
      repeated: consumed.length - fresh.length,
      malformed,
      next: [...positions.values()],
    };
  });
}

/**
 * Merges each `otel` table's small files, a day's into files of up to TELEMETRY_FILE_SIZE:
 * the collector's minute-long batches of metric points make a file a kind a minute, some
 * ten thousand a day, which a query of the day reads one by one. Measured on the stack, a 24-hour query of a gauge
 * over 2 500 such files took 8.8 s cold, 1.1 s once they were merged, in 8 s. The files
 * merged are only scheduled for deletion, which maintenance's cleanup does (./sync.ts),
 * so a branch that still names them keeps them.
 */
export function mergeTelemetry(db: DuckDBConnection): Promise<unknown> {
  return transaction(db, async () => {
    for (const table of OTEL_TABLES) await db.run(`call ducklake_merge_adjacent_files('${LAKE}', '${table}', schema => 'otel')`);
  });
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
