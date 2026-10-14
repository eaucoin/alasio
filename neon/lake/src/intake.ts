/**
 * The telemetry intake: a member of the log's consumer group `lake-intake`, consuming the
 * telemetry topics the stack's collector writes (cli/src/manifests/collector.ts), each
 * record an OTLP export request in binary protobuf, a topic a signal (TOPICS). It reads a
 * batch at a time, as much as BATCH's bounds take, decodes each
 * record (./otlp.ts), a part at a time as it writes it, into the `otel` schema's rows
 * (./otel.ts), and writes the batch to
 * the lake in one transaction with the offsets its partitions are written to
 * (writeConsumed), straight to Parquet: a batch is a large commit, and nothing is inlined
 * in the catalog. Only then does it commit the offsets to the group, which KEDA scales the
 * intake's replicas on the lag of; a crash between the two re-reads a batch the lake has,
 * which writeConsumed skips, so nothing is lost or written twice. A record that does not
 * decode is skipped, and counted.
 *
 * It holds the lake and its member of the group from its start, opening them until that
 * succeeds and anew whenever a write fails, so it rides out the compute or the log
 * restarting: a batch not written is read again from what the group was last told.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { WriteLockLost } from "./lake.ts";
import type { Log } from "./loader.ts";
import type { Metrics } from "./metrics.ts";
import { type Consumed, type ConsumedWrite, type Held, heldOffsets, type OtelTable, type Position, type TelemetryRows, telemetryRows, UnwritableRows, writeConsumed, DUCKDB_ERROR } from "./otel.ts";
import { MalformedRequest, REQUEST_ROW_BYTES, requestParts, requestRowBytes, type Signal } from "./otlp.ts";

/** The log's telemetry topics, and the signal each carries. */
export const TOPICS: Readonly<Record<string, Signal>> = { "otel.spans": "traces", "otel.logs": "logs", "otel.metrics": "metrics" };

/** A record as the intake reads it from the log: where it is, its topic's id, and its value. */
export interface LogRecord extends Position {
  readonly topicId: string;
  readonly value: Buffer;
}

/**
 * What a batch takes at most: the records that come within `ms` of its start, as many as
 * `records`, while their values come to `bytes` at most (the record that would take it
 * past them left for the next, but for a record larger alone, which makes a batch of its own);
 * and what each commit of it stages, its records' rows coming to `rowBytes` at most
 * (requestRowBytes), a batch whose rows come to more written in parts, each a commit of its
 * own, of a record at least: a record's rows may come to many times its value's bytes.
 */
export interface BatchBounds {
  readonly ms: number;
  readonly records: number;
  readonly bytes: number;
  readonly rowBytes: number;
}

/** The intake's member of the consumer group: batches of records, and the offsets told the group once they are written. */
export interface IntakeSource {
  /**
   * The next records, as many as `bounds` take: a partition's in order, from where the group
   * was last told. Those the lake has already, past that, writeConsumed skips.
   */
  next(bounds: BatchBounds): Promise<LogRecord[]>;
  /** Tells the group that each partition is written up to its offset, the next record's. */
  commit(offsets: readonly Position[]): Promise<void>;
  /**
   * Tells the group of each partition of its topics where `held`, the lake's offsets, is
   * behind what the group was told, or the log's start where the lake holds none of it: true
   * where it told it any, when a member must join again to read from there.
   */
  rewind(held: readonly Held[]): Promise<boolean>;
  /** How many records the group has yet to read of each topic. */
  lag(): Promise<ReadonlyMap<string, number>>;
  close(): Promise<void>;
}

/** The lake as the intake holds it, the `otel` schema ready. */
export interface IntakeLake {
  db: DuckDBConnection;
  close(): void;
}

export interface IntakeOptions {
  open: () => Promise<IntakeLake>;
  /** Joins the group. */
  join: () => Promise<IntakeSource>;
  metrics: Pick<Metrics, "add" | "set">;
  log: Log;
  /** What a batch takes at most. */
  batch?: BatchBounds;
  /** How soon an open that failed is tried again. */
  retryMs?: number;
  /** How long its loop may go without coming round before it is not well (STALL_MS). */
  stallMs?: number;
  /** How long its batches may fail one after another before it is not well (FAILING_MS). */
  failingMs?: number;
}

export interface Intake {
  /** Whether it holds the lake and its member of the group. */
  ready(): boolean;
  /** Whether its loop came round within the last stallMs, and its batches have not failed one after another for failingMs: not wedged, as its liveness says. */
  well(): boolean;
  /** Ends the loop, once a batch under way has been written, and leaves the group. */
  stop(): Promise<void>;
}

/**
 * What a batch takes: a minute's telemetry, the collector scraping each minute, written as
 * one commit, a file a table at most, some 1,440 a day each, which the lake's hourly merge
 * tidies (mergeTelemetry), within the minutes Grafana's alerts want the stack's telemetry
 * in; and of a burst, or a backlog after an outage, commits of a bounded size, in records
 * and in bytes: a scrape's points, decoded and staged (./otel.ts), with what the intake's
 * DuckDB keeps of its 256 MB, held it at some 850 MiB batch after batch of 4 MiB of them,
 * and 1.2 GiB of 8 MiB, against its memory limit of 1 GiB (measured).
 */
/** The intake's DuckDB's memory limit: a batch's rows, staged, are all it holds, and what it holds past this it spills. */
export const DUCKDB_MEMORY = "256MB";

/** A part's rows at most as one record's may be (REQUEST_ROW_BYTES), which the intake's DuckDB is proven to stage within its memory and its spill. */
export const BATCH: BatchBounds = { ms: 60_000, records: 2_000, bytes: 4 * 1024 * 1024, rowBytes: REQUEST_ROW_BYTES };
const RETRY_MS = 10_000;
/**
 * How long the intake's loop may go without coming round before it is held wedged, and
 * restarted: past a batch's minute, and a write's wait for the lake's write lock, which
 * gives up within WRITE_LOCK_WAIT_MS (./lake.ts), and the write itself.
 */
const STALL_MS = 15 * 60_000;
/**
 * How long the intake may fail to write the batch it holds, read again each time, before it
 * is not well, and restarted, which says it: past the lake's and the log's blips, which a
 * write rides out by reading the batch again.
 */
const FAILING_MS = 10 * 60_000;
/** How often a member asks the log how far behind the group is. */
const LAG_EVERY_MS = 60_000;

const positionKey = ({ topic, partition, offset }: Position) => `${topic}\u0000${partition}\u0000${offset}`;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Whether `error`, writing records, is the lake's to fail rather than theirs: its write lock
 * not held, or DuckDB failing the write but for the rows' own values (UnwritableRows), as
 * where the catalog's database or the object store does not answer, or a commit conflicts.
 */
const ofTheLake = (error: unknown): boolean =>
  error instanceof WriteLockLost || (error instanceof Error && !(error instanceof UnwritableRows) && DUCKDB_ERROR.test(error.message));

/** A record as the intake writes it: its rows taken as they are written. */
const consumed = (record: LogRecord): Consumed => ({ ...record, rows: rowsOf(record) });

/** What `record`'s rows come to, in bytes, as REQUEST_ROW_BYTES counts them; 0 for a record of no telemetry topic's. */
const rowBytesOf = ({ topic, value }: LogRecord): number => {
  const signal = TOPICS[topic];
  return signal ? requestRowBytes(signal, value) : 0;
};

/** `records` in parts, in order, each of a record at least, and of rows coming to `rowBytes` at most otherwise. */
export function partsWithin(records: readonly LogRecord[], rowBytes: number): LogRecord[][] {
  const parts: LogRecord[][] = [];
  let size = 0;
  for (const record of records) {
    const bytes = rowBytesOf(record);
    const last = parts.at(-1);
    if (last && size + bytes <= rowBytes) {
      last.push(record);
      size += bytes;
    } else {
      parts.push([record]);
      size = bytes;
    }
  }
  return parts;
}

/** `record`'s rows, a part's at a time, which throw MalformedRequest where it is not an OTLP export request of its topic's signal. */
function* rowsOf({ topic, value }: LogRecord): Generator<TelemetryRows> {
  const signal = TOPICS[topic];
  if (!signal) throw new MalformedRequest(`no signal is carried on ${topic}`);
  for (const part of requestParts(signal, value)) yield telemetryRows(signal, part);
}

/** Starts the intake: it opens the lake and joins the group now, and consumes until stopped. */
export function startIntake({ open, join, metrics, log, batch: bounds = BATCH, retryMs = RETRY_MS, stallMs = STALL_MS, failingMs = FAILING_MS }: IntakeOptions): Intake {
  const stopped = new AbortController();
  let lake: IntakeLake | null = null;
  let source: IntakeSource | null = null;
  let wake: (() => void) | null = null;

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, ms);
      wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });

  async function drop(error?: unknown) {
    if (error !== undefined) log("telemetry intake lost the lake or the log", { error: errorText(error) });
    const [held, member] = [lake, source];
    lake = null;
    source = null;
    held?.close();
    await member?.close().catch(() => {});
  }

  /**
   * Writes `records`, a batch, in parts within its rowBytes (partsWithin), each whole; or,
   * where a part of more than a record fails but for the lake's write lock, each of its
   * records alone, so that one whose rows fail to write, or a part too large for the lake,
   * stops no other. Where the lake fails a record alone (ofTheLake), or its write lock any
   * part, nothing is skipped: that is thrown, and the batch is read again once the lake is
   * held anew.
   */
  async function write(db: DuckDBConnection, records: readonly LogRecord[]): Promise<ConsumedWrite & { readonly unwritable: readonly LogRecord[] }> {
    const each: ConsumedWrite[] = [];
    const unwritable: LogRecord[] = [];
    for (const part of partsWithin(records, bounds.rowBytes)) {
      try {
        each.push(await writeConsumed(db, part.map(consumed)));
        continue;
      } catch (error) {
        if (part.length === 1 ? ofTheLake(error) : error instanceof WriteLockLost) throw error;
        log("telemetry intake could not write a batch, and writes its records each alone", { records: part.length, error: errorText(error) });
      }
      for (const record of part) {
        const { written, skipped } = await alone(db, record);
        each.push(written);
        if (skipped) unwritable.push(record);
      }
    }
    const sum = (counts: readonly Partial<Record<string, number>>[]) => {
      const summed: Partial<Record<OtelTable, number>> = {};
      for (const [table, count] of counts.flatMap((counted) => Object.entries(counted)) as [OtelTable, number][]) summed[table] = (summed[table] ?? 0) + count;
      return summed;
    };
    const next = new Map<string, Position>();
    for (const position of each.flatMap((written) => written.next)) {
      const key = `${position.topic}\u0000${position.partition}`;
      if ((next.get(key)?.offset ?? -1n) < position.offset) next.set(key, position);
    }
    return {
      written: sum(each.map(({ written }) => written)),
      repeated: each.reduce((total, { repeated }) => total + repeated, 0),
      malformed: each.flatMap(({ malformed }) => malformed),
      next: [...next.values()],
      unwritable,
    };
  }

  /**
   * Writes `record` alone. Where its rows fail (UnwritableRows), or it fails again once the
   * lake has had a while (retryMs), it is skipped, its offset written without its rows, and
   * said; where the lake fails, that is thrown.
   */
  async function alone(db: DuckDBConnection, record: LogRecord): Promise<{ readonly written: ConsumedWrite; readonly skipped: boolean }> {
    for (let attempt = 1; ; attempt += 1) {
      try {
        return { written: await writeConsumed(db, [consumed(record)]), skipped: false };
      } catch (error) {
        if (ofTheLake(error)) throw error;
        if (!(error instanceof UnwritableRows) && attempt === 1) {
          await sleep(retryMs);
          continue;
        }
        log("telemetry intake skipped a record whose rows it could not write", { topic: record.topic, partition: record.partition, offset: String(record.offset), error: errorText(error) });
        return { written: await writeConsumed(db, [{ ...record, rows: [] }]), skipped: true };
      }
    }
  }

  /** Reads, decodes and writes one batch; true where it was a batch of any. */
  async function batch(held: IntakeLake, member: IntakeSource): Promise<boolean> {
    const records = await member.next(bounds);
    if (records.length === 0) return false;
    const started = performance.now();
    const { written, repeated, malformed, next, unwritable } = await write(held.db, records);
    metrics.set("lake_telemetry_commit_seconds", {}, (performance.now() - started) / 1000);
    for (const [table, count] of Object.entries(written)) metrics.add("lake_telemetry_rows_total", { table: `otel.${table}` }, count);
    const outcomes = new Map([...malformed.map((record) => [positionKey(record), "malformed"] as const), ...unwritable.map((record) => [positionKey(record), "unwritable"] as const)]);
    for (const record of records) {
      const outcome = outcomes.get(positionKey(record)) ?? "written";
      metrics.add("lake_telemetry_records_total", { topic: record.topic, outcome });
    }
    if (repeated > 0) metrics.add("lake_telemetry_records_total", { topic: "", outcome: "repeated" }, repeated);
    if (malformed.length > 0) log("telemetry intake skipped records that do not decode", { malformed: malformed.length });
    await member.commit(next);
    return true;
  }

  let lagAt = 0;
  /** Says how far behind the group is, at most each LAG_EVERY_MS; a failure to ask is the log's, which the next batch meets. */
  async function measureLag(member: IntakeSource): Promise<void> {
    if (Date.now() - lagAt < LAG_EVERY_MS) return;
    lagAt = Date.now();
    const lag = await member.lag().catch(() => null);
    for (const [topic, records] of lag ?? []) metrics.set("lake_telemetry_lag_records", { topic }, records);
  }

  let cameRound = Date.now();
  /** Since when its batches have failed one after another; null while they are written. */
  let failingSince: number | null = null;
  const done = (async () => {
    for (; !stopped.signal.aborted; cameRound = Date.now()) {
      if (!lake || !source) {
        try {
          lake ??= await open();
          source ??= await join();
          // The lake put back to before what the group was told, as a restore of its catalog
          // puts it: the group is told the lake's, and the member joins again to read from
          // there, so the log refills what the lake lost.
          if (await source.rewind(await heldOffsets(lake.db))) {
            log("telemetry intake reads again what the lake does not hold");
            const rewound = source;
            source = null;
            await rewound.close();
            source = await join();
          }
          log("telemetry intake consuming");
        } catch (error) {
          log("telemetry intake could not open the lake or join the group", { error: errorText(error) });
          await drop();
          await sleep(retryMs);
          continue;
        }
      }
      try {
        await batch(lake, source);
        failingSince = null;
        await measureLag(source);
      } catch (error) {
        failingSince ??= Date.now();
        metrics.add("lake_telemetry_batches_total", { outcome: "failed" });
        await drop(error);
        if (!stopped.signal.aborted) await sleep(retryMs);
      }
    }
    await drop();
  })();

  return {
    ready: () => lake !== null && source !== null,
    well: () => Date.now() - cameRound < stallMs && (failingSince === null || Date.now() - failingSince < failingMs),
    async stop() {
      stopped.abort();
      wake?.();
      // A batch being gathered ends with its member, unwritten, to be read again.
      await source?.close().catch(() => {});
      await done;
    },
  };
}
