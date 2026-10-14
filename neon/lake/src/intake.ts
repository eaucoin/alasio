/**
 * The telemetry intake: a member of the log's consumer group `lake-intake`, consuming the
 * telemetry topics the stack's collector writes (cli/src/manifests/collector.ts), each
 * record an OTLP export request in binary protobuf, a topic a signal (TOPICS). It reads a
 * batch at a time, as much as BATCH_MS's worth or BATCH_RECORDS records, decodes each
 * record (./otlp.ts) into the `otel` schema's rows (./otel.ts), and writes the batch to
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

import type { Log } from "./loader.ts";
import type { Metrics } from "./metrics.ts";
import { type Consumed, type Position, telemetryRows, writeConsumed } from "./otel.ts";
import { decodeRequest, MalformedRequest, type Signal } from "./otlp.ts";

/** The log's telemetry topics, and the signal each carries. */
export const TOPICS: Readonly<Record<string, Signal>> = { "otel.spans": "traces", "otel.logs": "logs", "otel.metrics": "metrics" };

/** A record as the intake reads it from the log. */
export interface LogRecord extends Position {
  readonly value: Buffer;
}

/** The intake's member of the consumer group: batches of records, and the offsets told the group once they are written. */
export interface IntakeSource {
  /**
   * The next records, as many as `maxRecords`, those that came within `maxMs` of the first
   * (or of the call, for none): a partition's in order, from where the group was last told.
   * Those the lake has already, past that, writeConsumed skips.
   */
  next(maxMs: number, maxRecords: number): Promise<LogRecord[]>;
  /** Tells the group that each partition is written up to its offset, the next record's. */
  commit(offsets: readonly Position[]): Promise<void>;
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
  /** How long a batch gathers records, and how many it takes at most. */
  batchMs?: number;
  batchRecords?: number;
  /** How soon an open that failed is tried again. */
  retryMs?: number;
}

export interface Intake {
  /** Whether it holds the lake and its member of the group. */
  ready(): boolean;
  /** Ends the loop, once a batch under way has been written, and leaves the group. */
  stop(): Promise<void>;
}

/**
 * How long a batch gathers records: a minute's telemetry, the collector scraping each
 * minute, written as one commit, a file a table at most, some 1,440 a day each, which
 * the lake's hourly merge tidies (mergeTelemetry); within the minutes Grafana's alerts
 * want the stack's telemetry in.
 */
export const BATCH_MS = 60_000;
/** The most records a batch takes: a burst, or a backlog after an outage, in commits of a bounded size. */
export const BATCH_RECORDS = 2_000;
const RETRY_MS = 10_000;
/** How often a member asks the log how far behind the group is. */
const LAG_EVERY_MS = 60_000;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** `record`'s rows, or null where it is not an OTLP export request of its topic's signal. */
function decode(record: LogRecord): Consumed {
  const signal = TOPICS[record.topic];
  try {
    if (!signal) throw new MalformedRequest(`no signal is carried on ${record.topic}`);
    return { ...record, rows: telemetryRows(signal, decodeRequest(signal, record.value)) };
  } catch (error) {
    if (!(error instanceof MalformedRequest)) throw error;
    return { ...record, rows: null };
  }
}

/** Starts the intake: it opens the lake and joins the group now, and consumes until stopped. */
export function startIntake({ open, join, metrics, log, batchMs = BATCH_MS, batchRecords = BATCH_RECORDS, retryMs = RETRY_MS }: IntakeOptions): Intake {
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

  /** Reads, decodes and writes one batch; true where it was a batch of any. */
  async function batch(held: IntakeLake, member: IntakeSource): Promise<boolean> {
    const records = await member.next(batchMs, batchRecords);
    if (records.length === 0) return false;
    const consumed = records.map(decode);
    const started = performance.now();
    const { written, repeated, malformed, next } = await writeConsumed(held.db, consumed);
    metrics.set("lake_telemetry_commit_seconds", {}, (performance.now() - started) / 1000);
    for (const [table, count] of Object.entries(written)) metrics.add("lake_telemetry_rows_total", { table: `otel.${table}` }, count);
    for (const record of consumed) {
      const outcome = record.rows === null ? "malformed" : "written";
      metrics.add("lake_telemetry_records_total", { topic: record.topic, outcome });
    }
    if (repeated > 0) metrics.add("lake_telemetry_records_total", { topic: "", outcome: "repeated" }, repeated);
    if (malformed > 0) log("telemetry intake skipped records that do not decode", { malformed });
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

  const done = (async () => {
    while (!stopped.signal.aborted) {
      if (!lake || !source) {
        try {
          lake ??= await open();
          source ??= await join();
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
        await measureLag(source);
      } catch (error) {
        metrics.add("lake_telemetry_batches_total", { outcome: "failed" });
        await drop(error);
        if (!stopped.signal.aborted) await sleep(retryMs);
      }
    }
    await drop();
  })();

  return {
    ready: () => lake !== null && source !== null,
    async stop() {
      stopped.abort();
      wake?.();
      // A batch being gathered ends with its member, unwritten, to be read again.
      await source?.close().catch(() => {});
      await done;
    },
  };
}
