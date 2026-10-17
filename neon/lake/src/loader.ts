/**
 * The loader's loop: a load whenever alasio says it wrote transcripts. alasio writes a
 * record on its environment's wake topic (`lake.<environment>`) whenever it commits
 * transcript entries or rollout bytes, and KEDA wakes the loader while its group is behind
 * on it, which it reads as a member of that group. It gathers what records came within a
 * few seconds, loads, and only then tells the group it has read them: a load is everything
 * the source has that the lake does not, so it covers every record read before it began,
 * and a record never acted on is read again. Its source is a read replica, which replays
 * the primary's writes a little after them: a record names the primary's WAL position past
 * the writes it tells of, and a load waits, for REPLAY_WAIT_MS at most, until the source
 * has replayed the furthest its records name, or fails, its records told of nothing, so
 * they are read again. Its group caught up, KEDA puts it back to sleep after its cooldown.
 * Maintenance is not the loader's (./maintain.ts).
 *
 * The loop opens the lake and joins the group itself, and keeps both between loads. A load
 * that fails is logged and counted, the lake and the group are dropped, and both are tried
 * again soon on fresh connections: being transactional, a load left the lake as the last
 * did, and its records are read again. So the loader rides out the compute restarting, its
 * lock's connection being lost, the log restarting, or alasio not having granted it its
 * reads yet (src/neon/lake.ts).
 */
import type { DuckDBConnection } from "@duckdb/node-api";
import pg from "pg";

import type { LakeConfig } from "./config.ts";
import type { BatchBounds } from "./intake.ts";
import { CONNECT_TIMEOUT_MS, type Lake, openLake, takeLoaderLock } from "./lake.ts";
import type { Metrics } from "./metrics.ts";
import type { Position } from "./otel.ts";
import { type LakeLoad, type LakeSyncOptions, prepareLake, syncLake } from "./sync.ts";

/** The lake as the loader holds it: open for loading. */
export interface LoaderLake {
  db: DuckDBConnection;
  close(): Promise<unknown>;
  /** Whether its source has replayed the primary's writes up to the WAL position `lsn` (sourceReplayed). */
  replayed(lsn: bigint): Promise<boolean>;
  /** Runs `load` under the loader's lock, taken for it, waiting while another holds it, and let go after: `load` given its fence (../lake.ts's LoaderLock). */
  locked<T>(load: (fence: () => Promise<void>) => Promise<T>): Promise<T>;
}

/**
 * Opens the lake for loading, its model made ready under the loader's lock, and a connection
 * to its source, which says how far it has replayed. Each load then takes the lock for
 * itself, and lets it go after, so that a reconciliation waits for one load at most, never
 * for a loader merely awake; a lock taken waits while another holds it, until `signal`
 * aborts. `lake` is the lake opened, whose DuckDB's memory the loader's /metrics reports.
 */
export async function openLoaderLake(config: LakeConfig, log: Log, signal: AbortSignal, lockRetryMs?: number): Promise<LoaderLake & { lake: Lake }> {
  const underLock = async <T>(work: (fence: () => Promise<void>) => Promise<T>): Promise<T> => {
    const lock = await takeLoaderLock(config.catalog, log, { signal, retryMs: lockRetryMs });
    try {
      return await work(() => lock.fence());
    } finally {
      await lock.release();
    }
  };
  let lake: Lake | undefined;
  const source = new pg.Client({ ...config.catalog, ...config.source, connectionTimeoutMillis: CONNECT_TIMEOUT_MS });
  // A connection lost fails the next ask, and so the load, which opens all anew.
  source.on("error", () => {});
  try {
    const held = await openLake(config, { source: config.source });
    lake = held;
    const rebuilt = await underLock(() => prepareLake(held.db));
    await source.connect();
    log(rebuilt ? "lake built, to be loaded from the source" : "lake open");
  } catch (error) {
    lake?.close();
    await source.end().catch(() => {});
    throw error;
  }
  const opened = lake;
  return {
    lake: opened,
    db: opened.db,
    replayed: (lsn) => sourceReplayed(source, lsn),
    locked: underLock,
    async close() {
      opened.close();
      await source.end().catch(() => {});
    },
  };
}

/** A record of the wake topic, as the loader reads it: where it is, and what alasio wrote in it (src/neon/lake-wake.ts's wakeRecord). */
export interface WakeRecord extends Position {
  readonly value: Buffer;
}

/** The loader's member of its group on the wake topic: the records come since it last told the group, and its telling. */
export interface WakeSource {
  /** The records that came within `bounds` of the call, from where the group was last told. */
  next(bounds: Pick<BatchBounds, "ms" | "records" | "bytes">): Promise<readonly WakeRecord[]>;
  /** Tells the group that each partition is read up to its offset, the next record's. */
  commit(offsets: readonly Position[]): Promise<void>;
  close(): Promise<void>;
}

/** Logs an event, with its fields. */
export type Log = (message: string, fields?: Record<string, unknown>) => void;

export interface LoaderOptions {
  open: (signal: AbortSignal) => Promise<LoaderLake>;
  /** Joins the loader's group on the wake topic. */
  join: () => Promise<WakeSource>;
  metrics: Pick<Metrics, "add" | "set">;
  log: Log;
  /** How long records are gathered before a load; how long it waits for its source to replay what they name, and how often it asks; how soon a failed load is tried again; how long loads may fail before the loader is unhealthy. */
  gatherMs?: number;
  replayWaitMs?: number;
  replayPollMs?: number;
  retryMs?: number;
  unhealthyAfterMs?: number;
  sync?: (db: DuckDBConnection, options: LakeSyncOptions) => Promise<LakeLoad>;
}

/** Whether the loader is well (unhealthy once loads have failed long enough), and why. */
export interface LoaderHealth {
  ok: boolean;
  detail: string;
}

export interface Loader {
  health(): LoaderHealth;
  /** Whether it holds the lake open, and its member of the group. */
  ready(): boolean;
  stop(): Promise<void>;
}

/**
 * How long records are gathered before a load: alasio writes one a few seconds at most
 * while a turn writes its transcript, so a turn's are a load or two.
 */
export const GATHER_MS = 10_000;
/** The most records a load covers, and their most bytes: they say only that something was written, a few bytes each. */
const GATHER_RECORDS = 1_000;
const GATHER_BYTES = 1024 * 1024;
/** How long a load waits for its source to replay the writes its records name, and how often it asks meanwhile: a replica is seconds behind at most, but while it wakes. */
export const REPLAY_WAIT_MS = 120_000;
const REPLAY_POLL_MS = 500;
/** How soon a failed load is tried again. */
export const RETRY_MS = 30_000;
/** How long loads may fail before the loader is unhealthy. */
export const UNHEALTHY_AFTER_MS = 15 * 60_000;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** The WAL position `lsn`, as Postgres writes a pg_lsn (`16/B374D848`): a number. */
export function lsnValue(lsn: string): bigint {
  const [high, low] = lsn.split("/");
  if (!high || !low || !/^[0-9A-F]+$/iu.test(high) || !/^[0-9A-F]+$/iu.test(low)) throw new Error(`${JSON.stringify(lsn)} is no WAL position`);
  return (BigInt(`0x${high}`) << 32n) + BigInt(`0x${low}`);
}

/** What `records` name of the primary's writes (furthestWrite). */
export interface Writes {
  /** The furthest WAL position they name, which a load of theirs waits for its source to have replayed; null where none names one, as one alasio writes as it starts names none. */
  furthest: bigint | null;
  /** How many were not records alasio writes, or named a position that is none: loaded for, waiting for nothing of theirs. */
  malformed: number;
}

/** What `records` name of the primary's writes. */
export function furthestWrite(records: readonly WakeRecord[]): Writes {
  let furthest: bigint | null = null;
  let malformed = 0;
  for (const { value } of records) {
    let at: bigint | null;
    try {
      const named = (JSON.parse(value.toString("utf8")) as { lsn?: unknown }).lsn;
      at = named === null || named === undefined ? null : lsnValue(String(named));
    } catch {
      malformed += 1;
      continue;
    }
    if (at !== null && (furthest === null || at > furthest)) furthest = at;
  }
  return { furthest, malformed };
}

/** Whether the database `source` is on has replayed the primary's writes up to `lsn`: a replica, by pg_last_wal_replay_lsn(); a primary, which replays nothing, always. */
export async function sourceReplayed(source: pg.ClientBase, lsn: bigint): Promise<boolean> {
  const text = `${(lsn >> 32n).toString(16).toUpperCase()}/${(lsn & 0xffffffffn).toString(16).toUpperCase()}`;
  const { rows: [answer] } = await source.query<{ replayed: boolean }>("select coalesce(pg_last_wal_replay_lsn() >= $1::pg_lsn, true) as replayed", [text]);
  return answer?.replayed ?? false;
}

/** Where the group is to be told each partition of `records` is read up to: past the last record of each. */
export function readUpTo(records: readonly Position[]): Position[] {
  const next = new Map<string, Position>();
  for (const { topic, partition, offset } of records) {
    const key = `${topic}/${partition}`;
    if ((next.get(key)?.offset ?? -1n) <= offset) next.set(key, { topic, partition, offset: offset + 1n });
  }
  return [...next.values()];
}

/**
 * Starts the loop. `open(signal)` opens the lake for loading and resolves a LoaderLake,
 * whose `locked` takes the loader's lock for each load, waiting while another load holds
 * it, as a reconciliation's reload may, until `signal` aborts. `join()` joins the group. `sync` is the load, replaceable for
 * tests. Returns `{ health(), ready(), stop() }`: `health()` is `{ ok, detail }`, unhealthy
 * once loads have failed for `unhealthyAfterMs`; `stop()` lets a load under way finish,
 * closes the lake, leaves the group, and resolves once the loop has ended.
 */
export function startLoader({
  open,
  join,
  metrics,
  log,
  gatherMs = GATHER_MS,
  replayWaitMs = REPLAY_WAIT_MS,
  replayPollMs = REPLAY_POLL_MS,
  retryMs = RETRY_MS,
  unhealthyAfterMs = UNHEALTHY_AFTER_MS,
  sync = syncLake,
}: LoaderOptions): Loader {
  const stopped = new AbortController();
  let lake: LoaderLake | null = null;
  let source: WakeSource | null = null;
  let wake: (() => void) | null = null;
  let detail = "starting";
  let failingSince: number | null = null;

  /** Waits until `lake`'s source has replayed the writes up to `lsn`, failing past replayWaitMs. */
  async function untilReplayed(held: LoaderLake, lsn: bigint): Promise<void> {
    for (const deadline = Date.now() + replayWaitMs; !(await held.replayed(lsn));) {
      if (Date.now() >= deadline || stopped.signal.aborted) throw new Error(`its source had not replayed alasio's writes within ${replayWaitMs / 1000} s`);
      await new Promise((resolve) => setTimeout(resolve, replayPollMs));
    }
  }

  async function drop() {
    const [held, member] = [lake, source];
    lake = null;
    source = null;
    await held?.close().catch(() => {});
    await member?.close().catch(() => {});
  }

  /** Gathers records, and loads where any came; false where that failed. */
  async function cycle(): Promise<boolean> {
    try {
      lake ??= await open(stopped.signal);
      source ??= await join();
      const records = await source.next({ ms: gatherMs, records: GATHER_RECORDS, bytes: GATHER_BYTES });
      if (records.length === 0 || stopped.signal.aborted) return true;
      const { furthest, malformed } = furthestWrite(records);
      if (malformed > 0) metrics.add("lake_wake_records_malformed_total", {}, malformed);
      if (furthest !== null) await untilReplayed(lake, furthest);
      const started = performance.now();
      const [held, member] = [lake, source];
      // Under the loader's lock, each commit fenced by it, and its records told of only then.
      const { claude, codex } = await held.locked(async (fence) => {
        const load = await sync(held.db, { beforeCommit: fence });
        await fence();
        await member.commit(readUpTo(records));
        return load;
      });
      const seconds = (performance.now() - started) / 1000;
      metrics.add("lake_cycles_total", { outcome: "success" });
      metrics.set("lake_cycle_duration_seconds", {}, seconds);
      metrics.set("lake_last_success_timestamp_seconds", {}, Date.now() / 1000);
      metrics.add("lake_rows_total", { table: "claude.entries", change: "inserted" }, claude.inserted);
      metrics.add("lake_rows_total", { table: "claude.entries", change: "deleted" }, claude.deleted);
      metrics.add("lake_rows_total", { table: "codex.lines", change: "inserted" }, codex.inserted);
      metrics.add("lake_rows_total", { table: "codex.lines", change: "deleted" }, codex.deleted);
      log("loaded", { claude, codex, records: records.length, seconds: Number(seconds.toFixed(3)) });
      detail = "loaded";
      failingSince = null;
      return true;
    } catch (error) {
      if (stopped.signal.aborted) return false;
      metrics.add("lake_cycles_total", { outcome: "failure" });
      log("load failed", { error: errorText(error) });
      detail = `load failed: ${errorText(error)}`;
      failingSince ??= Date.now();
      await drop();
      return false;
    }
  }

  const done = (async () => {
    while (!stopped.signal.aborted) {
      if (await cycle() || stopped.signal.aborted) continue;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, retryMs);
        wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
    }
    await drop();
  })();

  return {
    health: () => ({ ok: failingSince === null || Date.now() - failingSince < unhealthyAfterMs, detail }),
    ready: () => lake !== null && source !== null,
    async stop() {
      stopped.abort();
      wake?.();
      // Records being gathered end with the member, unread, to be read again.
      await source?.close().catch(() => {});
      await done;
    },
  };
}
