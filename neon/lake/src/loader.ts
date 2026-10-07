/**
 * The loader's loop: a load now and then every interval, and a maintenance pass
 * whenever the last is older than its interval (it is recorded in the lake, so a
 * restart neither repeats nor skips one), which deletes the telemetry past its
 * retention too.
 *
 * The loop opens the lake itself and keeps it open between loads. A load that fails
 * is logged and counted, its connections are dropped, and it is tried again sooner
 * than the interval on fresh ones: being transactional, it left the lake as the last
 * load did. So the loader rides out the compute restarting, its lock's connection
 * being lost, or alasio not having granted it its reads yet (src/neon/lake.ts).
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import type { Metrics } from "./metrics.ts";
import { type LakeLoad, lastMaintained, maintainLake, syncLake } from "./sync.ts";

/** The lake as the loader holds it: open for loading, under the loader's lock. */
export interface LoaderLake {
  db: DuckDBConnection;
  close(): Promise<unknown>;
  /** Whether it can no longer be used. */
  lost(): boolean;
}

/** Logs an event, with its fields. */
export type Log = (message: string, fields?: Record<string, unknown>) => void;

export interface LoaderOptions {
  open: (signal: AbortSignal) => Promise<LoaderLake>;
  metrics: Pick<Metrics, "add" | "set">;
  log: Log;
  intervalMs: number;
  maintenanceIntervalMs: number;
  /** How many days of telemetry maintenance keeps. */
  retentionDays: number;
  retryMs?: number;
  sync?: (db: DuckDBConnection) => Promise<LakeLoad>;
}

/** Whether the loader is well (unhealthy once loads have failed long enough), and why. */
export interface LoaderHealth {
  ok: boolean;
  detail: string;
}

export interface Loader {
  health(): LoaderHealth;
  stop(): Promise<void>;
}

/** How soon a failed load is tried again, at most. */
export const RETRY_MS = 30_000;
/** How many intervals of failing loads make the loader unhealthy. */
const UNHEALTHY_AFTER_INTERVALS = 3;

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Starts the loop. `open(signal)` opens the lake for loading and resolves
 * `{ db, close, lost }` (`lost()` says whether it can no longer be used); it may wait,
 * until `signal` aborts. `sync` is the load, replaceable for tests. Returns
 * `{ health(), stop() }`: `health()` is `{ ok, detail }`, unhealthy once loads have
 * failed for UNHEALTHY_AFTER_INTERVALS intervals; `stop()` lets a load under way finish,
 * closes the lake, and resolves once the loop has ended.
 */
export function startLoader({ open, metrics, log, intervalMs, maintenanceIntervalMs, retentionDays, retryMs = RETRY_MS, sync = syncLake }: LoaderOptions): Loader {
  const stopped = new AbortController();
  let lake: LoaderLake | null = null;
  let wake: (() => void) | null = null;
  let detail = "starting";
  let failingSince: number | null = null;

  async function drop() {
    const closing = lake;
    lake = null;
    await closing?.close().catch(() => {});
  }

  async function cycle() {
    const started = performance.now();
    try {
      if (lake?.lost()) await drop();
      lake ??= await open(stopped.signal);
      const { claude, codex } = await sync(lake.db);
      const seconds = (performance.now() - started) / 1000;
      metrics.add("lake_cycles_total", { outcome: "success" });
      metrics.set("lake_cycle_duration_seconds", {}, seconds);
      metrics.set("lake_last_success_timestamp_seconds", {}, Date.now() / 1000);
      metrics.add("lake_rows_total", { table: "claude.entries", change: "inserted" }, claude.inserted);
      metrics.add("lake_rows_total", { table: "claude.entries", change: "deleted" }, claude.deleted);
      metrics.add("lake_rows_total", { table: "codex.lines", change: "inserted" }, codex.inserted);
      metrics.add("lake_rows_total", { table: "codex.lines", change: "deleted" }, codex.deleted);
      if (claude.inserted || claude.deleted || codex.inserted || codex.deleted) {
        log("loaded", { claude, codex, seconds: Number(seconds.toFixed(3)) });
      }
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

  async function maintainIfDue() {
    // Only ever after a load that succeeded, which leaves the lake open.
    const { db } = lake!;
    const last = await lastMaintained(db).catch(() => null);
    if (last && Date.now() - last.getTime() < maintenanceIntervalMs) return;
    try {
      await maintainLake(db, { retentionDays });
      metrics.add("lake_maintenance_total", { outcome: "success" });
      log("maintained");
    } catch (error) {
      metrics.add("lake_maintenance_total", { outcome: "failure" });
      log("maintenance failed", { error: errorText(error) });
    }
  }

  const done = (async () => {
    while (!stopped.signal.aborted) {
      const loaded = await cycle();
      if (stopped.signal.aborted) break;
      if (loaded) await maintainIfDue();
      if (stopped.signal.aborted) break;
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, loaded ? intervalMs : Math.min(intervalMs, retryMs));
        wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
    }
    await drop();
  })();

  return {
    health: () => ({
      ok: failingSince === null || Date.now() - failingSince < UNHEALTHY_AFTER_INTERVALS * intervalMs,
      detail,
    }),
    async stop() {
      stopped.abort();
      wake?.();
      await done;
    },
  };
}
