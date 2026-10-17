/**
 * Maintenance of the lake, a pass a process, as main's CronJobs run it, and a Job each
 * environment's reconciliation (cli/src/manifests/lake.ts): a fresh process each time, so
 * what a pass's Parquet reading and writing took, which glibc never gives back, leaves no
 * long-lived process holding it.
 *
 *   node src/maintain.ts            the daily pass: the telemetry past its retention deleted,
 *                                   and DuckLake's maintenance (sync.ts's maintainLake), which
 *                                   deletes no file while Neon has branches beside main, nor
 *                                   while they cannot be learned (branches.ts)
 *   node src/maintain.ts merge      the hourly merge of the telemetry's files, which the intake
 *                                   writes a commit a minute (otel.ts's mergeTelemetry)
 *   node src/maintain.ts reconcile  the catalog set right with the object store, where files
 *                                   it names are gone, what the transcripts lost loaded
 *                                   again from alasio's database, and what the telemetry
 *                                   lost read again from the log (reconcile.ts): of any
 *                                   environment's lake, on demand alone
 *
 * Its writes take turns with every other writer's of the lake under its write lock
 * (lake.ts). It logs a JSON line per event, exports each as telemetry too (telemetry.ts),
 * and exits with 1 where the pass failed, so its Job says so.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { branchesBesideMain } from "./branches.ts";
import { type DatabaseConfig, loadMaintenanceConfig, type StoreParts } from "./config.ts";
import { openLake, writeLock } from "./lake.ts";
import type { Log } from "./loader.ts";
import { ensureOtel, mergeTelemetry } from "./otel.ts";
import { reconcileLake, type Recovery } from "./reconcile.ts";
import { maintainLake } from "./sync.ts";
import { type LakeTelemetry, startTelemetry } from "./telemetry.ts";

/** The passes, by the argument that asks for each. */
export const PASSES = ["maintain", "merge", "reconcile"] as const;
export type Pass = (typeof PASSES)[number];

/** Whether `value` names a pass. */
const isPass = (value: string): value is Pass => (PASSES as readonly string[]).includes(value);

export interface PassOptions {
  /** How many days of telemetry the daily pass keeps. */
  retentionDays: number;
  /** Neon's branches besides main, asked before the daily pass, which deletes no file while there are any, nor when they cannot be learned. */
  branches: () => Promise<readonly string[]>;
  /** The catalog's database, and the data path, which the reconciliation compares. */
  catalog: DatabaseConfig;
  dataPath: string;
  log: Log;
  /** How often the reconciliation asks again for the loader's lock while a load holds it, and how long it waits for the intake to take up what it asks of the log. */
  lockRetryMs?: number | undefined;
  rereadWaitMs?: number | undefined;
  /** The bundled object store's parts, which the reconciliation asks whether a file it fails a read of is gone; none for another store. */
  store?: StoreParts | null | undefined;
  /** Whether the lake is a branch environment's, whose telemetry main's lake reads again. */
  branch?: boolean | undefined;
  /** Where the reconciliation's gauges go: what each table lost. */
  telemetry?: Pick<LakeTelemetry, "gauge"> | null;
}

/** What a pass that fails logs. */
const FAILED: Record<Pass, string> = {
  maintain: "maintenance failed",
  merge: "merging the telemetry's files failed",
  reconcile: "reconciling the catalog with the object store failed",
};

/** What a pass says where it failed on a file the catalog names that the store does not hold, which it cannot read until the reconciliation forgets it. */
const GONE = "a file of the lake's catalog is gone from the object store: once you know why, alasio lake reconcile forgets it, and loads again from alasio's database what the transcripts lost";

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Whether `error` is a read of a file its store does not hold: S3's 404, or no such local file. */
const fileGone = (error: unknown) => /\bHTTP 404\b|NoSuchKey|No such file or directory/u.test(errorText(error));

/** What a reconciliation says of a table it lost, by where its rows come back from (./reconcile.ts's Recovery). */
const LOST_SAID: Record<Recovery | "nowhere", string> = {
  database: "lost from the object store, loaded again from alasio's database",
  log: "lost from the object store, read again from the log, where it still holds it",
  main: "lost from the object store, main's telemetry as of the branch point, which main's lake reads again from the log",
  nowhere: "lost from the object store, kept nowhere else",
};

/** What a reconciliation says of telemetry the log no longer holds, and of telemetry the intake has not read again yet (./reconcile.ts's Rereading). */
const GAP_SAID = "lost from the object store, and from the log, which no longer holds it";
const PENDING_SAID = "lost from the object store, still to be read again from the log";

/** Runs `pass` on the lake `db`, as the module says: whether it succeeded. */
export async function runPass(db: DuckDBConnection, pass: Pass, { retentionDays, branches, catalog, dataPath, log, lockRetryMs, rereadWaitMs, store, branch, telemetry }: PassOptions): Promise<boolean> {
  try {
    await ensureOtel(db);
    if (pass === "merge") {
      await mergeTelemetry(db);
      log("merged the telemetry's files");
      return true;
    }
    if (pass === "reconcile") {
      const { lost, load, reread } = await reconcileLake(db, catalog, dataPath, { log, lockRetryMs, rereadWaitMs, store, branch });
      for (const table of lost) {
        log(LOST_SAID[table.recovered ?? "nowhere"], { ...table });
        telemetry?.gauge("lake_lost_files", table.files, { table: table.table });
        telemetry?.gauge("lake_lost_rows", table.rows, { table: table.table });
      }
      for (const gap of reread?.gaps ?? []) log(GAP_SAID, { ...gap });
      if (reread && reread.pending.length > 0) log(PENDING_SAID, { signals: reread.pending });
      log("reconciled", { lost: lost.length, ...(load ? { loaded: load } : {}) });
      return true;
    }
    const kept = await branches().catch((error: unknown) => {
      log("could not learn Neon's branches; keeping every file", { error: errorText(error) });
      return null;
    });
    const keepFiles = kept === null || kept.length > 0;
    await maintainLake(db, { retentionDays, keepFiles });
    log("maintained", keepFiles ? { keptFilesFor: kept ?? "unknown branches" } : {});
    return true;
  } catch (error) {
    log(FAILED[pass], { error: errorText(error), ...(pass !== "reconcile" && fileGone(error) ? { because: GONE } : {}) });
    return false;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const pass = process.argv[2] ?? "maintain";
  if (!isPass(pass)) throw new Error(`no pass ${pass}: ${PASSES.join(" or ")}`);
  const config = loadMaintenanceConfig();
  writeLock(config.catalog);
  const telemetry = startTelemetry();
  const log: Log = (message, fields = {}) => {
    console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
    telemetry?.emit(message, fields);
  };
  const { branches } = config;
  let succeeded = false;
  try {
    // The reconciliation loads what the transcripts lost from alasio's database.
    const lake = await openLake(config, pass === "reconcile" ? { source: config.source } : {});
    try {
      succeeded = await runPass(lake.db, pass, {
        retentionDays: config.retentionDays,
        branches: branches ? () => branchesBesideMain(branches) : async () => [],
        catalog: config.catalog,
        dataPath: config.dataPath,
        store: config.store,
        branch: config.branch !== null,
        log,
        telemetry,
      });
    } finally {
      lake.close();
    }
  } catch (error) {
    log("could not open the lake", { error: errorText(error) });
  }
  await telemetry?.shutdown();
  process.exit(succeeded ? 0 : 1);
}
