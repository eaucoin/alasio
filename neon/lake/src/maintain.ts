/**
 * Maintenance of main's lake, a pass a process, as its CronJobs run it
 * (cli/src/manifests/lake.ts): a fresh process each time, so what a pass's Parquet reading
 * and writing took, which glibc never gives back, leaves no long-lived process holding it.
 *
 *   node src/maintain.ts         the daily pass: the telemetry past its retention deleted,
 *                                and DuckLake's maintenance (sync.ts's maintainLake), which
 *                                deletes no file while Neon has branches beside main, nor
 *                                while they cannot be learned (branches.ts)
 *   node src/maintain.ts merge   the hourly merge of the telemetry's files, which the intake
 *                                writes a commit a minute (otel.ts's mergeTelemetry)
 *
 * Its writes take turns with every other writer's of the lake under its write lock
 * (lake.ts). It logs a JSON line per event, exports each as telemetry too (telemetry.ts),
 * and exits with 1 where the pass failed, so its Job says so.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { branchesBesideMain } from "./branches.ts";
import { loadMaintenanceConfig } from "./config.ts";
import { openLake, writeLock } from "./lake.ts";
import type { Log } from "./loader.ts";
import { ensureOtel, mergeTelemetry } from "./otel.ts";
import { maintainLake } from "./sync.ts";
import { startTelemetry } from "./telemetry.ts";

/** The passes, by the argument that asks for each. */
export const PASSES = ["maintain", "merge"] as const;
export type Pass = (typeof PASSES)[number];

/** Whether `value` names a pass. */
const isPass = (value: string): value is Pass => (PASSES as readonly string[]).includes(value);

export interface PassOptions {
  /** How many days of telemetry the daily pass keeps. */
  retentionDays: number;
  /** Neon's branches besides main, asked before the daily pass, which deletes no file while there are any, nor when they cannot be learned. */
  branches: () => Promise<readonly string[]>;
  log: Log;
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/** Runs `pass` on the lake `db`, as the module says: whether it succeeded. */
export async function runPass(db: DuckDBConnection, pass: Pass, { retentionDays, branches, log }: PassOptions): Promise<boolean> {
  try {
    await ensureOtel(db);
    if (pass === "merge") {
      await mergeTelemetry(db);
      log("merged the telemetry's files");
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
    log(pass === "merge" ? "merging the telemetry's files failed" : "maintenance failed", { error: errorText(error) });
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
    const lake = await openLake(config);
    try {
      succeeded = await runPass(lake.db, pass, { retentionDays: config.retentionDays, branches: branches ? () => branchesBesideMain(branches) : async () => [], log });
    } finally {
      lake.close();
    }
  } catch (error) {
    log("could not open the lake", { error: errorText(error) });
  }
  await telemetry?.shutdown();
  process.exit(succeeded ? 0 : 1);
}
