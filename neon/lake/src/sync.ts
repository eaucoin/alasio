/**
 * What the lake does, apart from serving queries: get ready (its model, and how
 * long it keeps what it no longer needs), load from its source, and keep its files
 * in order.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { type ClaudeLoad, type ClaudeSyncOptions, syncClaude } from "./claude.ts";
import { type CodexLoad, type CodexSyncOptions, syncCodex } from "./codex.ts";
import { LAKE, literal, rows, serially, transaction } from "./lake.ts";
import { ensureModel, type RebuildOptions } from "./model.ts";
import { deleteExpiredTelemetry, ensureOtel } from "./otel.ts";

export type LakeSyncOptions = ClaudeSyncOptions & CodexSyncOptions;

/** What one load changed, by source. */
export interface LakeLoad {
  claude: ClaudeLoad;
  codex: CodexLoad;
}

/** How long a snapshot is kept for time travel before it is expired. */
export const SNAPSHOT_RETENTION = "7 days";
/** How long a file no snapshot needs any longer is kept before it is deleted. */
export const FILE_RETENTION = "1 day";

/**
 * Makes the lake ready to load and maintain: its model, rebuilt empty where it is
 * another version's (or `rebuild` asks), the telemetry's schema, which is never
 * rebuilt, and how long it keeps what it no longer needs. Returns whether the model was
 * rebuilt.
 */
export async function prepareLake(db: DuckDBConnection, { rebuild = false }: RebuildOptions = {}): Promise<boolean> {
  const rebuilt = await ensureModel(db, { rebuild });
  await ensureOtel(db);
  await transaction(db, async () => {
    await db.run(`call ${LAKE}.set_option('expire_older_than', ${literal(SNAPSHOT_RETENTION)})`);
    await db.run(`call ${LAKE}.set_option('delete_older_than', ${literal(FILE_RETENTION)})`);
  });
  return rebuilt;
}

/** One load of everything the source has that the lake does not. Returns what changed. */
export async function syncLake(db: DuckDBConnection, options: LakeSyncOptions = {}): Promise<LakeLoad> {
  return { claude: await syncClaude(db, options), codex: await syncCodex(db, options) };
}

/**
 * DuckLake's maintenance, in the order its CHECKPOINT takes it: what is inlined in the
 * catalog moved to files, snapshots past their retention expired, small files merged,
 * files mostly deleted rewritten, and then the files nothing refers to any longer
 * deleted: those expiry and the merges left (cleanup), and those no snapshot ever
 * recorded (orphans), which are the two steps that delete. Each is a call of its own,
 * so a lake whose files others still use (a branch's) can be kept without them.
 */
export const MAINTENANCE = [
  "ducklake_flush_inlined_data",
  "ducklake_expire_snapshots",
  "ducklake_merge_adjacent_files",
  "ducklake_rewrite_data_files",
  "ducklake_cleanup_old_files",
  "ducklake_delete_orphaned_files",
] as const;

/**
 * Keeps the lake in order: deletes the telemetry older than `retentionDays` days
 * (./otel.ts), then runs DuckLake's maintenance (MAINTENANCE), each step on its own.
 * Records when, so a restart does not repeat it.
 */
export async function maintainLake(db: DuckDBConnection, { retentionDays }: { retentionDays: number }): Promise<void> {
  await deleteExpiredTelemetry(db, retentionDays);
  for (const step of MAINTENANCE) await serially(() => db.run(`call ${step}('${LAKE}')`));
  await transaction(db, async () => {
    await db.run(`delete from ${LAKE}.loader.meta where key = 'maintained_at'`);
    await db.run(`insert into ${LAKE}.loader.meta values ('maintained_at', ${literal(new Date().toISOString())})`);
  });
}

/** When the lake was last maintained, or null. */
export async function lastMaintained(db: DuckDBConnection): Promise<Date | null> {
  const [row] = await rows<{ value: string }>(db, `select value from ${LAKE}.loader.meta where key = 'maintained_at'`);
  return row ? new Date(row.value) : null;
}
