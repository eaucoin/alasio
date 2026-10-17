/**
 * What the lake does, apart from serving queries: get ready (its model, and how
 * long it keeps what it no longer needs), load from its source, and keep its files
 * in order.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { type ClaudeLoad, type ClaudeSyncOptions, syncClaude } from "./claude.ts";
import { type CodexLoad, type CodexSyncOptions, syncCodex } from "./codex.ts";
import { LAKE, literal, transaction } from "./lake.ts";
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
 * The size up to which maintenance merges a table's files: one this large is left as it
 * is, so a pass merges what was written since the last, not the whole table again.
 * DuckLake's own, 512 MB, made it rewrite every transcript entry each night: on the
 * stack, 160 000 of them in a 213 MB file, which took the lake's container past its
 * memory limit (1536 MiB) once it had run for a few hours.
 */
export const TARGET_FILE_SIZE = "64MB";

/**
 * Makes the lake ready to load and maintain: its model, rebuilt empty where it is
 * another version's (or `rebuild` asks), the telemetry's schema, which is never
 * rebuilt, how long it keeps what it no longer needs, and how large the files are that
 * it merges. Returns whether the model was rebuilt.
 */
export async function prepareLake(db: DuckDBConnection, { rebuild = false }: RebuildOptions = {}): Promise<boolean> {
  const rebuilt = await ensureModel(db, { rebuild });
  await ensureOtel(db);
  await transaction(db, async () => {
    await db.run(`call ${LAKE}.set_option('expire_older_than', ${literal(SNAPSHOT_RETENTION)})`);
    await db.run(`call ${LAKE}.set_option('delete_older_than', ${literal(FILE_RETENTION)})`);
    await db.run(`call ${LAKE}.set_option('target_file_size', ${literal(TARGET_FILE_SIZE)})`);
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
 * recorded (orphans), which are the two steps that delete (DELETING). Each is a call of
 * its own, so a lake whose files others still use can be kept without them.
 */
export const MAINTENANCE = [
  "ducklake_flush_inlined_data",
  "ducklake_expire_snapshots",
  "ducklake_merge_adjacent_files",
  "ducklake_rewrite_data_files",
  "ducklake_cleanup_old_files",
  "ducklake_delete_orphaned_files",
] as const;

/** The steps of MAINTENANCE that delete files. */
export const DELETING: ReadonlySet<(typeof MAINTENANCE)[number]> = new Set(["ducklake_cleanup_old_files", "ducklake_delete_orphaned_files"]);

/** What maintainLake is given. */
export interface MaintenanceOptions {
  /** How many days of telemetry it keeps. */
  retentionDays: number;
  /**
   * Whether it keeps every file: while Neon has branches, each of whose lakes is a copy of
   * this one's catalog at its branch point, which still names files this one's no longer
   * does, and writes files of its own beside them, which this one's never named.
   */
  keepFiles: boolean;
  /** The time of the pass, the one its retention's days are taken of; now unless given. */
  now?: Date;
}

/**
 * Keeps the lake in order: deletes the telemetry older than `retentionDays` days
 * (./otel.ts), then runs DuckLake's maintenance (MAINTENANCE), each step on its own, but
 * for those that delete files when it `keepFiles`; what they would have deleted waits
 * for the first pass that does not. Its retention is cut at `now`, the time it is given.
 */
export async function maintainLake(db: DuckDBConnection, { retentionDays, keepFiles, now = new Date() }: MaintenanceOptions): Promise<void> {
  await deleteExpiredTelemetry(db, retentionDays, now);
  for (const step of MAINTENANCE) {
    if (keepFiles && DELETING.has(step)) continue;
    await transaction(db, () => db.run(`call ${step}('${LAKE}')`));
  }
}
