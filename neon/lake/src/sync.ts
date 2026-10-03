/**
 * What the lake does, apart from serving queries: get ready (its model, and how
 * long it keeps what it no longer needs), load from its source, and keep its files
 * in order.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { type ClaudeLoad, type ClaudeSyncOptions, syncClaude } from "./claude.ts";
import { type CodexLoad, type CodexSyncOptions, syncCodex } from "./codex.ts";
import { LAKE, literal, rows, transaction } from "./lake.ts";
import { ensureModel, type RebuildOptions } from "./model.ts";

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
 * Makes the lake ready to load: its model, rebuilt empty where it is another
 * version's (or `rebuild` asks), and its retention. Returns whether it was rebuilt.
 */
export async function prepareLake(db: DuckDBConnection, { rebuild = false }: RebuildOptions = {}): Promise<boolean> {
  const rebuilt = await ensureModel(db, { rebuild });
  await db.run(`call ${LAKE}.set_option('expire_older_than', ${literal(SNAPSHOT_RETENTION)})`);
  await db.run(`call ${LAKE}.set_option('delete_older_than', ${literal(FILE_RETENTION)})`);
  return rebuilt;
}

/** One load of everything the source has that the lake does not. Returns what changed. */
export async function syncLake(db: DuckDBConnection, options: LakeSyncOptions = {}): Promise<LakeLoad> {
  return { claude: await syncClaude(db, options), codex: await syncCodex(db, options) };
}

/**
 * Keeps the lake's files in order: merges the small files loads write, expires old
 * snapshots, and deletes the files nothing refers to any longer (DuckLake's
 * CHECKPOINT does each in turn). Records when, so a restart does not repeat it.
 */
export async function maintainLake(db: DuckDBConnection): Promise<void> {
  await db.run(`checkpoint ${LAKE}`);
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
