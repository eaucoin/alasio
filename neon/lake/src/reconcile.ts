/**
 * The lake's catalog set right with its object store, where the store no longer holds files
 * the catalog names: as a restore of the bundled store's filer store loses every file written
 * after its backup, and the data of every file deleted since, which it names again
 * (cli/src/commands/restore-filer-store.ts). Every read of such a file fails, and so does
 * every pass that rewrites its table (./maintain.ts), until it is set right. A file is held
 * lost only on proof: the store answering it has none of it, or bytes of no Parquet file, or,
 * where the store fails its read, as it does both of a file whose chunks are gone and for a
 * moment while a volume server restarts, the bundled store's filer and masters showing a
 * chunk of it gone (gone); short of that, the reconciliation fails, to be run again.
 *
 * DuckLake has no way to forget a file it cannot read, as a delete reads the rows it deletes.
 * So the catalog is changed as DuckLake changes it when it drops a file whose rows are all
 * deleted (DuckLake's DuckLakeDelete::TryDropFullyDeletedFile): in one transaction of its
 * database, a snapshot of its own, at which each lost file, and its delete file, ends, with
 * its table's statistics less its rows. What was lost is recorded in that transaction too, in
 * `reconcile.lost_files` beside the catalog, with the range of the keys each file held, read
 * from the catalog's statistics of it.
 *
 * The transcripts' and rollouts' tables are derived from alasio's database (./model.ts), so
 * what they lost is loaded again from it: every row of the range a lost file held deleted,
 * Claude Code's entries and their blocks by `seq`, Codex's rollouts' lines and records by
 * name, which the load then finds missing and loads whole (./sync.ts). What the telemetry
 * lost is read again from the log, where it still holds it: the intake asked to read each
 * signal's topic again from the earliest time its tables' lost files held (./otel.ts's
 * askReread), writing each row the lake no longer has, once; what the log no longer holds,
 * past its retention, nothing refills, and the reconciliation says so, once the intake has
 * taken up what it asked, or that it has not yet. A branch environment's lake, whose
 * telemetry is main's as of its branch point and which no intake writes, asks nothing: main's
 * reads it again.
 */
import { setTimeout as sleep } from "node:timers/promises";

import type { DuckDBConnection } from "@duckdb/node-api";
import pg from "pg";

import type { DatabaseConfig, StoreParts } from "./config.ts";
import { heardClient, LAKE, literal, LOADER_LOCK_RETRY_MS, rows, serially, takeLoaderLock, transaction } from "./lake.ts";
import { askReread, type RereadGap, rereadGaps, rereadsAsked, telemetryTime } from "./otel.ts";
import type { Signal } from "./otlp.ts";
import { type LakeLoad, prepareLake, syncLake } from "./sync.ts";

/** A file the catalog names that the store does not hold, as the catalog describes it. */
interface CatalogFile {
  data_file_id: string;
  table_id: string;
  table_name: string;
  path: string;
  delete_path: string | null;
  rows: string;
  bytes: string;
  /** The range of the table's first time column in the file, as the catalog's statistics give it, as text. */
  first_at: string | null;
  last_at: string | null;
}

/** What one table lost: its files, their rows, and the time they spanned, as the catalog's statistics say. */
export interface LostTable {
  table: string;
  files: number;
  rows: number;
  from: string | null;
  to: string | null;
  /** Where what it lost comes back from: alasio's database, as the transcripts' tables' does; the log, as the telemetry's does, where it still holds it; or nowhere. */
  recovered: Recovery | null;
}

/**
 * Where a table's lost rows come back from (LostTable): alasio's database; the log; or, of a
 * branch environment's lake, whose telemetry is main's as of its branch point and which no
 * intake writes, main's lake, which reads them again from the log.
 */
export type Recovery = "database" | "log" | "main";

/** Where the lost rows of `table` come back from, of a branch environment's lake where `branch`: the derived tables' from alasio's database, the telemetry's from the log (../otel.ts's askReread), or main's. */
const recoveryOf = (table: string, branch: boolean): Recovery | null =>
  DERIVED.has(table) ? "database" : telemetryTime(table) === null ? null : branch ? "main" : "log";

/**
 * What came of asking the log again for what the telemetry lost: the signals asked of,
 * those the intake had not taken up within the reconciliation's wait, still to be read again,
 * and the times of those it took up that the log no longer holds, which nothing refills.
 */
export interface Rereading {
  asked: Signal[];
  pending: Signal[];
  gaps: RereadGap[];
}

/** What a reconciliation found and did. */
export interface Reconciled {
  lost: LostTable[];
  /** The load that brought back what the transcripts' tables lost, where any did. */
  load: LakeLoad | null;
  /** What came of asking the log again for what the telemetry lost, where any did. */
  reread: Rereading | null;
}

/**
 * The tables loaded from alasio's database whose rows are deleted together, by the range of
 * `key` a lost file of either held, so that the load finds each row missing in both and loads
 * it whole: an entry with its blocks, a rollout with its record.
 */
const RELOADS = [
  { tables: ["claude.entries", "claude.content_blocks"], key: "seq", type: "BIGINT" },
  { tables: ["codex.lines", "codex.files"], key: "name", type: "VARCHAR" },
] as const;

/** The tables loaded from alasio's database: those RELOADS deletes the ranges of, and the model's version, whose loss rebuilds it whole (./model.ts). */
const DERIVED = new Set<string>([...RELOADS.flatMap(({ tables }) => tables), "loader.meta"]);

/** The table of what was lost, beside the catalog, in its database. */
const LOST = "reconcile.lost_files";

/** The commit the catalog's snapshot of a reconciliation is recorded with, as DuckLake records its own. */
const AUTHOR = "alasio";
const MESSAGE = "files lost from the object store";

// Each live file, its path resolved as DuckLake resolves it: a schema's path relative to the
// data path, a table's to its schema's, a file's to its table's; and its first time column's
// range and its RELOADS key's, from the catalog's statistics of the file.
const CATALOG_FILES = `
  with data_path as (
    select value from ducklake.ducklake_metadata where key = 'data_path' and scope is null
  ),
  tables as (
    select t.table_id, s.schema_name || '.' || t.table_name as table_name,
      case when t.path_is_relative
        then case when s.path_is_relative then (select value from data_path) || s.path else s.path end || t.path
        else t.path end as path
    from ducklake.ducklake_table t join ducklake.ducklake_schema s on s.schema_id = t.schema_id
    where t.end_snapshot is null and s.end_snapshot is null
  ),
  times as (
    select distinct on (table_id) table_id, column_id from ducklake.ducklake_column
    where end_snapshot is null and parent_column is null and column_type like 'timestamp%'
    order by table_id, column_order
  )
  select f.data_file_id::text, f.table_id::text, t.table_name,
    case when f.path_is_relative then t.path || f.path else f.path end as path,
    case when d.path_is_relative then t.path || d.path else d.path end as delete_path,
    f.record_count::text as rows, f.file_size_bytes::text as bytes,
    stats.min_value as first_at, stats.max_value as last_at
  from ducklake.ducklake_data_file f
  join tables t on t.table_id = f.table_id
  left join ducklake.ducklake_delete_file d on d.data_file_id = f.data_file_id and d.end_snapshot is null
  left join times on times.table_id = f.table_id
  left join ducklake.ducklake_file_column_stats stats on stats.data_file_id = f.data_file_id and stats.column_id = times.column_id
  where f.end_snapshot is null`;

/** The range of `key` the data files `ids` held, as the catalog's statistics of them give it. */
const KEY_RANGES = `
  select f.data_file_id::text, stats.min_value, stats.max_value
  from ducklake.ducklake_data_file f
  join ducklake.ducklake_column c on c.table_id = f.table_id and c.column_name = $2 and c.parent_column is null and c.end_snapshot is null
  left join ducklake.ducklake_file_column_stats stats on stats.data_file_id = f.data_file_id and stats.column_id = c.column_id
  where f.data_file_id = any($1::bigint[])`;

/**
 * Whether the store holds the file at `path`: a listing of it, which names it alone. The
 * pattern's last character is a class of itself, as DuckDB's S3 glob gives back a path with
 * no wildcard as it is, listing nothing; one with a wildcard it lists, by the prefix before
 * it. The lake's paths end in `.parquet`, so the class is a letter's.
 */
export async function stored(db: DuckDBConnection, path: string): Promise<boolean> {
  return (await rows(db, "select file from glob($1)", [`${path.slice(0, -1)}[${path.slice(-1)}]`])).length > 0;
}

/**
 * What a read of a file says where the store no longer gives its data, its name kept: the
 * store answering it has none (404), none of the range asked (416, as of a file left empty),
 * or what it gives holding no Parquet file.
 */
const GONE = /\(HTTP (?:404|416)\b|No magic bytes found|too small to be a Parquet file/u;

/**
 * What a read of a file says where the store fails it (500): as the bundled store's filer
 * fails an entry whose chunks are gone, but as it fails one too while a volume server
 * restarts, or is overloaded, so it proves nothing on its own (gone).
 */
const FAILED = /returned HTTP 500 for/u;

/** How many times a file is read before a read that says its data is gone holds it lost, and how far apart. */
export const READ_ATTEMPTS = 3;
export const READ_RETRY_MS = 1000;

/** A file whose read the store failed, and whose data its parts do not show gone: to be reconciled again, once the store answers. */
export class StoreUndecided extends Error {
  override readonly name = "StoreUndecided";
  constructor(path: string, why: string) {
    super(`the object store failed a read of ${path}, and does not show its data gone: ${why}; run the reconciliation again once the store answers`);
  }
}

/** How long a part of the store is given to answer. */
const PART_TIMEOUT_MS = 10_000;

/** A part of the store's answer to `url`: its status, and its body, parsed as JSON where it is. */
async function asked(url: string, method = "GET"): Promise<{ status: number; body: unknown }> {
  const response = await fetch(url, { method, headers: { accept: "application/json" }, signal: AbortSignal.timeout(PART_TIMEOUT_MS) });
  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  return { status: response.status, body };
}

/**
 * Whether the bundled store's parts show the data of the file at `path`, an s3:// path, gone,
 * as a read of it the store failed cannot say: its filer's entry for it, under /buckets, and,
 * of each of its chunks, the masters' location of its volume and that volume server's answer
 * for its id. Gone where the filer has no entry (its /buckets known to be there:
 * bucketListed), an entry of bytes has no chunk, every server of a chunk's volume has no such
 * id, or the volume is not one the masters know while they know every volume server the
 * store runs, read the same TOPOLOGY_PULSES times (topologyWhole): a volume server
 * restarting is dropped from their layout, with its volumes, until it registers again.
 * Throws StoreUndecided where a part does not answer as one of those, or where every chunk
 * is served, as the failure was then the store's of the moment.
 */
export async function gone(path: string, store: StoreParts | null, { pulseMs = TOPOLOGY_PULSE_MS }: { pulseMs?: number } = {}): Promise<boolean> {
  if (store === null) throw new StoreUndecided(path, "the object store is not the bundled one, whose filer and masters would say");
  const { bucket, key } = /^s3:\/\/(?<bucket>[^/]+)\/(?<key>.+)$/u.exec(path)?.groups ?? {};
  if (!bucket || !key) throw new StoreUndecided(path, "it is not an s3:// path");
  const undecided = (why: string) => new StoreUndecided(path, why);
  const entry = await asked(`${store.filer}/buckets/${bucket}/${key.split("/").map(encodeURIComponent).join("/")}?metadata=true`).catch((error: unknown) => {
    throw undecided(`its filer did not answer: ${error instanceof Error ? error.message : String(error)}`);
  });
  if (entry.status === 404) return true;
  if (entry.status !== 200 || typeof entry.body !== "object" || entry.body === null) throw undecided(`its filer answered ${entry.status}`);
  const { chunks = [], FileSize: size = 0 } = entry.body as { chunks?: { file_id?: string }[] | null; FileSize?: number };
  if ((chunks ?? []).length === 0) {
    if (size > 0) return true;
    throw undecided("its filer's entry holds no chunk, as an empty file's");
  }
  for (const { file_id: fid = "" } of chunks ?? []) {
    const volume = /^(\d+),/u.exec(fid)?.[1];
    if (!volume) throw undecided(`its filer names a chunk ${fid || "with no id"}`);
    const lookup = await asked(`${store.master}/dir/lookup?volumeId=${volume}`).catch((error: unknown) => {
      throw undecided(`the masters did not answer for volume ${volume}: ${error instanceof Error ? error.message : String(error)}`);
    });
    const { locations = [], error = "" } = (lookup.body ?? {}) as { locations?: { url: string }[]; error?: string };
    if (/not found/u.test(error)) {
      const short = await topologyWhole(store, pulseMs);
      if (short === null) return true;
      throw undecided(`the masters know no volume ${volume}, but ${short}`);
    }
    if (lookup.status !== 200 || locations.length === 0) throw undecided(`the masters answered ${lookup.status} for volume ${volume}${error ? `: ${error}` : ""}`);
    const served = await Promise.all(locations.map(({ url }) =>
      asked(`http://${url}/${fid}`, "HEAD").then(({ status }) => status, () => null)
    ));
    if (served.every((status) => status === 404)) return true;
    if (!served.some((status) => status === 200)) throw undecided(`the servers of volume ${volume} answered ${served.map((status) => status ?? "nothing").join(", ")} for chunk ${fid}`);
  }
  throw undecided("its filer, masters and volume servers hold every chunk of it");
}

/** How many times the masters' topology is read before it is held whole, and how far apart. */
export const TOPOLOGY_PULSES = 3;
export const TOPOLOGY_PULSE_MS = 2000;

/**
 * Why the masters' topology (/dir/status) is not whole: short of the volume servers the
 * store runs, or not the same servers each of TOPOLOGY_PULSES reads `pulseMs` apart, or not
 * answered; null where it is whole.
 */
async function topologyWhole(store: StoreParts, pulseMs: number): Promise<string | null> {
  let seen: string | null = null;
  for (let pulse = 0; pulse < TOPOLOGY_PULSES; pulse += 1) {
    if (pulse > 0) await sleep(pulseMs);
    const status = await asked(`${store.master}/dir/status`).catch(() => null);
    if (status?.status !== 200) return `the masters did not say their topology${status ? ` (${status.status})` : ""}`;
    const servers: string[] = [];
    const walk = (node: unknown): void => {
      if (Array.isArray(node)) return node.forEach(walk);
      if (typeof node !== "object" || node === null) return;
      for (const [key, value] of Object.entries(node)) {
        if (key === "DataNodes" && Array.isArray(value)) servers.push(...value.map((server: { Url?: string }) => server.Url ?? ""));
        else walk(value);
      }
    };
    walk(status.body);
    if (servers.length < store.volumeServers) return `they know ${servers.length} of the store's ${store.volumeServers} volume servers, as while one restarts`;
    const now = servers.toSorted().join(",");
    if (seen !== null && now !== seen) return "their volume servers changed as they were read";
    seen = now;
  }
  return null;
}

/**
 * Throws where the filer of the store `store` has no /buckets/<bucket> of the data path
 * `dataPath`, an s3:// path, where it keeps the bucket: its having no entry of a file would
 * then prove nothing (gone). None of a data path of no bucket's.
 */
export async function bucketListed(store: StoreParts, dataPath: string): Promise<void> {
  const bucket = /^s3:\/\/([^/]+)\//u.exec(dataPath)?.[1];
  if (!bucket) return;
  const listing = await asked(`${store.filer}/buckets/${encodeURIComponent(bucket)}/?limit=1`).catch((error: unknown) => {
    throw new Error(`the object store's filer did not answer for /buckets/${bucket}: ${error instanceof Error ? error.message : String(error)}`);
  });
  if (listing.status !== 200) throw new Error(`the object store's filer has no /buckets/${bucket} (${listing.status}), where it keeps the lake's bucket: run the reconciliation again once it does`);
}

/**
 * Runs `read` with DuckDB's cache of the store's files off, as it was before. DuckDB checks
 * a cached file only by the store's HEAD of it, its size and ETag, and SeaweedFS answers
 * HEAD from the filer's entry alone. So a file whose chunks are gone, with its entry still
 * there or named again by a restore of the filer store, would be read from memory, though
 * the store fails every GET of it.
 */
async function uncached<T>(db: DuckDBConnection, read: () => Promise<T>): Promise<T> {
  const [setting] = await rows<{ cached: boolean }>(db, "select current_setting('enable_external_file_cache') as cached");
  await db.run("set enable_external_file_cache = false");
  try {
    return await read();
  } finally {
    await db.run(`set enable_external_file_cache = ${setting?.cached === false ? "false" : "true"}`);
  }
}

/**
 * Whether the store gives the data of the file at `path`, which it lists: its Parquet
 * footer read from the store itself, never DuckDB's cache of it (uncached), again
 * READ_ATTEMPTS times `retryMs` apart where the store's answer says its
 * data is gone (GONE), as a store may fail a read once. Where the store fails the read
 * (FAILED), whether it is gone is asked of the bundled store's parts (gone), which throws
 * where they do not show it; any other failure, as of the store not answering, is thrown,
 * and fails the reconciliation rather than lose a file.
 */
export async function readable(
  db: DuckDBConnection,
  path: string,
  { store = null, retryMs = READ_RETRY_MS, pulseMs = TOPOLOGY_PULSE_MS }: { store?: StoreParts | null | undefined; retryMs?: number; pulseMs?: number } = {},
): Promise<boolean> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      await uncached(db, () => rows(db, "select num_rows from parquet_file_metadata($1)", [path]));
      return true;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (FAILED.test(message)) return !(await gone(path, store, { pulseMs }));
      if (!GONE.test(message)) throw error;
      if (attempt >= READ_ATTEMPTS) return false;
      await sleep(retryMs);
    }
  }
}

/**
 * The catalog's live files the store does not hold: those its listing of the data path does
 * not name, each asked of the store again on its own, so a listing that leaves one out loses
 * nothing, and those it names but no longer gives the data of (readable), as a restore of the
 * filer store leaves a file deleted since its backup: named again, its chunks gone. A file
 * whose delete file is lost is lost with it, as its rows cannot be told apart.
 */
async function lostFiles(db: DuckDBConnection, catalog: pg.ClientBase, dataPath: string, store: StoreParts | null): Promise<CatalogFile[]> {
  const files = (await catalog.query<CatalogFile>(CATALOG_FILES)).rows;
  if (store) await bucketListed(store, dataPath);
  const listed = new Set((await rows<{ file: string }>(db, "select file from glob($1)", [`${dataPath}**`])).map(({ file }) => file));
  const lost: CatalogFile[] = [];
  for (const file of files) {
    for (const path of [file.path, file.delete_path]) {
      if (path === null || ((listed.has(path) || (await stored(db, path))) && (await readable(db, path, { store })))) continue;
      lost.push(file);
      break;
    }
  }
  return lost;
}

/**
 * Ends `lost` in the catalog, in one transaction of its database: a snapshot after its
 * last, at which each file and its delete file end, its table's statistics less them, and
 * the record of each in LOST, with the range of its RELOADS key where it has one, or of its
 * time where it is the telemetry's (telemetryTime); committed only as `fence` passes, the
 * lake's write lock it runs under held still.
 */
async function forget(catalog: pg.ClientBase, lost: readonly CatalogFile[], fence: () => Promise<void>): Promise<void> {
  await catalog.query("begin");
  try {
    await catalog.query("create schema if not exists reconcile");
    await catalog.query(`create table if not exists ${LOST} (
      data_file_id bigint primary key, snapshot_id bigint not null, found_at timestamptz not null,
      table_name text not null, path text not null, rows bigint not null, first_at text, last_at text,
      key_from text, key_to text, reloaded_at timestamptz)`);
    // Every other commit of the catalog's, DuckLake's own of whatever writes to the lake,
    // inserts its snapshot, which this lock holds back until this commits: so the snapshot
    // after the last is this one's alone.
    await catalog.query("lock table ducklake.ducklake_snapshot in share row exclusive mode");
    const [last] = (await catalog.query<{ snapshot_id: string | null }>("select max(snapshot_id)::text as snapshot_id from ducklake.ducklake_snapshot")).rows;
    if (!last?.snapshot_id) throw new Error("the catalog has no snapshot");
    const snapshot = BigInt(last.snapshot_id) + 1n;
    await catalog.query(
      `insert into ducklake.ducklake_snapshot
        select $1, now(), schema_version, next_catalog_id, next_file_id from ducklake.ducklake_snapshot where snapshot_id = $2`,
      [snapshot, last.snapshot_id],
    );
    const tables = [...new Set(lost.map(({ table_id }) => table_id))];
    await catalog.query("insert into ducklake.ducklake_snapshot_changes values ($1, $2, $3, $4, null)", [
      snapshot,
      tables.map((id) => `deleted_from_table:${id}`).join(","),
      AUTHOR,
      MESSAGE,
    ]);
    const ids = lost.map(({ data_file_id }) => data_file_id);
    await catalog.query("update ducklake.ducklake_data_file set end_snapshot = $1 where end_snapshot is null and data_file_id = any($2::bigint[])", [snapshot, ids]);
    await catalog.query("update ducklake.ducklake_delete_file set end_snapshot = $1 where end_snapshot is null and data_file_id = any($2::bigint[])", [snapshot, ids]);
    await catalog.query(
      `update ducklake.ducklake_table_stats s
        set record_count = greatest(0, s.record_count - gone.rows), file_size_bytes = greatest(0, s.file_size_bytes - gone.bytes)
        from (select table_id, sum(record_count) as rows, sum(file_size_bytes) as bytes from ducklake.ducklake_data_file
              where data_file_id = any($1::bigint[]) group by table_id) gone
        where s.table_id = gone.table_id`,
      [ids],
    );
    const ranges = new Map<string, { min_value: string | null; max_value: string | null }>();
    for (const { tables: family, key } of RELOADS) {
      const inFamily = lost.filter(({ table_name }) => (family as readonly string[]).includes(table_name)).map(({ data_file_id }) => data_file_id);
      if (inFamily.length === 0) continue;
      for (const range of (await catalog.query<{ data_file_id: string; min_value: string | null; max_value: string | null }>(KEY_RANGES, [inFamily, key])).rows) {
        ranges.set(range.data_file_id, range);
      }
    }
    // A telemetry table's by its time, from which the log is read again (askReread).
    for (const table of new Set(lost.map(({ table_name }) => table_name))) {
      const time = telemetryTime(table);
      if (time === null) continue;
      const ofTable = lost.filter(({ table_name }) => table_name === table).map(({ data_file_id }) => data_file_id);
      for (const range of (await catalog.query<{ data_file_id: string; min_value: string | null; max_value: string | null }>(KEY_RANGES, [ofTable, time])).rows) {
        ranges.set(range.data_file_id, range);
      }
    }
    for (const file of lost) {
      const range = ranges.get(file.data_file_id);
      await catalog.query(
        `insert into ${LOST} values ($1, $2, now(), $3, $4, $5, $6, $7, $8, $9, null) on conflict (data_file_id) do nothing`,
        [file.data_file_id, snapshot, file.table_name, file.path, file.rows, file.first_at, file.last_at, range?.min_value ?? null, range?.max_value ?? null],
      );
    }
    // None of it once the write lock it ran under is lost (../lake.ts's transaction).
    await fence();
    await catalog.query("commit");
  } catch (error) {
    await catalog.query("rollback").catch(() => {});
    throw error;
  }
}

/** A recorded loss not yet loaded, or read, again. */
interface Unloaded {
  data_file_id: string;
  table_name: string;
  key_from: string | null;
  key_to: string | null;
}

/** The losses LOST records and has not brought back yet whose rows come back from `from`. */
async function unbrought(catalog: pg.ClientBase, from: Recovery, branch: boolean): Promise<Unloaded[]> {
  const recorded = await catalog.query<{ found: boolean }>("select to_regclass($1) is not null as found", [LOST]);
  if (!recorded.rows[0]?.found) return [];
  return (await catalog.query<Unloaded>(`select data_file_id::text, table_name, key_from, key_to from ${LOST} where reloaded_at is null`)).rows
    .filter(({ table_name }) => recoveryOf(table_name, branch) === from);
}

/** Records `files` as brought back. */
async function broughtBack(catalog: pg.ClientBase, files: readonly Unloaded[]): Promise<void> {
  // None where LOST may not be there yet, as before a reconciliation first lost anything.
  if (files.length === 0) return;
  await catalog.query(`update ${LOST} set reloaded_at = now() where data_file_id = any($1::bigint[])`, [files.map(({ data_file_id }) => data_file_id)]);
}

/**
 * Asks the log again for what the telemetry lost, as LOST records it and has not asked
 * (../otel.ts's askReread): each table's from the earliest time its lost files held, or from
 * the log's first record where the catalog has no statistics of one, then each recorded as
 * asked. The ask and the record are two commits, of the lake and of the catalog: run again
 * between them, it asks again, which is harmless, as the intake reads from the earliest asked
 * and writes each row the lake lacks once, its dedupe skipping those it has. A branch
 * environment's lake (`branch`) asks nothing: it has no intake, and main's reads its telemetry
 * again. The signals asked of, and since when, as an ISO 8601 time; null where nothing was.
 */
async function askAgain(db: DuckDBConnection, catalog: pg.ClientBase, branch: boolean): Promise<{ signals: Signal[]; since: string } | null> {
  // A branch environment's lake has no intake to ask: main's reads its telemetry again.
  if (branch) {
    await broughtBack(catalog, await unbrought(catalog, "main", true));
    return null;
  }
  const lost = await unbrought(catalog, "log", false);
  if (lost.length === 0) return null;
  const from = new Map<string, string | null>();
  for (const { table_name, key_from } of lost) {
    const earliest = from.has(table_name) ? from.get(table_name) ?? null : key_from;
    from.set(table_name, earliest === null || key_from === null ? null : key_from < earliest ? key_from : earliest);
  }
  const since = new Date().toISOString();
  await askReread(db, [...from].map(([table, at]) => ({ table, from: at })));
  await broughtBack(catalog, lost);
  return { signals: (await rereadsAsked(db)).map(({ signal }) => signal), since };
}

/**
 * Loads again what the derived tables lost, as LOST records it and has not loaded: every
 * row of each lost file's range deleted from its RELOADS tables (all of them where the
 * catalog has no statistics of the range), in one transaction, then the model made ready
 * (rebuilt whole where its version was lost) and loaded from alasio's database, then each
 * recorded as loaded, each commit fenced by the loader's lock it runs under (`fence`). Run
 * again after any step, it loads the same again.
 */
async function reload(db: DuckDBConnection, catalog: pg.ClientBase, fence: () => Promise<void>): Promise<LakeLoad | null> {
  const unloaded = await unbrought(catalog, "database", false);
  if (unloaded.length === 0) return null;
  await transaction(db, async () => {
    for (const { tables, key, type } of RELOADS) {
      for (const file of unloaded.filter(({ table_name }) => (tables as readonly string[]).includes(table_name))) {
        const within = file.key_from === null || file.key_to === null
          ? "true"
          : `${key} between ${literal(file.key_from)}::${type} and ${literal(file.key_to)}::${type}`;
        for (const table of tables) await db.run(`delete from ${LAKE}.${table} where ${within}`);
      }
    }
    await fence();
  });
  await prepareLake(db);
  const load = await syncLake(db, { beforeCommit: fence });
  await fence();
  await broughtBack(catalog, unloaded);
  return load;
}

/** How long a reconciliation waits for the intake to take up what it asked of the log, and how often it looks. */
export const REREAD_WAIT_MS = 5 * 60_000;
const REREAD_POLL_MS = 1000;

/**
 * What the intake made of `asked` (askAgain) within `waitMs`: the signals it had not taken
 * up by then, still to be read again; and of those it took up, what the log no longer holds
 * (../otel.ts's rereadGaps), which the lake cannot be refilled with.
 */
async function rereadOf(db: DuckDBConnection, asked: { signals: Signal[]; since: string }, waitMs: number): Promise<Rereading> {
  let pending: Signal[] = [];
  for (const deadline = Date.now() + waitMs; ; await sleep(REREAD_POLL_MS)) {
    const still = new Set((await rereadsAsked(db)).map(({ signal }) => signal));
    pending = asked.signals.filter((signal) => still.has(signal));
    if (pending.length === 0 || Date.now() >= deadline) break;
  }
  return { asked: asked.signals, pending, gaps: await rereadGaps(db, asked.since) };
}

/** What `lost` lost, by table, the time spanned the widest the files' statistics give. */
function byTable(lost: readonly CatalogFile[], branch: boolean): LostTable[] {
  const tables = new Map<string, LostTable>();
  for (const file of lost) {
    const seen = tables.get(file.table_name) ?? { table: file.table_name, files: 0, rows: 0, from: null, to: null, recovered: recoveryOf(file.table_name, branch) };
    seen.files += 1;
    seen.rows += Number(file.rows);
    if (file.first_at !== null && (seen.from === null || file.first_at < seen.from)) seen.from = file.first_at;
    if (file.last_at !== null && (seen.to === null || file.last_at > seen.to)) seen.to = file.last_at;
    tables.set(file.table_name, seen);
  }
  return [...tables.values()].toSorted((a, b) => a.table.localeCompare(b.table));
}

/** Runs `work`, saying, where it fails, that `what` failed, and why. */
async function step<T>(what: string, work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (error) {
    throw new Error(`${what} failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
}

/** What a reconciliation says as it goes, and how often it asks again for the loader's lock while a load holds it. */
export interface ReconcileOptions {
  log: (message: string, fields?: Record<string, unknown>) => void;
  lockRetryMs?: number | undefined;
  /** How long it waits for the intake to take up what it asked of the log (REREAD_WAIT_MS). */
  rereadWaitMs?: number | undefined;
  /** The bundled object store's parts, which say whether a file the store fails a read of is gone (gone); none for another store. */
  store?: StoreParts | null | undefined;
  /** Whether the lake is a branch environment's, whose telemetry main's lake reads again (Recovery). */
  branch?: boolean | undefined;
}

/** The name the reconciliation's connection to the catalog gives itself (application_name). */
export const RECONCILE_APPLICATION = "alasio-lake-reconcile";

/**
 * Sets the lake's catalog right with its store, as the module says: forgets the files it
 * names that the store does not hold, under the lake's write lock; asks the log again for
 * what the telemetry lost; loads again from alasio's database what the derived tables lost,
 * then or by a reconciliation before that did not finish, under the loader's lock, waiting
 * while a load holds it, as two loads at once would each load what the lake lacks; and waits,
 * for `rereadWaitMs` at most, for the intake to take up what it asked, to say what the log no
 * longer holds. `db` is the lake, open with alasio's database as its source; `catalog` the
 * catalog's database, on the lake's role.
 */
export async function reconcileLake(
  db: DuckDBConnection,
  catalog: DatabaseConfig,
  dataPath: string,
  { log, lockRetryMs = LOADER_LOCK_RETRY_MS, rereadWaitMs = REREAD_WAIT_MS, store = null, branch = false }: ReconcileOptions,
): Promise<Reconciled> {
  // Named, as an operator sees its connection; and a connection lost, as its backend
  // terminated while it waits on the loader's lock, fails its next statement, and so the
  // reconciliation, not the process, as pg tells its 'error' listeners too, whose absence
  // Node takes as the process's to die of.
  const client = heardClient({ ...catalog, application_name: RECONCILE_APPLICATION });
  await client.connect();
  try {
    const lost = await serially(async (fence) => {
      const found = await lostFiles(db, client, dataPath, store);
      if (found.length > 0) await step("forgetting the lost files in the catalog", () => forget(client, found, fence));
      return found;
    });
    const asked = await step("asking the log again for what the telemetry lost", () => askAgain(db, client, branch));
    const lock = await takeLoaderLock(catalog, log, { retryMs: lockRetryMs });
    let load: LakeLoad | null;
    try {
      load = await step("loading again what the transcripts lost", () => reload(db, client, lock.fence));
    } finally {
      await lock.release();
    }
    return { lost: byTable(lost, branch), load, reread: asked && (await step("waiting for the intake to read the log again", () => rereadOf(db, asked, rereadWaitMs))) };
  } finally {
    await client.end().catch(() => {});
  }
}
