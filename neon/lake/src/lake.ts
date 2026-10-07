/**
 * A DuckDB connection to the lake: the DuckLake catalog in Postgres, its Parquet
 * files at the data path, and, for loading, alasio's database attached read-only as
 * the source. DuckDB runs in this process and keeps nothing of its own: everything
 * durable is in the catalog and the object store, so any process can open the lake
 * and none has to stay up for it.
 *
 * The lake service opens it twice, for the loader and for the telemetry intake, each
 * on a DuckDB of its own, so either can drop and reopen its connections without the
 * other's. Their writes still take turns: DuckLake 1.0 on Postgres mishandles commits
 * that race (duplicate snapshot ids, retries run out), so every write to the lake in
 * this process is made through `serially`, one at a time, and none ever races another.
 */
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue, type JS } from "@duckdb/node-api";

import type { DatabaseConfig, LakeConfig } from "./config.ts";
import { EXTENSIONS } from "./extensions.ts";

/** The names the lake and its source are attached under. */
export const LAKE = "lake";
export const SOURCE = "source";

/** The lake, open: a DuckDB connection, and how to close it. */
export interface Lake {
  db: DuckDBConnection;
  close(): void;
}

export interface OpenLakeOptions {
  /** Attach the lake read-only, for queries. */
  readOnly?: boolean;
  /** Attach alasio's database too, read-only, for loading. */
  source?: boolean;
  /** DuckDB's memory limit, when not the configuration's. */
  memoryLimit?: string;
}

/** A row as DuckDB returns it, its values as plain JavaScript. */
export type Row = Record<string, JS>;

/** `value` as an SQL string literal. */
export function literal(value: string): string {
  return `'${String(value).replaceAll("'", "''")}'`;
}

/** A libpq connection string for `database` (`{ host, port, user, password, database }`). */
export function conninfo({ host, port, user, password, database }: DatabaseConfig): string {
  const quote = (value: string | number) => `'${String(value).replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
  return [
    `host=${quote(host)}`,
    `port=${quote(port)}`,
    `dbname=${quote(database)}`,
    `user=${quote(user)}`,
    `password=${quote(password)}`,
  ].join(" ");
}

/**
 * Opens the lake. `readOnly` attaches it read-only, for queries; `source` also
 * attaches alasio's database read-only, for loading; `memoryLimit` replaces the
 * configuration's. Returns `{ db, close }`, `db` a DuckDB connection.
 */
export async function openLake(config: LakeConfig, { readOnly = false, source = !readOnly, memoryLimit = config.memoryLimit }: OpenLakeOptions = {}): Promise<Lake> {
  const options: Record<string, string> = {
    memory_limit: memoryLimit,
    threads: String(config.threads),
    // Where a large load spills past the memory limit: scratch, never the lake.
    temp_directory: join(tmpdir(), "duckdb-lake"),
  };
  if (config.extensionDirectory) {
    // Only what the image installed: an extension is never fetched at run time.
    Object.assign(options, { extension_directory: config.extensionDirectory, autoinstall_known_extensions: "false" });
  }
  const instance = await DuckDBInstance.create(":memory:", options);
  const db = await instance.connect();
  const close = () => {
    db.closeSync();
    instance.closeSync();
  };
  try {
    for (const extension of Object.keys(EXTENSIONS)) {
      if (!config.extensionDirectory) await db.run(`install ${extension}`);
      await db.run(`load ${extension}`);
    }
    if (config.s3) {
      const endpoint = new URL(config.s3.endpoint);
      await db.run(`create temporary secret lake_store (
        type s3, key_id ${literal(config.s3.key)}, secret ${literal(config.s3.secret)},
        endpoint ${literal(endpoint.host)}, use_ssl ${endpoint.protocol === "https:"},
        url_style 'path', region 'us-east-1')`);
    }
    if (source) {
      await db.run(`attach ${literal(conninfo(config.source))} as ${SOURCE} (type postgres, read_only)`);
    }
    // Serially, as the lake's first attach writes its catalog.
    await serially(() =>
      db.run(
        `attach ${literal(`ducklake:postgres:${conninfo(config.catalog)}`)} as ${LAKE}
          (data_path ${literal(config.dataPath)}, metadata_schema 'ducklake'${readOnly ? ", read_only" : ""})`,
      )
    );
    return { db, close };
  } catch (error) {
    close();
    throw error;
  }
}

/**
 * The rows `sql` returns, as plain JavaScript values. `Selected` names the columns
 * `sql` selects and the values DuckDB gives them, which the caller's SQL decides.
 */
export async function rows<Selected = Row>(db: DuckDBConnection, sql: string, values?: DuckDBValue[]): Promise<Selected[]> {
  return (await db.runAndReadAll(sql, values)).getRowObjectsJS() as Selected[];
}

/** Runs work it is given once all it was given before has ended, and resolves as that work does. */
export type Queue = <T>(work: () => Promise<T>) => Promise<T>;

/** A queue: work run one at a time, in the order it is given. */
export function queue(): Queue {
  let last: Promise<unknown> = Promise.resolve();
  return (work) => {
    const result = last.then(work);
    last = result.catch(() => {});
    return result;
  };
}

/** Runs `work`, which writes to the lake, after every write given before it: this process's writes, one at a time. */
export const serially: Queue = queue();

/** Runs `work` in one transaction on the lake, serially: all of it is committed, or none. */
export function transaction<T>(db: DuckDBConnection, work: () => Promise<T>): Promise<T> {
  return serially(async () => {
    await db.run("begin transaction");
    try {
      const result = await work();
      await db.run("commit");
      return result;
    } catch (error) {
      await db.run("rollback").catch(() => {});
      throw error;
    }
  });
}
