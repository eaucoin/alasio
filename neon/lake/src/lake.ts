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
 * The query endpoint opens it read-only, in a process of its own (./endpoint.ts).
 */
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue, type JS } from "@duckdb/node-api";

import type { LakeAccess, SourceConfig } from "./config.ts";
import { EXTENSIONS } from "./extensions.ts";

/** The names the lake and its source are attached under. */
export const LAKE = "lake";
export const SOURCE = "source";

/** The lake, open: a DuckDB connection, how much memory its DuckDB holds, and how to close it. */
export interface Lake {
  db: DuckDBConnection;
  /** The bytes the DuckDB's buffer manager holds now, asked on a connection of its own, so it never waits on `db`'s work. */
  memory(): Promise<number>;
  close(): void;
}

export interface OpenLakeOptions {
  /** Attach the lake read-only, for queries. */
  readOnly?: boolean;
  /** alasio's database, on the catalog's server, to attach too, read-only and as the catalog's role, for loading. */
  source?: SourceConfig;
  /** DuckDB's memory limit, when not the configuration's. */
  memoryLimit?: string;
}

/** A row as DuckDB returns it, its values as plain JavaScript. */
export type Row = Record<string, JS>;

/** `value` as an SQL string literal. */
export function literal(value: string): string {
  return `'${String(value).replaceAll("'", "''")}'`;
}

/**
 * A libpq connection string naming `database`, and the `options` sent with it, when any,
 * patient for as long as Neon's proxy may take to wake a compute that sleeps: the server,
 * the role and its password come from the DuckDB's Postgres secret (openLake), so nothing
 * DuckDB says of a connection, which quotes this, carries the password into a log.
 */
export function conninfo(database: string, options = ""): string {
  const quoted = (value: string) => `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
  return `dbname=${quoted(database)} connect_timeout=60${options ? ` options=${quoted(options)}` : ""}`;
}

/**
 * Opens the lake. `readOnly` attaches it read-only, for queries; `source` also
 * attaches alasio's database of that name read-only, for loading; `memoryLimit` replaces the
 * configuration's. Returns `{ db, memory, close }`, `db` a DuckDB connection.
 */
export async function openLake(config: LakeAccess, { readOnly = false, source, memoryLimit = config.memoryLimit }: OpenLakeOptions = {}): Promise<Lake> {
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
  const meter = await instance.connect();
  const close = () => {
    meter.closeSync();
    db.closeSync();
    instance.closeSync();
  };
  // A sum is one row.
  const memory = async () => Number((await rows<{ bytes: bigint }>(meter, "select coalesce(sum(memory_usage_bytes), 0) as bytes from duckdb_memory()"))[0]!.bytes);
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
    // Where every attach of Postgres connects, and as whom.
    const { host, port, user, password } = config.catalog;
    await db.run(`create temporary secret (
      type postgres, host ${literal(host)}, port ${port}, user ${literal(user)}, password ${literal(password)})`);
    if (source) {
      await db.run(`attach ${literal(conninfo(source.database, source.options))} as ${SOURCE} (type postgres, read_only)`);
    }
    // Serially, as the lake's first attach writes its catalog.
    await serially(() =>
      db.run(
        `attach ${literal(`ducklake:postgres:${conninfo(config.catalog.database, config.catalog.options)}`)} as ${LAKE}
          (data_path ${literal(config.dataPath)}, metadata_schema 'ducklake'${readOnly ? ", read_only" : ""})`,
      )
    );
    return { db, memory, close };
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
