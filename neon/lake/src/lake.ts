/**
 * A DuckDB connection to the lake: the DuckLake catalog in Postgres, its Parquet
 * files at the data path, and, for loading, alasio's database attached read-only as
 * the source. DuckDB runs in this process and keeps nothing of its own: everything
 * durable is in the catalog and the object store, so any process can open the lake
 * and none has to stay up for it.
 *
 * The lake service opens it for its loader, and each of the telemetry intake's replicas
 * for itself (./intake.ts), each on a DuckDB of its own. Their writes take turns:
 * DuckLake 1.0 on Postgres mishandles commits that race (duplicate snapshot ids, retries
 * run out), so every write to the lake is made through `serially`, one at a time in a
 * process and, once `writeLock` is set, under a Postgres advisory lock of the catalog's
 * across processes, so none ever races another. The query endpoint opens it read-only,
 * in a process of its own (./endpoint.ts).
 */
import { tmpdir } from "node:os";
import { join } from "node:path";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue, type JS } from "@duckdb/node-api";
import pg from "pg";

import type { DatabaseConfig, LakeAccess } from "./config.ts";
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
  source?: string;
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
 * A libpq connection string naming `database` alone: the server, the role and its
 * password come from the DuckDB's Postgres secret (openLake), so nothing DuckDB says of a
 * connection, which quotes this, carries the password into a log.
 */
export function conninfo(database: string): string {
  return `dbname='${database.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
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
      await db.run(`attach ${literal(conninfo(source))} as ${SOURCE} (type postgres, read_only)`);
    }
    // Serially, as the lake's first attach writes its catalog.
    await serially(() =>
      db.run(
        `attach ${literal(`ducklake:postgres:${conninfo(config.catalog.database)}`)} as ${LAKE}
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

/** Names the lock every writer of the lake holds while it writes, in the catalog's database. */
const WRITE_LOCK = "alasio.lake.write";

/** Runs `work` holding the lake's write lock across processes; unset, `work` alone. */
let acrossProcesses: <T>(work: () => Promise<T>) => Promise<T> = (work) => work();

/**
 * Makes every write of this process's (`serially`) hold the lake's write lock, a session
 * advisory lock of `catalog`'s database, on a connection of its own, made anew when lost:
 * the loader's and the intake replicas' writes then take turns as one process's do.
 */
export function writeLock(catalog: DatabaseConfig): void {
  let client: pg.Client | null = null;
  const connected = async (): Promise<pg.Client> => {
    if (client) return client;
    const fresh = new pg.Client({ ...catalog, connectionTimeoutMillis: 30_000 });
    fresh.on("error", () => {
      if (client === fresh) client = null;
    });
    fresh.on("end", () => {
      if (client === fresh) client = null;
    });
    await fresh.connect();
    client = fresh;
    return fresh;
  };
  // The lock's connection and the lake's are to one server: one that is lost with the
  // server mid-write is lost with the write, which commits nothing.
  acrossProcesses = async (work) => {
    const held = await connected();
    await held.query("select pg_advisory_lock(hashtext($1))", [WRITE_LOCK]);
    try {
      return await work();
    } finally {
      await held.query("select pg_advisory_unlock(hashtext($1))", [WRITE_LOCK]).catch(async () => {
        // A connection that cannot unlock is ended, which unlocks.
        if (client === held) client = null;
        await held.end().catch(() => {});
      });
    }
  };
}

const writes = queue();

/** Runs `work`, which writes to the lake, after every write given before it, one at a time, and under the write lock where one is set (writeLock). */
export const serially: Queue = (work) => writes(() => acrossProcesses(work));

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
