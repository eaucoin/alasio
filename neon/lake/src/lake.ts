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
 * run out), so every write to the lake is made through `transaction`, one at a time in a
 * process and, once `writeLock` is set, under a Postgres advisory lock of the catalog's
 * across processes, committed only while that is held, so none ever races another. The
 * query endpoint opens it read-only, in a process of its own (./endpoint.ts), as the
 * reader's role, which each writer lets read the catalog as it opens it (grantReads).
 */
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue, type JS } from "@duckdb/node-api";
import pg from "pg";

import { type DatabaseConfig, type LakeAccess, READER_ROLE, type SourceConfig } from "./config.ts";
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
 * Lets the reader's role read the catalog, as the lake's role, which owns it: its schema,
 * every table it makes from now on, as DuckLake makes one for each table's inlined rows
 * whenever a table's rows are first inlined or its columns change, and every table in it
 * now, in that order, so that a table a writer makes as this runs is either granted with
 * the rest or made under the default. Idempotent.
 */
export async function grantReads(catalog: pg.ClientBase): Promise<void> {
  await catalog.query(`grant usage on schema ducklake to ${READER_ROLE}`);
  await catalog.query(`alter default privileges in schema ducklake grant select on tables to ${READER_ROLE}`);
  await catalog.query(`grant select on all tables in schema ducklake to ${READER_ROLE}`);
}

/**
 * Opens the lake. `readOnly` attaches it read-only, for queries; `source` also
 * attaches alasio's database of that name read-only, for loading; `memoryLimit` replaces the
 * configuration's. Returns `{ db, memory, close }`, `db` a DuckDB connection. A writer's open
 * lets the reader's role read the catalog (grantReads) before it writes anything: each table a
 * writer makes is then the reader's to read, whichever writer opened the lake first.
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
    if (!readOnly) {
      // A connection lost fails the grants, and so the lake's open, not the process.
      const catalog = heardClient(config.catalog);
      await catalog.connect();
      try {
        await grantReads(catalog);
      } finally {
        await catalog.end().catch(() => {});
      }
    }
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

/**
 * How long a write waits for the write lock before it fails: far longer than any write
 * holds it, so only a writer that hangs holding it fails those behind it, which their
 * intake or loader retries, rather than leaving every one of them waiting for good.
 */
const WRITE_LOCK_WAIT_MS = 600_000;

/**
 * The lake's write lock not had, or not held to the end of a write: not taken within its
 * wait, its connection lost, or its database not answering. The write commits nothing, and
 * is the lock's to fail, not what it writes: its writer writes it again.
 */
export class WriteLockLost extends Error {
  override readonly name = "WriteLockLost";
  constructor(cause: unknown) {
    super(`the lake's write lock was not held: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
  }
}

/** Work that writes to the lake, given `fence`, which rejects where the write lock it runs under is no longer held. */
export type Write<T> = (fence: () => Promise<void>) => Promise<T>;

/** Runs `work` holding the lake's write lock across processes; unset, `work` alone, its fence passing at once. */
let acrossProcesses: <T>(work: Write<T>) => Promise<T> = (work) => work(async () => {});

/** How long a connection to the catalog may take: Neon's proxy holds it while it wakes a compute that sleeps, which has taken half a minute. */
export const CONNECT_TIMEOUT_MS = 60_000;

/**
 * A client of `config`'s, connecting within CONNECT_TIMEOUT_MS, its connection's loss heard, as
 * `onLost` says it, so it fails its statements, and its holder's work, not the process: pg tells
 * a connection's 'error' listeners of it as it is lost, its backend terminated as by a compute's
 * restart, a failover or an operator, whether a statement of it was running or none was, and
 * with none listening Node takes it as the process's to die of. Every pg client of the lake's is
 * made by it, as alasio's are by its own (src/persistence/sql.ts heardClient), which the lake's
 * package, built apart, does not reach; test/pg-heard.test.ts holds them to it.
 */
export function heardClient(config: pg.ClientConfig, onLost: (error: Error) => void = () => {}): pg.Client {
  const client = new pg.Client({ connectionTimeoutMillis: CONNECT_TIMEOUT_MS, ...config });
  client.on("error", onLost);
  return client;
}

/**
 * Makes every write of this process's (`serially`) hold the lake's write lock, a session
 * advisory lock of `catalog`'s database, on a connection of its own, made anew when lost,
 * waiting `waitMs` for it at most: the loader's and the intake replicas' writes then
 * take turns as one process's do.
 */
export function writeLock(catalog: DatabaseConfig, waitMs = WRITE_LOCK_WAIT_MS): void {
  let client: pg.Client | null = null;
  const connected = async (): Promise<pg.Client> => {
    if (client) return client;
    const fresh: pg.Client = heardClient(catalog, () => {
      if (client === fresh) client = null;
    });
    fresh.on("end", () => {
      if (client === fresh) client = null;
    });
    await fresh.connect();
    try {
      // pg_advisory_lock waits for as long as lock_timeout lets it, and then fails.
      await fresh.query(`set lock_timeout = ${waitMs}`);
    } catch (error) {
      await fresh.end().catch(() => {});
      throw error;
    }
    client = fresh;
    return fresh;
  };
  // The lock's connection can be lost alone, mid-write, and the lock with it, while the
  // write goes on on the lake's: its commit is fenced by a round trip on the lock's
  // connection, which answers only while its session, and so the session's lock, lasts.
  // The lock goes unchecked only from that answer to the commit, as close as two
  // connections allow.
  acrossProcesses = async (work) => {
    const held = await connected().catch((error: unknown) => Promise.reject(new WriteLockLost(error)));
    await held.query("select pg_advisory_lock(hashtext($1))", [WRITE_LOCK]).catch((error: unknown) => Promise.reject(new WriteLockLost(error)));
    try {
      return await work(async () => {
        await held.query("select").catch((error: unknown) => Promise.reject(new WriteLockLost(error)));
      });
    } finally {
      await held.query("select pg_advisory_unlock(hashtext($1))", [WRITE_LOCK]).catch(async () => {
        // A connection that cannot unlock is ended, which unlocks.
        if (client === held) client = null;
        await held.end().catch(() => {});
      });
    }
  };
}

/** Names the advisory lock of the one load of the lake from alasio's database at a time, in the catalog's database. */
const LOADER_LOCK = "alasio.lake.loader";
/** How often the lock is asked for again while another holds it. */
export const LOADER_LOCK_RETRY_MS = 10_000;

/**
 * The loader's lock, held: `fence()` rejects where it is no longer held, as its connection
 * was lost, which a write under it checks just before it commits, as the write lock's fence
 * does; `release()` lets it go.
 */
export interface LoaderLock {
  fence(): Promise<void>;
  release(): Promise<void>;
}

/** Where the loader's lock was lost under a load. */
export class LoaderLockLost extends Error {
  override readonly name = "LoaderLockLost";
  constructor(cause: unknown) {
    super(`the loader's lock was lost: ${cause instanceof Error ? cause.message : String(cause)}`, { cause });
  }
}

/**
 * Takes the lock of the one load of the lake from alasio's database at a time, waiting
 * while another holds it, until `signal` aborts: the loader's, for each load (./loader.ts),
 * and a reconciliation's reload (./reconcile.ts), so that no two compute what the lake
 * lacks, and load it, at once, which would load it twice. `log` says that it waits.
 */
export async function takeLoaderLock(
  catalog: DatabaseConfig,
  log: (message: string, fields?: Record<string, unknown>) => void,
  { signal, retryMs = LOADER_LOCK_RETRY_MS }: { signal?: AbortSignal | undefined; retryMs?: number | undefined } = {},
): Promise<LoaderLock> {
  // A connection lost is found by the fence's round trip.
  const client = heardClient(catalog);
  try {
    await client.connect();
    for (let attempt = 0; ; attempt += 1) {
      const { rows: [answer] } = await client.query<{ held: boolean }>("select pg_try_advisory_lock(hashtext($1)) as held", [LOADER_LOCK]);
      if (answer?.held) {
        return {
          // A round trip on the lock's connection answers only while its session, and so its lock, lasts.
          fence: () => client.query("select").then(() => {}, (error: unknown) => Promise.reject(new LoaderLockLost(error))),
          release: () => client.end().catch(() => {}),
        };
      }
      if (attempt % 6 === 0) log("another loader holds the lock; waiting");
      await sleep(retryMs, undefined, { signal });
    }
  } catch (error) {
    await client.end().catch(() => {});
    throw error;
  }
}

const writes = queue();

/** Runs `work`, which writes to the lake, after every write given before it, one at a time, and under the write lock where one is set (writeLock). */
export function serially<T>(work: Write<T>): Promise<T> {
  return writes(() => acrossProcesses(work));
}

/**
 * Runs `work` in one transaction on the lake, serially: all of it is committed, or none,
 * and none of it once the write lock it ran under is lost.
 */
export function transaction<T>(db: DuckDBConnection, work: () => Promise<T>): Promise<T> {
  return serially(async (fence) => {
    await db.run("begin transaction");
    try {
      const result = await work();
      await fence();
      await db.run("commit");
      return result;
    } catch (error) {
      await db.run("rollback").catch(() => {});
      throw error;
    }
  });
}
