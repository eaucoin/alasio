/**
 * Codex's rollout files in alasio's Neon database, byte for byte.
 *
 * A rollout is Codex's record of a thread, the one source every index Codex
 * keeps is rebuilt from. Each file under `$CODEX_HOME` is a row here, keyed
 * by its file name, which archiving does not change, with its bytes as
 * chunks in order: a file that grows gets its new bytes as one more chunk,
 * and one rewritten gets all of them again. The bytes are never interpreted.
 */
import type { Pool, PoolClient } from "pg";

const DEFAULT_SCHEMA = "codex_sessions";
/** Where the session-filesystem Codex home's rollouts are kept (codex/sessionfs.ts). */
export const SESSION_FS_SCHEMA = "codex_sessionfs_sessions";

const ddl = (SCHEMA: string): string => `
create schema if not exists ${SCHEMA};
create table if not exists ${SCHEMA}.rollouts (
  name text primary key,
  -- where the file is, relative to $CODEX_HOME
  path text not null,
  thread_id text not null,
  -- the thread's own id, or a reverted thread's new rollout id
  rollout_id text not null,
  -- the rollout id of the file this one's history starts in, if any
  history_base text,
  size bigint not null,
  -- sha256 of the file's first line, which a rewrite changes
  head_digest text not null,
  -- the file's modification time in milliseconds since the epoch, which
  -- Codex dates its thread by
  modified_ms bigint not null
);
create index if not exists rollouts_thread on ${SCHEMA}.rollouts (thread_id);
create index if not exists rollouts_rollout on ${SCHEMA}.rollouts (rollout_id);
create table if not exists ${SCHEMA}.rollout_chunks (
  name text not null references ${SCHEMA}.rollouts on delete cascade,
  -- where the chunk's bytes start in the file
  start bigint not null,
  bytes bytea not null,
  primary key (name, start)
);
`;

/** The most bytes one chunk holds, so that no statement carries more. */
const CHUNK_BYTES = 8 * 1024 * 1024;

/** A rollout as the store keeps it, enough to tell whether its file changed: as `list()` gives it. */
export interface KeptRollout {
  readonly name: string;
  path: string;
  readonly size: number;
  readonly headDigest: string;
}

/** A rollout file kept, as `files()` gives it: its name, where it is, its thread and its own id, when it last changed, and whether it is archived. */
export interface KeptRolloutFile {
  readonly name: string;
  readonly path: string;
  readonly threadId: string;
  readonly rolloutId: string;
  readonly modifiedMs: number;
  readonly archived: boolean;
}

/** A rollout file to keep, as `save()` takes it. */
export interface Rollout {
  readonly name: string;
  readonly path: string;
  readonly threadId: string;
  readonly rolloutId: string;
  /** The rollout id of the file this one's history starts in, if any. */
  readonly historyBase: string | null;
  readonly size: number;
  readonly headDigest: string;
  readonly modifiedMs: number;
}

/** Bytes of a rollout file, from `start` to its end. */
export interface RolloutBytes {
  readonly start: number;
  readonly bytes: Buffer;
}

/** A rollout a thread's resume needs, as `lineage()` gives it: where its file goes back, and how. */
export interface RestorableRollout {
  readonly name: string;
  readonly path: string;
  readonly size: number;
  readonly modifiedMs: number;
}

export interface NeonRolloutStoreOptions {
  readonly schema?: string;
  /** Told of each write once it is committed: the lake's wake (src/neon/lake-wake.ts). */
  readonly written?: () => void;
}

export class NeonRolloutStore {
  #pool: Pool;
  #schema: string;
  #written: () => void;

  /** `schema` is where the tables live: alasio's, or a test's own. */
  constructor(pool: Pool, { schema = DEFAULT_SCHEMA, written = () => {} }: NeonRolloutStoreOptions = {}) {
    if (!/^[a-z_][a-z0-9_]*$/u.test(schema)) {
      throw new Error(`invalid schema name: ${schema}`);
    }
    this.#pool = pool;
    this.#schema = schema;
    this.#written = written;
  }

  /** Creates the tables if they are missing. Idempotent. */
  async ensureSchema(): Promise<void> {
    await this.#pool.query(ddl(this.#schema));
  }

  /** Every rollout kept: `{ name, path, size, headDigest }`. */
  async list(): Promise<KeptRollout[]> {
    const { rows } = await this.#pool.query<{ name: string; path: string; size: string; head_digest: string }>(
      `select name, path, size, head_digest from ${this.#schema}.rollouts`,
    );
    return rows.map((row) => ({ name: row.name, path: row.path, size: Number(row.size), headDigest: row.head_digest }));
  }

  /**
   * Keeps a rollout's bytes from `start` on, which are all of them from 0,
   * replacing what was kept, or its new ones from where the kept ones end.
   * `rollout` is `{ name, path, threadId, rolloutId, historyBase, size, headDigest, modifiedMs }`.
   */
  async save(rollout: Rollout, { start, bytes }: RolloutBytes): Promise<void> {
    if (start + bytes.length !== rollout.size) {
      throw new Error(`${rollout.name}: ${bytes.length} bytes from ${start} do not make ${rollout.size}`);
    }
    await this.#transaction(async (client) => {
      if (start === 0) {
        await client.query(`delete from ${this.#schema}.rollout_chunks where name = $1`, [rollout.name]);
      }
      await client.query(
        `insert into ${this.#schema}.rollouts (name, path, thread_id, rollout_id, history_base, size, head_digest, modified_ms)
         values ($1, $2, $3, $4, $5, $6, $7, $8)
         on conflict (name) do update set path = excluded.path, thread_id = excluded.thread_id,
           rollout_id = excluded.rollout_id, history_base = excluded.history_base, size = excluded.size,
           head_digest = excluded.head_digest, modified_ms = excluded.modified_ms`,
        [rollout.name, rollout.path, rollout.threadId, rollout.rolloutId, rollout.historyBase, rollout.size, rollout.headDigest, Math.round(rollout.modifiedMs)],
      );
      for (let at = 0; at < bytes.length; at += CHUNK_BYTES) {
        await client.query(
          `insert into ${this.#schema}.rollout_chunks (name, start, bytes) values ($1, $2, $3)`,
          [rollout.name, start + at, bytes.subarray(at, at + CHUNK_BYTES)],
        );
      }
    });
    this.#written();
  }

  /** Records that a rollout's file is now at `path`, as archiving moves it. */
  async move(name: string, path: string): Promise<void> {
    await this.#pool.query(`update ${this.#schema}.rollouts set path = $2 where name = $1`, [name, path]);
    this.#written();
  }

  /**
   * The rollouts of these threads and of every file their history starts
   * in, transitively: `{ name, path, size, modifiedMs }`, the files resuming
   * them needs.
   */
  async lineage(threadIds: readonly string[]): Promise<RestorableRollout[]> {
    const { rows } = await this.#pool.query<{ name: string; path: string; size: string; modified_ms: string }>(
      `with recursive lineage as (
         select name, path, size, modified_ms, history_base from ${this.#schema}.rollouts where thread_id = any($1)
         union
         select r.name, r.path, r.size, r.modified_ms, r.history_base
           from ${this.#schema}.rollouts r join lineage l on r.rollout_id = l.history_base
       )
       select name, path, size, modified_ms from lineage order by name`,
      [threadIds],
    );
    return rows.map((row) => ({ name: row.name, path: row.path, size: Number(row.size), modifiedMs: Number(row.modified_ms) }));
  }

  /** The rollout files kept, of the threads `threadIds` where given, every one's otherwise. */
  async files({ threadIds }: { readonly threadIds?: readonly string[] }): Promise<KeptRolloutFile[]> {
    const { rows } = await this.#pool.query<{ name: string; path: string; thread_id: string; rollout_id: string; modified_ms: string }>(
      `select name, path, thread_id, rollout_id, modified_ms from ${this.#schema}.rollouts${threadIds ? " where thread_id = any($1)" : ""}`,
      threadIds ? [threadIds] : [],
    );
    return rows.map((row) => ({
      name: row.name,
      path: row.path,
      threadId: row.thread_id,
      rolloutId: row.rollout_id,
      modifiedMs: Number(row.modified_ms),
      archived: row.path.startsWith("archived_sessions/"),
    }));
  }

  /** The first bytes of a rollout, its first chunk's, up to CHUNK_BYTES: what of a file its first lines are in. */
  async head(name: string): Promise<Buffer> {
    const { rows } = await this.#pool.query<{ bytes: Buffer }>(`select bytes from ${this.#schema}.rollout_chunks where name = $1 and start = 0`, [name]);
    return rows[0]?.bytes ?? Buffer.alloc(0);
  }

  /** A rollout's bytes, whole. */
  async read(name: string): Promise<Buffer> {
    const { rows } = await this.#pool.query<{ bytes: Buffer }>(
      `select bytes from ${this.#schema}.rollout_chunks where name = $1 order by start`,
      [name],
    );
    return Buffer.concat(rows.map((row) => row.bytes));
  }

  async #transaction(work: (client: PoolClient) => Promise<void>): Promise<void> {
    const client = await this.#pool.connect();
    try {
      await client.query("begin");
      await work(client);
      await client.query("commit");
    } catch (error) {
      await client.query("rollback").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
