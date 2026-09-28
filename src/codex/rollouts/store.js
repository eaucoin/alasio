/**
 * Codex's rollout files in alasio's Neon database, byte for byte.
 *
 * A rollout is Codex's record of a thread, the one source every index Codex
 * keeps is rebuilt from. Each file under `$CODEX_HOME` is a row here, keyed
 * by its file name, which archiving does not change, with its bytes as
 * chunks in order: a file that grows gets its new bytes as one more chunk,
 * and one rewritten gets all of them again. The bytes are never interpreted.
 */

export const DEFAULT_SCHEMA = "codex_sessions";

const ddl = (SCHEMA) => `
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

export class NeonRolloutStore {
  #pool;
  #schema;

  /** `schema` is where the tables live: alasio's, or a test's own. */
  constructor(pool, { schema = DEFAULT_SCHEMA } = {}) {
    if (!/^[a-z_][a-z0-9_]*$/u.test(schema)) {
      throw new Error(`invalid schema name: ${schema}`);
    }
    this.#pool = pool;
    this.#schema = schema;
  }

  /** Creates the tables if they are missing. Idempotent. */
  async ensureSchema() {
    await this.#pool.query(ddl(this.#schema));
  }

  /** Every rollout kept: `{ name, path, size, headDigest }`. */
  async list() {
    const { rows } = await this.#pool.query(`select name, path, size, head_digest from ${this.#schema}.rollouts`);
    return rows.map((row) => ({ name: row.name, path: row.path, size: Number(row.size), headDigest: row.head_digest }));
  }

  /**
   * Keeps a rollout's bytes from `start` on, which are all of them from 0,
   * replacing what was kept, or its new ones from where the kept ones end.
   * `rollout` is `{ name, path, threadId, rolloutId, historyBase, size, headDigest, modifiedMs }`.
   */
  async save(rollout, { start, bytes }) {
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
  }

  /** Records that a rollout's file is now at `path`, as archiving moves it. */
  async move(name, path) {
    await this.#pool.query(`update ${this.#schema}.rollouts set path = $2 where name = $1`, [name, path]);
  }

  /**
   * The rollouts of these threads and of every file their history starts
   * in, transitively: `{ name, path, size, modifiedMs }`, the files resuming
   * them needs.
   */
  async lineage(threadIds) {
    const { rows } = await this.#pool.query(
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

  /** A rollout's bytes, whole. */
  async read(name) {
    const { rows } = await this.#pool.query(
      `select bytes from ${this.#schema}.rollout_chunks where name = $1 order by start`,
      [name],
    );
    return Buffer.concat(rows.map((row) => row.bytes));
  }

  async #transaction(work) {
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
