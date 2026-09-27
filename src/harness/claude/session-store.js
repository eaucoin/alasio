/**
 * Claude Code's transcripts in alasio's Neon database, through the Agent SDK's
 * SessionStore contract (@anthropic-ai/claude-agent-sdk, `SessionStore`).
 *
 * Every transcript entry Claude Code writes locally is mirrored here by the
 * SDK. This is the durable copy: a local transcript that is missing, on a new
 * machine or after Claude Code's own cleanup, is written back from it (see
 * transcripts.js). Queries resume in the real Claude home through
 * `mirrorOnly(store)`; the SDK's session helpers read this store directly.
 *
 * Entries are opaque JSON lines, one row each, ordered by insertion, kept as
 * `json`: their exact text, which `jsonb` would not keep (it rejects the
 * \u0000 a tool's binary output can carry, and reorders keys). An entry
 * the SDK re-delivers after a retried append carries the same uuid and is
 * kept once; entries without a uuid are kept as appended.
 *
 * Postgres's JSON operators fail on any entry holding a NUL or half a
 * surrogate pair, so each row also has `doc`, a `jsonb` copy of its entry
 * for SQL to query, with those characters as U+FFFD. It is written with the
 * entry and never read here. Where Postgres will not take even that as
 * `jsonb`, `doc` is null and the entry is stored all the same.
 */
import { foldSessionSummary } from "@anthropic-ai/claude-agent-sdk";

export const DEFAULT_SCHEMA = "claude_sessions";

const ddl = (SCHEMA) => `
create schema if not exists ${SCHEMA};
create table if not exists ${SCHEMA}.entries (
  seq bigint generated always as identity primary key,
  project_key text not null,
  session_id text not null,
  -- '' is the main transcript; otherwise e.g. subagents/agent-<id>
  subpath text not null default '',
  uuid text,
  entry json not null,
  -- milliseconds since the epoch, alasio's clock, as the SDK's mtimes are
  mtime bigint not null,
  doc jsonb
);
-- Tables made before doc existed.
alter table ${SCHEMA}.entries add column if not exists doc jsonb;
create unique index if not exists entries_uuid
  on ${SCHEMA}.entries (project_key, session_id, subpath, uuid) where uuid is not null;
create index if not exists entries_key on ${SCHEMA}.entries (project_key, session_id, subpath, seq);
create index if not exists entries_session on ${SCHEMA}.entries (session_id);
create table if not exists ${SCHEMA}.summaries (
  project_key text not null,
  session_id text not null,
  mtime bigint not null,
  data json not null,
  primary key (project_key, session_id)
);

-- JSON text as jsonb, or null where Postgres will not take it, such as past
-- jsonb's size limit: a doc that cannot be made never fails its entry.
create or replace function ${SCHEMA}.as_doc(doc text) returns jsonb
language plpgsql immutable parallel safe as $$
begin
  return doc::jsonb;
exception when others then
  return null;
end $$;
`;

/** Rows one insert takes, so that a large append, an import's, is a few statements of bounded size. */
const ROWS_PER_INSERT = 5000;

const subpathOf = (key) => key.subpath ?? "";

/** Rows given their doc at a time, for rows stored before doc existed. */
const DOCS_PER_UPDATE = 500;

/**
 * Text as Postgres can hold it, in `text` or in `jsonb`: NUL and any
 * surrogate without its other half as U+FFFD.
 */
export function storable(text) {
  return text.replaceAll("\0", "\ufffd").toWellFormed();
}

/**
 * An entry as its doc: the same JSON, with every string storable (keys and
 * values alike), which is all that keeps jsonb from taking it. Text that only
 * writes about those escapes is left as it is.
 */
export function docOf(value) {
  if (typeof value === "string") return storable(value);
  if (Array.isArray(value)) return value.map(docOf);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([name, item]) => [docOf(name), docOf(item)]));
  }
  return value;
}

export class NeonSessionStore {
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

  /** Creates the tables if they are missing, and fills in any missing doc. Idempotent. */
  async ensureSchema() {
    await this.#pool.query(ddl(this.#schema));
    // Rows stored before doc existed, and any whose doc Postgres would not
    // take before, which it is offered once more.
    for (let after = 0; ; ) {
      const { rows } = await this.#pool.query(
        `select seq, entry from ${this.#schema}.entries where doc is null and seq > $1 order by seq limit $2`,
        [after, DOCS_PER_UPDATE],
      );
      if (rows.length === 0) return;
      await this.#pool.query(
        `update ${this.#schema}.entries as e set doc = ${this.#schema}.as_doc(d.doc)
         from unnest($1::bigint[], $2::text[]) as d(seq, doc) where e.seq = d.seq`,
        [rows.map((row) => row.seq), rows.map((row) => JSON.stringify(docOf(row.entry)))],
      );
      after = rows.at(-1).seq;
    }
  }

  async append(key, entries) {
    if (entries.length === 0) return;
    const mtime = Date.now();
    const client = await this.#pool.connect();
    try {
      await client.query("begin");
      const inserted = [];
      for (let start = 0; start < entries.length; start += ROWS_PER_INSERT) {
        const batch = entries.slice(start, start + ROWS_PER_INSERT);
        const { rows } = await client.query(
          `insert into ${this.#schema}.entries (project_key, session_id, subpath, uuid, entry, mtime, doc)
           select $1, $2, $3, u, e, $4, ${this.#schema}.as_doc(d)
           from unnest($5::text[], $6::json[], $7::text[]) with ordinality as t(u, e, d, n)
           order by n
           on conflict (project_key, session_id, subpath, uuid) where uuid is not null do nothing
           returning entry`,
          [
            key.projectKey,
            key.sessionId,
            subpathOf(key),
            mtime,
            batch.map((entry) => (typeof entry.uuid === "string" ? entry.uuid : null)),
            batch.map((entry) => JSON.stringify(entry)),
            batch.map((entry) => JSON.stringify(docOf(entry))),
          ],
        );
        inserted.push(...rows.map((row) => row.entry));
      }
      // The session's listing summary folds what is new, never what a retry
      // re-delivered. Subagent transcripts do not contribute to it.
      if (key.subpath === undefined && inserted.length > 0) {
        await client.query(
          `insert into ${this.#schema}.summaries (project_key, session_id, mtime, data)
           values ($1, $2, 0, '{}') on conflict do nothing`,
          [key.projectKey, key.sessionId],
        );
        const { rows } = await client.query(
          `select mtime, data from ${this.#schema}.summaries
           where project_key = $1 and session_id = $2 for update`,
          [key.projectKey, key.sessionId],
        );
        const previous =
          rows[0].mtime === "0"
            ? undefined
            : { sessionId: key.sessionId, mtime: Number(rows[0].mtime), data: rows[0].data };
        const next = foldSessionSummary(previous, key, inserted, { mtime });
        await client.query(
          `update ${this.#schema}.summaries set mtime = $3, data = $4
           where project_key = $1 and session_id = $2`,
          [key.projectKey, key.sessionId, next.mtime, next.data],
        );
      }
      await client.query("commit");
    } catch (error) {
      await client.query("rollback").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async load(key) {
    const { rows } = await this.#pool.query(
      `select entry from ${this.#schema}.entries
       where project_key = $1 and session_id = $2 and subpath = $3 order by seq`,
      [key.projectKey, key.sessionId, subpathOf(key)],
    );
    return rows.length > 0 ? rows.map((row) => row.entry) : null;
  }

  async listSessions(projectKey) {
    const { rows } = await this.#pool.query(
      `select session_id, max(mtime) as mtime from ${this.#schema}.entries
       where project_key = $1 and subpath = ''
       group by session_id order by 2 desc`,
      [projectKey],
    );
    return rows.map((row) => ({ sessionId: row.session_id, mtime: Number(row.mtime) }));
  }

  async listSessionSummaries(projectKey) {
    const { rows } = await this.#pool.query(
      `select session_id, mtime, data from ${this.#schema}.summaries
       where project_key = $1 and mtime > 0 order by mtime desc`,
      [projectKey],
    );
    return rows.map((row) => ({ sessionId: row.session_id, mtime: Number(row.mtime), data: row.data }));
  }

  async delete(key) {
    if (key.subpath === undefined) {
      await this.#pool.query(
        `with gone as (delete from ${this.#schema}.entries where project_key = $1 and session_id = $2)
         delete from ${this.#schema}.summaries where project_key = $1 and session_id = $2`,
        [key.projectKey, key.sessionId],
      );
      return;
    }
    await this.#pool.query(
      `delete from ${this.#schema}.entries where project_key = $1 and session_id = $2 and subpath = $3`,
      [key.projectKey, key.sessionId, key.subpath],
    );
  }

  async listSubkeys(key) {
    const { rows } = await this.#pool.query(
      `select distinct subpath from ${this.#schema}.entries
       where project_key = $1 and session_id = $2 and subpath <> '' order by subpath`,
      [key.projectKey, key.sessionId],
    );
    return rows.map((row) => row.subpath);
  }

  /** The project a session's transcript is kept under, if it is here. */
  async projectKeyOf(sessionId) {
    const { rows } = await this.#pool.query(
      `select project_key from ${this.#schema}.entries where session_id = $1 and subpath = '' limit 1`,
      [sessionId],
    );
    return rows[0]?.project_key ?? null;
  }

  /** The uuids of a transcript's entries that have one. */
  async uuidsOf(key) {
    const { rows } = await this.#pool.query(
      `select uuid from ${this.#schema}.entries
       where project_key = $1 and session_id = $2 and subpath = $3 and uuid is not null`,
      [key.projectKey, key.sessionId, subpathOf(key)],
    );
    return new Set(rows.map((row) => row.uuid));
  }

  /** A transcript's entries that have no uuid, in order. */
  async uuidlessEntriesOf(key) {
    const { rows } = await this.#pool.query(
      `select entry from ${this.#schema}.entries
       where project_key = $1 and session_id = $2 and subpath = $3 and uuid is null order by seq`,
      [key.projectKey, key.sessionId, subpathOf(key)],
    );
    return rows.map((row) => row.entry);
  }
}

/**
 * The store as a query sees it: every write mirrored, but nothing to resume
 * from. On `resume`, the SDK asks `load` first; answering null is the SDK's
 * documented path for running under the real Claude home and resuming the
 * local transcript, with the operator's skills, memory, and settings, rather
 * than in a throwaway directory built from the store. transcripts.js makes
 * sure that local transcript exists before any resume.
 */
export function mirrorOnly(store) {
  return {
    append: (key, entries) => store.append(key, entries),
    load: async () => null,
  };
}
