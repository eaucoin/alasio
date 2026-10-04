/**
 * The indexer: reads the session store's entries into passages and their
 * occurrences, behind the SDK and never on its path. Each entry is read once
 * and marked `indexed`, whatever it held, in the transaction that indexes it.
 *
 * Appends can commit out of seq order, so the indexer looks for entries not
 * yet marked rather than past a high-water mark. `settled_seq` in
 * search_state only spares it from looking at old ones: every entry up to it
 * is indexed, and it moves only past entries older than SETTLE_MS, by which
 * time any append that took a lower seq has long committed.
 */
import type { SessionStoreEntry } from "@anthropic-ai/claude-agent-sdk";
import type { Pool, PoolClient } from "pg";

import { passagesOf, type Passage } from "./passages.ts";

/** Entries read into passages in one transaction. */
const ENTRIES_PER_BATCH = 200;

/** How old an entry must be before the settled mark moves past it. */
export const SETTLE_MS = 10 * 60 * 1000;

const SETTLED = "settled_seq";

/** Where a passage is written: an entry's part, with the entry's session and time. */
interface Occurrence {
  readonly seq: string;
  readonly part: number;
  readonly digest: string;
  readonly kind: string;
  readonly session: string;
  readonly subpath: string;
  readonly at: string | null;
}

async function settledSeq(client: PoolClient, schema: string): Promise<string> {
  const { rows } = await client.query<{ value: string }>(`select value from ${schema}.search_state where name = $1`, [SETTLED]);
  return rows[0]?.value ?? "0";
}

/** An entry's time, where it has a valid one. */
function timeOf(entry: SessionStoreEntry): string | null {
  return typeof entry.timestamp === "string" && !Number.isNaN(Date.parse(entry.timestamp)) ? entry.timestamp : null;
}

/** Indexes the next entries not yet indexed. Returns how many it read. */
export async function indexBatch(pool: Pool, schema: string, { limit = ENTRIES_PER_BATCH }: { readonly limit?: number } = {}): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    // Shared locks keep the SDK from deleting these entries until they are indexed.
    const { rows } = await client.query<{ seq: string; session_id: string; subpath: string; entry: SessionStoreEntry }>(
      `select e.seq, e.session_id, e.subpath, e.entry from ${schema}.entries e
       where e.seq > $1 and not exists (select 1 from ${schema}.indexed i where i.seq = e.seq)
       order by e.seq limit $2
       for share of e`,
      [await settledSeq(client, schema), limit],
    );
    if (rows.length === 0) {
      await client.query("commit");
      return 0;
    }

    const texts = new Map<string, Passage>();
    const occurrences: Occurrence[] = [];
    for (const row of rows) {
      const at = timeOf(row.entry);
      for (const passage of passagesOf(row.entry)) {
        if (!texts.has(passage.digest)) texts.set(passage.digest, passage);
        occurrences.push({ seq: row.seq, part: passage.part, digest: passage.digest, kind: passage.kind, session: row.session_id, subpath: row.subpath, at });
      }
    }
    const distinct = [...texts.values()];
    await client.query(
      `insert into ${schema}.passages (digest, prose, text)
       select * from unnest($1::text[], $2::boolean[], $3::text[])
       on conflict (digest) do nothing`,
      [distinct.map((p) => p.digest), distinct.map((p) => p.prose), distinct.map((p) => p.text)],
    );
    const ids = new Map(
      (await client.query<{ id: string; digest: string }>(`select id, digest from ${schema}.passages where digest = any ($1::text[])`, [[...texts.keys()]])).rows.map((row) => [row.digest, row.id]),
    );
    await client.query(
      `insert into ${schema}.occurrences (entry_seq, part, passage_id, kind, session_id, subpath, at)
       select * from unnest($1::bigint[], $2::integer[], $3::bigint[], $4::text[], $5::text[], $6::text[], $7::timestamptz[])
       on conflict do nothing`,
      [
        occurrences.map((o) => o.seq),
        occurrences.map((o) => o.part),
        occurrences.map((o) => ids.get(o.digest)),
        occurrences.map((o) => o.kind),
        occurrences.map((o) => o.session),
        occurrences.map((o) => o.subpath),
        occurrences.map((o) => o.at),
      ],
    );
    await client.query(`insert into ${schema}.indexed (seq) select unnest($1::bigint[]) on conflict do nothing`, [rows.map((row) => row.seq)]);
    await client.query("commit");
    return rows.length;
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Moves the settled mark as far as it may go: past entries that are indexed
 * and older than SETTLE_MS, up to the first entry that is not indexed.
 */
export async function settle(pool: Pool, schema: string, { now = Date.now() }: { readonly now?: number } = {}): Promise<void> {
  await pool.query(
    `insert into ${schema}.search_state (name, value)
     select $1, coalesce(max(e.seq), s.settled)
     from (select coalesce((select value from ${schema}.search_state where name = $1), 0) as settled) s
     left join ${schema}.entries e
       on e.seq > s.settled
       and e.mtime < $2
       and e.seq < coalesce(
         (select min(u.seq) from ${schema}.entries u
          where u.seq > s.settled and not exists (select 1 from ${schema}.indexed i where i.seq = u.seq)),
         9223372036854775807)
     group by s.settled
     on conflict (name) do update set value = excluded.value`,
    [SETTLED, now - SETTLE_MS],
  );
}

/** Drops passages no entry holds any more, as when the SDK deletes a session. Returns how many. */
export async function collectOrphans(pool: Pool, schema: string): Promise<number> {
  const { rowCount } = await pool.query(
    `delete from ${schema}.passages p where not exists (select 1 from ${schema}.occurrences o where o.passage_id = p.id)`,
  );
  // A delete always reports its count.
  return rowCount ?? 0;
}
