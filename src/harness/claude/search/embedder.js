/**
 * The embedder: gives passages of the kinds in EMBEDDED_KINDS their
 * embedding, from pgrag's bge-small-en-v1.5 in the compute, a batch at a time.
 *
 * Where the model is not to be had, it embeds nothing and waits: a passage is
 * only recorded as one the model cannot embed (a null embedding) when the
 * model embeds others and fails on it alone.
 */
import { EMBEDDED_KINDS } from "./schema.js";

/** Passages embedded in one statement: about a second of the model's time. */
export const PASSAGES_PER_BATCH = 16;

async function modelAvailable(pool, schema) {
  const { rows } = await pool.query(`select ${schema}.query_meaning('ready') is not null as available`);
  return rows[0].available;
}

async function embed(pool, schema, ids, texts) {
  await pool.query(
    `insert into ${schema}.embeddings (passage_id, embedding)
     select t.id, rag_bge_small_en_v15.embedding_for_passage(t.text)
     from unnest($1::bigint[], $2::text[]) as t(id, text)
     on conflict (passage_id) do nothing`,
    [ids, texts],
  );
}

/**
 * Embeds the next passages that want an embedding. Returns how many it dealt
 * with, embedded or recorded as unembeddable, or null where the model is not
 * to be had.
 */
export async function embedBatch(pool, schema, { limit = PASSAGES_PER_BATCH } = {}) {
  const { rows } = await pool.query(
    `select p.id, p.text from ${schema}.passages p
     where not exists (select 1 from ${schema}.embeddings e where e.passage_id = p.id)
       and exists (select 1 from ${schema}.occurrences o where o.passage_id = p.id and o.kind = any ($1::text[]))
     order by p.id limit $2`,
    [EMBEDDED_KINDS, limit],
  );
  if (rows.length === 0) return 0;
  if (!(await modelAvailable(pool, schema))) return null;
  try {
    await embed(pool, schema, rows.map((row) => row.id), rows.map((row) => row.text));
    return rows.length;
  } catch {
    // One passage the model cannot take fails the batch: embed them one by
    // one, and record as unembeddable only those that fail while the model
    // is still there.
    let handled = 0;
    for (const row of rows) {
      try {
        await embed(pool, schema, [row.id], [row.text]);
      } catch {
        if (!(await modelAvailable(pool, schema))) return handled > 0 ? handled : null;
        await pool.query(
          `insert into ${schema}.embeddings (passage_id, embedding) values ($1, null) on conflict (passage_id) do nothing`,
          [row.id],
        );
      }
      handled += 1;
    }
    return handled;
  }
}
