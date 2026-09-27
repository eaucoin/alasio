/**
 * Where transcript search lives: beside the session store's entries, in the
 * same schema, and written only by the indexer and embedder here, never on
 * the SDK's path.
 *
 * - passages: each distinct text once, with its words (`tsvector`: English
 *   for prose, as written otherwise) and its trigrams indexed;
 * - occurrences: every place a passage is written, with the entry's session,
 *   time, and kind, going with the entry when the SDK deletes it;
 * - indexed: the entries the indexer has read, whatever they held;
 * - embeddings: pgrag's bge-small-en-v1.5 embedding of a passage, where it
 *   was asked for one, indexed for nearest neighbours;
 * - search(): the one way in, from any SQL client.
 */

/** Kinds embedded for search by meaning: the conversation, and the calls it made. */
export const EMBEDDED_KINDS = [
  "user.text",
  "assistant.text",
  "assistant.thinking",
  "assistant.tool_use",
  "queue-operation",
  "summary",
  "ai-title",
  "custom-title",
  "last-prompt",
];

export const searchDdl = (SCHEMA) => `
create extension if not exists pg_trgm;
create extension if not exists vector;
-- pgrag's models are Neon's; elsewhere, search goes on by words alone.
do $$ begin
  create extension if not exists rag_bge_small_en_v15;
  create extension if not exists rag_jina_reranker_v1_tiny_en;
exception when others then
  null;
end $$;

create table if not exists ${SCHEMA}.passages (
  id bigint generated always as identity primary key,
  digest text not null unique,
  prose boolean not null,
  text text not null,
  words tsvector generated always as (
    to_tsvector(case when prose then 'english'::regconfig else 'simple'::regconfig end, text)
  ) stored
);
create index if not exists passages_words on ${SCHEMA}.passages using gin (words);
create index if not exists passages_trigrams on ${SCHEMA}.passages using gin (text gin_trgm_ops);

create table if not exists ${SCHEMA}.occurrences (
  entry_seq bigint not null references ${SCHEMA}.entries (seq) on delete cascade,
  part integer not null,
  passage_id bigint not null references ${SCHEMA}.passages (id),
  kind text not null,
  session_id text not null,
  subpath text not null,
  at timestamptz,
  primary key (entry_seq, part)
);
create index if not exists occurrences_passage on ${SCHEMA}.occurrences (passage_id);
create index if not exists occurrences_session on ${SCHEMA}.occurrences (session_id, at);

create table if not exists ${SCHEMA}.indexed (
  seq bigint primary key references ${SCHEMA}.entries (seq) on delete cascade
);

create table if not exists ${SCHEMA}.embeddings (
  passage_id bigint primary key references ${SCHEMA}.passages (id) on delete cascade,
  -- null where the model could not embed the passage, which is then not asked again
  embedding vector(384)
);
create index if not exists embeddings_nearest on ${SCHEMA}.embeddings using hnsw (embedding vector_cosine_ops);

-- How far the indexer has read: every entry up to this seq is indexed.
create table if not exists ${SCHEMA}.search_state (
  name text primary key,
  value bigint not null
);

-- How much a kind counts in results: the conversation above the tools, the
-- tools above the harness's own text.
create or replace function ${SCHEMA}.kind_weight(kind text) returns double precision
language sql immutable parallel safe as $$
  select case
    when kind in ('user.text', 'assistant.text') then 1.0
    when kind in ('queue-operation', 'last-prompt', 'summary', 'ai-title', 'custom-title') then 0.9
    when kind = 'assistant.thinking' then 0.8
    when kind = 'assistant.tool_use' then 0.7
    when kind = 'user.tool_result' then 0.6
    when kind = 'attachment' then 0.4
    else 0.5
  end
$$;

-- The query's embedding, or null where pgrag's model is not to be had.
create or replace function ${SCHEMA}.query_meaning(query text) returns vector
language plpgsql volatile as $$
begin
  return rag_bge_small_en_v15.embedding_for_query(query);
exception when others then
  return null;
end $$;

-- The reranker's distance from the query to each text, or null where it is not to be had.
create or replace function ${SCHEMA}.rerank_distances(query text, texts text[]) returns real[]
language plpgsql volatile as $$
begin
  return rag_jina_reranker_v1_tiny_en.rerank_distance(query, texts);
exception when others then
  return null;
end $$;

-- Passages matching q, best first: by their words, by their trigrams (typos
-- and substrings), and by meaning where embeddings are to be had, fused by
-- reciprocal rank and weighted by kind; with rerank, reordered by the
-- reranker. Each passage is reported once, at its most telling occurrence the
-- filters allow, with how often it occurs. Scores order results, nothing more.
create or replace function ${SCHEMA}.search(
  q text,
  only_kinds text[] default null,
  only_sessions text[] default null,
  since timestamptz default null,
  until timestamptz default null,
  max_results integer default 20,
  rerank boolean default false
) returns table (
  passage_id bigint,
  kind text,
  session_id text,
  subpath text,
  at timestamptz,
  occurrences bigint,
  score double precision,
  snippet text
)
language sql volatile as $$
  with
  allowed as not materialized (
    select o.* from ${SCHEMA}.occurrences o
    where (only_kinds is null or o.kind = any (only_kinds))
      and (only_sessions is null or o.session_id = any (only_sessions))
      and (since is null or o.at >= since)
      and (until is null or o.at < until)
  ),
  words as materialized (
    select websearch_to_tsquery('english', q) || websearch_to_tsquery('simple', q) as query
  ),
  meaning as materialized (
    select ${SCHEMA}.query_meaning(q) as v
  ),
  by_words as (
    select p.id, row_number() over (order by ts_rank_cd(p.words, w.query) desc) as rank
    from ${SCHEMA}.passages p, words w
    where p.words @@ w.query and exists (select 1 from allowed a where a.passage_id = p.id)
    order by rank limit 60
  ),
  by_trigrams as (
    select p.id, row_number() over (order by word_similarity(q, p.text) desc) as rank
    from ${SCHEMA}.passages p
    where length(q) >= 3 and q <% p.text and exists (select 1 from allowed a where a.passage_id = p.id)
    order by rank limit 60
  ),
  nearest as (
    select e.passage_id, e.embedding <=> (select v from meaning) as distance
    from ${SCHEMA}.embeddings e
    where (select v from meaning) is not null and e.embedding is not null
    order by e.embedding <=> (select v from meaning)
    limit 200
  ),
  by_meaning as (
    select n.passage_id as id, row_number() over (order by n.distance) as rank
    from nearest n
    where exists (select 1 from allowed a where a.passage_id = n.passage_id)
    order by rank limit 60
  ),
  fused as (
    select r.id, sum(1.0 / (60 + r.rank)) as rrf
    from (
      select id, rank from by_words
      union all select id, rank from by_trigrams
      union all select id, rank from by_meaning
    ) r
    group by r.id
  ),
  best as (
    select distinct on (a.passage_id)
      a.passage_id, a.kind, a.session_id, a.subpath, a.at,
      count(*) over (partition by a.passage_id) as occurrences
    from allowed a
    join fused f on f.id = a.passage_id
    order by a.passage_id, ${SCHEMA}.kind_weight(a.kind) desc, a.at desc nulls last
  ),
  ranked as (
    select b.*, f.rrf * ${SCHEMA}.kind_weight(b.kind) as fused_score, p.text, p.prose
    from fused f
    join best b on b.passage_id = f.id
    join ${SCHEMA}.passages p on p.id = f.id
    order by fused_score desc
    limit case when rerank then greatest(max_results, 30) else max_results end
  ),
  reranking as (
    select
      case when rerank then ${SCHEMA}.rerank_distances(q, array_agg(r.text order by r.fused_score desc)) end as distances,
      array_agg(r.passage_id order by r.fused_score desc) as ids
    from ranked r
  )
  select
    r.passage_id, r.kind, r.session_id, r.subpath, r.at, r.occurrences,
    coalesce(-x.distances[array_position(x.ids, r.passage_id)], r.fused_score) as score,
    ts_headline(
      case when r.prose then 'english'::regconfig else 'simple'::regconfig end,
      r.text, w.query, 'MaxWords=35, MinWords=15, MaxFragments=2'
    ) as snippet
  from ranked r, reranking x, words w
  order by score desc
  limit max_results
$$;
`;

/** Creates search's tables, indexes, and functions if they are missing. Idempotent. */
export async function ensureSearchSchema(pool, schema) {
  await pool.query(searchDdl(schema));
}
