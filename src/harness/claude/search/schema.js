/**
 * Where transcript search lives: beside the session store's entries, in the
 * same schema, and written only by the indexer here, never on the SDK's path.
 *
 * - passages: each distinct text once, with its words (`tsvector`: English
 *   for prose, as written otherwise) and its trigrams indexed;
 * - occurrences: every place a passage is written, with the entry's session,
 *   time, and kind, going with the entry when the SDK deletes it;
 * - indexed: the entries the indexer has read, whatever they held;
 * - search(): the one way in, from any SQL client.
 */

export const searchDdl = (SCHEMA) => `
create extension if not exists pg_trgm;

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

-- Passages matching q, best first: by their words and by their trigrams
-- (typos and substrings), fused by reciprocal rank and weighted by kind. Each
-- passage is reported once, at its most telling occurrence the filters allow,
-- with how often it occurs. Scores order results, nothing more.
create or replace function ${SCHEMA}.search(
  q text,
  only_kinds text[] default null,
  only_sessions text[] default null,
  since timestamptz default null,
  until timestamptz default null,
  max_results integer default 20
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
language sql stable as $$
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
  fused as (
    select r.id, sum(1.0 / (60 + r.rank)) as rrf
    from (select id, rank from by_words union all select id, rank from by_trigrams) r
    group by r.id
  ),
  best as (
    select distinct on (a.passage_id)
      a.passage_id, a.kind, a.session_id, a.subpath, a.at,
      count(*) over (partition by a.passage_id) as occurrences
    from allowed a
    join fused f on f.id = a.passage_id
    order by a.passage_id, ${SCHEMA}.kind_weight(a.kind) desc, a.at desc nulls last
  )
  select
    b.passage_id, b.kind, b.session_id, b.subpath, b.at, b.occurrences,
    f.rrf * ${SCHEMA}.kind_weight(b.kind) as score,
    ts_headline(
      case when p.prose then 'english'::regconfig else 'simple'::regconfig end,
      p.text, w.query, 'MaxWords=35, MinWords=15, MaxFragments=2'
    ) as snippet
  from fused f
  join best b on b.passage_id = f.id
  join ${SCHEMA}.passages p on p.id = f.id
  cross join words w
  order by score desc
  limit max_results
$$;
`;

/** Creates search's tables, indexes, and functions if they are missing. Idempotent. */
export async function ensureSearchSchema(pool, schema) {
  await pool.query(searchDdl(schema));
}
