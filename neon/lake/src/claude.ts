/**
 * Loads Claude Code's transcript entries (claude_sessions.entries, src/harness/
 * claude/session-store.ts) into the lake: every entry the source holds and the
 * lake does not, and out of the lake every entry the source no longer holds.
 *
 * Entries are told apart by `seq`, which the source assigns and never reuses.
 * Appends can commit out of seq order, so the lake compares the sets of seqs
 * rather than keep a high-water mark; seqs are all that is read until an entry is
 * known to be missing. Each batch, the entries and their content blocks, is one
 * transaction, so a load cut short leaves whole entries and loads the rest next
 * time.
 */
import type { DuckDBConnection } from "@duckdb/node-api";

import { LAKE, SOURCE, rows, transaction } from "./lake.ts";

export interface ClaudeSyncOptions {
  /** Entries loaded per transaction. */
  batchEntries?: number;
  /** For tests: runs inside each batch's transaction just before it commits. */
  beforeCommit?: () => Promise<void>;
}

/** What a load changed: entries inserted into and deleted from the lake. */
export interface ClaudeLoad {
  inserted: number;
  deleted: number;
}

const SOURCE_TABLE = `${SOURCE}.claude_sessions.entries`;

/** Entries loaded per transaction, which bounds a batch's memory. */
export const BATCH_ENTRIES = 1000;

const usage = (field: string) => `try_cast(entry->'message'->'usage'->>'${field}' as BIGINT)`;

const INSERT_ENTRIES = `
  insert into ${LAKE}.claude.entries
  select seq, project_key, session_id, subpath, uuid, make_timestamptz(mtime * 1000),
    entry->>'type', try_cast(entry->>'timestamp' as TIMESTAMPTZ),
    entry->>'parentUuid', entry->>'logicalParentUuid',
    try_cast(entry->>'isSidechain' as BOOLEAN), try_cast(entry->>'isMeta' as BOOLEAN),
    entry->>'userType', entry->>'entrypoint', entry->>'cwd', entry->>'gitBranch', entry->>'version',
    entry->>'agentId', entry->>'promptId', entry->>'requestId',
    entry->'message'->>'id', entry->'message'->>'role', entry->'message'->>'model', entry->'message'->>'stop_reason',
    ${usage("input_tokens")}, ${usage("output_tokens")},
    ${usage("cache_read_input_tokens")}, ${usage("cache_creation_input_tokens")},
    try_cast(entry->'message'->'usage'->'output_tokens_details'->>'thinking_tokens' as BIGINT),
    entry->'message'->'usage'->>'service_tier',
    entry, malformed
  from claude_batch`;

// A message's content is a string (a typed prompt) or a list of blocks; a string is
// one text block. A tool result's content is a string or a list of blocks of its own.
const INSERT_BLOCKS = `
  insert into ${LAKE}.claude.content_blocks
  with messages as (
    select seq, session_id, subpath, try_cast(entry->>'timestamp' as TIMESTAMPTZ) as occurred_at,
      entry->'message'->>'role' as role, entry->'message'->'content' as content
    from claude_batch
    where entry->>'type' in ('user', 'assistant')
  ),
  listed as (
    select *, case json_type(content)
        when 'ARRAY' then from_json(content, '["json"]')
        when 'VARCHAR' then [json_object('type', 'text', 'text', content->>'$')]
      end as blocks
    from messages
  ),
  blocks as (
    select seq, session_id, subpath, occurred_at, role,
      unnest(range(len(blocks)))::INTEGER as block_index, unnest(blocks) as block
    from listed
    where blocks is not null
  )
  select seq, session_id, subpath, occurred_at, block_index, role, block->>'type',
    case block->>'type'
      when 'text' then length(block->>'text')
      when 'thinking' then length(block->>'thinking')
      when 'tool_result' then case json_type(block->'content')
        when 'VARCHAR' then length(block->>'content')
        when 'ARRAY' then list_sum([coalesce(length(part->>'text'), 0) for part in from_json(block->'content', '["json"]')])
      end
    end,
    case block->>'type' when 'tool_use' then block->>'id' when 'tool_result' then block->>'tool_use_id' end,
    case block->>'type' when 'tool_use' then block->>'name' end,
    case block->>'type' when 'tool_use' then block->'input' end,
    case block->>'type' when 'tool_result' then coalesce(try_cast(block->>'is_error' as BOOLEAN), false) end
  from blocks`;

/**
 * Brings the lake's entries in line with the source's. `beforeCommit`, for tests,
 * runs inside each batch's transaction just before it commits. Returns
 * `{ inserted, deleted }`.
 */
export async function syncClaude(
  db: DuckDBConnection,
  { batchEntries = BATCH_ENTRIES, beforeCommit = async () => {} }: ClaudeSyncOptions = {},
): Promise<ClaudeLoad> {
  await db.run(`create or replace temp table claude_source_seq as select seq from ${SOURCE_TABLE}`);
  await db.run(`create or replace temp table claude_lake_seq as select seq from ${LAKE}.claude.entries`);
  await db.run(`create or replace temp table claude_gone as select seq from claude_lake_seq except select seq from claude_source_seq`);
  await db.run(`create or replace temp table claude_missing as select seq from claude_source_seq except select seq from claude_lake_seq`);

  // A count is one row.
  const { gone } = (await rows<{ gone: bigint }>(db, "select count(*) as gone from claude_gone"))[0]!;
  if (gone > 0n) {
    await transaction(db, async () => {
      await db.run(`delete from ${LAKE}.claude.content_blocks where seq in (select seq from claude_gone)`);
      await db.run(`delete from ${LAKE}.claude.entries where seq in (select seq from claude_gone)`);
      await beforeCommit();
    });
  }

  const missing = (await rows<{ seq: bigint }>(db, "select seq from claude_missing order by seq")).map((row) => row.seq);
  for (let at = 0; at < missing.length; at += batchEntries) {
    const batch = missing.slice(at, at + batchEntries);
    // The range is pushed down to Postgres; the membership test keeps what is missing.
    await db.run(
      `create or replace temp table claude_batch as
        select seq, project_key, session_id, subpath, uuid, mtime,
          -- Postgres's json takes text DuckDB's JSON does not (a lone surrogate escaped,
          -- say); such an entry is kept as a JSON string rather than stop the load.
          case when json_valid(text) then text::JSON else to_json(text) end as entry,
          not json_valid(text) as malformed
        from (select seq, project_key, session_id, subpath, uuid, mtime, entry::VARCHAR as text
              from ${SOURCE_TABLE}
              where seq between $1 and $2 and seq in (select seq from claude_missing))`,
      // A batch is never empty.
      [batch[0]!, batch.at(-1)!],
    );
    await transaction(db, async () => {
      await db.run(INSERT_ENTRIES);
      await db.run(INSERT_BLOCKS);
      await beforeCommit();
    });
  }
  await db.run("drop table if exists claude_batch");
  return { inserted: missing.length, deleted: Number(gone) };
}
