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

// Each entry, and each content block, is parsed once, its fields taken in one call:
// DuckDB keeps what each JSON function parses until a whole vector of rows is done, so
// a field taken a call at a time parses the entry again for each, and a batch of 1000
// entries of 11 kB (11 MB) took more than the loader's gigabyte.
const ENTRY_FIELDS = [
  "type", "timestamp", "parentUuid", "logicalParentUuid", "isSidechain", "isMeta", "userType", "entrypoint",
  "cwd", "gitBranch", "version", "agentId", "promptId", "requestId",
  "message.id", "message.role", "message.model", "message.stop_reason",
  "message.usage.input_tokens", "message.usage.output_tokens",
  "message.usage.cache_read_input_tokens", "message.usage.cache_creation_input_tokens",
  "message.usage.output_tokens_details.thinking_tokens", "message.usage.service_tier",
];

/** The fields of the JSON `json` names that `fields` name, as text, a list in their order, from one parse. */
const fieldsOf = (json: string, fields: readonly string[]) => `json_extract_string(${json}, [${fields.map((field) => `'$.${field}'`).join(", ")}])`;

/** The field `name` of ENTRY_FIELDS, as text, from the list fieldsOf took. */
const field = (name: string) => `field[${ENTRY_FIELDS.indexOf(name) + 1}]`;

const INSERT_ENTRIES = `
  insert into ${LAKE}.claude.entries
  select seq, project_key, session_id, subpath, uuid, make_timestamptz(mtime * 1000),
    ${field("type")}, try_cast(${field("timestamp")} as TIMESTAMPTZ),
    ${field("parentUuid")}, ${field("logicalParentUuid")},
    try_cast(${field("isSidechain")} as BOOLEAN), try_cast(${field("isMeta")} as BOOLEAN),
    ${["userType", "entrypoint", "cwd", "gitBranch", "version", "agentId", "promptId", "requestId"].map(field).join(", ")},
    ${["message.id", "message.role", "message.model", "message.stop_reason"].map(field).join(", ")},
    ${["input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens_details.thinking_tokens"]
      .map((name) => `try_cast(${field(`message.usage.${name}`)} as BIGINT)`).join(", ")},
    ${field("message.usage.service_tier")},
    entry, malformed
  from (select *, ${fieldsOf("entry", ENTRY_FIELDS)} as field from claude_batch)`;

// A message's content is a string (a typed prompt) or a list of blocks; a string is
// one text block. A tool result's content is a string or a list of blocks of its own.
const INSERT_BLOCKS = `
  insert into ${LAKE}.claude.content_blocks
  with messages as (
    select seq, session_id, subpath, try_cast(field[2]->>'$' as TIMESTAMPTZ) as occurred_at,
      field[3]->>'$' as role, field[4] as content
    from (select *, json_extract(entry, ['$.type', '$.timestamp', '$.message.role', '$.message.content']) as field from claude_batch)
    where field[1]->>'$' in ('user', 'assistant')
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
  ),
  parts as (
    select *, ${fieldsOf("block", ["type", "text", "thinking", "content", "id", "tool_use_id", "name", "is_error"])} as part,
      json_extract(block, ['$.content', '$.input']) as nested
    from blocks
  )
  select seq, session_id, subpath, occurred_at, block_index, role, part[1],
    case part[1]
      when 'text' then length(part[2])
      when 'thinking' then length(part[3])
      when 'tool_result' then case json_type(nested[1])
        when 'VARCHAR' then length(part[4])
        when 'ARRAY' then list_sum([coalesce(length(item->>'text'), 0) for item in from_json(nested[1], '["json"]')])
      end
    end,
    case part[1] when 'tool_use' then part[5] when 'tool_result' then part[6] end,
    case part[1] when 'tool_use' then part[7] end,
    case part[1] when 'tool_use' then nested[2] end,
    case part[1] when 'tool_result' then coalesce(try_cast(part[8] as BOOLEAN), false) end
  from parts`;

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
