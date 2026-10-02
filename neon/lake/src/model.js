/**
 * The lake's model: what each table and view holds, and its version.
 *
 * Every Claude Code transcript entry and every Codex rollout line is a row, with
 * the fields worth querying as typed columns beside the whole of it kept as JSON,
 * so nothing is lost and anything not modelled yet is still there to query. The
 * whole is JSON rather than DuckDB's VARIANT because DuckLake's writer shreds a
 * VARIANT column into one Parquet column per path it meets, which for entries this
 * varied runs out of memory and past what a Postgres catalog can describe.
 *
 * The views shape the rows into what is analysed: a Claude message once, with its
 * final usage (Claude Code writes an entry per content block, each with the usage
 * so far), each tool call with its result, and Codex's turns, token usage, and tool
 * calls.
 *
 * The lake is derived: alasio's Neon is the source of truth. A model this version
 * does not match is dropped and loaded again from it (ensureModel).
 */
import { LAKE, rows } from "./lake.js";

/** Bumped whenever a table changes shape; the lake then rebuilds from its source. */
export const MODEL_VERSION = 1;

const TABLES = {
  "claude.entries": `
    seq BIGINT, project_key VARCHAR, session_id VARCHAR, subpath VARCHAR, uuid VARCHAR,
    stored_at TIMESTAMPTZ,
    type VARCHAR, occurred_at TIMESTAMPTZ, parent_uuid VARCHAR, logical_parent_uuid VARCHAR,
    is_sidechain BOOLEAN, is_meta BOOLEAN, user_type VARCHAR, entrypoint VARCHAR,
    cwd VARCHAR, git_branch VARCHAR, version VARCHAR, agent_id VARCHAR,
    prompt_id VARCHAR, request_id VARCHAR,
    message_id VARCHAR, role VARCHAR, model VARCHAR, stop_reason VARCHAR,
    input_tokens BIGINT, output_tokens BIGINT,
    cache_read_input_tokens BIGINT, cache_creation_input_tokens BIGINT,
    thinking_tokens BIGINT, service_tier VARCHAR,
    entry JSON, malformed BOOLEAN`,
  "claude.content_blocks": `
    seq BIGINT, session_id VARCHAR, subpath VARCHAR, occurred_at TIMESTAMPTZ,
    block_index INTEGER, role VARCHAR, block_type VARCHAR, text_chars BIGINT,
    tool_use_id VARCHAR, tool_name VARCHAR, tool_input JSON, is_error BOOLEAN`,
  "codex.files": `
    home VARCHAR, name VARCHAR, path VARCHAR, thread_id VARCHAR, rollout_id VARCHAR,
    history_base VARCHAR, size BIGINT, head_digest VARCHAR, modified_at TIMESTAMPTZ,
    loaded_bytes BIGINT, lines BIGINT`,
  "codex.lines": `
    home VARCHAR, name VARCHAR, thread_id VARCHAR, rollout_id VARCHAR,
    line_number BIGINT, byte_offset BIGINT, occurred_at TIMESTAMPTZ, ordinal BIGINT,
    type VARCHAR, payload_type VARCHAR, turn_id VARCHAR, record JSON, malformed BOOLEAN`,
  "loader.meta": `key VARCHAR, value VARCHAR`,
};

const VIEWS = {
  // One row per Claude message: the values its last entry carries, which hold its
  // final usage and stop reason.
  "claude.messages": `
    select message_id,
      arg_max(session_id, seq) as session_id, arg_max(subpath, seq) as subpath,
      arg_max(model, seq) as model, arg_max(stop_reason, seq) as stop_reason,
      arg_max(service_tier, seq) as service_tier,
      arg_max(input_tokens, seq) as input_tokens, arg_max(output_tokens, seq) as output_tokens,
      arg_max(cache_read_input_tokens, seq) as cache_read_input_tokens,
      arg_max(cache_creation_input_tokens, seq) as cache_creation_input_tokens,
      arg_max(thinking_tokens, seq) as thinking_tokens,
      min(occurred_at) as started_at, max(occurred_at) as finished_at, count(*) as entries
    from claude.entries
    where type = 'assistant' and message_id is not null
    group by message_id`,
  // Each tool call with its result, where it has one.
  "claude.tool_calls": `
    select call.session_id, call.subpath, call.tool_use_id, call.tool_name, call.tool_input,
      call.occurred_at as called_at, result.occurred_at as answered_at,
      result.is_error, result.text_chars as result_chars, call.seq as call_seq, result.seq as result_seq
    from claude.content_blocks call
    left join claude.content_blocks result
      on result.block_type = 'tool_result' and result.tool_use_id = call.tool_use_id
    where call.block_type = 'tool_use'`,
  "codex.turns": `
    select home, thread_id, turn_id,
      min(occurred_at) filter (where payload_type = 'task_started') as started_at,
      max(occurred_at) filter (where payload_type = 'task_complete') as completed_at,
      max(try_cast(record->'payload'->>'duration_ms' as BIGINT)) filter (where payload_type = 'task_complete') as duration_ms,
      max(try_cast(record->'payload'->>'time_to_first_token_ms' as BIGINT)) filter (where payload_type = 'task_complete') as time_to_first_token_ms,
      arg_max(record->'payload'->>'model', line_number) filter (where type = 'turn_context') as model,
      arg_max(record->'payload'->'collaboration_mode'->'settings'->>'reasoning_effort', line_number) filter (where type = 'turn_context') as reasoning_effort
    from codex.lines
    where turn_id is not null
    group by home, thread_id, turn_id`,
  // Codex records each response's usage once, in a token_usage_record line.
  "codex.token_usage": `
    select home, thread_id, turn_id, record->'payload'->>'response_id' as response_id, occurred_at,
      try_cast(record->'payload'->'usage'->>'input_tokens' as BIGINT) as input_tokens,
      try_cast(record->'payload'->'usage'->>'cached_input_tokens' as BIGINT) as cached_input_tokens,
      try_cast(record->'payload'->'usage'->>'cache_write_input_tokens' as BIGINT) as cache_write_input_tokens,
      try_cast(record->'payload'->'usage'->>'output_tokens' as BIGINT) as output_tokens,
      try_cast(record->'payload'->'usage'->>'reasoning_output_tokens' as BIGINT) as reasoning_output_tokens,
      try_cast(record->'payload'->'usage'->>'total_tokens' as BIGINT) as total_tokens
    from codex.lines
    where type = 'token_usage_record'`,
  // Function and custom tool calls, each with its output, by call id within a thread.
  "codex.tool_calls": `
    with calls as (
      select home, thread_id, turn_id, occurred_at, record->'payload'->>'call_id' as call_id,
        record->'payload'->>'name' as name, payload_type as kind,
        coalesce(record->'payload'->>'arguments', record->'payload'->>'input') as input
      from codex.lines
      where type = 'response_item' and payload_type in ('function_call', 'custom_tool_call')
    ),
    outputs as (
      select home, thread_id, occurred_at, record->'payload'->>'call_id' as call_id, record->'payload'->'output' as output
      from codex.lines
      where type = 'response_item' and payload_type in ('function_call_output', 'custom_tool_call_output')
    )
    select calls.home, calls.thread_id, calls.turn_id, calls.call_id, calls.kind, calls.name, calls.input,
      calls.occurred_at as called_at, outputs.occurred_at as answered_at, outputs.output
    from calls
    left join outputs using (home, thread_id, call_id)`,
};

const SCHEMAS = ["claude", "codex", "loader"];

async function create(db) {
  for (const schema of SCHEMAS) await db.run(`create schema if not exists ${LAKE}.${schema}`);
  for (const [table, columns] of Object.entries(TABLES)) {
    await db.run(`create table if not exists ${LAKE}.${table} (${columns})`);
  }
  // Views name their tables within the lake, which is where DuckDB binds them.
  await db.run(`use ${LAKE}`);
  for (const [view, query] of Object.entries(VIEWS)) {
    await db.run(`create or replace view ${LAKE}.${view} as ${query}`);
  }
  await db.run("use memory");
}

async function drop(db) {
  for (const view of Object.keys(VIEWS)) await db.run(`drop view if exists ${LAKE}.${view}`);
  for (const table of Object.keys(TABLES)) await db.run(`drop table if exists ${LAKE}.${table}`);
}

/** The model version the lake was built with, or null for a lake not yet built. */
export async function lakeModelVersion(db) {
  const [schema] = await rows(db, `select 1 from duckdb_tables() where database_name = '${LAKE}' and schema_name = 'loader' and table_name = 'meta'`);
  if (!schema) return null;
  const [row] = await rows(db, `select value from ${LAKE}.loader.meta where key = 'model_version'`);
  return row ? Number(row.value) : null;
}

/**
 * Makes the lake's tables and views. A lake built with another model version is
 * dropped first, to be loaded again from its source; `rebuild` drops it whatever its
 * version. Returns whether it was (re)built empty.
 */
export async function ensureModel(db, { rebuild = false } = {}) {
  const version = await lakeModelVersion(db);
  if (version === MODEL_VERSION && !rebuild) {
    await create(db);
    return false;
  }
  await db.run("begin transaction");
  try {
    await drop(db);
    await create(db);
    await db.run(`insert into ${LAKE}.loader.meta values ('model_version', '${MODEL_VERSION}')`);
    await db.run("commit");
  } catch (error) {
    await db.run("rollback").catch(() => {});
    throw error;
  }
  return true;
}
