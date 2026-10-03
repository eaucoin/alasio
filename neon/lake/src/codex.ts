// @ts-nocheck
/**
 * Loads Codex's rollout files (codex_sessions and codex_sessionfs_sessions,
 * src/codex/rollouts/store.ts) into the lake, a row per line.
 *
 * The source keeps each file's bytes as written, with its size and the digest of
 * its first line. A file the lake has not seen, or whose first line changed (Codex
 * rewrote it), or that shrank, is loaded whole again; one that only grew has just
 * its new lines loaded; one gone from the source goes from the lake. Only complete
 * lines are loaded: a line still being written is loaded once its newline lands.
 * Each file's load is one transaction, its lines and its record together.
 */
import { LAKE, SOURCE, rows, transaction } from "./lake.ts";

/** Each Codex home the source mirrors, by the schema it is kept in. */
export const HOMES = [
  { home: "folder", schema: "codex_sessions" },
  { home: "sessionfs", schema: "codex_sessionfs_sessions" },
];

const NEWLINE = 0x0a;

const INSERT_LINES = `
  insert into ${LAKE}.codex.lines
  select $1, $2, $3, $4, line_number, byte_offset,
    try_cast(record->>'timestamp' as TIMESTAMPTZ), try_cast(record->>'ordinal' as BIGINT),
    record->>'type', record->'payload'->>'type',
    coalesce(record->'payload'->>'turn_id', record->'payload'->'internal_chat_message_metadata_passthrough'->>'turn_id'),
    record, malformed
  from (
    select line_number, byte_offset,
      -- A line that is not JSON is kept as a JSON string, so the whole file is still there.
      case when json_valid(text) then text::JSON else to_json(text) end as record,
      not json_valid(text) as malformed
    from codex_batch)`;

async function sourceHas(db, schema) {
  const found = await rows(
    db,
    `select 1 from duckdb_tables() where database_name = '${SOURCE}' and schema_name = $1 and table_name in ('rollouts', 'rollout_chunks')`,
    [schema],
  );
  return found.length === 2;
}

/**
 * Stages a file's complete lines from byte `from` into the temp table codex_batch,
 * reading its chunks in order so no more than one is held at a time. Returns
 * `{ lines, loadedBytes }`: how many lines, and the offset just past the last.
 */
async function stageLines(db, schema, name, { from, firstLine }) {
  await db.run(`create or replace temp table codex_batch (line_number BIGINT, byte_offset BIGINT, text VARCHAR)`);
  const appender = await db.createAppender("codex_batch", "main", "temp");
  const chunks = await rows(
    db,
    `select start, octet_length(bytes) as length from ${SOURCE}.${schema}.rollout_chunks where name = $1 order by start`,
    [name],
  );
  let carry = Buffer.alloc(0);
  let carryOffset = from;
  let lines = 0;
  try {
    for (const chunk of chunks) {
      const start = Number(chunk.start);
      const end = start + Number(chunk.length);
      if (end <= from) continue;
      const [{ bytes }] = await rows(db, `select bytes from ${SOURCE}.${schema}.rollout_chunks where name = $1 and start = $2`, [name, chunk.start]);
      const piece = Buffer.from(bytes).subarray(Math.max(0, from - start));
      const buffer = carry.length ? Buffer.concat([carry, piece]) : piece;
      let lineStart = 0;
      for (let newline = buffer.indexOf(NEWLINE); newline >= 0; newline = buffer.indexOf(NEWLINE, lineStart)) {
        appender.appendBigInt(BigInt(firstLine + lines));
        appender.appendBigInt(BigInt(carryOffset + lineStart));
        appender.appendVarchar(buffer.toString("utf8", lineStart, newline));
        appender.endRow();
        lines += 1;
        lineStart = newline + 1;
      }
      carry = buffer.subarray(lineStart);
      carryOffset += lineStart;
    }
  } finally {
    appender.closeSync();
  }
  return { lines, loadedBytes: carryOffset };
}

/** Replaces the lake's record of a file. */
async function recordFile(db, home, file, { loadedBytes, lines }) {
  await db.run(`delete from ${LAKE}.codex.files where home = $1 and name = $2`, [home, file.name]);
  await db.run(
    `insert into ${LAKE}.codex.files values ($1, $2, $3, $4, $5, $6, $7, $8, make_timestamptz($9::BIGINT * 1000), $10, $11)`,
    [home, file.name, file.path, file.thread_id, file.rollout_id, file.history_base, file.size, file.head_digest, file.modified_ms, BigInt(loadedBytes), BigInt(lines)],
  );
}

/**
 * Brings the lake's rollout lines in line with the source's, for every home the
 * source has. `beforeCommit`, for tests, runs inside each file's transaction just
 * before it commits. Returns `{ files, inserted, deleted }`: files loaded, and lines.
 */
export async function syncCodex(db, { beforeCommit = async () => {} } = {}) {
  const totals = { files: 0, inserted: 0, deleted: 0 };
  for (const { home, schema } of HOMES) {
    if (!(await sourceHas(db, schema))) continue;
    const source = await rows(db, `select name, path, thread_id, rollout_id, history_base, size, head_digest, modified_ms from ${SOURCE}.${schema}.rollouts`);
    const kept = new Map(
      (await rows(db, `select name, size, head_digest, loaded_bytes, lines, path, modified_at from ${LAKE}.codex.files where home = $1`, [home]))
        .map((file) => [file.name, file]),
    );
    const names = new Set(source.map((file) => file.name));

    for (const [name, file] of kept) {
      if (names.has(name)) continue;
      await transaction(db, async () => {
        await db.run(`delete from ${LAKE}.codex.lines where home = $1 and name = $2`, [home, name]);
        await db.run(`delete from ${LAKE}.codex.files where home = $1 and name = $2`, [home, name]);
        await beforeCommit();
      });
      totals.deleted += Number(file.lines);
    }

    for (const file of source) {
      const known = kept.get(file.name);
      const rewritten = !known || known.head_digest !== file.head_digest || file.size < known.loaded_bytes;
      if (!rewritten && file.size === known.size) {
        // Archiving moves a file without changing it; its record follows.
        if (file.path !== known.path) {
          await transaction(db, () => db.run(`update ${LAKE}.codex.files set path = $3 where home = $1 and name = $2`, [home, file.name, file.path]));
        }
        continue;
      }
      const from = rewritten ? 0 : Number(known.loaded_bytes);
      const firstLine = rewritten ? 0 : Number(known.lines);
      const staged = await stageLines(db, schema, file.name, { from, firstLine });
      await transaction(db, async () => {
        if (rewritten && known) await db.run(`delete from ${LAKE}.codex.lines where home = $1 and name = $2`, [home, file.name]);
        await db.run(INSERT_LINES, [home, file.name, file.thread_id, file.rollout_id]);
        await recordFile(db, home, file, { loadedBytes: staged.loadedBytes, lines: firstLine + staged.lines });
        await beforeCommit();
      });
      totals.files += 1;
      totals.inserted += staged.lines;
      if (rewritten && known) totals.deleted += Number(known.lines);
    }
  }
  await db.run("drop table if exists codex_batch");
  return totals;
}
