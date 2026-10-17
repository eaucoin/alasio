import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { after, before, describe, test } from "node:test";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue } from "@duckdb/node-api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { ConfigProvider, Effect } from "effect";
import pg from "pg";

import { LOG_RECORD_BYTES } from "../cli/src/manifests/log.ts";
import { LAKE_READER_ROLE, LAKE_ROLE } from "../neon/control/secrets.ts";
import { branchesBesideMain } from "../neon/lake/src/branches.ts";
import { syncClaude } from "../neon/lake/src/claude.ts";
import { syncCodex } from "../neon/lake/src/codex.ts";
import { type EndpointConfig, type LakeConfig, loadConfig, loadEndpointConfig, loadMaintenanceConfig } from "../neon/lake/src/config.ts";
import { startEndpoint } from "../neon/lake/src/endpoint.ts";
import { EXTENSIONS } from "../neon/lake/src/extensions.ts";
import { BATCH, DUCKDB_MEMORY, type IntakeSource, type LogRecord, type PartitionAt, REREAD_SKEW_MS, startIntake, unreachable } from "../neon/lake/src/intake.ts";
import { conninfo, LAKE, type Lake, openLake, type Row, rows, takeLoaderLock, transaction, writeLock, WriteLockLost } from "../neon/lake/src/lake.ts";
import { gathering, type Read } from "../neon/lake/src/log.ts";
import { furthestWrite, type LoaderLake, type Log, lsnValue, openLoaderLake, readUpTo, sourceReplayed, startLoader, type WakeRecord, type WakeSource } from "../neon/lake/src/loader.ts";
import { type PassOptions, runPass } from "../neon/lake/src/maintain.ts";
import { createMetrics, sampleMemory } from "../neon/lake/src/metrics.ts";
import { lakeModelVersion, MODEL_VERSION, ROW_GROUP_ROWS } from "../neon/lake/src/model.ts";
import { askReread, ensureOtel, type Held, LakeSetBack, mergeTelemetry, type Position, readAgainFrom, rereadGaps, rereadsAsked, type TelemetryRows, telemetryRows, telemetryTime, writeConsumed } from "../neon/lake/src/otel.ts";
import { encodeRequest, type LogsRequest, type MetricsRequest, PART_ITEMS, REQUEST_ROW_BYTES, requestParts, requestRowBytes } from "../neon/lake/src/otlp.ts";
import { format } from "../neon/lake/src/query.ts";
import { answer, grantReads, openReader } from "../neon/lake/src/reader.ts";
import { readable, StoreUndecided, stored } from "../neon/lake/src/reconcile.ts";
import { type LakeSyncOptions, maintainLake, prepareLake, syncLake } from "../neon/lake/src/sync.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { ensureLakeReaderRole, ensureLakeRole, lakeEnabled, syncLakeReads } from "../src/neon/lake.ts";
import { wakeRecord } from "../src/neon/lake-wake.ts";
import { startFakeS3 } from "./support/fake-s3.ts";
import { startPostgres, type TestPostgres } from "./support/postgres.ts";

// The lake loads alasio's Neon into DuckLake. Here its source is a throwaway Postgres
// written through alasio's own stores, its catalog a database there owned by the lake's
// role, and its files a local directory; DuckDB is the real one the lake runs.

const LAKE_PASSWORD = "lake-test-password";
const READER_PASSWORD = "reader-test-password";
const QUERY_TOKEN = "query-test-token";
/** Where the loader would be woken from, which these tests give it themselves (wakeSource). */
const LOADER_LOG = { LOG_BROKERS: "log:9092", LOG_USERNAME: "loader-main", LOG_PASSWORD: "log-test-password", LAKE_WAKE_TOPIC: "lake.main", LAKE_WAKE_GROUP: "lake-loader.main" };

/** A record of the wake topic's, at `offset` of its one partition. */
const woken = (offset: bigint): Position => ({ topic: "lake.main", partition: 0, offset });

/** The record of the wake topic's at `position`, as alasio writes one, naming the primary's WAL position `lsn` where given. */
const recordAt = (position: Position, lsn: string | null = null): WakeRecord => ({ ...position, value: wakeRecord(lsn) });

/** A member of the loader's group given `batches` of records in turn, then none, which says what it was told; a record a position, naming no WAL position. */
function wakeSource(batches: (Position | WakeRecord)[][]): WakeSource & { readonly told: Position[][] } {
  const told: Position[][] = [];
  return {
    told,
    next: async ({ ms }) => {
      const batch = batches.shift();
      if (!batch) await new Promise((resolve) => setTimeout(resolve, ms));
      return (batch ?? []).map((record) => ("value" in record ? record : recordAt(record)));
    },
    commit: async (offsets) => void told.push([...offsets]),
    close: async () => {},
  };
}

/** The loader's lock as a lake given the loader in these tests takes it: none to wait for, its fence passing. */
const unlocked: LoaderLake["locked"] = (load) => load(async () => {});

/** Waits until `done` says so, for at most `ms`. */
async function eventually(done: () => boolean, ms = 30_000): Promise<void> {
  for (let waited = 0; !done() && waited < ms; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
}

// Set by the first hook.
let postgres: TestPostgres | undefined;
let admin: pg.Pool; // the source database, as its owner (alasio)
let store: NeonSessionStore;
let rollouts: NeonRolloutStore;
let dataDir: string | undefined;
let config: LakeConfig;
let readerConfig: EndpointConfig;

before(async () => {
  postgres = await startPostgres();
  const server = new pg.Client({ connectionString: postgres.url });
  await server.connect();
  await server.query("create database alasio");
  await server.end();
  const url = new URL(postgres.url);
  url.pathname = "/alasio";
  admin = new pg.Pool({ connectionString: url.toString() });
  store = new NeonSessionStore(admin);
  await store.ensureSchema();
  rollouts = new NeonRolloutStore(admin);
  await rollouts.ensureSchema();
  // As alasio makes them as it starts (src/neon/connect.ts).
  await ensureLakeRole(admin, LAKE_PASSWORD);
  await ensureLakeReaderRole(admin, READER_PASSWORD);
  await syncLakeReads(admin, true);
  dataDir = mkdtempSync(join(tmpdir(), "alasio-lake-data-"));
  const env = { LAKE_DATABASE_HOST: url.hostname, LAKE_DATABASE_PORT: url.port, LAKE_DATA_PATH: `${dataDir}/` };
  config = loadConfig({ ...env, ...LOADER_LOG, LAKE_DATABASE_PASSWORD: LAKE_PASSWORD });
  readerConfig = loadEndpointConfig({ ...env, LAKE_READER_PASSWORD: READER_PASSWORD, LAKE_QUERY_TOKEN: QUERY_TOKEN });
  // As the intake does as it opens the lake (neon/lake/src/service.ts): the first open makes the catalog.
  (await openLake(config)).close();
  const catalog = new pg.Client({ ...config.catalog });
  await catalog.connect();
  try {
    await grantReads(catalog);
  } finally {
    await catalog.end();
  }
});

after(async () => {
  await admin?.end();
  await postgres?.stop();
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
});

/** Opens the lake with a fresh, empty model, and DuckDB's `memoryLimit` where given, and closes it after `fn`. */
async function withLake<T>(fn: (db: DuckDBConnection) => Promise<T>, { memoryLimit }: { memoryLimit?: string } = {}): Promise<T> {
  const lake = await openLake(config, { source: config.source, ...(memoryLimit ? { memoryLimit } : {}) });
  try {
    await prepareLake(lake.db, { rebuild: true });
    return await fn(lake.db);
  } finally {
    lake.close();
  }
}

/** What maintenance's passes are given (neon/lake/src/maintain.ts): `log`, and `branches` beside main, none unless given. */
const passing = (log: Log, branches: () => Promise<readonly string[]> = async () => []): PassOptions =>
  ({ retentionDays: 30, branches, catalog: config.catalog, dataPath: config.dataPath, log });

/** The first row `sql` selects, which it has. */
async function one<Selected = Row>(db: DuckDBConnection, sql: string, values?: DuckDBValue[]): Promise<Selected> {
  const [first] = await rows<Selected>(db, sql, values);
  assert.ok(first, `a row from ${sql}`);
  return first;
}

/** A JSON column's value, parsed: the test reads it as `T`, and its assertions check that it is. */
function parsed<T>(json: string | null): T {
  assert.ok(json !== null, "a JSON value");
  return JSON.parse(json);
}

/** A row as DuckDB gives the lake's columns, every one of which may be null. */
type Nullable<T> = { readonly [Column in keyof T]: T[Column] | null };

/** A row's count. */
interface Count {
  readonly n: bigint;
}

// --- Claude Code transcripts ----------------------------------------------------

const KEY = { projectKey: "-home-operator-bayma", sessionId: "11111111-1111-4111-8111-111111111111" };
const SUBAGENT = { ...KEY, subpath: "subagents/agent-a1" };

const at = (second: number) => `2026-10-01T12:00:${String(second).padStart(2, "0")}.000Z`;
const assistant = (uuid: string, second: number, content: readonly object[], usage: object, extra: { readonly stop_reason?: string } = {}) => ({
  type: "assistant", uuid, timestamp: at(second), sessionId: KEY.sessionId, cwd: "/home/operator/bayma", gitBranch: "main", version: "2.9.1",
  message: { id: "msg_1", role: "assistant", model: "claude-opus-5-5", content, usage, stop_reason: extra.stop_reason ?? null },
});

// One assistant message written as two entries, the first with the usage so far, as
// Claude Code writes them; a tool call and its failed result; a typed prompt.
const TRANSCRIPT = [
  { type: "user", uuid: "u1", timestamp: at(1), message: { role: "user", content: "fix the build\u0000 please" } },
  assistant("a1", 2, [{ type: "thinking", thinking: "look first", signature: "sig" }], { input_tokens: 5, output_tokens: 8 }),
  assistant("a2", 3, [{ type: "tool_use", id: "toolu_1", name: "Bash", input: { command: "npm test" } }], {
    input_tokens: 5, output_tokens: 153, cache_read_input_tokens: 10173, cache_creation_input_tokens: 9427,
    service_tier: "standard", output_tokens_details: { thinking_tokens: 33 },
  }, { stop_reason: "tool_use" }),
  {
    type: "user", uuid: "u2", timestamp: at(9),
    message: { role: "user", content: [{ type: "tool_result", tool_use_id: "toolu_1", is_error: true, content: [{ type: "text", text: "1 failing" }] }] },
    toolUseResult: { stdout: "1 failing" },
  },
  { type: "ai-title", aiTitle: "Fixing the build", sessionId: KEY.sessionId },
];

/** A row of lake.claude.entries, as far as these tests read it. */
type ClaudeEntryRow = Nullable<{
  type: string;
  subpath: string;
  occurred_at: Date;
  malformed: boolean;
  entry: string;
  is_sidechain: boolean;
  message_id: string;
  model: string;
  stop_reason: string;
  output_tokens: bigint;
  cache_read_input_tokens: bigint;
  thinking_tokens: bigint;
  service_tier: string;
  cwd: string;
  git_branch: string;
}>;

/** A row of lake.claude.messages, as far as these tests read it. */
type ClaudeMessageRow = Nullable<{ entries: bigint; output_tokens: bigint; stop_reason: string }>;

/** A row of lake.claude.content_blocks, as far as these tests read it. */
type ContentBlockRow = Nullable<{ block_type: string; role: string; text_chars: bigint; tool_name: string; is_error: boolean }>;

/** A row of lake.claude.tool_calls, as far as these tests read it. */
type ClaudeToolCallRow = Nullable<{
  tool_name: string;
  tool_input: string;
  is_error: boolean;
  result_chars: bigint;
  called_at: Date;
  answered_at: Date;
}>;

describe("Claude Code transcripts", () => {
  test("every entry is loaded with its fields typed and its whole kept", async () => {
    await store.append(KEY, TRANSCRIPT);
    await store.append(SUBAGENT, [{ type: "user", uuid: "s1", timestamp: at(4), isSidechain: true, message: { role: "user", content: "subtask" } }]);
    await withLake(async (db) => {
      assert.deepEqual(await syncClaude(db), { inserted: 6, deleted: 0 });
      const entries = await rows<ClaudeEntryRow>(db, `select * from ${LAKE}.claude.entries order by seq`);
      assert.deepEqual(entries.map((entry) => [entry.type, entry.subpath]), [
        ["user", ""], ["assistant", ""], ["assistant", ""], ["user", ""], ["ai-title", ""], ["user", "subagents/agent-a1"],
      ]);
      const [prompt, , call] = entries;
      assert.ok(prompt && call);
      assert.equal(prompt.occurred_at?.toISOString(), at(1));
      assert.equal(prompt.malformed, false);
      assert.equal(parsed<{ message: { content: string } }>(prompt.entry).message.content, "fix the build\u0000 please"); // whole, NUL and all
      assert.deepEqual(
        [call.message_id, call.model, call.stop_reason, call.output_tokens, call.cache_read_input_tokens, call.thinking_tokens, call.service_tier, call.cwd, call.git_branch],
        ["msg_1", "claude-opus-5-5", "tool_use", 153n, 10173n, 33n, "standard", "/home/operator/bayma", "main"],
      );
      assert.equal(entries[5]?.is_sidechain, true);
    });
  });

  test("a message is counted once, with the usage its last entry carries", async () => {
    await withLake(async (db) => {
      await syncClaude(db);
      const messages = await rows<ClaudeMessageRow>(db, `select * from ${LAKE}.claude.messages`);
      assert.equal(messages.length, 1);
      const [message] = messages;
      assert.ok(message);
      assert.deepEqual([message.entries, message.output_tokens, message.stop_reason], [2n, 153n, "tool_use"]);
    });
  });

  test("content blocks are typed, and each tool call is paired with its result", async () => {
    await withLake(async (db) => {
      await syncClaude(db);
      const blocks = await rows<ContentBlockRow>(db, `select block_type, role, text_chars, tool_name, is_error from ${LAKE}.claude.content_blocks order by seq, block_index`);
      assert.deepEqual(blocks.map((block) => [block.block_type, block.role, block.text_chars]), [
        ["text", "user", 21n], ["thinking", "assistant", 10n], ["tool_use", "assistant", null], ["tool_result", "user", 9n], ["text", "user", 7n],
      ]);
      const [call] = await rows<ClaudeToolCallRow>(db, `select * from ${LAKE}.claude.tool_calls`);
      assert.ok(call);
      assert.deepEqual(
        [call.tool_name, parsed<{ command: string }>(call.tool_input).command, call.is_error, call.result_chars, Number(call.answered_at) - Number(call.called_at)],
        ["Bash", "npm test", true, 9n, 6000],
      );
    });
  });

  test("a load is idempotent, follows deletions, and loads an entry committed late", async () => {
    await withLake(async (db) => {
      await syncClaude(db);
      assert.deepEqual(await syncClaude(db), { inserted: 0, deleted: 0 });

      // An append that took its seq first but commits after a later one.
      const late = await admin.connect();
      await late.query("begin");
      await late.query(
        `insert into claude_sessions.entries (project_key, session_id, subpath, uuid, entry, mtime) values ($1, $2, '', 'late', $3, 0)`,
        [KEY.projectKey, KEY.sessionId, JSON.stringify({ type: "user", uuid: "late" })],
      );
      await store.append(KEY, [{ type: "user", uuid: "later", message: { role: "user", content: "after" } }]);
      assert.deepEqual(await syncClaude(db), { inserted: 1, deleted: 0 });
      await late.query("commit");
      late.release();
      assert.deepEqual(await syncClaude(db), { inserted: 1, deleted: 0 });

      await store.delete(SUBAGENT);
      assert.deepEqual(await syncClaude(db), { inserted: 0, deleted: 1 });
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.claude.entries where subpath <> ''`)).n, 0n);
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.claude.content_blocks where subpath <> ''`)).n, 0n);
    });
  });

  test("an entry DuckDB cannot read as JSON is kept as a string and flagged, not a stopped load", async () => {
    // Postgres's json takes an escaped lone surrogate; DuckDB's JSON does not.
    await admin.query(
      `insert into claude_sessions.entries (project_key, session_id, subpath, uuid, entry, mtime) values ($1, $2, '', 'odd', $3, 0)`,
      [KEY.projectKey, KEY.sessionId, '{"type":"user","uuid":"odd","message":{"role":"user","content":"\\ud800"}}'],
    );
    await withLake(async (db) => {
      await syncClaude(db);
      const odd = await one<ClaudeEntryRow>(db, `select type, malformed, entry from ${LAKE}.claude.entries where uuid = 'odd'`);
      assert.equal(odd.malformed, true);
      assert.equal(odd.type, null);
      assert.match(parsed<string>(odd.entry), /\\ud800/);
    });
  });

  test("a load cut short leaves whole batches and the next load finishes it", async () => {
    await withLake(async (db) => {
      let batches = 0;
      const failing = async () => {
        batches += 1;
        if (batches === 2) throw new Error("cut short");
      };
      await assert.rejects(syncClaude(db, { batchEntries: 3, beforeCommit: failing }), /cut short/);
      const { n: kept } = await one<Count>(db, `select count(*) as n from ${LAKE}.claude.entries`);
      assert.equal(kept, 3n); // the first batch, whole; nothing of the second
      const [counted] = (await admin.query<{ n: number }>("select count(*)::int as n from claude_sessions.entries")).rows;
      assert.ok(counted);
      const total = counted.n;
      assert.deepEqual(await syncClaude(db, { batchEntries: 3 }), { inserted: total - 3, deleted: 0 });
    });
  });

  test("a batch of large entries loads within a memory limit that parsing each field apart would exceed", async () => {
    // A batch of 1000 entries of 4 kB, 4 MB: with each entry parsed once, DuckDB loads it in
    // 256 MB; parsed again for each field, it ran out of that.
    const long = { ...KEY, sessionId: "22222222-2222-4222-8222-222222222222" };
    await store.append(long, Array.from({ length: 1000 }, (_, index) => ({
      type: "assistant", uuid: `long-${index}`, timestamp: at(index % 60),
      message: { id: `msg_long_${index}`, role: "assistant", model: "claude-opus-5-5", content: [{ type: "text", text: randomBytes(2000).toString("hex") }], usage: { output_tokens: index } },
    })));
    try {
      await withLake(async (db) => {
        await syncClaude(db);
        const { n } = await one<Count>(db, `select count(*) as n from ${LAKE}.claude.content_blocks where session_id = '${long.sessionId}' and text_chars = 4000`);
        assert.equal(n, 1000n);
      }, { memoryLimit: "256MB" });
    } finally {
      await store.delete(long);
    }
  });
});

// --- Codex rollouts ---------------------------------------------------------------

const line = (record: object) => `${JSON.stringify(record)}\n`;
const TURN = "turn-1";
const ROLLOUT_LINES = [
  { timestamp: at(1), ordinal: 0, type: "session_meta", payload: { id: "thread-1", cwd: "/home/operator/bayma" } },
  { timestamp: at(1), ordinal: 1, type: "event_msg", payload: { type: "task_started", turn_id: TURN } },
  { timestamp: at(1), ordinal: 2, type: "turn_context", payload: { turn_id: TURN, model: "gpt-5.6-sol", collaboration_mode: { settings: { reasoning_effort: "high" } } } },
  { timestamp: at(2), ordinal: 3, type: "response_item", payload: { type: "custom_tool_call", call_id: "call_1", name: "exec", input: "6*7", internal_chat_message_metadata_passthrough: { turn_id: TURN } } },
  { timestamp: at(3), ordinal: 4, type: "response_item", payload: { type: "custom_tool_call_output", call_id: "call_1", output: [{ type: "input_text", text: "42" }] } },
  { timestamp: at(4), ordinal: 5, type: "token_usage_record", payload: { turn_id: TURN, response_id: "resp_1", usage: { input_tokens: 13737, cached_input_tokens: 11008, output_tokens: 576, reasoning_output_tokens: 470, total_tokens: 14313 } } },
  { timestamp: at(5), ordinal: 6, type: "event_msg", payload: { type: "task_complete", turn_id: TURN, duration_ms: 21773, time_to_first_token_ms: 18515 } },
].map(line);

/** The rollout's line `index`, which it has. */
function rolloutLine(index: number): string {
  const text = ROLLOUT_LINES[index];
  assert.ok(text !== undefined, `the rollout has line ${index}`);
  return text;
}

/** Where saveRollout's bytes start, the file's size and first bytes then, where it is, and the store it is saved in. */
interface SaveRolloutOptions {
  readonly start?: number;
  readonly size?: number;
  readonly head?: Buffer;
  readonly path?: string;
  readonly store?: NeonRolloutStore;
}

/** Writes a rollout's bytes from `start` as the rollout store keeps them. */
async function saveRollout(name: string, bytes: Buffer, { start = 0, size = bytes.length + start, head = bytes, path = `sessions/2026/10/01/${name}`, store: target = rollouts }: SaveRolloutOptions = {}) {
  const firstLine = head.subarray(0, head.indexOf(0x0a) >= 0 ? head.indexOf(0x0a) : head.length);
  await target.save(
    { name, path, threadId: "thread-1", rolloutId: "thread-1", historyBase: null, size, headDigest: createHash("sha256").update(firstLine).digest("hex"), modifiedMs: 1790000000000 },
    { start, bytes },
  );
}

/** A row of lake.codex.lines, as far as these tests read it. */
type CodexLineRow = Nullable<{
  line_number: bigint;
  byte_offset: bigint;
  type: string;
  payload_type: string;
  turn_id: string;
  record: string;
  malformed: boolean;
}>;

/** A row of lake.codex.files, as far as these tests read it. */
type CodexFileRow = Nullable<{ path: string; size: bigint; loaded_bytes: bigint; lines: bigint }>;

/** A row of lake.codex.turns, as far as these tests read it. */
type CodexTurnRow = Nullable<{ turn_id: string; model: string; reasoning_effort: string; duration_ms: bigint; time_to_first_token_ms: bigint }>;

/** A row of lake.codex.token_usage, as far as these tests read it. */
type CodexTokenUsageRow = Nullable<{ response_id: string; input_tokens: bigint; output_tokens: bigint; reasoning_output_tokens: bigint }>;

/** A row of lake.codex.tool_calls, as far as these tests read it. */
type CodexToolCallRow = Nullable<{ name: string; kind: string; input: string; output: string; turn_id: string }>;

describe("Codex rollouts", () => {
  const NAME = "rollout-2026-10-01T12-00-00-thread-1.jsonl";
  const whole = Buffer.from(ROLLOUT_LINES.join(""));

  test("complete lines are loaded as they are written, a partial one once its newline lands", async () => {
    const firstPart = whole.subarray(0, whole.indexOf(rolloutLine(3)) + 20); // three lines and part of the fourth
    await saveRollout(NAME, firstPart);
    await withLake(async (db) => {
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 3, deleted: 0 });
      await saveRollout(NAME, whole.subarray(firstPart.length), { start: firstPart.length, head: whole });
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 4, deleted: 0 });
      const lines = await rows<CodexLineRow>(db, `select line_number, byte_offset, type, payload_type, turn_id, malformed from ${LAKE}.codex.lines order by line_number`);
      assert.deepEqual(lines.map((row) => Number(row.line_number)), [0, 1, 2, 3, 4, 5, 6]);
      let offset = 0;
      for (const [index, row] of lines.entries()) {
        assert.equal(Number(row.byte_offset), offset);
        offset += Buffer.byteLength(rolloutLine(index));
      }
      assert.deepEqual([lines[3]?.payload_type, lines[3]?.turn_id], ["custom_tool_call", TURN]);
      const file = await one<CodexFileRow>(db, `select loaded_bytes, lines, size from ${LAKE}.codex.files`);
      assert.deepEqual([file.loaded_bytes, file.lines, file.size], [BigInt(whole.length), 7n, BigInt(whole.length)]);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 0 });
    });
  });

  test("turns, token usage, and tool calls are shaped from the lines", async () => {
    await withLake(async (db) => {
      await syncCodex(db);
      const turn = await one<CodexTurnRow>(db, `select * from ${LAKE}.codex.turns`);
      assert.deepEqual([turn.turn_id, turn.model, turn.reasoning_effort, turn.duration_ms, turn.time_to_first_token_ms], [TURN, "gpt-5.6-sol", "high", 21773n, 18515n]);
      const usage = await one<CodexTokenUsageRow>(db, `select * from ${LAKE}.codex.token_usage`);
      assert.deepEqual([usage.response_id, usage.input_tokens, usage.output_tokens, usage.reasoning_output_tokens], ["resp_1", 13737n, 576n, 470n]);
      const call = await one<CodexToolCallRow>(db, `select * from ${LAKE}.codex.tool_calls`);
      const [output] = parsed<{ text: string }[]>(call.output);
      assert.deepEqual([call.name, call.kind, call.input, output?.text, call.turn_id], ["exec", "custom_tool_call", "6*7", "42", TURN]);
    });
  });

  test("a rewritten file is loaded again whole, a moved one followed, a removed one dropped", async () => {
    await withLake(async (db) => {
      await syncCodex(db);
      // Codex rewrote it: a new first line, and a line that is not JSON.
      const rewritten = Buffer.from(line({ timestamp: at(9), type: "session_meta", payload: { id: "thread-1", rewritten: true } }) + "not json\n");
      await saveRollout(NAME, rewritten);
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 2, deleted: 7 });
      const lines = await rows<CodexLineRow>(db, `select line_number, malformed, record from ${LAKE}.codex.lines order by line_number`);
      assert.deepEqual(lines.map((row) => [Number(row.line_number), row.malformed]), [[0, false], [1, true]]);
      const [, notJson] = lines;
      assert.ok(notJson);
      assert.equal(parsed<string>(notJson.record), "not json"); // kept, as a JSON string

      await rollouts.move(NAME, `archived_sessions/${NAME}`);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 0 });
      assert.equal((await one<CodexFileRow>(db, `select path from ${LAKE}.codex.files`)).path, `archived_sessions/${NAME}`);

      await admin.query("delete from codex_sessions.rollouts where name = $1", [NAME]);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 2 });
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.codex.lines`)).n, 0n);
    });
  });

  test("a file of large lines loads within a memory limit that parsing each field apart would exceed", async () => {
    // 4000 lines of 4 kB, 16 MB, one transaction: with each line parsed once, DuckDB loads
    // it in 256 MB; parsed again for each field, it ran out of that.
    const name = "rollout-2026-10-01T12-00-00-thread-long.jsonl";
    const lines = Array.from({ length: 4000 }, (_, ordinal) =>
      line({ timestamp: at(ordinal % 60), ordinal, type: "response_item", payload: { type: "function_call_output", turn_id: TURN, output: randomBytes(2000).toString("hex") } })
    );
    await saveRollout(name, Buffer.from(lines.join("")));
    try {
      await withLake(async (db) => {
        assert.deepEqual(await syncCodex(db), { files: 1, inserted: 4000, deleted: 0 });
        assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.codex.lines where turn_id = '${TURN}' and payload_type = 'function_call_output'`)).n, 4000n);
      }, { memoryLimit: "256MB" });
    } finally {
      await admin.query("delete from codex_sessions.rollouts where name = $1", [name]);
    }
  });

  test("the session-filesystem home is loaded beside the folder one, once the lake may read it", async () => {
    const sessionFs = new NeonRolloutStore(admin, { schema: SESSION_FS_SCHEMA });
    await sessionFs.ensureSchema();
    await syncLakeReads(admin, true);
    await saveRollout("rollout-sessionfs.jsonl", whole, { store: sessionFs });
    await withLake(async (db) => {
      await syncCodex(db);
      assert.deepEqual(await rows(db, `select home, count(*) as n from ${LAKE}.codex.lines group by home`), [{ home: "sessionfs", n: 7n }]);
    });
  });
});

// --- Telemetry ---------------------------------------------------------------------

const NOW = BigInt(Date.now()) * 1_000_000n;
const DAY = 86_400_000_000_000n;

/** A JSON export request of a log record from `service` at each of `times`, in nanoseconds. */
const logRequest = (service: string, ...times: bigint[]): LogsRequest => ({
  resourceLogs: [{
    resource: { attributes: [{ key: "service.name", value: { stringValue: service } }] },
    scopeLogs: [{ logRecords: times.map((time) => ({ timeUnixNano: String(time), body: { stringValue: "an event" } })) }],
  }],
});

/** A JSON export request of a gauge's point from `service` at `time`, in nanoseconds. */
const gaugeRequest = (service: string, time: bigint): MetricsRequest => ({
  resourceMetrics: [{
    resource: { attributes: [{ key: "service.name", value: { stringValue: service } }] },
    scopeMetrics: [{ metrics: [{ name: "lake.test", gauge: { dataPoints: [{ timeUnixNano: String(time), asDouble: 1 }] } }] }],
  }],
});

/** The log's start of every partition the tests write of themselves: their first record. */
const fromStart = async () => 0n;

let testOffset = 0n;
/** Writes `rows` as the intake writes a record's: of a partition of the tests' own, at its next offset. */
const writeTelemetry = (db: DuckDBConnection, rows: TelemetryRows) => writeConsumed(db, [{ topic: "otel.test", topicId: "otel.test-id", partition: 0, offset: testOffset++, rows: [rows] }], fromStart);

/** The lake as the intake opens it: no source, the telemetry's schema ready. */
async function openForIntake() {
  const lake = await openLake(config);
  await ensureOtel(lake.db);
  return lake;
}


describe("telemetry", () => {
  /** A record of the log's at `offset` of a topic's `partition`, its value a request in binary protobuf, as the collector writes each. */
  const record = (topic: string, partition: number, offset: number, value: Uint8Array, topicId = `${topic}-id`): LogRecord => ({ topic, topicId, partition, offset: BigInt(offset), value: Buffer.from(value) });
  const logRecord = (partition: number, offset: number, service: string, ...times: bigint[]) => record("otel.logs", partition, offset, encodeRequest("logs", logRequest(service, ...times)));

  /**
   * A member of the group that hands out `batches` in turn, once `ready`, then nothing,
   * recording what it is told; its commits fail where `failCommits`, it tells the group the
   * lake's offsets where `behind`, a group told past them, and the log has its topics'
   * records of a time where `at` says; it holds every partition from its first record.
   */
  function member(
    batches: LogRecord[][],
    { failCommits = false, behind = false, at = {}, ready = () => true }: {
      failCommits?: boolean;
      behind?: boolean;
      at?: Readonly<Record<string, readonly PartitionAt[]>>;
      ready?: () => boolean;
    } = {},
  ) {
    const committed: Position[][] = [];
    const rewound: Held[][] = [];
    const askedAt: { topic: string; ms: number }[] = [];
    let closed = false;
    const source: IntakeSource = {
      async next() {
        const batch = ready() ? batches.shift() : undefined;
        if (batch) return batch;
        await new Promise((resolve) => setTimeout(resolve, 20));
        return [];
      },
      async commit(offsets) {
        if (failCommits) throw new Error("the group rebalanced");
        committed.push([...offsets]);
      },
      async rewind(held) {
        rewound.push([...held]);
        return behind;
      },
      async start() {
        return 0n;
      },
      async offsetsAt(topic, ms) {
        askedAt.push({ topic, ms });
        return [...(at[topic] ?? [])];
      },
      async lag() {
        return new Map([["otel.logs", batches.flat().length], ["otel.spans", 0]]);
      },
      async close() {
        closed = true;
      },
    };
    return { source, committed, rewound, askedAt, closed: () => closed };
  }

  const until = async (what: string, done: () => boolean) => {
    for (let waited = 0; !done(); waited += 20) {
      if (waited > 30_000) assert.fail(`waited for ${what}`);
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  };

  test("a batch gathers records within its time, as many as it takes, while their values come to its bytes, the read under way, or one that would take it past them, kept for the next", async () => {
    const MiB = 1024 * 1024;
    const reads: Read[] = Array.from({ length: 5 }, (_, offset) => ({ topic: "otel.metrics", partition: 0, offset: BigInt(offset), value: Buffer.alloc(MiB) }));
    let asked = 0;
    // Five records of a MiB, then none for a long while.
    const gather = gathering({
      next: () => (asked < reads.length ? Promise.resolve({ done: false, value: reads[asked++]! }) : new Promise(() => {})),
    });
    const offsets = (batch: readonly Read[]) => batch.map(({ offset }) => Number(offset));
    // Up to its bytes, the record that would take it past them left for the next; then up to its count; then what is left, once its time is up.
    assert.deepEqual(offsets(await gather({ ms: 1000, records: 10, bytes: 1.5 * MiB })), [0]);
    assert.deepEqual(offsets(await gather({ ms: 1000, records: 10, bytes: 2 * MiB })), [1, 2]);
    assert.deepEqual(offsets(await gather({ ms: 1000, records: 1, bytes: 32 * MiB })), [3]);
    const started = Date.now();
    assert.deepEqual(offsets(await gather({ ms: 50, records: 10, bytes: 32 * MiB })), [4]);
    assert.ok(Date.now() - started >= 45);
    // A record larger than the bytes is a batch of its own.
    const large = gathering({ next: () => Promise.resolve({ done: false, value: { topic: "otel.metrics", partition: 0, offset: 0n, value: Buffer.alloc(3 * MiB) } }) });
    assert.equal((await large({ ms: 1000, records: 10, bytes: 2 * MiB })).length, 1);
  });

  test("the intake writes each batch it consumes in one commit with its offsets, queryable from another DuckDB, skips a record that does not decode, only then tells the group, and says how far behind it is", async () => {
    const spans = new InMemorySpanExporter();
    const tracer = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "intake-test" }), spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer("t");
    tracer.startSpan("alasio.turn", { attributes: { "alasio.conversation.id": "telegram:7" } }).end();
    const traces = ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans());
    assert.ok(traces);
    const group = member([[record("otel.spans", 0, 0, traces), logRecord(1, 0, "intake-test", NOW, NOW + 1n), record("otel.logs", 1, 1, Uint8Array.from([0x0a, 0x05, 0x01]))]]);
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => group.source, metrics, log: () => {}, batch: { ...BATCH, ms: 10 } });
    try {
      await until("the batch's commit to the group", () => group.committed.length > 0);
      assert.deepEqual(group.committed[0]?.toSorted((a, b) => a.topic.localeCompare(b.topic)), [{ topic: "otel.logs", partition: 1, offset: 2n }, { topic: "otel.spans", partition: 0, offset: 1n }]);
      assert.ok(intake.ready());
      const reader = await openLake(readerConfig, { readOnly: true });
      try {
        const [span] = await rows<{ SpanName: string; conversation: string }>(reader.db, `select SpanName, SpanAttributes['alasio.conversation.id'] as conversation from ${LAKE}.otel.traces where ServiceName = 'intake-test'`);
        assert.deepEqual(span, { SpanName: "alasio.turn", conversation: "telegram:7" });
        assert.equal((await one<Count>(reader.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = 'intake-test'`)).n, 2n);
      } finally {
        reader.close();
      }
      const text = metrics.render();
      assert.match(text, /^lake_telemetry_rows_total\{table="otel.logs"\} 2$/m);
      assert.match(text, /^lake_telemetry_records_total\{topic="otel.logs",outcome="malformed"\} 1$/m);
      // How far behind the group is, as the member asked once the batch was written.
      assert.match(text, /^lake_telemetry_lag_records\{topic="otel.logs"\} 0$/m);
      assert.match(text, /^lake_telemetry_lag_records\{topic="otel.spans"\} 0$/m);
    } finally {
      await intake.stop();
    }
    assert.ok(group.closed());
  });

  test("a batch the lake has, read again as the group was not told of it, is written once", async () => {
    const service = `repeated-${Date.now()}`;
    const batch = [logRecord(2, 0, service, NOW), logRecord(2, 1, service, NOW + 1n)];
    // The first member writes the batch, then cannot tell the group, as after a rebalance or a crash.
    const first = member([batch], { failCommits: true });
    const second = member([[...batch, logRecord(2, 2, service, NOW + 2n)]]);
    const members = [first, second];
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a third time"), metrics, log: () => {}, batch: { ...BATCH, ms: 10 }, retryMs: 10 });
    try {
      await until("the second member's commit", () => second.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.ok(first.closed());
    assert.deepEqual(second.committed[0], [{ topic: "otel.logs", partition: 2, offset: 3n }]);
    const lake = await openLake(config, { readOnly: true });
    try {
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 3n);
    } finally {
      lake.close();
    }
    assert.match(metrics.render(), /^lake_telemetry_records_total\{topic="",outcome="repeated"\} 2$/m);
  });

  test("a topic made anew, as a restore of the object store makes the log's, is written from its start, the offsets the lake held being the topic's before", async () => {
    const service = `made-anew-${Date.now()}`;
    const before = [logRecord(4, 0, service, NOW), logRecord(4, 1, service, NOW + 1n)];
    const made = (offset: number) => record("otel.logs", 4, offset, encodeRequest("logs", logRequest(service, NOW + 10n + BigInt(offset))), "otel.logs-made-anew");
    // The first member writes its batch, then loses the log, which comes back made anew.
    const first = member([before], { failCommits: true });
    const second = member([[made(0), made(1), made(2)]]);
    const members = [first, second];
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a third time"), metrics, log: () => {}, batch: { ...BATCH, ms: 10 }, retryMs: 10 });
    try {
      await until("the second member's commit", () => second.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.deepEqual(second.committed[0], [{ topic: "otel.logs", partition: 4, offset: 3n }]);
    const lake = await openLake(config, { readOnly: true });
    try {
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 5n);
    } finally {
      lake.close();
    }
    assert.doesNotMatch(metrics.render(), /outcome="repeated"/u);
  });

  test("what the log holds twice, at two offsets, as a producer's retry writes it, is in the lake once: a span by its ids, a log record by its id, its producer's or one made of what it is, a point by its series and time", async () => {
    const service = `twice-${Date.now()}`;
    const span = (spanId: string) => encodeRequest("traces", {
      resourceSpans: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: service } }] },
        scopeSpans: [{ spans: [{ traceId: Buffer.from("5b8efff798038103d269b633813fc60c", "hex"), spanId: Buffer.from(spanId, "hex"), name: "twice", startTimeUnixNano: String(NOW), endTimeUnixNano: String(NOW + 1n) }] }],
      }],
    });
    const logOf = (id: string | null, time = NOW) => encodeRequest("logs", {
      resourceLogs: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: service } }] },
        scopeLogs: [{ logRecords: [{ timeUnixNano: String(time), body: { stringValue: "an event" }, attributes: id ? [{ key: "log.record.uid", value: { stringValue: id } }] : [] }] }],
      }],
    });
    const point = (time: bigint) => encodeRequest("metrics", gaugeRequest(service, time));
    // The first batch, then a second that repeats each of its records at offsets of its own,
    // and repeats some within itself; a log record without an id is known by what it is, as an
    // SDK's export sent again is, and one of another time is another.
    const group = member([
      [record("otel.spans", 5, 0, span("eee19b7ec3c1b174")), record("otel.logs", 5, 0, logOf("a")), record("otel.logs", 5, 1, logOf(null)), record("otel.metrics", 5, 0, point(NOW))],
      [
        record("otel.spans", 5, 1, span("eee19b7ec3c1b174")),
        record("otel.spans", 5, 2, span("eee19b7ec3c1b175")),
        record("otel.spans", 5, 3, span("eee19b7ec3c1b175")),
        record("otel.logs", 5, 2, logOf("a")),
        record("otel.logs", 5, 3, logOf("b")),
        record("otel.logs", 5, 4, logOf("b")),
        record("otel.logs", 5, 5, logOf(null)),
        record("otel.logs", 5, 6, logOf(null, NOW + 1n)),
        record("otel.metrics", 5, 1, point(NOW)),
        record("otel.metrics", 5, 2, point(NOW + 1n)),
      ],
    ]);
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => group.source, metrics, log: () => {}, batch: { ...BATCH, ms: 10 } });
    try {
      await until("the second batch's commit", () => group.committed.length > 1);
    } finally {
      await intake.stop();
    }
    const lake = await openLake(config, { readOnly: true });
    try {
      const count = async (table: string) => (await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.${table} where ServiceName = '${service}'`)).n;
      assert.deepEqual([await count("traces"), await count("logs"), await count("metrics_gauge")], [2n, 4n, 2n]);
      // An id it was not given is the lake's to see, as one it was is.
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}' and LogAttributes['log.record.uid'] similar to '[0-9a-f]{32}'`)).n, 2n);
    } finally {
      lake.close();
    }
    const text = metrics.render();
    assert.match(text, /^lake_telemetry_duplicate_rows_total\{table="otel.traces"\} 2$/m);
    assert.match(text, /^lake_telemetry_duplicate_rows_total\{table="otel.logs"\} 3$/m);
    assert.match(text, /^lake_telemetry_duplicate_rows_total\{table="otel.metrics_gauge"\} 1$/m);
  });

  test("the intake, as it joins, has the group told the lake's offsets, so a lake put back reads again from the log what it lost, and joins again to read it", async () => {
    const service = `rewound-${Date.now()}`;
    // The lake holds two records of partition 17.
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeConsumed(db, [0, 1].map((offset) => ({ topic: "otel.logs", topicId: "otel.logs-id", partition: 17, offset: BigInt(offset), rows: [telemetryRows("logs", logRequest(service, NOW + BigInt(offset)))] })), fromStart);
    });
    // A member of a group told past them, which tells it the lake's; then one that reads from there.
    const behind = member([], { behind: true });
    const again = member([[logRecord(17, 2, service, NOW + 2n)]]);
    const members = [behind, again];
    const intake = startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a third time"), metrics: createMetrics(), log: () => {}, batch: { ...BATCH, ms: 10 } });
    try {
      await until("the second member's commit", () => again.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.ok(behind.rewound[0]?.some((held) => held.topic === "otel.logs" && held.topicId === "otel.logs-id" && held.partition === 17 && held.offset === 2n), JSON.stringify(behind.rewound, (_, value) => (typeof value === "bigint" ? String(value) : value)));
    assert.ok(behind.closed());
    assert.deepEqual(behind.committed, []);
    assert.deepEqual(again.committed[0], [{ topic: "otel.logs", partition: 17, offset: 3n }]);
  });

  test("a table of the telemetry's rows' times are read by its time column, and a reread is asked of the telemetry's tables alone", async () => {
    assert.deepEqual(["otel.traces", "otel.logs", "otel.metrics_sum", "claude.entries", "otel.intake_offsets"].map(telemetryTime), ["Timestamp", "Timestamp", "TimeUnix", null, null]);
    await withLake(async (db) => {
      await ensureOtel(db);
      await assert.rejects(askReread(db, [{ table: "claude.entries", from: "2026-10-16 00:00:00" }]), /^Error: claude\.entries is no table of the telemetry's$/u);
    });
  });

  test("asked to read again what the lake lost, the intake sets its offsets back to where the log has records of its time, and writes again each row the lake no longer has, once", async () => {
    const service = `reread-${Date.now()}`;
    const second = 1_000_000_000n;
    const at = (offset: number) => logRecord(24, offset, service, NOW + BigInt(offset) * second);
    let since = "";
    await withLake(async (db) => {
      await ensureOtel(db);
      // The lake holds records 0 to 3 of partition 24, then loses the rows of 2 and 3, as a
      // reconciliation forgets the file that held them, and asks them read again from their time.
      await writeConsumed(db, [0, 1, 2, 3].map((offset) => ({ ...at(offset), rows: [telemetryRows("logs", logRequest(service, NOW + BigInt(offset) * second))] })), fromStart);
      since = (await one<{ since: string }>(db, `select min(Timestamp)::varchar as since from ${LAKE}.otel.logs where ServiceName = '${service}' and Timestamp >= make_timestamp_ns(${NOW + 2n * second})`)).since;
      await transaction(db, () => db.run(`delete from ${LAKE}.otel.logs where ServiceName = '${service}' and Timestamp >= make_timestamp_ns(${NOW + 2n * second})`));
      await askReread(db, [{ table: "otel.logs", from: since }]);
    });
    // The member whose next write takes that up, the log having records of that time from offset
    // 1 on, as its clocks' skew allows; one that joins again, the group told past the lake's
    // offsets; and one that reads from there.
    const asked = member([[at(4)]], { at: { "otel.logs": [{ partition: 24, offset: 1n, at: Number(NOW / 1_000_000n) + 1000, start: 0n, end: 5n }] } });
    const behind = member([], { behind: true });
    const again = member([[at(1), at(2), at(3), at(4)]]);
    const members = [asked, behind, again];
    const logged: string[] = [];
    const intake = startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a fourth time"), metrics: createMetrics(), log: (message) => logged.push(message), batch: { ...BATCH, ms: 10 } });
    try {
      await until("the third member's commit", () => again.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.deepEqual(asked.askedAt, [{ topic: "otel.logs", ms: Number(NOW / 1_000_000n) + 2000 - REREAD_SKEW_MS }]);
    assert.deepEqual(asked.committed, []);
    assert.ok(asked.closed());
    assert.ok(logged.includes("telemetry intake reads again from the log what the lake lost"), logged.join("\n"));
    assert.ok(behind.rewound[0]?.some((held) => held.partition === 24 && held.offset === 1n));
    assert.deepEqual(again.committed[0], [{ topic: "otel.logs", partition: 24, offset: 5n }]);
    const lake = await openLake(config, { readOnly: true });
    try {
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 5n);
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.intake_rereads`)).n, 0n);
      // The log held all of it: nothing it could not refill.
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.intake_reread_gaps where Signal = 'logs' and FromTime = $1::TIMESTAMP_NS`, [since])).n, 0n);
    } finally {
      lake.close();
    }
  });

  test("a write asks the log where a partition starts only of a batch read past the lake's offset, and refuses it unless the log starts there", async () => {
    const service = `starts-${Date.now()}`;
    const asked: string[] = [];
    const startingAt = (start: bigint) => async (topic: string, partition: number) => {
      asked.push(`${topic}/${partition}`);
      return start;
    };
    const batch = (...offsets: number[]) => offsets.map((offset) => ({ ...logRecord(32, offset, service, NOW + BigInt(offset)), rows: [telemetryRows("logs", logRequest(service, NOW + BigInt(offset)))] }));
    await withLake(async (db) => {
      await ensureOtel(db);
      // Following on from the lake's offset, or read again before it: nothing asked.
      await writeConsumed(db, batch(0, 1), startingAt(0n));
      await writeConsumed(db, batch(2, 3), startingAt(0n));
      await writeConsumed(db, batch(2, 3, 4), startingAt(0n));
      assert.deepEqual(asked, []);
      // Read past it: asked, refused where the log holds the records between, taken where it starts there.
      await assert.rejects(writeConsumed(db, batch(7, 8), startingAt(0n)), LakeSetBack);
      await writeConsumed(db, batch(7, 8), startingAt(7n));
      assert.deepEqual(asked, ["otel.logs/32", "otel.logs/32"]);
    });
  });

  test("a reread one replica takes up, setting back the partitions of another's, has the other read its own again from the lake's offsets, not past them", async () => {
    const services = { a: `replica-a-${Date.now()}`, b: `replica-b-${Date.now()}` };
    const second = 1_000_000_000n;
    const recordOf = (replica: "a" | "b", offset: number) => logRecord(replica === "a" ? 30 : 31, offset, services[replica], NOW + BigInt(offset) * second);
    await withLake(async (db) => {
      await ensureOtel(db);
      // Each replica's partition holds records 0 to 3; the lake loses the rows of 2 and 3 of both.
      for (const replica of ["a", "b"] as const) {
        await writeConsumed(db, [0, 1, 2, 3].map((offset) => ({ ...recordOf(replica, offset), rows: [telemetryRows("logs", logRequest(services[replica], NOW + BigInt(offset) * second))] })), fromStart);
      }
      const since = (await one<{ since: string }>(db, `select min(Timestamp)::varchar as since from ${LAKE}.otel.logs where ServiceName = '${services.a}' and Timestamp >= make_timestamp_ns(${NOW + 2n * second})`)).since;
      await transaction(db, () => db.run(`delete from ${LAKE}.otel.logs where ServiceName in ('${services.a}', '${services.b}') and Timestamp >= make_timestamp_ns(${NOW + 2n * second})`));
      await askReread(db, [{ table: "otel.logs", from: since }]);
    });
    // Replica A takes up the reread, the log having both partitions' records of that time from 2 on.
    const ms = Number(NOW / 1_000_000n) + 2000;
    const takes = member([[recordOf("a", 4)]], { at: { "otel.logs": [30, 31].map((partition) => ({ partition, offset: 2n, at: ms, start: 0n, end: 5n })) } });
    const aAgain = [member([], { behind: true }), member([[recordOf("a", 2), recordOf("a", 3), recordOf("a", 4)]])];
    // Replica B, told nothing, reads its next record, past its partition's offset set back, once A has.
    const bFirst = member([[recordOf("b", 4)]], { ready: () => takes.closed() });
    const bAgain = [member([], { behind: true }), member([[recordOf("b", 2), recordOf("b", 3), recordOf("b", 4)]])];
    const aMembers = [takes, ...aAgain];
    const bMembers = [bFirst, ...bAgain];
    const logged: string[] = [];
    const start = (members: ReturnType<typeof member>[]) =>
      startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a fourth time"), metrics: createMetrics(), log: (message) => logged.push(message), batch: { ...BATCH, ms: 10 }, retryMs: 10 });
    const replicas = [start(aMembers), start(bMembers)];
    try {
      await until("both replicas' commits", () => (aAgain[1]?.committed.length ?? 0) > 0 && (bAgain[1]?.committed.length ?? 0) > 0);
    } finally {
      await Promise.all(replicas.map((replica) => replica.stop()));
    }
    assert.deepEqual(bFirst.committed, []);
    assert.ok(bFirst.closed());
    assert.ok(logged.includes("telemetry intake reads again from the lake's offsets, set back beneath it"), logged.join("\n"));
    assert.deepEqual(bAgain[1]?.committed[0], [{ topic: "otel.logs", partition: 31, offset: 5n }]);
    const lake = await openLake(config, { readOnly: true });
    try {
      for (const service of Object.values(services)) {
        assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 5n, service);
      }
    } finally {
      lake.close();
    }
  });

  test("what of a reread the log no longer holds is the time from it to the first record a partition still holds, where its retention deleted any", () => {
    const since = Date.parse("2026-10-16T10:00:00.000Z");
    const reread = { signal: "traces" as const, since: "2026-10-16 10:00:00", sinceMs: since };
    const now = Date.parse("2026-10-16T12:00:00.000Z");
    const partition = (offset: bigint | null, at: number | null, start: bigint, end: bigint): PartitionAt => ({ partition: 0, offset, at, start, end });
    // All it ever held; a record before the time still held; records before it alone: nothing lost.
    assert.equal(unreachable(reread, [partition(0n, since + 5_000, 0n, 9n)], now), null);
    assert.equal(unreachable(reread, [partition(7n, since + 5_000, 3n, 9n)], now), null);
    assert.equal(unreachable(reread, [partition(null, null, 3n, 9n)], now), null);
    // Its first record held of the time or later, of a time no later than the reread's: nothing of it lost.
    assert.equal(unreachable(reread, [partition(3n, since, 3n, 9n)], now), null);
    // Its first record held of later, records before it deleted: to that record's time, the latest of the partitions'.
    assert.deepEqual(unreachable(reread, [partition(3n, since + 60_000, 3n, 9n), partition(4n, since + 30_000, 4n, 9n)], now), { signal: "traces", from: "2026-10-16T10:00:00.000Z", to: "2026-10-16T10:01:00.000Z" });
    // Every record it held deleted: to now.
    assert.deepEqual(unreachable(reread, [partition(null, null, 9n, 9n)], now), { signal: "traces", from: "2026-10-16T10:00:00.000Z", to: "2026-10-16T12:00:00.000Z" });
  });

  test("a reread the log no longer holds all of reads what it holds, and says what it cannot refill, in its log, as a metric and in the lake", async () => {
    const service = `gap-${Date.now()}`;
    const second = 1_000_000_000n;
    const at = (offset: number) => logRecord(26, offset, service, NOW + BigInt(offset) * second);
    const started = new Date().toISOString();
    let since = "";
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeConsumed(db, [0, 1, 2, 3].map((offset) => ({ ...at(offset), rows: [telemetryRows("logs", logRequest(service, NOW + BigInt(offset) * second))] })), fromStart);
      since = (await one<{ since: string }>(db, `select min(Timestamp)::varchar as since from ${LAKE}.otel.logs where ServiceName = '${service}'`)).since;
      await transaction(db, () => db.run(`delete from ${LAKE}.otel.logs where ServiceName = '${service}'`));
      await askReread(db, [{ table: "otel.logs", from: since }]);
    });
    // The log deleted records 0 and 1 of partition 26: it holds those of 2 seconds on alone.
    const sinceMs = Number(NOW / 1_000_000n);
    const asked = member([[at(4)]], { at: { "otel.logs": [{ partition: 26, offset: 2n, at: sinceMs + 2000, start: 2n, end: 5n }] } });
    const behind = member([], { behind: true });
    const again = member([[at(2), at(3), at(4)]]);
    const members = [asked, behind, again];
    const logged: { message: string; fields?: Record<string, unknown> }[] = [];
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => members.shift()?.source ?? assert.fail("joined a fourth time"), metrics, log: (message, fields) => logged.push({ message, ...(fields ? { fields } : {}) }), batch: { ...BATCH, ms: 10 } });
    try {
      await until("the third member's commit", () => again.committed.length > 0);
    } finally {
      await intake.stop();
    }
    const gap = { signal: "logs", from: new Date(sinceMs).toISOString(), to: new Date(sinceMs + 2000).toISOString() };
    assert.deepEqual(logged.find(({ message }) => message === "telemetry intake cannot read again what the lake lost of a time the log no longer holds")?.fields, gap);
    const text = metrics.render();
    assert.match(text, /^lake_telemetry_reread_gaps_total\{signal="logs"\} 1$/m);
    assert.match(text, /^lake_telemetry_reread_gap_seconds\{signal="logs"\} 2$/m);
    const lake = await openLake(config, { readOnly: true });
    try {
      assert.deepEqual(await rereadGaps(lake.db, started), [gap]);
      // What the log still held is in the lake again.
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 3n);
    } finally {
      lake.close();
    }
  });

  test("a write that meets a file the lake names and the store does not give back fails naming it and what sets it right, and the intake's health says so once that goes on", async () => {
    const service = `unreadable-${Date.now()}`;
    // A day of its own, its one file then gone from the store.
    const start = NOW - 30n * DAY;
    const day = new Date(Number(start / 1_000_000n));
    const partition = `year=${day.getUTCFullYear()}/month=${day.getUTCMonth() + 1}/day=${day.getUTCDate()}/`;
    const file = await withLake(async (db) => {
      await ensureOtel(db);
      await writeConsumed(db, [{ topic: "otel.logs", topicId: "otel.logs-id", partition: 25, offset: 0n, rows: [telemetryRows("logs", logRequest(service, start))] }], fromStart);
      const [only, ...others] = await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'logs', schema => 'otel') where contains(data_file, '${partition}')`);
      assert.ok(only && others.length === 0);
      return only.data_file;
    });
    const held = readFileSync(file);
    rmSync(file);
    try {
      const group = member(Array.from({ length: 1000 }, () => [logRecord(25, 1, service, start - 1n, start + 1n)]));
      const failures: string[] = [];
      const log = (message: string, fields?: Record<string, unknown>) => {
        if (message === "telemetry intake lost the lake or the log") failures.push(String(fields?.["error"]));
      };
      const intake = startIntake({ open: openForIntake, join: async () => group.source, metrics: createMetrics(), log, batch: { ...BATCH, ms: 10 }, retryMs: 10, failingMs: 1000 });
      try {
        await until("a batch's failure", () => failures.length > 0);
        const named = `the object store did not give back ${file}, a file of otel.logs the lake's catalog names (`;
        assert.ok(failures[0]?.startsWith(named), failures[0]);
        assert.ok(failures[0]?.endsWith("): where this goes on, the store lost it, and run alasio lake reconcile to set the lake right"), failures[0]);
        await until("the intake not well", () => !intake.health().ok);
        assert.ok(intake.health().detail.includes(named), intake.health().detail);
        assert.deepEqual(group.committed, []);
      } finally {
        await intake.stop();
      }
    } finally {
      writeFileSync(file, held);
    }
  });

  test("the intake joins the group only once it holds the lake, and tries again until it can open it", async () => {
    let opens = 0;
    let joins = 0;
    const group = member([[logRecord(3, 0, "late", NOW)]]);
    const intake = startIntake({
      open: async () => {
        opens += 1;
        if (opens < 3) throw new Error("the compute is restarting");
        return openForIntake();
      },
      join: async () => {
        joins += 1;
        return group.source;
      },
      metrics: createMetrics(),
      log: () => {},
      batch: { ...BATCH, ms: 10 },
      retryMs: 10,
    });
    try {
      assert.equal(intake.ready(), false);
      await until("the batch's commit", () => group.committed.length > 0);
      assert.deepEqual([opens, joins], [3, 1]);
    } finally {
      await intake.stop();
    }
  });

  test("the telemetry's merges leave a file already as large as they merge files to, and merge what came since", async () => {
    await withLake(async (db) => {
      const { value } = await one<{ value: string }>(db, `select value from ducklake_options('${LAKE}') where option_name = 'target_file_size' and scope_entry = 'otel.metrics_gauge'`);
      const target = BigInt(value);
      // A day of its own, as the hours of points merged before come to make.
      const start = NOW - 5n * DAY;
      const day = new Date(Number(start / 1_000_000n));
      const partition = `year=${day.getUTCFullYear()}/month=${day.getUTCMonth() + 1}/day=${day.getUTCDate()}/`;
      const files = async () => rows<{ data_file: string; size: bigint }>(db, `select data_file, data_file_size_bytes as size from ducklake_list_files('${LAKE}', 'metrics_gauge', schema => 'otel') where contains(data_file, '${partition}')`);
      await db.run(`insert into ${LAKE}.otel.metrics_gauge by name
        select 'merged' as ServiceName, md5(i::VARCHAR) as MetricName, make_timestamp_ns(${start} + i) as TimeUnix, i::DOUBLE as Value
        from range(${target / 10n}) t(i)`);
      for (const offset of [0, 1, 2]) {
        await writeConsumed(db, [{ topic: "otel.metrics", topicId: "otel.metrics-id", partition: 9, offset: BigInt(offset), rows: [telemetryRows("metrics", gaugeRequest("merged", start + BigInt(offset)))] }], fromStart);
      }
      const large = (await files()).filter(({ size }) => size >= target).map(({ data_file }) => data_file);
      assert.ok(large.length > 0);
      await mergeTelemetry(db);
      const after = await files();
      assert.deepEqual(after.filter(({ data_file }) => large.includes(data_file)).map(({ data_file }) => data_file).sort(), large.sort());
      assert.equal(after.length, large.length + 1);
    });
  });

  test("nothing of the telemetry is inlined in the catalog: each commit writes its files at once", async () => {
    await withLake(async (db) => {
      const files = async (table: string) => (await one<Count>(db, `select count(*) as n from ducklake_list_files('${LAKE}', '${table}', schema => 'otel')`)).n;
      const before = { logs: await files("logs"), gauge: await files("metrics_gauge") };
      await writeConsumed(db, [
        { topic: "otel.logs", topicId: "otel.logs-id", partition: 8, offset: 0n, rows: [telemetryRows("logs", logRequest("files", NOW))] },
        { topic: "otel.metrics", topicId: "otel.metrics-id", partition: 8, offset: 0n, rows: [telemetryRows("metrics", gaugeRequest("files", NOW))] },
      ], fromStart);
      assert.deepEqual({ logs: await files("logs"), gauge: await files("metrics_gauge") }, { logs: before.logs + 1n, gauge: before.gauge + 1n });
    });
  });

  test("a record that turns out not to decode once some of its rows are staged is skipped whole, and the rest of its batch written", async () => {
    // A request of a resource of PART_ITEMS + 1 log records, then a resource that does not decode.
    const valid = encodeRequest("logs", logRequest("half", ...Array.from({ length: PART_ITEMS + 1 }, (_, index) => NOW + BigInt(index))));
    const broken = Buffer.concat([valid, Uint8Array.from([0x0a, 0x02, 0x0b, 0x01])]);
    const rowsOf = (value: Uint8Array) => (function*() {
      for (const part of requestParts("logs", value)) yield telemetryRows("logs", part);
    })();
    await withLake(async (db) => {
      await ensureOtel(db);
      const written = await writeConsumed(db, [
        { topic: "otel.logs", topicId: "otel.logs-id", partition: 14, offset: 0n, rows: rowsOf(broken) },
        { topic: "otel.logs", topicId: "otel.logs-id", partition: 14, offset: 1n, rows: rowsOf(encodeRequest("logs", logRequest("whole", NOW))) },
      ], fromStart);
      assert.deepEqual(written.malformed.map(({ offset }) => offset), [0n]);
      const count = async (service: string) => (await one<Count>(db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n;
      assert.deepEqual([await count("half"), await count("whole")], [0n, 1n]);
    });
  });

  test("a record of a time no TIMESTAMP_NS holds, or whose rows would repeat its resource's attributes past REQUEST_ROW_BYTES, is skipped as malformed, and the rest of its batch written", async () => {
    const spans = (service: string, count: number, start: bigint, attributes: { key: string; value: { stringValue: string } }[] = []) => encodeRequest("traces", {
      resourceSpans: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: service } }, ...attributes] },
        scopeSpans: [{ spans: Array.from({ length: count }, (_, index) => ({ traceId: Buffer.alloc(16, 1), spanId: Buffer.alloc(8, index + 1), name: "span", startTimeUnixNano: String(start), endTimeUnixNano: String(start + 1n) })) }],
      }],
    });
    // A record of a MiB of attributes and a hundred spans: some 100 MiB of rows.
    const amplifying = spans("amplifying", 100, NOW, [{ key: "padding", value: { stringValue: "x".repeat(1024 * 1024) } }]);
    const group = member([[record("otel.spans", 15, 0, spans("overflowing", 1, 2n ** 63n)), record("otel.spans", 15, 1, amplifying), record("otel.spans", 15, 2, spans("kept", 1, NOW))]]);
    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, join: async () => group.source, metrics, log: () => {}, batch: { ...BATCH, ms: 10 } });
    try {
      await until("the batch's commit", () => group.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.deepEqual(group.committed[0], [{ topic: "otel.spans", partition: 15, offset: 3n }]);
    assert.match(metrics.render(), /^lake_telemetry_records_total\{topic="otel.spans",outcome="malformed"\} 2$/m);
    const lake = await openLake(config, { readOnly: true });
    try {
      const count = async (service: string) => (await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.traces where ServiceName = '${service}'`)).n;
      assert.deepEqual([await count("overflowing"), await count("amplifying"), await count("kept")], [0n, 0n, 1n]);
    } finally {
      lake.close();
    }
  });

  test("a record whose rows fail to write is written alone, skipped where it still fails, its offset written past it, and the rest of its batch written", async () => {
    const group = member([[logRecord(16, 0, "before", NOW), logRecord(16, 1, "refused", NOW), logRecord(16, 2, "after", NOW)]]);
    // A lake that refuses the rows of the service "refused", as one may refuse a row no check foresaw.
    const open = async () => {
      const lake = await openForIntake();
      const refusing = (appender: object) => new Proxy(appender, {
        get(target, key, receiver) {
          const value = Reflect.get(target, key, receiver);
          if (key !== "appendDataChunk") return typeof value === "function" ? value.bind(target) : value;
          // ServiceName, the seventh of otel.logs' columns, alone: DuckDB's Node API reads a map of a
          // chunk its rows were set in from memory the chunk no longer holds them in.
          return (chunk: { getColumnValues(column: number): unknown[] }) => {
            if (chunk.getColumnValues(6).includes("refused")) throw new Error("the lake refuses the row");
            return value.call(target, chunk);
          };
        },
      });
      const db = new Proxy(lake.db, {
        get(target, key, receiver) {
          const value = Reflect.get(target, key, receiver);
          if (key !== "createAppender") return typeof value === "function" ? value.bind(target) : value;
          return async (...args: unknown[]) => refusing(await value.apply(target, args));
        },
      });
      return { db, close: () => lake.close() };
    };
    const metrics = createMetrics();
    const logged: string[] = [];
    const intake = startIntake({ open, join: async () => group.source, metrics, log: (message) => logged.push(message), batch: { ...BATCH, ms: 10 } });
    try {
      await until("the batch's commit", () => group.committed.length > 0);
    } finally {
      await intake.stop();
    }
    assert.deepEqual(group.committed[0], [{ topic: "otel.logs", partition: 16, offset: 3n }]);
    assert.ok(logged.includes("telemetry intake skipped a record whose rows it could not write"), logged.join("\n"));
    assert.match(metrics.render(), /^lake_telemetry_records_total\{topic="otel.logs",outcome="unwritable"\} 1$/m);
    const lake = await openLake(config, { readOnly: true });
    try {
      const count = async (service: string) => (await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n;
      assert.deepEqual([await count("before"), await count("refused"), await count("after")], [1n, 0n, 1n]);
    } finally {
      lake.close();
    }
  });

  /**
   * Has the intake write `records`, handed out again after each rejoin, as the group hands out
   * what it was not told of, on a lake whose next `failures` commits throw `failure()`: what
   * it logged, and how many rows of otel.logs each of `services` has then.
   */
  async function committedAfter(records: LogRecord[], failures: number, failure: () => Error, services: readonly string[]) {
    const group = member(Array.from({ length: failures + 1 }, () => records));
    let failing = failures;
    const open = async () => {
      const lake = await openForIntake();
      const db = new Proxy(lake.db, {
        get(target, key, receiver) {
          const value = Reflect.get(target, key, receiver);
          if (key !== "run") return typeof value === "function" ? value.bind(target) : value;
          return async (sql: string, ...rest: unknown[]) => {
            if (sql === "commit" && failing > 0) {
              failing -= 1;
              throw failure();
            }
            return value.call(target, sql, ...rest);
          };
        },
      });
      return { db, close: () => lake.close() };
    };
    const logged: string[] = [];
    const intake = startIntake({ open, join: async () => group.source, metrics: createMetrics(), log: (message) => logged.push(message), batch: { ...BATCH, ms: 10 }, retryMs: 10 });
    try {
      await until("the batch's commit", () => group.committed.length > 0);
    } finally {
      await intake.stop();
    }
    const lake = await openLake(config, { readOnly: true });
    try {
      const counts = [];
      for (const service of services) counts.push((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n);
      return { logged, counts };
    } finally {
      lake.close();
    }
  }

  test("a batch whose write loses the lake's write lock, again as its records are written alone, is read again whole, none of it skipped", async () => {
    const services = ["lock-first", "lock-second", "lock-third"];
    // The lock's connection lost as the write is about to commit, as its fence finds it.
    const { logged, counts } = await committedAfter(services.map((service, offset) => logRecord(19, offset, service, NOW)), 2, () => new WriteLockLost(new Error("Connection terminated unexpectedly")), services);
    assert.ok(!logged.includes("telemetry intake skipped a record whose rows it could not write"), logged.join("\n"));
    assert.deepEqual(counts, [1n, 1n, 1n]);
  });

  test("a record whose write DuckDB fails for its own memory is read again, not skipped", async () => {
    const { logged, counts } = await committedAfter([logRecord(23, 0, "memory", NOW)], 2, () => new Error("Out of Memory Error: failed to offload data block of size 256.0 KiB"), ["memory"]);
    assert.ok(!logged.includes("telemetry intake skipped a record whose rows it could not write"), logged.join("\n"));
    assert.deepEqual(counts, [1n]);
  });

  test("a record whose write fails, not for its rows nor as the lake fails, is written again once the lake has had a while, not skipped", async () => {
    const { logged, counts } = await committedAfter([logRecord(20, 0, "once-failed", NOW)], 2, () => new Error("a failure of neither"), ["once-failed"]);
    assert.ok(!logged.includes("telemetry intake skipped a record whose rows it could not write"), logged.join("\n"));
    assert.deepEqual(counts, [1n]);
  });

  test("a batch whose records' rows, each within a record's bound, come together past the intake's DuckDB's memory and its spill, is written in parts", async () => {
    // Each a record of some 10 MiB of rows, a resource's attributes of 200 KiB repeated on 50 log records, of 200 KiB on the wire.
    const amplified = (offset: number): LogRecord => record("otel.logs", 21, offset, encodeRequest("logs", {
      resourceLogs: [{
        resource: { attributes: [{ key: "service.name", value: { stringValue: "parts" } }, { key: "k8s.pod.annotation", value: { stringValue: "x".repeat(200 * 1024) } }] },
        scopeLogs: [{ logRecords: Array.from({ length: 50 }, (_, index) => ({ timeUnixNano: String(NOW + BigInt(offset * 100 + index)), body: { stringValue: "an event" } })) }],
      }],
    }));
    const records = Array.from({ length: 24 }, (_, offset) => amplified(offset));
    assert.ok(records.every(({ value }) => requestRowBytes("logs", value) > 9 * 1024 * 1024 && requestRowBytes("logs", value) < REQUEST_ROW_BYTES));
    // Handed out again after each rejoin, as the group hands out what it was not told of.
    const group = member([records, records, records]);
    // Its DuckDB of 128 MB, spilling 16 MB at most: twenty-four such records, some 240 MiB staged at once, are past both.
    const open = async () => {
      const lake = await openLake(config, { memoryLimit: "128MB" });
      await lake.db.run("set max_temp_directory_size = '16MB'");
      await ensureOtel(lake.db);
      return lake;
    };
    const intake = startIntake({ open, join: async () => group.source, metrics: createMetrics(), log: () => {}, batch: { ...BATCH, ms: 10, rowBytes: 16 * 1024 * 1024 }, retryMs: 10 });
    try {
      await until("the batch's commit", () => group.committed.length > 0);
    } finally {
      await intake.stop();
    }
    const lake = await openLake(config, { readOnly: true });
    try {
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = 'parts'`)).n, BigInt(records.length * 50));
    } finally {
      lake.close();
    }
  });

  test("the intake is not well once its batches have failed one after another past failingMs, though its loop comes round", async () => {
    const group = member(Array.from({ length: 1000 }, () => [logRecord(22, 0, "failing", NOW)]));
    // A lake whose write lock is never held as a write commits.
    const open = async () => {
      const lake = await openForIntake();
      const db = new Proxy(lake.db, {
        get(target, key, receiver) {
          const value = Reflect.get(target, key, receiver);
          if (key !== "run") return typeof value === "function" ? value.bind(target) : value;
          return async (sql: string, ...rest: unknown[]) => {
            if (sql === "commit") throw new WriteLockLost(new Error("Connection terminated unexpectedly"));
            return value.call(target, sql, ...rest);
          };
        },
      });
      return { db, close: () => lake.close() };
    };
    let failures = 0;
    const log = (message: string) => {
      if (message === "telemetry intake lost the lake or the log") failures += 1;
    };
    const intake = startIntake({ open, join: async () => group.source, metrics: createMetrics(), log, batch: { ...BATCH, ms: 10 }, retryMs: 10, failingMs: 1000 });
    try {
      await until("a batch's failure", () => failures > 0);
      assert.equal(intake.health().ok, true);
      await new Promise((resolve) => setTimeout(resolve, 1500));
      assert.ok(failures > 1, `${failures} failures`);
      const { ok, detail } = intake.health();
      assert.equal(ok, false);
      assert.match(detail, /^its batches have failed for \d+ s, the last as the lake's write lock was not held: Connection terminated unexpectedly$/u);
    } finally {
      await intake.stop();
    }
  });

  test("the intake is well while its loop comes round, and not once it stops", async () => {
    // A member whose batch never comes, until it is closed.
    let gathered = false;
    let closing = () => {};
    const stuck: IntakeSource = {
      next() {
        gathered = true;
        return new Promise((resolve) => {
          closing = () => resolve([]);
        });
      },
      async commit() {},
      async rewind() {
        return false;
      },
      async offsetsAt() {
        return [];
      },
      async start() {
        return 0n;
      },
      async lag() {
        return new Map();
      },
      async close() {
        closing();
      },
    };
    const intake = startIntake({ open: openForIntake, join: async () => stuck, metrics: createMetrics(), log: () => {}, stallMs: 200 });
    try {
      assert.deepEqual(intake.health(), { ok: true, detail: "its loop comes round" });
      await until("the intake gathering", () => gathered);
      await new Promise((resolve) => setTimeout(resolve, 300));
      const { ok, detail } = intake.health();
      assert.equal(ok, false);
      assert.match(detail, /^its loop has not come round for \d+ s$/u);
    } finally {
      await intake.stop();
    }
  });

  describe("written within a bound of the intake's memory, whatever its records' size", () => {
    const MiB = 1024 * 1024;
    const attribute = (key: string, value: string) => ({ key, value: { stringValue: value } });
    /** A scrape of `points` points of a service's, each with its attributes, as the collector's scraper exports them. */
    const scrape = (service: string, index: number, points: number): MetricsRequest => ({
      resourceMetrics: [{
        resource: { attributes: [attribute("service.name", service), attribute("k8s.pod.name", `pod-${index % 7}`)] },
        scopeMetrics: [{
          metrics: Array.from({ length: Math.ceil(points / 10) }, (_, metric) => ({
            name: `scraped_${metric}`,
            gauge: {
              dataPoints: Array.from({ length: Math.min(10, points - metric * 10) }, (_, point) => ({
                attributes: ["collection", "type", "rp", "instance", "job", "pod", "namespace", "le"].map((key) => attribute(key, `${key}-${point}`)),
                timeUnixNano: String(NOW + BigInt(index)),
                asDouble: point,
              })),
            },
          })),
        }],
      }],
    });

    /**
     * Has the intake write `records`, a batch: how much more memory the process held at most as
     * it did, in all and of JavaScript's heap, and the rows of `service` in `table` then.
     */
    async function writing(records: LogRecord[], service: string, table: string): Promise<{ grew: number; heap: number; rows: bigint }> {
      const group = member([records]);
      const before = process.memoryUsage();
      const peak = { rss: before.rss, heap: before.heapUsed };
      const sampling = setInterval(() => {
        const now = process.memoryUsage();
        peak.rss = Math.max(peak.rss, now.rss);
        peak.heap = Math.max(peak.heap, now.heapUsed);
      }, 20);
      // As the intake opens it, its DuckDB held to the intake's memory.
      const open = async () => {
        const lake = await openLake(config, { memoryLimit: DUCKDB_MEMORY });
        await ensureOtel(lake.db);
        return lake;
      };
      const intake = startIntake({ open, join: async () => group.source, metrics: createMetrics(), log: () => {}, batch: { ...BATCH, ms: 10 } });
      try {
        await until("the batch's commit", () => group.committed.length > 0);
      } finally {
        clearInterval(sampling);
        await intake.stop();
      }
      const lake = await openLake(config, { readOnly: true });
      try {
        const rows = (await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.${table} where ServiceName = '${service}'`)).n;
        return { grew: peak.rss - before.rss, heap: peak.heap - before.heapUsed, rows };
      } finally {
        lake.close();
      }
    }

    /**
     * What a batch may hold: of JavaScript's heap, a part's rows and a chunk's of a table, some
     * tens of MiB; in all, that, what its DuckDB holds of the batch, its memory limit, past
     * which it spills, and what the heap and the process hold beside. Decoded whole, the
     * largest record of points took 166 MiB of heap, and a batch staged a value at a time over
     * a GB in all.
     */
    const HEAP_BOUND = 96 * MiB;
    const BOUND = Number.parseInt(DUCKDB_MEMORY, 10) * 1_000_000 + HEAP_BOUND + 96 * MiB;
    const bounded = (what: string, { grew, heap }: { grew: number; heap: number }) => {
      assert.ok(heap < HEAP_BOUND, `${what} took ${Math.round(heap / MiB)} MiB more of the heap`);
      assert.ok(grew < BOUND, `${what} took ${Math.round(grew / MiB)} MiB more`);
    };

    test("the largest record the log takes, a scrape's points, a part at a time", async () => {
      // As many points as come within the record's bytes: some 50,000.
      let points = Math.floor((LOG_RECORD_BYTES / encodeRequest("metrics", scrape("largest-scrape", 0, 1000)).length) * 1000);
      let value = encodeRequest("metrics", scrape("largest-scrape", 0, points));
      for (; value.length > LOG_RECORD_BYTES; value = encodeRequest("metrics", scrape("largest-scrape", 0, points))) points -= 100;
      assert.ok(value.length <= LOG_RECORD_BYTES, `a record of ${value.length} bytes`);
      const written = await writing([record("otel.metrics", 11, 0, value)], "largest-scrape", "metrics_gauge");
      assert.equal(written.rows, BigInt(points));
      bounded(`writing a record of ${points} points`, written);
    });

    test("the largest record the log takes, one log record's body", async () => {
      const body = "x".repeat(LOG_RECORD_BYTES - 1024);
      const value = encodeRequest("logs", {
        resourceLogs: [{ resource: { attributes: [attribute("service.name", "largest-log")] }, scopeLogs: [{ logRecords: [{ timeUnixNano: String(NOW), body: { stringValue: body } }] }] }],
      });
      assert.ok(value.length <= LOG_RECORD_BYTES, `a record of ${value.length} bytes`);
      const written = await writing([record("otel.logs", 12, 0, value)], "largest-log", "logs");
      assert.equal(written.rows, 1n);
      bounded(`writing a log record of ${body.length} bytes`, written);
    });

    test("a backlog's batch of scrapes, as large as a batch takes, a data chunk at a time, not a DuckDB value a cell", async () => {
      const records: LogRecord[] = [];
      for (let bytes = 0; bytes < BATCH.bytes; bytes += records.at(-1)?.value.length ?? 0) {
        records.push(record("otel.metrics", 13, records.length, encodeRequest("metrics", scrape("backlog", records.length, 2_000))));
      }
      const written = await writing(records, "backlog", "metrics_gauge");
      assert.equal(written.rows, BigInt(records.length * 2_000));
      bounded(`writing ${records.length * 2_000} points`, written);
    });
  });

  test("a maintenance pass cuts the days it keeps of the one time it is given", async () => {
    await withLake(async (db) => {
      const service = `cut-${Date.now()}`;
      await writeConsumed(db, [{ topic: "otel.logs", topicId: "otel.logs-id", partition: 33, offset: 0n, rows: [telemetryRows("logs", logRequest(service, NOW - 30n * DAY, NOW - 29n * DAY))] }], fromStart);
      // A pass a day on from the rows' now: the day 30 days before that is past its 30 days, the one 29 before is not.
      const now = new Date(Number((NOW + DAY) / 1_000_000n));
      await maintainLake(db, { retentionDays: 30, keepFiles: true, now });
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = '${service}'`)).n, 1n);
    });
  });

  test("maintenance deletes the days of telemetry past its retention, and then their files", async () => {
    await withLake(async (db) => {
      const days = [40n, 31n, 30n, 0n];
      await writeConsumed(db, [{ topic: "otel.logs", topicId: "otel.logs-id", partition: 7, offset: 0n, rows: [telemetryRows("logs", logRequest("retained", ...days.map((ago) => NOW - ago * DAY)))] }], fromStart);
      const day = (ago: bigint) => new Date(Number((NOW - ago * DAY) / 1_000_000n));
      const partition = (date: Date) => join(dataDir ?? "", "otel", "logs", `year=${date.getUTCFullYear()}`, `month=${date.getUTCMonth() + 1}`, `day=${date.getUTCDate()}`);
      const files = (ago: bigint) => readdirSync(partition(day(ago))).filter((name) => name.endsWith(".parquet"));
      assert.ok(files(40n).length > 0);
      // Nothing kept past its need, so maintenance deletes what it may at once.
      await db.run(`call ${LAKE}.set_option('expire_older_than', '0 seconds')`);
      await db.run(`call ${LAKE}.set_option('delete_older_than', '0 seconds')`);

      // Of the one time the test's rows are of, whatever the clock says as it runs, across midnight too.
      const now = new Date(Number(NOW / 1_000_000n));
      await maintainLake(db, { retentionDays: 30, keepFiles: false, now });
      const kept = await rows<{ day: Date }>(db, `select Timestamp::DATE::TIMESTAMP as day from ${LAKE}.otel.logs where ServiceName = 'retained' order by Timestamp`);
      assert.deepEqual(kept.map(({ day }) => day.toISOString().slice(0, 10)), [30n, 0n].map((ago) => day(ago).toISOString().slice(0, 10)));
      // The pass after the one that deleted them deletes their files, once their snapshots expire.
      await new Promise((resolve) => setTimeout(resolve, 1000));
      await maintainLake(db, { retentionDays: 30, keepFiles: false, now });
      assert.deepEqual([files(40n), files(31n)], [[], []]);
      assert.ok(files(30n).length > 0);
    });
  });
});

test("the lake's image has its DuckDB's allocator give back the memory it frees at once", () => {
  const lake = join(import.meta.dirname, "../neon/lake");
  const conf = /^\s*DUCKDB_JE_MALLOC_CONF=(\S+)$/mu.exec(readFileSync(join(lake, "Dockerfile"), "utf8"))?.[1];
  assert.ok(conf, "the image sets no DUCKDB_JE_MALLOC_CONF");
  // jemalloc says the options it runs with as the process exits.
  const opened = spawnSync(process.execPath, ["--input-type=module", "-e", "import { DuckDBInstance } from '@duckdb/node-api'; await (await DuckDBInstance.create(':memory:')).connect(); process.exit(0);"], {
    cwd: lake,
    env: { ...process.env, DUCKDB_JE_MALLOC_CONF: `${conf},stats_print:true` },
    encoding: "utf8",
  });
  assert.equal(opened.status, 0, opened.stderr);
  assert.match(opened.stderr, /^\s*opt\.dirty_decay_ms: 0 /mu);
  assert.match(opened.stderr, /^\s*opt\.muzzy_decay_ms: 0 /mu);
});

test("the lake's writes take turns, whichever DuckDB makes them", async () => {
  const instances = await Promise.all([DuckDBInstance.create(":memory:"), DuckDBInstance.create(":memory:")]);
  const [first, second] = await Promise.all(instances.map((instance) => instance.connect()));
  assert.ok(first && second);
  const events: string[] = [];
  let release = () => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const one = transaction(first, async () => {
    events.push("first began");
    await held;
    events.push("first ends");
  });
  const other = transaction(second, async () => {
    events.push("second began");
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.deepEqual(events, ["first began"]);
  release();
  await Promise.all([one, other]);
  assert.deepEqual(events, ["first began", "first ends", "second began"]);
  await assert.rejects(transaction(first, async () => {
    throw new Error("a write that failed");
  }));
  assert.equal(await transaction(second, async () => "the next still runs"), "the next still runs");
  for (const db of [first, second]) db.closeSync();
  for (const instance of instances) instance.closeSync();
});

test("once its write lock is set, every write of the lake waits for it, as every other writer's does, in any process", async () => {
  writeLock(config.catalog);
  const other = new pg.Client({ ...config.catalog });
  await other.connect();
  try {
    // Another writer, the loader's process or another of the intake's, holds the lock.
    await other.query("select pg_advisory_lock(hashtext('alasio.lake.write'))");
    const instance = await DuckDBInstance.create(":memory:");
    const db = await instance.connect();
    const events: string[] = [];
    const write = transaction(db, async () => {
      events.push("began");
    });
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.deepEqual(events, []);
    await other.query("select pg_advisory_unlock(hashtext('alasio.lake.write'))");
    await write;
    assert.deepEqual(events, ["began"]);
    db.closeSync();
    instance.closeSync();
  } finally {
    await other.end();
  }
});

test("a write of the lake whose write lock is lost as it writes commits nothing, and the next takes the lock anew", async () => {
  writeLock(config.catalog);
  const lake = await openLake(config);
  try {
    await transaction(lake.db, () => lake.db.run(`create table ${LAKE}.fenced (id integer)`));
    // The lock's connection lost alone, while the write goes on on the lake's.
    await assert.rejects(transaction(lake.db, async () => {
      await lake.db.run(`insert into ${LAKE}.fenced values (1)`);
      const { rowCount } = await admin.query("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and granted");
      assert.equal(rowCount, 1);
    }));
    assert.deepEqual(await rows(lake.db, `from ${LAKE}.fenced`), []);
    await transaction(lake.db, () => lake.db.run(`insert into ${LAKE}.fenced values (2)`));
    assert.deepEqual(await rows(lake.db, `from ${LAKE}.fenced`), [{ id: 2 }]);
  } finally {
    await transaction(lake.db, () => lake.db.run(`drop table if exists ${LAKE}.fenced`));
    lake.close();
  }
});

test("a write of the lake waits for its write lock only so long, and then fails", async () => {
  writeLock(config.catalog, 500);
  const other = new pg.Client({ ...config.catalog });
  await other.connect();
  const instance = await DuckDBInstance.create(":memory:");
  const db = await instance.connect();
  try {
    // Another writer holds the lock, and holds it on past the wait.
    await other.query("select pg_advisory_lock(hashtext('alasio.lake.write'))");
    const events: string[] = [];
    // The lock's to fail, not what the write writes: its writer writes it again.
    await assert.rejects(transaction(db, async () => {
      events.push("began");
    }), (error) => error instanceof WriteLockLost && /lock timeout/.test(error.message));
    assert.deepEqual(events, []);
  } finally {
    db.closeSync();
    instance.closeSync();
    await other.end();
    writeLock(config.catalog);
  }
});

test("the lake loads the extension builds its image pins", async () => {
  const lake = await DuckDBInstance.create(":memory:");
  const db = await lake.connect();
  try {
    for (const extension of Object.keys(EXTENSIONS)) {
      await db.run(`install ${extension}`);
      await db.run(`load ${extension}`);
    }
    const loaded = await rows<{ extension_name: string; extension_version: string }>(db, "select extension_name, extension_version from duckdb_extensions() where loaded and install_path <> '(BUILT-IN)' order by 1");
    assert.deepEqual(Object.fromEntries(loaded.map(({ extension_name, extension_version }) => [extension_name, extension_version])), EXTENSIONS);
  } finally {
    db.closeSync();
    lake.closeSync();
  }
});

// --- The lake as a whole -------------------------------------------------------------

describe("the lake", () => {
  test("a lake of another model version is rebuilt empty, and one of this version kept", async () => {
    const lake = await openLake(config, { source: config.source });
    try {
      await prepareLake(lake.db, { rebuild: true });
      await syncLake(lake.db);
      await writeTelemetry(lake.db, telemetryRows("logs", logRequest("rebuilt", NOW)));
      assert.equal(await prepareLake(lake.db), false);
      assert.ok((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.claude.entries`)).n > 0n);
      await lake.db.run(`update ${LAKE}.loader.meta set value = '0' where key = 'model_version'`);
      assert.equal(await prepareLake(lake.db), true);
      assert.equal(await lakeModelVersion(lake.db), MODEL_VERSION);
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.claude.entries`)).n, 0n);
      // Telemetry is not derived, and is kept through any rebuild.
      assert.equal((await one<Count>(lake.db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = 'rebuilt'`)).n, 1n);
    } finally {
      lake.close();
    }
  });

  test("a lake built before its transcripts' row groups were bounded, model version 1, is rebuilt", async () => {
    const lake = await openLake(config, { source: config.source });
    try {
      await prepareLake(lake.db, { rebuild: true });
      await lake.db.run(`update ${LAKE}.loader.meta set value = '1' where key = 'model_version'`);
      assert.equal(await prepareLake(lake.db), true);
    } finally {
      lake.close();
    }
  });

  test("the loader loads once records come on its wake topic, tells its group it has read them only then, and loads nothing while none come", async () => {
    await withLake(async (db) => {
      const metrics = createMetrics();
      const logs: string[] = [];
      const source = wakeSource([[woken(0n), woken(1n)], [], [woken(2n)]]);
      const open = async () => ({ db, close: async () => {}, replayed: async () => true, locked: unlocked });
      const loader = startLoader({ open, join: async () => source, metrics, log: (message) => logs.push(message), gatherMs: 10 });
      await eventually(() => source.told.length === 2);
      await loader.stop();
      assert.deepEqual(logs, ["loaded", "loaded"]);
      assert.deepEqual(source.told, [[woken(2n)], [woken(3n)]]);
      assert.deepEqual(loader.health(), { ok: true, detail: "loaded" });
      const text = metrics.render();
      assert.match(text, /^lake_cycles_total\{outcome="success"\} 2$/m);
      assert.match(text, /^# TYPE lake_rows_total counter$/m);
    });
  });

  test("the loader loads its records once its source has replayed the furthest write they name, and, past its wait, tells its group nothing, reading them again", async () => {
    await withLake(async (db) => {
      const logs: string[] = [];
      // A replica held behind 0/3000 while asked, then caught up.
      let replayedTo = lsnValue("0/1000");
      const asked: bigint[] = [];
      const replayed = async (lsn: bigint) => {
        asked.push(lsn);
        return replayedTo >= lsn;
      };
      const source = wakeSource([[recordAt(woken(0n), "0/2000"), recordAt(woken(1n), "0/3000"), recordAt(woken(2n))]]);
      let synced = 0;
      const sync = async (lake: DuckDBConnection) => {
        synced += 1;
        return syncLake(lake);
      };
      const loader = startLoader({ open: async () => ({ db, close: async () => {}, replayed, locked: unlocked }), join: async () => source, metrics: createMetrics(), log: (message) => logs.push(message), gatherMs: 10, replayWaitMs: 60_000, replayPollMs: 10, sync });
      await eventually(() => asked.length >= 3);
      assert.deepEqual([synced, source.told], [0, []]);
      replayedTo = lsnValue("0/3000");
      await eventually(() => source.told.length === 1);
      await loader.stop();
      assert.deepEqual([synced, source.told, asked.every((lsn) => lsn === lsnValue("0/3000"))], [1, [[woken(3n)]], true]);
      // Never replayed within the wait: the load fails, its records told of nothing.
      const behind = wakeSource([[recordAt(woken(3n), "0/4000")]]);
      const stuck = startLoader({ open: async () => ({ db, close: async () => {}, replayed, locked: unlocked }), join: async () => behind, metrics: createMetrics(), log: (message) => logs.push(message), gatherMs: 10, replayWaitMs: 50, replayPollMs: 10, retryMs: 60_000, sync });
      await eventually(() => logs.includes("load failed"));
      await stuck.stop();
      assert.deepEqual([synced, behind.told], [1, []]);
    });
  });

  test("a wake record names the primary's WAL position, which the loader reads, as Postgres's source answers how far it replayed", async () => {
    assert.deepEqual(furthestWrite([recordAt(woken(0n), "0/16B3748"), recordAt(woken(1n), "1/0"), recordAt(woken(2n))]), { furthest: lsnValue("1/0"), malformed: 0 });
    assert.deepEqual(furthestWrite([recordAt(woken(0n)), { ...woken(1n), value: Buffer.from("{}") }]), { furthest: null, malformed: 0 });
    // Neither alasio's record nor a position: skipped, and counted, the others still waited for.
    assert.deepEqual(furthestWrite([{ ...woken(0n), value: Buffer.from("not json") }, recordAt(woken(1n), "16"), recordAt(woken(2n), "0/10")]), { furthest: lsnValue("0/10"), malformed: 2 });
    assert.equal(lsnValue("16/B374D848"), (0x16n << 32n) + 0xb374d848n);
    assert.throws(() => lsnValue("16"), /is no WAL position/u);
    // A primary, as the tests' Postgres is, replays nothing: it is as far as any write.
    const client = await admin.connect();
    try {
      assert.equal(await sourceReplayed(client, lsnValue("FFFF/0")), true);
    } finally {
      client.release();
    }
  });

  test("a load whose loader's lock is lost as it loads commits nothing, and tells its group nothing, so that its records are read again", async () => {
    const unfenced = { projectKey: "-home-operator-unfenced", sessionId: "44444444-4444-4444-8444-444444444444" };
    await withLake(async (db) => {
      await store.append(unfenced, [{ type: "user", uuid: "unfenced-0", timestamp: at(0), message: { role: "user", content: "entry" } }]);
      const failures: unknown[] = [];
      const source = wakeSource([[woken(0n)]]);
      // The lock's connection cut as the load begins, as a compute restarting or an operator's terminating it would.
      const sync = async (lake: DuckDBConnection, options: LakeSyncOptions) => {
        await admin.query("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and granted and objsubid = 1 and objid = oid(hashtext($1)::bigint & 4294967295)", ["alasio.lake.loader"]);
        return syncLake(lake, options);
      };
      const loader = startLoader({
        open: (signal) => openLoaderLake(config, () => {}, signal),
        join: async () => source,
        metrics: createMetrics(),
        log: (message, fields) => void (message === "load failed" && failures.push(fields?.["error"])),
        gatherMs: 10,
        retryMs: 60_000,
        sync,
      });
      await eventually(() => failures.length === 1);
      await loader.stop();
      assert.match(String(failures[0]), /the loader's lock was lost/u);
      assert.deepEqual(source.told, []);
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.claude.entries where uuid = 'unfenced-0'`)).n, 0n);
    });
  });

  test("the loader tells its group each partition is read past the last record it loaded for", () => {
    const other = (offset: bigint): Position => ({ topic: "lake.main", partition: 1, offset });
    assert.deepEqual(readUpTo([woken(4n), woken(2n), other(9n), woken(7n)]), [woken(8n), other(10n)]);
    assert.deepEqual(readUpTo([]), []);
  });

  test("maintenance's daily pass deletes the telemetry past its retention and runs DuckLake's maintenance; its merge pass merges the telemetry's files", async () => {
    await withLake(async (db) => {
      const logs: string[] = [];
      const log = (message: string) => void logs.push(message);
      await writeTelemetry(db, telemetryRows("logs", logRequest("expired", NOW - 40n * DAY)));
      assert.equal(await runPass(db, "maintain", passing(log)), true);
      assert.equal((await one<Count>(db, `select count(*) as n from ${LAKE}.otel.logs where ServiceName = 'expired'`)).n, 0n);
      assert.equal(await runPass(db, "merge", passing(log)), true);
      assert.deepEqual(logs, ["maintained", "merged the telemetry's files"]);
    });
  });

  test("maintenance merges the files written since its last pass, and leaves one already as large as it merges files to", async () => {
    await withLake(async (db) => {
      const { value } = await one<{ value: string }>(db, `select value from ducklake_options('${LAKE}') where option_name = 'target_file_size' and scope = 'GLOBAL'`);
      const target = BigInt(value);
      // Entries of 7 kB that hardly compress, as many as make a file past the target, as
      // a table's history comes to be; then a day's loads, each a small file.
      const entries = (from: number, count: number) =>
        db.run(`insert into ${LAKE}.claude.entries (seq, entry)
          select i, to_json(array_to_string(list_transform(range(220), x -> md5((i * 220 + x)::VARCHAR)), ''))
          from range(${from}, ${from + count}) t(i)`);
      await entries(0, Number(target / 7000n) + 1000);
      for (const [from, count] of [[1_000_000, 30], [1_000_100, 40], [1_000_200, 50]] as const) await entries(from, count);
      const files = async () => rows<{ data_file: string; size: bigint }>(db, `select data_file, data_file_size_bytes as size from ducklake_list_files('${LAKE}', 'entries', schema => 'claude')`);
      const large = (await files()).filter(({ size }) => size >= target).map(({ data_file }) => data_file);
      assert.ok(large.length > 0);
      await maintainLake(db, { retentionDays: 30, keepFiles: false });
      const after = await files();
      assert.deepEqual(after.filter(({ data_file }) => large.includes(data_file)).map(({ data_file }) => data_file).sort(), large.sort());
      assert.equal(after.length, large.length + 1);
    });
  });

  test("transcripts and rollouts are written in row groups of ROW_GROUP_ROWS rows", async () => {
    await withLake(async (db) => {
      await db.run(`insert into ${LAKE}.claude.entries (seq) select i from range(${ROW_GROUP_ROWS + 1}) t(i)`);
      await db.run(`insert into ${LAKE}.codex.lines (line_number) select i from range(${ROW_GROUP_ROWS + 1}) t(i)`);
      for (const [schema, table] of [["claude", "entries"], ["codex", "lines"]] as const) {
        const [file, ...others] = await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', '${table}', schema => '${schema}')`);
        assert.ok(file && others.length === 0);
        const groups = await rows<{ n: bigint }>(db, `select distinct row_group_id, row_group_num_rows as n from parquet_metadata('${file.data_file}') order by row_group_id`);
        assert.deepEqual(groups.map(({ n }) => n), [BigInt(ROW_GROUP_ROWS), 1n], `${schema}.${table}`);
      }
    });
  });

  test("while Neon has branches maintenance deletes no file, nor when it cannot learn them, and says so; a pass that fails says so and fails", async () => {
    await withLake(async (db) => {
      const logs: string[] = [];
      const log = (message: string, fields?: Record<string, unknown>) => void logs.push(`${message}${fields ? ` ${JSON.stringify(fields)}` : ""}`);
      const maintained = async (branches: () => Promise<readonly string[]>) => {
        assert.equal(await runPass(db, "maintain", passing(log, branches)), true);
        return logs.splice(0);
      };
      assert.deepEqual(await maintained(async () => ["try-codex"]), ['maintained {"keptFilesFor":["try-codex"]}']);
      assert.deepEqual(await maintained(async () => {
        throw new Error("neon-control answered 503");
      }), ['could not learn Neon\'s branches; keeping every file {"error":"neon-control answered 503"}', 'maintained {"keptFilesFor":"unknown branches"}']);
      assert.deepEqual(await maintained(async () => []), ["maintained {}"]);
      // Its lake closed under it, a pass fails, and says why.
      const lake = await openLake(config);
      lake.close();
      assert.equal(await runPass(lake.db, "merge", passing(log)), false);
      assert.match(logs.join("\n"), /^merging the telemetry's files failed \{"error":/u);
    });
  });

  test("the reconciliation forgets the files the store lost, says what each table lost, loads again what the transcripts lost, and asks the log again for what the telemetry lost, saying what the log no longer holds, after which the passes that failed on them do not", async () => {
    const name = "rollout-2026-10-01T12-00-00-thread-reconciled.jsonl";
    const reconciled = { projectKey: "-home-operator-reconciled", sessionId: "22222222-2222-4222-8222-222222222222" };
    // More rows than DuckLake inlines in its catalog, so each load writes a file.
    const entries = (from: number) => Array.from({ length: 20 }, (_, n) => ({ type: "user", uuid: `r${from + n}`, timestamp: at(n), message: { role: "user", content: `entry ${from + n}` } }));
    const lines = Array.from({ length: 40 }, (_, ordinal) => line({ timestamp: at(ordinal % 60), ordinal, type: "response_item", payload: { type: "message" } }));
    const firstPart = Buffer.from(lines.slice(0, 20).join(""));
    await withLake(async (db) => {
      try {
        // Two loads of each, so each table has a file the store keeps and one it loses.
        await store.append(reconciled, entries(0));
        await saveRollout(name, firstPart);
        await syncLake(db);
        await writeTelemetry(db, telemetryRows("logs", logRequest("kept", NOW)));
        // Each table's files before the write of the one the store loses. The telemetry's
        // tables outlive withLake's rebuild, so they hold the files of the tests before this.
        const filesOf = async (schema: string, table: string) =>
          (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', '${table}', schema => '${schema}')`)).map(({ data_file }) => data_file);
        const kept = new Set([...await filesOf("claude", "entries"), ...await filesOf("codex", "lines"), ...await filesOf("otel", "logs")]);
        await store.append(reconciled, entries(20));
        await saveRollout(name, Buffer.from(lines.slice(20).join("")), { start: firstPart.length, head: Buffer.from(lines.join("")) });
        await syncLake(db);
        await writeTelemetry(db, telemetryRows("logs", logRequest("lost", NOW, NOW + 1n)));
        const { since: lostSince } = await one<{ since: string }>(db, `select min(Timestamp)::varchar as since from ${LAKE}.otel.logs where ServiceName = 'lost'`);
        const counts = async () => one<Record<string, bigint>>(db, `select
          (select count(*) from ${LAKE}.claude.entries) as entries, (select count(distinct seq) from ${LAKE}.claude.entries) as seqs,
          (select count(*) from ${LAKE}.claude.content_blocks) as blocks, (select count(*) from ${LAKE}.codex.lines) as lines`);
        const whole = await counts();
        const written = async (schema: string, table: string) => (await filesOf(schema, table)).filter((file) => !kept.has(file));
        const lost = [await written("claude", "entries"), await written("codex", "lines"), await written("otel", "logs")];
        for (const files of lost) {
          assert.equal(files.length, 1);
          rmSync(files[0]!);
        }
        await assert.rejects(rows(db, `select sum(seq) from ${LAKE}.claude.entries`), /No such file|IO Error/iu);
        const logs: string[] = [];
        const log = (message: string, fields?: Record<string, unknown>) => void logs.push(`${message}${fields ? ` ${JSON.stringify(fields)}` : ""}`);
        // A pass on a file the store lost fails, naming what sets it right.
        assert.equal(await runPass(db, "merge", passing(log)), false);
        assert.match(logs.splice(0).join("\n"), /^merging the telemetry's files failed .*"because":"a file of the lake's catalog is gone from the object store: once you know why, alasio lake reconcile forgets it/mu);

        // Its write lock lost as it forgets them, as where the lock's connection drops alone, it forgets none.
        writeLock(config.catalog);
        const writer = new pg.Client({ ...config.catalog });
        await writer.connect();
        // Another writer's commit under way, which the reconciliation's lock of the catalog's snapshots waits for.
        await writer.query("begin");
        await writer.query(`insert into ducklake.ducklake_snapshot
          select snapshot_id + 1, now(), schema_version, next_catalog_id, next_file_id from ducklake.ducklake_snapshot order by snapshot_id desc limit 1`);
        const fenced = runPass(db, "reconcile", passing(log));
        for (const deadline = Date.now() + 30_000; (await writer.query("select 1 from pg_locks where relation = 'ducklake.ducklake_snapshot'::regclass and not granted")).rowCount === 0;) {
          assert.ok(Date.now() < deadline, "the reconciliation never waited on the catalog's snapshots");
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        const { rowCount } = await admin.query("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and granted");
        assert.equal(rowCount, 1);
        await writer.query("rollback");
        await writer.end();
        assert.equal(await fenced, false);
        assert.match(logs.splice(0).join("\n"), /forgetting the lost files in the catalog failed/u);
        await assert.rejects(rows(db, `select sum(seq) from ${LAKE}.claude.entries`), /No such file|IO Error/iu);

        const gauges: [string, number, Record<string, string>][] = [];
        // Another writer commits a snapshot of its own as the reconciliation runs: it takes the next after it.
        const other = new pg.Client({ ...config.catalog });
        await other.connect();
        // The catalog's snapshots past a power of ten, as a lake's run, whose last is the greatest, not the last in their text's order.
        const lastSnapshot = async () => Number((await other.query<{ id: string }>("select max(snapshot_id)::text as id from ducklake.ducklake_snapshot")).rows[0]?.id);
        for (const past = 10 ** String(await lastSnapshot()).length; (await lastSnapshot()) < past;) {
          await other.query(`insert into ducklake.ducklake_snapshot
            select snapshot_id + 1, now(), schema_version, next_catalog_id, next_file_id from ducklake.ducklake_snapshot order by snapshot_id desc limit 1`);
        }
        await other.query("begin");
        await other.query(`insert into ducklake.ducklake_snapshot
          select snapshot_id + 1, now(), schema_version, next_catalog_id, next_file_id from ducklake.ducklake_snapshot order by snapshot_id desc limit 1`);
        const committed = new Promise((resolve) => setTimeout(resolve, 500)).then(() => other.query("commit")).finally(() => other.end());
        // An intake, which takes up what the reconciliation asks of the log, as it finds the log
        // no longer holds a minute of it; what was asked, as it took it up.
        const intake = await openLake(config);
        const gap = { signal: "logs", from: "2026-10-01T00:00:00.000Z", to: "2026-10-01T00:01:00.000Z" } as const;
        const takenUp = (async () => {
          for (const deadline = Date.now() + 60_000; Date.now() < deadline; await new Promise((resolve) => setTimeout(resolve, 100))) {
            const asked = await rereadsAsked(intake.db);
            if (asked.length === 0) continue;
            const fromLost = await rows(intake.db, `select Signal as signal, Since = $1::TIMESTAMP_NS as fromLost from ${LAKE}.otel.intake_rereads`, [lostSince]);
            await readAgainFrom(intake.db, asked, [], [gap]);
            return fromLost;
          }
          return null;
        })().finally(() => intake.close());
        assert.equal(await runPass(db, "reconcile", { ...passing(log), telemetry: { gauge: (...gauge) => void gauges.push(gauge) } }), true);
        await committed;
        const said = logs.splice(0).map((line) => ({ message: line.slice(0, line.indexOf(" {")), ...JSON.parse(line.slice(line.indexOf(" {"))) }) as Record<string, unknown>);
        const lostOf = (table: string) => said.find((event) => event["table"] === table);
        assert.deepEqual([lostOf("claude.entries")?.["files"], lostOf("claude.entries")?.["recovered"]], [1, "database"]);
        assert.deepEqual([lostOf("codex.lines")?.["files"], lostOf("codex.lines")?.["recovered"]], [1, "database"]);
        assert.deepEqual([lostOf("otel.logs")?.["files"], lostOf("otel.logs")?.["rows"], lostOf("otel.logs")?.["recovered"]], [1, 2, "log"]);
        // The intake asked to read the logs' topic again from the earliest time the lost file
        // held; and what it found the log no longer holds, said.
        assert.deepEqual(await takenUp, [{ signal: "logs", fromLost: true }]);
        assert.deepEqual(said.filter(({ message }) => message === "lost from the object store, and from the log, which no longer holds it"), [
          { message: "lost from the object store, and from the log, which no longer holds it", ...gap },
        ]);
        assert.deepEqual(gauges.filter(([gauge, , { table }]) => gauge === "lake_lost_rows" && table === "otel.logs").map(([, value]) => value), [2]);

        // The transcripts whole again, each entry once; the telemetry the store kept, and none it lost.
        assert.deepEqual(await counts(), whole);
        assert.deepEqual(await rows(db, `select ServiceName as service from ${LAKE}.otel.logs where ServiceName in ('kept', 'lost')`), [{ service: "kept" }]);
        assert.equal(await runPass(db, "merge", passing(log)), true);
        // Nothing more to set right.
        assert.equal(await runPass(db, "reconcile", passing(log)), true);
        assert.deepEqual(logs.splice(0).at(-1), 'reconciled {"lost":0}');
      } finally {
        // What it asked of the log, and what the log no longer held, are no later test's.
        await db.run(`delete from ${LAKE}.otel.intake_rereads`);
        await db.run(`delete from ${LAKE}.otel.intake_reread_gaps`);
      }
    });
  });

  test("the reconciliation says the telemetry it asked the log for again is still to be read where no intake takes it up within its wait", async () => {
    await withLake(async (db) => {
      try {
        await writeTelemetry(db, telemetryRows("logs", logRequest("pending-kept", NOW)));
        const kept = new Set((await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'logs', schema => 'otel')`)).map(({ data_file }) => data_file));
        await writeTelemetry(db, telemetryRows("logs", logRequest("pending-lost", NOW)));
        const lost = (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'logs', schema => 'otel')`)).map(({ data_file }) => data_file).filter((file) => !kept.has(file));
        assert.equal(lost.length, 1);
        rmSync(lost[0]!);
        const logs: string[] = [];
        assert.equal(await runPass(db, "reconcile", { ...passing((message, fields) => void logs.push(`${message} ${JSON.stringify(fields ?? {})}`)), rereadWaitMs: 300 }), true, logs.join("\n"));
        assert.ok(logs.includes('lost from the object store, still to be read again from the log {"signals":["logs"]}'), logs.join("\n"));
      } finally {
        await db.run(`delete from ${LAKE}.otel.intake_rereads`);
      }
    });
  });

  test("a branch environment's reconciliation asks no log again, as its lake has no intake, and says main's lake reads its telemetry again", async () => {
    await withLake(async (db) => {
      // Before any reconciliation lost anything, its record not made yet.
      const catalog = new pg.Client({ ...config.catalog });
      await catalog.connect();
      try {
        await catalog.query("drop schema if exists reconcile cascade");
      } finally {
        await catalog.end();
      }
      assert.equal(await runPass(db, "reconcile", { ...passing(() => {}), branch: true }), true);
      await writeTelemetry(db, telemetryRows("logs", logRequest("branch-kept", NOW)));
      const kept = new Set((await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'logs', schema => 'otel')`)).map(({ data_file }) => data_file));
      await writeTelemetry(db, telemetryRows("logs", logRequest("branch-lost", NOW)));
      const lost = (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'logs', schema => 'otel')`)).map(({ data_file }) => data_file).filter((file) => !kept.has(file));
      assert.equal(lost.length, 1);
      rmSync(lost[0]!);
      const logs: string[] = [];
      const started = Date.now();
      // Its whole wait for an intake, were it to wait.
      assert.equal(await runPass(db, "reconcile", { ...passing((message, fields) => void logs.push(`${message} ${JSON.stringify(fields ?? {})}`)), branch: true, rereadWaitMs: 60_000 }), true, logs.join("\n"));
      assert.ok(Date.now() - started < 30_000, `it took ${Date.now() - started} ms`);
      const said = logs.find((line) => line.includes('"table":"otel.logs"'));
      assert.match(said ?? "", /^lost from the object store, main's telemetry as of the branch point, which main's lake reads again from the log .*"recovered":"main"/u);
      assert.ok(!logs.some((line) => line.includes("still to be read again")), logs.join("\n"));
      assert.deepEqual(await rows(db, `select * from ${LAKE}.otel.intake_rereads`), []);
    });
  });

  test("the reconciliation loads what the transcripts lost under the loader's lock, waiting while a loader loads, so that each entry is loaded once", async () => {
    const fenced = { projectKey: "-home-operator-fenced", sessionId: "33333333-3333-4333-8333-333333333333" };
    // More rows than DuckLake inlines in its catalog, so each load writes a file.
    const entries = (from: number) => Array.from({ length: 20 }, (_, n) => ({ type: "user", uuid: `fenced-${from + n}`, timestamp: at(n), message: { role: "user", content: `entry ${from + n}` } }));
    await withLake(async (db) => {
      await store.append(fenced, entries(0));
      await syncLake(db);
      const files = async () => (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'entries', schema => 'claude')`)).map(({ data_file }) => data_file);
      const kept = new Set(await files());
      await store.append(fenced, entries(20));
      await syncLake(db);
      const lost = (await files()).filter((file) => !kept.has(file));
      assert.equal(lost.length, 1);
      rmSync(lost[0]!);
      const logs: string[] = [];
      const log = (message: string) => void logs.push(message);
      // A loader holding its lock, which the reconciliation waits for once it has forgotten the file.
      const loader = await takeLoaderLock(config.catalog, () => {});
      const catalog = new pg.Client({ ...config.catalog });
      await catalog.connect();
      let settled = false;
      const reconciled = runPass(db, "reconcile", { ...passing(log), lockRetryMs: 100 }).finally(() => {
        settled = true;
      });
      try {
        const forgotten = async () => (await catalog.query<{ n: number }>("select count(*)::int as n from reconcile.lost_files where path = $1", [lost[0]]).catch(() => ({ rows: [{ n: 0 }] }))).rows[0]?.n ?? 0;
        for (const deadline = Date.now() + 30_000; (await forgotten()) === 0; await new Promise((resolve) => setTimeout(resolve, 100))) {
          assert.ok(Date.now() < deadline && !settled, "the reconciliation never forgot the file");
        }
        // The loader loads meanwhile, from a DuckDB of its own, as one awake beside a restore's reconciliation does.
        await store.append(fenced, entries(40));
        const other = await openLake(config, { source: config.source });
        try {
          await syncLake(other.db);
        } finally {
          other.close();
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
        assert.equal(settled, false, "the reconciliation loaded while the loader held its lock");
      } finally {
        await loader.release();
        await catalog.end();
      }
      assert.equal(await reconciled, true, logs.join("\n"));
      const { n, uuids } = await one<{ n: bigint; uuids: bigint }>(db, `select count(*) as n, count(distinct uuid) as uuids from ${LAKE}.claude.entries where uuid like 'fenced-%'`);
      assert.deepEqual([n, uuids], [60n, 60n]);
    });
  });

  test("a reconciliation whose loader's lock is lost as it loads again commits nothing of that load, and the next loads it whole", async () => {
    const cut = { projectKey: "-home-operator-cut", sessionId: "55555555-5555-4555-8555-555555555555" };
    const entries = (from: number) => Array.from({ length: 20 }, (_, n) => ({ type: "user", uuid: `cut-${from + n}`, timestamp: at(n), message: { role: "user", content: `entry ${from + n}` } }));
    await withLake(async (db) => {
      await store.append(cut, entries(0));
      await syncLake(db);
      const files = async () => (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'entries', schema => 'claude')`)).map(({ data_file }) => data_file);
      const kept = new Set(await files());
      await store.append(cut, entries(20));
      await syncLake(db);
      const lost = (await files()).filter((file) => !kept.has(file));
      assert.equal(lost.length, 1);
      rmSync(lost[0]!);
      const logs: string[] = [];
      const log: Log = (message, fields) => void logs.push(`${message} ${JSON.stringify(fields ?? {})}`);
      // The lock's connection cut as soon as the reconciliation holds it.
      let cutting = true;
      const cutter = (async () => {
        while (cutting) {
          const { rowCount } = await admin.query("select pg_terminate_backend(pid) from pg_locks where locktype = 'advisory' and granted and objsubid = 1 and objid = oid(hashtext($1)::bigint & 4294967295)", ["alasio.lake.loader"]);
          if (rowCount) return;
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      })();
      try {
        assert.equal(await runPass(db, "reconcile", passing(log)), false);
      } finally {
        cutting = false;
        await cutter;
      }
      assert.match(logs.join("\n"), /the loader's lock was lost/u);
      const count = async () => (await one<Count>(db, `select count(*) as n from ${LAKE}.claude.entries where uuid like 'cut-%'`)).n;
      assert.equal(await count(), 20n);
      assert.equal(await runPass(db, "reconcile", passing(log)), true, logs.join("\n"));
      assert.equal(await count(), 40n);
    });
  });

  test("a reconciliation runs to its end beside a loader awake, which holds the loader's lock only while it loads", async () => {
    await withLake(async (db) => {
      const source = wakeSource([[woken(0n)]]);
      const loader = startLoader({ open: (signal) => openLoaderLake(config, () => {}, signal, 100), join: async () => source, metrics: createMetrics(), log: () => {}, gatherMs: 10 });
      const logs: string[] = [];
      try {
        await eventually(() => source.told.length === 1);
        assert.equal(loader.ready(), true);
        const waited = new Promise<"waited">((resolve) => setTimeout(() => resolve("waited"), 20_000).unref());
        assert.equal(await Promise.race([runPass(db, "reconcile", { ...passing((message) => void logs.push(message)), lockRetryMs: 100 }), waited]), true, logs.join("\n"));
        assert.equal(loader.ready(), true);
      } finally {
        await loader.stop();
      }
    });
  });

  test("the reconciliation asks an object store of a file by a listing of its name, as DuckDB's S3 glob gives back a name with no wildcard unlisted", async () => {
    const s3 = await startFakeS3({ bucket: "lake", region: "us-east-1", accessKeyId: "k", secretAccessKey: "s" });
    const instance = await DuckDBInstance.create(":memory:");
    const db = await instance.connect();
    try {
      await db.run("install httpfs");
      await db.run("load httpfs");
      await db.run(`create secret (type s3, key_id 'k', secret 's', region 'us-east-1', endpoint '${new URL(s3.endpoint).host}', url_style 'path', use_ssl false)`);
      s3.objects.set("claude/entries/kept.parquet", Buffer.from("PAR1"));
      assert.deepEqual([await stored(db, "s3://lake/claude/entries/kept.parquet"), await stored(db, "s3://lake/claude/entries/lost.parquet")], [true, false]);
    } finally {
      db.closeSync();
      instance.closeSync();
      await s3.close();
    }
  });

  test("the reconciliation reads each file the store lists, holding lost one whose data the store no longer gives, after asking again, and failing where the store does not answer", async () => {
    const s3 = await startFakeS3({ bucket: "lake", region: "us-east-1", accessKeyId: "k", secretAccessKey: "s" });
    const instance = await DuckDBInstance.create(":memory:");
    const db = await instance.connect();
    const parquet = join(mkdtempSync(join(tmpdir(), "alasio-lake-readable-")), "one.parquet");
    try {
      await db.run("install httpfs");
      await db.run("load httpfs");
      await db.run(`create secret (type s3, key_id 'k', secret 's', region 'us-east-1', endpoint '${new URL(s3.endpoint).host}', url_style 'path', use_ssl false)`);
      await db.run(`copy (select 1 as i) to '${parquet}' (format parquet)`);
      for (const name of ["kept", "back", "gone"]) s3.objects.set(`claude/entries/${name}.parquet`, readFileSync(parquet));
      s3.objects.set("claude/entries/emptied.parquet", Buffer.alloc(0));
      s3.objects.set("claude/entries/other.parquet", Buffer.from("not a Parquet file at all"));
      // A file the store has none of, every time, or for a moment.
      let missing = 0;
      s3.refuse = (method, key) => {
        if (key === "claude/entries/gone.parquet") return 404;
        if (key === "claude/entries/back.parquet" && method === "HEAD" && missing < 2) {
          missing += 1;
          return 404;
        }
        return null;
      };
      const read = (name: string) => readable(db, `s3://lake/claude/entries/${name}.parquet`, { retryMs: 10 });
      assert.deepEqual(
        [await read("kept"), await read("back"), await read("gone"), await read("emptied"), await read("other")],
        [true, true, false, false, false],
      );
      // A store that does not answer says nothing of the file.
      s3.refuse = (_method, key) => (key === "claude/entries/kept.parquet" ? 503 : null);
      await assert.rejects(read("kept"), /HTTP 503/u);
    } finally {
      db.closeSync();
      instance.closeSync();
      await s3.close();
      rmSync(dirname(parquet), { recursive: true, force: true });
    }
  });

  test("the reconciliation holds lost a file the store fails a read of only where its filer and masters show a chunk of it gone, and fails, naming it, where they do not", async () => {
    const s3 = await startFakeS3({ bucket: "lake", region: "us-east-1", accessKeyId: "k", secretAccessKey: "s" });
    const instance = await DuckDBInstance.create(":memory:");
    const db = await instance.connect();
    // The bundled store's filer, masters and volume server, at one address, each answering
    // as `parts` says: the filer's entry of the file, the masters' lookup of its volume, and
    // the volume server's status for its chunk, and the volume servers the masters' topology holds, each time it is read.
    type Parts = { entry: number | object; lookup: [number, object]; chunk: number; servers?: () => string[] };
    const both = () => ["volume-0:8080", "volume-1:8080"];
    let parts: Parts = { entry: 200, lookup: [200, {}], chunk: 200 };
    const server = createServer((request, response) => {
      const url = new URL(request.url ?? "/", "http://parts");
      const json = (status: number, body: unknown) => response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
      if (url.pathname === "/buckets/lake/claude/entries/failing.parquet" && url.searchParams.has("metadata")) {
        return typeof parts.entry === "number" ? json(parts.entry, {}) : json(200, parts.entry);
      }
      if (url.pathname === "/dir/lookup" && url.searchParams.get("volumeId") === "7") return json(...parts.lookup);
      if (url.pathname === "/dir/status") return json(200, { Topology: { DataCenters: [{ Racks: [{ DataNodes: (parts.servers ?? both)().map((Url) => ({ Url })) }] }] } });
      if (url.pathname === "/7,01637037d6" && request.method === "HEAD") return response.writeHead(parts.chunk).end();
      return json(500, { error: `no ${request.method} ${request.url}` });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const host = `127.0.0.1:${(server.address() as AddressInfo).port}`;
    const store = { filer: `http://${host}`, master: `http://${host}`, volumeServers: 2 };
    const entry = { FullPath: "/buckets/lake/claude/entries/failing.parquet", FileSize: 4, chunks: [{ file_id: "7,01637037d6", size: 4 }] };
    const located: [number, object] = [200, { volumeOrFileId: "7", locations: [{ url: host, publicUrl: host }] }];
    try {
      await db.run("install httpfs");
      await db.run("load httpfs");
      await db.run(`create secret (type s3, key_id 'k', secret 's', region 'us-east-1', endpoint '${new URL(s3.endpoint).host}', url_style 'path', use_ssl false)`);
      const parquet = join(mkdtempSync(join(tmpdir(), "alasio-lake-failing-")), "failing.parquet");
      await db.run(`copy (select 1 as i) to '${parquet}' (format parquet)`);
      s3.objects.set("claude/entries/failing.parquet", readFileSync(parquet));
      rmSync(dirname(parquet), { recursive: true, force: true });
      const path = "s3://lake/claude/entries/failing.parquet";
      // Read once while the store gives it, as the lake reads its files, so DuckDB holds its footer, which the store's HEAD of it, still answered, says is current.
      assert.deepEqual(await rows(db, "select num_rows::INTEGER as n from parquet_file_metadata($1)", [path]), [{ n: 1 }]);
      // The store fails every GET of it, as SeaweedFS does of an entry whose chunks are gone, its HEAD answered from the entry, and as it does for a moment while a volume server restarts.
      s3.refuse = (method, key) => (key === "claude/entries/failing.parquet" && method === "GET" ? 500 : null);
      const read = () => readable(db, path, { store, retryMs: 10, pulseMs: 10 });
      const unknown: [number, object] = [404, { volumeId: "7", error: "volume id 7 not found" }];
      // Its chunk's volume unknown to masters that know every volume server, its chunk gone from its volume, its entry gone, or an entry of bytes with no chunk: lost.
      for (const shown of [
        { entry, lookup: unknown, chunk: 200 },
        { entry, lookup: located, chunk: 404 },
        { entry: 404, lookup: located, chunk: 200 },
        { entry: { ...entry, chunks: [] }, lookup: located, chunk: 200 },
      ] as const) {
        parts = shown as typeof parts;
        assert.equal(await read(), false, JSON.stringify(shown));
      }
      // Its chunk served, as where the store failed the read for a moment, or a part not
      // answering as one that knows: the reconciliation fails, naming the file.
      // Its volume unknown to masters short of a volume server, as one restarts, or whose servers change as they are read, is that too.
      let reads = 0;
      for (const shown of [
        { entry, lookup: located, chunk: 200 },
        { entry, lookup: located, chunk: 503 },
        { entry: 503, lookup: located, chunk: 200 },
        { entry, lookup: [503, {}], chunk: 200 },
        { entry, lookup: unknown, chunk: 200, servers: () => ["volume-0:8080"] },
        { entry, lookup: unknown, chunk: 200, servers: () => (reads++ === 0 ? ["volume-0:8080", "volume-1:8080"] : ["volume-0:8080", "volume-2:8080"]) },
      ] as const) {
        parts = shown as Parts;
        await assert.rejects(read(), (error: unknown) => error instanceof StoreUndecided && error.message.includes(path), JSON.stringify(shown));
      }
      // Another store than the bundled one, whose parts are not asked.
      await assert.rejects(readable(db, path, { retryMs: 10 }), /not the bundled one/u);
    } finally {
      server.close();
      db.closeSync();
      instance.closeSync();
      await s3.close();
    }
  });

  test("a reconciliation on the bundled store forgets nothing while its masters miss a volume server, nor where its filer has no /buckets, and forgets the file once they know every one", async () => {
    const s3 = await startFakeS3({ bucket: "lake", region: "us-east-1", accessKeyId: "k", secretAccessKey: "s" });
    // The filer, masters and volume server, at one address: the bucket, the file's entry and
    // its chunk's volume unknown to the masters, whose topology holds `servers`.
    let buckets = 200;
    let servers = ["volume-0:8080"];
    let entry: object = {};
    const server = createServer((request, response) => {
      const url = new URL(request.url ?? "/", "http://parts");
      const json = (status: number, body: unknown) => response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
      if (url.pathname === "/buckets/lake/") return json(buckets, { Path: "/buckets/lake", Entries: [] });
      if (url.pathname.startsWith("/buckets/lake/") && url.searchParams.has("metadata")) return json(200, entry);
      if (url.pathname === "/dir/lookup") return json(404, { volumeId: url.searchParams.get("volumeId"), error: `volume id ${url.searchParams.get("volumeId")} not found` });
      if (url.pathname === "/dir/status") return json(200, { Topology: { DataCenters: [{ Racks: [{ DataNodes: servers.map((Url) => ({ Url })) }] }] } });
      return json(500, { error: `no ${request.method} ${request.url}` });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const host = `127.0.0.1:${(server.address() as AddressInfo).port}`;
    const store = { filer: `http://${host}`, master: `http://${host}`, volumeServers: 2 };
    const superuser = new pg.Client({ connectionString: postgres?.url });
    await superuser.connect();
    await superuser.query("create database reconcile_s3 owner lake");
    const onStore: LakeConfig = { ...config, dataPath: "s3://lake/", s3: { endpoint: s3.endpoint, key: "k", secret: "s" }, catalog: { ...config.catalog, database: "reconcile_s3" } };
    const lake = await openLake(onStore);
    try {
      await prepareLake(lake.db);
      // A file of the store's, which the catalog names as one of its table's.
      const local = join(mkdtempSync(join(tmpdir(), "alasio-lake-held-")), "held.parquet");
      await lake.db.run(`copy (select range::INTEGER as i from range(100)) to '${local}' (format parquet)`);
      s3.objects.set("held/one.parquet", readFileSync(local));
      rmSync(dirname(local), { recursive: true, force: true });
      await lake.db.run(`create table ${LAKE}.held (i integer)`);
      await lake.db.run(`call ducklake_add_data_files('${LAKE}', 'held', 's3://lake/held/one.parquet')`);
      const file = "s3://lake/held/one.parquet";
      assert.deepEqual((await rows<{ data_file: string }>(lake.db, `select data_file from ducklake_list_files('${LAKE}', 'held')`)).map(({ data_file }) => data_file), [file]);
      entry = { FullPath: `/buckets/lake/${file.slice("s3://lake/".length)}`, FileSize: 4, chunks: [{ file_id: "7,01637037d6", size: 4 }] };
      // The store fails every read of it, as while a volume server restarts.
      s3.refuse = (method, key) => (method === "GET" && `s3://lake/${key}` === file ? 500 : null);
      const named = async () => (await rows(lake.db, `select data_file from ducklake_list_files('${LAKE}', 'held')`)).length;
      const pass = async () => {
        const logs: string[] = [];
        const passed = await runPass(lake.db, "reconcile", { ...passing((message, fields) => void logs.push(`${message} ${JSON.stringify(fields ?? {})}`)), catalog: onStore.catalog, dataPath: onStore.dataPath, store });
        return { passed, said: logs.join("\n") };
      };
      // One of its two volume servers not registered: the pass fails, naming the file, and forgets nothing.
      const missing = await pass();
      assert.deepEqual([missing.passed, missing.said.includes(file), /1 of the store's 2 volume servers/u.test(missing.said), await named()], [false, true, true, 1]);
      // Its filer without the bucket's /buckets, where an entry's absence proves nothing: no more.
      [buckets, servers] = [404, ["volume-0:8080", "volume-1:8080"]];
      const unlisted = await pass();
      assert.deepEqual([unlisted.passed, /has no \/buckets\/lake/u.test(unlisted.said), await named()], [false, true, 1]);
      // Every volume server known, the volume not: the file is gone, and forgotten.
      buckets = 200;
      const known = await pass();
      assert.deepEqual([known.passed, await named()], [true, 0], known.said);
    } finally {
      lake.close();
      server.close();
      await s3.close();
      await superuser.query("drop database reconcile_s3 with (force)").catch(() => {});
      await superuser.end();
    }
  });

  test("the reconciliation forgets a file the store names whose data is gone, and loads again what it held", async () => {
    const emptied = { projectKey: "-home-operator-emptied", sessionId: "66666666-6666-4666-8666-666666666666" };
    const entries = (from: number) => Array.from({ length: 20 }, (_, n) => ({ type: "user", uuid: `emptied-${from + n}`, timestamp: at(n), message: { role: "user", content: `entry ${from + n}` } }));
    await withLake(async (db) => {
      await store.append(emptied, entries(0));
      await syncLake(db);
      const files = async () => (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 'entries', schema => 'claude')`)).map(({ data_file }) => data_file);
      const kept = new Set(await files());
      await store.append(emptied, entries(20));
      await syncLake(db);
      const named = (await files()).filter((file) => !kept.has(file));
      assert.equal(named.length, 1);
      // Named still, as the restored filer names it, its data gone.
      writeFileSync(named[0]!, "");
      const logs: string[] = [];
      assert.equal(await runPass(db, "reconcile", passing((message, fields) => void logs.push(`${message} ${JSON.stringify(fields ?? {})}`))), true, logs.join("\n"));
      assert.match(logs.join("\n"), /"lost":1/u);
      assert.deepEqual(await one<{ n: bigint; uuids: bigint }>(db, `select count(*) as n, count(distinct uuid) as uuids from ${LAKE}.claude.entries where uuid like 'emptied-%'`), { n: 40n, uuids: 40n });
    });
  });

  test("a branch's lake reads every file its catalog names through a week of main's maintenance, and once no branch is left main deletes what the branch wrote", async () => {
    // A branch's catalog is a copy of main's at its branch point, as Neon's branch of the
    // `lake` database is: here, one database made from the other as its template.
    const superuser = new pg.Client({ connectionString: postgres?.url });
    await superuser.connect();
    const gateDir = mkdtempSync(join(tmpdir(), "alasio-lake-gate-"));
    const lakeOf = (database: string): LakeConfig => ({ ...config, dataPath: gateDir, catalog: { ...config.catalog, database } });
    const parquet = () => readdirSync(gateDir, { recursive: true, encoding: "utf8" }).filter((name) => name.endsWith(".parquet")).map((name) => join(gateDir, name));
    const named = async (db: DuckDBConnection) => (await rows<{ data_file: string }>(db, `select data_file from ducklake_list_files('${LAKE}', 't')`)).map(({ data_file }) => data_file);
    const total = async (db: DuckDBConnection) => (await one<Count>(db, `select count(*) as n from ${LAKE}.t`)).n;
    // Each open made ready as the lake service makes it, but keeping nothing past its need,
    // so what a pass may delete it deletes at once.
    const opened = async (database: string) => {
      const lake = await openLake(lakeOf(database));
      await prepareLake(lake.db);
      await lake.db.run(`call ${LAKE}.set_option('expire_older_than', '0 seconds')`);
      await lake.db.run(`call ${LAKE}.set_option('delete_older_than', '0 seconds')`);
      return lake;
    };
    try {
      await superuser.query("create database gate_main owner lake");
      let main = await opened("gate_main");
      await main.db.run(`create table ${LAKE}.t (i integer)`);
      for (let batch = 0; batch < 3; batch++) await main.db.run(`insert into ${LAKE}.t select range from range(${batch * 100}, ${batch * 100 + 100})`);
      const shared = await named(main.db);
      main.close();
      await superuser.query("select pg_terminate_backend(pid) from pg_stat_activity where datname = 'gate_main'");
      await superuser.query("create database gate_branch template gate_main owner lake");

      const branch = await opened("gate_branch");
      await branch.db.run(`insert into ${LAKE}.t select range from range(1000, 1100)`);
      const branchFiles = await named(branch.db);
      const ownFiles = branchFiles.filter((file) => !shared.includes(file));
      assert.equal(ownFiles.length, 1);
      assert.equal(await total(branch.db), 400n);
      branch.close();

      // A week of main's maintenance, a pass a day: main changes what the branch shares,
      // merging it, deleting from it, expiring every snapshot before the pass, with the
      // branch alive.
      main = await opened("gate_main");
      for (let day = 0; day < 7; day++) {
        await main.db.run(`insert into ${LAKE}.t select range from range(${2000 + day * 100}, ${2100 + day * 100})`);
        await main.db.run(`delete from ${LAKE}.t where i % 7 = ${day}`);
        await new Promise((resolve) => setTimeout(resolve, 1100));
        await maintainLake(main.db, { retentionDays: 30, keepFiles: true });
      }
      const mainFiles = await named(main.db);
      assert.ok(shared.some((file) => !mainFiles.includes(file)), "main's maintenance no longer names some of what it shares with the branch");
      const remaining = await total(main.db);
      main.close();

      const again = await opened("gate_branch");
      const missing = (await named(again.db)).filter((file) => !parquet().includes(file));
      assert.deepEqual(missing, []);
      assert.equal(await total(again.db), 400n);
      again.close();

      // The branch deleted, main's next pass deletes what it kept, and what the branch wrote.
      await superuser.query("drop database gate_branch with (force)");
      main = await opened("gate_main");
      await new Promise((resolve) => setTimeout(resolve, 1100));
      await maintainLake(main.db, { retentionDays: 30, keepFiles: false });
      assert.deepEqual(parquet().filter((file) => ownFiles.includes(file) || (shared.includes(file) && !mainFiles.includes(file))), []);
      assert.equal(await total(main.db), remaining);
      main.close();
    } finally {
      await superuser.query("select pg_terminate_backend(pid) from pg_stat_activity where datname like 'gate_%'");
      await superuser.query("drop database if exists gate_branch with (force)");
      await superuser.query("drop database if exists gate_main with (force)");
      await superuser.end();
      rmSync(gateDir, { recursive: true, force: true });
    }
  });

  test("a load that fails drops its connections and its member of the group, and the next is made on fresh ones, its records read again", async () => {
    await withLake(async (db) => {
      const events: string[] = [];
      let attempts = 0;
      const sources = [wakeSource([[woken(0n)]]), wakeSource([[woken(0n)]])];
      const loader = startLoader({
        open: async () => {
          events.push("open");
          return { db, close: async () => events.push("close"), replayed: async () => true, locked: unlocked };
        },
        join: async () => {
          events.push("join");
          return sources[events.filter((event) => event === "join").length - 1]!;
        },
        metrics: createMetrics(),
        log: (message) => events.push(message),
        gatherMs: 10,
        retryMs: 10,
        sync: async (lake) => {
          attempts += 1;
          if (attempts === 1) throw new Error("terminating connection due to administrator command");
          return syncLake(lake);
        },
      });
      await eventually(() => events.includes("loaded"));
      await loader.stop();
      assert.deepEqual(events, ["open", "join", "load failed", "close", "open", "join", "loaded", "close"]);
      assert.deepEqual(sources.map(({ told }) => told), [[], [[woken(1n)]]]);
      assert.deepEqual(loader.health(), { ok: true, detail: "loaded" });
    });
  });

  test("a connection that fails is logged with where it was going, never with the password, which no query can read either", async () => {
    // Nothing listens on port 1.
    const unreachable = { ...config, catalog: { ...config.catalog, port: 1 } };
    const logs: string[] = [];
    const loader = startLoader({
      open: async () => {
        const lake = await openLake(unreachable, { source: unreachable.source });
        return { db: lake.db, close: async () => lake.close(), replayed: async () => true, locked: unlocked };
      },
      metrics: createMetrics(),
      join: async () => wakeSource([]),
      log: (message, fields) => logs.push(JSON.stringify({ message, ...fields })),
    });
    for (let waited = 0; logs.length === 0 && waited < 30_000; waited += 50) await new Promise((resolve) => setTimeout(resolve, 50));
    await loader.stop();
    assert.match(logs.join("\n"), /"load failed".*dbname='alasio'.*port 1 failed/u);
    assert.ok(!logs.join("\n").includes(LAKE_PASSWORD));
    const lake = await openLake(config, { source: config.source });
    try {
      const said = JSON.stringify(await rows(lake.db, "select (select list(path) from duckdb_databases()) as paths, (select list(secret_string) from duckdb_secrets()) as secrets"));
      assert.match(said, /dbname='lake'/u);
      assert.ok(!said.includes(LAKE_PASSWORD));
    } finally {
      lake.close();
    }
  });

  test("the lake reports the memory its process holds, and each of its DuckDBs, 0 for one not open", async () => {
    const lake = await openLake(config);
    try {
      await ensureOtel(lake.db);
      // Staged in the DuckDB's memory first.
      await writeTelemetry(lake.db, telemetryRows("logs", logRequest("metered", NOW)));
      const metrics = createMetrics();
      await sampleMemory(metrics, { loader: lake, intake: null });
      const text = metrics.render();
      assert.match(text, /^lake_resident_memory_bytes [1-9]\d*$/mu);
      assert.match(text, /^lake_heap_used_bytes [1-9]\d*$/mu);
      assert.match(text, /^lake_duckdb_memory_bytes\{duckdb="loader"\} [1-9]\d*$/mu);
      assert.match(text, /^lake_duckdb_memory_bytes\{duckdb="intake"\} 0$/mu);
    } finally {
      lake.close();
    }
  });

  test("the lake opened read-only refuses writes", async () => {
    const lake = await openLake(readerConfig, { readOnly: true });
    try {
      await assert.rejects(lake.db.run(`delete from ${LAKE}.claude.entries`), /read-only|read only/i);
      assert.ok((await rows(lake.db, `select count(*) from ${LAKE}.claude.entries`)).length);
    } finally {
      lake.close();
    }
  });

  test("the lake's role is a member of no other role, and making it again changes nothing", async () => {
    await ensureLakeRole(admin, LAKE_PASSWORD);
    const { rows } = await admin.query<{ n: number }>("select count(*)::int as n from pg_auth_members where member = $1::regrole", [LAKE_ROLE]);
    assert.equal(rows[0]?.n, 0);
    const lake = await openLake(config, { source: config.source });
    lake.close(); // its password still lets it in
    const { rows: [database] } = await admin.query<{ public_connects: boolean }>("select has_database_privilege('public', 'lake', 'connect') as public_connects");
    assert.equal(database?.public_connects, false);
  });

  test("with the lake off, its role reads none of alasio's data", async () => {
    await syncLakeReads(admin, false);
    try {
      const lake = await openLake(config, { source: config.source });
      try {
        await assert.rejects(rows(lake.db, "select count(*) from source.claude_sessions.entries"), /permission denied/);
      } finally {
        lake.close();
      }
    } finally {
      await syncLakeReads(admin, true);
    }
  });
});

// --- Reading the lake ----------------------------------------------------------------

describe("the lake's reader", () => {
  test("reads the lake as its own role, and can write nothing, reach no other file, and change no setting", async () => {
    await withLake(async (db) => {
      await writeTelemetry(db, telemetryRows("logs", logRequest("read", NOW)));
    });
    const reader = await openReader(readerConfig);
    try {
      assert.deepEqual(await answer(reader.db, "select Body from otel.logs where ServiceName = 'read'"), [{ Body: "an event" }]);
      const [catalogRole] = await answer(reader.db, "from postgres_query('__ducklake_metadata_lake', 'select current_user::text as role')");
      assert.deepEqual(catalogRole, { role: LAKE_READER_ROLE });
      await assert.rejects(answer(reader.db, "insert into otel.logs (Body) values ('written')"), /read-only/);
      // Its role may only read the catalog: no table of it can it change.
      const catalog = new pg.Client({ ...config.catalog });
      await catalog.connect();
      try {
        const { rows: [tables] } = await catalog.query<{ total: number; readable: number; writable: number }>(
          `select count(*)::int as total, count(*) filter (where has_table_privilege($1, oid, 'select'))::int as readable,
             count(*) filter (where has_table_privilege($1, oid, 'insert, update, delete, truncate'))::int as writable
           from pg_class where relnamespace = 'ducklake'::regnamespace and relkind = 'r'`,
          [LAKE_READER_ROLE],
        );
        // Those DuckLake made after the grant too, as it makes one for each table's inlined rows.
        assert.ok(tables && tables.total > 0 && tables.readable === tables.total && tables.writable === 0, JSON.stringify(tables));
      } finally {
        await catalog.end();
      }
      await assert.rejects(answer(reader.db, "select * from read_text('/etc/hostname')"), /disabled by configuration/);
      await assert.rejects(answer(reader.db, "copy (select 1) to '/tmp/alasio-lake-read.csv'"), /disabled by configuration/);
      await assert.rejects(answer(reader.db, "set enable_external_access = true"), /locked/);
    } finally {
      reader.close();
    }
  });

  test("answers one statement's rows as JSON: nested values as JSON text, times in RFC 3339, numbers as numbers where they fit", async () => {
    const reader = await openReader(readerConfig);
    try {
      const [row] = await answer(reader.db, `select map {'k': 'v'} as map, [1, 2] as list, {'a': [{'b': map {'c': 'd'}}]} as struct,
        42::bigint as count, 18446744073709551615::ubigint as large, 2.5::decimal(4, 2) as decimal, null as nothing,
        '2026-10-07 04:09:17.123456789'::timestamp_ns as time, '2026-10-07 06:09:17+02'::timestamptz as zoned`);
      assert.deepEqual(row, {
        map: '{"k":"v"}',
        list: "[1,2]",
        struct: '{"a":[{"b":{"c":"d"}}]}',
        count: 42,
        large: "18446744073709551615",
        decimal: 2.5,
        nothing: null,
        time: "2026-10-07T04:09:17.123456789Z",
        zoned: "2026-10-07T04:09:17+00:00",
      });
      await assert.rejects(answer(reader.db, "select 1; select 2"), /a query is one statement, not 2/);
      assert.equal((await answer(reader.db, "from range(10)", { maxRows: 10 })).length, 10);
      await assert.rejects(answer(reader.db, "from range(11)", { maxRows: 10 }), /more than 10 rows/);
      await assert.rejects(answer(reader.db, "select count(*) from range(1000000000000)", { timeoutMs: 200 }), /ran past 0.2 s/);
      assert.deepEqual(await answer(reader.db, "select 1 as one"), [{ one: 1 }]);
    } finally {
      reader.close();
    }
  });

  test("the query endpoint answers queries that carry its token, and rides out the compute dropping its connections", async () => {
    let opened = 0;
    const endpoint = startEndpoint({
      open: () => {
        opened += 1;
        return openReader(readerConfig);
      },
      token: QUERY_TOKEN,
      log: () => {},
    });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const query = (sql: string, token = QUERY_TOKEN) => fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${token}` }, body: sql });
    try {
      assert.equal((await fetch(`${base}/healthz`)).status, 200);
      assert.equal((await query("select 42 as answer", "wrong")).status, 401);
      assert.equal((await fetch(`${base}/query`, { method: "POST", body: "select 42 as answer" })).status, 401);
      const answered = await query("select 42 as answer");
      assert.deepEqual([answered.status, answered.headers.get("content-type"), await answered.json()], [200, "application/json", [{ answer: 42 }]]);
      // Why, in the body, and in the status line, on one line, which is what Grafana's Infinity shows.
      const refused = await query("select nothing");
      const why = await refused.text();
      assert.deepEqual([refused.status, why.startsWith("Binder Error"), why.includes("\n")], [400, true, true]);
      assert.equal(refused.statusText, why.trim().replace(/\n+/gu, " "));
      await admin.query("select pg_terminate_backend(pid) from pg_stat_activity where usename = $1", [LAKE_READER_ROLE]);
      const after = await query("select count(*) as n from otel.logs where ServiceName = 'read'");
      assert.deepEqual([after.status, await after.json()], [200, [{ n: 1 }]]);
      assert.equal(opened, 1);
    } finally {
      server.close();
      await endpoint.close();
    }
  });

  test("the query endpoint answers a query that meets a writer's lock on the lake's catalog within the reader's bound, and the next once it is let go", async () => {
    await withLake((db) => ensureOtel(db));
    const endpoint = startEndpoint({ open: () => openReader(readerConfig), token: QUERY_TOKEN, log: () => {} });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const query = (sql: string) => fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${QUERY_TOKEN}` }, body: sql, signal: AbortSignal.timeout(25_000) });
    // A writer of the catalog's, as DuckLake's dropping a table of its own takes, holding it on.
    const writer = new pg.Client({ ...config.catalog });
    await writer.connect();
    try {
      assert.equal((await query("select 42 as answer")).status, 200);
      await writer.query("begin");
      await writer.query("lock table ducklake.ducklake_snapshot in access exclusive mode");
      const started = Date.now();
      const held = await query("select count(*) as n from otel.logs");
      assert.ok(held.status >= 400, `answered ${held.status}`);
      assert.match(await held.text(), /lock timeout/u);
      assert.ok(Date.now() - started < 20_000, `answered after ${Date.now() - started} ms`);
      await writer.query("rollback");
      const after = await query("select 42 as answer");
      assert.deepEqual([after.status, await after.json()], [200, [{ answer: 42 }]]);
    } finally {
      await writer.query("rollback").catch(() => {});
      await writer.end();
      server.close();
      await endpoint.close();
    }
  });

  test("the query endpoint answers 503 within its bound where a query before holds the lake for good, counts both until the lake is free of them, and is not well once it has held it long", async () => {
    // A DuckDB blocked on a statement it sent Postgres, which nothing interrupts.
    const db = { extractStatements: async () => ({ count: 1 }), streamAndReadUntil: () => new Promise(() => {}), interrupt() {} };
    const logged: string[] = [];
    const endpoint = startEndpoint({ open: async () => ({ db, memory: async () => 0, close() {} }) as unknown as Lake, token: QUERY_TOKEN, log: (message) => logged.push(message), answerWithinMs: 300, wedgedMs: 1000 });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const query = (sql: string) => fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${QUERY_TOKEN}` }, body: sql, signal: AbortSignal.timeout(5_000) });
    try {
      const [first, second] = await Promise.all([query("select 'held'"), query("select 'behind'")]);
      assert.deepEqual([first.status, second.status], [503, 503]);
      assert.match(await second.text(), /a query \(select 'held'\) has held the lake for \d+ s/u);
      assert.deepEqual(await (await fetch(`${base}/queue`)).json(), { queries: 2 });
      assert.equal((await fetch(`${base}/healthz`)).status, 200);
      await new Promise((resolve) => setTimeout(resolve, 1100));
      const healthz = await fetch(`${base}/healthz`);
      assert.deepEqual([healthz.status, (await healthz.text()).includes("select 'held'")], [503, true]);
      assert.deepEqual(logged.filter((message) => message === "query endpoint wedged"), ["query endpoint wedged"]);
    } finally {
      server.close();
    }
  });

  test("the query endpoint is not well once an opening of the lake that never settles has held it long", async () => {
    const endpoint = startEndpoint({ open: () => new Promise<Lake>(() => {}), token: QUERY_TOKEN, log: () => {}, wedgedMs: 300 });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      assert.equal((await fetch(`${base}/healthz`)).status, 200);
      await new Promise((resolve) => setTimeout(resolve, 400));
      const healthz = await fetch(`${base}/healthz`);
      assert.deepEqual([healthz.status, (await healthz.text()).includes("opening it has held the lake")], [503, true]);
    } finally {
      server.close();
    }
  });

  test("the query endpoint opens the lake as it starts, again until it can, is ready once it has, and says how many queries it has not answered yet", async () => {
    let opens = 0;
    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const metrics = createMetrics();
    const endpoint = startEndpoint({
      open: async () => {
        opens += 1;
        if (opens === 1) throw new Error("the compute is waking");
        await held;
        return openReader(readerConfig);
      },
      token: QUERY_TOKEN,
      log: () => {},
      metrics,
      retryMs: 10,
    });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    const queue = async () => (await (await fetch(`${base}/queue`)).json()) as { queries: number };
    try {
      await eventually(() => opens === 2);
      assert.equal((await fetch(`${base}/readyz`)).status, 503);
      // Two queries taken while it opens the lake, the second behind the first.
      const answers = [1, 2].map((n) => fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${QUERY_TOKEN}` }, body: `select ${n} as n` }));
      for (let waited = 0; (await queue()).queries < 2 && waited < 5000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
      assert.deepEqual(await queue(), { queries: 2 });
      assert.match(await (await fetch(`${base}/metrics`)).text(), /^lake_query_queue 2$/mu);
      release();
      assert.deepEqual(await Promise.all(answers.map(async (answered) => (await answered).json())), [[{ n: 1 }], [{ n: 2 }]]);
      assert.deepEqual([(await fetch(`${base}/readyz`)).status, await queue(), opens], [200, { queries: 0 }, 2]);
      assert.match(metrics.render(), /^lake_queries_total\{status="200"\} 2$/mu);
    } finally {
      server.close();
      await endpoint.close();
    }
  });

  test("a reader says the queries every reader of its pool has not answered yet, of those that say within their patience", async () => {
    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const listening = async (endpoint: ReturnType<typeof startEndpoint>) => {
      const server = createServer(endpoint.handle);
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      return { server, base: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
    };
    const bases: string[] = [];
    // One busy, its lake held from opening, and one idle; and one that does not answer, as one gone.
    const busy = startEndpoint({
      open: async () => {
        await held;
        return openReader(readerConfig);
      },
      token: QUERY_TOKEN,
      log: () => {},
      peers: async () => bases,
    });
    const idle = startEndpoint({ open: () => openReader(readerConfig), token: QUERY_TOKEN, log: () => {}, peers: async () => bases });
    const [a, b] = [await listening(busy), await listening(idle)];
    bases.push(a.base, b.base, "http://127.0.0.1:1");
    try {
      const answers = [1, 2, 3].map((n) => fetch(`${a.base}/query`, { method: "POST", headers: { authorization: `Bearer ${QUERY_TOKEN}` }, body: `select ${n} as n` }));
      const pool = async (base: string) => (await (await fetch(`${base}/pool`)).json()) as { queries: number; readers: number };
      for (let waited = 0; (await pool(a.base)).queries < 3 && waited < 5000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
      // Either reader says the pool's, which a scaler reading one of them through their Service reads.
      assert.deepEqual([await pool(a.base), await pool(b.base)], [{ queries: 3, readers: 2 }, { queries: 3, readers: 2 }]);
      release();
      await Promise.all(answers);
      assert.deepEqual(await pool(b.base), { queries: 0, readers: 2 });
    } finally {
      a.server.close();
      b.server.close();
      await Promise.all([busy.close(), idle.close()]);
    }
  });

  test("runs every query of Grafana's dashboards and alert rules", async () => {
    await withLake((db) => ensureOtel(db));
    const grafana = new URL("../neon/grafana/", import.meta.url);
    // Each panel's and variable's query, as Infinity posts it.
    const sql = readdirSync(new URL("dashboards/", grafana)).flatMap((file) => {
      const dashboard: { panels: { title: string; targets?: { url_options: { data: string } }[] }[]; templating: { list: { name: string; query: { infinityQuery: { url_options: { data: string } } } }[] } } =
        JSON.parse(readFileSync(new URL(`dashboards/${file}`, grafana), "utf8"));
      return [
        ...dashboard.panels.flatMap(({ title, targets = [] }) => targets.map(({ url_options }) => [`${file}: ${title}`, url_options.data])),
        ...dashboard.templating.list.map(({ name, query }) => [`${file}: $${name}`, query.infinityQuery.url_options.data]),
      ];
    });
    // Each rule's, the block after its query's `data: |`.
    const rules = readFileSync(new URL("provisioning/alerting/rules.yaml", grafana), "utf8");
    for (const [, indent, block] of rules.matchAll(/^( *)data: \|\n((?:\1 {2}.*\n)+)/gmu)) {
      sql.push(["rules.yaml", (block ?? "").replaceAll(`\n${indent}  `, "\n").trim()]);
    }
    assert.ok(sql.length > 20, `${sql.length} queries`);
    // Grafana's variables, as it fills them in.
    const filled = (query: string) =>
      [["${__from}", String(Date.now() - 3_600_000)], ["${__to}", String(Date.now())], ["${__interval_ms}", "60000"], ["${trace}", "0af7651916cd43dd8448eb211c80319c"], ["${conversation:sqlstring}", "'telegram:1'"], ["${branch:sqlstring}", "'main'"]]
        .reduce((text, [variable = "", value = ""]) => text.replaceAll(variable, value), query);
    const reader = await openReader(readerConfig);
    try {
      const failed: string[] = [];
      for (const [where, query = ""] of sql) await answer(reader.db, filled(query)).catch((error: Error) => failed.push(`${where}: ${error.message}`));
      assert.deepEqual(failed, []);
    } finally {
      reader.close();
    }
  });

  test("the Agents dashboard shows main's turns or a branch environment's, as its Branch chooses, and a branch's no transcript", async () => {
    const turn = (traceId: string, resource: Record<string, string>) => ({
      resource: { attributes: Object.entries({ "service.name": "alasio", ...resource }).map(([key, value]) => ({ key, value: { stringValue: value } })) },
      scopeSpans: [{ spans: [{ traceId, spanId: "00f067aa0ba902b7", name: "alasio.turn", startTimeUnixNano: String(NOW), endTimeUnixNano: String(NOW + 1_000_000_000n) }] }],
    });
    const MAIN_TURN = "1af7651916cd43dd8448eb211c80319c";
    const BRANCH_TURN = "2af7651916cd43dd8448eb211c80319c";
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeTelemetry(db, telemetryRows("traces", { resourceSpans: [turn(MAIN_TURN, {}), turn(BRANCH_TURN, { "alasio.branch": "try" })] }));
    });
    const dashboard: { panels: { title: string; type: string; targets?: { url_options: { data: string } }[] }[]; templating: { list: { name: string; query: { infinityQuery: { url_options: { data: string } } } }[] } } =
      JSON.parse(readFileSync(new URL("../neon/grafana/dashboards/agents.json", import.meta.url), "utf8"));
    // A table's query, or a variable's.
    const query = (name: string) => {
      const found = dashboard.panels.find(({ title, type }) => title === name && type === "table")?.targets?.[0]?.url_options.data ??
        dashboard.templating.list.find((variable) => variable.name === name)?.query.infinityQuery.url_options.data;
      assert.ok(found, name);
      return found;
    };
    const on = (branch: string, sql: string) =>
      [["${__from}", String(Number(NOW / 1_000_000n) - 3_600_000)], ["${__to}", String(Number(NOW / 1_000_000n) + 3_600_000)], ["${__interval_ms}", "60000"], ["${branch:sqlstring}", `'${branch}'`]]
        .reduce((text, [variable = "", value = ""]) => text.replaceAll(variable, value), sql);
    const reader = await openReader(readerConfig);
    try {
      const branches = await answer(reader.db, on("main", query("branch")));
      assert.deepEqual(branches, [{ __text: "main", __value: "main" }, { __text: "try", __value: "try" }]);
      const traces = async (branch: string) => (await answer(reader.db, on(branch, query("Turns")))).map((row) => row["trace"]).filter((trace) => trace === MAIN_TURN || trace === BRANCH_TURN);
      assert.deepEqual(await traces("main"), [MAIN_TURN]);
      assert.deepEqual(await traces("try"), [BRANCH_TURN]);
      // Its transcripts are in its own lake, which Grafana does not read.
      assert.deepEqual(await answer(reader.db, on("try", query("Tool calls"))), []);
    } finally {
      reader.close();
    }
  });

  test("the alert rules count main's failed turns and telemetry alone, not a branch environment's", async () => {
    const resource = (branch: string | null) => ({ attributes: [{ key: "service.name", value: { stringValue: "alasio" } }, ...(branch ? [{ key: "alasio.branch", value: { stringValue: branch } }] : [])] });
    const failed = (conversation: string, branch: string | null) => ({
      resource: resource(branch),
      scopeSpans: [{
        spans: [{
          traceId: createHash("md5").update(conversation).digest("hex"),
          spanId: "00f067aa0ba902b7",
          name: "alasio.turn",
          startTimeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
          endTimeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
          attributes: [{ key: "alasio.conversation.id", value: { stringValue: conversation } }, { key: "alasio.turn.outcome", value: { stringValue: "failed" } }],
        }],
      }],
    });
    const gauge = (branch: string | null) => ({
      resource: resource(branch),
      scopeMetrics: [{ metrics: [{ name: "alasio.alive", gauge: { dataPoints: [{ timeUnixNano: String(BigInt(Date.now()) * 1_000_000n), asDouble: 1 }] } }] }],
    });
    // Each rule's query, by its title.
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    const rule = (title: string) => {
      const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf(`title: ${title}`))) ?? [];
      return block.replaceAll(`\n${indent}  `, "\n").trim();
    };
    await withLake((db) => ensureOtel(db));
    const reader = await openReader(readerConfig);
    const points = async () => (await answer(reader.db, rule("Telemetry is absent")))[0]?.["points"];
    try {
      const before = await points();
      await withLake(async (db) => {
        await writeTelemetry(db, telemetryRows("traces", { resourceSpans: [failed("telegram:main-failed", null), failed("telegram:branch-failed", "try")] }));
        await writeTelemetry(db, telemetryRows("metrics", { resourceMetrics: [gauge("try")] }));
      });
      const failing = (await answer(reader.db, rule("Turns are failing"))).map((row) => row["conversation"]);
      assert.ok(failing.includes("telegram:main-failed"), JSON.stringify(failing));
      assert.ok(!failing.includes("telegram:branch-failed"), JSON.stringify(failing));
      // A branch's telemetry arriving does not hide main's being absent.
      assert.equal(await points(), before);
    } finally {
      reader.close();
    }
  });

  test("the Sandboxes row counts what alasio's running totals added in each interval, and the fallback and release alerts main's failures alone", async () => {
    const minute = 60_000;
    const now = Math.floor(Date.now() / minute) * minute;
    const keyValues = (attributes: Readonly<Record<string, string>>) => Object.entries(attributes).map(([key, value]) => ({ key, value: { stringValue: value } }));
    const resource = (instance: string, branch?: string) => ({ attributes: keyValues({ "service.name": "alasio", "service.instance.id": instance, ...(branch ? { "alasio.branch": branch } : {}) }) });
    const nanos = (ms: number) => String(BigInt(ms) * 1_000_000n);
    /**
     * A point a minute of a series, as alasio's SDK exports one, from the minute after its
     * first value, each `{ count, sum }` of the values `at` (minutes before now, each half a
     * minute before the next point) by then.
     */
    const points = <Point extends object>(startedAgo: number, at: readonly (readonly [number, number])[], point: (seen: readonly number[]) => Point) =>
      Array.from({ length: Math.max(...at.map(([ago]) => ago)) + 1 }, (_, index) => Math.max(...at.map(([ago]) => ago)) - index).map((ago) => ({
        startTimeUnixNano: nanos(now - startedAgo * minute),
        timeUnixNano: nanos(now - ago * minute),
        ...point(at.filter(([happened]) => happened >= ago).map(([, value]) => value)),
      }));
    const counter = (name: string, startedAgo: number, attributes: Readonly<Record<string, string>>, at: readonly number[]) => ({
      name,
      sum: {
        aggregationTemporality: 2,
        isMonotonic: true,
        dataPoints: points(startedAgo, at.map((ago) => [ago, 1]), (seen) => ({
          asDouble: seen.length,
          attributes: keyValues(attributes),
        })),
      },
    });
    const histogram = (name: string, startedAgo: number, at: readonly (readonly [number, number])[]) => ({
      name,
      histogram: {
        aggregationTemporality: 2,
        dataPoints: points(startedAgo, at, (seen) => ({ count: String(seen.length), sum: seen.reduce((total, value) => total + value, 0), bucketCounts: [String(seen.length)], explicitBounds: [] })),
      },
    });
    const outcome = (restore: string, fallback?: string) => ({ "alasio.sandbox.restore.outcome": restore, ...(fallback ? { "alasio.sandbox.restore.fallback": fallback } : {}) });
    const restores = "alasio.sandbox.restores";
    // One alasio up for three hours, which counted before the last hour too; one started
    // fifty minutes ago, counting from zero; a branch's, which pages no one.
    const metrics: MetricsRequest = {
      resourceMetrics: [
        {
          resource: resource("long"),
          scopeMetrics: [{
            metrics: [
              counter(restores, 180, outcome("restored"), [150, 70, 40, 20, 19]),
              counter(restores, 180, outcome("fallback", "prepare"), [170, 10]),
              counter(restores, 180, outcome("fallback", "template"), [30, 29, 28]),
              counter("alasio.sandbox.releases", 180, { "alasio.sandbox.release.outcome": "snapshotted" }, [90, 50, 10]),
              counter("alasio.sandbox.releases", 180, { "alasio.sandbox.release.outcome": "failed" }, [30]),
              counter("alasio.sandbox.releases", 180, { "alasio.sandbox.release.outcome": "lost" }, [20]),
              counter("alasio.sandbox.releases", 180, { "alasio.sandbox.release.outcome": "error" }, [100, 15]),
              counter("alasio.sandbox.releases", 180, { "alasio.sandbox.release.outcome": "kept" }, [5, 4]),
              histogram("alasio.sandbox.restore.duration", 180, [[100, 99], [20, 3], [5, 5]]),
            ],
          }],
        },
        { resource: resource("restarted"), scopeMetrics: [{ metrics: [counter(restores, 50, outcome("restored"), [45, 5]), counter(restores, 50, outcome("fallback", "restore"), [15])] }] },
        { resource: resource("branch", "try"), scopeMetrics: [{ metrics: [counter(restores, 120, outcome("fallback", "node"), [25, 24, 23, 22])] }] },
      ],
    };
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeTelemetry(db, telemetryRows("metrics", metrics));
    });
    const dashboard: { panels: { title: string; targets?: { url_options: { data: string } }[] }[] } =
      JSON.parse(readFileSync(new URL("../neon/grafana/dashboards/stack-health.json", import.meta.url), "utf8"));
    // A panel's query over the last hour, by the minute.
    const panel = (title: string) => {
      const sql = dashboard.panels.find((found) => found.title === title)?.targets?.[0]?.url_options.data;
      assert.ok(sql, title);
      return [["${__from}", String(now - 60 * minute)], ["${__to}", String(now + minute)], ["${__interval_ms}", String(minute)]]
        .reduce((text, [variable = "", value = ""]) => text.replaceAll(variable, value), sql);
    };
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    /** The query of the alert titled `title`. */
    const alertQuery = (title: string) => {
      const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf(`title: ${title}`))) ?? [];
      return block.replaceAll(`\n${indent}  `, "\n").trim();
    };
    const reader = await openReader(readerConfig);
    try {
      /** Each label's total over the panel's rows. */
      const totals = async (title: string, label: string, value: string) => {
        const summed: Record<string, number> = {};
        for (const row of await answer(reader.db, panel(title))) summed[String(row[label])] = (summed[String(row[label])] ?? 0) + Number(row[value]);
        return summed;
      };
      assert.deepEqual(await totals("Restores", "outcome", "restores"), { "restored": 5, "fell back: prepare": 1, "fell back: template": 3, "fell back: restore": 1, "fell back: node": 4 });
      assert.deepEqual(await totals("Releases", "outcome", "releases"), { snapshotted: 2, failed: 1, lost: 1, error: 1, kept: 2 });
      assert.deepEqual((await answer(reader.db, panel("Restore time"))).map((row) => row["seconds"]), [3, 5]);
      // Main's restores that failed, of those that could have restored.
      assert.deepEqual(await answer(reader.db, alertQuery("Restores are falling back")), [{ failed: 2 / 7 }]);
      // Main's releases that failed, lost their sandbox, or failed beside their save, of those that snapshot.
      assert.deepEqual(await answer(reader.db, alertQuery("Releases are failing")), [{ failed: 3 / 5 }]);
    } finally {
      reader.close();
    }
  });

  test("the Sandbox gateway row times its binds by how their pod came and its attaches and detaches, counts its releases by why and the pods the policy refused, and the alerts page on refusals and slow warm binds", async () => {
    const minute = 60_000;
    const now = Math.floor(Date.now() / minute) * minute;
    const keyValues = (attributes: Readonly<Record<string, string>>) => Object.entries(attributes).map(([key, value]) => ({ key, value: { stringValue: value } }));
    const nanos = (ms: number) => String(BigInt(ms) * 1_000_000n);
    /** A point a minute of a series begun `startedAgo` minutes ago, of the values `at` (minutes before now) by each. */
    const points = <Point extends object>(startedAgo: number, at: readonly (readonly [number, number])[], point: (seen: readonly number[]) => Point) =>
      Array.from({ length: Math.max(...at.map(([ago]) => ago)) + 1 }, (_, index) => Math.max(...at.map(([ago]) => ago)) - index).map((ago) => ({
        startTimeUnixNano: nanos(now - startedAgo * minute),
        timeUnixNano: nanos(now - ago * minute),
        ...point(at.filter(([happened]) => happened >= ago).map(([, value]) => value)),
      }));
    const counter = (name: string, attributes: Readonly<Record<string, string>>, at: readonly number[]) => ({
      name,
      sum: { aggregationTemporality: 2, isMonotonic: true, dataPoints: points(180, at.map((ago) => [ago, 1]), (seen) => ({ asDouble: seen.length, attributes: keyValues(attributes) })) },
    });
    const histogram = (name: string, attributes: Readonly<Record<string, string>>, at: readonly (readonly [number, number])[]) => ({
      name,
      histogram: {
        aggregationTemporality: 2,
        dataPoints: points(180, at, (seen) => ({ count: String(seen.length), sum: seen.reduce((total, value) => total + value, 0), bucketCounts: [String(seen.length)], explicitBounds: [], attributes: keyValues(attributes) })),
      },
    });
    const main = { "k8s.namespace.name": "alasio-sessions" };
    const bind = (start: string) => ({ ...main, "alasio.binder.pool": "sandboxes-none", "alasio.binder.start": start });
    const reason = (why: string) => ({ ...main, "alasio.gateway.release.reason": why });
    // The gateway up for three hours, which timed and counted before the last hour too.
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeTelemetry(db, telemetryRows("metrics", {
        resourceMetrics: [{
          resource: { attributes: keyValues({ "service.name": "alasio-sandbox-gateway", "service.instance.id": "gateway-0" }) },
          scopeMetrics: [{
            metrics: [
              histogram("alasio.binder.bind.duration", bind("warm"), [[150, 99], [20, 12], [10, 14]]),
              histogram("alasio.binder.bind.duration", bind("cold"), [[20, 90]]),
              histogram("alasio.binder.bind.duration", bind("made"), [[5, 40]]),
              histogram("alasio.gateway.attach.duration", main, [[20, 2], [10, 4]]),
              histogram("alasio.gateway.detach.duration", main, [[5, 1]]),
              counter("alasio.gateway.releases", reason("idle"), [150, 40, 30]),
              counter("alasio.gateway.releases", reason("asked"), [20]),
              counter("alasio.gateway.releases", reason("lost"), [10]),
              counter("alasio.gateway.pod_policy.refusals", { "k8s.namespace.name": "alasio-branch-try-sessions" }, [100, 5, 4]),
            ],
          }],
        }],
      }));
    });
    const dashboard: { panels: { title: string; targets?: { url_options: { data: string } }[] }[] } =
      JSON.parse(readFileSync(new URL("../neon/grafana/dashboards/stack-health.json", import.meta.url), "utf8"));
    const panel = (title: string) => {
      const sql = dashboard.panels.find((found) => found.title === title)?.targets?.[0]?.url_options.data;
      assert.ok(sql, title);
      return [["${__from}", String(now - 60 * minute)], ["${__to}", String(now + minute)], ["${__interval_ms}", String(minute)]]
        .reduce((text, [variable = "", value = ""]) => text.replaceAll(variable, value), sql);
    };
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    const alertQuery = (title: string) => {
      const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf(`title: ${title}`))) ?? [];
      return block.replaceAll(`\n${indent}  `, "\n").trim();
    };
    const reader = await openReader(readerConfig);
    try {
      const rows = async (title: string, label: string, value: string) =>
        (await answer(reader.db, panel(title))).map((row) => `${String(row[label])} ${Number(row[value])}`).sort();
      // Each bind of the last hour, by how its pod came; the one before it, not.
      assert.deepEqual(await rows("Bind time", "start", "seconds"), ["cold 90", "made 40", "warm 12", "warm 14"]);
      assert.deepEqual(await rows("Attach and detach time", "phase", "seconds"), ["attach 2", "attach 4", "detach 1"]);
      assert.deepEqual(await rows("Bindings released", "reason", "releases"), ["asked 1", "idle 1", "idle 1", "lost 1"]);
      assert.deepEqual(await rows("Pods the policy refused", "namespace", "refused"), ["alasio-branch-try-sessions 1", "alasio-branch-try-sessions 1"]);
      // Two refused in the last fifteen minutes; the warm binds of the last half hour, 13 s on average.
      assert.deepEqual(await answer(reader.db, alertQuery("The sandbox pods' policy refuses pods")), [{ refused: 2 }]);
      assert.deepEqual(await answer(reader.db, alertQuery("Binds are slow")), [{ seconds: 13 }]);
    } finally {
      reader.close();
    }
  });

  test("the alert rules say which volume server is filling its disk, which Valkey nears its memory limit, and which collection keeps too few copies", async () => {
    const now = String(BigInt(Date.now()) * 1_000_000n);
    const strings = (values: Record<string, string>) => Object.entries(values).map(([key, value]) => ({ key, value: { stringValue: value } }));
    const gauge = (resource: Record<string, string>, name: string, value: number, attributes: Record<string, string> = {}) => ({
      resource: { attributes: strings(resource) },
      scopeMetrics: [{ metrics: [{ name, gauge: { dataPoints: [{ timeUnixNano: now, asDouble: value, attributes: strings(attributes) }] } }] }],
    });
    // Two volume servers' disks of 1000 bytes, one with 50 to fill, the other 500; as the collector names what it scrapes for its job.
    const disk = (pod: string, type: string, bytes: number) => gauge({ "service.name": "seaweedfs" }, "SeaweedFS_volumeServer_resource", bytes, { pod, type, name: "/data" });
    // Two Valkeys, one holding 95% of its limit, the other 10%, each named by its receiver.
    const valkey = (address: string, name: string, bytes: number) => gauge({ "service.name": "redis", "server.address": address }, name, bytes);
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeTelemetry(db, telemetryRows("metrics", {
        resourceMetrics: [
          disk("filling-0", "all", 1000),
          disk("filling-0", "avail", 50),
          disk("roomy-0", "all", 1000),
          disk("roomy-0", "avail", 500),
          valkey("filer-store", "redis.memory.used", 950),
          valkey("filer-store", "redis.maxmemory", 1000),
          valkey("roomy-valkey", "redis.memory.used", 100),
          valkey("roomy-valkey", "redis.maxmemory", 1000),
          // Two masters, the leader saying 2 of neon's volumes under-replicated, the other its stale 0.
          gauge({ "service.name": "seaweedfs" }, "SeaweedFS_master_under_replicated_volumes", 2, { pod: "leader-0", collection: "neon-gate", rp: "001" }),
          gauge({ "service.name": "seaweedfs" }, "SeaweedFS_master_under_replicated_volumes", 0, { pod: "follower-1", collection: "neon-gate", rp: "001" }),
        ],
      }));
    });
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    const rule = (title: string) => {
      const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf(`title: ${title}`))) ?? [];
      return block.replaceAll(`\n${indent}  `, "\n").trim();
    };
    const reader = await openReader(readerConfig);
    try {
      const left = Object.fromEntries((await answer(reader.db, rule("The object store is filling its disk"))).map((row) => [row["server"], row["left"]]));
      assert.deepEqual([left["filling-0"], left["roomy-0"]], [5, 50]);
      const held = Object.fromEntries((await answer(reader.db, rule("A Valkey nears its memory limit"))).map((row) => [row["valkey"], row["held"]]));
      assert.deepEqual([held["filer-store"], held["roomy-valkey"]], [95, 10]);
      const under = Object.fromEntries((await answer(reader.db, rule("The object store keeps fewer copies than it should"))).map((row) => [row["collection"], row["volumes"]]));
      assert.equal(under["neon-gate"], 2);
    } finally {
      reader.close();
    }
  });

  test("the deliveries alert says whether main's alasio last found Telegram's deliveries failing, not a branch environment's", async () => {
    const point = (branch: string | null, minutesAgo: number, value: number) => ({
      resource: { attributes: [{ key: "service.name", value: { stringValue: "alasio" } }, ...(branch ? [{ key: "alasio.branch", value: { stringValue: branch } }] : [])] },
      scopeMetrics: [{ metrics: [{ name: "alasio.telegram.deliveries.failing", gauge: { dataPoints: [{ timeUnixNano: String(BigInt(Date.now() - minutesAgo * 60_000) * 1_000_000n), asDouble: value }] } }] }],
    });
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf("title: Telegram's deliveries are failing"))) ?? [];
    const query = block.replaceAll(`\n${indent}  `, "\n").trim();
    await withLake(async (db) => {
      await ensureOtel(db);
      await writeTelemetry(db, telemetryRows("metrics", { resourceMetrics: [point(null, 3, 1), point("try", 2, 0)] }));
    });
    const reader = await openReader(readerConfig);
    try {
      assert.deepEqual(await answer(reader.db, query), [{ failing: 1 }]);
      // Delivering again, as main's next ask found; a branch's failing pages no one.
      await withLake((db) => writeTelemetry(db, telemetryRows("metrics", { resourceMetrics: [point(null, 1, 0), point("try", 0, 1)] })));
      assert.deepEqual(await answer(reader.db, query), [{ failing: 0 }]);
    } finally {
      reader.close();
    }
  });

  test("the alert rules count the lake's passes that failed on a file the store does not hold, and no other failure of theirs", async () => {
    // As a pass exports what it logs (neon/lake/src/maintain.ts): one that failed on a lost file, one that failed otherwise.
    const failure = (because: string | null) => ({
      timeUnixNano: String(BigInt(Date.now()) * 1_000_000n),
      body: { stringValue: "merging the telemetry's files failed" },
      attributes: [{ key: "error", value: { stringValue: "HTTP 404" } }, ...(because ? [{ key: "because", value: { stringValue: because } }] : [])],
    });
    await withLake(async (db) => {
      await writeTelemetry(db, telemetryRows("logs", {
        resourceLogs: [{
          resource: { attributes: [{ key: "service.name", value: { stringValue: "alasio-lake-maintenance" } }] },
          scopeLogs: [{ logRecords: [failure("a file of the lake's catalog is gone from the object store"), failure(null)] }],
        }],
      }));
    });
    const rules = readFileSync(new URL("../neon/grafana/provisioning/alerting/rules.yaml", import.meta.url), "utf8");
    const [, indent = "", block = ""] = /^ *title: .*\n[\s\S]*?^( *)data: \|\n((?:\1 {2}.*\n)+)/mu.exec(rules.slice(rules.indexOf("title: The lake names files the object store does not hold"))) ?? [];
    const reader = await openReader(readerConfig);
    try {
      assert.deepEqual(await answer(reader.db, block.replaceAll(`\n${indent}  `, "\n").trim()), [{ failed: 1 }]);
    } finally {
      reader.close();
    }
  });

  test("the query endpoint answers 503 while it cannot open the lake", async () => {
    const endpoint = startEndpoint({ open: () => Promise.reject(new Error("the compute is restarting")), token: QUERY_TOKEN, log: () => {} });
    const server = createServer(endpoint.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      const answered = await fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${QUERY_TOKEN}` }, body: "select 1" });
      assert.deepEqual([answered.status, answered.statusText, await answered.text()], [503, "the lake cannot be read now: the compute is restarting", "the lake cannot be read now: the compute is restarting\n"]);
    } finally {
      server.close();
      await endpoint.close();
    }
  });
});

test("a failing load is tried again soon, and the loader turns unhealthy only once loads keep failing for long", async () => {
  let attempts = 0;
  const logs: string[] = [];
  // A connection of its own, which the load, replaced, never uses.
  const instance = await DuckDBInstance.create(":memory:");
  const db = await instance.connect();
  const loader = startLoader({
    open: async () => ({ db, close: async () => {}, replayed: async () => true, locked: unlocked }),
    join: async () => wakeSource([[woken(0n)], [woken(0n)], [woken(0n)], [woken(0n)], [woken(0n)], [woken(0n)]]),
    metrics: createMetrics(),
    log: (message) => logs.push(message),
    gatherMs: 1,
    retryMs: 10,
    unhealthyAfterMs: 300,
    sync: async () => {
      attempts += 1;
      throw new Error("permission denied for table entries");
    },
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.ok(attempts >= 3, `retried soon (${attempts} attempts)`);
    assert.deepEqual(loader.health(), { ok: true, detail: "load failed: permission denied for table entries" });
    await new Promise((resolve) => setTimeout(resolve, 300)); // past failing for long
    assert.equal(loader.health().ok, false);
  } finally {
    await loader.stop();
    db.closeSync();
    instance.closeSync();
  }
  assert.ok(logs.every((message) => message === "load failed"));
});

test("the lake is off unless ALASIO_LAKE_ENABLED is 1", () => {
  const enabled = (env: Record<string, string>) => Effect.runSync(lakeEnabled.parse(ConfigProvider.fromEnv({ env })));
  assert.equal(enabled({}), false);
  assert.equal(enabled({ ALASIO_LAKE_ENABLED: "0" }), false);
  assert.equal(enabled({ ALASIO_LAKE_ENABLED: " 1 " }), true);
});

test("the lake's configuration names what is missing and refuses what is malformed", () => {
  const base = { LAKE_DATABASE_HOST: "compute", LAKE_DATABASE_PASSWORD: "p", LAKE_DATA_PATH: "s3://lake/" };
  assert.throws(() => loadConfig({ ...base, ...LOADER_LOG }), /LAKE_S3_ENDPOINT is not set/);
  assert.throws(() => loadConfig({ ...base, LAKE_DATA_PATH: "/data", ...LOADER_LOG, LAKE_WAKE_GROUP: "" }), /LAKE_WAKE_GROUP is not set/);
  assert.throws(() => loadConfig({ ...base, LAKE_DATA_PATH: "/data", ...LOADER_LOG, LOG_PASSWORD: "" }), /LOG_PASSWORD is not set/);
  const config = loadConfig({ ...base, ...LOADER_LOG, LAKE_S3_ENDPOINT: "http://seaweedfs:8333", LAKE_S3_KEY: "k", LAKE_S3_SECRET: "s" });
  assert.deepEqual([config.source, config.catalog.database, config.catalog.user], [{ database: "alasio", options: "" }, "lake", "lake"]);
  assert.deepEqual([config.log, config.wake], [{ brokers: ["log:9092"], username: "loader-main", password: "log-test-password" }, { topic: "lake.main", group: "lake-loader.main" }]);
  // The query endpoint has the reader's credentials alone.
  const reader = { ...base, LAKE_S3_ENDPOINT: "http://seaweedfs:8333", LAKE_READER_PASSWORD: "r", LAKE_READER_S3_KEY: "rk", LAKE_READER_S3_SECRET: "rs" };
  assert.throws(() => loadEndpointConfig(reader), /LAKE_QUERY_TOKEN is not set/);
  const endpoint = loadEndpointConfig({ ...reader, LAKE_QUERY_TOKEN: "t" });
  assert.deepEqual(
    [endpoint.catalog.user, endpoint.catalog.password, endpoint.catalog.database, endpoint.s3?.key, endpoint.token, endpoint.port, endpoint.memoryLimit],
    ["lake_reader", "r", "lake", "rk", "t", 8090, "256MB"],
  );
  // Maintenance learns Neon's branches where it is told, and keeps a month of telemetry unless told otherwise.
  const local = { ...base, LAKE_DATA_PATH: "/data" };
  assert.deepEqual([loadMaintenanceConfig(local).branches, loadMaintenanceConfig(local).retentionDays], [null, 30]);
  assert.throws(() => loadMaintenanceConfig({ ...local, LAKE_RETENTION_DAYS: "-1" }), /LAKE_RETENTION_DAYS must be a positive number/);
  assert.throws(() => loadMaintenanceConfig({ ...local, LAKE_BRANCHES_URL: "http://control:8080/branches" }), /LAKE_BRANCHES_TOKEN is not set/);
  assert.deepEqual(loadMaintenanceConfig({ ...local, LAKE_BRANCHES_URL: "http://control:8080/branches", LAKE_BRANCHES_TOKEN: "t" }).branches, { url: "http://control:8080/branches", token: "t" });
  // Through Neon's proxy, every connection names its compute; straight to a Postgres, none.
  assert.equal(loadConfig({ ...local, ...LOADER_LOG }).catalog.options, "");
  assert.equal(loadConfig({ ...local, ...LOADER_LOG, LAKE_DATABASE_ENDPOINT: "branch-try" }).catalog.options, "endpoint=branch-try");
  assert.equal(loadEndpointConfig({ ...reader, LAKE_QUERY_TOKEN: "t", LAKE_DATABASE_ENDPOINT: "alasio" }).catalog.options, "endpoint=alasio");
  // Its source on the read replica, its catalog on the primary, each named apart.
  assert.deepEqual(loadConfig({ ...local, ...LOADER_LOG, LAKE_DATABASE_ENDPOINT: "alasio", LAKE_SOURCE_ENDPOINT: "alasio-ro" }).source, { database: "alasio", options: "endpoint=alasio-ro" });
  assert.deepEqual(loadConfig({ ...local, ...LOADER_LOG, LAKE_DATABASE_ENDPOINT: "alasio" }).source, { database: "alasio", options: "" });
  assert.deepEqual(loadMaintenanceConfig({ ...local, LAKE_SOURCE_ENDPOINT: "alasio-ro" }).source, { database: "alasio", options: "endpoint=alasio-ro" });
  assert.equal(conninfo("lake"), "dbname='lake' connect_timeout=60");
  assert.equal(conninfo("it's", "endpoint=alasio"), "dbname='it\\'s' connect_timeout=60 options='endpoint=alasio'");
});

test("the lake learns Neon's branches but main from neon-control, with its token, and fails on what is not a list of them", async () => {
  const asked: (string | undefined)[] = [];
  let answer: { status: number; body: unknown } = { status: 200, body: { branches: [{ name: "main" }, { name: "try-codex", state: "ready" }, { name: "gone", state: "deleting" }] } };
  const server = createServer((request, response) => {
    asked.push(request.headers.authorization);
    response.writeHead(answer.status, { "content-type": "application/json" }).end(JSON.stringify(answer.body));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const source = { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/branches`, token: "lake-token" };
  try {
    assert.deepEqual(await branchesBesideMain(source), ["try-codex", "gone"]);
    assert.deepEqual(asked, ["Bearer lake-token"]);
    answer = { status: 401, body: {} };
    await assert.rejects(branchesBesideMain(source), /neon-control answered 401/);
    answer = { status: 200, body: { error: "no" } };
    await assert.rejects(branchesBesideMain(source), /no list of branches/);
  } finally {
    server.close();
  }
});

test("query results print as a table, CSV, or JSON lines", () => {
  const results = [{ tool: "Bash", calls: 2828, input: '{"command":"ls"}' }, { tool: "a,b", calls: 1, input: null }];
  assert.equal(format(results, "json"), '{"tool":"Bash","calls":2828,"input":"{\\"command\\":\\"ls\\"}"}\n{"tool":"a,b","calls":1,"input":null}');
  assert.equal(format(results, "csv"), 'tool,calls,input\nBash,2828,"{""command"":""ls""}"\n"a,b",1,');
  assert.equal(format(results, "table"), [
    "tool | calls | input",
    "-----+-------+-----------------",
    'Bash | 2828  | {"command":"ls"}',
    "a,b  | 1     |",
    "(2 rows)",
  ].join("\n"));
});

/** A package.json, as far as this test reads it. */
interface PackageManifest {
  readonly dependencies?: Readonly<Record<string, string>>;
  readonly devDependencies?: Readonly<Record<string, string>>;
}

test("the lake's image pins the DuckDB and the protobuf decoder alasio tests it with", () => {
  const lake: PackageManifest = JSON.parse(readFileSync(new URL("../neon/lake/package.json", import.meta.url), "utf8"));
  const alasio: PackageManifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  for (const dependency of ["@duckdb/node-api", "protobufjs"]) {
    const pinned = lake.dependencies?.[dependency];
    assert.ok(pinned, dependency);
    assert.equal(alasio.devDependencies?.[dependency], pinned, dependency);
    assert.match(pinned, /^\d+\.\d+\.\d+/, dependency); // exact, not a range
  }
});
