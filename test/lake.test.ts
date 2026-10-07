import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { gzipSync } from "node:zlib";

import { type DuckDBConnection, DuckDBInstance, type DuckDBValue } from "@duckdb/node-api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { ConfigProvider, Effect } from "effect";
import pg from "pg";

import { branchesBesideMain } from "../neon/lake/src/branches.ts";
import { syncClaude } from "../neon/lake/src/claude.ts";
import { syncCodex } from "../neon/lake/src/codex.ts";
import { type EndpointConfig, type LakeConfig, loadConfig, loadEndpointConfig } from "../neon/lake/src/config.ts";
import { startEndpoint } from "../neon/lake/src/endpoint.ts";
import { EXTENSIONS } from "../neon/lake/src/extensions.ts";
import { startIntake } from "../neon/lake/src/intake.ts";
import { LAKE, openLake, type Row, rows, transaction } from "../neon/lake/src/lake.ts";
import { startLoader } from "../neon/lake/src/loader.ts";
import { createMetrics } from "../neon/lake/src/metrics.ts";
import { lakeModelVersion, MODEL_VERSION } from "../neon/lake/src/model.ts";
import { ensureOtel, flushTelemetry, telemetryRows, writeTelemetry } from "../neon/lake/src/otel.ts";
import type { LogsRequest, MetricsRequest } from "../neon/lake/src/otlp.ts";
import { format } from "../neon/lake/src/query.ts";
import { answer, grantReads, openReader } from "../neon/lake/src/reader.ts";
import { lastMaintained, maintainLake, prepareLake, syncLake } from "../neon/lake/src/sync.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { ensureLakeReaderRole, ensureLakeRole, LAKE_READER_ROLE, LAKE_ROLE, lakeEnabled, syncLakeReads } from "../src/neon/lake.ts";
import { startPostgres, type TestPostgres } from "./support/postgres.ts";

// The lake loads alasio's Neon into DuckLake. Here its source is a throwaway Postgres
// written through alasio's own stores, its catalog a database there owned by the lake's
// role, and its files a local directory; DuckDB is the real one the lake runs.

const LAKE_PASSWORD = "lake-test-password";
const READER_PASSWORD = "reader-test-password";
const QUERY_TOKEN = "query-test-token";

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
  config = loadConfig({ ...env, LAKE_DATABASE_PASSWORD: LAKE_PASSWORD });
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

/** Opens the lake with a fresh, empty model, and closes it after `fn`. */
async function withLake<T>(fn: (db: DuckDBConnection) => Promise<T>): Promise<T> {
  const lake = await openLake(config, { source: config.source });
  try {
    await prepareLake(lake.db, { rebuild: true });
    return await fn(lake.db);
  } finally {
    lake.close();
  }
}

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

/** The lake as the intake opens it: no source, the telemetry's schema ready. */
async function openForIntake() {
  const lake = await openLake(config);
  await ensureOtel(lake.db);
  return lake;
}

/** POSTs `body` to the intake at `base`, as the collector's exporter does. */
const post = (base: string, path: string, body: Uint8Array | string, headers: Record<string, string>) =>
  fetch(`${base}${path}`, { method: "POST", headers, body });

describe("telemetry", () => {
  test("the intake writes each request it takes, at once queryable from another DuckDB, and refuses what is not OTLP", async () => {
    const spans = new InMemorySpanExporter();
    const tracer = new BasicTracerProvider({ resource: resourceFromAttributes({ "service.name": "intake-test" }), spanProcessors: [new SimpleSpanProcessor(spans)] }).getTracer("t");
    tracer.startSpan("alasio.turn", { attributes: { "alasio.conversation.id": "telegram:7" } }).end();
    const traces = ProtobufTraceSerializer.serializeRequest(spans.getFinishedSpans());
    assert.ok(traces);

    const metrics = createMetrics();
    const intake = startIntake({ open: openForIntake, metrics, log: () => {} });
    const server = createServer(intake.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    try {
      for (let waited = 0; !intake.ready() && waited < 30_000; waited += 50) await new Promise((resolve) => setTimeout(resolve, 50));
      assert.ok(intake.ready());
      const protobuf = await post(base, "/v1/traces", gzipSync(traces), { "content-type": "application/x-protobuf", "content-encoding": "gzip" });
      assert.deepEqual([protobuf.status, protobuf.headers.get("content-type"), (await protobuf.arrayBuffer()).byteLength], [200, "application/x-protobuf", 0]);
      const json = await post(base, "/v1/logs", JSON.stringify(logRequest("intake-test", NOW, NOW + 1n)), { "content-type": "application/json; charset=utf-8" });
      assert.deepEqual([json.status, await json.text()], [200, "{}"]);

      assert.equal((await post(base, "/v1/logs", "{", { "content-type": "application/json" })).status, 400);
      assert.equal((await post(base, "/v1/logs", "{}", { "content-type": "text/plain" })).status, 415);
      assert.equal((await post(base, "/v1/logs", "{}", { "content-type": "application/json", "content-encoding": "br" })).status, 415);
      assert.equal((await post(base, "/v1/profiles", "{}", { "content-type": "application/json" })).status, 404);

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
      assert.match(text, /^lake_telemetry_requests_total\{signal="logs",outcome="malformed"\} 1$/m);
    } finally {
      server.close();
      await intake.stop();
    }
  });

  test("the intake flushes what it inlined and merges the small files it wrote, every so often", async () => {
    const intake = startIntake({ open: openForIntake, metrics: createMetrics(), log: () => {}, tidyIntervalMs: 500 });
    const server = createServer(intake.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const reader = await openLake(config, { readOnly: true });
    try {
      // A server listening on a TCP port has an address of its own.
      const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      for (let waited = 0; !intake.ready() && waited < 30_000; waited += 50) await new Promise((resolve) => setTimeout(resolve, 50));
      const today = new Date(Number(NOW / 1_000_000n));
      const partition = `year=${today.getUTCFullYear()}/month=${today.getUTCMonth() + 1}/day=${today.getUTCDate()}/`;
      // Today's files, which are those the intake merges.
      const files = async () => (await one<Count>(reader.db, `select count(*) as n from ducklake_list_files('${LAKE}', 'metrics_gauge', schema => 'otel') where contains(data_file, '${partition}')`)).n;
      const before = await files();
      for (const offset of [0n, 1n, 2n]) {
        assert.equal((await post(base, "/v1/metrics", JSON.stringify(gaugeRequest("tidied", NOW + offset)), { "content-type": "application/json" })).status, 200);
      }
      assert.equal(await files(), before + 3n);
      for (let waited = 0; (await files()) > 1n && waited < 30_000; waited += 200) await new Promise((resolve) => setTimeout(resolve, 200));
      assert.equal(await files(), 1n);
      assert.equal((await one<Count>(reader.db, `select count(*) as n from ${LAKE}.otel.metrics_gauge where ServiceName = 'tidied'`)).n, 3n);
    } finally {
      reader.close();
      server.close();
      await intake.stop();
    }
  });

  test("the intake answers 503 while it cannot open the lake, so the collector sends again", async () => {
    const intake = startIntake({ open: () => Promise.reject(new Error("the compute is restarting")), metrics: createMetrics(), log: () => {}, retryMs: 10 });
    const server = createServer(intake.handle);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      assert.equal(intake.ready(), false);
      assert.equal((await post(base, "/v1/logs", JSON.stringify(logRequest("unwritten", NOW)), { "content-type": "application/json" })).status, 503);
    } finally {
      server.close();
      await intake.stop();
    }
  });

  test("spans and log records are inlined in the catalog, and metric points written to files at once", async () => {
    await withLake(async (db) => {
      const files = async (table: string) => (await one<Count>(db, `select count(*) as n from ducklake_list_files('${LAKE}', '${table}', schema => 'otel')`)).n;
      const before = { logs: await files("logs"), gauge: await files("metrics_gauge") };
      await writeTelemetry(db, telemetryRows("logs", logRequest("inlined", NOW)));
      await writeTelemetry(db, telemetryRows("metrics", gaugeRequest("inlined", NOW)));
      assert.deepEqual({ logs: await files("logs"), gauge: await files("metrics_gauge") }, { logs: before.logs, gauge: before.gauge + 1n });
    });
  });

  test("maintenance deletes the days of telemetry past its retention, and then their files", async () => {
    await withLake(async (db) => {
      const days = [40n, 31n, 30n, 0n];
      await writeTelemetry(db, telemetryRows("logs", logRequest("retained", ...days.map((ago) => NOW - ago * DAY))));
      await flushTelemetry(db); // to files, whose deletion follows
      const day = (ago: bigint) => new Date(Number((NOW - ago * DAY) / 1_000_000n));
      const partition = (date: Date) => join(dataDir ?? "", "otel", "logs", `year=${date.getUTCFullYear()}`, `month=${date.getUTCMonth() + 1}`, `day=${date.getUTCDate()}`);
      const files = (ago: bigint) => readdirSync(partition(day(ago))).filter((name) => name.endsWith(".parquet"));
      assert.ok(files(40n).length > 0);
      // Nothing kept past its need, so maintenance deletes what it may at once.
      await db.run(`call ${LAKE}.set_option('expire_older_than', '0 seconds')`);
      await db.run(`call ${LAKE}.set_option('delete_older_than', '0 seconds')`);

      await maintainLake(db, { retentionDays: 30, keepFiles: false });
      const kept = await rows<{ day: Date }>(db, `select Timestamp::DATE::TIMESTAMP as day from ${LAKE}.otel.logs where ServiceName = 'retained' order by Timestamp`);
      assert.deepEqual(kept.map(({ day }) => day.toISOString().slice(0, 10)), [30n, 0n].map((ago) => day(ago).toISOString().slice(0, 10)));
      // The pass after the one that deleted them deletes their files, once their snapshots expire.
      await new Promise((resolve) => setTimeout(resolve, 1000));
      await maintainLake(db, { retentionDays: 30, keepFiles: false });
      assert.deepEqual([files(40n), files(31n)], [[], []]);
      assert.ok(files(30n).length > 0);
    });
  });
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

  test("maintenance runs and is recorded, and the loader loop loads, maintains, and stops", async () => {
    await withLake(async (db) => {
      const metrics = createMetrics();
      const logs: string[] = [];
      const open = async () => ({ db, close: async () => {}, lost: () => false });
      const loader = startLoader({ open, metrics, log: (message) => logs.push(message), intervalMs: 60_000, maintenanceIntervalMs: 3_600_000, retentionDays: 30 });
      for (let waited = 0; !logs.includes("maintained") && waited < 30_000; waited += 50) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      await loader.stop();
      assert.deepEqual(logs, ["loaded", "maintained"]);
      assert.deepEqual(loader.health(), { ok: true, detail: "loaded" });
      assert.ok(await lastMaintained(db));
      const text = metrics.render();
      assert.match(text, /^lake_cycles_total\{outcome="success"\} 1$/m);
      assert.match(text, /^lake_maintenance_total\{outcome="success",files="deleted"\} 1$/m);
      assert.match(text, /^# TYPE lake_rows_total counter$/m);
      // Maintained within the interval: the next loader does not maintain again.
      const again = startLoader({ open, metrics, log: (message) => logs.push(message), intervalMs: 60_000, maintenanceIntervalMs: 3_600_000, retentionDays: 30 });
      await new Promise((resolve) => setTimeout(resolve, 500));
      await again.stop();
      assert.equal(logs.filter((message) => message === "maintained").length, 1);
      await maintainLake(db, { retentionDays: 30, keepFiles: false }); // and on demand it runs at once
    });
  });

  test("while Neon has branches the loader's maintenance deletes no file, nor when it cannot learn them; a branch's lake is never maintained", async () => {
    await withLake(async (db) => {
      const metrics = createMetrics();
      const logs: string[] = [];
      const open = async () => ({ db, close: async () => {}, lost: () => false });
      const maintained = async (options: Pick<Parameters<typeof startLoader>[0], "maintenanceIntervalMs" | "branches">) => {
        await db.run(`delete from ${LAKE}.loader.meta where key = 'maintained_at'`);
        const loader = startLoader({ open, metrics, log: (message, fields) => logs.push(`${message}${fields ? ` ${JSON.stringify(fields)}` : ""}`), intervalMs: 60_000, retentionDays: 30, ...options });
        for (let waited = 0; !logs.some((line) => line.startsWith("maintained")) && waited < 2000; waited += 50) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        await loader.stop();
        return logs.splice(0).filter((line) => !line.startsWith("loaded"));
      };
      assert.deepEqual(await maintained({ maintenanceIntervalMs: 3_600_000, branches: async () => ["try-codex"] }), ['maintained {"keptFilesFor":["try-codex"]}']);
      assert.deepEqual(await maintained({
        maintenanceIntervalMs: 3_600_000,
        branches: async () => {
          throw new Error("neon-control answered 503");
        },
      }), ['could not learn Neon\'s branches; keeping every file {"error":"neon-control answered 503"}', 'maintained {"keptFilesFor":"unknown branches"}']);
      assert.deepEqual(await maintained({ maintenanceIntervalMs: 3_600_000, branches: async () => [] }), ["maintained {}"]);
      assert.deepEqual(await maintained({ maintenanceIntervalMs: null }), []);
      assert.equal(await lastMaintained(db), null);
      const text = metrics.render();
      assert.match(text, /^lake_maintenance_total\{outcome="success",files="kept"\} 2$/m);
      assert.match(text, /^lake_maintenance_total\{outcome="success",files="deleted"\} 1$/m);
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

  test("a load that fails drops its connections, and the next is made on fresh ones", async () => {
    await withLake(async (db) => {
      const events: string[] = [];
      let attempts = 0;
      const loader = startLoader({
        open: async () => {
          events.push("open");
          return { db, close: async () => events.push("close"), lost: () => false };
        },
        metrics: createMetrics(),
        log: (message) => events.push(message),
        intervalMs: 60_000,
        maintenanceIntervalMs: 3_600_000,
        retentionDays: 30,
        retryMs: 10,
        sync: async (lake) => {
          attempts += 1;
          if (attempts === 1) throw new Error("terminating connection due to administrator command");
          return syncLake(lake);
        },
      });
      for (let waited = 0; !events.includes("maintained") && waited < 30_000; waited += 20) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      await loader.stop();
      assert.deepEqual(events, ["open", "load failed", "close", "open", "loaded", "maintained", "close"]);
      assert.deepEqual(loader.health(), { ok: true, detail: "loaded" });
    });
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
      await flushTelemetry(db); // to files, which the reader reads
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
      await flushTelemetry(db);
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
        await flushTelemetry(db);
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

test("a failing load is tried again soon, and the loader turns unhealthy only once loads keep failing", async () => {
  let attempts = 0;
  const logs: string[] = [];
  // A connection of its own, which the load, replaced, never uses.
  const instance = await DuckDBInstance.create(":memory:");
  const db = await instance.connect();
  const loader = startLoader({
    open: async () => ({ db, close: async () => {}, lost: () => false }),
    metrics: createMetrics(),
    log: (message) => logs.push(message),
    intervalMs: 100,
    maintenanceIntervalMs: 3_600_000,
    retentionDays: 30,
    retryMs: 10,
    sync: async () => {
      attempts += 1;
      throw new Error("permission denied for table entries");
    },
  });
  try {
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.ok(attempts >= 3, `retried sooner than the interval (${attempts} attempts)`);
    assert.deepEqual(loader.health(), { ok: true, detail: "load failed: permission denied for table entries" });
    await new Promise((resolve) => setTimeout(resolve, 300)); // past three intervals of failing
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
  assert.throws(() => loadConfig(base), /LAKE_S3_ENDPOINT is not set/);
  assert.throws(() => loadConfig({ ...base, LAKE_DATA_PATH: "/data", LAKE_INTERVAL_SECONDS: "soon" }), /LAKE_INTERVAL_SECONDS must be a positive number/);
  const config = loadConfig({ ...base, LAKE_S3_ENDPOINT: "http://seaweedfs:8333", LAKE_S3_KEY: "k", LAKE_S3_SECRET: "s" });
  assert.deepEqual([config.source.database, config.catalog.database, config.source.user, config.intervalMs], ["alasio", "lake", "lake", 300_000]);
  // The query endpoint has the reader's credentials alone.
  const reader = { ...base, LAKE_S3_ENDPOINT: "http://seaweedfs:8333", LAKE_READER_PASSWORD: "r", LAKE_READER_S3_KEY: "rk", LAKE_READER_S3_SECRET: "rs" };
  assert.throws(() => loadEndpointConfig(reader), /LAKE_QUERY_TOKEN is not set/);
  const endpoint = loadEndpointConfig({ ...reader, LAKE_QUERY_TOKEN: "t" });
  assert.deepEqual(
    [endpoint.catalog.user, endpoint.catalog.password, endpoint.catalog.database, endpoint.s3?.key, endpoint.token, endpoint.port, endpoint.memoryLimit],
    ["lake_reader", "r", "lake", "rk", "t", 8090, "256MB"],
  );
  assert.deepEqual([config.maintenanceIntervalMs, config.branches], [86_400_000, null]);
  const local = { ...base, LAKE_DATA_PATH: "/data" };
  assert.equal(loadConfig({ ...local, LAKE_MAINTENANCE_HOURS: "0" }).maintenanceIntervalMs, null);
  assert.throws(() => loadConfig({ ...local, LAKE_MAINTENANCE_HOURS: "-1" }), /LAKE_MAINTENANCE_HOURS must be a positive number/);
  assert.throws(() => loadConfig({ ...local, LAKE_BRANCHES_URL: "http://control:8080/branches" }), /LAKE_BRANCHES_TOKEN is not set/);
  assert.deepEqual(loadConfig({ ...local, LAKE_BRANCHES_URL: "http://control:8080/branches", LAKE_BRANCHES_TOKEN: "t" }).branches, { url: "http://control:8080/branches", token: "t" });
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
