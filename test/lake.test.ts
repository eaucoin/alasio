// @ts-nocheck
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import pg from "pg";

import { syncClaude } from "../neon/lake/src/claude.ts";
import { syncCodex } from "../neon/lake/src/codex.ts";
import { loadConfig } from "../neon/lake/src/config.ts";
import { LAKE, openLake, rows } from "../neon/lake/src/lake.ts";
import { startLoader } from "../neon/lake/src/loader.ts";
import { createMetrics } from "../neon/lake/src/metrics.ts";
import { lakeModelVersion, MODEL_VERSION } from "../neon/lake/src/model.ts";
import { format } from "../neon/lake/src/query.ts";
import { lastMaintained, maintainLake, prepareLake, syncLake } from "../neon/lake/src/sync.ts";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../src/harness/claude/session-store.ts";
import { ensureLakeRole, LAKE_ROLE, lakeEnabled, syncLakeReads } from "../src/neon/lake.ts";
import { dockerAvailable, startPostgres } from "./support/postgres.ts";

// The lake loads alasio's Neon into DuckLake. Here its source is a throwaway Postgres
// written through alasio's own stores, its catalog a database there owned by the lake's
// role, and its files a local directory; DuckDB is the real one the lake runs.

const skip = !dockerAvailable() && "needs Docker";
const LAKE_PASSWORD = "lake-test-password";

let postgres;
let admin; // the source database, as its owner (alasio)
let store;
let rollouts;
let dataDir;
let config;

before(async () => {
  if (skip) return;
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
  await syncLakeReads(admin, true);
  dataDir = mkdtempSync(join(tmpdir(), "alasio-lake-data-"));
  config = loadConfig({
    LAKE_DATABASE_HOST: url.hostname,
    LAKE_DATABASE_PORT: url.port,
    LAKE_DATABASE_PASSWORD: LAKE_PASSWORD,
    LAKE_DATA_PATH: dataDir,
  });
});

after(async () => {
  await admin?.end();
  await postgres?.stop();
  if (dataDir) rmSync(dataDir, { recursive: true, force: true });
});

/** Opens the lake with a fresh, empty model, and closes it after `fn`. */
async function withLake(fn) {
  const lake = await openLake(config);
  try {
    await prepareLake(lake.db, { rebuild: true });
    return await fn(lake.db);
  } finally {
    lake.close();
  }
}

const one = async (db, sql, values) => (await rows(db, sql, values))[0];

// --- Claude Code transcripts ----------------------------------------------------

const KEY = { projectKey: "-home-operator-bayma", sessionId: "11111111-1111-4111-8111-111111111111" };
const SUBAGENT = { ...KEY, subpath: "subagents/agent-a1" };

const at = (second) => `2026-10-01T12:00:${String(second).padStart(2, "0")}.000Z`;
const assistant = (uuid, second, content, usage, extra = {}) => ({
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

describe("Claude Code transcripts", { skip }, () => {
  test("every entry is loaded with its fields typed and its whole kept", async () => {
    await store.append(KEY, TRANSCRIPT);
    await store.append(SUBAGENT, [{ type: "user", uuid: "s1", timestamp: at(4), isSidechain: true, message: { role: "user", content: "subtask" } }]);
    await withLake(async (db) => {
      assert.deepEqual(await syncClaude(db), { inserted: 6, deleted: 0 });
      const entries = await rows(db, `select * from ${LAKE}.claude.entries order by seq`);
      assert.deepEqual(entries.map((entry) => [entry.type, entry.subpath]), [
        ["user", ""], ["assistant", ""], ["assistant", ""], ["user", ""], ["ai-title", ""], ["user", "subagents/agent-a1"],
      ]);
      const [prompt, , call] = entries;
      assert.equal(prompt.occurred_at.toISOString(), at(1));
      assert.equal(prompt.malformed, false);
      assert.equal(JSON.parse(prompt.entry).message.content, "fix the build\u0000 please"); // whole, NUL and all
      assert.deepEqual(
        [call.message_id, call.model, call.stop_reason, call.output_tokens, call.cache_read_input_tokens, call.thinking_tokens, call.service_tier, call.cwd, call.git_branch],
        ["msg_1", "claude-opus-5-5", "tool_use", 153n, 10173n, 33n, "standard", "/home/operator/bayma", "main"],
      );
      assert.equal(entries[5].is_sidechain, true);
    });
  });

  test("a message is counted once, with the usage its last entry carries", async () => {
    await withLake(async (db) => {
      await syncClaude(db);
      const messages = await rows(db, `select * from ${LAKE}.claude.messages`);
      assert.equal(messages.length, 1);
      assert.deepEqual([messages[0].entries, messages[0].output_tokens, messages[0].stop_reason], [2n, 153n, "tool_use"]);
    });
  });

  test("content blocks are typed, and each tool call is paired with its result", async () => {
    await withLake(async (db) => {
      await syncClaude(db);
      const blocks = await rows(db, `select block_type, role, text_chars, tool_name, is_error from ${LAKE}.claude.content_blocks order by seq, block_index`);
      assert.deepEqual(blocks.map((block) => [block.block_type, block.role, block.text_chars]), [
        ["text", "user", 21n], ["thinking", "assistant", 10n], ["tool_use", "assistant", null], ["tool_result", "user", 9n], ["text", "user", 7n],
      ]);
      const [call] = await rows(db, `select * from ${LAKE}.claude.tool_calls`);
      assert.deepEqual(
        [call.tool_name, JSON.parse(call.tool_input).command, call.is_error, call.result_chars, call.answered_at - call.called_at],
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
      assert.equal((await one(db, `select count(*) as n from ${LAKE}.claude.entries where subpath <> ''`)).n, 0n);
      assert.equal((await one(db, `select count(*) as n from ${LAKE}.claude.content_blocks where subpath <> ''`)).n, 0n);
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
      const odd = await one(db, `select type, malformed, entry from ${LAKE}.claude.entries where uuid = 'odd'`);
      assert.equal(odd.malformed, true);
      assert.equal(odd.type, null);
      assert.match(JSON.parse(odd.entry), /\\ud800/);
    });
  });

  test("a load cut short leaves whole batches and the next load finishes it", async () => {
    await withLake(async (db) => {
      let batches = 0;
      const failing = () => {
        batches += 1;
        if (batches === 2) throw new Error("cut short");
      };
      await assert.rejects(syncClaude(db, { batchEntries: 3, beforeCommit: failing }), /cut short/);
      const [{ n: kept }] = await rows(db, `select count(*) as n from ${LAKE}.claude.entries`);
      assert.equal(kept, 3n); // the first batch, whole; nothing of the second
      const { n: total } = await (await admin.query("select count(*)::int as n from claude_sessions.entries")).rows[0];
      assert.deepEqual(await syncClaude(db, { batchEntries: 3 }), { inserted: total - 3, deleted: 0 });
    });
  });
});

// --- Codex rollouts ---------------------------------------------------------------

const line = (record) => `${JSON.stringify(record)}\n`;
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

/** Writes a rollout's bytes from `start` as the rollout store keeps them. */
async function saveRollout(name, bytes, { start = 0, size = bytes.length + start, head = bytes, path = `sessions/2026/10/01/${name}`, store: target = rollouts } = {}) {
  const firstLine = head.subarray(0, head.indexOf(0x0a) >= 0 ? head.indexOf(0x0a) : head.length);
  await target.save(
    { name, path, threadId: "thread-1", rolloutId: "thread-1", historyBase: null, size, headDigest: createHash("sha256").update(firstLine).digest("hex"), modifiedMs: 1790000000000 },
    { start, bytes },
  );
}

describe("Codex rollouts", { skip }, () => {
  const NAME = "rollout-2026-10-01T12-00-00-thread-1.jsonl";
  const whole = Buffer.from(ROLLOUT_LINES.join(""));

  test("complete lines are loaded as they are written, a partial one once its newline lands", async () => {
    const firstPart = whole.subarray(0, whole.indexOf(ROLLOUT_LINES[3]) + 20); // three lines and part of the fourth
    await saveRollout(NAME, firstPart);
    await withLake(async (db) => {
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 3, deleted: 0 });
      await saveRollout(NAME, whole.subarray(firstPart.length), { start: firstPart.length, head: whole });
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 4, deleted: 0 });
      const lines = await rows(db, `select line_number, byte_offset, type, payload_type, turn_id, malformed from ${LAKE}.codex.lines order by line_number`);
      assert.deepEqual(lines.map((row) => Number(row.line_number)), [0, 1, 2, 3, 4, 5, 6]);
      let offset = 0;
      for (const [index, row] of lines.entries()) {
        assert.equal(Number(row.byte_offset), offset);
        offset += Buffer.byteLength(ROLLOUT_LINES[index]);
      }
      assert.deepEqual([lines[3].payload_type, lines[3].turn_id], ["custom_tool_call", TURN]);
      const file = await one(db, `select loaded_bytes, lines, size from ${LAKE}.codex.files`);
      assert.deepEqual([file.loaded_bytes, file.lines, file.size], [BigInt(whole.length), 7n, BigInt(whole.length)]);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 0 });
    });
  });

  test("turns, token usage, and tool calls are shaped from the lines", async () => {
    await withLake(async (db) => {
      await syncCodex(db);
      const turn = await one(db, `select * from ${LAKE}.codex.turns`);
      assert.deepEqual([turn.turn_id, turn.model, turn.reasoning_effort, turn.duration_ms, turn.time_to_first_token_ms], [TURN, "gpt-5.6-sol", "high", 21773n, 18515n]);
      const usage = await one(db, `select * from ${LAKE}.codex.token_usage`);
      assert.deepEqual([usage.response_id, usage.input_tokens, usage.output_tokens, usage.reasoning_output_tokens], ["resp_1", 13737n, 576n, 470n]);
      const call = await one(db, `select * from ${LAKE}.codex.tool_calls`);
      assert.deepEqual([call.name, call.kind, call.input, JSON.parse(call.output)[0].text, call.turn_id], ["exec", "custom_tool_call", "6*7", "42", TURN]);
    });
  });

  test("a rewritten file is loaded again whole, a moved one followed, a removed one dropped", async () => {
    await withLake(async (db) => {
      await syncCodex(db);
      // Codex rewrote it: a new first line, and a line that is not JSON.
      const rewritten = Buffer.from(line({ timestamp: at(9), type: "session_meta", payload: { id: "thread-1", rewritten: true } }) + "not json\n");
      await saveRollout(NAME, rewritten);
      assert.deepEqual(await syncCodex(db), { files: 1, inserted: 2, deleted: 7 });
      const lines = await rows(db, `select line_number, malformed, record from ${LAKE}.codex.lines order by line_number`);
      assert.deepEqual(lines.map((row) => [Number(row.line_number), row.malformed]), [[0, false], [1, true]]);
      assert.equal(JSON.parse(lines[1].record), "not json"); // kept, as a JSON string

      await rollouts.move(NAME, `archived_sessions/${NAME}`);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 0 });
      assert.equal((await one(db, `select path from ${LAKE}.codex.files`)).path, `archived_sessions/${NAME}`);

      await admin.query("delete from codex_sessions.rollouts where name = $1", [NAME]);
      assert.deepEqual(await syncCodex(db), { files: 0, inserted: 0, deleted: 2 });
      assert.equal((await one(db, `select count(*) as n from ${LAKE}.codex.lines`)).n, 0n);
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

// --- The lake as a whole -------------------------------------------------------------

describe("the lake", { skip }, () => {
  test("a lake of another model version is rebuilt empty, and one of this version kept", async () => {
    const lake = await openLake(config);
    try {
      await prepareLake(lake.db, { rebuild: true });
      await syncLake(lake.db);
      assert.equal(await prepareLake(lake.db), false);
      assert.ok((await one(lake.db, `select count(*) as n from ${LAKE}.claude.entries`)).n > 0n);
      await lake.db.run(`update ${LAKE}.loader.meta set value = '0' where key = 'model_version'`);
      assert.equal(await prepareLake(lake.db), true);
      assert.equal(await lakeModelVersion(lake.db), MODEL_VERSION);
      assert.equal((await one(lake.db, `select count(*) as n from ${LAKE}.claude.entries`)).n, 0n);
    } finally {
      lake.close();
    }
  });

  test("maintenance runs and is recorded, and the loader loop loads, maintains, and stops", async () => {
    await withLake(async (db) => {
      const metrics = createMetrics();
      const logs = [];
      const open = async () => ({ db, close: async () => {}, lost: () => false });
      const loader = startLoader({ open, metrics, log: (message) => logs.push(message), intervalMs: 60_000, maintenanceIntervalMs: 3_600_000 });
      for (let waited = 0; !logs.includes("maintained") && waited < 30_000; waited += 50) {
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      await loader.stop();
      assert.deepEqual(logs, ["loaded", "maintained"]);
      assert.deepEqual(loader.health(), { ok: true, detail: "loaded" });
      assert.ok(await lastMaintained(db));
      const text = metrics.render();
      assert.match(text, /^lake_cycles_total\{outcome="success"\} 1$/m);
      assert.match(text, /^lake_maintenance_total\{outcome="success"\} 1$/m);
      assert.match(text, /^# TYPE lake_rows_total counter$/m);
      // Maintained within the interval: the next loader does not maintain again.
      const again = startLoader({ open, metrics, log: (message) => logs.push(message), intervalMs: 60_000, maintenanceIntervalMs: 3_600_000 });
      await new Promise((resolve) => setTimeout(resolve, 500));
      await again.stop();
      assert.equal(logs.filter((message) => message === "maintained").length, 1);
      await maintainLake(db); // and on demand it runs at once
    });
  });

  test("a load that fails drops its connections, and the next is made on fresh ones", async () => {
    await withLake(async (db) => {
      const events = [];
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
    const lake = await openLake(config, { readOnly: true });
    try {
      await assert.rejects(lake.db.run(`delete from ${LAKE}.claude.entries`), /read-only|read only/i);
      assert.ok((await rows(lake.db, `select count(*) from ${LAKE}.claude.entries`)).length);
    } finally {
      lake.close();
    }
  });

  test("the lake's role is a member of no other role, and making it again changes nothing", async () => {
    await ensureLakeRole(admin, LAKE_PASSWORD);
    const { rows } = await admin.query("select count(*)::int as n from pg_auth_members where member = $1::regrole", [LAKE_ROLE]);
    assert.equal(rows[0].n, 0);
    const lake = await openLake(config);
    lake.close(); // its password still lets it in
    const { rows: [database] } = await admin.query("select has_database_privilege('public', 'lake', 'connect') as public_connects");
    assert.equal(database.public_connects, false);
  });

  test("with the lake off, its role reads none of alasio's data", async () => {
    await syncLakeReads(admin, false);
    try {
      const lake = await openLake(config);
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

test("a failing load is tried again soon, and the loader turns unhealthy only once loads keep failing", async () => {
  let attempts = 0;
  const logs = [];
  const loader = startLoader({
    open: async () => ({ db: null, close: async () => {}, lost: () => false }),
    metrics: createMetrics(),
    log: (message) => logs.push(message),
    intervalMs: 100,
    maintenanceIntervalMs: 3_600_000,
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
  }
  assert.ok(logs.every((message) => message === "load failed"));
});

test("the lake is off unless ALASIO_LAKE_ENABLED is 1", () => {
  assert.equal(lakeEnabled({}), false);
  assert.equal(lakeEnabled({ ALASIO_LAKE_ENABLED: "0" }), false);
  assert.equal(lakeEnabled({ ALASIO_LAKE_ENABLED: "1" }), true);
});

test("the lake's configuration names what is missing and refuses what is malformed", () => {
  const base = { LAKE_DATABASE_HOST: "compute", LAKE_DATABASE_PASSWORD: "p", LAKE_DATA_PATH: "s3://lake/" };
  assert.throws(() => loadConfig(base), /LAKE_S3_ENDPOINT is not set/);
  assert.throws(() => loadConfig({ ...base, LAKE_DATA_PATH: "/data", LAKE_INTERVAL_SECONDS: "soon" }), /LAKE_INTERVAL_SECONDS must be a positive number/);
  const config = loadConfig({ ...base, LAKE_S3_ENDPOINT: "http://seaweedfs:8333", LAKE_S3_KEY: "k", LAKE_S3_SECRET: "s" });
  assert.deepEqual([config.source.database, config.catalog.database, config.source.user, config.intervalMs], ["alasio", "lake", "lake", 300_000]);
});

test("query results print as a table, CSV, or JSON lines", () => {
  const results = [{ tool: "Bash", calls: 2828n, input: { command: "ls" } }, { tool: "a,b", calls: 1n, input: null }];
  assert.equal(format(results, "json"), '{"tool":"Bash","calls":"2828","input":{"command":"ls"}}\n{"tool":"a,b","calls":"1","input":null}');
  assert.equal(format(results, "csv"), 'tool,calls,input\nBash,2828,"{""command"":""ls""}"\n"a,b",1,');
  assert.equal(format(results, "table"), [
    "tool | calls | input",
    "-----+-------+-----------------",
    'Bash | 2828  | {"command":"ls"}',
    "a,b  | 1     |",
    "(2 rows)",
  ].join("\n"));
});

test("the lake's image pins the DuckDB alasio tests it with", () => {
  const lake = JSON.parse(readFileSync(new URL("../neon/lake/package.json", import.meta.url), "utf8"));
  const alasio = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.equal(alasio.devDependencies["@duckdb/node-api"], lake.dependencies["@duckdb/node-api"]);
  assert.match(lake.dependencies["@duckdb/node-api"], /^\d+\.\d+\.\d+/); // exact, not a range
});
