/**
 * What the cut-over from 9.0.0 gives each conversation of the sessions its rows name
 * (upgrades/from-9.0.0.sh, its SESSIONS_CLAIMED), as 9.0.0's rows recorded none of them: on this
 * release's state, made as alasio makes it, its rows written, and what was recorded of them as
 * they were let go of, as 9.0.0's was never recorded, it gives each conversation the sessions
 * its rows name, as this release records them as they are written, and the Codex threads their
 * histories start in, as the rollouts say; 9.0.0's tally of albums whose files failed gone, as
 * this release keeps none; run again, nothing more.
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { connect, createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

import { Effect } from "effect";
import pg from "pg";

import { CODEX_HARNESS } from "../src/harness/names.ts";
import { NeonRolloutStore } from "../src/codex/rollouts/store.ts";
import { Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

/**
 * The cut-over's SQL, as bash reads the script's definition of it, and as its pod is given it,
 * in an environment variable (its one_off), whose value Kubernetes gives with each `$$` a `$`,
 * as that escapes a `$(VAR)` reference.
 */
const SESSIONS_CLAIMED = spawnSync("bash", ["-c", 'eval "$(sed -n \'/^SESSIONS_CLAIMED="/,/^select count[^"]*"$/p\' "$0")"; printf %s "$SESSIONS_CLAIMED"', fileURLToPath(new URL("../upgrades/from-9.0.0.sh", import.meta.url))], { encoding: "utf8" }).stdout
  .split("$$").join("$");

/**
 * What the cut-over's pod runs its SQL with (the script's ON_DATABASE), as bash reads it, its
 * dependencies this checkout's, where the release's image keeps them at /opt/alasio.
 */
const ON_DATABASE_SCRIPT = spawnSync("bash", ["-c", 'eval "$(sed -n \'/^ON_DATABASE=\x27/,/^}\x27$/p\' "$0")"; printf %s "$ON_DATABASE"', fileURLToPath(new URL("../upgrades/from-9.0.0.sh", import.meta.url))], { encoding: "utf8" }).stdout;
const ON_DATABASE = ON_DATABASE_SCRIPT.split("/opt/alasio/package.json").join(fileURLToPath(new URL("../package.json", import.meta.url)));

/**
 * A package whose pg is this checkout's, but for the first connections, which fail as `fails`
 * says, in turn, each before any connection is made: a Node error of a connection's, by its code
 * (EPIPE), or the database's, by its SQLSTATE (57P03); for ON_DATABASE to be run on.
 */
function failingPg(fails: readonly string[]): { readonly packageJson: string; readonly remove: () => void } {
  const root = mkdtempSync(join(tmpdir(), "alasio-failing-pg-"));
  mkdirSync(join(root, "node_modules", "pg"), { recursive: true });
  writeFileSync(join(root, "package.json"), JSON.stringify({ name: "failing-pg" }));
  writeFileSync(join(root, "node_modules", "pg", "package.json"), JSON.stringify({ name: "pg", main: "index.js" }));
  writeFileSync(join(root, "node_modules", "pg", "index.js"), `const real = require(${JSON.stringify(createRequire(import.meta.url).resolve("pg"))});
const fails = ${JSON.stringify(fails)};
class Client extends real.Client {
  async connect() {
    const next = fails.shift();
    if (next === undefined) return super.connect();
    if (/^E[A-Z]+$/.test(next)) throw Object.assign(new Error("write " + next), { code: next, syscall: "write" });
    const refused = new real.DatabaseError("the database system is starting up", 0, "error");
    refused.code = next;
    throw refused;
  }
}
module.exports = { ...real, Client };
`);
  return { packageJson: join(root, "package.json"), remove: () => rmSync(root, { recursive: true, force: true }) };
}

/** What the cut-over's pod's script (ON_DATABASE) came to, run on `url`'s database with `sql`: its exit code, and what it printed. */
async function onDatabase(url: string, sql: string, script = ON_DATABASE): Promise<{ readonly code: number | null; readonly stdout: string; readonly stderr: string }> {
  const ran = spawn(process.execPath, ["--input-type=module", "-e", script], {
    env: { ...process.env, ALASIO_DATABASE_URL: url, ALASIO_CUTOVER_SQL: sql },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  ran.stdout.on("data", (chunk: Buffer) => (stdout += chunk));
  ran.stderr.on("data", (chunk: Buffer) => (stderr += chunk));
  const code = await new Promise<number | null>((resolve) => ran.on("exit", resolve));
  return { code, stdout, stderr };
}

test("the cut-over's pod, its first connections refused as a new pod's are until the network policies let its address in, connects as they do and runs its SQL once", async () => {
  assert.match(ON_DATABASE, /ALASIO_CUTOVER_SQL/u);
  const database = await startPostgres();
  // The database's address, refusing until it is let through, then reaching the database.
  const free = createServer();
  await new Promise<void>((resolve) => free.listen(0, "127.0.0.1", resolve));
  const port = (free.address() as { port: number }).port;
  await new Promise((resolve) => free.close(resolve));
  const { hostname, port: databasePort } = new URL(database.url);
  const open = new Set<Socket>();
  const through = createServer((socket) => {
    const upstream = connect(Number(databasePort), hostname);
    socket.pipe(upstream).pipe(socket);
    for (const end of [socket, upstream]) {
      open.add(end);
      end.on("close", () => open.delete(end));
      end.on("error", () => (socket.destroy(), upstream.destroy()));
    }
  });
  const letThrough = setTimeout(() => through.listen(port, "127.0.0.1"), 1_000);
  try {
    const url = new URL(database.url);
    url.port = String(port);
    const { code, stdout, stderr } = await onDatabase(url.toString(), "select 41 + 1; select 'once'");
    assert.equal(code, 0, stderr);
    assert.match(stderr, /^waiting for the database: ECONNREFUSED: /u);
    assert.equal(stdout, "42\nonce\n");
  } finally {
    clearTimeout(letThrough);
    for (const socket of open) socket.destroy();
    if (through.listening) await new Promise((resolve) => through.close(resolve));
    await database.stop();
  }
});

test("the cut-over's pod waits out a connection Node says failed, as of EPIPE, and a database taking none yet, as of 57P03, a compute starting; any other answer of the database's fails at once", async () => {
  const database = await startPostgres();
  try {
    for (const [fails, waited] of [[["EPIPE", "57P03"], 2], [["08006"], 1]] as const) {
      const stub = failingPg(fails);
      try {
        const { code, stdout, stderr } = await onDatabase(database.url, "select 1", ON_DATABASE_SCRIPT.split("/opt/alasio/package.json").join(stub.packageJson));
        assert.equal(code, 0, stderr);
        assert.equal(stdout, "1\n");
        assert.equal(stderr.split("\n").filter((line) => line.startsWith("waiting for the database: ")).length, waited, stderr);
        for (const fail of fails) assert.ok(stderr.includes(`waiting for the database: ${fail}: `), stderr);
      } finally {
        stub.remove();
      }
    }
    const stub = failingPg(["53300"]);
    try {
      const asked = Date.now();
      const { code, stderr } = await onDatabase(database.url, "select 1", ON_DATABASE_SCRIPT.split("/opt/alasio/package.json").join(stub.packageJson));
      assert.equal(code, 1);
      assert.match(stderr, /^it failed: 53300: /u);
      assert.ok(Date.now() - asked < 5_000, "it waited before it failed");
    } finally {
      stub.remove();
    }
  } finally {
    await database.stop();
  }
});

test("the cut-over's pod, refused by the database itself, as of a password it does not take, fails at once, waiting on nothing no wait mends", async () => {
  const database = await startPostgres();
  try {
    const url = new URL(database.url);
    url.password = "not-the-password";
    const asked = Date.now();
    const { code, stdout, stderr } = await onDatabase(url.toString(), "select 1");
    assert.equal(code, 1);
    assert.equal(stdout, "");
    assert.match(stderr, /^it failed: 28P01: /u);
    assert.ok(Date.now() - asked < 5_000, "it waited before it failed");
  } finally {
    await database.stop();
  }
});

test("the cut-over gives each conversation the sessions its rows name, as this release records them as they are written, and the threads their histories start in, drops 9.0.0's tally of albums and its restart events, gives its prompt jobs a goal's session, keeps its kept presses as controls, and nothing more run again", async () => {
  assert.match(SESSIONS_CLAIMED, /insert into state\.conversation_sessions/u);
  const database = await startPostgres();
  const pool = new pg.Pool({ connectionString: database.url, max: 2 });
  try {
    const store = await Effect.runPromise(Effect.provide(Store, Store.layer({ pool })));
    // Codex's rollouts: a fork's history starting in its parent's, which starts in one before, which no row names.
    const rollouts = new NeonRolloutStore(pool);
    await rollouts.ensureSchema();
    const kept = (threadId: string, historyBase: string | null) =>
      rollouts.save({ name: `rollout-${threadId}.jsonl`, path: `sessions/rollout-${threadId}.jsonl`, threadId, rolloutId: threadId, historyBase, size: 1, headDigest: "d", modifiedMs: 1 }, { start: 0, bytes: Buffer.from("x") });
    await kept("a-root", null);
    await kept("a-parent", "a-root");
    await kept("a-mounted", "a-parent");
    const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect);
    const naming = async (chatId: string, session: string) => {
      const conversationId = await run(store.upsertConversation({ chatId, user: { id: Number(chatId) } }));
      await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
      // Mounted as its operator chose it, a thread kept before any row named it.
      await run(store.mountGivenSession(conversationId, `${session}-mounted`));
      await run(store.claimTurn({ conversationId, chatId, messageId: "1", sessionId: `${session}-turn`, harness: CODEX_HARNESS, prompt: "go" }));
      await pool.query("insert into state.workspace_sessions (conversation_id, harness, working_directory, session_id) values ($1, 'claude', '/before', $2)", [conversationId, `${session}-before`]);
      return conversationId;
    };
    const first = await naming("41", "a");
    const second = await naming("42", "b");
    const recorded = async () => (await pool.query<{ session_id: string; conversation_id: string }>("select session_id, conversation_id from state.conversation_sessions order by 1, 2")).rows;
    const named = await recorded();
    // As this release records them, the mounted thread's lineage with it.
    assert.deepEqual(named.map(({ session_id }) => session_id), ["a-before", "a-mounted", "a-parent", "a-root", "a-turn", "b-before", "b-mounted", "b-turn"]);
    assert.deepEqual(new Set(named.map(({ conversation_id }) => conversation_id)), new Set([first, second]));
    // As 9.0.0's rows were: none of it recorded; its restart events, which name the sessions of the turns its stop cut short,
    // and which this release reads nothing of, as a turn's row names its session; its tally of albums whose files failed:
    // both of which this release keeps none of; and its turns, which have no column of the binding each runs on.
    await pool.query("delete from state.conversation_sessions");
    await pool.query("alter table state.turns drop column binding");
    await pool.query("alter table state.prompt_jobs drop column goal_session_id");
    await pool.query("create table state.media_group_failures (media_group_id text primary key, failures integer not null default 1)");
    await pool.query("create table state.restart_events (thread_key text primary key, cause text not null, channel text not null, thread_ts text not null, session_id text, command text, recorded_at timestamptz not null default now())");
    await pool.query("insert into state.restart_events (thread_key, cause, channel, thread_ts, session_id) values ($1, 'external', '41', '1', 'a-restarted')", [first]);
    // And its presses kept as their controls ran on, one its stop let go of.
    await pool.query("create table state.press_jobs (id text primary key, conversation_id text not null, callback_query jsonb not null, action jsonb not null, holder text not null, held_until timestamptz not null, answer text, created_at timestamptz not null default now())");
    const callbackQuery = { id: "press-1", data: "action-1" };
    const action = { id: "action-1", conversationId: first, kind: "workspace:refresh", payload: {} };
    await pool.query("insert into state.press_jobs (id, conversation_id, callback_query, action, holder, held_until) values ('press-1', $1, $2, $3, 'stopped', now())", [first, JSON.stringify(callbackQuery), JSON.stringify(action)]);
    /** How many sessions the conversations have, as the cut-over's SQL says last. */
    const claimed = async () => {
      const results = (await pool.query(SESSIONS_CLAIMED)) as unknown as { rows: { count: string }[] }[];
      return results.at(-1)?.rows[0]?.count;
    };
    assert.equal(await claimed(), "8");
    assert.equal((await pool.query("select to_regclass('state.media_group_failures') as kept")).rows[0]?.kept, null, "9.0.0's tally of albums is kept");
    assert.equal((await pool.query("select to_regclass('state.restart_events') as kept")).rows[0]?.kept, null, "9.0.0's restart events are kept");
    assert.equal((await pool.query("select count(*) from information_schema.columns where table_schema = 'state' and table_name = 'turns' and column_name = 'binding'")).rows[0]?.count, "1", "9.0.0's turns have no binding");
    assert.equal((await pool.query("select count(*) from information_schema.columns where table_schema = 'state' and table_name = 'prompt_jobs' and column_name = 'goal_session_id'")).rows[0]?.count, "1", "9.0.0's jobs have no goal's session");
    // Its kept press a control of this release's, to run again; its table gone.
    assert.deepEqual(await run(store.takeLapsedControls("next", 60_000)), [{ control: { kind: "press", callbackQuery, action }, answer: null }]);
    assert.equal((await pool.query("select to_regclass('state.press_jobs') as kept")).rows[0]?.kept, null, "9.0.0's kept presses' table is kept");
    // The rows' sessions, and the first's mounted thread's lineage, its: as this release records them; none its restart event alone named.
    assert.deepEqual(await recorded(), named);
    // Run again, as a cut-over run again past it would: the same.
    assert.equal(await claimed(), "8");
  } finally {
    await pool.end();
    await database.stop();
  }
});
