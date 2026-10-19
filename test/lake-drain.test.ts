import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { drain, queriesRunning, queriesTaken } from "../neon/lake/src/drain.ts";
import { startEndpoint } from "../neon/lake/src/endpoint.ts";
import type { Lake } from "../neon/lake/src/lake.ts";

const TOKEN = "drain-token";

test("a reader's drain counts the processes of a procfs that run alasio lake's query script, other than its own", async (t) => {
  const proc = mkdtempSync(join(tmpdir(), "alasio-proc-"));
  t.after(() => rmSync(proc, { recursive: true, force: true }));
  const process_ = (pid: number, argv: string[] | null) => {
    mkdirSync(join(proc, String(pid)));
    if (argv) writeFileSync(join(proc, String(pid), "cmdline"), `${argv.join("\0")}\0`);
  };
  process_(1, ["node", "src/endpoint.ts"]);
  process_(40, ["node", "/opt/lake/src/query.ts", "--format", "table", "SELECT 1"]);
  process_(41, ["node", "/opt/lake/src/query.ts", "--format", "json", "SELECT 'src/query.ts'"]);
  process_(42, ["node", "src/drain.ts"]);
  // Ended as it was read.
  process_(43, null);
  // Its own, as the drain is never a query.
  process_(44, ["node", "src/query.ts"]);
  mkdirSync(join(proc, "self"));
  assert.equal(await queriesRunning(proc, 44), 2);
  assert.equal(await queriesRunning(proc, 1), 3);
});

test("a reader's drain sees a query script run on this host as it runs, and none once it has ended", async () => {
  const before = await queriesRunning();
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60_000)", "/opt/lake/src/query.ts"], { stdio: "ignore" });
  try {
    await new Promise((resolve) => child.once("spawn", resolve));
    assert.equal(await queriesRunning(), before + 1);
  } finally {
    child.kill("SIGKILL");
    await new Promise((resolve) => child.once("exit", resolve));
  }
  assert.equal(await queriesRunning(), before);
});

test("a reader's drain waits for a query run just before the stop to start, then until none is under way, within its bound", async () => {
  let clock = 0;
  const slept: number[] = [];
  const sleep = async (ms: number) => {
    slept.push(ms);
    clock += ms;
  };
  // None under way: it waits the settling alone.
  assert.equal(await drain({ underway: async () => 0, settleMs: 10_000, withinMs: 60_000, pollMs: 250, sleep, now: () => clock }), true);
  assert.deepEqual(slept, [10_000]);
  // Queries under way until the clock reaches 12 s: it waits for them.
  clock = 0;
  slept.length = 0;
  assert.equal(await drain({ underway: async () => (clock < 12_000 ? 2 : 0), settleMs: 10_000, withinMs: 60_000, pollMs: 250, sleep, now: () => clock }), true);
  assert.equal(clock, 12_000);
  // One that never ends: it gives up at its bound, from its start.
  clock = 0;
  assert.equal(await drain({ underway: async () => 1, settleMs: 10_000, withinMs: 60_000, pollMs: 250, sleep, now: () => clock }), false);
  assert.equal(clock, 60_000);
});

test("a reader's drain counts the queries its endpoint has taken and not answered, and none of an endpoint that does not say", async () => {
  // A DuckDB blocked on a statement it sent Postgres, which nothing interrupts: the query it takes holds the lake.
  const db = { extractStatements: async () => ({ count: 1 }), streamAndReadUntil: () => new Promise(() => {}), interrupt() {} };
  const endpoint = startEndpoint({ open: async () => ({ db, memory: async () => 0, close() {} }) as unknown as Lake, token: TOKEN, log: () => {}, answerWithinMs: 60_000 });
  const server = createServer(endpoint.handle);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    assert.equal(await queriesTaken(base), 0);
    const held = fetch(`${base}/query`, { method: "POST", headers: { authorization: `Bearer ${TOKEN}` }, body: "select 'held'", signal: AbortSignal.timeout(5_000) }).catch(() => null);
    for (const deadline = Date.now() + 5_000; (await queriesTaken(base)) === 0; await new Promise((resolve) => setTimeout(resolve, 20))) {
      assert.ok(Date.now() < deadline, "the endpoint never took the query");
    }
    assert.equal(await queriesTaken(base), 1);
    assert.equal(await drain({ underway: () => queriesTaken(base), settleMs: 0, withinMs: 300, pollMs: 50 }), false);
    void held;
  } finally {
    server.closeAllConnections();
    server.close();
  }
  assert.equal(await queriesTaken(base), 0);
});
