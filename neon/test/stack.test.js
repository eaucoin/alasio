/**
 * alasio's Neon, as alasio runs it: a throwaway stack brought up from nothing
 * through startNeon, with the analytics lake on, then crashed, restarted, and
 * made to lose a disk, with nothing committed lost. Needs Docker and the stack's
 * images; slow (about ten minutes). `npm run test:neon`.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { promisify } from "node:util";

import pg from "pg";

import { NeonSessionStore } from "../../src/harness/claude/session-store.js";
import { composeCommand, startNeon } from "../../src/neon/stack.js";
import { dockerAvailable } from "../../test/support/postgres.js";
import { sessionStoreConformance } from "../../test/support/session-store-conformance.js";
import { signToken } from "../control/jwt.js";
import { neonLayout } from "../control/setup.js";

const run = promisify(execFile);
const skip = !dockerAvailable() && "needs Docker";

const project = `alasio-neon-test-${process.pid}`;
const stateDir = mkdtempSync(join(tmpdir(), "alasio-neon-test-"));
const layout = neonLayout(stateDir);
let computePort;
let neon;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function freePort() {
  const server = createServer();
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

const compose = (...args) =>
  run("docker", [...composeCommand(layout, project), ...args], { maxBuffer: 64 * 1024 * 1024 });
const container = (service) => `${project}-${service}-1`;

/**
 * Brings every service up and waits for it to be healthy. Where one does not
 * come up, the error carries the compute's state and last log lines, the
 * compute being the service that depends on all the others.
 */
async function up() {
  try {
    await compose("up", "--detach", "--wait", "--wait-timeout", "300");
  } catch (error) {
    const state = await run("docker", ["inspect", "--format", "{{json .State}}", container("compute")]).then((r) => r.stdout, () => "");
    const logs = await run("docker", ["logs", "--tail", "80", container("compute")], { maxBuffer: 16 * 1024 * 1024 }).then((r) => r.stdout + r.stderr, () => "");
    error.message += `\ncompute state: ${state}\ncompute log:\n${logs}`;
    throw error;
  }
}

/** Starts alasio's side of the stack, with the analytics lake on as alasio runs it here. */
async function start({ lake = true } = {}) {
  // Forgotten before it is closed, so a start that fails leaves nothing to close twice.
  const previous = neon;
  neon = null;
  await previous?.close();
  neon = await startNeon({ stateDir, project, computePort, lake });
}

/** A query on a connection of its own, which survives the stack restarting. */
async function query(sql, params) {
  const client = new pg.Client({
    connectionString: readFileSync(layout.databaseUrlFile, "utf8").trim(),
    connectionTimeoutMillis: 60_000,
    query_timeout: 300_000,
  });
  // A connection the stack drops surfaces on the query; nothing to add here.
  client.on("error", () => {});
  await client.connect();
  try {
    return (await client.query(sql, params)).rows;
  } finally {
    await client.end();
  }
}

function record() {
  return JSON.parse(readFileSync(join(layout.control, "bootstrap.json"), "utf8"));
}

function token(scope) {
  return signToken(readFileSync(layout.privateKey, "utf8"), scope);
}

/** An HTTP call from inside one of the stack's containers, which have curl. */
async function inside(service, method, url, body) {
  const args = ["exec", container(service), "curl", "-sS", "-X", method, "-H", `authorization: Bearer ${token(service === "pageserver" ? "pageserverapi" : "safekeeperdata")}`];
  if (body !== undefined) args.push("-H", "content-type: application/json", "-d", JSON.stringify(body));
  const { stdout } = await run("docker", [...args, url], { maxBuffer: 64 * 1024 * 1024 });
  return stdout;
}

async function pageserverMetric(name) {
  const metrics = await inside("pageserver", "GET", "http://127.0.0.1:9898/metrics");
  const line = metrics.split("\n").find((l) => l.startsWith(`${name} `));
  return Number(line?.split(" ")[1] ?? NaN);
}

/** Writes rows until stopped, counting only those whose commit returned. */
function writer(table) {
  const committed = [];
  let stopping = false;
  let next = 1;
  const done = (async () => {
    while (!stopping) {
      try {
        const client = new pg.Client({
          connectionString: readFileSync(layout.databaseUrlFile, "utf8").trim(),
          connectionTimeoutMillis: 60_000,
          query_timeout: 60_000,
        });
        // Killing the compute drops the connection: the loop reconnects.
        client.on("error", () => {});
        await client.connect();
        try {
          while (!stopping) {
            const id = next++;
            await client.query(`insert into ${table} (id) values ($1)`, [id]);
            committed.push(id);
          }
        } finally {
          await client.end().catch(() => {});
        }
      } catch {
        await sleep(500);
      }
    }
  })();
  return { committed, stop: async () => ((stopping = true), await done) };
}

before(async () => {
  if (skip) return;
  computePort = await freePort();
  await start();
});

after(async () => {
  // Whatever state the tests left, the throwaway stack goes.
  await neon?.close().catch(() => {});
  // Every profile's services, the lake's included whether or not the last start turned it on.
  if (!skip) await compose("--profile", "*", "down", "--volumes", "--remove-orphans").catch(() => {});
  rmSync(stateDir, { recursive: true, force: true });
});

let schemas = 0;
sessionStoreConformance(
  async () => {
    const store = new NeonSessionStore(neon.pool, { schema: `conformance_${++schemas}` });
    await store.ensureSchema();
    return store;
  },
  { skip },
);

describe("alasio's Neon stack", { skip }, () => {
  test("recreates neon-control and the compute when neon-control's code changes, and nothing else", async () => {
    const services = async () =>
      Object.fromEntries(
        (await run("docker", ["ps", "--filter", `label=com.docker.compose.project=${project}`, "--format", '{{.Label "com.docker.compose.service"}} {{.ID}}']))
          .stdout.trim().split("\n").map((line) => line.split(" ")),
      );
    const before = await services();
    const env = readFileSync(layout.composeEnv, "utf8");
    writeFileSync(layout.composeEnv, env.replace(/^ALASIO_NEON_CONTROL_REVISION=.*$/mu, "ALASIO_NEON_CONTROL_REVISION='changed'"));
    try {
      await up();
    } finally {
      writeFileSync(layout.composeEnv, env);
    }
    const after = await services();
    const recreated = Object.keys(after).filter((service) => after[service] !== before[service]).sort();
    assert.deepEqual(recreated, ["compute", "neon-control"]);
    assert.deepEqual(await query("select 1 as ok"), [{ ok: 1 }]);
    await start();
  });

  test("bootstraps its tenant and a timeline on three safekeepers", () => {
    const { safekeepers } = record();
    assert.deepEqual([...safekeepers.ids].sort(), [1, 2, 3]);
  });

  test("keeps every row through a full stop and start", async () => {
    await query("create table kept (id int primary key)");
    await query("insert into kept select g from generate_series(1, 50000) g");
    await compose("stop");
    await start();
    assert.deepEqual(await query("select count(*)::int as n from kept"), [{ n: 50000 }]);
  });

  for (const victim of ["pageserver", "safekeeper-2", "compute", "storage-controller", "seaweedfs"]) {
    test(`loses nothing committed when ${victim} is killed mid-write`, async () => {
      const table = `crash_${victim.replace("-", "_")}`;
      await query(`create table ${table} (id int primary key)`);
      const rows = writer(table);
      await sleep(3000);
      await run("docker", ["kill", "--signal", "KILL", container(victim)]);
      await sleep(15_000);
      await rows.stop();
      await up();
      const present = new Set((await query(`select id from ${table}`)).map((row) => row.id));
      assert.ok(rows.committed.length > 0);
      assert.deepEqual(rows.committed.filter((id) => !present.has(id)), []);
    });
  }

  test("a safekeeper that lost its disk is rebuilt from its peers", async () => {
    await compose("stop", "safekeeper-3");
    for (const name of readdirSync(layout.safekeeper(3))) {
      rmSync(join(layout.safekeeper(3), name), { recursive: true, force: true });
    }
    await query("create table after_loss (id int primary key)");
    await query("insert into after_loss values (1)");
    await compose("up", "--detach", "--wait", "safekeeper-3");
    const { tenantId, timelineId } = record();
    const url = `http://127.0.0.1:7676/v1/tenant/${tenantId}/timeline/${timelineId}`;
    const deadline = Date.now() + 120_000;
    let state = "";
    while (Date.now() < deadline) {
      state = await inside("safekeeper-3", "GET", url);
      if (state.includes("flush_lsn")) break;
      await sleep(5000);
    }
    assert.match(state, /flush_lsn/u);
    await query("insert into after_loss values (2)");
  });

  test("garbage the pageserver collects is deleted from S3, validated by the storage controller", async () => {
    const { tenantId, timelineId } = record();
    await query("create table churn (id int primary key, payload text)");
    for (let pass = 0; pass < 3; pass++) {
      await query("truncate churn");
      await query("insert into churn select g, repeat(md5(random()::text), 30) from generate_series(1, 60000) g");
    }
    const before = await pageserverMetric("pageserver_deletion_queue_executed_total");
    const timeline = `http://127.0.0.1:9898/v1/tenant/${tenantId}/timeline/${timelineId}`;
    await inside("pageserver", "PUT", `${timeline}/compact?force_l0_compaction=true&force_repartition=true&force_image_layer_creation=true&wait_until_uploaded=true`);
    const deadline = Date.now() + 120_000;
    let executed = before;
    while (Date.now() < deadline && !(executed > before)) {
      await sleep(5000);
      executed = await pageserverMetric("pageserver_deletion_queue_executed_total");
    }
    assert.ok(executed > before, `deletions executed: ${before} -> ${executed}`);
    assert.equal(
      await pageserverMetric("pageserver_deletion_queue_validated_total"),
      await pageserverMetric("pageserver_deletion_queue_executed_total"),
    );
  });

  test("backs alasio's database up to a dump any Postgres restores", async () => {
    const dumps = readdirSync(layout.backups).filter((name) => name.endsWith(".dump"));
    assert.ok(dumps.length > 0, "a dump was written when the stack started");
    const { stdout } = await run("docker", [
      "run", "--rm", "--volume", `${layout.backups}:/backups:ro`, "--entrypoint", "pg_restore",
      "neondatabase/compute-node-v17:release-compute-9073@sha256:ed6a613231d7026b4df8b00563444b9f33745370a3b3f0a2183e723f460ba974",
      "--list", `/backups/${dumps.sort().at(-1)}`,
    ]);
    assert.match(stdout, /TABLE DATA/u);
  });

  test("reads the database as it was at a past moment", async () => {
    await query("create table history (id int primary key)");
    await query("insert into history select g from generate_series(1, 100) g");
    await sleep(2000);
    const moment = new Date().toISOString();
    await sleep(2000);
    await query("insert into history select g from generate_series(101, 200) g");

    const { tenantId, timelineId } = record();
    const found = JSON.parse(
      await inside("pageserver", "GET", `http://127.0.0.1:9898/v1/tenant/${tenantId}/timeline/${timelineId}/get_lsn_by_timestamp?timestamp=${encodeURIComponent(moment)}`),
    );
    assert.equal(found.kind, "present");

    // A read-only compute pinned to that moment's LSN.
    const specDir = mkdtempSync(join(tmpdir(), "alasio-neon-static-"));
    const config = JSON.parse(readFileSync(join(layout.control, "compute", "config.json"), "utf8"));
    config.spec.mode = { Static: found.lsn };
    config.spec.safekeeper_connstrings = [];
    delete config.spec.safekeepers_generation;
    writeFileSync(join(specDir, "config.json"), JSON.stringify(config));
    const name = `${project}-static`;
    await run("docker", [
      "run", "--detach", "--rm", "--name", name, "--network", `${project}_default`,
      "--volume", `${specDir}:/config:ro`,
      "neondatabase/compute-node-v17:release-compute-9073@sha256:ed6a613231d7026b4df8b00563444b9f33745370a3b3f0a2183e723f460ba974",
      "--pgdata", "/var/db/postgres/compute", "--connstr", "postgresql://cloud_admin@localhost:55433/postgres",
      "--pgbin", "/usr/local/bin/postgres", "--compute-id", "static", "--config", "/config/config.json",
    ]);
    try {
      let count = null;
      const deadline = Date.now() + 120_000;
      while (count === null && Date.now() < deadline) {
        try {
          const { stdout } = await run("docker", ["exec", name, "psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", "select count(*) from history"]);
          count = Number(stdout.trim());
        } catch {
          await sleep(2000);
        }
      }
      assert.equal(count, 100);
    } finally {
      await run("docker", ["rm", "--force", "--volumes", name]).catch(() => {});
      rmSync(specDir, { recursive: true, force: true });
    }
  });
});

/** A read-only query of the lake, run in its container as `npm run lake` runs one. */
async function lakeQuery(sql) {
  const { stdout } = await run("docker", ["exec", container("lake"), "node", "src/query.js", "--format", "json", sql], { maxBuffer: 64 * 1024 * 1024 });
  return stdout.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
}

/** Waits until `check` holds, a load of the lake being what makes it so. */
async function untilLoaded(check, what) {
  const deadline = Date.now() + 180_000;
  let last;
  while (Date.now() < deadline) {
    last = await check().catch((error) => error);
    if (last === true) return;
    await sleep(2000);
  }
  assert.fail(`the lake never ${what}: ${last instanceof Error ? last.message : last}`);
}

describe("alasio's analytics lake", { skip }, () => {
  const KEY = { projectKey: "-stack-test", sessionId: "22222222-2222-4222-8222-222222222222" };
  const entry = (n) => ({ type: "user", uuid: `stack-${n}`, timestamp: new Date(1790000000000 + n).toISOString(), message: { role: "user", content: `entry ${n}` } });
  const sourceCount = async () => (await query("select count(*)::int as n from claude_sessions.entries"))[0].n;
  const lakeCounts = async () => (await lakeQuery("select count(*) as n, count(distinct seq) as seqs from claude.entries"))[0];

  test("loads what alasio's stores hold, as it starts and as it runs", async () => {
    await neon.store.append(KEY, Array.from({ length: 50 }, (_, n) => entry(n)));
    const line = `${JSON.stringify({ timestamp: new Date().toISOString(), type: "session_meta", payload: { id: "thread-s" } })}\n`;
    await neon.rollouts.save(
      { name: "rollout-stack.jsonl", path: "sessions/rollout-stack.jsonl", threadId: "thread-s", rolloutId: "thread-s", historyBase: null, size: Buffer.byteLength(line), headDigest: "h", modifiedMs: Date.now() },
      { start: 0, bytes: Buffer.from(line) },
    );
    await run("docker", ["restart", container("lake")]); // a load as it starts
    const expected = await sourceCount();
    await untilLoaded(async () => Number((await lakeCounts()).n) === expected, `held all ${expected} entries`);
    assert.deepEqual(await lakeQuery("select type, thread_id from codex.lines"), [{ type: "session_meta", thread_id: "thread-s" }]);
  });

  test("keeps every entry exactly once through its loader being killed mid-load", async () => {
    await neon.store.append(KEY, Array.from({ length: 20_000 }, (_, n) => entry(1000 + n)));
    await run("docker", ["restart", container("lake")]);
    await sleep(1500);
    await run("docker", ["kill", "--signal", "KILL", container("lake")]);
    await up();
    const expected = await sourceCount();
    await untilLoaded(async () => {
      const { n, seqs } = await lakeCounts();
      return Number(n) === expected && Number(seqs) === expected;
    }, `held each of ${expected} entries exactly once`);
  });

  test("answers queries read-only", async () => {
    await assert.rejects(lakeQuery("delete from claude.entries"), /read-only|read only/iu);
    assert.ok(Number((await lakeCounts()).n) > 0);
  });

  test("turned off, stops, and its role reads none of alasio's data", async () => {
    await start({ lake: false });
    try {
      const { stdout } = await compose("--profile", "lake", "ps", "--all", "--format", "{{.Service}}", "lake");
      assert.equal(stdout.trim(), "");
      const password = JSON.parse(readFileSync(layout.secretsFile, "utf8")).lakePassword;
      const lake = new pg.Client({ host: "127.0.0.1", port: computePort, user: "lake", password, database: "alasio" });
      await lake.connect();
      try {
        // A member of no role, so not of neon_superuser, which reads every table.
        assert.deepEqual(await query("select count(*)::int as n from pg_auth_members where member = 'lake'::regrole"), [{ n: 0 }]);
        await assert.rejects(lake.query("select count(*) from claude_sessions.entries"), /permission denied/u);
      } finally {
        await lake.end();
      }
    } finally {
      await start();
    }
  });
});
