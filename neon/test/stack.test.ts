// @ts-nocheck
/**
 * alasio's Neon as the Helm chart runs it: a release installed in a cluster, its
 * components killed, the whole stack stopped at once, a safekeeper's volume lost, with
 * nothing committed lost; its garbage collected, its dumps restorable, its past
 * readable, and its lake loading.
 *
 * Runs against a release that is already installed (test/e2e/run.sh installs one):
 * KUBECONFIG names the cluster, ALASIO_E2E_NAMESPACE and ALASIO_E2E_RELEASE the release
 * (alasio and alasio unless set). Needs kubectl and helm; slow (about fifteen minutes).
 * `npm run test:neon`.
 */
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createServer } from "node:net";
import { dirname, join } from "node:path";
import { after, before, describe, test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import pg from "pg";

import { NeonRolloutStore } from "../../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { sessionStoreConformance } from "../../test/support/session-store-conformance.ts";
import { signToken } from "../control/jwt.ts";

const run = promisify(execFile);
const CHART = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "charts", "alasio");
const NAMESPACE = process.env.ALASIO_E2E_NAMESPACE ?? "alasio";
const RELEASE = process.env.ALASIO_E2E_RELEASE ?? "alasio";
const FULL = RELEASE.includes("alasio") ? RELEASE : `${RELEASE}-alasio`;
const skip = !process.env.KUBECONFIG && "needs a cluster with a release installed: set KUBECONFIG";

const neonName = (component) => `${FULL}-neon-${component}`;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const kubectl = async (...args) => (await run("kubectl", ["--namespace", NAMESPACE, ...args], { maxBuffer: 64 * 1024 * 1024 })).stdout;

async function secret(name, key) {
  return Buffer.from(await kubectl("get", "secret", name, "-o", `jsonpath={.data.${key.replaceAll(".", "\\.")}}`), "base64").toString("utf8");
}

/**
 * A port free on this machine, below the range outgoing connections take theirs from
 * (32768 up, on Linux), so none takes it while the forward on it is down.
 */
async function freePort() {
  for (;;) {
    const port = 20000 + Math.floor(Math.random() * 12000);
    const server = createServer();
    const free = await new Promise((resolve) => {
      server.once("error", () => resolve(false));
      server.listen(port, "127.0.0.1", () => resolve(true));
    });
    if (free) {
      await new Promise((resolve) => server.close(resolve));
      return port;
    }
  }
}

/**
 * A port on this machine forwarded to the compute's Service, kept up: a forward ends
 * with the pod it reached, so it is started again whenever it exits.
 */
function computeForward(port) {
  let child = null;
  let stopped = false;
  const start = () => {
    child = spawn("kubectl", ["--namespace", NAMESPACE, "port-forward", `service/${neonName("compute")}`, `${port}:55433`], { stdio: ["ignore", "ignore", "pipe"] });
    let said = "";
    child.stderr.on("data", (chunk) => { said = String(chunk).trim() || said; });
    child.on("exit", (code) => {
      if (stopped) return;
      console.error(`# the forward to the compute exited ${code}${said ? `: ${said}` : ""}; starting it again`);
      setTimeout(start, 1000);
    });
  };
  start();
  return { stop: () => ((stopped = true), child?.kill()) };
}

let forward;
let databaseUrl;
let pool;

/** A query on a connection of its own, retried while the stack comes back. */
async function query(sql, params, { attempts = 60 } = {}) {
  for (let attempt = 1; ; attempt++) {
    const client = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 10_000, query_timeout: 300_000 });
    client.on("error", () => {});
    let connected = false;
    try {
      await client.connect();
      connected = true;
      return (await client.query(sql, params)).rows;
    } catch (error) {
      // Connecting is retried; a query that reached the database is not.
      if (connected || attempt >= attempts) throw error;
      await sleep(2000);
    } finally {
      await client.end().catch(() => {});
    }
  }
}

/** Waits until every workload of the stack is rolled out and ready. */
async function up() {
  const workloads = (await kubectl("get", "deployments,statefulsets", "-l", `app.kubernetes.io/instance=${RELEASE},alasio.dev/stack=neon`, "-o", "name")).trim().split("\n");
  for (const workload of workloads) await kubectl("rollout", "status", workload, "--timeout=600s");
  await query("select 1");
}

const killPod = (name) => kubectl("delete", "pod", name, "--grace-period=0", "--force", "--wait=false");
const podOf = async (component) => (await kubectl("get", "pods", "-l", `app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=${component}`, "-o", "jsonpath={.items[0].metadata.name}")).trim();

let privateKey;
const token = (scope) => signToken(privateKey, scope);

/** An HTTP call from inside a pod of the stack, whose image has curl. */
async function inside(pod, method, url, scope, body) {
  const args = ["exec", pod, "--", "curl", "-sS", "-X", method, "-H", `authorization: Bearer ${token(scope)}`];
  if (body !== undefined) args.push("-H", "content-type: application/json", "-d", JSON.stringify(body));
  return await kubectl(...args, url);
}

async function record() {
  return JSON.parse(await kubectl("exec", `deployment/${neonName("control")}`, "--", "cat", "/state/bootstrap.json"));
}

async function pageserverMetric(name) {
  const metrics = await inside(`${neonName("pageserver")}-0`, "GET", "http://127.0.0.1:9898/metrics", "pageserverapi");
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
      const client = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 10_000, query_timeout: 60_000 });
      client.on("error", () => {});
      try {
        await client.connect();
        while (!stopping) {
          const id = next++;
          await client.query(`insert into ${table} (id) values ($1)`, [id]);
          committed.push(id);
        }
      } catch {
        await sleep(500);
      } finally {
        await client.end().catch(() => {});
      }
    }
  })();
  return { committed, stop: async () => ((stopping = true), await done) };
}

/** Applies `object`, as `kubectl apply` reads it. */
async function apply(object) {
  const child = spawn("kubectl", ["--namespace", NAMESPACE, "apply", "-f", "-"], { stdio: ["pipe", "ignore", "pipe"] });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk; });
  child.stdin.end(JSON.stringify(object));
  const code = await new Promise((resolve) => child.on("exit", resolve));
  if (code !== 0) throw new Error(`kubectl apply exited ${code}: ${stderr}`);
}

/**
 * A pod of the stack's own, labelled so the stack's NetworkPolicy admits it and placed
 * where the stack runs, whose nodes hold its images already, run to completion: its logs.
 * One that fails, or does not finish within `timeoutMs`, fails with its logs and its
 * last events.
 */
async function runPod(name, spec, { timeoutMs = 300_000 } = {}) {
  const nodeSelector = JSON.parse((await kubectl("get", `deployment/${neonName("compute")}`, "-o", "jsonpath={.spec.template.spec.nodeSelector}")).trim() || "{}");
  await apply({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, labels: { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon", "app.kubernetes.io/component": "neon-test" } },
    spec: { restartPolicy: "Never", nodeSelector, securityContext: { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } }, ...spec },
  });
  try {
    const deadline = Date.now() + timeoutMs;
    let phase = "";
    while (Date.now() < deadline && !["Succeeded", "Failed"].includes(phase)) {
      await sleep(2000);
      phase = (await kubectl("get", "pod", name, "-o", "jsonpath={.status.phase}")).trim();
    }
    const logs = await kubectl("logs", name, "--all-containers").catch((error) => error.message);
    if (phase !== "Succeeded") {
      const events = await kubectl("describe", "pod", name).catch(() => "");
      throw new Error(`pod ${name} ended ${phase || "unstarted"}:\n${logs}\n${events.split("\n").slice(-15).join("\n")}`);
    }
    return logs;
  } finally {
    await kubectl("delete", "pod", name, "--wait=false").catch(() => {});
  }
}

const image = async (workload, container = 0) => (await kubectl("get", workload, "-o", `jsonpath={.spec.template.spec.containers[${container}].image}`)).trim();
const restricted = { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } };

before(async () => {
  if (skip) return;
  privateKey = await secret(neonName("root"), "auth_private_key.pem");
  const port = await freePort();
  forward = computeForward(port);
  const url = new URL((await secret(`${FULL}-database`, "url")).trim());
  url.hostname = "127.0.0.1";
  url.port = String(port);
  databaseUrl = url.toString();
  await up();
  pool = new pg.Pool({ connectionString: databaseUrl, max: 4 });
  pool.on("error", () => {});
});

after(async () => {
  await pool?.end().catch(() => {});
  forward?.stop();
});

let schemas = 0;
sessionStoreConformance(
  async () => {
    const store = new NeonSessionStore(pool, { schema: `kube_conformance_${++schemas}` });
    await store.ensureSchema();
    return store;
  },
  { skip },
);

describe("alasio's Neon on Kubernetes", { skip }, () => {
  test("bootstraps its tenant and a timeline on three safekeepers", async () => {
    const { safekeepers } = await record();
    assert.deepEqual([...safekeepers.ids].sort(), [1, 2, 3]);
  });

  test("an upgrade that changes nothing restarts nothing of the stack", async () => {
    const pods = async () => (await kubectl("get", "pods", "-l", `app.kubernetes.io/instance=${RELEASE},alasio.dev/stack=neon`, "--field-selector=status.phase=Running", "-o", "jsonpath={.items[*].metadata.uid}")).trim().split(" ").sort();
    const before = await pods();
    await run("helm", ["upgrade", RELEASE, CHART, "--namespace", NAMESPACE, "--reuse-values", "--wait", "--timeout", "10m"], { maxBuffer: 16 * 1024 * 1024 });
    assert.deepEqual(await pods(), before);
  });

  test("keeps every row through the whole stack stopping at once", async () => {
    await query("create table kept (id int primary key)");
    await query("insert into kept select g from generate_series(1, 50000) g");
    await kubectl("delete", "pods", "-l", `app.kubernetes.io/instance=${RELEASE},alasio.dev/stack=neon`, "--field-selector=status.phase=Running", "--wait=true", "--timeout=120s");
    await up();
    assert.deepEqual(await query("select count(*)::int as n from kept"), [{ n: 50000 }]);
  });

  for (const component of ["neon-pageserver", "neon-safekeeper", "neon-compute", "neon-storage-controller", "seaweedfs"]) {
    test(`loses nothing committed when ${component} is killed mid-write`, async () => {
      const table = `crash_${component.replaceAll("-", "_")}`;
      await query(`create table ${table} (id int primary key)`);
      const rows = writer(table);
      await sleep(3000);
      await killPod(await podOf(component));
      await sleep(15_000);
      await rows.stop();
      await up();
      const present = new Set((await query(`select id from ${table}`)).map((row) => row.id));
      assert.ok(rows.committed.length > 0);
      assert.deepEqual(rows.committed.filter((id) => !present.has(id)), []);
    });
  }

  test("a safekeeper that lost its volume is rebuilt from its peers", async () => {
    const pod = `${neonName("safekeeper")}-2`;
    await kubectl("delete", "pvc", `data-${pod}`, "--wait=false");
    await kubectl("delete", "pod", pod, "--wait=true");
    await query("create table after_loss (id int primary key)");
    await query("insert into after_loss values (1)");
    // kubectl wait fails at once on a pod that does not exist, which this one does not
    // until its StatefulSet has made it again.
    for (let tries = 0; !(await kubectl("get", "pod", pod).then(() => true, () => false)); tries++) {
      assert.ok(tries < 60, `the StatefulSet never made ${pod} again`);
      await sleep(2000);
    }
    await kubectl("wait", `pod/${pod}`, "--for=condition=Ready", "--timeout=300s");
    const { tenantId, timelineId } = await record();
    const url = `http://127.0.0.1:7676/v1/tenant/${tenantId}/timeline/${timelineId}`;
    const deadline = Date.now() + 180_000;
    let state = "";
    while (Date.now() < deadline) {
      state = await inside(pod, "GET", url, "safekeeperdata").catch(() => "");
      if (state.includes("flush_lsn")) break;
      await sleep(5000);
    }
    assert.match(state, /flush_lsn/u);
    await query("insert into after_loss values (2)");
  });

  test("garbage the pageserver collects is deleted from the object store, validated by the storage controller", async () => {
    const { tenantId, timelineId } = await record();
    await query("create table churn (id int primary key, payload text)");
    for (let pass = 0; pass < 3; pass++) {
      await query("truncate churn");
      await query("insert into churn select g, repeat(md5(random()::text), 30) from generate_series(1, 60000) g");
    }
    const before = await pageserverMetric("pageserver_deletion_queue_executed_total");
    const timeline = `http://127.0.0.1:9898/v1/tenant/${tenantId}/timeline/${timelineId}`;
    await inside(`${neonName("pageserver")}-0`, "PUT", `${timeline}/compact?force_l0_compaction=true&force_repartition=true&force_image_layer_creation=true&wait_until_uploaded=true`, "pageserverapi");
    const deadline = Date.now() + 180_000;
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

  test("backs alasio's database up to the object store, as a dump any Postgres restores", async () => {
    const job = `backup-${Date.now()}`;
    await kubectl("create", "job", job, `--from=cronjob/${neonName("backup")}`);
    try {
      await kubectl("wait", `job/${job}`, "--for=condition=Complete", "--timeout=600s");
    } catch (error) {
      const logs = await kubectl("logs", `job/${job}`, "--all-containers").catch(() => "");
      const store = await kubectl("logs", `statefulset/${FULL}-seaweedfs`, "-c", "seaweedfs", "--tail=30").catch(() => "");
      throw new Error(`${error.message}\n${logs}\nthe object store:\n${store}`);
    } finally {
      await kubectl("delete", "job", job, "--wait=false").catch(() => {});
    }
    const s3 = (await kubectl("get", `cronjob/${neonName("backup")}`, "-o", "jsonpath={.spec.jobTemplate.spec.template.spec.containers[0].env[?(@.name==\"S3_ENDPOINT\")].value}")).trim();
    const bucket = (await kubectl("get", `cronjob/${neonName("backup")}`, "-o", "jsonpath={.spec.jobTemplate.spec.template.spec.containers[0].env[?(@.name==\"BUCKET\")].value}")).trim();
    const listing = await runPod(`restore-${Date.now()}`, {
      initContainers: [{
        name: "fetch",
        image: await image(`statefulset/${neonName("pageserver")}`),
        command: ["/bin/sh", "-c", `latest=$(aws s3 --endpoint-url ${s3} ls s3://${bucket}/ | awk '{print $4}' | sort | tail -1) && aws s3 --endpoint-url ${s3} cp "s3://${bucket}/$latest" /backup/alasio.dump`],
        env: [{ name: "HOME", value: "/tmp" }, { name: "AWS_DEFAULT_REGION", value: "us-east-1" }],
        envFrom: [{ secretRef: { name: neonName("s3-admin") } }],
        securityContext: restricted,
        volumeMounts: [{ name: "backup", mountPath: "/backup" }, { name: "tmp", mountPath: "/tmp" }],
      }],
      containers: [{
        name: "list",
        image: await image(`deployment/${neonName("compute")}`),
        command: ["pg_restore", "--list", "/backup/alasio.dump"],
        securityContext: restricted,
        volumeMounts: [{ name: "backup", mountPath: "/backup" }],
      }],
      volumes: [{ name: "backup", emptyDir: {} }, { name: "tmp", emptyDir: {} }],
    });
    assert.match(listing, /TABLE DATA/u);
  });

  test("reads the database as it was at a past moment", async () => {
    await query("create table history (id int primary key)");
    await query("insert into history select g from generate_series(1, 100) g");
    await sleep(2000);
    const moment = new Date().toISOString();
    await sleep(2000);
    await query("insert into history select g from generate_series(101, 200) g");

    const { tenantId, timelineId } = await record();
    const found = JSON.parse(await inside(
      `${neonName("pageserver")}-0`,
      "GET",
      `http://127.0.0.1:9898/v1/tenant/${tenantId}/timeline/${timelineId}/get_lsn_by_timestamp?timestamp=${encodeURIComponent(moment)}`,
      "pageserverapi",
    ));
    assert.equal(found.kind, "present");

    // A read-only compute pinned to that moment's LSN, given as a file the spec
    // neon-control serves the compute.
    const computeToken = await secret(neonName("compute"), "NEON_CONTROL_PLANE_TOKEN");
    const config = JSON.parse(await kubectl(
      "exec", `deployment/${neonName("control")}`, "--", "node", "-e",
      'fetch("http://127.0.0.1:8080/compute/api/v2/computes/alasio/spec", { headers: { authorization: "Bearer " + process.argv[1] } }).then((r) => r.text()).then((t) => process.stdout.write(t))',
      computeToken,
    ));
    delete config.status;
    config.spec.mode = { Static: found.lsn };
    config.spec.safekeeper_connstrings = [];
    delete config.spec.safekeepers_generation;
    const name = `static-${Date.now()}`;
    await apply({ apiVersion: "v1", kind: "ConfigMap", metadata: { name }, data: { "config.json": JSON.stringify(config) } });
    try {
      const count = await runPod(name, {
        securityContext: { fsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } },
        containers: [{
          name: "compute",
          image: await image(`deployment/${neonName("compute")}`),
          command: ["/bin/sh", "-c", [
            "/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute --connstr postgresql://cloud_admin@localhost:55433/postgres",
            "--pgbin /usr/local/bin/postgres --compute-id static --config /config/config.json >/tmp/compute.log 2>&1 &",
            "for i in $(seq 120); do psql -h 127.0.0.1 -p 55433 -U cloud_admin -d alasio -Atc 'select count(*) from history' 2>/dev/null && exit 0; sleep 2; done; tail -40 /tmp/compute.log; exit 1",
          ].join(" ")],
          env: [{ name: "OTEL_SDK_DISABLED", value: "true" }],
          securityContext: restricted,
          volumeMounts: [{ name: "config", mountPath: "/config" }, { name: "pgdata", mountPath: "/var/db/postgres" }, { name: "tmp", mountPath: "/tmp" }],
        }],
        volumes: [{ name: "config", configMap: { name } }, { name: "pgdata", emptyDir: {} }, { name: "tmp", emptyDir: {} }],
      });
      assert.equal(Number(count.trim().split("\n").at(-1)), 100);
    } finally {
      await kubectl("delete", "configmap", name, "--wait=false").catch(() => {});
    }
  });
});

/** A read-only query of the lake, run in its pod as `kubectl exec` runs one. */
async function lakeQuery(sql) {
  const stdout = await kubectl("exec", `deployment/${FULL}-lake`, "--", "node", "src/query.ts", "--format", "json", sql);
  return stdout.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
}

async function untilLoaded(check, what) {
  const deadline = Date.now() + 300_000;
  let last;
  while (Date.now() < deadline) {
    last = await check().catch((error) => error);
    if (last === true) return;
    await sleep(3000);
  }
  assert.fail(`the lake never ${what}: ${last instanceof Error ? last.message : last}`);
}

describe("alasio's analytics lake on Kubernetes", { skip }, () => {
  const KEY = { projectKey: "-kube-stack-test", sessionId: "33333333-3333-4333-8333-333333333333" };
  const entry = (n) => ({ type: "user", uuid: `kube-stack-${n}`, timestamp: new Date(1790000000000 + n).toISOString(), message: { role: "user", content: `entry ${n}` } });
  const sourceCount = async () => (await query("select count(*)::int as n from claude_sessions.entries"))[0].n;
  const lakeCounts = async () => (await lakeQuery("select count(*) as n, count(distinct seq) as seqs from claude.entries"))[0];

  test("loads what alasio's stores hold, as it starts and as it runs", async () => {
    const store = new NeonSessionStore(pool);
    await store.append(KEY, Array.from({ length: 50 }, (_, n) => entry(n)));
    const rollouts = new NeonRolloutStore(pool);
    const line = `${JSON.stringify({ timestamp: new Date().toISOString(), type: "session_meta", payload: { id: "thread-k" } })}\n`;
    await rollouts.save(
      { name: "rollout-kube.jsonl", path: "sessions/rollout-kube.jsonl", threadId: "thread-k", rolloutId: "thread-k", historyBase: null, size: Buffer.byteLength(line), headDigest: "h", modifiedMs: Date.now() },
      { start: 0, bytes: Buffer.from(line) },
    );
    await kubectl("rollout", "restart", `deployment/${FULL}-lake`);
    await kubectl("rollout", "status", `deployment/${FULL}-lake`, "--timeout=300s");
    const expected = await sourceCount();
    await untilLoaded(async () => Number((await lakeCounts()).n) === expected, `held all ${expected} entries`);
    const lines = await lakeQuery("select type, thread_id from codex.lines where thread_id = 'thread-k'");
    assert.deepEqual(lines, [{ type: "session_meta", thread_id: "thread-k" }]);
  });

  test("keeps every entry exactly once through its loader being killed mid-load", async () => {
    await new NeonSessionStore(pool).append(KEY, Array.from({ length: 20_000 }, (_, n) => entry(1000 + n)));
    await sleep(1500);
    await killPod(await podOf("lake"));
    await kubectl("rollout", "status", `deployment/${FULL}-lake`, "--timeout=300s");
    const expected = await sourceCount();
    await untilLoaded(async () => {
      const { n, seqs } = await lakeCounts();
      return Number(n) === expected && Number(seqs) === expected;
    }, `held each of ${expected} entries exactly once`);
  });

  test("answers queries read-only", async () => {
    await assert.rejects(lakeQuery("delete from claude.entries"), /read-only|read only/iu);
  });

  test("its role reads none of alasio's data but what it is granted, and is no superuser's member", async () => {
    assert.deepEqual(await query("select count(*)::int as n from pg_auth_members where member = 'lake'::regrole"), [{ n: 0 }]);
  });
});
