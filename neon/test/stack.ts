/**
 * alasio's Neon as alasio installs it: its components killed, the whole stack stopped at
 * once, a safekeeper's volume lost, with nothing committed lost; a day of its history
 * kept, its garbage collected, its dumps restorable, its past readable, its branches
 * made, run and deleted, and its lake loading; and an upgrade that changes nothing
 * restarting none of it.
 *
 * Part of the end-to-end run (test/e2e/alasio.test.ts), which registers it with
 * neonStack() and has installed alasio before it runs; slow (about fifteen minutes).
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, describe, test } from "node:test";

import type { CoreV1Event, V1ConfigMap, V1CronJob, V1Deployment, V1Job, V1Pod, V1PodSpec, V1StatefulSet } from "@kubernetes/client-node";
import pg, { type QueryResultRow } from "pg";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, neonName, RELEASE } from "../../cli/src/manifests/common.ts";
import { NeonRolloutStore } from "../../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { alasioOk, type Forward, kube, lakeQuery, ref } from "../../test/e2e/harness.ts";
import { sessionStoreConformance } from "../../test/support/session-store-conformance.ts";
import { type BranchesFile, type BranchView, computeId, MAIN, type ReadyBranch, tokenSha256 } from "../control/branches.ts";
import { signToken } from "../control/jwt.ts";
import { scramVerifier } from "../control/scram.ts";
import type { StackSecrets } from "../control/secrets.ts";

/** The stack's pods, by their labels. */
const STACK = selectorOf({ "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon" });

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// Set by the suite's first hook.
let forward: Forward | undefined;
let databaseUrl: string;
let pool: pg.Pool;

/** A query on a connection of its own, retried while the stack comes back. */
async function query<Row extends QueryResultRow = Record<string, unknown>>(
  sql: string,
  params?: unknown[],
  { attempts = 60 }: { readonly attempts?: number } = {},
): Promise<Row[]> {
  for (let attempt = 1; ; attempt++) {
    const client = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 10_000, query_timeout: 300_000 });
    client.on("error", () => {});
    let connected = false;
    try {
      await client.connect();
      connected = true;
      return (await client.query<Row>(sql, params)).rows;
    } catch (error) {
      // Connecting is retried; a query that reached the database is not.
      if (connected || attempt >= attempts) throw error;
      await sleep(2000);
    } finally {
      await client.end().catch(() => {});
    }
  }
}

/** Waits until every workload of the stack is rolled out and ready, and the database answers. */
async function up() {
  const workloads = [...await kube.list<V1Deployment>("Deployment", { namespace: NAMESPACE, labelSelector: STACK }), ...await kube.list<V1StatefulSet>("StatefulSet", { namespace: NAMESPACE, labelSelector: STACK })];
  await kube.awaitReady(workloads.map((workload) => ref(workload.kind === "Deployment" ? "Deployment" : "StatefulSet", workload.metadata?.name ?? "", NAMESPACE)), "10 minutes");
  await query("select 1");
}

/** The stack's pods that run now. */
const runningPods = () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: STACK, fieldSelector: "status.phase=Running" });

/** The name of a pod of the component. */
async function podOf(component: string): Promise<string> {
  const [pod] = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": component }) });
  assert.ok(pod?.metadata?.name, `${component} has no pod`);
  return pod.metadata.name;
}

/** Waits until none of `pods` is there any longer: each gone, or made anew under its name, as a StatefulSet's are. */
async function awaitReplaced(pods: readonly V1Pod[], timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  for (const pod of pods) {
    const name = pod.metadata?.name ?? "";
    while ((await kube.get<V1Pod>(ref("Pod", name, NAMESPACE)))?.metadata?.uid === pod.metadata?.uid) {
      assert.ok(Date.now() < deadline, `${name} was not deleted`);
      await sleep(1000);
    }
  }
}

/** The pod of `name` made anew, once it runs ready. */
async function awaitPodReady(name: string, timeoutMs = 300_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const pod = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
    if (pod?.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True")) return;
    assert.ok(Date.now() < deadline, `${name} is not ready: ${pod ? pod.status?.phase : "it was not made again"}`);
    await sleep(2000);
  }
}

let privateKey: string;
const token = (scope: string) => signToken(privateKey, scope);

/** An HTTP call from inside a pod of the stack, whose image has curl. */
async function inside(pod: string, method: string, url: string, scope: string, body?: unknown) {
  const args = ["curl", "-sS", "-X", method, "-H", `authorization: Bearer ${token(scope)}`];
  if (body !== undefined) args.push("-H", "content-type: application/json", "-d", JSON.stringify(body));
  return await kube.execOk(NAMESPACE, pod, [...args, url]);
}

/** Runs `command` in neon-control's pod: what it printed. */
const inControl = async (command: readonly string[]) => kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, neonName("control")), command);

/** The tenant of alasio's timelines; set by the suite's first hook. */
let tenantId: string;

/** Main's timeline, as neon-control recorded it once the stack was up, and where it is placed. */
async function record(): Promise<{ tenantId: string; timelineId: string; safekeepers: ReadyBranch["safekeepers"] }> {
  const { branches }: BranchesFile = JSON.parse(await inControl(["cat", "/state/branches.json"]));
  const main = branches.find((branch): branch is ReadyBranch => branch.name === MAIN && branch.state === "ready");
  assert.ok(main, "neon-control has recorded main");
  return { tenantId, timelineId: main.timelineId, safekeepers: main.safekeepers };
}

async function pageserverMetric(name: string) {
  const metrics = await inside(`${neonName("pageserver")}-0`, "GET", "http://127.0.0.1:9898/metrics", "pageserverapi");
  const line = metrics.split("\n").find((l) => l.startsWith(`${name} `));
  return Number(line?.split(" ")[1] ?? NaN);
}

/** Writes rows until stopped, counting only those whose commit returned. */
function writer(table: string) {
  const committed: number[] = [];
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

/** What the cluster last said of the object `name`, a line an event. */
async function eventsOf(name: string, last: number): Promise<string> {
  const events = await kube.list<CoreV1Event>("Event", { namespace: NAMESPACE, fieldSelector: `involvedObject.name=${name}` });
  return events.slice(-last).map(({ type, reason, message }) => `${type} ${reason}: ${message?.trim()}`).join("\n");
}

/** What each container of `pod` logged, one after the other; why, for one that logged nothing that could be read. */
async function logsOf(pod: V1Pod): Promise<string> {
  const name = pod.metadata?.name ?? "";
  const containers = [...(pod.spec?.initContainers ?? []), ...(pod.spec?.containers ?? [])];
  const logged = await Promise.all(containers.map((container) => kube.logs(NAMESPACE, name, container.name).catch((error: unknown) => (error instanceof Error ? error.message : String(error)))));
  return logged.join("");
}

/**
 * A pod of the stack's own, labelled so the stack's NetworkPolicy admits it and placed
 * where the stack runs, whose nodes hold its images already, run to completion: its logs.
 * One that fails, or does not finish within `timeoutMs`, fails with its logs and its
 * last events.
 */
async function runPod(name: string, spec: V1PodSpec, { timeoutMs = 300_000 }: { readonly timeoutMs?: number } = {}) {
  const compute = await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE));
  const pod: V1Pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: NAMESPACE, labels: { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon", "app.kubernetes.io/component": "neon-test" } },
    spec: {
      restartPolicy: "Never",
      nodeSelector: compute?.spec?.template.spec?.nodeSelector ?? {},
      securityContext: { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } },
      ...spec,
    },
  };
  await kube.apply(pod);
  try {
    const deadline = Date.now() + timeoutMs;
    let phase = "";
    while (Date.now() < deadline && !["Succeeded", "Failed"].includes(phase)) {
      await sleep(2000);
      phase = (await kube.get<V1Pod>(ref("Pod", name, NAMESPACE)))?.status?.phase ?? "";
    }
    const logs = await logsOf(pod);
    if (phase !== "Succeeded") throw new Error(`pod ${name} ended ${phase || "unstarted"}:\n${logs}\n${await eventsOf(name, 15)}`);
    return logs;
  } finally {
    await kube.remove(ref("Pod", name, NAMESPACE)).catch(() => {});
  }
}

/** The image of the workload's first container. */
const image = (workload: V1Deployment | V1StatefulSet | null) => workload?.spec?.template.spec?.containers[0]?.image ?? "";
const restricted = { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] } };

async function untilLoaded(check: () => Promise<boolean>, what: string) {
  const deadline = Date.now() + 300_000;
  let last: unknown;
  while (Date.now() < deadline) {
    last = await check().catch((error: unknown) => error);
    if (last === true) return;
    await sleep(3000);
  }
  assert.fail(`the lake never ${what}: ${last instanceof Error ? last.message : last}`);
}

/** Restarts the lake, as a rollout restart of its Deployment does, and waits until it runs again. */
async function restartLake() {
  const lake = ref("Deployment", `${RELEASE}-lake`, NAMESPACE);
  await kube.patch(lake, { spec: { template: { metadata: { annotations: { "kubectl.kubernetes.io/restartedAt": new Date().toISOString() } } } } });
  await kube.awaitReady([lake], "5 minutes");
}

/** What the pageserver answers to which LSN a moment was at. */
interface LsnAtTimestamp {
  readonly kind: string;
  readonly lsn: string;
}

/** What the pageserver answers of a tenant's configuration, as far as this test reads it. */
interface TenantConfigAnswered {
  readonly effective_config: { readonly pitr_interval: string };
}

/** What neon-control serves a compute, as far as this test changes it. */
interface ServedComputeConfig {
  status?: unknown;
  spec: {
    mode: unknown;
    safekeeper_connstrings: string[];
    safekeepers_generation?: number;
    [field: string]: unknown;
  };
  [field: string]: unknown;
}

/** A branch as neon-control's API answers it. */
type Listed = BranchView;

/** Registers the suite, which runs once the end-to-end run has installed alasio. */
export function neonStack(): void {
  describe("alasio's Neon, as alasio installs it", () => {
    before(async () => {
      privateKey = await kube.secret(NAMESPACE, neonName("root"), "auth_private_key.pem");
      const secrets: StackSecrets = JSON.parse(await kube.secret(NAMESPACE, neonName("root"), "secrets.json"));
      tenantId = secrets.tenantId;
      forward = await kube.forward(NAMESPACE, neonName("compute"), 55433);
      const url = new URL((await kube.secret(NAMESPACE, `${RELEASE}-database`, "url")).trim());
      url.hostname = "127.0.0.1";
      url.port = String(forward.port);
      databaseUrl = url.toString();
      await up();
      pool = new pg.Pool({ connectionString: databaseUrl, max: 4 });
      pool.on("error", () => {});
    });

    after(async () => {
      await pool?.end().catch(() => {});
      forward?.close();
    });

    let schemas = 0;
    sessionStoreConformance(async () => {
      const store = new NeonSessionStore(pool, { schema: `kube_conformance_${++schemas}` });
      await store.ensureSchema();
      return store;
    });

    describe("the database", () => {
      test("bootstraps its tenant and a timeline on three safekeepers", async () => {
        const { safekeepers } = await record();
        assert.deepEqual([...safekeepers.ids].sort(), [1, 2, 3]);
      });

      test("keeps a day of history, as its tenant is configured", async () => {
        const config: TenantConfigAnswered = JSON.parse(await inside(`${neonName("pageserver")}-0`, "GET", `http://127.0.0.1:9898/v1/tenant/${tenantId}/config`, "pageserverapi"));
        assert.equal(config.effective_config.pitr_interval, "1day");
      });

      test("an upgrade that changes nothing restarts nothing of the stack", async () => {
        const pods = async () => (await runningPods()).map((pod) => pod.metadata?.uid).sort();
        const before = await pods();
        await alasioOk("upgrade", "--timeout", "10m");
        assert.deepEqual(await pods(), before);
      });

      test("keeps every row through the whole stack stopping at once", async () => {
        await query("create table kept (id int primary key)");
        await query("insert into kept select g from generate_series(1, 50000) g");
        const stopped = await runningPods();
        await Promise.all(stopped.map((pod) => kube.remove(ref("Pod", pod.metadata?.name ?? "", NAMESPACE))));
        await awaitReplaced(stopped);
        await up();
        assert.deepEqual(await query("select count(*)::int as n from kept"), [{ n: 50000 }]);
      });

      for (const component of ["neon-pageserver", "neon-safekeeper", "neon-compute", "neon-storage-controller", "seaweedfs"]) {
        test(`loses nothing committed when ${component} is killed mid-write`, async () => {
          const table = `crash_${component.replaceAll("-", "_")}`;
          await query(`create table ${table} (id int primary key)`);
          const rows = writer(table);
          await sleep(3000);
          await kube.kill(NAMESPACE, await podOf(component));
          await sleep(15_000);
          await rows.stop();
          await up();
          const present = new Set((await query<{ id: number }>(`select id from ${table}`)).map((row) => row.id));
          assert.ok(rows.committed.length > 0);
          assert.deepEqual(rows.committed.filter((id) => !present.has(id)), []);
        });
      }

      test("a safekeeper that lost its volume is rebuilt from its peers", async () => {
        const name = `${neonName("safekeeper")}-2`;
        const pod = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
        assert.ok(pod);
        await kube.remove(ref("PersistentVolumeClaim", `data-${name}`, NAMESPACE));
        await kube.remove(ref("Pod", name, NAMESPACE));
        await awaitReplaced([pod]);
        await query("create table after_loss (id int primary key)");
        await query("insert into after_loss values (1)");
        await awaitPodReady(name);
        const { tenantId, timelineId } = await record();
        const url = `http://127.0.0.1:7676/v1/tenant/${tenantId}/timeline/${timelineId}`;
        const deadline = Date.now() + 180_000;
        let state = "";
        while (Date.now() < deadline) {
          state = await inside(name, "GET", url, "safekeeperdata").catch(() => "");
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

      test("the object store's lifecycle pass reads the filer's log of changes to now, not a count of changes", async () => {
        const name = `lifecycle-${Date.now()}`;
        const template = (await kube.get<V1CronJob>(ref("CronJob", componentName("seaweedfs-lifecycle"), NAMESPACE)))?.spec?.jobTemplate;
        assert.ok(template?.spec);
        // A Job of the CronJob's template, as one it starts on its schedule is.
        const job: V1Job = { apiVersion: "batch/v1", kind: "Job", metadata: { name, namespace: NAMESPACE, labels: template.metadata?.labels ?? {} }, spec: template.spec };
        await kube.apply(job);
        try {
          await kube.awaitReady([ref("Job", name, NAMESPACE)], "10 minutes");
          const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "job-name": name }) });
          const logs = (await Promise.all(pods.map(logsOf))).join("");
          assert.match(logs, /running shards 0-15 \(event budget=0, /u);
          assert.match(logs, /shards 0-15 complete; cursors checkpointed/u);
        } finally {
          await kube.remove(ref("Job", name, NAMESPACE)).catch(() => {});
        }
      });

      test("backs alasio's database up to the object store, as a dump any Postgres restores", async () => {
        const name = `backup-${Date.now()}`;
        const backup = await kube.get<V1CronJob>(ref("CronJob", neonName("backup"), NAMESPACE));
        const template = backup?.spec?.jobTemplate;
        assert.ok(template?.spec);
        // A Job of the CronJob's template, as one it starts on its schedule is.
        const job: V1Job = { apiVersion: "batch/v1", kind: "Job", metadata: { name, namespace: NAMESPACE, labels: template.metadata?.labels ?? {} }, spec: template.spec };
        await kube.apply(job);
        try {
          await kube.awaitReady([ref("Job", name, NAMESPACE)], "10 minutes");
        } catch (error) {
          const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "job-name": name }) });
          const logs = (await Promise.all(pods.map(logsOf))).join("");
          const store = (await kube.logs(NAMESPACE, `${RELEASE}-seaweedfs-0`, "seaweedfs").catch(() => "")).trimEnd().split("\n").slice(-30).join("\n");
          throw new Error(`${error instanceof Error ? error.message : String(error)}\n${logs}\nthe object store:\n${store}`);
        } finally {
          await kube.remove(ref("Job", name, NAMESPACE)).catch(() => {});
        }
        const upload = template.spec.template.spec?.containers[0]?.env ?? [];
        const s3 = upload.find((variable) => variable.name === "S3_ENDPOINT")?.value;
        const bucket = upload.find((variable) => variable.name === "BUCKET")?.value;
        const listing = await runPod(`restore-${Date.now()}`, {
          initContainers: [{
            name: "fetch",
            image: image(await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("pageserver"), NAMESPACE))),
            // The newest of the database's dumps, which share the bucket with JuiceFS's.
            command: [
              "/bin/sh",
              "-c",
              `latest=$(aws s3 --endpoint-url ${s3} ls s3://${bucket}/ | awk '{print $4}' | grep '^alasio-.*\\.dump$' | sort | tail -1) && aws s3 --endpoint-url ${s3} cp "s3://${bucket}/$latest" /backup/alasio.dump`,
            ],
            env: [{ name: "HOME", value: "/tmp" }, { name: "AWS_DEFAULT_REGION", value: "us-east-1" }],
            envFrom: [{ secretRef: { name: neonName("s3-admin") } }],
            securityContext: restricted,
            volumeMounts: [{ name: "backup", mountPath: "/backup" }, { name: "tmp", mountPath: "/tmp" }],
          }],
          containers: [{
            name: "list",
            image: image(await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE))),
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
        const found: LsnAtTimestamp = JSON.parse(await inside(
          `${neonName("pageserver")}-0`,
          "GET",
          `http://127.0.0.1:9898/v1/tenant/${tenantId}/timeline/${timelineId}/get_lsn_by_timestamp?timestamp=${encodeURIComponent(moment)}`,
          "pageserverapi",
        ));
        assert.equal(found.kind, "present");

        // A read-only compute pinned to that moment's LSN, given as a file the spec
        // neon-control serves the compute.
        const computeToken = await kube.secret(NAMESPACE, neonName("compute"), "NEON_CONTROL_PLANE_TOKEN");
        const config: ServedComputeConfig = JSON.parse(await inControl([
          "node",
          "-e",
          'fetch("http://127.0.0.1:8080/compute/api/v2/computes/alasio/spec", { headers: { authorization: "Bearer " + process.argv[1] } }).then((r) => r.text()).then((t) => process.stdout.write(t))',
          // The token is random, and one starting with "-" would be read as an option of node's.
          "--",
          computeToken,
        ]));
        delete config.status;
        config.spec.mode = { Static: found.lsn };
        config.spec.safekeeper_connstrings = [];
        delete config.spec.safekeepers_generation;
        const name = `static-${Date.now()}`;
        const configMap: V1ConfigMap = { apiVersion: "v1", kind: "ConfigMap", metadata: { name, namespace: NAMESPACE }, data: { "config.json": JSON.stringify(config) } };
        await kube.apply(configMap);
        try {
          const count = await runPod(name, {
            securityContext: { fsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } },
            containers: [{
              name: "compute",
              image: image(await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE))),
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
          await kube.remove(ref("ConfigMap", name, NAMESPACE)).catch(() => {});
        }
      });
    });

    // After the database's tests: the whole stack stopping at once would stop the branch's compute too.
    describe("branches", () => {
      const BRANCH = "e2e";
      /** The pod of the branch's compute, which runs as main's does, but on the branch. */
      const COMPUTE = `${computeId(BRANCH)}-compute`;
      /** Its compute's own credentials: its role's password, and its token, which neon-control is given the hashes of. */
      const PASSWORD = `branch-${randomUUID()}`;
      const COMPUTE_TOKEN = `compute-${randomUUID()}`;
      const compute = { passwordVerifier: scramVerifier(PASSWORD), tokenSha256: tokenSha256(COMPUTE_TOKEN) };
      let control: Forward | undefined;

      /** neon-control's API, called with `bearer`, a token of the admin scope unless given: its status, and what it answered. */
      async function api<Body = unknown>(method: string, path: string, { body, bearer = token("admin") }: { readonly body?: unknown; readonly bearer?: string } = {}) {
        assert.ok(control);
        const response = await fetch(`${control.base}${path}`, {
          method,
          headers: { authorization: `Bearer ${bearer}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        });
        const text = await response.text();
        const answered: Body = text ? JSON.parse(text) : null;
        return { status: response.status, body: answered };
      }

      /** The branches neon-control answers, main first. */
      const listed = async () => (await api<{ branches: Listed[] }>("GET", "/branches")).body.branches;

      /** Runs `sql` on the branch's compute, from its pod: what psql printed. */
      const onBranch = async (sql: string) => (await kube.execOk(NAMESPACE, COMPUTE, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", "alasio", "-Atc", sql])).trim();

      before(async () => {
        control = await kube.forward(NAMESPACE, neonName("control"), 8080);
      });

      after(async () => {
        control?.close();
        await kube.remove(ref("Pod", COMPUTE, NAMESPACE)).catch(() => {});
      });

      test("are managed with a token of the admin scope alone, not with the tenant's every compute holds", async () => {
        const tenant = signToken(privateKey, "tenant", tenantId);
        for (const [method, path, body] of [["GET", "/branches"], ["POST", "/branches", { name: BRANCH, compute }], ["DELETE", `/branches/${BRANCH}`], ["PUT", "/notify-attach", {}]] as const) {
          assert.equal((await api(method, path, { body, bearer: tenant })).status, 401, `${method} ${path}`);
        }
        assert.deepEqual((await listed()).map(({ name, state, computeId }) => ({ name, state, computeId })), [{ name: MAIN, state: "ready", computeId: "alasio" }]);
      });

      test("a branch's compute reads its parent as of its branch point, and writes on it alone", async () => {
        await query("create table branched (id int primary key)");
        await query("insert into branched select g from generate_series(1, 100) g");
        const [point] = await query<{ lsn: string }>("select pg_current_wal_lsn()::text as lsn");
        assert.ok(point);
        await query("insert into branched select g from generate_series(101, 200) g");

        const created = await api<Listed>("POST", "/branches", { body: { name: BRANCH, lsn: point.lsn, compute } });
        assert.equal(created.status, 201, JSON.stringify(created.body));
        assert.equal(created.body.parent, MAIN);
        assert.equal(created.body.computeId, `branch-${BRANCH}`);
        assert.ok(created.body.state === "ready");
        assert.deepEqual([...created.body.safekeepers.ids].sort(), [1, 2, 3]);
        const again = await api<Listed>("POST", "/branches", { body: { name: BRANCH, compute } });
        assert.equal(again.status, 200);
        assert.equal(again.body.timelineId, created.body.timelineId);

        // Main's compute's pod, under labels of its own, with the branch's compute id and its token.
        const template = (await kube.get<V1Deployment>(ref("Deployment", neonName("compute"), NAMESPACE)))?.spec?.template.spec;
        const [container] = template?.containers ?? [];
        assert.ok(template && container?.args);
        const pod: V1Pod = {
          apiVersion: "v1",
          kind: "Pod",
          metadata: { name: COMPUTE, namespace: NAMESPACE, labels: { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon", "app.kubernetes.io/component": "neon-test" } },
          spec: {
            ...template,
            containers: [{
              ...container,
              args: container.args.map((arg, at, args) => (args[at - 1] === "--compute-id" ? computeId(BRANCH) : arg)),
              env: (container.env ?? []).map((variable) => (variable.name === "NEON_CONTROL_PLANE_TOKEN" ? { name: variable.name, value: COMPUTE_TOKEN } : variable)),
            }],
          },
        };
        await kube.apply(pod);
        await awaitPodReady(COMPUTE);
        assert.equal(await onBranch("select count(*) from branched"), "100");
        await onBranch("insert into branched select g from generate_series(1001, 1050) g");
        assert.equal(await onBranch("select count(*) from branched"), "150");
        assert.deepEqual(await query("select count(*)::int as n, max(id) as top from branched"), [{ n: 200, top: 200 }]);
      });

      test("a branch's compute is given its own spec alone, with its own token, and its role takes its own password, not main's", async () => {
        const mainToken = await kube.secret(NAMESPACE, neonName("compute"), "NEON_CONTROL_PLANE_TOKEN");
        const specStatus = async (id: string, bearer: string) => (await inControl([
          "node",
          "-e",
          'fetch("http://127.0.0.1:8080/compute/api/v2/computes/" + process.argv[1] + "/spec", { headers: { authorization: "Bearer " + process.argv[2] } }).then((r) => process.stdout.write(String(r.status)))',
          "--",
          id,
          bearer,
        ])).trim();
        assert.deepEqual(
          [await specStatus(computeId(BRANCH), COMPUTE_TOKEN), await specStatus(computeId(MAIN), COMPUTE_TOKEN), await specStatus(computeId(BRANCH), mainToken), await specStatus(computeId(BRANCH), "none")],
          ["200", "403", "403", "401"],
        );
        const mainPassword = (JSON.parse(await kube.secret(NAMESPACE, neonName("root"), "secrets.json")) as StackSecrets).alasioPassword;
        const loginWith = async (password: string) => (await kube.execOk(NAMESPACE, COMPUTE, [
          "sh",
          "-c",
          // By the pod's address, as alasio connects, not the loopback compute_ctl trusts.
          'PGPASSWORD="$1" psql -h "$(hostname -i | cut -d" " -f1)" -p 55433 -U alasio -d alasio -Atc "select current_user" 2>/dev/null || echo refused',
          "sh",
          password,
        ])).trim();
        assert.deepEqual([await loginWith(PASSWORD), await loginWith(mainPassword)], ["alasio", "refused"]);
      });

      test("branch from branches, at a point their parent still has, and are deleted before their parent", async () => {
        const child = await api<Listed>("POST", "/branches", { body: { name: `${BRANCH}-child`, parent: BRANCH, compute } });
        assert.equal(child.status, 201, JSON.stringify(child.body));
        assert.equal(child.body.parent, BRANCH);
        assert.equal((await api("DELETE", `/branches/${BRANCH}`)).status, 412);
        assert.equal((await api("DELETE", `/branches/${MAIN}`)).status, 400);
        // Below the start of main's history, which no timeline has.
        const early = await api("POST", "/branches", { body: { name: `${BRANCH}-early`, lsn: "0/8", compute } });
        assert.equal(early.status, 406, JSON.stringify(early.body));
        assert.equal((await api("POST", "/branches", { body: { name: "Early" } })).status, 400);
        assert.equal((await api("DELETE", `/branches/${BRANCH}-child`)).status, 204);
        assert.deepEqual((await listed()).map(({ name }) => name), [MAIN, BRANCH]);
      });

      test("a branch deleted once its compute has stopped is gone from the storage controller and the safekeepers", async () => {
        const branch = (await listed()).find(({ name }) => name === BRANCH);
        assert.ok(branch);
        const compute = await kube.get<V1Pod>(ref("Pod", COMPUTE, NAMESPACE));
        assert.ok(compute);
        await kube.remove(ref("Pod", COMPUTE, NAMESPACE));
        await awaitReplaced([compute]);
        // A 409 says the storage controller is deleting it still, and it is asked again.
        const deadline = Date.now() + 180_000;
        let deleted = await api("DELETE", `/branches/${BRANCH}`);
        while (deleted.status === 409 && Date.now() < deadline) deleted = await api("DELETE", `/branches/${BRANCH}`);
        assert.equal(deleted.status, 204, JSON.stringify(deleted.body));
        assert.deepEqual((await listed()).map(({ name }) => name), [MAIN]);

        const controller = await podOf("neon-storage-controller");
        const timeline = `http://127.0.0.1:1234/v1/tenant/${tenantId}/timeline/${branch.timelineId}`;
        const found = await kube.execOk(NAMESPACE, controller, ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "-H", `authorization: Bearer ${token("admin")}`, timeline]);
        assert.equal(found, "404");
        const replicas = (await kube.get<V1StatefulSet>(ref("StatefulSet", neonName("safekeeper"), NAMESPACE)))?.spec?.replicas ?? 0;
        for (let index = 0; index < replicas; index++) {
          // Each keeps its timelines in /data/<tenant>/<timeline>.
          const held = () => kube.execOk(NAMESPACE, `${neonName("safekeeper")}-${index}`, ["ls", `/data/${tenantId}`]);
          while ((await held()).split("\n").includes(branch.timelineId)) {
            assert.ok(Date.now() < deadline, `safekeeper ${index + 1} keeps the deleted branch's timeline`);
            await sleep(3000);
          }
        }
      });
    });

    describe("the analytics lake", () => {
      const KEY = { projectKey: "-kube-stack-test", sessionId: "33333333-3333-4333-8333-333333333333" };
      const entry = (n: number) => ({ type: "user", uuid: `kube-stack-${n}`, timestamp: new Date(1790000000000 + n).toISOString(), message: { role: "user", content: `entry ${n}` } });
      const sourceCount = async () => {
        const [counted] = await query<{ n: number }>("select count(*)::int as n from claude_sessions.entries");
        assert.ok(counted);
        return counted.n;
      };
      const lakeCounts = async () => {
        const [counts] = await lakeQuery("select count(*) as n, count(distinct seq) as seqs from claude.entries");
        assert.ok(counts);
        return counts;
      };

      test("loads what alasio's stores hold, as it starts and as it runs", async () => {
        const store = new NeonSessionStore(pool);
        await store.append(KEY, Array.from({ length: 50 }, (_, n) => entry(n)));
        const rollouts = new NeonRolloutStore(pool);
        const line = `${JSON.stringify({ timestamp: new Date().toISOString(), type: "session_meta", payload: { id: "thread-k" } })}\n`;
        await rollouts.save(
          { name: "rollout-kube.jsonl", path: "sessions/rollout-kube.jsonl", threadId: "thread-k", rolloutId: "thread-k", historyBase: null, size: Buffer.byteLength(line), headDigest: "h", modifiedMs: Date.now() },
          { start: 0, bytes: Buffer.from(line) },
        );
        await restartLake();
        const expected = await sourceCount();
        await untilLoaded(async () => Number((await lakeCounts())["n"]) === expected, `held all ${expected} entries`);
        const lines = await lakeQuery("select type, thread_id from codex.lines where thread_id = 'thread-k'");
        assert.deepEqual(lines, [{ type: "session_meta", thread_id: "thread-k" }]);
      });

      test("keeps every entry exactly once through its loader being killed mid-load", async () => {
        await new NeonSessionStore(pool).append(KEY, Array.from({ length: 20_000 }, (_, n) => entry(1000 + n)));
        await sleep(1500);
        await kube.kill(NAMESPACE, await podOf("lake"));
        await kube.awaitReady([ref("Deployment", `${RELEASE}-lake`, NAMESPACE)], "5 minutes");
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
  });
}
