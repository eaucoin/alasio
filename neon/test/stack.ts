/**
 * alasio's Neon as alasio installs it: its components killed, the whole stack stopped at
 * once, a safekeeper's volume lost, with nothing committed lost; a day of its history
 * kept, its garbage collected, its dumps restorable, its past readable, its branches
 * made, run and deleted, its compute reached through Neon's proxy alone and woken by
 * it, and its lake loading through main's read replica; on a cluster of three nodes, its
 * tenant split across two pageservers, drained and filled under a running compute, which
 * one starting on shards placed before is restarted from; and an upgrade that changes
 * nothing restarting none of it.
 *
 * Part of the end-to-end run (test/e2e/alasio.test.ts), which registers it with
 * neonStack() and has installed alasio before it runs; slow (about fifteen minutes).
 */
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { after, before, describe, test } from "node:test";

import type { CoreV1Event, V1ConfigMap, V1CronJob, V1Deployment, V1Job, V1Node, V1Pod, V1PodSpec, V1PriorityClass, V1StatefulSet } from "@kubernetes/client-node";
import pg, { type QueryResultRow } from "pg";

import { cpuMillis } from "../../cli/src/capacity.ts";
import type { ObjectRef } from "../../cli/src/kube/api.ts";
import { dataNodes } from "../../cli/src/kube/object-store-scale.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { componentName, NAMESPACE, neonName, RELEASE } from "../../cli/src/manifests/common.ts";
import { pageserverName, REPLICA_NAME } from "../../cli/src/manifests/neon.ts";
import { NeonRolloutStore } from "../../src/codex/rollouts/store.ts";
import { NeonSessionStore } from "../../src/harness/claude/session-store.ts";
import { AGENTS, alasioOk, type Forward, kube, lakeQuery, paths, ref, tcp } from "../../test/e2e/harness.ts";
import { sessionStoreConformance } from "../../test/support/session-store-conformance.ts";
import { type Branch, type BranchView, COMPUTE_PORT, computeId, MAIN, type ReadyBranch, replicaId, tokenSha256 } from "../control/branches.ts";
import { SUSPEND_AFTER } from "../control/endpoints.ts";
import { computeCtlToken, signToken } from "../control/jwt.ts";
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

/** The object store's gateways' pods that run now. */
async function gateways(): Promise<string[]> {
  const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "seaweedfs" }) });
  return pods.filter((pod) => pod.status?.phase === "Running" && !pod.metadata?.deletionTimestamp).map((pod) => pod.metadata?.name ?? "");
}

/**
 * The tests' own S3 identity, admin of every bucket, which no part of alasio is: each of
 * the stack's identities reaches its own buckets alone, and the tests make buckets of their
 * own. It is added as an operator would add one, through weed shell, to the filer's
 * identities, which the gateways take beside those of their file, and removed after.
 */
const E2E_S3 = { user: "e2e", accessKey: `e2e${randomUUID().replaceAll("-", "").slice(0, 16)}`, secretKey: randomUUID() };

/** A shell's prelude that makes its aws commands the tests' own identity's. */
const AS_E2E = `export AWS_ACCESS_KEY_ID=${E2E_S3.accessKey} AWS_SECRET_ACCESS_KEY=${E2E_S3.secretKey}`;

/** What weed shell answers `command`, run in a gateway's buckets container, as alasio makes its buckets there. */
async function weedShell(command: string): Promise<string> {
  const gateway = (await gateways())[0] ?? assert.fail("no gateway runs");
  const shell = `printf '%s\\n' "$1" | weed shell -master=${componentName("seaweedfs-master")}:9333 -filer=${componentName("seaweedfs-filer")}:8888`;
  return kube.execOk(NAMESPACE, gateway, ["/bin/sh", "-c", shell, "sh", command], { container: "buckets" });
}

/** Adds the tests' own S3 identity, and waits until every gateway takes it. */
async function addE2eIdentity(): Promise<void> {
  await weedShell(`s3.configure -user=${E2E_S3.user} -actions=Admin -access_key=${E2E_S3.accessKey} -secret_key=${E2E_S3.secretKey} -apply`);
  for (const gateway of await gateways()) {
    const deadline = Date.now() + 120_000;
    while (!(await kube.execOk(NAMESPACE, gateway, ["/bin/sh", "-c", `${AS_E2E}; aws s3api --endpoint-url http://127.0.0.1:8333 list-buckets >/dev/null 2>&1 && echo taken || echo not`], { container: "lifecycle" })).includes("taken")) {
      assert.ok(Date.now() < deadline, `the gateway ${gateway} did not take the tests' own S3 identity`);
      await sleep(2000);
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

/** An HTTP call from inside a pod of the stack, as `inside` makes it, with `bearer`: its status, and what it answered. */
async function answeredInside(pod: string, method: string, url: string, bearer: string, body?: unknown): Promise<{ status: number; text: string }> {
  const args = ["curl", "-sS", "-w", "\n%{http_code}", "-X", method, "-H", `authorization: Bearer ${bearer}`];
  if (body !== undefined) args.push("-H", "content-type: application/json", "-d", JSON.stringify(body));
  const said = await kube.execOk(NAMESPACE, pod, [...args, url]);
  const end = said.lastIndexOf("\n");
  return { status: Number(said.slice(end + 1)), text: said.slice(0, end) };
}

/** Runs `command` in neon-control's pod: what it printed. */
const inControl = async (command: readonly string[]) => kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, neonName("control")), command);

/** The spec neon-control's replica `pod` serves main's compute now, fetched with main's compute's token, as compute_ctl fetches it. */
async function servedSpec(pod: string): Promise<ServedComputeConfig> {
  const computeToken = await kube.secret(NAMESPACE, neonName("compute"), "NEON_CONTROL_PLANE_TOKEN");
  return JSON.parse(await kube.execOk(NAMESPACE, pod, [
    "node",
    "-e",
    'fetch("http://127.0.0.1:8080/compute/api/v2/computes/alasio/spec", { headers: { authorization: "Bearer " + process.argv[1] } }).then((r) => r.text()).then((t) => process.stdout.write(t))',
    // The token is random, and one starting with "-" would be read as an option of node's.
    "--",
    computeToken,
  ]));
}

/**
 * Asks the API server to merge-patch the object at `path` with `patch`, as neon-control's
 * ServiceAccount, from its pod, as a dry run, which admission sees whole and which changes
 * nothing: its status, and what it answered.
 */
async function patchedAsNeonControl(path: string, patch: unknown): Promise<{ status: number; text: string }> {
  const said = await kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, neonName("control")), [
    "sh",
    "-c",
    'cd /var/run/secrets/kubernetes.io/serviceaccount && curl -sS -w "\n%{http_code}" --cacert ca.crt -H "Authorization: Bearer $(cat token)" ' +
    '-H "Content-Type: application/merge-patch+json" -X PATCH --data-binary @- "https://kubernetes.default.svc$0?dryRun=All"',
    path,
  ], { stdin: JSON.stringify(patch) });
  const end = said.lastIndexOf("\n");
  return { status: Number(said.slice(end + 1)), text: said.slice(0, end) };
}

/** The tenant of alasio's timelines; set by the suite's first hook. */
let tenantId: string;

/** The branches as neon-control recorded them, in the storage controller's Postgres, read there. */
async function recorded(): Promise<Branch[]> {
  const pod = `${neonName("controller-db")}-0`;
  const rows = await kube.execOk(NAMESPACE, pod, ["psql", "-U", "storage_controller", "-d", "storage_controller", "-Atc", "select branch from neon_control.branches"], { container: "postgres" });
  return rows.trim().split("\n").filter(Boolean).map((row) => JSON.parse(row) as Branch);
}

/** Main's timeline, as neon-control recorded it once the stack was up, and where it is placed. */
async function record(): Promise<{ tenantId: string; timelineId: string; safekeepers: ReadyBranch["safekeepers"] }> {
  const main = (await recorded()).find((branch): branch is ReadyBranch => branch.name === MAIN && branch.state === "ready");
  assert.ok(main, "neon-control has recorded main");
  return { tenantId, timelineId: main.timelineId, safekeepers: main.safekeepers };
}

/** A shard of the tenant: its id, as its pageserver names it, the tenant's own while it is of one shard, and the pod of the pageserver it is attached to. */
interface Shard {
  readonly id: string;
  readonly node: number;
  readonly pod: string;
}

/** An answer of the storage controller's API to `method` of `path`, asked from its pod with a token of the admin scope. */
async function controller<Body>(method: string, path: string, body?: unknown): Promise<Body> {
  const said = await inside(await podOf("neon-storage-controller"), method, `http://127.0.0.1:1234${path}`, "admin", body);
  return (said ? JSON.parse(said) : null) as Body;
}

/** The tenant's shards, as the storage controller locates them, in shard order: shard zero first, which answers for the tenant's timelines. */
async function shards(): Promise<Shard[]> {
  const located = await controller<{ shards: { shard_id: string; node_id: number }[] }>("GET", `/debug/v1/tenant/${tenantId}/locate`);
  return located.shards.map(({ shard_id, node_id }) => ({ id: shard_id, node: node_id, pod: `${neonName("pageserver")}-${node_id - 1}` }));
}

/** Shard zero of the tenant. */
async function shardZero(): Promise<Shard> {
  const [zero] = await shards();
  assert.ok(zero, "the tenant has no shard");
  return zero;
}

async function pageserverMetric(name: string, pod: string) {
  const metrics = await inside(pod, "GET", "http://127.0.0.1:9898/metrics", "pageserverapi");
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

/** The CPU a pod of `spec` requests, in millicores, as the scheduler counts it: its containers', or an init container's where more, and its runtime's overhead. */
function cpuRequested({ containers, initContainers = [], overhead }: V1PodSpec): number {
  const of = (each: readonly { readonly resources?: { readonly requests?: Record<string, string> } }[]) => each.map(({ resources }) => cpuMillis(resources?.requests?.["cpu"] ?? "0"));
  return Math.max(of(containers).reduce((sum, millis) => sum + millis, 0), ...of(initContainers)) + cpuMillis(overhead?.["cpu"] ?? "0");
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

/** A volume of the object store's, as its master's /vol/status answers it, as far as this test reads it. */
interface Volume {
  readonly Id: number;
  readonly Collection: string;
  readonly CompactRevision: number;
  readonly DeletedByteCount: number;
}

/** What the object store's master answers at /vol/status: its volumes by data center, rack and server. */
interface VolumeStatus {
  readonly Volumes: { readonly DataCenters: Record<string, Record<string, Record<string, Volume[]>>> };
}

/** A branch as neon-control's API answers it. */
type Listed = BranchView;

/**
 * A test that, when it fails, says at once what neon-control, the storage controller and
 * main's compute last logged, as the tests after it may take long to fail in turn.
 */
function explainedTest(name: string, body: () => Promise<void>): void {
  test(name, async () => {
    try {
      await body();
    } catch (error) {
      const logged = async (component: string, container: string) => {
        const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": component }) });
        return Promise.all(pods.map(async ({ metadata }) => {
          const lines = (await kube.logs(NAMESPACE, metadata?.name ?? "", container).catch(String)).trimEnd().split("\n").slice(-60);
          return `--- ${metadata?.name} ${container}:\n${lines.join("\n")}`;
        }));
      };
      const said = await Promise.all([logged("neon-control", "neon-control"), logged("neon-storage-controller", "storage-controller"), logged("neon-compute", "compute")]);
      console.error(`# ${name} failed: ${error instanceof Error ? error.message : String(error)}\n${said.flat().join("\n")}`);
      throw error;
    }
  });
}

/** Registers the suite, which runs once the end-to-end run has installed alasio. */
export function neonStack(): void {
  describe("alasio's Neon, as alasio installs it", () => {
    before(async () => {
      privateKey = await kube.secret(NAMESPACE, neonName("root"), "auth_private_key.pem");
      const secrets: StackSecrets = JSON.parse(await kube.secret(NAMESPACE, neonName("root"), "secrets.json"));
      tenantId = secrets.tenantId;
      // Through Neon's proxy, as every client of the compute connects.
      forward = await kube.forward(NAMESPACE, neonName("proxy"), 5432);
      const url = new URL((await kube.secret(NAMESPACE, `${RELEASE}-database`, "url")).trim());
      url.hostname = "127.0.0.1";
      url.port = String(forward.port);
      databaseUrl = url.toString();
      await up();
      await addE2eIdentity();
      pool = new pg.Pool({ connectionString: databaseUrl, max: 4 });
      pool.on("error", () => {});
    });

    after(async () => {
      await pool?.end().catch(() => {});
      forward?.close();
      await weedShell(`s3.configure -user=${E2E_S3.user} -delete -apply`).catch(() => {});
    });

    let schemas = 0;
    sessionStoreConformance(async () => {
      const store = new NeonSessionStore(pool, { schema: `kube_conformance_${++schemas}` });
      await store.ensureSchema();
      return store;
    });

    // Before the database's tests, which then run on the tenant split.
    describe("its storage, scaled out", { skip: AGENTS < 2 && "one pageserver, where the cluster is of one node" }, () => {
      /** A pageserver as the storage controller describes it. */
      interface Node {
        readonly id: number;
        readonly availability: string;
        readonly scheduling: string;
        readonly listen_pg_addr: string;
      }

      /** Runs `sql` on main's compute as its superuser, from its pod: what psql printed. */
      const onCompute = async (sql: string) =>
        (await kube.execOk(NAMESPACE, await kube.runningPod(NAMESPACE, neonName("compute")), ["psql", "-h", "127.0.0.1", "-p", String(COMPUTE_PORT), "-U", "cloud_admin", "-d", "postgres", "-Atc", sql], { container: "compute" })).trim();

      /** The Services of the pageservers main's compute reads from now, one a shard, in shard order. */
      const computeReads = async () => (await onCompute("show neon.pageserver_connstring")).split(",").map((connstring) => new URL(connstring).hostname.split(".")[0]);

      /** The Services of the pageservers the shards are attached to, in shard order. */
      const attachedTo = async () => (await shards()).map(({ node }) => pageserverName(node - 1));

      /** Main's compute's pod, and how often its containers restarted, which a new layout must not change. */
      const computePod = async () => {
        const pod = await kube.get<V1Pod>(ref("Pod", await kube.runningPod(NAMESPACE, neonName("compute")), NAMESPACE));
        return { uid: pod?.metadata?.uid, restarts: pod?.status?.containerStatuses?.map(({ restartCount }) => restartCount) };
      };

      /** Waits until `check` holds, every two seconds for `timeoutMs`, three minutes unless given, saying `what` did not. */
      async function until(what: string, check: () => Promise<boolean>, timeoutMs = 180_000) {
        const deadline = Date.now() + timeoutMs;
        while (!(await check())) {
          assert.ok(Date.now() < deadline, what);
          await sleep(2000);
        }
      }

      /**
       * Waits until the secondary of each of `moving` on pageserver `node` is warm, having its
       * shard's heatmap and every layer in it: a drain or a fill moves a shard only to a
       * secondary whose lag it knows, and leaves the others where they are.
       */
      async function warmed(moving: readonly Shard[], node: number) {
        await until(`the secondaries on pageserver ${node} did not warm`, async () => {
          await controller("POST", `/v1/tenant/${tenantId}/secondary/download?wait_ms=10000`);
          const statuses = await Promise.all(moving.map(async ({ id }) =>
            JSON.parse(await inside(`${neonName("pageserver")}-${node - 1}`, "GET", `http://127.0.0.1:9898/v1/tenant/${id}/secondary/status`, "pageserverapi")) as {
              heatmap_mtime?: unknown;
              bytes_total?: unknown;
              bytes_downloaded?: unknown;
            }
          ));
          return statuses.every(({ heatmap_mtime, bytes_total, bytes_downloaded }) => Boolean(heatmap_mtime) && typeof bytes_total === "number" && bytes_downloaded === bytes_total);
        }, 300_000);
      }

      /** Brings alasio up with `neon.shards` `shards`, and `controls` replicas of neon-control. */
      async function upWith(shards: number, controls: number) {
        const { configFile } = paths();
        const config = JSON.parse(readFileSync(configFile, "utf8"));
        config.install.neon.shards = shards;
        config.install.neon.control = { ...config.install.neon.control, scaling: { min: controls, max: controls } };
        writeFileSync(configFile, JSON.stringify(config));
        return await alasioOk("up", "--timeout", "20m");
      }

      /** neon-control's replicas, by their pods' names. */
      const controls = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-control" }), fieldSelector: "status.phase=Running" }))
          .filter((pod) => pod.metadata?.deletionTimestamp === undefined)
          .map((pod) => pod.metadata?.name ?? "");

      /** The pageservers' Services in the pageserver connstring of `config`, one a shard, in shard order. */
      const readsOf = (config: ServedComputeConfig) => String(config.spec["pageserver_connstring"]).split(",").map((connstring) => new URL(connstring).hostname.split(".")[0]);

      // As the database's tests after expect it: one replica of neon-control, on the split tenant.
      after(async () => {
        await upWith(2, 1);
      });

      explainedTest("registers each pageserver as its ordinal plus one, at its own Service's name", async () => {
        const nodes = await controller<Node[]>("GET", "/control/v1/node");
        assert.deepEqual(
          nodes.map(({ id, availability, listen_pg_addr }) => ({ id, availability, listen_pg_addr })).sort((a, b) => a.id - b.id),
          [{ id: 1, availability: "Active", listen_pg_addr: pageserverName(0) }, { id: 2, availability: "Active", listen_pg_addr: pageserverName(1) }],
        );
      });

      explainedTest("splits main's tenant as neon.shards is raised, under a client's writes, losing nothing, its compute reading the shards where they are without a restart, and every replica of neon-control serving them", async () => {
        assert.equal((await shards()).length, 1);
        const before = await computePod();
        await query("create table split (id int primary key)");
        const rows = writer("split");
        try {
          await upWith(2, 2);
          // By the replica of neon-control that leads, at its next round.
          await until("the tenant was not split", async () => (await shards()).length === 2);
        } finally {
          await rows.stop();
        }
        assert.deepEqual((await shards()).map(({ id }) => id), [`${tenantId}-0002`, `${tenantId}-0102`]);
        // The split's shards start where the tenant was; neon-control has one moved to the
        // other pageserver, once its secondary there is warm, each keeping a secondary on the other.
        await until("the shards were not placed across both pageservers, each with a secondary on the other", async () => {
          const described = await controller<{ shards: { node_attached: number | null; node_secondary: number[] }[] }>("GET", `/control/v1/tenant/${tenantId}`);
          return new Set(described.shards.map(({ node_attached }) => node_attached)).size === 2 &&
            described.shards.every(({ node_attached, node_secondary }) => node_secondary.length === 1 && node_secondary[0] !== node_attached);
        }, 600_000);
        // neon-control gave the running compute the new layout, and every replica of it, whichever
        // the storage controller notified, serves a compute starting now the same.
        await until("the compute does not read the shards where they are", async () => (await computeReads()).join() === (await attachedTo()).join());
        const replicas = await controls();
        assert.equal(replicas.length, 2);
        for (const pod of replicas) assert.deepEqual(readsOf(await servedSpec(pod)), await attachedTo(), pod);
        // In pages, as the storage controller split it.
        const { shard_params } = await controller<{ shard_params: { stripe_size: number } }>("GET", `/debug/v1/tenant/${tenantId}/locate`);
        assert.equal(await onCompute("select setting from pg_settings where name = 'neon.stripe_size'"), String(shard_params.stripe_size));
        assert.deepEqual(await computePod(), before);
        const present = new Set((await query<{ id: number }>("select id from split")).map(({ id }) => id));
        assert.ok(rows.committed.length > 0);
        assert.deepEqual(rows.committed.filter((id) => !present.has(id)), []);
      });

      explainedTest("refuses fewer shards than the tenant has, as Neon merges none, saying so", async () => {
        await assert.rejects(upWith(1, 2), /neon\.shards is 1, but alasio's Neon has 2 shards already, and Neon merges no shards: set it to 2 or more/u);
        await upWith(2, 2);
      });

      explainedTest("moves a drained pageserver's shards to the other, the compute following them with its session held, and back once filled", async () => {
        const before = await computePod();
        const held = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 60_000 });
        held.on("error", () => {});
        await held.connect();
        try {
          await held.query("create table drained (id int primary key)");
          await held.query("insert into drained select g from generate_series(1, 10000) g");
          // The drained shards' secondaries on the other pageserver warm first.
          await warmed((await shards()).filter(({ node }) => node === 2), 1);
          await controller("PUT", "/control/v1/node/2/drain");
          await until("node 2 was not drained", async () => (await controller<Node>("GET", "/control/v1/node/2")).scheduling === "PauseForRestart");
          assert.deepEqual((await shards()).map(({ node }) => node), [1, 1]);
          await until("the compute does not follow the drained shards", async () => (await computeReads()).join() === [pageserverName(0), pageserverName(0)].join());
          await held.query("insert into drained select g from generate_series(10001, 20000) g");
          assert.deepEqual((await held.query("select count(*)::int as n from drained")).rows, [{ n: 20000 }]);

          await controller("PUT", "/control/v1/node/2/config", { node_id: 2, scheduling: "Active" });
          await warmed(await shards(), 2);
          await controller("PUT", "/control/v1/node/2/fill");
          await until("node 2 was not filled", async () => new Set((await shards()).map(({ node }) => node)).size === 2);
          await until("the compute does not follow the filled shards", async () => (await computeReads()).join() === (await attachedTo()).join());
          assert.deepEqual((await held.query("select count(*)::int as n from drained")).rows, [{ n: 20000 }]);
        } finally {
          await held.end().catch(() => {});
        }
        assert.deepEqual(await computePod(), before);
      });

      explainedTest("fails a lost pageserver's shards over to their secondaries once the storage controller finds it offline, neon-control naming where they are, the compute following them without a restart, losing nothing committed", async () => {
        const set = ref("StatefulSet", neonName("pageserver"), NAMESPACE);
        /** Where neon-control's record has the tenant's shards: their pageservers' Services, in shard order. */
        const recorded = async () => {
          const layout = (await kube.execOk(NAMESPACE, `${neonName("controller-db")}-0`, ["psql", "-U", "storage_controller", "-d", "storage_controller", "-Atc", `select layout from neon_control.layout where tenant_id = '${tenantId}'`], { container: "postgres" })).trim();
          return (JSON.parse(layout) as { pageservers: string[] }).pageservers.map((address) => address.split(".")[0]);
        };
        const before = await computePod();
        await query("create table failed_over (id int primary key)");
        await query("insert into failed_over select g from generate_series(1, 10000) g");
        assert.equal(new Set((await shards()).map(({ node }) => node)).size, 2);
        // A failover makes a secondary attached, which serves at once once warm.
        await warmed((await shards()).filter(({ node }) => node === 2), 1);
        // Pageserver 2 lost, as its node would be: its pod gone, and none made again.
        await kube.patch(set, { spec: { replicas: 1 } });
        try {
          await until("node 2 did not go offline", async () => (await controller<Node>("GET", "/control/v1/node/2")).availability === "Offline", 300_000);
          await until("node 2's shards were not failed over to their secondaries on node 1", async () => (await shards()).every(({ node }) => node === 1), 300_000);
          await until("neon-control's record does not name pageserver 1 for every shard", async () => (await recorded()).join() === [pageserverName(0), pageserverName(0)].join());
          await until("the compute does not follow the shards failed over", async () => (await computeReads()).join() === [pageserverName(0), pageserverName(0)].join());
          await query("insert into failed_over select g from generate_series(10001, 20000) g");
          assert.deepEqual(await query("select count(*)::int as n from failed_over"), [{ n: 20000 }]);
          assert.deepEqual(await computePod(), before);
        } finally {
          await kube.patch(set, { spec: { replicas: 2 } });
          await until("node 2 did not come back", async () => (await controller<Node>("GET", "/control/v1/node/2")).availability === "Active", 600_000);
          // As the tests after expect them: one shard on each, as neon-control's round spreads them again.
          await until("the shards were not spread again", async () => new Set((await shards()).map(({ node }) => node)).size === 2, 600_000);
        }
      });

      explainedTest("restarts a compute that started on shards placed before, which never comes to run, once the storage controller notifies where they are, and gives a running one its spec live", async () => {
        const COMPUTE = ref("Deployment", neonName("compute"), NAMESPACE);
        /** Runs `sql` on neon-control's record, in the storage controller's Postgres. */
        const onRecord = (sql: string) => kube.execOk(NAMESPACE, `${neonName("controller-db")}-0`, ["psql", "-U", "storage_controller", "-d", "storage_controller", "-Atc", sql], { container: "postgres" });
        // What the storage controller notifies of where the shards are.
        const located = await controller<{ shards: { node_id: number }[]; shard_params: { stripe_size: number } }>("GET", `/debug/v1/tenant/${tenantId}/locate`);
        const notification = { tenant_id: tenantId, stripe_size: located.shard_params.stripe_size, shards: located.shards.map(({ node_id }, shard_number) => ({ node_id, shard_number })) };
        /** Notifies `pod` of neon-control, as the storage controller does, until it answers 200: the statuses it answered. */
        const notified = async (pod: string) => {
          const answered: number[] = [];
          while (answered.at(-1) !== 200) {
            assert.ok(answered.length < 60, `neon-control answered ${answered.join(", ")}`);
            if (answered.length > 0) await sleep(5000);
            answered.push((await answeredInside(pod, "PUT", "http://127.0.0.1:8080/notify-attach", token("admin"), notification)).status);
          }
          return answered;
        };
        const [control] = await controls();
        assert.ok(control);

        // A running compute last given another layout is given its spec anew, live.
        const running = await computePod();
        await onRecord("update neon_control.compute_layouts set layout = '{\"pageservers\": [], \"stripeSize\": null}' where id = 'alasio'");
        assert.deepEqual(await notified(control), [200]);
        assert.deepEqual(await computePod(), running);
        assert.deepEqual(JSON.parse(await onRecord("select layout from neon_control.compute_layouts where id = 'alasio'")).pageservers, (await attachedTo()).map((name) => `${name}.${NAMESPACE}.svc.cluster.local:6400`));

        // One starting on a layout whose shard 1 is where shard 0 is, as one did in CI after
        // the whole stack restarted, never comes to run: compute_ctl stays in init. Notified,
        // or at the next round of the replica that leads, which sets the layout right on its
        // own, neon-control restarts it, as a 412 from a compute in init is answered.
        const restartedAt = async () => (await kube.get<V1Deployment>(COMPUTE))?.spec?.template.metadata?.annotations?.["kubectl.kubernetes.io/restartedAt"];
        const restartedBefore = await restartedAt();
        const recordedLayout = async () => JSON.parse(await onRecord(`select layout from neon_control.layout where tenant_id = '${tenantId}'`)) as { pageservers: string[] };
        const givenLayout = async () => (JSON.parse((await onRecord("select layout from neon_control.compute_layouts where id = 'alasio'")) || "null") as { pageservers: string[] } | null);
        try {
          const computePods = () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-compute" }) });
          /** How compute_ctl says the compute is; null while it does not answer, as before it starts. */
          const statusOf = async () => {
            const answered = await answeredInside(control, "GET", `http://${neonName("compute")}.${NAMESPACE}.svc.cluster.local:3080/status`, computeCtlToken(privateKey, computeId(MAIN))).catch(() => null);
            return answered?.status === 200 ? (JSON.parse(answered.text) as { status: string }).status : null;
          };
          let stuck: string | undefined;
          for (let attempt = 1; stuck === undefined; attempt++) {
            assert.ok(attempt <= 3, "the leader's round set the layout right before the compute fetched its spec, three times");
            const old = (await computePods()).map(({ metadata }) => metadata?.uid);
            await kube.patch(COMPUTE, { spec: { replicas: 0 } });
            await until("the compute did not stop", async () => !(await computePods()).some(({ metadata }) => old.includes(metadata?.uid)));
            // Main's clients wake it at once: the stale layout is recorded before it may start.
            await onRecord(`update neon_control.layout set layout = jsonb_set(layout, '{pageservers,1}', layout->'pageservers'->0) where tenant_id = '${tenantId}'`);
            await onRecord("delete from neon_control.compute_layouts where id = 'alasio'");
            const stale = (await recordedLayout()).pageservers;
            await kube.patch(COMPUTE, { spec: { replicas: 1 } });
            // It fetches its spec with the stale layout, unless the leader's round set the record right first.
            await until("the compute did not fetch its spec", async () => (await givenLayout()) !== null || (await recordedLayout()).pageservers.join() !== stale.join());
            if ((await givenLayout())?.pageservers.join() !== stale.join()) continue;
            stuck = (await computePods()).find(({ metadata }) => metadata?.deletionTimestamp === undefined)?.metadata?.uid;
          }
          // Notified, neon-control restarts it, and it starts on the shards where they are.
          await notified(control);
          assert.notEqual(await restartedAt(), restartedBefore, "neon-control did not restart the compute");
          await until("the compute did not come to run", async () => (await statusOf()) === "running", 300_000);
          // compute_ctl says so before its pod's readiness probe has passed, which computePod waits for.
          await kube.awaitReady([COMPUTE], "5 minutes");
          assert.notEqual((await computePod()).uid, stuck);
          assert.deepEqual(await computeReads(), await attachedTo());
          await query("select 1");
        } finally {
          // The layout recorded as it is, for the tests after, whatever failed.
          await notified(control);
        }
      });
    });

    describe("the database", () => {
      test("bootstraps its tenant and a timeline on three safekeepers", async () => {
        const { safekeepers } = await record();
        assert.deepEqual([...safekeepers.ids].sort(), [1, 2, 3]);
      });

      test("checkpoints hourly, as its spec says", async () => {
        assert.deepEqual(await query("select current_setting('checkpoint_timeout') as timeout, current_setting('max_wal_size') as wal"), [{ timeout: "1h", wal: "2GB" }]);
      });

      test("keeps a day of history, as its tenant is configured", async () => {
        const zero = await shardZero();
        const config: TenantConfigAnswered = JSON.parse(await inside(zero.pod, "GET", `http://127.0.0.1:9898/v1/tenant/${zero.id}/config`, "pageserverapi"));
        assert.equal(config.effective_config.pitr_interval, "1day");
      });

      test("lets neon-control change no more of a compute's Deployment than the annotation that restarts it", async () => {
        const path = `/apis/apps/v1/namespaces/${NAMESPACE}/deployments/${neonName("compute")}`;
        const annotated = (annotations: Record<string, string>) => ({ spec: { template: { metadata: { annotations } } } });
        assert.equal((await patchedAsNeonControl(path, annotated({ "kubectl.kubernetes.io/restartedAt": new Date().toISOString() }))).status, 200);
        try {
          const refused = [
            annotated({ "e2e.alasio.dev/other": "x" }),
            { spec: { template: { metadata: { generateName: "other-" } } } },
            { spec: { template: { spec: { containers: [{ name: "compute", image: "registry.k8s.io/pause:3.10.1" }] } } } },
            { spec: { minReadySeconds: 5 } },
            { metadata: { labels: { "e2e.alasio.dev/other": "x" } } },
          ];
          for (const patch of refused) {
            const { status, text } = await patchedAsNeonControl(path, patch);
            assert.ok(status === 422 && text.includes("ValidatingAdmissionPolicy 'alasio-compute-restarts'"), `${JSON.stringify(patch)}: ${status} ${text}`);
          }
        } finally {
          // The restart it let through rolled the compute: done, its old pod gone, before the tests after it count the stack's pods.
          await kube.awaitReady([ref("Deployment", neonName("compute"), NAMESPACE)], "5 minutes");
        }
      });

      test("an upgrade that changes nothing restarts nothing of the stack", async () => {
        // Its workloads' pods: not a Job's, which a CronJob starts at its minute whatever an upgrade does.
        const pods = async () => (await runningPods()).filter(({ metadata }) => !metadata?.ownerReferences?.some(({ kind }) => kind === "Job")).map((pod) => pod.metadata?.uid).sort();
        const before = await pods();
        await alasioOk("upgrade", "--timeout", "10m");
        assert.deepEqual(await pods(), before);
      });

      explainedTest("keeps every row through the whole stack stopping at once", async () => {
        await query("create table kept (id int primary key)");
        await query("insert into kept select g from generate_series(1, 50000) g");
        const stopped = await runningPods();
        await Promise.all(stopped.map((pod) => kube.remove(ref("Pod", pod.metadata?.name ?? "", NAMESPACE))));
        await awaitReplaced(stopped);
        await up();
        assert.deepEqual(await query("select count(*)::int as n from kept"), [{ n: 50000 }]);
      });

      for (const component of ["neon-pageserver", "neon-safekeeper", "neon-compute", "neon-storage-controller", "seaweedfs", "seaweedfs-filer", "seaweedfs-volume"]) {
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

      test("a safekeeper made again takes the place of a pod of the floor that may run elsewhere, which took its room on its node", async () => {
        const set = ref("StatefulSet", neonName("safekeeper"), NAMESPACE);
        const name = `${neonName("safekeeper")}-2`;
        const pod = await kube.get<V1Pod>(ref("Pod", name, NAMESPACE));
        const node = pod?.spec?.nodeName;
        assert.ok(pod && node);
        // Two of three keep the quorum while it is away.
        await kube.patch(set, { spec: { replicas: 2 } });
        await awaitReplaced([pod]);
        // What its node has left, its room with it, taken by a pod of the floor that prefers its node and may run on any:
        // all but what runs at the floor or above, as it takes the place of any pod below the floor there, a read
        // replica's, which it does only where no other node takes it, so the others are cordoned until it runs.
        const others = (await kube.list<V1Node>("Node")).map(({ metadata }) => metadata?.name ?? "").filter((other) => other !== node);
        const cordoned = (unschedulable: boolean) => Promise.all(others.map((other) => kube.patch(ref("Node", other), { spec: { unschedulable } })));
        const allocatable = await kube.get<V1Node>(ref("Node", node));
        const floor = (await kube.get<V1PriorityClass>(ref("PriorityClass", `${RELEASE}-floor`)))?.value ?? 0;
        const pods = await kube.list<V1Pod>("Pod", { fieldSelector: `spec.nodeName=${node},status.phase!=Succeeded,status.phase!=Failed` });
        const requested = pods.filter(({ spec }) => (spec?.priority ?? 0) >= floor).reduce((sum, { spec }) => sum + (spec ? cpuRequested(spec) : 0), 0);
        const free = cpuMillis(allocatable?.status?.allocatable?.["cpu"] ?? "0") - requested;
        assert.ok(free > 0, `${node} has ${free}m of CPU free`);
        const filler = `${RELEASE}-e2e-filler`;
        const labels = { "app.kubernetes.io/name": "e2e-filler" };
        const safekeepers = await kube.get<V1StatefulSet>(set);
        const deployment: V1Deployment = {
          apiVersion: "apps/v1",
          kind: "Deployment",
          metadata: { name: filler, namespace: NAMESPACE, labels },
          spec: {
            replicas: 1,
            selector: { matchLabels: labels },
            template: {
              metadata: { labels },
              spec: {
                priorityClassName: `${RELEASE}-floor`,
                affinity: { nodeAffinity: { preferredDuringSchedulingIgnoredDuringExecution: [{ weight: 100, preference: { matchFields: [{ key: "metadata.name", operator: "In", values: [node] }] } }] } },
                automountServiceAccountToken: false,
                terminationGracePeriodSeconds: 0,
                securityContext: { runAsNonRoot: true, runAsUser: 1000, runAsGroup: 1000, seccompProfile: { type: "RuntimeDefault" } },
                containers: [{ name: "filler", image: image(safekeepers), command: ["sleep", "infinity"], securityContext: restricted, resources: { requests: { cpu: `${free}m` } } }],
              },
            },
          },
        };
        try {
          await cordoned(true);
          await kube.apply(deployment);
          const placed = async () => (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(labels) })).find(({ spec, status }) => spec?.nodeName === node && status?.phase === "Running");
          const deadline = Date.now() + 120_000;
          let first = await placed();
          while (!first) {
            assert.ok(Date.now() < deadline, `the filler did not run on ${node}`);
            await sleep(2000);
            first = await placed();
          }
          await cordoned(false);
          await kube.patch(set, { spec: { replicas: 3 } });
          await awaitPodReady(name, 180_000);
          assert.equal((await kube.get<V1Pod>(ref("Pod", name, NAMESPACE)))?.spec?.nodeName, node);
          // The filler gave its place, and is made again, elsewhere where there is room.
          await awaitReplaced([first], 30_000);
        } finally {
          await cordoned(false);
          await kube.remove(ref("Deployment", filler, NAMESPACE)).catch(() => {});
          await kube.patch(set, { spec: { replicas: 3 } });
        }
        await awaitPodReady(name);
      });

      test("garbage the pageserver collects is deleted from the object store, validated by the storage controller", async () => {
        const { timelineId } = await record();
        await query("create table churn (id int primary key, payload text)");
        for (let pass = 0; pass < 3; pass++) {
          await query("truncate churn");
          await query("insert into churn select g, repeat(md5(random()::text), 30) from generate_series(1, 60000) g");
        }
        // Shard zero's, on its pageserver, as each shard is collected alike.
        const zero = await shardZero();
        const before = await pageserverMetric("pageserver_deletion_queue_executed_total", zero.pod);
        const timeline = `http://127.0.0.1:9898/v1/tenant/${zero.id}/timeline/${timelineId}`;
        // The churn is in the shard's open in-memory layer, which the pageserver writes out as
        // an L0 layer only past checkpoint_distance (256 MiB) or checkpoint_timeout (10 min):
        // until then compaction finds nothing of it to compact, and nothing to delete. A
        // shorter timeout, for this test alone, writes it out within seconds.
        /** The tenant's configuration patched with `patch`, by the storage controller. */
        const patched = async (patch: object) => {
          const answered = await answeredInside(await podOf("neon-storage-controller"), "PATCH", "http://127.0.0.1:1234/v1/tenant/config", token("admin"), { tenant_id: tenantId, ...patch });
          assert.equal(answered.status, 200, answered.text);
        };
        await patched({ checkpoint_timeout: "5s" });
        // Compacted until it schedules a deletion, and no more: the deletion queue writes its
        // list out only once ten seconds pass with no deletion more (FRONTEND_DEFAULT_TIMEOUT
        // in pageserver/src/deletion_queue/list_writer.rs at release-9129), which compacting
        // every few seconds never lets happen.
        const submittedBefore = await pageserverMetric("pageserver_deletion_queue_submitted_total", zero.pod);
        let executed = before;
        try {
          const scheduling = Date.now() + 120_000;
          while ((await pageserverMetric("pageserver_deletion_queue_submitted_total", zero.pod)) <= submittedBefore) {
            assert.ok(Date.now() < scheduling, "compaction scheduled no deletion");
            const compacted = await answeredInside(zero.pod, "PUT", `${timeline}/compact?force_l0_compaction=true&force_repartition=true&force_image_layer_creation=true&wait_until_uploaded=true`, token("pageserverapi"));
            assert.equal(compacted.status, 200, compacted.text);
            await sleep(5000);
          }
        } finally {
          await patched({ checkpoint_timeout: null });
        }
        const deadline = Date.now() + 180_000;
        while (Date.now() < deadline && !(executed > before)) {
          await sleep(5000);
          executed = await pageserverMetric("pageserver_deletion_queue_executed_total", zero.pod);
        }
        if (!(executed > before)) {
          // What the pageserver's deletion queue and layers were, to say why.
          const metrics = (await inside(zero.pod, "GET", "http://127.0.0.1:9898/metrics", "pageserverapi")).split("\n").filter((line) => line.startsWith("pageserver_deletion_queue"));
          const layers = JSON.parse(await inside(zero.pod, "GET", `${timeline}/layer`, "pageserverapi")) as { historic_layers?: { kind: string; layer_file_name: string }[] };
          const deltas = (layers.historic_layers ?? []).filter(({ kind }) => kind === "Delta").map(({ layer_file_name }) => layer_file_name);
          const logged = async (pod: string, container: string, pattern: RegExp) =>
            (await kube.logs(NAMESPACE, pod, container).catch(String)).split("\n").filter((line) => pattern.test(line)).slice(-15).join("\n");
          const pageserver = await logged(zero.pod, "pageserver", /deletion|validat|upcall|generation/iu);
          const storageController = await logged(await podOf("neon-storage-controller"), "storage-controller", /validat|upcall/iu);
          assert.fail(`deletions executed: ${before} -> ${executed}; ${metrics.join("; ")}; ${deltas.length} delta layers: ${deltas.slice(0, 10).join(", ")}\n--- ${zero.pod}:\n${pageserver}\n--- storage controller:\n${storageController}`);
        }
        assert.equal(
          await pageserverMetric("pageserver_deletion_queue_validated_total", zero.pod),
          await pageserverMetric("pageserver_deletion_queue_executed_total", zero.pod),
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

      test("the object store holds Neon's bucket to its lifecycle rules, the safekeepers' full WAL segments expiring after three days", async () => {
        const answered: { Rules: { ID: string }[] } = JSON.parse(await kube.execOk(
          NAMESPACE,
          await kube.runningPod(NAMESPACE, componentName("seaweedfs")),
          // As the stack's admin, Neon's bucket's, which gave it them.
          ["/bin/sh", "-c", 'AWS_ACCESS_KEY_ID="$ADMIN_KEY_ID" AWS_SECRET_ACCESS_KEY="$ADMIN_SECRET" aws s3api --endpoint-url http://127.0.0.1:8333 get-bucket-lifecycle-configuration --bucket neon'],
          { container: "lifecycle" },
        ));
        const rule = (id: string) => answered.Rules.find(({ ID }) => ID === id);
        assert.deepEqual(rule("expire-wal"), {
          ID: "expire-wal",
          Status: "Enabled",
          Filter: { And: { Prefix: "safekeeper/", ObjectSizeGreaterThan: 16 * 1024 * 1024 - 1 } },
          Expiration: { Days: 3 },
        });
        assert.ok(rule("expire-noncurrent-wal"));
        assert.ok(rule("expire-noncurrent"));
      });

      /** Runs `script` in the gateway `pod`'s lifecycle container, as the tests' own S3 identity, its `s3api` at `endpoint` (its own, unless another's). */
      const onGateway = (pod: string, script: string) =>
        kube.execOk(NAMESPACE, pod, ["/bin/sh", "-c", `set -e; ${AS_E2E}; s3api() { endpoint=$1; shift; aws s3api --endpoint-url "http://$endpoint" "$@"; }; ${script}`], { container: "lifecycle" });

      /** The masters' pods that run now. */
      const masters = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "seaweedfs-master" }) }))
          .filter(({ status, metadata }) => status?.phase === "Running" && !metadata?.deletionTimestamp).map(({ metadata }) => metadata?.name ?? "");
      /** What a running master answers at `path` of its HTTP port, which it asks its leader where it is not one. */
      const onMaster = async (path: string, master?: string) =>
        kube.execOk(NAMESPACE, master ?? (await masters())[0] ?? assert.fail("no master runs"), ["wget", "-q", "-O", "-", `http://127.0.0.1:9333${path}`], { container: "master" });
      /** The pod of a part's address, as SeaweedFS names a server: its name in its headless Service, and a port. */
      const podOfAddress = (address: string) => address.split(".")[0] ?? "";

      test("the object store, its objects kept twice, serves every one while a volume server holding them is away, and takes new ones", { skip: AGENTS < 2 && "three volume servers, on three nodes" }, async () => {
        const gateway = (await gateways())[0] ?? assert.fail("no gateway runs");
        const bucket = `replicated-${Date.now()}`;
        await onGateway(gateway, `s3api 127.0.0.1:8333 create-bucket --bucket ${bucket} >/dev/null; for n in $(seq 1 20); do head -c 65536 /dev/urandom > /tmp/$n; s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key $n --body /tmp/$n >/dev/null; done`);
        // Each of the bucket's volumes on two volume servers, as the masters say: they grow a
        // collection by another volume whenever none of its volumes takes a write at once, and
        // two volumes' servers need not be the same two.
        const status: VolumeStatus = JSON.parse(await onMaster("/vol/status"));
        const holders = new Map<number, string[]>();
        for (const [address, volumes] of Object.values(status.Volumes.DataCenters).flatMap((racks) => Object.values(racks)).flatMap((nodes) => Object.entries(nodes))) {
          for (const { Id } of volumes.filter(({ Collection }) => Collection === bucket)) holders.set(Id, [...(holders.get(Id) ?? []), address]);
        }
        const placed = [...holders].map(([id, on]) => `${id} on ${on.join(", ")}`).join("; ");
        assert.ok(holders.size > 0 && [...holders.values()].every((on) => on.length === 2), `the bucket's volumes: ${placed || "none"}`);
        // A volume server holding some of them.
        const away = podOfAddress([...holders.values()][0]?.[0] ?? "");
        const node = (await kube.get<V1Pod>(ref("Pod", away, NAMESPACE)))?.spec?.nodeName ?? assert.fail(`${away} runs on no node`);
        const registered = async () => dataNodes(JSON.parse(await onMaster("/dir/status"))).map(({ Url }) => podOfAddress(Url));
        // Its node takes no pod, and its pod, made anew, is placed nowhere else, beside another volume server.
        await kube.patch(ref("Node", node), { spec: { unschedulable: true } });
        try {
          await kube.kill(NAMESPACE, away);
          for (const deadline = Date.now() + 120_000; (await registered()).includes(away); await sleep(2000)) {
            if (Date.now() > deadline) assert.fail(`the masters still count ${away}`);
          }
          // While it is away: every object read back as written, and a new one taken.
          const read = await onGateway(gateway, `missing=0; for n in $(seq 1 20); do s3api 127.0.0.1:8333 get-object --bucket ${bucket} --key $n /tmp/back >/dev/null 2>&1 && cmp -s /tmp/$n /tmp/back || missing=$((missing + 1)); done; echo "missing $missing"; s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key new --body /tmp/1 >/dev/null && echo taken`);
          assert.match(read, /^missing 0$/mu);
          assert.match(read, /^taken$/mu);
          // And it was away throughout: unready, unplaced, and not among the masters' volume servers.
          const pod = await kube.get<V1Pod>(ref("Pod", away, NAMESPACE));
          assert.deepEqual([pod?.spec?.nodeName, pod?.status?.conditions?.find(({ type }) => type === "Ready")?.status ?? "False"], [undefined, "False"]);
          assert.ok(!(await registered()).includes(away), `the masters count ${away} again`);
        } finally {
          await kube.patch(ref("Node", node), { spec: { unschedulable: null } });
          await onGateway(gateway, `aws s3 --endpoint-url http://127.0.0.1:8333 rb --force s3://${bucket} >/dev/null`).catch(() => {});
          await kube.awaitReady([ref("StatefulSet", componentName("seaweedfs-volume"), NAMESPACE)], "10 minutes");
        }
      });

      test("the object store's three masters elect another leader when theirs is killed, and the store takes and serves objects through it", { skip: AGENTS < 2 && "three masters, on three nodes" }, async () => {
        const running = await masters();
        assert.equal(running.length, 3);
        const leaderAsSaidBy = async (master: string) => podOfAddress((JSON.parse(await onMaster("/cluster/status", master)) as { Leader?: string }).Leader ?? "");
        const leader = await leaderAsSaidBy(running[0] ?? "");
        assert.ok(running.includes(leader), `the masters' leader is ${leader}`);
        const other = running.find((master) => master !== leader) ?? "";
        // A leader is a pod's instance: the killed one's pod is made anew, which may be elected.
        const instance = async (pod: string) => (await kube.get<V1Pod>(ref("Pod", pod, NAMESPACE)))?.metadata?.uid ?? "";
        const killed = await instance(leader);
        await kube.kill(NAMESPACE, leader);
        for (const deadline = Date.now() + 120_000;; await sleep(2000)) {
          const elected = await leaderAsSaidBy(other).catch(() => "");
          if (elected !== "" && (elected !== leader || (await instance(elected)) !== killed)) break;
          if (Date.now() > deadline) assert.fail(`the masters elected no leader but the one killed, ${leader}: ${elected || "none"} as ${other} says`);
        }
        const gateway = (await gateways())[0] ?? assert.fail("no gateway runs");
        const bucket = `leader-${Date.now()}`;
        try {
          const said = await onGateway(gateway, `s3api 127.0.0.1:8333 create-bucket --bucket ${bucket} >/dev/null; head -c 65536 /dev/urandom > /tmp/led; s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key led --body /tmp/led >/dev/null; s3api 127.0.0.1:8333 get-object --bucket ${bucket} --key led /tmp/back >/dev/null; cmp -s /tmp/led /tmp/back && echo same`);
          assert.match(said, /^same$/mu);
        } finally {
          await onGateway(gateway, `aws s3 --endpoint-url http://127.0.0.1:8333 rb --force s3://${bucket} >/dev/null`).catch(() => {});
          await kube.awaitReady([ref("StatefulSet", componentName("seaweedfs-master"), NAMESPACE)], "10 minutes");
        }
      });

      test("the object store's parts refuse the stack's other pods: the lake reaches a gateway's S3 port, and neither its gRPC and metrics ports, nor any master, volume server or filer", async () => {
        const lake = await kube.runningPod(NAMESPACE, componentName("lake"));
        const addressOf = async (pod: string) => (await kube.get<V1Pod>(ref("Pod", pod, NAMESPACE)))?.status?.podIP ?? assert.fail(`${pod} has no address`);
        const reached = async (pod: string, port: number) => (await kube.execOk(NAMESPACE, lake, ["node", "-e", tcp(JSON.stringify(await addressOf(pod)), port)], { container: "lake" })).trim();
        const gateway = await kube.runningPod(NAMESPACE, componentName("seaweedfs"));
        assert.equal(await reached(gateway, 8333), "open");
        for (const port of [18333, 9327]) assert.equal(await reached(gateway, port), "blocked", `seaweedfs:${port}`);
        for (const [part, port] of [["seaweedfs-master", 9333], ["seaweedfs-master", 19333], ["seaweedfs-volume", 8080], ["seaweedfs-filer", 8888], ["seaweedfs-filer", 18888]] as const) {
          assert.equal(await reached(`${componentName(part)}-0`, port), "blocked", `${part}:${port}`);
        }
      });

      test("an object written through one gateway is read through another at once, every time, its filers on one store", { skip: AGENTS < 2 && "two gateways and two filers, on three nodes" }, async () => {
        const [one, other] = await gateways();
        if (!one || !other) return assert.fail("two gateways run");
        const otherAddress = (await kube.get<V1Pod>(ref("Pod", other, NAMESPACE)))?.status?.podIP;
        const bucket = `read-after-write-${Date.now()}`;
        await onGateway(one, `s3api 127.0.0.1:8333 create-bucket --bucket ${bucket} >/dev/null`);
        try {
          const said = await onGateway(one, [
            "missing=0; echo body > /tmp/body",
            "for n in $(seq 1 100); do",
            `  s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key $n --body /tmp/body >/dev/null`,
            `  s3api ${otherAddress}:8333 head-object --bucket ${bucket} --key $n >/dev/null 2>&1 || missing=$((missing + 1))`,
            "done",
            'echo "missing $missing"',
          ].join("\n"));
          assert.match(said, /^missing 0$/mu);
        } finally {
          await onGateway(one, `aws s3 --endpoint-url http://127.0.0.1:8333 rb --force s3://${bucket} >/dev/null`).catch(() => {});
        }
      });

      test("the object store compacts a volume once a tenth of it is deleted", async () => {
        const gateway = await kube.runningPod(NAMESPACE, componentName("seaweedfs"));
        const bucket = `vacuum-${Date.now()}`;
        const s3 = (script: string) => kube.execOk(NAMESPACE, gateway, ["/bin/sh", "-c", `set -e; ${AS_E2E}; s3() { aws s3 --endpoint-url http://127.0.0.1:8333 "$@"; }; ${script}`], { container: "lifecycle" });
        const master = (path: string) => onMaster(path);
        const volumes = async () => {
          const status: VolumeStatus = JSON.parse(await master("/vol/status"));
          return Object.values(status.Volumes.DataCenters).flatMap((racks) => Object.values(racks)).flatMap((nodes) => Object.values(nodes)).flat()
            .filter(({ Collection }) => Collection === bucket);
        };
        // A bucket of its own, so a volume of its own: ten equal objects, two of them deleted.
        await s3(`aws s3api --endpoint-url http://127.0.0.1:8333 create-bucket --bucket ${bucket} >/dev/null; head -c 1048576 /dev/urandom >/tmp/object; for n in 0 1 2 3 4 5 6 7 8 9; do s3 cp --quiet /tmp/object s3://${bucket}/$n; done; rm /tmp/object; s3 rm --quiet s3://${bucket}/0; s3 rm --quiet s3://${bucket}/1`);
        try {
          const deadline = Date.now() + 600_000;
          let last: readonly Volume[] = [];
          while (Date.now() < deadline) {
            last = await volumes();
            if (last.some(({ CompactRevision }) => CompactRevision > 0) && last.every(({ DeletedByteCount }) => DeletedByteCount === 0)) return;
            // Once the deletions have reached the volume, a pass at the master's own
            // threshold; while another runs, the call returns at once.
            if (last.some(({ DeletedByteCount }) => DeletedByteCount > 0)) await master("/vol/vacuum");
            await sleep(5000);
          }
          assert.fail(`the bucket's volumes were not compacted: ${JSON.stringify(last)}`);
        } finally {
          await s3(`s3 rb --force s3://${bucket} >/dev/null`).catch(() => {});
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
          const gateway = await kube.runningPod(NAMESPACE, componentName("seaweedfs")).catch(() => "");
          const store = (await kube.logs(NAMESPACE, gateway, "s3").catch(() => "")).trimEnd().split("\n").slice(-30).join("\n");
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
            // As the backups' own identity, which wrote it.
            envFrom: [{ secretRef: { name: neonName("s3-backups") } }],
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

        const { timelineId } = await record();
        const zero = await shardZero();
        const found: LsnAtTimestamp = JSON.parse(await inside(
          zero.pod,
          "GET",
          `http://127.0.0.1:9898/v1/tenant/${zero.id}/timeline/${timelineId}/get_lsn_by_timestamp?timestamp=${encodeURIComponent(moment)}`,
          "pageserverapi",
        ));
        assert.equal(found.kind, "present");

        // A read-only compute pinned to that moment's LSN, given as a file the spec
        // neon-control serves the compute.
        const config = await servedSpec(await kube.runningPod(NAMESPACE, neonName("control")));
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
                `/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute --connstr postgresql://cloud_admin@localhost:${COMPUTE_PORT}/postgres`,
                "--pgbin /usr/local/bin/postgres --compute-id static --config /config/config.json >/tmp/compute.log 2>&1 &",
                `for i in $(seq 120); do psql -h 127.0.0.1 -p ${COMPUTE_PORT} -U cloud_admin -d alasio -Atc 'select count(*) from history' 2>/dev/null && exit 0; sleep 2; done; tail -40 /tmp/compute.log; exit 1`,
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

      test("alasio restore-filer-store puts the filer store back from its backup: what was there then is, what was written since is not, and the stack runs on it again", async () => {
        const bucket = `restore-${Date.now()}`;
        const gateway = async () => (await gateways())[0] ?? assert.fail("no gateway runs");
        const held = async (key: string) =>
          (await onGateway(await gateway(), `s3api 127.0.0.1:8333 head-object --bucket ${bucket} --key ${key} >/dev/null 2>&1 && echo held || echo missing`)).trim();
        await onGateway(await gateway(), `s3api 127.0.0.1:8333 create-bucket --bucket ${bucket} >/dev/null; echo before > /tmp/body; s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key before --body /tmp/body >/dev/null`);
        await query("create table if not exists restored (said text)");
        await query("insert into restored values ('before')");
        try {
          // A backup, as the CronJob takes one hourly.
          const name = `filer-store-backup-${Date.now()}`;
          const template = (await kube.get<V1CronJob>(ref("CronJob", componentName("seaweedfs-filer-store-backup"), NAMESPACE)))?.spec?.jobTemplate;
          assert.ok(template?.spec);
          const job: V1Job = { apiVersion: "batch/v1", kind: "Job", metadata: { name, namespace: NAMESPACE, labels: template.metadata?.labels ?? {} }, spec: template.spec };
          await kube.apply(job);
          try {
            await kube.awaitReady([ref("Job", name, NAMESPACE)], "10 minutes");
          } finally {
            await kube.remove(ref("Job", name, NAMESPACE)).catch(() => {});
          }
          await onGateway(await gateway(), `echo after > /tmp/body; s3api 127.0.0.1:8333 put-object --bucket ${bucket} --key after --body /tmp/body >/dev/null`);
          assert.deepEqual([await held("before"), await held("after")], ["held", "held"]);

          const listed = await alasioOk("restore-filer-store");
          const stamp = /^ {2}(\d{8}T\d{6}Z) /mu.exec(listed)?.[1] ?? assert.fail(`no backup is listed:\n${listed}`);
          const restored = await alasioOk("restore-filer-store", stamp, "--yes", "--timeout", "30m");
          assert.match(restored, /^The filer store is as it was at .+, with its \d+ keys, and alasio runs on it again\.$/mu);
          await up();
          assert.deepEqual([await held("before"), await held("after")], ["held", "missing"]);
          assert.deepEqual(await query("select said from restored"), [{ said: "before" }]);
        } finally {
          await onGateway(await gateway(), `aws s3 --endpoint-url http://127.0.0.1:8333 rb --force s3://${bucket} >/dev/null`).catch(() => {});
          await query("drop table if exists restored").catch(() => {});
        }
      });
    });

    // After the database's tests: the whole stack stopping at once would stop the branch's compute too.
    describe("branches", () => {
      const BRANCH = "e2e";
      /** The pod of the branch's compute, which runs as main's does, but on the branch. */
      const COMPUTE = `${computeId(BRANCH)}-compute`;
      /** Its compute's own credentials: its roles' passwords, and its token, which neon-control is given the hashes of. */
      const PASSWORD = `branch-${randomUUID()}`;
      const COMPUTE_TOKEN = `compute-${randomUUID()}`;
      const compute = { verifiers: { alasio: scramVerifier(PASSWORD), lake: scramVerifier(`lake-${randomUUID()}`) }, tokenSha256: tokenSha256(COMPUTE_TOKEN) };
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
      const onBranch = async (sql: string) => (await kube.execOk(NAMESPACE, COMPUTE, ["psql", "-h", "127.0.0.1", "-p", String(COMPUTE_PORT), "-U", "cloud_admin", "-d", "alasio", "-Atc", sql])).trim();

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
          `PGPASSWORD="$1" psql -h "$(hostname -i | cut -d" " -f1)" -p ${COMPUTE_PORT} -U alasio -d alasio -Atc "select current_user" 2>/dev/null || echo refused`,
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

    // After the branches': this stops main's clients, and main's compute, as neon-control does when no client has a session on it.
    describe("Neon's proxy", () => {
      const COMPUTE_DEPLOYMENT = ref("Deployment", neonName("compute"), NAMESPACE);
      const CONTROL = ref("Deployment", neonName("control"), NAMESPACE);
      /** Main's clients, which hold sessions on its compute, its lake's loader and intake always. */
      const CLIENTS = [RELEASE, componentName("lake"), componentName("lake-intake"), componentName("grafana")].map((name) => ref("Deployment", name, NAMESPACE));
      /** What scales the intake by what it has yet to read of the log, which would start it again while it is stopped: held at none meanwhile. */
      const INTAKE_SCALER = ref("ScaledObject", componentName("lake-intake"), NAMESPACE);
      const PAUSED_REPLICAS = "autoscaling.keda.sh/paused-replicas";

      /** The Deployment scaled to none, and its pods gone, a terminating one too. */
      async function stopped(workload: ObjectRef) {
        await kube.patch(workload, { spec: { replicas: 0 } });
        const labels = (await kube.get<V1Deployment>(workload))?.spec?.selector.matchLabels ?? {};
        const deadline = Date.now() + 120_000;
        while ((await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(labels) })).length > 0) {
          assert.ok(Date.now() < deadline, `${workload.name} did not stop`);
          await sleep(1000);
        }
      }

      /** The compute put to sleep, as neon-control puts it, and gone. */
      const asleep = () => stopped(COMPUTE_DEPLOYMENT);

      /** Every pod of main's compute, one stopping among them. */
      const COMPUTE_PODS = selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-compute" });

      /** How long main's compute may be idle before it sleeps, as installed, which the suite's tests shorten and its end gives back. */
      let installedSuspend: string | undefined;

      /** Has main's compute sleep once idle for `seconds`, as its Deployment's annotation tells neon-control. */
      async function suspendingAfter(seconds: number): Promise<void> {
        const annotations = (await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.metadata?.annotations ?? {};
        installedSuspend ??= annotations[SUSPEND_AFTER];
        await kube.patch(COMPUTE_DEPLOYMENT, { metadata: { annotations: { [SUSPEND_AFTER]: String(seconds) } } });
      }

      /** neon-control's running replicas. */
      const controlReplicas = async () =>
        (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-control" }), fieldSelector: "status.phase=Running" }))
          .filter((pod) => pod.metadata?.deletionTimestamp === undefined)
          .map((pod) => pod.metadata?.name ?? "");

      /** What `pod` of neon-control logged, a line each. */
      const logsOf = async (pod: string) => (await kube.logs(NAMESPACE, pod, "neon-control")).split("\n");

      /** Every line every running replica of neon-control logged. */
      const controlLogs = async () => (await Promise.all((await controlReplicas()).map(logsOf))).flat();

      /** Of `pods` of neon-control, those that lead now: that last said they lead, and not since that they no longer do. */
      async function leaders(pods: readonly string[]): Promise<string[]> {
        const said = await Promise.all(pods.map(async (pod) => [pod, (await logsOf(pod)).filter((line) => /"message":"(no longer )?leading"/u.test(line)).at(-1)] as const));
        return said.filter(([, last]) => last?.includes('"message":"leading"')).map(([pod]) => pod);
      }

      /** How long one connection through the proxy takes to its first answer, with alasio's patience. */
      async function connected(): Promise<number> {
        const started = Date.now();
        const client = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 60_000 });
        client.on("error", () => {});
        try {
          await client.connect();
          await client.query("select 1");
          return Date.now() - started;
        } finally {
          await client.end().catch(() => {});
        }
      }

      after(async () => {
        if (installedSuspend !== undefined) await kube.patch(COMPUTE_DEPLOYMENT, { metadata: { annotations: { [SUSPEND_AFTER]: installedSuspend } } });
        await kube.patch(INTAKE_SCALER, { metadata: { annotations: { [PAUSED_REPLICAS]: null } } });
        for (const workload of [CONTROL, ...CLIENTS]) await kube.patch(workload, { spec: { replicas: 1 } });
        await up();
        await kube.awaitReady(CLIENTS, "5 minutes");
      });

      test("is what every client's session on the compute comes from", async () => {
        const proxies = (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-proxy" }) }))
          .map((pod) => pod.status?.podIP);
        const compute = await kube.runningPod(NAMESPACE, neonName("compute"));
        const sessions = await kube.execOk(NAMESPACE, compute, [
          "psql", "-h", "127.0.0.1", "-p", String(COMPUTE_PORT), "-U", "cloud_admin", "-d", "postgres", "-Atc",
          "select distinct usename || ' ' || host(client_addr) from pg_stat_activity where usename in ('alasio', 'lake', 'lake_reader', 'grafana')",
        ], { container: "compute" });
        const seen = sessions.trim().split("\n").filter(Boolean).map((line) => line.split(" "));
        assert.ok(seen.some(([role]) => role === "lake"), `the lake's loader holds a session: ${sessions}`);
        for (const [role, address] of seen) assert.ok(proxies.includes(address), `${role}'s session comes from ${address}, not the proxy (${proxies.join(", ")})`);
      });

      test("wakes a compute that sleeps on the first connection to it, within alasio's patience, warm and with the pageserver just restarted", async () => {
        // Main's clients stopped, which would wake it again at once, a terminating one's last reconnection too.
        await kube.patch(INTAKE_SCALER, { metadata: { annotations: { [PAUSED_REPLICAS]: "0" } } });
        for (const workload of CLIENTS) await stopped(workload);
        const warm: number[] = [];
        for (let wake = 0; wake < 3; wake++) {
          await asleep();
          warm.push(await connected());
        }
        await asleep();
        await kube.kill(NAMESPACE, await podOf("neon-pageserver"));
        await sleep(2000);
        await kube.awaitReady([ref("StatefulSet", neonName("pageserver"), NAMESPACE)], "5 minutes");
        const cold = await connected();
        console.error(`# woken in ${warm.join(", ")} ms; with the pageserver just restarted, ${cold} ms`);
        for (const ms of [...warm, cold]) assert.ok(ms < 60_000, `a wake took ${ms} ms`);
      });

      test("lets twenty first connections at once all through", async () => {
        await asleep();
        const answered = await Promise.allSettled(Array.from({ length: 20 }, connected));
        assert.deepEqual(answered.filter(({ status }) => status === "rejected"), []);
      });

      test("wakes a compute whose wake neon-control was restarted in, once asked again", async () => {
        await asleep();
        const first = connected().catch(() => null);
        // In flight: neon-control has scaled the compute, and waits for it to take connections.
        const deadline = Date.now() + 60_000;
        while ((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas !== 1) {
          assert.ok(Date.now() < deadline, "no wake began");
          await sleep(100);
        }
        const computes = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: COMPUTE_PODS });
        assert.ok(!computes.some((pod) => pod.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True")), "the compute is not ready yet");
        await kube.kill(NAMESPACE, await podOf("neon-control"));
        await first;
        // The client asks again, as alasio's pool does, until neon-control is back: the compute is scaling already, and the wake waits for it.
        const started = Date.now();
        while (!(await connected().then(() => true, () => false))) {
          assert.ok(Date.now() - started < 120_000, "no connection got through since neon-control was restarted");
          await sleep(1000);
        }
        assert.equal((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas, 1);
      });

      test("is served by neon-control's replicas alike, each waking a compute that sleeps, which record a branch made through two at once, once, and of which one leads", async () => {
        await kube.patch(CONTROL, { spec: { replicas: 2 } });
        await kube.awaitReady([CONTROL], "5 minutes");
        const replicas = await controlReplicas();
        assert.equal(replicas.length, 2);
        const name = `twice-${Date.now()}`;
        const body = { name, compute: { verifiers: { alasio: scramVerifier(randomUUID()), lake: scramVerifier(randomUUID()) }, tokenSha256: tokenSha256(randomUUID()) } };
        const answers = await Promise.all(replicas.map(async (pod) => JSON.parse(await inside(pod, "POST", "http://127.0.0.1:8080/branches", "admin", body)) as Listed));
        assert.equal(new Set(answers.map(({ timelineId }) => timelineId)).size, 1, JSON.stringify(answers));
        assert.equal((await recorded()).filter((branch) => branch.name === name).length, 1);
        const [one] = replicas;
        assert.ok(one);
        assert.equal(await inside(one, "DELETE", `http://127.0.0.1:8080/branches/${name}`, "admin"), "");
        // Each answers the proxy, a role's secret and a wake from sleep, itself.
        for (const pod of replicas) {
          await asleep();
          const access = JSON.parse(await inside(pod, "GET", "http://127.0.0.1:8080/get_endpoint_access_control?endpointish=alasio&role=alasio", "proxy")) as { role_secret?: string };
          assert.match(access.role_secret ?? "", /^SCRAM-SHA-256\$/u, pod);
          const woke = JSON.parse(await inside(pod, "GET", "http://127.0.0.1:8080/wake_compute?endpointish=alasio", "proxy")) as { aux?: { cold_start_info?: string } };
          assert.equal(woke.aux?.cold_start_info, "pool_miss", pod);
          assert.equal((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas, 1, pod);
        }
        // One leads; once it is gone, one other does: the replica left, or the one made in its place.
        const leading = await leaders(replicas);
        assert.equal(leading.length, 1, `leading: ${leading.join(", ")}`);
        const [gone] = leading;
        assert.ok(gone);
        await kube.kill(NAMESPACE, gone);
        const handedOver = Date.now();
        for (;;) {
          const now = await leaders((await controlReplicas()).filter((pod) => pod !== gone));
          assert.ok(now.length <= 1, `leading at once: ${now.join(", ")}`);
          if (now.length === 1) break;
          assert.ok(Date.now() - handedOver < 120_000, "no replica took the lead");
          await sleep(2000);
        }
        await kube.patch(CONTROL, { spec: { replicas: 1 } });
        await kube.awaitReady([CONTROL], "5 minutes");
      });

      test("reaches main's compute awake while the storage controller's Postgres, neon-control's record, is down", async () => {
        await connected();
        const database = ref("StatefulSet", neonName("controller-db"), NAMESPACE);
        await kube.patch(database, { spec: { replicas: 0 } });
        try {
          const deadline = Date.now() + 120_000;
          while ((await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-controller-db" }) })).length > 0) {
            assert.ok(Date.now() < deadline, "the storage controller's Postgres did not stop");
            await sleep(1000);
          }
          // Past the proxy's ten seconds of a role's secret, so neon-control is asked for it again.
          await sleep(11_000);
          assert.ok((await connected()) < 60_000);
          // And a wake, which a running compute answers without the record.
          for (const pod of await controlReplicas()) {
            const woke = JSON.parse(await inside(pod, "GET", "http://127.0.0.1:8080/wake_compute?endpointish=alasio", "proxy")) as { aux?: { cold_start_info?: string } };
            assert.equal(woke.aux?.cold_start_info, "warm", pod);
          }
        } finally {
          await kube.patch(database, { spec: { replicas: 1 } });
          await kube.awaitReady([database], "5 minutes");
        }
      });

      test("is put to sleep by neon-control once idle as long as its Deployment says, a client connecting as it stops finding it awake again, and never two of it at once", async () => {
        await suspendingAfter(60);
        await connected();
        // Every compute pod whose containers may run, a stopping one too, counted all the while:
        // one deleted whose containers all ended is in a terminal phase until it is gone.
        let most = 0;
        let seen = "";
        let counting = true;
        const counted = (async () => {
          while (counting) {
            const pods = (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: COMPUTE_PODS }))
              .filter(({ status }) => status?.phase !== "Succeeded" && status?.phase !== "Failed");
            if (pods.length > most) seen = pods.map(({ metadata, status }) => `${metadata?.name} ${status?.phase}${metadata?.deletionTimestamp ? " deleted" : ""}`).join(", ");
            most = Math.max(most, pods.length);
            await sleep(200);
          }
        })();
        try {
          const deadline = Date.now() + 240_000;
          while ((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas !== 0) {
            assert.ok(Date.now() < deadline, "neon-control did not put the compute to sleep");
            await sleep(200);
          }
          // As it stops: a connection may yet reach the pod stopping, which ends it; asked again, as
          // alasio's pool asks, one gets through to the compute woken once that pod is gone.
          const asked = Date.now();
          while (!(await connected().then(() => true, () => false)) || (await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas !== 1) {
            assert.ok(Date.now() - asked < 120_000, "no connection got through to the compute woken again");
            await sleep(1000);
          }
        } finally {
          counting = false;
          await counted;
        }
        assert.equal(most, 1, `two compute pods ran at once: ${seen}`);
        assert.ok((await controlLogs()).some((line) => line.includes('"compute suspended"') && line.includes('"compute":"alasio"')));
      });

      test("keeps a compute awake while a client's idle session holds an advisory lock, and puts it to sleep once that ends", async () => {
        await suspendingAfter(60);
        const holder = new pg.Client({ connectionString: databaseUrl, connectionTimeoutMillis: 60_000 });
        holder.on("error", () => {});
        await holder.connect();
        try {
          await holder.query("select pg_advisory_lock(42)");
          // Idle well past its minute, and two of neon-control's rounds.
          await sleep(150_000);
          assert.equal((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas, 1);
        } finally {
          await holder.end();
        }
        const deadline = Date.now() + 240_000;
        while ((await kube.get<V1Deployment>(COMPUTE_DEPLOYMENT))?.spec?.replicas !== 0) {
          assert.ok(Date.now() < deadline, "neon-control did not put the compute to sleep once the session ended");
          await sleep(1000);
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

      test("keeps every entry exactly once through a pageserver killed mid-load", async () => {
        await new NeonSessionStore(pool).append(KEY, Array.from({ length: 20_000 }, (_, n) => entry(30_000 + n)));
        await restartLake();
        await sleep(1500);
        await kube.kill(NAMESPACE, await podOf("neon-pageserver"));
        await kube.awaitReady([ref("StatefulSet", neonName("pageserver"), NAMESPACE)], "5 minutes");
        const expected = await sourceCount();
        await untilLoaded(async () => {
          const { n, seqs } = await lakeCounts();
          return Number(n) === expected && Number(seqs) === expected;
        }, `held each of ${expected} entries exactly once`);
      });

      test("loads through main's read replica, which replays main's writes and takes none", async () => {
        const url = new URL(databaseUrl);
        url.searchParams.set("options", `endpoint=${replicaId(MAIN)}`);
        const replica = new pg.Client({ connectionString: url.toString(), connectionTimeoutMillis: 60_000 });
        replica.on("error", () => {});
        await replica.connect();
        try {
          assert.deepEqual((await replica.query("select pg_is_in_recovery() as recovering")).rows, [{ recovering: true }]);
          await query("create table replayed (id int primary key)");
          await query("insert into replayed values (1)");
          const deadline = Date.now() + 60_000;
          while (!(await replica.query("select count(*)::int as n from replayed").then(({ rows }) => rows[0]?.n === 1, () => false))) {
            assert.ok(Date.now() < deadline, "the replica did not replay main's write");
            await sleep(1000);
          }
          await assert.rejects(replica.query("insert into replayed values (2)"), /read-only transaction/u);
        } finally {
          await replica.end().catch(() => {});
        }
        // The lake's loader reads it: put to sleep, it is woken by the loader's next load, which loads all.
        const deployment = ref("Deployment", REPLICA_NAME, NAMESPACE);
        await kube.patch(deployment, { spec: { replicas: 0 } });
        const labels = (await kube.get<V1Deployment>(deployment))?.spec?.selector.matchLabels ?? {};
        const stopping = Date.now() + 120_000;
        while ((await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(labels) })).length > 0) {
          assert.ok(Date.now() < stopping, "the replica did not stop");
          await sleep(1000);
        }
        await new NeonSessionStore(pool).append(KEY, [entry(60_000)]);
        await restartLake();
        const expected = await sourceCount();
        await untilLoaded(async () => Number((await lakeCounts())["n"]) === expected, `held all ${expected} entries, read on the replica woken`);
        assert.equal((await kube.get<V1Deployment>(deployment))?.spec?.replicas, 1);
        // neon-control measures how far behind main it is, while both are awake: as it answers
        // alasio status, and as the replica that leads serves it at its next round.
        const control = await kube.runningPod(NAMESPACE, neonName("control"));
        const measuring = Date.now() + 120_000;
        for (;;) {
          const answered = await inside(control, "GET", "http://127.0.0.1:8080/computes", "admin");
          const lag = (JSON.parse(answered) as { computes: { id: string; lagBytes?: number | null }[] }).computes.find(({ id }) => id === replicaId(MAIN))?.lagBytes;
          const metrics = await inside(control, "GET", "http://127.0.0.1:8080/metrics", "admin");
          const served = metrics.split("\n").find((line) => line.startsWith(`neon_control_replica_lag_bytes{compute="${replicaId(MAIN)}"} `));
          if (typeof lag === "number" && served) {
            // A real distance, in bytes of WAL, far under what the alert fires at.
            for (const bytes of [lag, Number(served.split(" ")[1])]) assert.ok(Number.isInteger(bytes) && bytes >= 0 && bytes < 256 * 1024 * 1024, `${answered}\n${served}`);
            break;
          }
          assert.ok(Date.now() < measuring, `neon-control did not measure the replica's lag: ${answered}\n${served}`);
          await sleep(5000);
        }
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
