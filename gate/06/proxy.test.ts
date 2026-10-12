/**
 * 06's gate: Neon's proxy, from the pinned Neon image, before alasio's compute, on alasio
 * installed as its operator installs it (test/e2e/harness.ts), its control plane the
 * gate's stand-in for what 06 adds to neon-control (./cplane.ts):
 *
 * - every client names the endpoint by `options=endpoint=alasio`, without TLS:
 *   node-postgres (alasio), libpq (psql, pg_dump, DuckDB's postgres extension: the lake),
 *   and Grafana's database settings;
 * - every role's SCRAM verifier from one source (./verifiers.ts) lets its session through,
 *   and one Postgres salted itself does not;
 * - a cold wake, the compute at none, answers within alasio's 30 s connect timeout
 *   (src/neon/connect.ts), warm and with the pageserver just restarted.
 *
 * What it measures goes to GATE_OUT/results.json.
 *
 *   ALASIO_E2E_TARGET=docker GATE_OUT=/tmp/gate-06 node --test --test-timeout=7200000 gate/06/proxy.test.ts
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { promisify } from "node:util";
import { after, afterEach, before, describe, test } from "node:test";

import type { KubernetesObject, V1Deployment, V1Pod, V1StatefulSet } from "@kubernetes/client-node";
import pg from "pg";

import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import type { StackSecrets } from "../../neon/control/secrets.ts";
import { dumpClusterState, type Forward, kube, ref, setUp, tearDown } from "../../test/e2e/harness.ts";
import { ROLES, type Role, verifier } from "./verifiers.ts";

const run = promisify(execFile);
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const OUT = process.env["GATE_OUT"] ?? "/tmp/gate-06";
const ENDPOINT = "alasio";
const DATABASE = "alasio";
/** alasio's connect timeout (src/neon/connect.ts). */
const CONNECT_TIMEOUT_MS = 30_000;
const COMPUTE = "alasio-neon-compute";
const STACK = { "app.kubernetes.io/instance": RELEASE, "alasio.dev/stack": "neon" };

const results: Record<string, unknown> = {};
const record = (key: string, value: unknown) => {
  results[key] = value;
  console.error(`# ${key}: ${JSON.stringify(value)}`);
  mkdirSync(OUT, { recursive: true });
  writeFileSync(join(OUT, "results.json"), JSON.stringify(results, null, 2) + "\n");
};

let secrets: StackSecrets;
let proxy: Forward;

before(async () => {
  try {
    await setUp();
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 120 * 60_000 });

afterEach(async (t) => {
  if ("passed" in t && t.passed === false) await dumpClusterState();
});

after(async () => {
  proxy?.close();
  await tearDown();
}, { timeout: 30 * 60_000 });

/** The image the stack runs `workload`'s first container from. */
async function imageOf(kind: "Deployment" | "StatefulSet", name: string): Promise<string> {
  const workload = await kube.get<V1Deployment | V1StatefulSet>(ref(kind, name, NAMESPACE));
  const image = workload?.spec?.template.spec?.containers[0]?.image;
  assert.ok(image, `${name} has no image`);
  return image;
}

/** SQL as cloud_admin, in the compute's pod. */
async function asAdmin(sql: string): Promise<string> {
  const pod = await kube.runningPod(NAMESPACE, COMPUTE);
  return kube.execOk(NAMESPACE, pod, ["psql", "-h", "127.0.0.1", "-p", "55433", "-U", "cloud_admin", "-d", DATABASE, "-v", "ON_ERROR_STOP=1", "-Atc", sql], { container: "compute" });
}

/** Each role's password set as its verifier from the one source. */
async function setVerifiers(): Promise<void> {
  for (const role of Object.keys(ROLES) as Role[]) await asAdmin(`alter role ${role} with login password '${verifier(secrets, role)}'`);
}

/** A session through the proxy as `role`: who it is, and how long connecting took. */
async function through(role: Role, password = secrets[ROLES[role]]): Promise<{ user: string; ms: number }> {
  const client = new pg.Client({ host: "127.0.0.1", port: proxy.port, database: DATABASE, user: role, password, options: `endpoint=${ENDPOINT}`, connectionTimeoutMillis: CONNECT_TIMEOUT_MS });
  client.on("error", () => {});
  const started = Date.now();
  try {
    await client.connect();
    const { rows } = await client.query<{ user: string }>("select current_user as user");
    return { user: rows[0]?.user ?? "", ms: Date.now() - started };
  } finally {
    await client.end().catch(() => {});
  }
}

const computePods = () => kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-compute" }) });

/** The compute scaled to none, and its pod gone. */
async function sleepCompute(): Promise<void> {
  await kube.patch(ref("Deployment", COMPUTE, NAMESPACE), { spec: { replicas: 0 } });
  for (let i = 0; i < 300 && (await computePods()).length > 0; i++) await sleep(1000);
  assert.equal((await computePods()).length, 0, "the compute's pod is still there");
}

const median = (values: number[]) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)] ?? NaN;

describe("06's gate: Neon's proxy before alasio's compute", () => {
  before(async () => {
    secrets = JSON.parse(await kube.secret(NAMESPACE, "alasio-neon-root", "secrets.json")) as StackSecrets;
    const token = randomBytes(24).toString("base64url");
    const files = { "cplane.ts": "cplane.ts", "verifiers.ts": "verifiers.ts" };
    const labels = (app: string) => ({ ...STACK, app });
    const objects: KubernetesObject[] = [
      { apiVersion: "v1", kind: "Secret", metadata: { name: "gate-proxy", namespace: NAMESPACE }, stringData: { token } } as KubernetesObject,
      {
        apiVersion: "v1",
        kind: "ConfigMap",
        metadata: { name: "gate-cplane", namespace: NAMESPACE },
        data: Object.fromEntries(Object.entries(files).map(([key, file]) => [key, readFileSync(new URL(file, import.meta.url), "utf8")])),
      } as KubernetesObject,
      { apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "gate-cplane", namespace: NAMESPACE } },
      {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "Role",
        metadata: { name: "gate-cplane", namespace: NAMESPACE },
        rules: [
          { apiGroups: ["apps"], resources: ["deployments"], resourceNames: [COMPUTE], verbs: ["get"] },
          { apiGroups: ["apps"], resources: ["deployments/scale"], resourceNames: [COMPUTE], verbs: ["get", "patch"] },
        ],
      } as KubernetesObject,
      {
        apiVersion: "rbac.authorization.k8s.io/v1",
        kind: "RoleBinding",
        metadata: { name: "gate-cplane", namespace: NAMESPACE },
        roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "gate-cplane" },
        subjects: [{ kind: "ServiceAccount", name: "gate-cplane", namespace: NAMESPACE }],
      } as KubernetesObject,
      {
        apiVersion: "apps/v1",
        kind: "Deployment",
        metadata: { name: "gate-cplane", namespace: NAMESPACE },
        spec: {
          replicas: 1,
          selector: { matchLabels: { app: "gate-cplane" } },
          template: {
            metadata: { labels: labels("gate-cplane") },
            spec: {
              serviceAccountName: "gate-cplane",
              containers: [{
                name: "cplane",
                image: await imageOf("Deployment", "alasio-neon-control"),
                command: ["node", "/opt/alasio/gate/06/cplane.ts"],
                env: [{ name: "TOKEN", valueFrom: { secretKeyRef: { name: "gate-proxy", key: "token" } } }],
                readinessProbe: { httpGet: { path: "/healthz", port: 8080 }, periodSeconds: 2 },
                // Each file mounted by itself, not the ConfigMap's directory of links, which Node
                // would resolve imports from: their real paths are in a directory of its own.
                volumeMounts: [
                  ...Object.keys(files).map((file) => ({ name: "code", mountPath: `/opt/alasio/gate/06/${file}`, subPath: file })),
                  { name: "root", mountPath: "/secrets", readOnly: true },
                ],
              }],
              volumes: [{ name: "code", configMap: { name: "gate-cplane" } }, { name: "root", secret: { secretName: "alasio-neon-root" } }],
            },
          },
        },
      } as KubernetesObject,
      { apiVersion: "v1", kind: "Service", metadata: { name: "gate-cplane", namespace: NAMESPACE }, spec: { selector: { app: "gate-cplane" }, ports: [{ port: 8080 }] } } as KubernetesObject,
      {
        apiVersion: "apps/v1",
        kind: "Deployment",
        metadata: { name: "gate-proxy", namespace: NAMESPACE },
        spec: {
          replicas: 1,
          selector: { matchLabels: { app: "gate-proxy" } },
          template: {
            metadata: { labels: labels("gate-proxy") },
            spec: {
              containers: [{
                name: "proxy",
                image: await imageOf("StatefulSet", "alasio-neon-pageserver"),
                command: [
                  "/usr/local/bin/proxy",
                  "--proxy", "0.0.0.0:5432",
                  "--http", "0.0.0.0:7001",
                  "--auth-backend", "control-plane",
                  "--auth-endpoint", `http://gate-cplane.${NAMESPACE}.svc:8080`,
                  "--wake-compute-cache", "size=4000,ttl=4m",
                ],
                env: [{ name: "NEON_PROXY_TO_CONTROLPLANE_TOKEN", valueFrom: { secretKeyRef: { name: "gate-proxy", key: "token" } } }],
                ports: [{ name: "postgres", containerPort: 5432 }, { name: "http", containerPort: 7001 }],
                readinessProbe: { tcpSocket: { port: 5432 }, periodSeconds: 2 },
              }],
            },
          },
        },
      } as KubernetesObject,
      { apiVersion: "v1", kind: "Service", metadata: { name: "gate-proxy", namespace: NAMESPACE }, spec: { selector: { app: "gate-proxy" }, ports: [{ name: "postgres", port: 5432 }, { name: "http", port: 7001 }] } } as KubernetesObject,
    ];
    for (const object of objects) await kube.apply(object);
    try {
      await kube.awaitReady([ref("Deployment", "gate-cplane", NAMESPACE), ref("Deployment", "gate-proxy", NAMESPACE)], "5 minutes");
    } catch (error) {
      await dumpClusterState();
      throw error;
    }
    proxy = await kube.forward(NAMESPACE, "gate-proxy", 5432);
    await setVerifiers();
  }, { timeout: 30 * 60_000 });

  test("every role's session reaches the compute through the proxy, naming its endpoint by options, without TLS", async () => {
    const seen: Record<string, number> = {};
    for (const role of Object.keys(ROLES) as Role[]) {
      const { user, ms } = await through(role);
      assert.equal(user, role);
      seen[role] = ms;
    }
    record("node-postgres, each role, connect ms (warm)", seen);
  });

  test("a verifier Postgres salted itself does not let its role through, though its password is right", async () => {
    await asAdmin(`alter role lake with login password '${secrets.lakePassword.replaceAll("'", "''")}'`);
    try {
      const outcome = await through("lake").then(() => "connected", (error: Error) => `refused: ${error.message}`);
      record("lake, salted by Postgres", outcome);
      assert.match(outcome, /^refused/u);
    } finally {
      await setVerifiers();
    }
    assert.equal((await through("lake")).user, "lake");
  });

  test("libpq names the endpoint by options too: psql and pg_dump", async () => {
    const conninfo = `host=127.0.0.1 port=${proxy.port} dbname=${DATABASE} user=lake options=endpoint=${ENDPOINT} sslmode=disable connect_timeout=30`;
    const env = { ...process.env, PGPASSWORD: secrets.lakePassword };
    const psql = await run("psql", [conninfo, "-Atc", "select current_user"], { env });
    assert.equal(psql.stdout.trim(), "lake");
    const dump = await run("pg_dump", ["--schema-only", "--dbname", conninfo.replace("user=lake", "user=alasio")], { env: { ...process.env, PGPASSWORD: secrets.alasioPassword }, maxBuffer: 64 * 1024 * 1024 });
    assert.match(dump.stdout, /PostgreSQL database dump/u);
    record("psql and pg_dump through the proxy", "connected");
  });

  test("DuckDB's postgres extension, as the lake attaches alasio's database, names the endpoint by options", async () => {
    const conninfo = `host=127.0.0.1 port=${proxy.port} dbname=${DATABASE} user=lake password=${secrets.lakePassword} options=endpoint=${ENDPOINT} sslmode=disable`;
    // The lake's own DuckDB (neon/lake's @duckdb/node-api), as its service attaches.
    const code = [
      "const { DuckDBInstance } = await import('@duckdb/node-api');",
      "const connection = await (await DuckDBInstance.create()).connect();",
      "await connection.run('install postgres; load postgres');",
      `await connection.run(\`attach '${conninfo}' as alasio (type postgres, read_only)\`);`,
      "const read = await connection.runAndReadAll(\"select * from postgres_query('alasio', 'select current_user')\");",
      "console.log(JSON.stringify(read.getRows()));",
    ].join("\n");
    const { stdout } = await run("node", ["--input-type=module", "-e", code], { cwd: new URL("../../neon/lake/", import.meta.url).pathname });
    assert.match(stdout, /lake/u);
    record("DuckDB through the proxy", "connected");
  });

  test("Grafana reaches its database through the proxy: by a URL with options, or by the password's endpoint", async () => {
    const tried: Record<string, string> = {};
    // alasio-grafana's base (neon/grafana/Dockerfile): its database client is Grafana's own.
    const grafana = "grafana/grafana:13.2.3-ubuntu@sha256:efa8e935921096c08e4636c320d2f955cb843fa0889f471022e0d568af780d3f";
    const attempt = async (name: string, env: Record<string, string>) => {
      const container = `gate-grafana-${name}`;
      const args = ["run", "-d", "--name", container, "--network", "host", "-e", "GF_DATABASE_TYPE=postgres"];
      for (const [key, value] of Object.entries(env)) args.push("-e", `${key}=${value}`);
      await run("docker", [...args, grafana]);
      let logs = "";
      for (let i = 0; i < 90; i++) {
        const said = await run("docker", ["logs", container]).catch((error: { stdout?: string; stderr?: string }) => ({ stdout: error.stdout ?? "", stderr: error.stderr ?? "" }));
        logs = said.stdout + said.stderr;
        if (/HTTP Server Listen/u.test(logs)) break;
        if ((await run("docker", ["inspect", "-f", "{{.State.Running}}", container])).stdout.trim() !== "true") break;
        await sleep(2000);
      }
      await run("docker", ["rm", "-f", container]).catch(() => {});
      tried[name] = /HTTP Server Listen/u.test(logs) ? "connected" : `failed: ${logs.split("\n").filter((line) => /level=(error|crit)/u.test(line)).slice(0, 3).join(" | ")}`;
    };
    // Grafana's own database on the compute, beside alasio's (src/neon/grafana.ts).
    const password = encodeURIComponent(secrets.grafanaPassword);
    await attempt("url", { GF_DATABASE_URL: `postgres://grafana:${password}@127.0.0.1:${proxy.port}/grafana?options=endpoint%3D${ENDPOINT}&sslmode=disable` });
    await attempt("password", { GF_DATABASE_HOST: `127.0.0.1:${proxy.port}`, GF_DATABASE_NAME: "grafana", GF_DATABASE_USER: "grafana", GF_DATABASE_PASSWORD: `endpoint=${ENDPOINT};${secrets.grafanaPassword}`, GF_DATABASE_SSL_MODE: "disable" });
    record("Grafana through the proxy", tried);
    assert.ok(Object.values(tried).includes("connected"), JSON.stringify(tried));
  });

  test("a cold wake answers within alasio's 30 s connect timeout", async () => {
    const warm: number[] = [];
    for (let i = 0; i < 5; i++) warm.push((await through("lake")).ms);
    const cold: number[] = [];
    for (let i = 0; i < 5; i++) {
      await sleepCompute();
      cold.push((await through("lake")).ms);
    }
    record("connect ms, compute running (5)", { all: warm, median: median(warm) });
    record("connect ms, cold wake (5)", { all: cold, median: median(cold) });
    assert.ok(Math.max(...cold) < CONNECT_TIMEOUT_MS, `a cold wake took ${Math.max(...cold)} ms`);
  });

  test("a cold wake with the pageserver just restarted", async () => {
    const cold: Array<number | string> = [];
    for (let i = 0; i < 3; i++) {
      await sleepCompute();
      const [pageserver] = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf({ "app.kubernetes.io/instance": RELEASE, "app.kubernetes.io/component": "neon-pageserver" }) });
      assert.ok(pageserver?.metadata?.name);
      await kube.kill(NAMESPACE, pageserver.metadata.name);
      await sleep(2000);
      await kube.awaitReady([ref("StatefulSet", "alasio-neon-pageserver", NAMESPACE)], "5 minutes");
      cold.push(await through("lake").then(({ ms }) => ms, (error: Error) => `failed: ${error.message}`));
    }
    record("connect ms, cold wake, pageserver just restarted (3)", cold);
    const timings = (await kube.logs(NAMESPACE, await kube.runningPod(NAMESPACE, "gate-cplane"), "cplane")).split("\n").filter((line) => line.includes("\"woken\""));
    record("the control plane's wakes (scale, ready ms)", timings);
    assert.ok(cold.every((ms) => typeof ms === "number" && ms < CONNECT_TIMEOUT_MS), JSON.stringify(cold));
  });

  test("after a wake, alasio's role still lets its session through (the compute's spec sets its verifier)", async () => {
    const outcome = await through("alasio").then(({ user }) => `connected as ${user}`, (error: Error) => `refused: ${error.message}`);
    record("alasio's role after a wake", outcome);
  });
});
