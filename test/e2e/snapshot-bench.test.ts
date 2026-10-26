/**
 * The memory-snapshot benchmark's alasio system: alasio installed as the end-to-end runs install
 * it (./harness.ts, its `bench` shard), its sessions saved and restored through its own path,
 * each run of each case of bench/common/cases.json as the benchmark's design says (a save under
 * load, a save that stops the session, a warm restore, a cold one), each phase a line of
 * $BENCH_DIR/results.jsonl in the benchmark's result schema (bench/common/schema).
 *
 * - The fixtures are driven by the benchmark's driver (bench/common/dist/driver.mjs), run in
 *   alasio's pod, the only place the sandbox gateway is reached from, through the gateway's
 *   bayma endpoint, as alasio reaches a session: its first request after a release binds the
 *   session, which restores it.
 * - The save under load is the node agent's live checkpoint, asked of the agent as the gateway's
 *   keepLive asks it (src/gateway/snapshots.ts saveLive); the save that stops is the gateway's
 *   release (POST /release), as alasio's own.
 * - Between the warm restore and the cold one the session's workspace is put back as it was at
 *   the release (a JuiceFS clone through the node agent), its snapshot made current again
 *   (./bench-bucket.ts recommit) and its release record written again, and every node's cache of
 *   it removed, with the node's page cache.
 * - The emulated store profiles put toxiproxy in each node agent's network namespace, the only
 *   client of the snapshots bucket (bench/common/store/shape.ts), the agent's connections to the
 *   store's Service sent to it.
 * - The sandboxes' own logs (runsc's boot logs, `debug-log` added to each release's runsc
 *   options for the boot command alone) give the pause, the pre-copy rounds and the restore.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=bench BENCH_DIR=... BENCH_PROFILE=local node --test test/e2e/snapshot-bench.test.ts
 */
import { appendFileSync, mkdirSync, readFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { after, before, test } from "node:test";

import type { V1Deployment, V1Pod, V1Service } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, RELEASE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { GATEWAY as STORE_GATEWAY } from "../../cli/src/manifests/object-store.ts";
import { GATEWAY, GATEWAY_PORT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { BINDING_LABEL, FREE_LABEL, keyHash, POOL_LABEL } from "../../src/binder/names.ts";
import { hookOf, stateDirOf } from "../../src/gateway/snapshots.ts";
import { BAYMA_CONTAINER, RESTORE_ANNOTATION } from "../../src/kube/sandbox-shape.ts";
import type { Manifest } from "../../src/node-agent/agent.ts";
import { CONTAINERD, gvisorDirectory, gvisorHandler, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, RUNSC_ROOT, SNAPSHOT_CACHE_DIR } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import type { Held, Recommitted } from "./bench-bucket.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, inNodeAgent, kube, mainToken, nodeAgents, onNode, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const now = (): bigint => process.hrtime.bigint();
const msBetween = (from: bigint, to: bigint): number => Number(to - from) / 1e6;

function variable(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

const BENCH_DIR = variable("BENCH_DIR");
const PROFILE = variable("BENCH_PROFILE");
const RUNS = Number(process.env["BENCH_RUNS"] || 5);
/**
 * The variant: `as-shipped`, alasio 10.0.0 as it runs, whose live checkpoints of a pool's pod
 * fail (mountedStateDir), so its release is a whole stop-and-copy; or `with-fix`, its live
 * checkpoint given bayma's state directory by its mounted path, as alasio's fix, not yet
 * released, gives it, its release then a delta of the live save's image.
 */
const VARIANT = variable("BENCH_VARIANT");
if (VARIANT !== "as-shipped" && VARIANT !== "with-fix") throw new Error(`BENCH_VARIANT is ${VARIANT}, not as-shipped or with-fix`);
const COMMON = new URL("../../bench/common/", import.meta.url);
const read = (relative: string): string => readFileSync(new URL(relative, COMMON), "utf8");
const PROFILES = JSON.parse(read("store/profiles.json")) as Record<string, { readonly emulated: boolean }>;
const ALL_CASES = JSON.parse(read("cases.json")) as { readonly fixture: string; readonly size_mib: number }[];
const ASKED = (process.env["BENCH_CASES"] ?? "").split(/[\s,]+/u).filter(Boolean);
const CASES = ASKED.length === 0 ? ALL_CASES : ALL_CASES.filter(({ fixture, size_mib }) => ASKED.includes(`${fixture}/${size_mib}`));
const HOST = JSON.parse(readFileSync(join(BENCH_DIR, "host.json"), "utf8")) as { readonly copy_mib_s: number };
const RELEASE_UNDER_TEST = NODE_PINS.gvisor.releases[0]?.release ?? "";

/** Where the benchmark's bundles go in the pods that run them. */
const IN_POD = "/tmp/bench";
/** The gateway's bayma endpoint, as alasio's own client of it names it (src/sandbox/gateway-client.ts). */
const MCP_URL = `http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}/mcp`;
/** The pattern of the sandboxes' boot logs, on the node. */
const RUNSC_LOGS = "/var/log/runsc";
/** toxiproxy's release, as the runner systems' store runs it (bench/common/store/up.sh). */
const TOXIPROXY = "2.13.0";
/** The user toxiproxy runs as in an agent's network namespace, whose connections are not sent to it again. */
const PROXY_UID = 65534;

/** The metrics a result has, null unless measured (bench/common/schema/result.schema.json). */
const METRICS = ["save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "image_bytes", "uploaded_bytes", "first_exec_ms", "verify_ms", "resident_ms", "resident_how", "memory_peak_bytes", "memory_bytes", "workload_rss_bytes"] as const;
type Metrics = Partial<Record<(typeof METRICS)[number], number | string | null>>;
const NO_CHECKS = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

let store: Record<string, number> = {};

/** Appends one phase's result. */
function emit(fixture: string, sizeMib: number, run: number, phase: string, outcome: { readonly ok: boolean; readonly failure: string | null }, metrics: Metrics, correctness: Record<string, unknown> = NO_CHECKS, details: Record<string, unknown> = {}): void {
  const line = {
    schema: 1,
    system: "alasio",
    variant: VARIANT,
    target: TARGET,
    fixture,
    size_mib: sizeMib,
    profile: PROFILE,
    emulated: PROFILES[PROFILE]?.emulated ?? false,
    run,
    phase,
    ok: outcome.ok,
    failure: outcome.failure,
    metrics: Object.fromEntries(METRICS.map((name) => [name, metrics[name] ?? null])),
    correctness,
    host: JSON.parse(readFileSync(join(BENCH_DIR, "host.json"), "utf8")),
    store,
    versions: { system: `alasio 10.0.0, gVisor ${RELEASE_UNDER_TEST}`, bayma: "0.15.2", seaweedfs: "4.48", toxiproxy: TOXIPROXY },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "local", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details,
  };
  appendFileSync(join(BENCH_DIR, "results.jsonl"), `${JSON.stringify(line)}\n`);
}

const alasioPod = () => kube.runningPod(NAMESPACE, RELEASE);

/** Copies `file` of bench/common/dist into the container of the pod, as IN_POD/<file>. */
async function copyIn(pod: string, container: string, file: string): Promise<void> {
  await kube.execOk(NAMESPACE, pod, ["sh", "-c", `mkdir -p ${IN_POD} && cat > ${IN_POD}/${file}`], { container, stdin: read(`dist/${file}`) });
}

/** Runs the driver's `command` in alasio's pod, of the workspace `workspace`, through the gateway: what it printed. */
async function driver<T = Record<string, unknown>>(workspace: string, command: string, ...args: string[]): Promise<T> {
  const pod = await alasioPod();
  await copyIn(pod, "alasio", "driver.mjs");
  const token = await mainToken(workspace);
  const printed = await kube.execOk(NAMESPACE, pod, ["node", `${IN_POD}/driver.mjs`, command, "--url", MCP_URL, "--header", `Authorization: Bearer ${token}`, ...args], { container: "alasio" });
  return JSON.parse(printed.trim().split("\n").at(-1) ?? "null") as T;
}

/** Asks the node agent on `node` `method path`, as the gateway asks it: its status and what it answered. */
async function agentApi(node: string, method: string, path: string, body?: unknown): Promise<{ readonly status: number; readonly said: string }> {
  const pod = (await nodeAgents()).get(node);
  if (!pod) throw new Error(`no node agent runs ready on ${node}`);
  const printed = await kube.execOk(NAMESPACE, pod, [
    "sh",
    "-c",
    `curl -sS -w '\\n%{http_code}' -X "$1" -H "authorization: Bearer $${NODE_AGENT_TOKEN_VARIABLE}" -H 'content-type: application/json'${body === undefined ? "" : " --data-binary @-"} "http://127.0.0.1:${NODE_AGENT_PORT}$2"`,
    "sh",
    method,
    path,
  ], { container: "node-agent", ...(body === undefined ? {} : { stdin: JSON.stringify(body) }) });
  const lines = printed.trimEnd().split("\n");
  return { status: Number(lines.at(-1)), said: lines.slice(0, -1).join("\n") };
}

async function agentOk(node: string, method: string, path: string, body?: unknown): Promise<string> {
  const { status, said } = await agentApi(node, method, path, body);
  if (status < 200 || status >= 300) throw new Error(`the node agent of ${node} answered ${method} ${path} with ${status}: ${said}`);
  return said;
}

/**
 * The node agent's live checkpoint of the workspace's sandbox, asked in the agent's pod, as the
 * gateway asks it, timed there from its ask to its manifest committed: the time and the manifest.
 */
const CHECKPOINT = `
const [port, environment, workspace] = process.argv.slice(1);
const body = require("node:fs").readFileSync(0, "utf8");
const headers = { authorization: "Bearer " + process.env.${NODE_AGENT_TOKEN_VARIABLE}, "content-type": "application/json" };
const base = "http://127.0.0.1:" + port;
(async () => {
  const started = process.hrtime.bigint();
  const asked = await fetch(base + "/checkpoints", { method: "POST", headers, body });
  if (asked.status !== 202) throw new Error("POST /checkpoints answered " + asked.status + ": " + (await asked.text()));
  for (;;) {
    const standing = await (await fetch(base + "/checkpoints/" + environment + "/" + workspace, { headers })).json();
    if (standing.state === "done") return console.log(JSON.stringify({ ms: Number(process.hrtime.bigint() - started) / 1e6, manifest: standing.value }));
    if (standing.state === "failed") throw new Error(JSON.stringify(standing));
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
})().catch((error) => {
  console.error(error.stack ?? String(error));
  process.exit(1);
});`;

/** The gateway's release of the workspace, asked from alasio's pod, as alasio asks it, timed by curl: its status and time. */
async function release(workspace: string): Promise<{ readonly status: number; readonly ms: number; readonly said: string }> {
  const token = await mainToken(workspace);
  const printed = await kube.execOk(NAMESPACE, await alasioPod(), [
    "sh",
    "-c",
    `curl -sS -o /tmp/bench-release -w '%{http_code} %{time_total}\\n' -X POST -H "authorization: Bearer $1" "http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}/release"; cat /tmp/bench-release`,
    "sh",
    token,
  ], { container: "alasio" });
  const [first = "", ...said] = printed.split("\n");
  const [status = "0", seconds = "0"] = first.trim().split(" ");
  return { status: Number(status), ms: Number(seconds) * 1000, said: said.join("\n").trim() };
}

/** The pod `name` of the sessions' namespace, which must be there. */
async function podNamed(name: string): Promise<V1Pod> {
  const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
  if (!pod) throw new Error(`there is no pod ${SESSIONS}/${name}`);
  return pod;
}

const containerIdOf = (pod: V1Pod): string => pod.status?.containerStatuses?.find(({ name }) => name === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";

/** Waits until the pod `name` is gone. */
async function gone(name: string, timeoutMs = 180_000): Promise<void> {
  for (const deadline = Date.now() + timeoutMs; Date.now() < deadline; await sleep(500)) {
    if ((await kube.get(ref("Pod", name, SESSIONS))) === null) return;
  }
  throw new Error(`the pod ${SESSIONS}/${name} is still there after ${timeoutMs / 1000} s`);
}

/** The id of the pod sandbox of the pod `name`, as containerd names it, which its sandbox's boot log is under. */
async function sandboxIdOf(node: string, name: string): Promise<string> {
  return (await onNode(node, `k3s crictl pods --name '^${name}$' --namespace ${SESSIONS} -q | head -n 1`)).trim();
}

/** The boot log of the pod sandbox `sandboxId` on `node`: its lines. */
async function bootLog(node: string, sandboxId: string): Promise<string[]> {
  if (sandboxId === "") return [];
  return (await onNode(node, `cat ${RUNSC_LOGS}/${sandboxId}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
}

/** A Go duration, as runsc logs one (1.5s, 250ms, 1m2.5s, 800µs), in milliseconds. */
export function goDurationMs(text: string): number {
  const units: Record<string, number> = { h: 3_600_000, m: 60_000, s: 1000, ms: 1, "µs": 0.001, us: 0.001, ns: 0.000001 };
  let total = 0;
  for (const [, value = "0", unit = "s"] of text.matchAll(/([0-9.]+)(h|ms|m|s|µs|us|ns)/gu)) total += Number(value) * (units[unit] ?? 0);
  return total;
}

/** The time of day of a runsc log line (I1026 20:01:02.123456 ...), in milliseconds. */
function logTimeMs(line: string): number | null {
  const matched = /^[IWEF]\d{4} (\d{2}):(\d{2}):(\d{2}\.\d+)/u.exec(line);
  return matched ? (Number(matched[1]) * 3600 + Number(matched[2]) * 60 + Number(matched[3])) * 1000 : null;
}

/** What a sandbox's boot log says of its last save: its pause, and its pre-copy's rounds and bytes. */
function saveLogged(lines: readonly string[]): { readonly pauseMs: number | null; readonly passes: number | null; readonly precopyBytes: number | null; readonly said: string[] } {
  const paused = lines.findLast((line) => line.includes("Tasks resumed after save, paused for"));
  const done = lines.findLast((line) => line.includes("Pre-copy done after"));
  const skipped = lines.findLast((line) => line.includes("Pre-copy skipped"));
  const rounds = /Pre-copy done after (\d+) rounds \([^)]*\): (\d+) bytes/u.exec(done ?? "");
  return {
    pauseMs: paused ? goDurationMs(/paused for (\S+?)\.?$/u.exec(paused)?.[1] ?? "") : null,
    passes: rounds ? Number(rounds[1]) : skipped ? 0 : null,
    precopyBytes: rounds ? Number(rounds[2]) : skipped ? 0 : null,
    said: [paused, done, skipped].filter((line): line is string => line !== undefined),
  };
}

/** What a restored sandbox's boot log says: how long runsc took from its start to the restore's success, and its pages' loading. */
function restoreLogged(lines: readonly string[]): { readonly runscRestoreMs: number | null; readonly said: string[] } {
  const first = lines.find((line) => logTimeMs(line) !== null);
  const restored = lines.find((line) => line.includes("Restore successful"));
  const loaded = lines.find((line) => line.includes("Async page loading completed"));
  const from = first ? logTimeMs(first) : null;
  const to = restored ? logTimeMs(restored) : null;
  return { runscRestoreMs: from !== null && to !== null ? to - from : null, said: [restored, loaded].filter((line): line is string => line !== undefined) };
}

/** The memory of the pod of uid `uid` on `node`, as its cgroup says: its peak and what it holds now. */
async function podMemory(node: string, uid: string): Promise<{ readonly peak: number | null; readonly current: number | null; readonly stat: Record<string, number> }> {
  const printed = await onNode(node, [
    `dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name '*pod${uid}*' -o -name '*pod${uid.replaceAll("-", "_")}*' \\) | head -n 1)`,
    `[ -n "$dir" ] || exit 0`,
    `echo "peak $(cat "$dir/memory.peak")"`,
    `echo "current $(cat "$dir/memory.current")"`,
    `grep -E '^(anon|file) ' "$dir/memory.stat"`,
  ].join("; "));
  const field = (name: string) => {
    const value = printed.split("\n").find((line) => line.startsWith(`${name} `))?.split(" ")[1];
    return value === undefined ? null : Number(value);
  };
  return { peak: field("peak"), current: field("current"), stat: { anon: field("anon") ?? 0, file: field("file") ?? 0 } };
}

/**
 * Watches the restore of `workspace`: once its restore's pod has its bayma container, waits on
 * runsc for its restore to complete, every page of it loaded (`runsc wait --restore`), and says
 * when, with the pod.
 */
async function watchRestore(workspace: string, release: string): Promise<{ readonly residentNs: bigint | null; readonly pod: V1Pod; readonly error: string | null }> {
  const selector = `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}`;
  for (const deadline = Date.now() + 900_000; Date.now() < deadline; await sleep(50)) {
    const [pod] = (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: selector })).filter(({ metadata }) => !metadata?.deletionTimestamp && metadata?.name?.includes("-restore-"));
    const containerId = pod ? containerIdOf(pod) : "";
    if (!pod || containerId === "") continue;
    const node = pod.spec?.nodeName ?? "";
    const waited = await onNode(node, `${gvisorDirectory(release)}/runsc --root=${RUNSC_ROOT} wait --restore ${containerId}`).then(() => null, (error: unknown) => (error instanceof Error ? error.message : String(error)));
    return { residentNs: waited === null ? now() : null, pod, error: waited };
  }
  throw new Error(`no restore's pod of ${workspace} had its container within 900 s`);
}

/**
 * Where the restore into `pod` read its image, as the directory its pod restored from holds it
 * (src/node-agent/cache.ts prepare and prepareRemote): the image's files, linked from the node's
 * cache; or the checkpoint gofer's options alone, the bucket, which the agent removes once the
 * sandbox resumes, leaving the directory without the image's files.
 */
async function sourceOf(node: string, pod: V1Pod): Promise<"cache" | "bucket" | "unknown"> {
  const directory = pod.metadata?.annotations?.[RESTORE_ANNOTATION] ?? "";
  if (directory === "") return "unknown";
  const said = (await onNode(node, `if [ -e '${directory}/pages.img' ]; then echo cache; elif [ -d '${directory}' ]; then echo bucket; else echo unknown; fi`)).trim();
  return said === "cache" || said === "bucket" ? said : "unknown";
}

/** What the node agent of `node` and the sandbox gateway logged of `workspace`, timestamped: their last lines of it, which say where a restore spent its time. */
async function loggedOf(node: string, workspace: string): Promise<{ readonly agent: string[]; readonly gateway: string[] }> {
  const of = async (pod: string, container: string) =>
    (await kube.logs(NAMESPACE, pod, container, { timestamps: true }).catch(() => "")).split("\n").filter((line) => line.includes(workspace)).slice(-25).map((line) => line.slice(0, 400));
  return { agent: await of((await nodeAgents()).get(node) ?? "", "node-agent"), gateway: await of(await kube.runningPod(NAMESPACE, GATEWAY), "sandbox-gateway") };
}

/** What the agent of `node` logged of preparing the snapshot `snapshot`: its last line of it. */
async function preparedLine(node: string, snapshot: string): Promise<string> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const line = (await kube.logs(NAMESPACE, pod, "node-agent")).split("\n").findLast((each) => each.includes(`prepared the snapshot ${snapshot} `));
  return line ? line.slice(line.indexOf("prepared")) : "not logged";
}

/** A restore of the workspace from its snapshot `manifest` through the gateway, its first request the probe's: its result's measures, checks and details, and its pod. */
async function restore(workspace: string, sessionId: string, fixture: string, runId: string, manifest: Manifest) {
  const watching = watchRestore(workspace, manifest.release);
  const probed = await driver<{
    readonly t0_ns: string;
    readonly first_exec_ms: number;
    readonly verify_ms: number;
    readonly ok: boolean;
    readonly failure: string | null;
    readonly correctness: Record<string, unknown>;
    readonly workload_rss_bytes: number;
    readonly notice: string | null;
  }>(workspace, "probe", "--session", sessionId, "--fixture", fixture, "--run-id", runId);
  const { residentNs, pod, error } = await watching;
  const node = pod.spec?.nodeName ?? "";
  const name = pod.metadata?.name ?? "";
  const memory = await podMemory(node, pod.metadata?.uid ?? "");
  const logged = restoreLogged(await bootLog(node, await sandboxIdOf(node, name)));
  const prepared = await preparedLine(node, manifest.snapshot);
  const source = await sourceOf(node, pod);
  const logs = await loggedOf(node, workspace);
  const resident = residentNs === null ? null : msBetween(BigInt(probed.t0_ns), residentNs);
  const metrics: Metrics = {
    first_exec_ms: probed.first_exec_ms,
    verify_ms: probed.verify_ms,
    resident_ms: resident,
    resident_how: resident === null ? null : "signal",
    memory_peak_bytes: memory.peak,
    memory_bytes: memory.current,
    workload_rss_bytes: probed.workload_rss_bytes,
  };
  const details = { pod: name, node, source, prepared, runsc_restore_ms: logged.runscRestoreMs, logged: logged.said, memory_stat: memory.stat, resident_error: error, logs };
  return { probed, metrics, details, pod: name };
}

/** How long a restore may take to its first exec's answer, and to every page resident, from its t0 (the benchmark's design, section 5). */
const RESTORE_BOUND_MS = 300_000;

/** Whether a restore passed: its probe's checks, its image read from `source`, within RESTORE_BOUND_MS; or why not. */
function restoreOutcome(restored: Awaited<ReturnType<typeof restore>>, source: "cache" | "bucket"): { readonly ok: boolean; readonly failure: string | null } {
  const { probed, details, metrics } = restored;
  const resident = typeof metrics.resident_ms === "number" ? metrics.resident_ms : null;
  const failures = [
    ...(probed.failure === null ? [] : [probed.failure]),
    ...(details.source === source ? [] : [`restored from the ${details.source}, not the ${source === "cache" ? "node's cache" : "bucket"}: ${details.prepared}`]),
    ...(probed.first_exec_ms <= RESTORE_BOUND_MS ? [] : [`its first exec answered ${Math.round(probed.first_exec_ms / 1000)} s after its restore was asked, past ${RESTORE_BOUND_MS / 1000} s`]),
    ...(resident === null ? [`its pages were not told resident: ${details.resident_error ?? "runsc did not say"}`] : resident <= RESTORE_BOUND_MS ? [] : [`its pages were all resident ${Math.round(resident / 1000)} s after its restore was asked, past ${RESTORE_BOUND_MS / 1000} s`]),
  ];
  return { ok: probed.ok && failures.length === 0, failure: failures.length === 0 ? null : failures.join("; ") };
}

/** Ends the restored pod `name` without a save, as a crash ends it, and waits until it is gone. */
async function endUnsaved(name: string): Promise<void> {
  await kube.kill(SESSIONS, name);
  await gone(name);
}

/** Drops the page cache of `node`, as every restore starts with it dropped, so what ran before leaves it nothing. */
const dropPageCache = (node: string): Promise<string> => onNode(node, "sync && echo 1 > /proc/sys/vm/drop_caches");

/** Removes every node's cache of the workspace, and the nodes' page cache, so a restore reads it from the bucket. */
async function coldCaches(workspace: string): Promise<void> {
  for (const [node, pod] of await nodeAgents()) {
    await kube.execOk(NAMESPACE, pod, ["rm", "-rf", `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}`], { container: "node-agent" });
    await dropPageCache(node);
  }
}

/**
 * Has the store's masters compact every volume of which a hundredth is deleted, between runs, as
 * the runner systems' harnesses do (bench/common/store/up.sh): a run's snapshots, forgotten with
 * its workspace, free their disk only once compacted, and the next runs' saves need it.
 */
async function vacuum(): Promise<void> {
  const [master] = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("seaweedfs-master")) });
  if (!master?.metadata?.name) throw new Error("no master of the store runs");
  await kube.execOk(NAMESPACE, master.metadata.name, ["wget", "-q", "-O", "-", "http://127.0.0.1:9333/vol/vacuum?garbageThreshold=0.01"], { container: "master" });
}

/** The bytes the workspace's snapshots hold in the bucket. */
const bucketBytes = async (node: string, workspace: string): Promise<number> => (await inNodeAgent<Held>(node, "./bench-bucket.ts", "bytes", SESSIONS, workspace)).bytes;

/** bayma's state directory `stateDir`, under /home/agent, by the path of the volume the image's /home/agent links into (sandbox/agent/Dockerfile). */
const mountedStateDir = (stateDir: string): string => (stateDir.startsWith("/home/agent/") ? `/attach/ws/home/${stateDir.slice("/home/agent/".length)}` : stateDir);

/** The Deployment of the pool the pod is of, whose template the gateway records with a snapshot (src/gateway/snapshots.ts take). */
async function poolTemplate(pod: V1Pod): Promise<Record<string, unknown>> {
  const pool = pod.metadata?.labels?.[POOL_LABEL] ?? "";
  const deployment = await kube.get<V1Deployment>(ref("Deployment", pool, SESSIONS));
  if (!deployment) throw new Error(`the pool ${pool} of ${pod.metadata?.name} has no Deployment`);
  return { ...deployment.spec?.template } as Record<string, unknown>;
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** The job's ticker check failed: none of its measures is written. */
class Uncalibrated extends Error {}

/** Whether the job's ticker check has passed (calibrate). */
let calibrated = false;

/**
 * The job's check of the fixtures' ticker (the benchmark's design, section 3): the sandbox of the
 * pod `name`, its container `containerId`, paused for a second by runsc and resumed, the gap the
 * ticker saw within what the harness saw of the freeze, 20 ms either side. Written to
 * logs/calibration.json; the job fails where it is not.
 */
async function calibrate(workspace: string, sessionId: string, fixture: string, node: string, containerId: string): Promise<void> {
  const runsc = `${gvisorDirectory(RELEASE_UNDER_TEST)}/runsc --root=${RUNSC_ROOT}`;
  await driver(workspace, "mark", "--session", sessionId, "--fixture", fixture);
  const pauseStart = now();
  await onNode(node, `${runsc} pause ${containerId}`);
  const pauseEnd = now();
  await sleep(1000);
  const resumeStart = now();
  await onNode(node, `${runsc} resume ${containerId}`);
  const resumeEnd = now();
  const { max_gap_ms: gap } = await driver<{ readonly max_gap_ms: number }>(workspace, "gaps", "--session", sessionId, "--fixture", fixture);
  const least = msBetween(pauseEnd, resumeStart) - 20;
  const most = msBetween(pauseStart, resumeEnd) + 20;
  const seen = { gap_ms: gap, least_ms: least, most_ms: most, ok: gap >= least && gap <= most };
  appendFileSync(join(BENCH_DIR, "logs", "calibration.json"), `${JSON.stringify(seen)}\n`);
  if (!seen.ok) throw new Uncalibrated(`the ticker saw a gap of ${gap} ms of a freeze of ${least + 20} to ${most - 20} ms: its measures are not to be trusted`);
  calibrated = true;
}

/** One run of a case, every phase of it written, as the module says; what fails is written as failed, and the run goes on to its cleanup. */
async function runOnce(fixture: string, sizeMib: number, run: number): Promise<void> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const copy = `${workspace}-copy`;
  const runId = `${process.env["GITHUB_RUN_ID"] ?? "local"}-${PROFILE}-${fixture}-${sizeMib}-${run}`;
  const phases = ["save-live", "save-stop", "restore-warm", "restore-cold"];
  let phase = "setup";
  const done = new Set<string>();
  let node = "";
  try {
    const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
    if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
    await bindSession(workspace);
    const setup = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", fixture, "--size-mib", String(sizeMib), "--run-id", runId, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
    const sessionId = setup.session_id;

    phase = "save-live";
    const bound = await boundPodOf(workspace);
    if (!bound) throw new Error(`no pod is bound to ${workspace}`);
    node = bound.node;
    const pod = await podNamed(bound.name);
    const bayma = pod.spec?.containers.find(({ name }) => name === BAYMA_CONTAINER);
    const hook = bayma ? hookOf(bayma) : null;
    const stateDir = bayma ? stateDirOf(bayma) : null;
    if (!hook || !stateDir) throw new Error(`the pod ${bound.name} runs no bayma with a state directory`);
    if (!calibrated) await calibrate(workspace, sessionId, fixture, node, containerIdOf(pod));
    // bayma's state directory as the pool's pods give it, under /home/agent, which the image links to /attach/ws/home: held
    // by no volume of the pod's spec, so the agent, which reads it by the spec's mounts alone (src/node-agent/volume.ts
    // hostPathOf), fails every live checkpoint of a pool's pod as shipped: "no volume of its pod holds
    // /home/agent/.bayma/last-save.json" (CI run 40396803554). With the fix, the same directory by the path that is mounted.
    const request = { environment: SESSIONS, workspace, namespace: SESSIONS, pod: bound.name, containerId: containerIdOf(pod), hook, podTemplate: await poolTemplate(pod), live: { stateDir: VARIANT === "with-fix" ? mountedStateDir(stateDir) : stateDir } };
    const sandboxId = await sandboxIdOf(node, bound.name);
    const heldBefore = await bucketBytes(node, workspace);
    await driver(workspace, "mark", "--session", sessionId, "--fixture", fixture);
    const agent = (await nodeAgents()).get(node) ?? "";
    const checkpointed = await kube.exec(NAMESPACE, agent, ["node", "-e", CHECKPOINT, String(NODE_AGENT_PORT), SESSIONS, workspace], { container: "node-agent", stdin: JSON.stringify(request) });
    const gaps = await driver<{ readonly max_gap_ms: number; readonly slowdown: number | null }>(workspace, "gaps", "--session", sessionId, "--fixture", fixture);
    const heldLive = await bucketBytes(node, workspace);
    const liveLogged = saveLogged(await bootLog(node, sandboxId));
    const logged = { state_dir: request.live.stateDir, logged: liveLogged.said, gaps };
    const measured = { unavailable_ms: gaps.max_gap_ms, pause_reported_ms: liveLogged.pauseMs, slowdown: gaps.slowdown, precopy_passes: liveLogged.passes, precopy_bytes: liveLogged.precopyBytes, uploaded_bytes: heldLive - heldBefore };
    if (checkpointed.code === 0) {
      const live = JSON.parse(checkpointed.stdout.trim().split("\n").at(-1) ?? "{}") as { readonly ms: number; readonly manifest: Manifest };
      emit(fixture, sizeMib, run, "save-live", { ok: true, failure: null }, {
        ...measured,
        save_total_ms: live.ms,
        image_bytes: live.manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null,
      }, NO_CHECKS, { ...logged, snapshot: live.manifest.snapshot, runsc_checkpoint_ms: live.manifest.pauseMs, upload_ms: live.manifest.uploadMs, chain: live.manifest.layers.length });
    } else {
      // As alasio 10.0.0 fails it (mountedStateDir): its sandbox saved and running on, the snapshot never committed.
      const said = /"error":"([^"]*)"/u.exec(checkpointed.stderr)?.[1] ?? checkpointed.stderr.trim().split("\n").at(-1) ?? "";
      emit(fixture, sizeMib, run, "save-live", { ok: false, failure: `the node agent's live checkpoint failed: ${said}` }, measured, NO_CHECKS, logged);
    }
    done.add(phase);

    phase = "save-stop";
    const released = await release(workspace);
    if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release answered ${released.status}: ${released.said}`);
    const manifest = JSON.parse(await agentOk(node, "GET", `/snapshots/${SESSIONS}/${workspace}`)) as Manifest;
    const record = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/released`)) as { readonly snapshot: string; readonly release: string };
    if (record.snapshot !== manifest.snapshot) throw new Error(`${workspace} was released into ${record.snapshot}, not its current snapshot ${manifest.snapshot}`);
    const heldStop = await bucketBytes(node, workspace);
    const stopLogged = saveLogged(await bootLog(node, sandboxId));
    emit(fixture, sizeMib, run, "save-stop", { ok: true, failure: null }, {
      save_total_ms: released.ms,
      pause_reported_ms: stopLogged.pauseMs,
      precopy_passes: 0,
      precopy_bytes: 0,
      image_bytes: manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null,
      uploaded_bytes: heldStop - heldLive,
    }, NO_CHECKS, { snapshot: manifest.snapshot, runsc_checkpoint_ms: manifest.pauseMs, upload_ms: manifest.uploadMs, chain: manifest.layers.length, bytes: manifest.bytes, logged: stopLogged.said });
    done.add(phase);
    await gone(bound.name);
    // The workspace as the release left it, for the cold restore's.
    const quota = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/quota`)) as { readonly capacityBytes: number };
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace }, to: { environment: SESSIONS, workspace: copy }, quotaBytes: quota.capacityBytes });

    phase = "restore-warm";
    await dropPageCache(node);
    const warm = await restore(workspace, sessionId, fixture, runId, manifest);
    emit(fixture, sizeMib, run, "restore-warm", restoreOutcome(warm, "cache"), warm.metrics, warm.probed.correctness, warm.details);
    done.add(phase);
    await endUnsaved(warm.pod);

    phase = "restore-cold";
    await agentOk(node, "DELETE", `/workspaces/${SESSIONS}/${workspace}`);
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace: copy }, to: { environment: SESSIONS, workspace }, quotaBytes: quota.capacityBytes });
    const recommitted = await inNodeAgent<Recommitted>(node, "./bench-bucket.ts", "recommit", SESSIONS, workspace, manifest.snapshot);
    await agentOk(node, "PUT", `/workspaces/${SESSIONS}/${workspace}/released`, { snapshot: recommitted.snapshot, release: recommitted.release });
    await coldCaches(workspace);
    const cold = await restore(workspace, sessionId, fixture, runId, manifest);
    emit(fixture, sizeMib, run, "restore-cold", restoreOutcome(cold, "bucket"), cold.metrics, cold.probed.correctness, cold.details);
    done.add(phase);
    await endUnsaved(cold.pod);
  } catch (error) {
    if (error instanceof Uncalibrated) throw error;
    const why = `${phase}: ${errorText(error)}`;
    console.error(`# ${fixture} ${sizeMib} MiB run ${run} failed at ${why}`);
    for (const each of phases.filter((name) => !done.has(name))) emit(fixture, sizeMib, run, each, { ok: false, failure: each === phase ? why : `not run: ${why}` }, {});
    const bound = await boundPodOf(workspace).catch(() => null);
    if (bound) await endUnsaved(bound.name).catch(() => {});
  } finally {
    // Its snapshots forgotten with it, in the bucket and every node's cache, and the copy of its files.
    const removed = await gatewayCall(await mainToken(workspace), "DELETE", "/workspace").catch((error: unknown) => ({ status: 0, said: errorText(error) }));
    if (removed.status !== 204) console.error(`# ${workspace} was not removed: ${removed.status} ${removed.said}`);
    if (node !== "") await agentApi(node, "DELETE", `/workspaces/${SESSIONS}/${copy}`).catch(() => {});
    await vacuum().catch((error: unknown) => console.error(`# the store was not compacted after ${workspace}: ${errorText(error)}`));
  }
}

/**
 * Adds a boot log to each kept release's runsc options, for the boot command alone, which the
 * sandboxes' pause, pre-copy and restore are read from, and has every pool's free pods made
 * again, so that each runs with it.
 */
async function bootLogs(node: string): Promise<void> {
  for (const { release } of NODE_PINS.gvisor.releases) {
    const options = `${CONTAINERD}/${gvisorHandler(release)}.toml`;
    // [runsc_config] is the options' last table (cli/src/cluster/k3s.ts).
    await onNode(node, `grep -q '^  debug-log' ${options} || printf '  debug-log = "${RUNSC_LOGS}/%%ID%%/"\\n  debug-command = "boot"\\n' >> ${options}; tail -n 4 ${options}`);
  }
  const free = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` });
  for (const pod of free) await kube.remove(ref("Pod", pod.metadata?.name ?? "", SESSIONS));
  for (const pod of free) await gone(pod.metadata?.name ?? "");
}

/** The process of the node agent on `node`, in the node's PID namespace, as its runtime says. */
async function agentPid(node: string): Promise<string> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const agentId = (await kube.get<V1Pod>(ref("Pod", pod, NAMESPACE)))?.status?.containerStatuses?.find(({ name }) => name === "node-agent")?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";
  return (await onNode(node, `k3s crictl inspect --output go-template --template '{{.info.pid}}' ${agentId}`)).trim();
}

/** The address of the store's Service, which the node agents reach the bucket at. */
async function storeAddress(): Promise<string> {
  const address = (await kube.get<V1Service>(ref("Service", STORE_GATEWAY, NAMESPACE)))?.spec?.clusterIP;
  if (!address) throw new Error(`the store's Service ${STORE_GATEWAY} has no address`);
  return address;
}

/**
 * How the store answers through the profile, as the node agent on `node` reaches it: the probe
 * run in the agent's network namespace, as the agent's own client of the bucket, its key and
 * bucket, from the node, as the agent's pod has no room for its 256 MiB.
 */
async function probeStore(node: string): Promise<Record<string, number>> {
  const pid = await agentPid(node);
  const environment = (await onNode(node, `tr '\\0' '\\n' < /proc/${pid}/environ`)).split("\n");
  const of = (name: string) => environment.find((line) => line.startsWith(`${name}=`))?.slice(name.length + 1) ?? "";
  const given = { S3_ENDPOINT: `http://${await storeAddress()}:8333`, S3_REGION: of("S3_REGION"), S3_BUCKET: of("S3_BUCKET"), AWS_ACCESS_KEY_ID: of("AWS_ACCESS_KEY_ID"), AWS_SECRET_ACCESS_KEY: of("AWS_SECRET_ACCESS_KEY") };
  const assignments = Object.entries(given).map(([name, value]) => `${name}='${value}'`).join(" ");
  const printed = await onNode(node, `nsenter --target ${pid} --net env ${assignments} ${process.execPath} ${new URL("dist/probe.mjs", COMMON).pathname} --prefix bench-probe/`);
  return JSON.parse(printed.trim().split("\n").at(-1) ?? "{}") as Record<string, number>;
}

/**
 * Puts the profile's toxics between each node agent and the store: toxiproxy in the agent's
 * network namespace, as an unprivileged user, its proxy to the store's Service, and the agent's
 * connections to the Service sent to it; then the agent started again, so it makes them anew.
 */
async function shapeAgents(): Promise<void> {
  if (!PROFILES[PROFILE]?.emulated) return;
  if (TARGET !== "host") throw new Error("the emulated profiles shape the node agents' traffic on the host target alone");
  const address = await storeAddress();
  const server = join(BENCH_DIR, "toxiproxy-server");
  const shape = new URL("store/shape.ts", COMMON).pathname;
  for (const node of (await nodeAgents()).keys()) {
    await onNode(node, `[ -x ${server} ] || { curl -fsSL -o ${server} https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY}/toxiproxy-server-linux-amd64 && chmod +x ${server}; }`);
    const pid = await agentPid(node);
    const inNetns = `nsenter --target ${pid} --net`;
    await onNode(node, `${inNetns} setpriv --reuid=${PROXY_UID} --regid=${PROXY_UID} --clear-groups ${server} -host 127.0.0.1 -port 8474 >${BENCH_DIR}/logs/toxiproxy-${node}.log 2>&1 &`);
    await onNode(node, `for _ in $(seq 50); do ${inNetns} curl -fsS http://127.0.0.1:8474/version && exit 0; sleep 0.2; done; exit 1`);
    await onNode(node, `${inNetns} ${process.execPath} ${shape} --api http://127.0.0.1:8474 --profile ${PROFILE} --listen 127.0.0.1:28333 --upstream ${address}:8333`);
    await onNode(node, `${inNetns} iptables -t nat -A OUTPUT -p tcp -d ${address} --dport 8333 -m owner ! --uid-owner ${PROXY_UID} -j DNAT --to-destination 127.0.0.1:28333`);
    // Started again in its pod, whose network namespace, the rule and the proxy in it, stays.
    await onNode(node, `kill ${pid}`);
  }
  for (const deadline = Date.now() + 300_000; ; await sleep(2000)) {
    const agents = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
    const restarted = agents.every(({ status }) => (status?.containerStatuses?.find(({ name }) => name === "node-agent")?.restartCount ?? 0) > 0);
    if (restarted && (await nodeAgents()).size === agents.length) return;
    if (Date.now() > deadline) throw new Error("the node agents did not come back within 300 s of their restart");
  }
}

before(async () => {
  try {
    mkdirSync(join(BENCH_DIR, "logs"), { recursive: true });
    await setUp();
    const agents = await nodeAgents();
    for (const node of agents.keys()) await bootLogs(node);
    await shapeAgents();
    // How the store answers through the profile, from where the saves run: the node agent's network.
    const [node = ""] = (await nodeAgents()).keys();
    store = await probeStore(node);
    console.error(`# the store through ${PROFILE}: ${JSON.stringify(store)}`);
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 150 * 60_000 });

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

test(`alasio's sessions saved and restored, ${RUNS} runs of ${CASES.length} cases, through ${PROFILE}`, { timeout: 330 * 60_000 }, async () => {
  for (let run = 1; run <= RUNS; run++) {
    for (const { fixture, size_mib } of CASES) {
      const started = Date.now();
      await runOnce(fixture, size_mib, run);
      console.error(`# ${fixture} ${size_mib} MiB run ${run} took ${Math.round((Date.now() - started) / 1000)} s`);
    }
  }
});
