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
 *   (its manifest put back as current.json) and its release record written again, and every node's cache of
 *   it removed, with the node's page cache.
 * - The emulated store profiles put toxiproxy in each node agent's network namespace, the only
 *   client of the snapshots bucket (bench/common/store/shape.ts), the agent's connections to the
 *   store's Service sent to it.
 * - The sandboxes' own logs (runsc's boot logs, `debug-log` added to each release's runsc
 *   options for the boot command alone) give the pause, the pre-copy rounds and the restore.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=bench BENCH_DIR=... BENCH_PROFILE=local node --test test/e2e/snapshot-bench.test.ts
 */
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { dirname, join } from "node:path";
import { after, before, test } from "node:test";

import type { V1Deployment, V1Pod, V1Service } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, RELEASE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { GATEWAY as STORE_GATEWAY } from "../../cli/src/manifests/object-store.ts";
import { GATEWAY, GATEWAY_PORT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { BINDING_LABEL, FREE_LABEL, keyHash, leaseName, POOL_LABEL } from "../../src/binder/names.ts";
import { hookOf, stateDirOf } from "../../src/gateway/snapshots.ts";
import { LAYERS_DIR } from "../../src/node-agent/cache.ts";
import { BAYMA_CONTAINER, RESTORE_ANNOTATION } from "../../src/kube/sandbox-shape.ts";
import { compactionDue, type Manifest } from "../../src/node-agent/agent.ts";
import { CONTAINERD, gvisorDirectory, gvisorHandler, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, RUNSC_ROOT, SNAPSHOT_CACHE_DIR } from "../../src/node-agent/names.ts";
import { runscFlags } from "../../src/node-agent/runsc.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, kube, lakeQuery, mainToken, nodeAgents, onNode, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";

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
 * The variant: `as-shipped`, alasio as it runs, its live checkpoints given bayma's state
 * directory as the pool's pods name it, its release a delta of the live save's image, and the
 * node's page cache dropped before every restore; `kept-cache`, the same, but the node's page
 * cache left as it is for the warm restore, as a node running other sandboxes keeps the sandbox
 * image's files; `with-fix`, its live checkpoint given bayma's state directory by its mounted
 * path; or `whole`, no live save taken, as of a session released before its first, its release a
 * whole stop-and-copy.
 */
const VARIANT = variable("BENCH_VARIANT");
/**
 * Spike: as-shipped, but `no-compaction` kills the release's compaction of the chain as it starts,
 * `settled` waits until it ends before the warm restore, and `kept-cache` leaves the node's page
 * cache as it is for the warm restore.
 */
const SPIKE_VARIANTS = ["no-compaction", "settled", "kept-cache", "mixed", "mixed-iocost", "cold-by-hand"];
/** Spike: `chain-N`, as-shipped but N - 1 live saves taken, each a delta of the one before, so that the release's delta makes a chain of N images. */
const CHAIN = /^chain-([0-9]+)$/u.exec(VARIANT);
if (VARIANT !== "as-shipped" && VARIANT !== "with-fix" && VARIANT !== "whole" && !SPIKE_VARIANTS.includes(VARIANT) && !CHAIN) throw new Error(`BENCH_VARIANT is ${VARIANT}, not as-shipped, with-fix or whole`);
/** The live saves a run takes before its release. */
const LIVE_SAVES = CHAIN ? Number(CHAIN[1]) - 1 : 1;
const COMMON = new URL("../../bench/common/", import.meta.url);
const read = (relative: string): string => readFileSync(new URL(relative, COMMON), "utf8");
const PROFILES = JSON.parse(read("store/profiles.json")) as Record<string, { readonly emulated: boolean }>;
const ALL_CASES = JSON.parse(read("cases.json")) as { readonly fixture: string; readonly size_mib: number }[];
const ASKED = (process.env["BENCH_CASES"] ?? "").split(/[\s,]+/u).filter(Boolean);
/** The cases asked, as fixture/MiB, of the benchmark's or not (a deep chain's idle/1024); every case of the benchmark's where none is. */
const CASES = ASKED.length === 0 ? ALL_CASES : ASKED.map((asked) => {
  const [fixture = "", size = ""] = asked.split("/");
  if (!ALL_CASES.some((each) => each.fixture === fixture) || !/^[1-9][0-9]*$/u.test(size)) throw new Error(`BENCH_CASES asks for ${asked}, no fixture/MiB of the benchmark's fixtures`);
  return { fixture, size_mib: Number(size) };
});
const HOST = JSON.parse(readFileSync(join(BENCH_DIR, "host.json"), "utf8")) as { readonly copy_mib_s: number };
const RELEASE_UNDER_TEST = NODE_PINS.gvisor.releases[0]?.release ?? "";
/** The release of alasio under test, and of the bayma its sessions run (sandbox/agent/Dockerfile's BAYMA_IMAGE). */
const ALASIO_UNDER_TEST = (JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as { readonly version: string }).version;
const BAYMA_UNDER_TEST = /^ARG BAYMA_IMAGE=\S+:([0-9][^@\s]*)/mu.exec(readFileSync(new URL("../../sandbox/agent/Dockerfile", import.meta.url), "utf8"))?.[1] ?? "";
/** Spike: runsc flags for the release under test, as BENCH_RUNSC_CONFIG names them: state-engine:wire, say. */
const RUNSC_CONFIG = (process.env["BENCH_RUNSC_CONFIG"] ?? "").split(",").filter(Boolean);
for (const word of RUNSC_CONFIG) if (!/^[a-z][a-z0-9-]*:[A-Za-z0-9_./-]+$/u.test(word)) throw new Error(`BENCH_RUNSC_CONFIG's ${word} is no flag:value`);
/** Whether the node's page cache is dropped before the warm restore: kept by `kept-cache` alone. */
const PAGE_CACHE = VARIANT === "kept-cache" ? "kept" : "dropped";

/** Where the benchmark's bundles go in the pods that run them. */
const IN_POD = "/tmp/bench";
/** The gateway's bayma endpoint, as alasio's own client of it names it (src/sandbox/gateway-client.ts). */
const MCP_URL = `http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}/mcp`;
/** The pattern of the sandboxes' boot logs, on the node. */
const RUNSC_LOGS = "/var/log/runsc";
/** The CRI client on a node: k3s's own on the host target; the docker target's node image's, as k3s there has no crictl command. */
const CRICTL = TARGET === "host" ? "k3s crictl" : "crictl";
/** Spike: the chain of rules, in each node agent's network namespace, that counts its traffic with the store (storeBytes). */
const STORE_CHAIN = "BENCH-STORE";
/** toxiproxy's release, as the runner systems' store runs it (bench/common/store/up.sh). */
const TOXIPROXY = "2.13.0";
/** The user toxiproxy runs as in an agent's network namespace, whose connections are not sent to it again. */
const PROXY_UID = 65534;

/** The metrics a result has, null unless measured (bench/common/schema/result.schema.json). */
const METRICS = [
  "save_total_ms", "unavailable_ms", "pause_reported_ms", "slowdown", "precopy_passes", "precopy_bytes", "image_bytes", "uploaded_bytes", "first_exec_ms", "verify_ms", "resident_ms", "resident_how", "memory_peak_bytes", "memory_bytes", "workload_rss_bytes",
  // Schema 2: the mechanism's own splits, from runsc's boot log; the product's, from the trace lines; what moved, from the node's counters.
  "kernel_save_ms", "memory_save_ms", "overall_save_ms", "durable_store_ms", "store_sent_bytes", "uploaded_over_needed",
  "restore_kernel_load_ms", "restore_memory_load_ms", "restore_to_running_ms", "runsc_resume_ms", "policy_wait_ms", "probe_exec_ms", "orchestration_ms",
  "loaded_bytes", "store_received_bytes", "disk_read_bytes", "read_amplification", "page_cache", "compaction_at_t0",
] as const;
type Metrics = Partial<Record<(typeof METRICS)[number], number | string | null>>;
const NO_CHECKS = { marker: null, memory: null, children: null, file: null, socket: null, running: null, notice: null };

let store: Record<string, number> = {};
/** Spike: when the last restore's probe began, on the wall clock, whose bayma span the lake is waited on for. */
let lastRestoreAt = 0;

/** Appends one phase's result. */
function emit(fixture: string, sizeMib: number, run: number, phase: string, outcome: { readonly ok: boolean; readonly failure: string | null }, metrics: Metrics, correctness: Record<string, unknown> = NO_CHECKS, details: Record<string, unknown> = {}): void {
  const line = {
    schema: 2,
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
    versions: { system: `alasio ${ALASIO_UNDER_TEST}, gVisor ${RELEASE_UNDER_TEST}`, bayma: BAYMA_UNDER_TEST, seaweedfs: "4.48", toxiproxy: TOXIPROXY },
    ci: { run_id: Number(process.env["GITHUB_RUN_ID"] ?? 0), job: process.env["GITHUB_JOB"] ?? "local", sha: process.env["GITHUB_SHA"] ?? "" },
    at: new Date().toISOString(),
    details,
  };
  appendFileSync(join(BENCH_DIR, "results.jsonl"), `${JSON.stringify(line)}\n`);
}

const alasioPod = () => kube.runningPod(NAMESPACE, RELEASE);

/** Copies `file` of bench/common/dist into the container of the pod, as IN_POD/<file>, renamed into place, so that a driver another run of it started meanwhile reads it whole. */
async function copyIn(pod: string, container: string, file: string): Promise<void> {
  await kube.execOk(NAMESPACE, pod, ["sh", "-c", `mkdir -p ${IN_POD} && cat > ${IN_POD}/.${file}.$$ && mv ${IN_POD}/.${file}.$$ ${IN_POD}/${file}`], { container, stdin: read(`dist/${file}`) });
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
 * The node agent's live checkpoint of the workspace's sandbox, asked in the agent's network, as the
 * gateway asks it, timed there from its ask to its manifest committed: the time and the manifest.
 */
const CHECKPOINT = `
const [port, environment, workspace, request] = process.argv.slice(2);
const body = require("node:fs").readFileSync(request, "utf8");
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
  return (await onNode(node, `${CRICTL} pods --name '^${name}$' --namespace ${SESSIONS} -q | head -n 1`)).trim();
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

/** The last bracketed Go duration a line matching `pattern` of `lines` gives (runsc's "... took [1.5s]."), in milliseconds, or null. */
function tookMs(lines: readonly string[], pattern: RegExp): number | null {
  const said = lines.findLast((line) => pattern.test(line));
  const duration = said === undefined ? undefined : /took \[([^\]]+)\]/u.exec(said)?.[1];
  return duration === undefined ? null : goDurationMs(duration);
}

/**
 * What a sandbox's boot log says of its last save: its pause, its pre-copy's rounds and bytes,
 * and the save's own splits, of the lines after its last "Sandbox save started": the kernel's
 * state, the memory files', and the whole, and the collection before it with the time it
 * stopped the world.
 */
function saveLogged(lines: readonly string[]): {
  readonly pauseMs: number | null;
  readonly passes: number | null;
  readonly precopyBytes: number | null;
  readonly kernelMs: number | null;
  readonly memoryMs: number | null;
  readonly overallMs: number | null;
  readonly split: Record<string, number | null>;
  readonly said: string[];
} {
  const started = lines.findLastIndex((line) => line.includes("Sandbox save started"));
  const last = started === -1 ? [] : lines.slice(started);
  const paused = last.findLast((line) => line.includes("Tasks resumed after save, paused for"));
  const done = lines.findLast((line) => line.includes("Pre-copy done after"));
  const skipped = lines.findLast((line) => line.includes("Pre-copy skipped"));
  // The last save's scan of its memory and what it wrote, zero pages among them.
  const scanned = last.findLast((line) => line.includes("save scanning took"));
  const written = last.findLast((line) => line.includes("bytes written to the pages file"));
  const collected = last.findLast((line) => line.includes("Collected the save's garbage"));
  // When the save paused the sandbox, and each arming of its dirty tracking, its time and size, before and after.
  const pausing = started === -1 ? undefined : lines[started];
  const armed = lines.filter((line) => line.includes("Dirty tracking: armed")).slice(-10);
  const timers = last.filter((line) => /(Kernel|Memory files|Overall|Split filesystem|CPUID) save took|Pausing root network namespace took|Kernel save stats/u.test(line));
  const rounds = /Pre-copy done after (\d+) rounds \([^)]*\): (\d+) bytes/u.exec(done ?? "");
  const gc = /in (\S+), the world stopped for (\S+) of it/u.exec(collected ?? "");
  const split = {
    gc_ms: gc?.[1] === undefined ? null : goDurationMs(gc[1]),
    gc_world_stopped_ms: gc?.[2] === undefined ? null : goDurationMs(gc[2]),
    cpuid_ms: tookMs(last, /CPUID save took/u),
    network_pause_ms: tookMs(last, /Pausing root network namespace took/u),
    kernel_ms: tookMs(last, /Kernel save took/u),
    split_fs_ms: tookMs(last, /Split filesystem save took/u),
    memory_files_ms: tookMs(last, /Memory files save took/u),
    overall_ms: tookMs(last, /Overall save took/u),
  };
  return {
    pauseMs: paused ? goDurationMs(/paused for (\S+?)\.?$/u.exec(paused)?.[1] ?? "") : null,
    passes: rounds ? Number(rounds[1]) : skipped ? 0 : null,
    precopyBytes: rounds ? Number(rounds[2]) : skipped ? 0 : null,
    kernelMs: split.kernel_ms,
    memoryMs: split.memory_files_ms,
    overallMs: split.overall_ms,
    split,
    said: [pausing, paused, done, skipped, scanned, written, collected, ...timers, ...armed].filter((line): line is string => line !== undefined),
  };
}

/**
 * What a restored sandbox's boot log says: how long runsc took from its start to the restore's
 * success, every page loaded; its splits, the kernel's state and the memory files' metadata
 * loaded, and its start to its load's end, runnable; the bytes its layers' loaders loaded; and
 * those lines.
 */
function restoreLogged(lines: readonly string[]): {
  readonly runscRestoreMs: number | null;
  readonly kernelLoadMs: number | null;
  readonly memoryLoadMs: number | null;
  readonly toRunningMs: number | null;
  readonly loadedBytes: number | null;
  readonly said: string[];
} {
  const first = lines.find((line) => logTimeMs(line) !== null);
  const restored = lines.find((line) => line.includes("Restore successful"));
  const loaded = lines.find((line) => line.includes("Overall load took"));
  // Each layer's loader's lines: its working set, its end, its rate and its budget.
  const loading = lines.filter((line) => /Async page loading (completed|read|: )|async loading \d+ bytes from|working set holds/u.test(line));
  const timers = lines.filter((line) => /(Kernel|Memory files|Overall|CPUID) load took|Starting to restore|Restored container|Dirty tracking: armed/u.test(line));
  const completed = loading.flatMap((line) => /Async page loading completed in \S+ \((\d+) bytes/u.exec(line)?.[1] ?? []);
  const from = first ? logTimeMs(first) : null;
  const between = (line: string | undefined) => {
    const to = line ? logTimeMs(line) : null;
    return from !== null && to !== null ? to - from : null;
  };
  return {
    runscRestoreMs: between(restored),
    kernelLoadMs: tookMs(lines, /Kernel load took/u),
    memoryLoadMs: tookMs(lines, /Memory files load took/u),
    toRunningMs: between(loaded),
    loadedBytes: completed.length === 0 ? null : completed.reduce((sum, bytes) => sum + Number(bytes), 0),
    said: [restored, ...timers, ...loading].filter((line): line is string => line !== undefined),
  };
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

/**
 * A restore of the workspace from its snapshot `manifest` through the gateway, its first request
 * the probe's, on `node`, whose agent restores it: its result's measures, checks and details, and
 * its pod. A cold one's bytes from the store are counted until the fetch it starts is through.
 */
async function restore(node: string, workspace: string, sessionId: string, fixture: string, runId: string, manifest: Manifest, kind: "warm" | "cold") {
  const storeBefore = await storeBytes(node);
  const diskBefore = await diskReadBytes(node);
  // Settled whatever comes of the probe, which fails a restore that never answers before its pod is seen.
  const watching = watchRestore(workspace, manifest.release).then((watched) => ({ watched }), (error: unknown) => ({ error }));
  const probed = await driver<{
    readonly t0_ns: string;
    readonly t0_wall_ms: number;
    readonly first_exec_ms: number;
    readonly verify_ms: number;
    readonly ok: boolean;
    readonly failure: string | null;
    readonly correctness: Record<string, unknown>;
    readonly workload_rss_bytes: number;
    readonly notice: string | null;
  }>(workspace, "probe", "--session", sessionId, "--fixture", fixture, "--run-id", runId);
  lastRestoreAt = probed.t0_wall_ms;
  const settled = await watching;
  if ("error" in settled) throw settled.error;
  const { residentNs, pod, error } = settled.watched;
  const diskRead = (await diskReadBytes(node)) - diskBefore;
  const storeReceived = (kind === "cold" ? await storeQuiet(node) : (await storeBytes(node)).received) - storeBefore.received;
  const name = pod.metadata?.name ?? "";
  if (pod.spec?.nodeName !== node) throw new Error(`the restore's pod ${name} runs on ${pod.spec?.nodeName}, not on ${node}, whose counters the run reads`);
  const memory = await podMemory(node, pod.metadata?.uid ?? "");
  const logged = restoreLogged(await bootLog(node, await sandboxIdOf(node, name)));
  const prepared = await preparedLine(node, manifest.snapshot);
  const source = await sourceOf(node, pod);
  const logs = await loggedOf(node, workspace);
  const spike = await tracedRestore(node, workspace, probed as unknown as Record<string, unknown>);
  const resident = residentNs === null ? null : msBetween(BigInt(probed.t0_ns), residentNs);
  const resumed = stepAt(spike.steps, "agent runsc-resumed");
  const forgotten = stepAt(spike.steps, "agent connections-forgotten");
  const runscResume = resumed !== null && forgotten !== null ? resumed - forgotten : null;
  const policyWait = /^agent policies-in-force (\d+)$/u.exec(spike.steps.find(([step]) => step.startsWith("agent policies-in-force"))?.[0] ?? "")?.[1];
  const { initialized, first_exec_end: firstEnd } = spike.probe as { readonly initialized: number | null; readonly first_exec_end: number | null };
  const probeExec = initialized !== null && firstEnd !== null ? firstEnd - initialized : null;
  const loaded = logged.loadedBytes;
  const metrics: Metrics = {
    first_exec_ms: probed.first_exec_ms,
    verify_ms: probed.verify_ms,
    resident_ms: resident,
    resident_how: resident === null ? null : "signal",
    memory_peak_bytes: memory.peak,
    memory_bytes: memory.current,
    workload_rss_bytes: probed.workload_rss_bytes,
    restore_kernel_load_ms: logged.kernelLoadMs,
    restore_memory_load_ms: logged.memoryLoadMs,
    restore_to_running_ms: logged.toRunningMs,
    runsc_resume_ms: runscResume,
    policy_wait_ms: policyWait === undefined ? null : Number(policyWait),
    probe_exec_ms: probeExec,
    // What of the first exec's time is neither runsc's restore to runnable, its resume with bayma's hook, nor the probe's MCP session and exec.
    orchestration_ms: logged.toRunningMs !== null && runscResume !== null && probeExec !== null ? probed.first_exec_ms - logged.toRunningMs - runscResume - probeExec : null,
    loaded_bytes: loaded,
    store_received_bytes: storeReceived,
    disk_read_bytes: diskRead,
    // Of a cold restore, what the store sent the node for it, its fetch included; of a warm one, what the node's disks read.
    read_amplification: loaded === null || loaded === 0 ? null : (kind === "cold" ? storeReceived : diskRead) / loaded,
    page_cache: kind === "cold" ? "dropped" : PAGE_CACHE,
    compaction_at_t0: compactionAtT0(spike.steps),
  };
  // Spike: the fixture's own timing of its first exec and its verify, with the loop's stalls.
  const timingOf = (said: unknown): unknown => (typeof said === "object" && said !== null ? ((said as Record<string, unknown>)["timing"] ?? null) : null);
  const answered = probed as unknown as Record<string, unknown>;
  const fixtureTiming = { first: timingOf(answered["first"]), verify: timingOf(answered["verified"]) };
  // Spike: a restore that failed keeps its sandbox's whole boot log, and the node's JuiceFS log's end.
  if (!probed.ok) await keepFailedLogs(node, name, `${runId}-${kind}`);
  const details = { pod: name, node, source, prepared, chain: manifest.layers.length, runsc_restore_ms: logged.runscRestoreMs, logged: logged.said, memory_stat: memory.stat, resident_error: error, logs, t0_wall_ms: probed.t0_wall_ms, end_wall_ms: Date.now(), spike, fixture_timing: fixtureTiming };
  return { probed, metrics, details, pod: name };
}

/** Spike: keeps the boot log of the sandbox of pod `name` on `node`, and the end of the node's JuiceFS log, as `label`'s, gzipped, in the job's logs. */
async function keepFailedLogs(node: string, name: string, label: string): Promise<void> {
  const logs = join(BENCH_DIR, "logs");
  const sandboxId = await sandboxIdOf(node, name).catch(() => "");
  await onNode(node, [
    `cat ${RUNSC_LOGS}/${sandboxId}/runsc.log.*.boot.txt 2>/dev/null | gzip > ${logs}/boot-${label}.txt.gz`,
    `for each in $(find /run/alasio -name juicefs.log 2>/dev/null); do echo "# $each"; tail -c 2000000 "$each"; done | gzip > ${logs}/juicefs-${label}.txt.gz`,
  ].join("; ")).catch((failed: unknown) => console.error(`# the logs of ${label} were not kept: ${errorText(failed)}`));
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

/**
 * Ends the restored pod `name` of `workspace` without a save, as a crash ends it, and waits until it
 * is gone, and the gateway has let its binding go: its Lease gone, as the gateway records what a
 * binding whose pod went is released into while its Lease holds (src/gateway/sessions.ts
 * recordLost), which would otherwise write over the record a cold restore is given.
 */
async function endUnsaved(workspace: string, name: string): Promise<void> {
  await kube.kill(SESSIONS, name);
  await gone(name);
  const lease = ref("Lease", leaseName(sessionKey(workspace)), SESSIONS);
  for (const deadline = Date.now() + 300_000; ; await sleep(1000)) {
    if ((await kube.get(lease)) === null) return;
    if (Date.now() > deadline) throw new Error(`the binding of ${workspace} was not let go within 300 s of its pod's end`);
  }
}

/** Spike: the node agent's log lines of `node`, timestamped. */
async function agentLines(node: string): Promise<string[]> {
  return (await kube.logs(NAMESPACE, (await nodeAgents()).get(node) ?? "", "node-agent", { timestamps: true })).split("\n");
}

/** Spike: kills the compaction of `snapshot` as it starts, and waits until the agent says it failed: when, from the ask, it was killed. */
async function killCompaction(node: string, snapshot: string): Promise<Record<string, unknown>> {
  const started = Date.now();
  for (const deadline = started + 120_000; Date.now() < deadline; await sleep(50)) {
    const killed = (await onNode(node, "pkill -e -f ' image [c]ompact ' || true")).trim();
    if (killed !== "") {
      const at = Date.now() - started;
      for (const until = Date.now() + 60_000; Date.now() < until; await sleep(500)) {
        const said = (await agentLines(node)).find((line) => line.includes(`compacting the snapshot ${snapshot} `));
        if (said) return { killed_after_ms: at, killed, said: said.slice(0, 300) };
      }
      return { killed_after_ms: at, killed, said: null };
    }
  }
  return { killed_after_ms: null };
}

/** Spike: waits until the compaction of `snapshot` has ended, however it did: how long that took, and what the agent said. */
async function compactionEnded(node: string, snapshot: string): Promise<Record<string, unknown>> {
  const started = Date.now();
  for (const deadline = started + 900_000; Date.now() < deadline; await sleep(1000)) {
    const said = (await agentLines(node)).find((line) => line.includes(`compacted the snapshot ${snapshot} `) || line.includes(`compacting the snapshot ${snapshot} `));
    if (said) return { waited_ms: Date.now() - started, said: said.slice(0, 300) };
  }
  throw new Error(`the compaction of ${snapshot} did not end within 900 s`);
}

/**
 * Spike: the steps of `workspace`'s phase that began at `t0` (the wall clock's milliseconds), as
 * the gateway's and the agent's trace lines say them, each in milliseconds from `t0`: of each step,
 * its first line at or after `t0`, or, where none is, its last in the minute before (a compaction
 * the phase began under). A step's line's number, or single word, is kept in its name.
 */
async function traced(node: string, workspace: string, t0: number): Promise<[string, number][]> {
  const gateways = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) });
  const lines = [
    ...(await Promise.all(gateways.map((pod) => kube.logs(NAMESPACE, pod.metadata?.name ?? "", "sandbox-gateway").catch(() => "")))).flatMap((logged) => logged.split("\n")),
    ...(await agentLines(node)),
  ];
  const after = new Map<string, number>();
  const before = new Map<string, number>();
  for (const line of lines) {
    const matched = /trace (\S+) (\S+) (\S+)(.*) @(\d+)$/u.exec(line);
    if (!matched || !matched[1]?.includes(workspace)) continue;
    const rest = matched[4]?.trim() ?? "";
    const step = `${matched[2]} ${matched[3]}${/^(\d+|[a-z]+)$/u.test(rest) ? ` ${rest}` : ""}`;
    const at = Number(matched[5]) - t0;
    if (at >= 0) {
      if (!after.has(step) || (after.get(step) ?? 0) > at) after.set(step, at);
    } else if (at >= -60_000) {
      before.set(step, Math.max(before.get(step) ?? -Infinity, at));
    }
  }
  for (const [step, at] of before) if (!after.has(step)) after.set(step, at);
  return [...after].toSorted(([, a], [, b]) => a - b);
}

/** Spike: a restore's steps (traced), and the probe's own, in milliseconds from the probe's t0. */
async function tracedRestore(node: string, workspace: string, probed: Record<string, unknown>): Promise<{ readonly steps: [string, number][]; readonly probe: Record<string, unknown> }> {
  const t0Ns = BigInt(String(probed["t0_ns"]));
  const fromT0 = (key: string) => (typeof probed[key] === "string" ? Number(BigInt(probed[key] as string) - t0Ns) / 1e6 : null);
  return {
    steps: await traced(node, workspace, Number(probed["t0_wall_ms"])),
    probe: { initialized: fromT0("initialized_ns"), acquired: fromT0("connected_ns"), first_exec_end: fromT0("first_exec_end_ns"), refusals: probed["refusals"] },
  };
}

/** Spike: the step `name` of `steps`, or null. */
const stepAt = (steps: readonly [string, number][], name: string): number | null => steps.find(([step]) => step === name)?.[1] ?? null;

/** Spike: from the step `from` of `steps` to the step `to`, or null where either is not there. */
function between(steps: readonly [string, number][], from: string, to: string): number | null {
  const [start, end] = [stepAt(steps, from), stepAt(steps, to)];
  return start === null || end === null ? null : end - start;
}

/**
 * Spike: whether the release's compaction of the chain ran as the phase whose steps are `steps`
 * began: started before it, not yet through its upload.
 */
function compactionAtT0(steps: readonly [string, number][]): "running" | "none" {
  const started = steps.find(([step]) => step === "agent compact-start");
  const uploaded = steps.find(([step]) => step === "agent compact-uploaded");
  return started !== undefined && started[1] < 0 && (uploaded === undefined || uploaded[1] >= 0) ? "running" : "none";
}

/**
 * Spike: the bytes that moved between the node agents' network and the store, as counted by
 * the rules countStore puts in the agent's network namespace on `node`: sent to it and received
 * from it, all the agent's clients of the bucket together (its uploads, its proxy of the restores'
 * reads, its fetches), and toxiproxy's on the emulated profiles, which carries them.
 */
async function storeBytes(node: string): Promise<{ readonly sent: number; readonly received: number }> {
  const listed = await onNode(node, `nsenter --target ${await agentPid(node)} --net sh -c 'iptables -L ${STORE_CHAIN} -v -x -n'`);
  const bytes = (direction: string) => Number(listed.split("\n").find((line) => line.includes(`/* ${direction} */`))?.trim().split(/\s+/u)[1] ?? NaN);
  return { sent: bytes("sent"), received: bytes("received") };
}

/** Spike: counts, in the agent's network namespace on `node`, what it sends to the store's Service and receives from it (storeBytes). */
async function countStore(node: string): Promise<void> {
  const address = await storeAddress();
  const inNetns = `nsenter --target ${await agentPid(node)} --net`;
  await onNode(node, [
    `${inNetns} iptables -N ${STORE_CHAIN}`,
    `${inNetns} iptables -A ${STORE_CHAIN} ! -o lo -d ${address} -p tcp --dport 8333 -m comment --comment sent`,
    `${inNetns} iptables -A ${STORE_CHAIN} ! -i lo -s ${address} -p tcp --sport 8333 -m comment --comment received`,
    `${inNetns} iptables -I OUTPUT -j ${STORE_CHAIN}`,
    `${inNetns} iptables -I INPUT -j ${STORE_CHAIN}`,
  ].join(" && "));
}

/** Spike: the bytes the node's disks have read, as /proc/diskstats counts their sectors. */
async function diskReadBytes(node: string): Promise<number> {
  const listed = await onNode(node, "grep -E ' (sd[a-z]+|nvme[0-9]+n[0-9]+|vd[a-z]+) ' /proc/diskstats");
  return listed.split("\n").filter(Boolean).reduce((sum, line) => sum + Number(line.trim().split(/\s+/u)[5] ?? 0) * 512, 0);
}

/** Spike: waits until the store has sent `node`'s agent nothing for 3 s, within 180 s, as the fetch a cold restore starts runs: what it received meanwhile. */
async function storeQuiet(node: string): Promise<number> {
  let last = (await storeBytes(node)).received;
  let quietSince = Date.now();
  for (const deadline = Date.now() + 180_000; Date.now() < deadline && Date.now() - quietSince < 3000; await sleep(500)) {
    const now = (await storeBytes(node)).received;
    if (now !== last) quietSince = Date.now();
    last = now;
  }
  return last;
}

/** Drops the page cache of `node`, as every restore starts with it dropped, so what ran before leaves it nothing. */
const dropPageCache = (node: string): Promise<string> => onNode(node, "sync && echo 1 > /proc/sys/vm/drop_caches");

/**
 * Removes every node's cache of the workspace, by its agent, as the gateway removes a deleted
 * workspace's (src/gateway/snapshots.ts forgetDeleted), which spares a workspace used within the
 * cache's grace, as a restore may still read it (src/node-agent/cache.ts discard), as the warm
 * restore's was a minute before; asked again for 20 s, then, where the agent still spares it, by
 * hand, as the 10.0.4 baseline removed it, which no compaction is writing by then (the warm
 * restore's prepare stopped the release's); and the nodes' page cache, so a restore reads it from
 * the bucket. How each node's went.
 */
async function coldCaches(workspace: string): Promise<Record<string, unknown>> {
  const how: Record<string, unknown> = {};
  for (const [node, pod] of await nodeAgents()) {
    const started = Date.now();
    let discarded = false;
    // Spike: `cold-by-hand` removes it by hand at once, as the 10.0.4 baseline did, with no 20 s asking the agent first.
    for (const deadline = started + (VARIANT === "cold-by-hand" ? 0 : 20_000); !discarded && Date.now() < deadline; await sleep(1000)) {
      discarded = (JSON.parse(await agentOk(node, "DELETE", `/cache/${SESSIONS}/${workspace}`)) as { readonly discarded: boolean }).discarded;
    }
    if (!discarded) {
      const held = await onNode(node, `ls -la ${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace} ${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}/${LAYERS_DIR} 2>&1 | head -n 30`);
      await kube.execOk(NAMESPACE, pod, ["rm", "-rf", `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}`], { container: "node-agent" });
      how[node] = { by: "hand", agent_refused_ms: Date.now() - started, held: held.trim().split("\n") };
    } else {
      how[node] = { by: "agent", ms: Date.now() - started };
    }
    await dropPageCache(node);
  }
  return how;
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
const bucketBytes = async (node: string, workspace: string): Promise<number> =>
  (JSON.parse(await inAgentNetwork(node, [new URL("dist/s3.mjs", COMMON).pathname, "bytes", `${SESSIONS}/${workspace}/`])) as { readonly bytes: number }).bytes;

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

/** What the runner says of its memory, its kernel's last words and k3s's, as a run fails: in logs/failure-<name>.txt. */
async function sayRunner(name: string): Promise<void> {
  const said = await onNode("", "free -m; echo; dmesg | tail -n 60; echo; journalctl -u k3s --since '-10 min' --no-pager | tail -n 80").catch((error: unknown) => errorText(error));
  appendFileSync(join(BENCH_DIR, "logs", `failure-${name}.txt`), said);
}

/** Spike: whether the run's restores run beside another session's release and restore: `mixed`, as shipped; `mixed-iocost`, with blk-iocost on (ioCost). */
const MIXED = VARIANT === "mixed" || VARIANT === "mixed-iocost";
/** Spike: the background session of a mixed run: a whole-heap writer, whose release's delta makes its chain due to be compacted. */
const BACKGROUND = { fixture: "saturating", sizeMib: 1024 } as const;

/** Spike: a mixed run's background session, set up and saved live once, so that its release is a delta: its workspace and session. */
async function background(runId: string): Promise<{ readonly workspace: string; readonly sessionId: string; readonly node: string }> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no background workspace: ${created.status} ${created.said}`);
  await bindSession(workspace);
  const { session_id: sessionId } = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", BACKGROUND.fixture, "--size-mib", String(BACKGROUND.sizeMib), "--run-id", `${runId}-background`, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
  const bound = await boundPodOf(workspace);
  if (!bound) throw new Error(`no pod is bound to the background ${workspace}`);
  const pod = await podNamed(bound.name);
  const bayma = pod.spec?.containers.find(({ name }) => name === BAYMA_CONTAINER);
  const hook = bayma ? hookOf(bayma) : null;
  const stateDir = bayma ? stateDirOf(bayma) : null;
  if (!hook || !stateDir) throw new Error(`the background pod ${bound.name} runs no bayma with a state directory`);
  const request = { environment: SESSIONS, workspace, namespace: SESSIONS, pod: bound.name, containerId: containerIdOf(pod), hook, podTemplate: await poolTemplate(pod), live: { stateDir } };
  const asking = join(BENCH_DIR, "checkpoint.cjs");
  const asked = join(BENCH_DIR, "checkpoint-background.json");
  writeFileSync(asking, CHECKPOINT);
  writeFileSync(asked, JSON.stringify(request));
  await inAgentNetwork(bound.node, [asking, String(NODE_AGENT_PORT), SESSIONS, workspace, asked]);
  return { workspace, sessionId, node: bound.node };
}

/** Spike: what of a promise's work came: its value or its error's text, and when it ended, in milliseconds from `t0`. */
const settledFrom = <T>(work: Promise<T>, t0: number) =>
  work.then((value) => ({ ok: true, value, ended_ms: Date.now() - t0 }), (error: unknown) => ({ ok: false, value: errorText(error), ended_ms: Date.now() - t0 }));

/**
 * Spike: blk-iocost on for the node's disk that holds /var/lib (the node's cache, the store's
 * volumes and k3s), its cost model measured there by fio, not the kernel's default for its kind,
 * its QoS the kernel's defaults; and the node agents' and the store's pods weighted 10 to the
 * sandboxes' 100, the io controller enabled down to them. What it set, in logs/iocost.txt.
 */
async function ioCost(node: string): Promise<void> {
  const uids = (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE })).filter(({ metadata }) => /seaweedfs|node-agent/u.test(metadata?.name ?? "")).map(({ metadata }) => metadata?.uid ?? "");
  const script = [
    "set -eu",
    "disk=$(lsblk -no PKNAME \"$(findmnt -no SOURCE --target /var/lib)\" | head -n 1)",
    "[ -n \"$disk\" ] || disk=$(basename \"$(findmnt -no SOURCE --target /var/lib)\")",
    "dev=$(cat /sys/block/$disk/dev)",
    "echo \"disk $disk $dev rotational $(cat /sys/block/$disk/queue/rotational) scheduler $(cat /sys/block/$disk/queue/scheduler)\"",
    "command -v fio >/dev/null || apt-get install -y -qq fio >/dev/null",
    "f=/var/lib/bench-iocost.fio",
    "run() { fio --name=m --filename=$f --size=2G --direct=1 --ioengine=libaio --runtime=8 --time_based --output-format=json \"$@\" | python3 -c 'import json,sys; j=json.load(sys.stdin)[\"jobs\"][0]; print(int(j[\"read\"][\"bw_bytes\"]+j[\"write\"][\"bw_bytes\"]), int(j[\"read\"][\"iops\"]+j[\"write\"][\"iops\"]))'; }",
    "set -- $(run --rw=read --bs=1M --iodepth=32); rbps=$1",
    "set -- $(run --rw=read --bs=4k --iodepth=32); rseqiops=$2",
    "set -- $(run --rw=randread --bs=4k --iodepth=32); rrandiops=$2",
    "set -- $(run --rw=write --bs=1M --iodepth=32); wbps=$1",
    "set -- $(run --rw=write --bs=4k --iodepth=32); wseqiops=$2",
    "set -- $(run --rw=randwrite --bs=4k --iodepth=32); wrandiops=$2",
    "rm -f $f",
    "echo \"$dev ctrl=user model=linear rbps=$rbps rseqiops=$rseqiops rrandiops=$rrandiops wbps=$wbps wseqiops=$wseqiops wrandiops=$wrandiops\" > /sys/fs/cgroup/io.cost.model",
    "echo \"$dev enable=1\" > /sys/fs/cgroup/io.cost.qos",
    "echo model; cat /sys/fs/cgroup/io.cost.model; echo qos; cat /sys/fs/cgroup/io.cost.qos",
    `for uid in ${uids.join(" ")}; do`,
    "  dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name \"*pod$uid*\" -o -name \"*pod$(echo $uid | tr - _)*\" \\) | head -n 1)",
    "  [ -n \"$dir\" ] || continue",
    "  up=$(dirname \"$dir\"); chain=\"\"; while [ \"$up\" != /sys/fs/cgroup ]; do chain=\"$up $chain\"; up=$(dirname \"$up\"); done",
    "  for each in /sys/fs/cgroup $chain; do grep -qw io \"$each/cgroup.subtree_control\" || echo +io > \"$each/cgroup.subtree_control\"; done",
    "  echo \"default 10\" > \"$dir/io.weight\"",
    "  echo \"weight $dir $(cat $dir/io.weight | head -n 1)\"",
    "done",
  ].join("\n");
  writeFileSync(join(BENCH_DIR, "iocost.sh"), script);
  appendFileSync(join(BENCH_DIR, "logs", "iocost.txt"), await onNode(node, `sh ${join(BENCH_DIR, "iocost.sh")} 2>&1`));
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
  let behind: Awaited<ReturnType<typeof background>> | null = null;
  try {
    const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
    if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
    await bindSession(workspace);
    const setup = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", fixture, "--size-mib", String(sizeMib), "--run-id", runId, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
    const sessionId = setup.session_id;
    // Spike: a mixed run's background session, beside the run's own.
    behind = MIXED ? await background(runId) : null;

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
    let heldLive = heldBefore;
    // No live save of `whole`, whose release saves the session whole; a chain-N's N - 1, each a delta of the one before, all but the last written in its details.
    const lives: Record<string, unknown>[] = [];
    for (let saved = 1; VARIANT !== "whole" && saved <= LIVE_SAVES; saved++) {
      const heldBeforeSave = heldLive;
      await driver(workspace, "mark", "--session", sessionId, "--fixture", fixture);
      const asking = join(BENCH_DIR, "checkpoint.cjs");
      const asked = join(BENCH_DIR, "checkpoint-request.json");
      writeFileSync(asking, CHECKPOINT);
      writeFileSync(asked, JSON.stringify(request));
      const liveT0 = Date.now();
      const liveSent = (await storeBytes(node)).sent;
      const checkpointed = await inAgentNetwork(node, [asking, String(NODE_AGENT_PORT), SESSIONS, workspace, asked]).then(
        (stdout) => ({ code: 0, stdout, stderr: "" }),
        (error: unknown) => ({ code: 1, stdout: "", stderr: errorText(error) }),
      );
      const gaps = await driver<{ readonly max_gap_ms: number; readonly slowdown: number | null }>(workspace, "gaps", "--session", sessionId, "--fixture", fixture);
      const liveSentBytes = (await storeBytes(node)).sent - liveSent;
      heldLive = await bucketBytes(node, workspace);
      const liveLogged = saveLogged(await bootLog(node, sandboxId));
      const liveSteps = await traced(node, workspace, liveT0);
      const logged = { state_dir: request.live.stateDir, logged: liveLogged.said, gaps, pause_split: liveLogged.split, t0_wall_ms: liveT0, end_wall_ms: Date.now(), spike: { steps: liveSteps } };
      const measured = {
        unavailable_ms: gaps.max_gap_ms, pause_reported_ms: liveLogged.pauseMs, slowdown: gaps.slowdown, precopy_passes: liveLogged.passes, precopy_bytes: liveLogged.precopyBytes, uploaded_bytes: heldLive - heldBeforeSave,
        kernel_save_ms: liveLogged.kernelMs, memory_save_ms: liveLogged.memoryMs, overall_save_ms: liveLogged.overallMs, store_sent_bytes: liveSentBytes,
      };
      if (checkpointed.code === 0) {
        const live = JSON.parse(checkpointed.stdout.trim().split("\n").at(-1) ?? "{}") as { readonly ms: number; readonly manifest: Manifest };
        const imageBytes = live.manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null;
        if (saved < LIVE_SAVES) {
          lives.push({ ...measured, save_total_ms: live.ms, image_bytes: imageBytes, chain: live.manifest.layers.length, snapshot: live.manifest.snapshot });
          continue;
        }
        emit(fixture, sizeMib, run, "save-live", { ok: true, failure: null }, {
          ...measured,
          save_total_ms: live.ms,
          image_bytes: imageBytes,
          // From the agent's ask, as the gateway's keepLive asks it, to its manifest committed in the store.
          durable_store_ms: between(liveSteps, "agent checkpoint-asked", "agent checkpoint-committed"),
          uploaded_over_needed: imageBytes ? liveSentBytes / imageBytes : null,
        }, NO_CHECKS, { ...logged, snapshot: live.manifest.snapshot, runsc_checkpoint_ms: live.manifest.pauseMs, upload_ms: live.manifest.uploadMs, chain: live.manifest.layers.length, lives });
      } else {
        // As alasio 10.0.0 fails it (mountedStateDir): its sandbox saved and running on, the snapshot never committed.
        const said = /"error":"([^"]*)"/u.exec(checkpointed.stderr)?.[1] ?? checkpointed.stderr.trim().split("\n").at(-1) ?? "";
        emit(fixture, sizeMib, run, "save-live", { ok: false, failure: `the node agent's live checkpoint failed: ${said}` }, measured, NO_CHECKS, { ...logged, lives });
        break;
      }
    }
    done.add(phase);

    phase = "save-stop";
    const stopT0 = Date.now();
    const stopSent = (await storeBytes(node)).sent;
    const released = await release(workspace);
    const stopSentBytes = (await storeBytes(node)).sent - stopSent;
    if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release answered ${released.status}: ${released.said}`);
    const manifest = JSON.parse(await agentOk(node, "GET", `/snapshots/${SESSIONS}/${workspace}`)) as Manifest;
    const record = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/released`)) as { readonly snapshot: string; readonly release: string };
    // Its current snapshot is the one it was released into, or one the agent compacted from it since, which is the same
    // (src/node-agent/agent.ts compactNow), as a chain of the live save's image and the release's delta may be due.
    if (record.snapshot !== manifest.snapshot && record.snapshot !== manifest.compactedFrom) {
      throw new Error(`${workspace} was released into ${record.snapshot}, not its current snapshot ${manifest.snapshot}`);
    }
    const heldStop = await bucketBytes(node, workspace);
    const stopLogged = saveLogged(await bootLog(node, sandboxId));
    const stopSteps = await traced(node, workspace, stopT0);
    // The release's own image, which a compaction since may have replaced as the manifest's.
    const releasedLayer = manifest.compactedFrom === undefined ? manifest.layers[0] : undefined;
    const stopImageBytes = releasedLayer?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null;
    emit(fixture, sizeMib, run, "save-stop", { ok: true, failure: null }, {
      save_total_ms: released.ms,
      pause_reported_ms: stopLogged.pauseMs,
      precopy_passes: 0,
      precopy_bytes: 0,
      image_bytes: manifest.layers[0]?.files.reduce((sum, { bytes }) => sum + bytes, 0) ?? null,
      uploaded_bytes: heldStop - heldLive,
      kernel_save_ms: stopLogged.kernelMs,
      memory_save_ms: stopLogged.memoryMs,
      overall_save_ms: stopLogged.overallMs,
      // From the release's arrival at the front to its checkpoint's manifest committed in the store.
      durable_store_ms: between(stopSteps, "front release", "agent checkpoint-committed"),
      store_sent_bytes: stopSentBytes,
      uploaded_over_needed: stopImageBytes ? stopSentBytes / stopImageBytes : null,
    }, NO_CHECKS, {
      snapshot: manifest.snapshot, runsc_checkpoint_ms: manifest.pauseMs, upload_ms: manifest.uploadMs, chain: manifest.layers.length, bytes: manifest.bytes, compacted_from: manifest.compactedFrom ?? null,
      logged: stopLogged.said, pause_split: stopLogged.split, t0_wall_ms: stopT0, end_wall_ms: Date.now(), spike: { steps: stopSteps },
    });
    done.add(phase);
    // Spike: the release's compaction of the chain, where one is due, killed as it starts, or waited for.
    const compaction = compactionDue(manifest.layers) === null ? null : VARIANT === "no-compaction" ? await killCompaction(node, manifest.snapshot) : VARIANT === "settled" ? await compactionEnded(node, manifest.snapshot) : null;
    await gone(bound.name);
    // The workspace as the release left it, for the cold restore's.
    const quota = JSON.parse(await agentOk(node, "GET", `/workspaces/${SESSIONS}/${workspace}/quota`)) as { readonly capacityBytes: number };
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace }, to: { environment: SESSIONS, workspace: copy }, quotaBytes: quota.capacityBytes });

    phase = "restore-warm";
    if (PAGE_CACHE === "dropped") await dropPageCache(node);
    // What the agent sent the store from the release's end on, before the warm restore: the release's compaction's upload, where one ran.
    const compactionSent = (await storeBytes(node)).sent - stopSent - stopSentBytes;
    // Spike: a mixed run's warm restore beside the background session's release, its checkpoint, upload and compaction, begun half a second before.
    const warmBeside = behind === null ? null : settledFrom(release(behind.workspace), Date.now());
    if (warmBeside !== null) await sleep(500);
    const warm = await restore(node, workspace, sessionId, fixture, runId, manifest, "warm");
    const mixedWarm = warmBeside === null ? null : { beside: "release", ...(await warmBeside) };
    emit(fixture, sizeMib, run, "restore-warm", restoreOutcome(warm, "cache"), warm.metrics, warm.probed.correctness, { ...warm.details, compaction_sent_bytes: compactionSent, mixed: mixedWarm, spike: { ...warm.details.spike, compaction } });
    done.add(phase);
    await endUnsaved(workspace, warm.pod);

    phase = "restore-cold";
    await agentOk(node, "DELETE", `/workspaces/${SESSIONS}/${workspace}`);
    await agentOk(node, "POST", "/workspaces/clones", { from: { environment: SESSIONS, workspace: copy }, to: { environment: SESSIONS, workspace }, quotaBytes: quota.capacityBytes });
    // Its snapshot current again, its manifest put back as current.json, as a checkpoint commits it (src/node-agent/agent.ts commit).
    await inAgentNetwork(node, [new URL("dist/s3.mjs", COMMON).pathname, "copy", `${SESSIONS}/${workspace}/manifests/${manifest.snapshot}.json`, `${SESSIONS}/${workspace}/current.json`]);
    await agentOk(node, "PUT", `/workspaces/${SESSIONS}/${workspace}/released`, { snapshot: manifest.snapshot, release: manifest.release });
    const cacheRemoved = await coldCaches(workspace);
    // Spike: a mixed run's cold restore beside the background session's restore, from the node's cache, begun as it is.
    const coldBeside = behind === null ? null : settledFrom(driver(behind.workspace, "probe", "--session", behind.sessionId, "--fixture", BACKGROUND.fixture, "--run-id", `${runId}-background`), Date.now());
    const cold = await restore(node, workspace, sessionId, fixture, runId, manifest, "cold");
    const mixedCold = coldBeside === null ? null : { beside: "restore", ...(await coldBeside) };
    emit(fixture, sizeMib, run, "restore-cold", restoreOutcome(cold, "bucket"), cold.metrics, cold.probed.correctness, { ...cold.details, cache_removed: cacheRemoved, mixed: mixedCold });
    done.add(phase);
    await endUnsaved(workspace, cold.pod);
  } catch (error) {
    if (error instanceof Uncalibrated) throw error;
    const why = `${phase}: ${errorText(error)}`;
    console.error(`# ${fixture} ${sizeMib} MiB run ${run} failed at ${why}`);
    await sayRunner(`${fixture}-${sizeMib}-${run}`);
    if (node !== "") {
      const logs = await loggedOf(node, workspace).catch((failed: unknown) => ({ agent: [errorText(failed)], gateway: [] }));
      // How each pod of its ended, where one did: its containers' last states, as kubelet said them.
      const pods = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` }).catch(() => [] as V1Pod[]);
      const states = pods.map((pod) => `${pod.metadata?.name}: ${pod.status?.phase} ${JSON.stringify(pod.status?.containerStatuses?.map(({ name, state, lastState, restartCount }) => ({ name, state, lastState, restartCount })))}`);
      appendFileSync(join(BENCH_DIR, "logs", `failure-${fixture}-${sizeMib}-${run}.txt`), `\n\n# the node agent of ${workspace}\n${logs.agent.join("\n")}\n\n# the gateway of ${workspace}\n${logs.gateway.join("\n")}\n\n# its pods\n${states.join("\n")}\n`);
    }
    for (const each of phases.filter((name) => !done.has(name))) emit(fixture, sizeMib, run, each, { ok: false, failure: each === phase ? why : `not run: ${why}` }, {});
    const bound = await boundPodOf(workspace).catch(() => null);
    if (bound) await endUnsaved(workspace, bound.name).catch(() => {});
  } finally {
    // Its snapshots forgotten with it, in the bucket and every node's cache, and the copy of its files: asked again
    // while the gateway still lets the binding of the pod ended under it go (409), so no run's images outlive it.
    let removed = { status: 0, said: "" };
    for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
      removed = await mainToken(workspace).then((token) => gatewayCall(token, "DELETE", "/workspace")).catch((error: unknown) => ({ status: 0, said: errorText(error) }));
      if (removed.status !== 409) break;
    }
    if (removed.status !== 204) console.error(`# ${workspace} was not removed: ${removed.status} ${removed.said}`);
    if (node !== "") await agentApi(node, "DELETE", `/workspaces/${SESSIONS}/${copy}`).catch(() => {});
    // Spike: a mixed run's background session ended unsaved and forgotten with its workspace.
    if (behind !== null) {
      const backgroundPod = await boundPodOf(behind.workspace).catch(() => null);
      if (backgroundPod) await endUnsaved(behind.workspace, backgroundPod.name).catch(() => {});
      const forgotten = behind.workspace;
      for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
        const said = await mainToken(forgotten).then((token) => gatewayCall(token, "DELETE", "/workspace")).catch((error: unknown) => ({ status: 0, said: errorText(error) }));
        if (said.status !== 409) break;
      }
    }
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
    // Spike: the release under test's runsc flags of BENCH_RUNSC_CONFIG, as name:value words separated by commas.
    if (release === RELEASE_UNDER_TEST) {
      for (const word of RUNSC_CONFIG) {
        const [flag = "", value = ""] = word.split(":");
        await onNode(node, `grep -q '^  ${flag} = ' ${options} || printf '  ${flag} = "${value}"\\n' >> ${options}; tail -n 6 ${options}`);
      }
    }
  }
  const free = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` });
  for (const pod of free) await kube.remove(ref("Pod", pod.metadata?.name ?? "", SESSIONS));
  for (const pod of free) await gone(pod.metadata?.name ?? "");
}

/** The process of the node agent on `node`, in the node's PID namespace, as its runtime says. */
async function agentPid(node: string): Promise<string> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const agentId = (await kube.get<V1Pod>(ref("Pod", pod, NAMESPACE)))?.status?.containerStatuses?.find(({ name }) => name === "node-agent")?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";
  return (await onNode(node, `${CRICTL} inspect --output go-template --template '{{.info.pid}}' ${agentId}`)).trim();
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
  return JSON.parse(await inAgentNetwork(node, [new URL("dist/probe.mjs", COMMON).pathname, "--prefix", "bench-probe/"])) as Record<string, number>;
}

/**
 * Runs the Node program `argv` from the node, in the network namespace of the node agent on
 * `node`, with the agent's key and bucket, the store by its Service's address, and the agent's
 * token, as the agent's own client of the bucket and the gateway's of the agent: from the node, as
 * a program run in the agent's container is charged to its memory, which the agent's own work
 * fills (one such, 137, ended the agent under a save). Its last line printed.
 */
async function inAgentNetwork(node: string, argv: readonly string[]): Promise<string> {
  const pid = await agentPid(node);
  const environment = (await onNode(node, `tr '\\0' '\\n' < /proc/${pid}/environ`)).split("\n");
  const of = (name: string) => environment.find((line) => line.startsWith(`${name}=`))?.slice(name.length + 1) ?? "";
  const given = {
    S3_ENDPOINT: `http://${await storeAddress()}:8333`,
    S3_REGION: of("S3_REGION"),
    S3_BUCKET: of("S3_BUCKET"),
    AWS_ACCESS_KEY_ID: of("AWS_ACCESS_KEY_ID"),
    AWS_SECRET_ACCESS_KEY: of("AWS_SECRET_ACCESS_KEY"),
    [NODE_AGENT_TOKEN_VARIABLE]: of(NODE_AGENT_TOKEN_VARIABLE),
  };
  const assignments = Object.entries(given).map(([name, value]) => `${name}='${value}'`).join(" ");
  const printed = await onNode(node, `nsenter --target ${pid} --net env ${assignments} ${process.execPath} ${argv.map((arg) => `'${arg}'`).join(" ")}`);
  return printed.trim().split("\n").at(-1) ?? "";
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

/** What the job runs: `runs`, every case's runs, as the module says; or `store`, the store's own experiments alone (storeExperiments). */
const MODE = process.env["BENCH_MODE"] || "runs";
if (MODE !== "runs" && MODE !== "store") throw new Error(`BENCH_MODE is ${MODE}, not runs or store`);

/** Spike: the CPU time the cgroup of the pod of uid `uid` on `node` has used, in microseconds, or null. */
async function podCpuUs(node: string, uid: string): Promise<number | null> {
  const printed = await onNode(node, [
    `dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name '*pod${uid}*' -o -name '*pod${uid.replaceAll("-", "_")}*' \\) | head -n 1)`,
    `[ -n "$dir" ] && sed -n 's/^usage_usec //p' "$dir/cpu.stat"`,
  ].join("; ")).catch(() => "");
  return printed.trim() === "" ? null : Number(printed.trim());
}

/** Spike: the CPU time each of the store's pods, and each node agent, has used, in microseconds, by pod name. */
async function storeCpu(node: string): Promise<Record<string, number | null>> {
  const pods = (await kube.list<V1Pod>("Pod", { namespace: NAMESPACE })).filter(({ metadata }) => /seaweedfs|node-agent|valkey/u.test(metadata?.name ?? ""));
  return Object.fromEntries(await Promise.all(pods.map(async (pod) => [pod.metadata?.name ?? "", await podCpuUs(node, pod.metadata?.uid ?? "")] as const)));
}

/** Spike: `run` timed, with the CPU each store pod used meanwhile, in seconds. */
async function withCpu<T>(node: string, run: () => Promise<T>): Promise<{ readonly said: T; readonly seconds: number; readonly cpu_s: Record<string, number | null> }> {
  const before = await storeCpu(node);
  const started = Date.now();
  const said = await run();
  const seconds = (Date.now() - started) / 1000;
  const after = await storeCpu(node);
  const cpu = Object.fromEntries(Object.entries(after).map(([name, used]) => [name, used === null || before[name] === null || before[name] === undefined ? null : (used - (before[name] ?? 0)) / 1e6]));
  return { said, seconds, cpu_s: cpu };
}

/**
 * Spike: whether a delta checkpoint needs of its parent image its pages' metadata alone
 *: a session saved live, whole, by its agent; then, on the node, runsc's own
 * checkpoint of its sandbox, running on, as a delta of a directory that holds nothing of the
 * parent (which must fail), then of one that holds the parent's pages_meta.img alone, each traced
 * for the files it opens; the delta's inspection and layers; and the session ended unsaved.
 */
async function parentMetadata(): Promise<Record<string, unknown>> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  await bindSession(workspace);
  try {
    const { session_id: sessionId } = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", "poller", "--size-mib", "64", "--run-id", `parent-metadata-${workspace}`, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
    const bound = await boundPodOf(workspace);
    if (!bound) throw new Error(`no pod is bound to ${workspace}`);
    const pod = await podNamed(bound.name);
    const bayma = pod.spec?.containers.find(({ name }) => name === BAYMA_CONTAINER);
    const hook = bayma ? hookOf(bayma) : null;
    const stateDir = bayma ? stateDirOf(bayma) : null;
    if (!hook || !stateDir) throw new Error(`the pod ${bound.name} runs no bayma with a state directory`);
    const containerId = containerIdOf(pod);
    const request = { environment: SESSIONS, workspace, namespace: SESSIONS, pod: bound.name, containerId, hook, podTemplate: await poolTemplate(pod), live: { stateDir } };
    const asking = join(BENCH_DIR, "checkpoint.cjs");
    const asked = join(BENCH_DIR, "checkpoint-request.json");
    writeFileSync(asking, CHECKPOINT);
    writeFileSync(asked, JSON.stringify(request));
    const live = JSON.parse(await inAgentNetwork(bound.node, [asking, String(NODE_AGENT_PORT), SESSIONS, workspace, asked])) as { readonly manifest: Manifest };
    const [layer] = live.manifest.layers;
    if (!layer || live.manifest.layers.length !== 1) throw new Error(`the live save of ${workspace} is not one whole image: ${live.manifest.layers.length} layers`);
    const parent = `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}/${LAYERS_DIR}/${layer.digest}`;
    const work = join(BENCH_DIR, "parent-metadata");
    const release = live.manifest.release;
    const flags = runscFlags(await onNode(bound.node, `cat ${CONTAINERD}/${gvisorHandler(release)}.toml`));
    const runsc = [`${gvisorDirectory(release)}/runsc`, `--root=${RUNSC_ROOT}`, ...flags].map((arg) => `'${arg}'`).join(" ");
    await onNode(bound.node, `mkdir -p ${work}/empty ${work}/metadata-only && cp ${parent}/pages_meta.img ${work}/metadata-only/ && (command -v strace >/dev/null || apt-get install -y -qq strace >/dev/null)`);
    const parentFiles = (await onNode(bound.node, `cd ${parent} && ls -l`)).trim().split("\n");
    const deltas: Record<string, unknown> = {};
    for (const name of ["empty", "metadata-only"]) {
      const delta = `${work}/delta-${name}`;
      const said = await onNode(bound.node, `strace -f -qq -e trace=openat,open -o ${work}/strace-${name}.txt ${runsc} checkpoint --image-path=${delta} --compression=none --parent-image-path=${work}/${name} --leave-running ${containerId} 2>&1; echo "exit $?"`);
      const opened = (await onNode(bound.node, `grep -E '${work}/${name}|${parent}' ${work}/strace-${name}.txt | grep -v ENOENT | sed -E 's/^[0-9]+ +//' | sort -u | head -n 40 || true`)).trim().split("\n").filter(Boolean);
      const missing = (await onNode(bound.node, `grep -E '${work}/${name}' ${work}/strace-${name}.txt | grep ENOENT | sed -E 's/^[0-9]+ +//' | sort -u | head -n 20 || true`)).trim().split("\n").filter(Boolean);
      const inspected = await onNode(bound.node, `${runsc} image inspect --json ${delta} 2>&1 | head -c 4000 || true`);
      const layers = await onNode(bound.node, `${runsc} image layers ${delta} 2>&1 || true`);
      const files = (await onNode(bound.node, `ls -l ${delta} 2>&1 || true`)).trim().split("\n");
      deltas[name] = { said: said.trim().split("\n").slice(-5), opened, missing, layers: layers.trim().split("\n"), refers_to_parent: layers.includes(layer.digest), files, inspected: inspected.slice(0, 4000) };
    }
    const sandboxLog = saveLogged(await bootLog(bound.node, await sandboxIdOf(bound.node, bound.name)));
    void sessionId;
    await endUnsaved(workspace, bound.name);
    return { workspace, parent: layer.digest, parent_files: parentFiles, deltas, logged: sandboxLog.said };
  } finally {
    await mainToken(workspace).then((token) => gatewayCall(token, "DELETE", "/workspace")).catch(() => {});
  }
}

/**
 * Spike: the store's own experiments, for the job's profile: the sweep of alasio's store from the
 * node agent's network, with the CPU its pods used, and its conditional PUTs;
 * the same sweep of a single SeaweedFS server on the runner, as the other systems' store
 * (bench/common/store/up.sh), through the same profile; and, on the local profile, whether a
 * delta needs only its parent's metadata; and, asked alone, the conditional PUTs again with
 * alasio's gateways and filers scaled out, as the box's may run several (`scaled`). Each in
 * logs/store-<what>.json. BENCH_STORE_SCALED=1 runs that alone.
 */
async function storeExperiments(): Promise<void> {
  const [node = ""] = (await nodeAgents()).keys();
  const write = (what: string, value: unknown) => writeFileSync(join(BENCH_DIR, "logs", `store-${what}.json`), `${JSON.stringify(value, null, 1)}\n`);
  const sweepScript = new URL("dist/sweep.mjs", COMMON).pathname;
  if (process.env["BENCH_STORE_SCALED"] === "1") {
    // Three gateways and two filers, each request of the racers going to any gateway through the Service.
    const kubectl = `k3s kubectl --namespace ${NAMESPACE}`;
    await onNode(node, `${kubectl} scale deployment/${STORE_GATEWAY} --replicas=3 && ${kubectl} scale statefulset/${STORE_GATEWAY}-filer --replicas=2`);
    await onNode(node, `${kubectl} rollout status deployment/${STORE_GATEWAY} --timeout=600s && ${kubectl} rollout status statefulset/${STORE_GATEWAY}-filer --timeout=600s`);
    const pods = (await onNode(node, `${kubectl} get pods -o wide`)).trim().split("\n").filter((line) => line.includes("seaweedfs"));
    write("conditional-alasio-scaled", { pods, ...(await withCpu(node, async () => JSON.parse(await inAgentNetwork(node, [sweepScript, "conditional", "--prefix", "bench-conditional-scaled/", "--races", "100", "--racers", "32"])))) });
    return;
  }
  for (const round of [1, 2, 3]) {
    write(`sweep-alasio-${round}`, await withCpu(node, async () => JSON.parse(await inAgentNetwork(node, [sweepScript, "sweep", "--prefix", `bench-sweep-${round}/`]))));
  }
  write("conditional-alasio", await withCpu(node, async () => JSON.parse(await inAgentNetwork(node, [sweepScript, "conditional", "--prefix", "bench-conditional/"]))));
  // The other systems' store, one server on the runner, its profile shaped by its own toxiproxy.
  const single = join(BENCH_DIR, "single-store");
  await onNode(node, `PATH=${dirname(process.execPath)}:$PATH ${new URL("store/up.sh", COMMON).pathname} ${single} ${PROFILE} > ${BENCH_DIR}/logs/single-store-up.txt 2>&1`);
  try {
    for (const round of [1, 2, 3]) {
      const started = Date.now();
      const cpuBefore = await onNode(node, "cat /sys/fs/cgroup/system.slice/docker-$(docker inspect --format '{{.Id}}' bench-seaweedfs).scope/cpu.stat | sed -n 's/^usage_usec //p'");
      const said = await onNode(node, `set -a; . ${single}/store.env; set +a; ${process.execPath} ${sweepScript} sweep --prefix bench-sweep-${round}/`);
      const cpuAfter = await onNode(node, "cat /sys/fs/cgroup/system.slice/docker-$(docker inspect --format '{{.Id}}' bench-seaweedfs).scope/cpu.stat | sed -n 's/^usage_usec //p'");
      write(`sweep-single-${round}`, { said: JSON.parse(said.trim().split("\n").at(-1) ?? "null"), seconds: (Date.now() - started) / 1000, cpu_s: { "bench-seaweedfs": (Number(cpuAfter) - Number(cpuBefore)) / 1e6 } });
    }
    write("conditional-single", JSON.parse((await onNode(node, `set -a; . ${single}/store.env; set +a; ${process.execPath} ${sweepScript} conditional`)).trim().split("\n").at(-1) ?? "null"));
  } finally {
    await onNode(node, "docker rm -f bench-seaweedfs bench-toxiproxy >/dev/null 2>&1 || true");
  }
  if (PROFILE === "local") write("parent-metadata", await parentMetadata());
}

before(async () => {
  try {
    mkdirSync(join(BENCH_DIR, "logs"), { recursive: true });
    await setUp();
    const agents = await nodeAgents();
    for (const node of agents.keys()) await bootLogs(node);
    await shapeAgents();
    for (const node of (await nodeAgents()).keys()) await countStore(node);
    if (VARIANT === "mixed-iocost") for (const node of (await nodeAgents()).keys()) await ioCost(node);
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
  if (MODE === "store") return await storeExperiments();
  for (let run = 1; run <= RUNS; run++) {
    for (const { fixture, size_mib } of CASES) {
      const started = Date.now();
      // Where the cluster itself fails under a run, what the runner says of it is kept before the run is.
      await runOnce(fixture, size_mib, run).catch(async (error: unknown) => {
        await sayRunner(`${fixture}-${size_mib}-${run}-aborted`);
        throw error;
      });
      console.error(`# ${fixture} ${size_mib} MiB run ${run} took ${Math.round((Date.now() - started) / 1000)} s`);
    }
  }
  // Spike: bayma's own spans of its sandbox's hook, as the lake has them once it has one of the last restore's: each one's
  // start and length, in logs/bayma-spans.jsonl, which data/build.py matches to the phase whose window holds its start.
  const spans = "select SpanName as name, epoch_ns(Timestamp) as at_ns, Duration as ns, SpanAttributes as attributes from otel.traces where SpanName like 'bayma.sandbox.%' order by Timestamp";
  const until = BigInt(lastRestoreAt) * 1_000_000n;
  let rows: Record<string, unknown>[] = [];
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(10_000)) {
    rows = await lakeQuery(spans).catch((error: unknown) => [{ error: errorText(error) }]);
    if (rows.some((row) => row["name"] === "bayma.sandbox.restore" && BigInt(String(row["at_ns"])) > until)) break;
  }
  writeFileSync(join(BENCH_DIR, "logs", "bayma-spans.jsonl"), rows.map((row) => JSON.stringify(row, (_key, value: unknown) => (typeof value === "bigint" ? String(value) : value))).join("\n"));
});
