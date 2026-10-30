/**
 * Spike, not for main: a warm restore of a small session traced end to end, RUNS times. Each
 * run makes a workspace, sets a Bun REPL in it (session-repl.ts set), has the gateway release
 * it into a snapshot (POST /release), waits SETTLE_MS, then reads the REPL back through the
 * gateway (session-repl.ts trace), which binds the workspace and so restores it from the node's
 * cache. Meanwhile the restore's pod is watched every WATCH_MS through the API server; after,
 * the gateway's and the node agent's `trace` lines (src/shared/log.ts), the sandbox's boot log
 * and the pod's events are read. Each run is printed as one `# trace-run <json>` line.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=trace node --test test/e2e/warm-restore-trace.test.ts
 */
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";

import type { CoreV1Event, V1Pod } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { BINDING_LABEL, FREE_LABEL, keyHash } from "../../src/binder/names.ts";
import { CONTAINERD, gvisorHandler } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, inAlasio, kube, lakeQuery, mainToken, nodeAgents, onNode, recordSession, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";
import type { ReplSet } from "./session-repl.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const RUNS = Number(process.env["TRACE_RUNS"] || 6);
/** How long after its release each session is restored. */
const SETTLE_MS = 10_000;
/** How often the restore's pod is looked at. */
const WATCH_MS = 50;
const RUNSC_LOGS = "/var/log/runsc";

/**
 * What starting a process costs in the restored sandbox, as runsc's resume runs bayma's hook: each
 * of node alone, node with node:net loaded, and bayma's own bundle, as its hook starts, three
 * times each, in milliseconds.
 */
const HOOK_COSTS = [
  "t() { a=$(date +%s%N); \"$@\" >/dev/null 2>&1; b=$(date +%s%N); echo \"$(( (b - a) / 1000000 ))\"; }",
  "for i in 1 2 3; do",
  "  echo \"node $(t node -e 0) net $(t node -e 'require(\"node:net\")') bayma $(t node /opt/bayma/bayma.js --version) true $(t true)\"",
  "done",
].join("\n");

/** What the pod looked like, first seen so, by when. */
type Seen = Record<string, number>;

/** Watches the restore's pod of `workspace` until `stop` says so: when each of its states was first seen. */
function watch(workspace: string, stop: () => boolean): Promise<{ readonly seen: Seen; readonly pod: string; readonly node: string }> {
  return (async () => {
    const seen: Seen = {};
    let pod = "";
    let node = "";
    const mark = (what: string) => void (seen[what] ??= Date.now());
    while (!stop()) {
      const [found] = (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` })).filter(({ metadata }) => metadata?.name?.includes("-restore-") && !metadata.deletionTimestamp);
      if (found) {
        pod = found.metadata?.name ?? "";
        node = found.spec?.nodeName ?? "";
        mark("pod-seen");
        if (found.spec?.nodeName) mark("pod-scheduled");
        const status = found.status?.containerStatuses?.[0];
        if (status?.containerID) mark("container-id");
        if (status?.state?.running) mark("container-running");
        if (found.status?.podIP) mark("pod-ip");
        if (status?.ready) mark("container-ready");
        for (const { type, status: holds } of found.status?.conditions ?? []) if (holds === "True") mark(`condition-${type}`);
      }
      await sleep(WATCH_MS);
    }
    return { seen, pod, node };
  })();
}

/** The trace lines of `workspace` every replica of the gateway logged since `since`: by what each marks, when. */
async function gatewayTrace(workspace: string, since: number): Promise<Seen> {
  const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) });
  const lines = await Promise.all(pods.map((pod) => kube.logs(NAMESPACE, pod.metadata?.name ?? "", "sandbox-gateway").catch(() => "")));
  return traced(lines.flatMap((logged) => logged.split("\n")), workspace, since);
}

/** The trace lines of `workspace` the node agent of `node` logged since `since`. */
async function agentTrace(node: string, workspace: string, since: number): Promise<Seen> {
  const pod = (await nodeAgents()).get(node) ?? "";
  return traced((await kube.logs(NAMESPACE, pod, "node-agent")).split("\n"), workspace, since);
}

/** Of `lines`, the first trace line of each step of `workspace`'s since `since`: `<who> <step>` by its millisecond; what one said beside, at 0. */
function traced(lines: readonly string[], workspace: string, since: number): Seen {
  const seen: Seen = {};
  for (const line of lines) {
    const matched = /trace (\S+) (\S+) (\S+)(.*) @(\d+)$/u.exec(line);
    if (!matched || !matched[1]?.includes(workspace) || Number(matched[5]) < since) continue;
    const step = `${matched[2]} ${matched[3]}`;
    if (step in seen) continue;
    seen[step] = Number(matched[5]);
    if (matched[4]?.trim()) seen[`${step} said ${matched[4].trim()}`] = 0;
  }
  return seen;
}

/** The time of day of a runsc log line (I1026 20:01:02.123456 ...), as a time of `day`, in milliseconds since the epoch. */
function bootLineMs(line: string, day: number): number | null {
  const matched = /^[IWEF]\d{4} (\d{2}):(\d{2}):(\d{2}\.\d+)/u.exec(line);
  if (!matched) return null;
  const midnight = Math.floor(day / 86_400_000) * 86_400_000;
  return midnight + (Number(matched[1]) * 3600 + Number(matched[2]) * 60 + Number(matched[3])) * 1000;
}

/** Of the boot log of the restore's pod sandbox, its first line's time and those of the lines that say how its restore went. */
async function bootTrace(node: string, pod: string, day: number): Promise<{ readonly seen: Seen; readonly said: string[] }> {
  const sandbox = (await onNode(node, `(k3s crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q 2>/dev/null || crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q) | head -n 1`)).trim();
  if (sandbox === "") return { seen: {}, said: ["no sandbox"] };
  const lines = (await onNode(node, `cat ${RUNSC_LOGS}/${sandbox}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
  const seen: Seen = {};
  const said: string[] = [];
  const first = lines.find((line) => bootLineMs(line, day) !== null);
  if (first) seen["runsc first-line"] = bootLineMs(first, day) ?? 0;
  for (const line of lines) {
    if (!/[Rr]estor|[Rr]esum|Async page|[Pp]aused|working set|Kernel loaded|Load(ed|ing)/u.test(line)) continue;
    said.push(line.slice(0, 300));
    const at = bootLineMs(line, day);
    if (at === null) continue;
    if (line.includes("Restore successful")) seen["runsc restore-successful"] ??= at;
    if (line.includes("Async page loading completed")) seen["runsc pages-loaded"] ??= at;
    if (/[Rr]esum/u.test(line)) seen["runsc resume-line"] ??= at;
  }
  return { seen, said: said.slice(0, 40) };
}

/** The pod's events, as kubelet and the scheduler said them. */
async function eventsOf(pod: string): Promise<string[]> {
  const events = await kube.list<CoreV1Event>("Event", { namespace: SESSIONS, fieldSelector: `involvedObject.name=${pod}` });
  return events.map((event) => `${event.reason} x${event.count ?? 1} first=${String(event.firstTimestamp ?? event.eventTime)} last=${String(event.lastTimestamp)}: ${event.message ?? ""}`.slice(0, 300));
}

/** Kubelet's lines of `pod` on the host, at the microsecond, as journald keeps k3s's. */
async function kubeletOf(pod: string): Promise<string[]> {
  if (TARGET !== "host") return [];
  return (await onNode("", `grep -F '${pod}' /var/log/k3s-spike.log | grep -E 'SyncLoop|status_manager|atus for pod|RECONCILE|eadiness|prober|Probe|pod_workers|syncPod|PATCH|/status' | head -n 400 || true`)).split("\n").filter(Boolean).map((line) => line.slice(0, 600));
}

/** Waits until no pod is bound to `workspace`. */
async function unbound(workspace: string): Promise<void> {
  for (const deadline = Date.now() + 300_000; ; await sleep(250)) {
    if ((await boundPodOf(workspace)) === null) return;
    if (Date.now() > deadline) throw new Error(`${workspace} is still bound 300 s after its release`);
  }
}

/** Has the gateway release `workspace`, as alasio asks it. */
async function release(workspace: string): Promise<void> {
  const released = await gatewayCall(await mainToken(workspace), "POST", "/release");
  if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release of ${workspace} answered ${released.status}: ${released.said}`);
  await unbound(workspace);
}

async function runOnce(run: number): Promise<void> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  recordSession(workspace, "none");
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  await bindSession(workspace);
  const { sessionId } = await inAlasio<ReplSet>("./session-repl.ts", "set", workspace, `held by ${workspace}`);
  await release(workspace);
  await sleep(SETTLE_MS);
  let done = false;
  const watching = watch(workspace, () => done);
  const client = await inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId).finally(() => {
    done = true;
  });
  const { seen, pod, node } = await watching;
  await sleep(2_000);
  const day = Number(client["asked"]);
  const boot = await bootTrace(node, pod, day).catch((error: unknown) => ({ seen: {}, said: [String(error)] }));
  const steps: Seen = {
    ...Object.fromEntries(["asked", "connecting", "connected", "acquired", "answered", "again"].map((step) => [`client ${step}`, Number(client[step])])),
    ...Object.fromEntries(Object.entries(seen).map(([step, at]) => [`api ${step}`, at])),
    ...(await gatewayTrace(workspace, Number(client["connecting"]))),
    ...(await agentTrace(node, workspace, Number(client["connecting"]))),
    ...boot.seen,
  };
  const zero = steps["client connecting"] ?? 0;
  const timeline = Object.entries(steps).filter(([, at]) => at > 0).map(([step, at]) => [step, at - zero] as const).toSorted(([, a], [, b]) => a - b);
  const said = Object.keys(steps).filter((step) => steps[step] === 0);
  const starts = await kube.execOk(SESSIONS, pod, ["sh", "-c", HOOK_COSTS], { container: "bayma" }).catch((error: unknown) => String(error));
  console.log(`# trace-run ${JSON.stringify({ run, target: TARGET, workspace, pod, node, notice: client["notice"], live: client["live"], timeline, said, starts, boot: boot.said, events: await eventsOf(pod), kubelet: await kubeletOf(pod) })}`);
  await release(workspace);
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
    const removed = await gatewayCall(await mainToken(workspace), "DELETE", "/workspace");
    if (removed.status !== 409) break;
  }
}

/** Adds a boot log to each kept release's runsc options, for the boot command alone, and has the pools' free pods made again with it. */
async function bootLogs(node: string): Promise<void> {
  for (const { release: kept } of NODE_PINS.gvisor.releases) {
    const options = `${CONTAINERD}/${gvisorHandler(kept)}.toml`;
    await onNode(node, `grep -q '^  debug-log' ${options} || printf '  debug-log = "${RUNSC_LOGS}/%%ID%%/"\\n  debug-command = "boot"\\n' >> ${options}; tail -n 4 ${options}`);
  }
  const free = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` });
  for (const pod of free) await kube.remove(ref("Pod", pod.metadata?.name ?? "", SESSIONS));
}

before(async () => {
  try {
    await setUp();
    for (const node of (await nodeAgents()).keys()) await bootLogs(node);
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 150 * 60_000 });

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

test(`a small session's warm restore traced, ${RUNS} times`, { timeout: 120 * 60_000 }, async () => {
  for (let run = 1; run <= RUNS; run++) await runOnce(run);
  // What reading the node's filter table costs, whole as the agent's policy check reads it, and by chain: 20 of each, in ms.
  const costs = TARGET !== "host" ? "" : await onNode("", [
    "t() { a=$(date +%s%N); for i in $(seq 20); do \"$@\" >/dev/null 2>&1; done; b=$(date +%s%N); echo \"$(( (b - a) / 20000 ))\"; }",
    "chain=$(iptables-save -t filter | grep -o 'KUBE-POD-FW-[A-Z0-9]*' | head -n 1)",
    "for bin in iptables ip6tables; do $bin -t filter -S KUBE-POD-FW-NONE; echo \"$bin missing exit $?\"; $bin -t filter -S KUBE-ROUTER-FORWARD | head -n 3; done 2>&1",
    "echo \"bytes $(iptables-save -t filter | wc -c) rules $(iptables-save -t filter | grep -c '^-A') save $(t iptables-save -t filter) forward $(t iptables -t filter -S KUBE-ROUTER-FORWARD) pod $(t iptables -t filter -S \"$chain\") nsenter $(t nsenter --target 1 --mount --net -- true) iptables $(readlink -f $(command -v iptables))\"",
  ].join("\n")).catch((error: unknown) => String(error));
  console.log(`# trace-policy-costs ${JSON.stringify(costs)}`);
  // bayma's own spans of its sandbox's hook, as the lake has them, once it has them all: each one's start and length.
  const spans = "select SpanName as name, epoch_ns(Timestamp) as at, Duration as ns, ResourceAttributes as resource from otel.traces where SpanName like 'bayma.sandbox.%' order by Timestamp";
  let rows: Record<string, unknown>[] = [];
  for (const deadline = Date.now() + 600_000; Date.now() < deadline; await sleep(10_000)) {
    rows = await lakeQuery(spans).catch((error: unknown) => [{ error: String(error) }]);
    if (rows.filter((row) => row["name"] === "bayma.sandbox.restore").length >= RUNS) break;
  }
  console.log(`# trace-spans ${JSON.stringify(rows, (_key, value: unknown) => (typeof value === "bigint" ? String(value) : value))}`);
});
