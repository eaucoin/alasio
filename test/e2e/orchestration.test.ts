/**
 * Spike, not for main: the orchestration experiments of the endpoint package's validation
 * (E3, E4, E9, E10), each a part of this file, ORCH_PARTS naming those run (all if unset).
 *
 * - `restore` (E3, E10): RUNS times, a workspace made and bound at once (its first attach, of
 *   a quota set a moment before), a Bun REPL set in it (session-repl.ts set), released by the
 *   gateway (POST /release), restored SETTLE_MS later by reading it back (session-repl.ts
 *   trace). Meanwhile the restore's pod is watched every WATCH_MS through the API server and
 *   containerd's events are followed on the node; after, the gateway's and the agent's `trace`
 *   lines (src/shared/log.ts), the quota waits of both attaches among them, the sandbox's boot
 *   log, kubelet's and kube-router's lines (k3s at v=3, cli/src/cluster/node.ts) and the pod's
 *   events are read. Each run is one `# orch-restore <json>` line.
 * - `density` (E4): for each count of DENSITY_PODS, that many idle pods (the pause image, runc)
 *   on the sessions' node, in a namespace of the test's own (DENSITY_NAMESPACE: alasio's admission
 *   policy lets no pod but a pool's into the sessions' namespace), under network policies of its
 *   own shaped as the pools' are (a default deny, ingress and egress for the pool's pods, the full
 *   internet for those labelled so); then PROBES pods made, each timed from its address in the API
 *   to its rules in the node's filter table; then RELABELS relabels of one idle pod into and out
 *   of the full-internet policy, each timed from the patch to its rules. kube-router's own sync
 *   times are read from its lines. Each count is one `# orch-density <json>` line.
 * - `pause` (E9): a bound session's bayma container paused and resumed through containerd's task
 *   API (`ctr task pause`, the runsc shim's Pause), PAUSES times: what containerd, runsc, kubelet
 *   and the API say of it paused, its pod cgroup's memory, an exec asked while it is paused and
 *   answered once it is resumed (thaw to serving), and an exec after (serving). One of them held
 *   paused for LONG_PAUSE_MS, to see what kubelet's probes and alasio's sweep do. Each pause is
 *   one `# orch-pause <json>` line.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=orch node --test test/e2e/orchestration.test.ts
 */
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";

import type { CoreV1Event, KubernetesObject, V1Namespace, V1Pod } from "@kubernetes/client-node";

import { SANDBOX_IMAGE } from "../../cli/src/cluster/k3s.ts";
import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { BINDING_LABEL, FREE_LABEL, keyHash } from "../../src/binder/names.ts";
import { BAYMA_CONTAINER } from "../../src/kube/sandbox-shape.ts";
import { NODE_PATH } from "../../src/node-agent/host.ts";
import { CONTAINERD, gvisorDirectory, gvisorHandler, RUNSC_ROOT } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, inAlasio, kube, mainToken, nodeAgents, onNode, recordSession, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";
import type { ReplSet } from "./session-repl.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const PARTS = (process.env["ORCH_PARTS"] || "restore density pause").split(/[\s,]+/u).filter(Boolean);
const RUNS = Number(process.env["ORCH_RUNS"] || 12);
const DENSITY_PODS = (process.env["ORCH_DENSITY_PODS"] || "1 10 30 50").split(/[\s,]+/u).filter(Boolean).map(Number);
const PROBES = Number(process.env["ORCH_PROBES"] || 10);
const RELABELS = Number(process.env["ORCH_RELABELS"] || 20);
const PAUSES = Number(process.env["ORCH_PAUSES"] || 20);
const LONG_PAUSE_MS = 60_000;
/** How long after its release each session is restored. */
const SETTLE_MS = 10_000;
/** How often the restore's pod is looked at. */
const WATCH_MS = 50;
const RUNSC_LOGS = "/var/log/runsc";
const K3S_LOG = "/var/log/k3s-spike.log";
const CTR_EVENTS = "/var/log/ctr-events.log";
/** The density part's namespace, its pods' label, and the label its full-internet policy selects. */
const DENSITY_NAMESPACE = "val-density";
const IDLE_LABEL = "val.alasio.dev/idle";
const FULL_LABEL = "val.alasio.dev/net";

/** When each step was first seen, in milliseconds since the epoch. */
type Seen = Record<string, number>;

/** Watches the restore's pod of `workspace` until `stop` says so: when each of its states was first seen. */
function watch(workspace: string, stop: () => boolean): Promise<{ readonly seen: Seen; readonly pod: string; readonly node: string; readonly ip: string }> {
  return (async () => {
    const seen: Seen = {};
    let pod = "";
    let node = "";
    let ip = "";
    const mark = (what: string) => void (seen[what] ??= Date.now());
    while (!stop()) {
      const [found] = (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}` })).filter(({ metadata }) => metadata?.name?.includes("-restore-") && !metadata.deletionTimestamp);
      if (found) {
        pod = found.metadata?.name ?? "";
        node = found.spec?.nodeName ?? "";
        mark("pod-seen");
        if (found.spec?.nodeName) mark("pod-scheduled");
        const statuses = found.status?.containerStatuses ?? [];
        if (statuses.some(({ containerID }) => containerID)) mark("container-id");
        if (statuses.length > 0 && statuses.every(({ state }) => state?.running)) mark("containers-running");
        if (found.status?.podIP) {
          mark("pod-ip");
          ip = found.status.podIP;
        }
        if (statuses.some(({ ready }) => ready)) mark("container-ready");
        for (const { type, status: holds } of found.status?.conditions ?? []) if (holds === "True") mark(`condition-${type}`);
      }
      await sleep(WATCH_MS);
    }
    return { seen, pod, node, ip };
  })();
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

/** The trace lines of `workspace` every replica of the gateway logged since `since`. */
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

/** The time of day of a klog line (I1026 20:01:02.123456 ...), as a time of `day`, in milliseconds since the epoch. */
function klogMs(line: string, day: number): number | null {
  const matched = /^[IWEF]\d{4} (\d{2}):(\d{2}):(\d{2}\.\d+)/u.exec(line);
  if (!matched) return null;
  const midnight = Math.floor(day / 86_400_000) * 86_400_000;
  return midnight + (Number(matched[1]) * 3600 + Number(matched[2]) * 60 + Number(matched[3])) * 1000;
}

/** The id of the pod sandbox of `pod`, as containerd names it. */
async function sandboxIdOf(node: string, pod: string): Promise<string> {
  return (await onNode(node, `(k3s crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q 2>/dev/null || crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q) | head -n 1`)).trim();
}

/** Of the boot log of the pod sandbox `sandbox`, its first line's time and those of the lines that say how its restore went. */
async function bootTrace(node: string, sandbox: string, day: number): Promise<{ readonly seen: Seen; readonly said: string[] }> {
  if (sandbox === "") return { seen: {}, said: ["no sandbox"] };
  const lines = (await onNode(node, `cat ${RUNSC_LOGS}/${sandbox}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
  const seen: Seen = {};
  const said: string[] = [];
  const first = lines.find((line) => klogMs(line, day) !== null);
  if (first) seen["runsc first-line"] = klogMs(first, day) ?? 0;
  for (const line of lines) {
    if (!/[Rr]estor|[Rr]esum|[Pp]aus|Kernel load|Load(ed|ing)|Dirty tracking|Async page loading completed/u.test(line)) continue;
    said.push(line.slice(0, 300));
    const at = klogMs(line, day);
    if (at === null) continue;
    if (line.includes("Restore successful")) seen["runsc restore-successful"] ??= at;
    if (line.includes("Async page loading completed")) seen["runsc pages-loaded"] ??= at;
  }
  return { seen, said: said.slice(0, 60) };
}

/** containerd's events of the task `id` since `since`, as `ctr events` printed them: by topic, its first time. */
async function eventsOfTask(node: string, ids: readonly string[], since: number): Promise<Seen> {
  if (ids.length === 0) return {};
  const lines = (await onNode(node, `grep -F -e ${ids.map((id) => `'${id}'`).join(" -e ")} ${CTR_EVENTS} || true`)).split("\n");
  const seen: Seen = {};
  for (const line of lines) {
    // 2026-11-01 10:00:00.123456789 +0000 UTC k8s.io /tasks/start {"container_id":"...","pid":1}
    const matched = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(\.\d+)? \+0000 UTC \S+ (\/\S+) (.*)$/u.exec(line);
    if (!matched) continue;
    const at = Date.parse(`${matched[1]}T${matched[2]}${(matched[3] ?? "").slice(0, 4)}Z`);
    if (at < since) continue;
    const which = ids.findIndex((id) => matched[5]?.includes(id));
    seen[`containerd ${matched[4]} ${which === 0 ? "sandbox" : `container${which}`}`] ??= at;
  }
  return seen;
}

/** The pod's events, as kubelet and the scheduler said them. */
async function eventsOf(pod: string): Promise<string[]> {
  const events = await kube.list<CoreV1Event>("Event", { namespace: SESSIONS, fieldSelector: `involvedObject.name=${pod}` });
  return events.map((event) => `${event.reason} x${event.count ?? 1} first=${String(event.firstTimestamp ?? event.eventTime)} last=${String(event.lastTimestamp)}: ${event.message ?? ""}`.slice(0, 300));
}

/** k3s's lines (kubelet's, at v=3) of `pod` since `since`, with their times. */
async function kubeletOf(pod: string, since: number): Promise<{ readonly seen: Seen; readonly lines: string[] }> {
  if (TARGET !== "host") return { seen: {}, lines: [] };
  const lines = (await onNode("", `grep -F '${pod}' ${K3S_LOG} | grep -v -F 'trace ' | grep -E 'SyncLoop|status_manager|atus for pod|PLEG|ContainerStarted|podIP|PodIP|syncPod|Patch|prober|Probe' | tail -n 400 || true`)).split("\n").filter(Boolean);
  const seen: Seen = {};
  const kept: string[] = [];
  for (const line of lines) {
    const at = klogMs(line, since);
    if (at === null || at < since - 1000) continue;
    kept.push(line.slice(0, 500));
    if (/[Pp]atch status|tatus for pod updated/u.test(line) && /10\.\d+\.\d+\.\d+/u.test(line)) seen["kubelet status-with-ip"] ??= at;
    if (line.includes("ContainerStarted")) seen["kubelet pleg-container-started"] ??= at;
  }
  return { seen, lines: kept.slice(-120) };
}

/** kube-router's syncs since `since`, as its lines at v=1 say: when each ended, and how long it took, in milliseconds. */
async function syncsSince(since: number): Promise<{ readonly at: number; readonly ms: number }[]> {
  if (TARGET !== "host") return [];
  const lines = (await onNode("", `grep -F 'sync iptables took' ${K3S_LOG} | tail -n 2000 || true`)).split("\n").filter(Boolean);
  return lines.flatMap((line) => {
    const at = klogMs(line, since);
    const took = /sync iptables took (\S+)/u.exec(line)?.[1];
    return at !== null && at >= since && took ? [{ at, ms: goMs(took) }] : [];
  });
}

/** Milliseconds in a Go duration's string, as 812ms, 1.2s or 40µs. */
function goMs(text: string): number {
  const units: Record<string, number> = { h: 3_600_000, m: 60_000, s: 1000, ms: 1, us: 1e-3, "µs": 1e-3, ns: 1e-6 };
  return [...text.matchAll(/([\d.]+)(h|ms|m|s|us|µs|ns)/gu)].reduce((sum, [, value, unit]) => sum + Number(value) * (units[unit ?? "ms"] ?? 0), 0);
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

/** Deletes `workspace` once nothing is bound to it. */
async function removeWorkspace(workspace: string): Promise<void> {
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
    const removed = await gatewayCall(await mainToken(workspace), "DELETE", "/workspace");
    if (removed.status !== 409) break;
  }
}

/** A workspace made, bound at once, and holding a Bun REPL: its name, its REPL's session, and the agent's trace of its first attach. */
async function freshSession(): Promise<{ readonly workspace: string; readonly sessionId: string; readonly node: string; readonly attach: Seen }> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  recordSession(workspace, "none");
  const asked = Date.now();
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  const bound = await bindSession(workspace);
  const bindMs = Date.now() - asked;
  const { sessionId } = await inAlasio<ReplSet>("./session-repl.ts", "set", workspace, `held by ${workspace}`);
  const attach = { ...(await agentTrace(bound.node, workspace, asked)), "client bind-ms": bindMs };
  return { workspace, sessionId, node: bound.node, attach };
}

async function restoreOnce(run: number): Promise<void> {
  const { workspace, sessionId, node: madeOn, attach } = await freshSession();
  await release(workspace);
  await sleep(SETTLE_MS);
  let done = false;
  const watching = watch(workspace, () => done);
  const client = await inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId).finally(() => {
    done = true;
  });
  const { seen, pod, node } = await watching;
  await sleep(2_000);
  const since = Number(client["connecting"]);
  const sandbox = await sandboxIdOf(node, pod).catch(() => "");
  const boot = await bootTrace(node, sandbox, since).catch((error: unknown) => ({ seen: {}, said: [String(error)] }));
  const restorePod = await kube.get<V1Pod>(ref("Pod", pod, SESSIONS));
  const containerIds = (restorePod?.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^[a-z]+:\/\//u, "")] : []));
  const kubelet = await kubeletOf(pod, since);
  const steps: Seen = {
    ...Object.fromEntries(["asked", "connecting", "connected", "acquired", "answered", "again"].map((step) => [`client ${step}`, Number(client[step])])),
    ...Object.fromEntries(Object.entries(seen).map(([step, at]) => [`api ${step}`, at])),
    ...(await gatewayTrace(workspace, since)),
    ...(await agentTrace(node, workspace, since)),
    ...(await eventsOfTask(node, [sandbox, ...containerIds], since - 5000)),
    ...kubelet.seen,
    ...boot.seen,
  };
  const zero = steps["client connecting"] ?? 0;
  const timeline = Object.entries(steps).filter(([, at]) => at > 0).map(([step, at]) => [step, at - zero] as const).toSorted(([, a], [, b]) => a - b);
  const said = Object.keys(steps).filter((step) => steps[step] === 0);
  const syncs = (await syncsSince(since)).filter(({ at }) => at <= Number(client["again"]) + 1000).map(({ at, ms }) => [at - zero, ms]);
  console.log(`# orch-restore ${JSON.stringify({ run, target: TARGET, workspace, pod, node, madeOn, notice: client["notice"], live: client["live"], timeline, said, attach, syncs, boot: boot.said, events: await eventsOf(pod), kubelet: kubelet.lines })}`);
  await release(workspace);
  await removeWorkspace(workspace);
}

/** The node every session's pod runs on, as the pool's free pods say. */
async function sessionsNode(): Promise<string> {
  const [free] = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` });
  const node = free?.spec?.nodeName;
  if (!node) throw new Error("no free pod of a pool says which node sessions run on");
  return node;
}

/** An idle pod named `name` on `node`, of the pause image, run by runc, in DENSITY_NAMESPACE. */
function idlePod(name: string, node: string): V1Pod {
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: DENSITY_NAMESPACE, labels: { [IDLE_LABEL]: "true" } },
    spec: {
      nodeName: node,
      automountServiceAccountToken: false,
      enableServiceLinks: false,
      terminationGracePeriodSeconds: 0,
      securityContext: { runAsNonRoot: true, runAsUser: 65535, runAsGroup: 65535, seccompProfile: { type: "RuntimeDefault" } },
      containers: [{
        name: "idle",
        image: SANDBOX_IMAGE,
        imagePullPolicy: "IfNotPresent",
        resources: { requests: { cpu: "1m", memory: "4Mi" }, limits: { memory: "16Mi" } },
        securityContext: { allowPrivilegeEscalation: false, capabilities: { drop: ["ALL"] }, readOnlyRootFilesystem: true },
      }],
    },
  };
}

/**
 * DENSITY_NAMESPACE and its network policies, shaped as the sessions' are
 * (cli/src/manifests/network-policies.ts sessionPolicies, poolPolicies): a default deny; the idle
 * pods reached from main's namespace on one port and reaching it on one; those labelled FULL_LABEL
 * full reaching the internet beside. Returns the full-internet policy's name.
 */
async function densityNamespace(): Promise<string> {
  await kube.apply({ apiVersion: "v1", kind: "Namespace", metadata: { name: DENSITY_NAMESPACE } } as V1Namespace);
  // As the API server takes it, its ingress's peers under `from` (client-node's model says `_from`).
  const policy = (name: string, spec: Record<string, unknown>): KubernetesObject => ({ apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name, namespace: DENSITY_NAMESPACE }, spec } as KubernetesObject);
  const main = { namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": NAMESPACE } } };
  await kube.apply(policy("val-default-deny", { podSelector: {}, policyTypes: ["Ingress", "Egress"] }));
  await kube.apply(policy("val-pool", {
    podSelector: { matchLabels: { [IDLE_LABEL]: "true" } },
    policyTypes: ["Ingress", "Egress"],
    ingress: [{ from: [main], ports: [{ protocol: "TCP", port: 7290 }] }],
    egress: [{ to: [main], ports: [{ protocol: "TCP", port: 4318 }] }],
  }));
  await kube.apply(policy("val-full", {
    podSelector: { matchLabels: { [IDLE_LABEL]: "true", [FULL_LABEL]: "full" } },
    policyTypes: ["Egress"],
    egress: [{ to: [{ ipBlock: { cidr: "0.0.0.0/0", except: ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"] } }] }],
  }));
  return "val-full";
}

/** Waits until the pod `name` runs with an address: when the API first said its address, and the address. */
async function addressed(name: string, timeoutMs = 120_000): Promise<{ readonly at: number; readonly ip: string }> {
  for (const deadline = Date.now() + timeoutMs; ; await sleep(20)) {
    const pod = await kube.get<V1Pod>(ref("Pod", name, DENSITY_NAMESPACE));
    if (pod?.status?.podIP) return { at: Date.now(), ip: pod.status.podIP };
    if (Date.now() > deadline) throw new Error(`${DENSITY_NAMESPACE}/${name} has no address after ${timeoutMs} ms: ${JSON.stringify(pod?.status ?? null)}`);
  }
}

/**
 * Polls the node's filter table every 10 ms, for up to `timeoutMs`, until the rules of the pod
 * of address `ip` hold `policy`'s jump (`present`) or no longer do, and its chain is there: when
 * they first did, in milliseconds since the epoch, and how many looks it took; null where they
 * did not in time. As the node agent lists them (src/node-agent/agent.ts policiesInForce):
 * KUBE-ROUTER-FORWARD, then the pod's chain its jump names.
 */
async function rulesWhen(node: string, ip: string, policy: string, present: boolean, timeoutMs = 30_000): Promise<{ readonly at: number | null; readonly looks: number }> {
  const script = [
    `export PATH='${NODE_PATH}'`,
    `end=$(( $(date +%s%3N) + ${timeoutMs} )); looks=0`,
    `while [ "$(date +%s%3N)" -lt "$end" ]; do`,
    `  looks=$((looks + 1))`,
    `  chain=$(iptables -t filter -S KUBE-ROUTER-FORWARD 2>/dev/null | grep -F -- '-s ${ip}/32' | grep -o 'KUBE-POD-FW-[A-Z0-9]*' | head -n 1)`,
    `  if [ -n "$chain" ]; then`,
    `    if iptables -t filter -S "$chain" 2>/dev/null | grep -qF '${policy}'; then has=1; else has=0; fi`,
    `    if [ "$has" = ${present ? 1 : 0} ]; then echo "at $(date +%s%3N) looks $looks"; exit 0; fi`,
    `  fi`,
    `  sleep 0.01`,
    `done`,
    `echo "none looks $looks"`,
  ].join("\n");
  const said = (await onNode(node, script)).trim();
  const matched = /at (\d+) looks (\d+)/u.exec(said);
  return matched ? { at: Number(matched[1]), looks: Number(matched[2]) } : { at: null, looks: Number(/looks (\d+)/u.exec(said)?.[1] ?? 0) };
}

const median = (values: readonly number[]): number | null => {
  const sorted = values.toSorted((a, b) => a - b);
  return sorted.length === 0 ? null : (sorted[Math.floor((sorted.length - 1) / 2)] ?? null);
};

async function density(): Promise<void> {
  const node = await sessionsNode();
  const full = await densityNamespace();
  let made = 0;
  for (const count of DENSITY_PODS) {
    for (; made < count; made++) await kube.apply(idlePod(`val-idle-${made}`, node));
    for (let index = 0; index < count; index++) await addressed(`val-idle-${index}`);
    const onNodeNow = (await kube.list<V1Pod>("Pod", { fieldSelector: `spec.nodeName=${node}` })).filter(({ status }) => status?.phase === "Running").length;
    const syncFrom = Date.now();
    // A pod made, from its address in the API to its rules on the node, as a restore's pod waits for them.
    const probes: { readonly ipToRulesMs: number | null; readonly looks: number }[] = [];
    for (let probe = 0; probe < PROBES; probe++) {
      const name = `val-probe-${count}-${probe}`;
      await kube.apply(idlePod(name, node));
      const { at, ip } = await addressed(name);
      const rules = await rulesWhen(node, ip, "KUBE-NWPLCY-", true);
      probes.push({ ipToRulesMs: rules.at === null ? null : rules.at - at, looks: rules.looks });
      await kube.remove(ref("Pod", name, DENSITY_NAMESPACE));
    }
    // One idle pod relabelled into the full-internet policy and out again, from the patch to its rules.
    const relabelled = "val-idle-0";
    const { ip } = await addressed(relabelled);
    const relabels: { readonly into: boolean; readonly patchMs: number; readonly patchToRulesMs: number | null; readonly looks: number }[] = [];
    for (let relabel = 0; relabel < RELABELS; relabel++) {
      const into = relabel % 2 === 0;
      const polling = rulesWhen(node, ip, full, into);
      await sleep(30);
      const patched = Date.now();
      await kube.patch(ref("Pod", relabelled, DENSITY_NAMESPACE), { metadata: { labels: { [FULL_LABEL]: into ? "full" : null } } });
      const patchMs = Date.now() - patched;
      const rules = await polling;
      relabels.push({ into, patchMs, patchToRulesMs: rules.at === null ? null : rules.at - patched, looks: rules.looks });
    }
    const syncs = await syncsSince(syncFrom);
    const rulesBytes = Number((await onNode(node, `export PATH='${NODE_PATH}'; iptables-save -t filter | wc -c`)).trim());
    console.log(`# orch-density ${JSON.stringify({
      idle: count,
      podsOnNode: onNodeNow,
      filterTableBytes: rulesBytes,
      ipToRulesMedianMs: median(probes.flatMap(({ ipToRulesMs }) => (ipToRulesMs === null ? [] : [ipToRulesMs]))),
      patchToRulesMedianMs: median(relabels.flatMap(({ patchToRulesMs }) => (patchToRulesMs === null ? [] : [patchToRulesMs]))),
      syncMedianMs: median(syncs.map(({ ms }) => ms)),
      probes,
      relabels,
      syncs: syncs.map(({ at, ms }) => [at - syncFrom, ms]),
    })}`);
  }
  for (let index = 0; index < made; index++) await kube.remove(ref("Pod", `val-idle-${index}`, DENSITY_NAMESPACE));
}

/** The task of `id` as containerd lists it, as `ctr task ls` prints its line: its status. */
async function taskStatus(node: string, id: string): Promise<string> {
  return (await onNode(node, `k3s ctr -n k8s.io task ls | awk '$1 == "${id}" { print $3 }'`)).trim();
}

/** What runsc says of the containers under the shim's root: each's id, its first 12 characters, and status. */
async function runscStatus(node: string): Promise<string> {
  const binary = `${gvisorDirectory(NODE_PINS.gvisor.releases[0]?.release ?? "")}/runsc`;
  return (await onNode(node, `${binary} --root=${RUNSC_ROOT} list --format=json 2>&1 | grep -o '"id":"[0-9a-f]\\{12\\}\\|"status":"[a-z]*"' | paste -sd ' ' || true`)).trim();
}

/** The memory its pod's cgroup charges, as memory.current says, in bytes. */
async function podMemory(node: string, uid: string): Promise<number | null> {
  const printed = await onNode(node, `dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name '*pod${uid}*' -o -name '*pod${uid.replaceAll("-", "_")}*' \\) | head -n 1); [ -n "$dir" ] && cat "$dir/memory.current" || true`);
  return printed.trim() === "" ? null : Number(printed.trim());
}

/** The bayma container's id and the pod's uid and sandbox of the pod `name`. */
async function podIds(node: string, name: string): Promise<{ readonly container: string; readonly uid: string; readonly sandbox: string; readonly pod: V1Pod }> {
  const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
  if (!pod) throw new Error(`there is no pod ${SESSIONS}/${name}`);
  const container = pod.status?.containerStatuses?.find(({ name: named }) => named === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";
  return { container, uid: pod.metadata?.uid ?? "", sandbox: await sandboxIdOf(node, name), pod };
}

/** The pod's state as the API has it: phase, its containers' ready, running and restarts, and its conditions. */
const apiState = (pod: V1Pod | null): string =>
  JSON.stringify({
    phase: pod?.status?.phase,
    containers: (pod?.status?.containerStatuses ?? []).map(({ name, ready, restartCount, state }) => ({ name, ready, restartCount, state: Object.keys(state ?? {}) })),
    conditions: (pod?.status?.conditions ?? []).map(({ type, status }) => `${type}=${status}`),
  });

async function pauseOnce(index: number, workspace: string, sessionId: string, node: string, podName: string, holdMs: number): Promise<void> {
  const { container, uid, sandbox } = await podIds(node, podName);
  const memoryBefore = await podMemory(node, uid);
  const since = Date.now();
  const pausing = Date.now();
  const paused = await onNode(node, `k3s ctr -n k8s.io task pause ${container} 2>&1; echo "exit $?"`);
  const pausedAt = Date.now();
  const status = { task: await taskStatus(node, container), sandboxTask: await taskStatus(node, sandbox), runsc: await runscStatus(node), crictl: (await onNode(node, `k3s crictl inspect ${container} 2>/dev/null | grep -m1 '"state"' || true`)).trim() };
  // Asked while paused: answered once resumed.
  const asked = inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId).then((seen) => ({ seen }), (error: unknown) => ({ error: String(error) }));
  await sleep(holdMs);
  const whilePaused = apiState(await kube.get<V1Pod>(ref("Pod", podName, SESSIONS)));
  const memoryPaused = await podMemory(node, uid);
  const resuming = Date.now();
  const resumed = await onNode(node, `k3s ctr -n k8s.io task resume ${container} 2>&1; echo "exit $?"`);
  const resumedAt = Date.now();
  const answer = await asked;
  await sleep(1000);
  const after = await inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId).catch((error: unknown) => ({ error: String(error) }));
  const events = await eventsOfTask(node, [sandbox, container], since - 1000);
  const answered = "seen" in answer ? Number(answer.seen["answered"]) : null;
  console.log(`# orch-pause ${JSON.stringify({
    index,
    holdMs,
    pauseCommandMs: pausedAt - pausing,
    paused: paused.trim().slice(-300),
    status,
    whilePaused,
    resumeCommandMs: resumedAt - resuming,
    resumed: resumed.trim().slice(-300),
    // From the resume's command returning to the exec asked while paused answered, and from its own ask to its answer.
    thawToAnsweredMs: answered === null ? null : answered - resumedAt,
    pending: answer,
    serving: after,
    events: Object.fromEntries(Object.entries(events).map(([step, at]) => [step, at - pausing])),
    memoryBefore,
    memoryPaused,
    afterState: apiState(await kube.get<V1Pod>(ref("Pod", podName, SESSIONS))),
    podEvents: (await eventsOf(podName)).slice(-10),
  })}`);
}

async function pauses(): Promise<void> {
  const { workspace, sessionId, node } = await freshSession();
  const bound = await boundPodOf(workspace);
  if (!bound) throw new Error(`no pod is bound to ${workspace}`);
  const { pod } = await podIds(node, bound.name);
  console.log(`# orch-pause-pod ${JSON.stringify({ probes: pod.spec?.containers.map(({ name, readinessProbe, livenessProbe, startupProbe }) => ({ name, readinessProbe, livenessProbe, startupProbe })), runtimeClass: pod.spec?.runtimeClassName })}`);
  for (let index = 0; index < PAUSES; index++) await pauseOnce(index, workspace, sessionId, node, bound.name, index === PAUSES - 1 ? LONG_PAUSE_MS : 2000);
  // What alasio's sweep and gateway said meanwhile of the session.
  const gateway = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("sandbox-gateway")) });
  const logged = (await Promise.all(gateway.map((each) => kube.logs(NAMESPACE, each.metadata?.name ?? "", "sandbox-gateway").catch(() => "")))).join("\n").split("\n").filter((line) => line.includes(workspace) && !line.includes("trace "));
  console.log(`# orch-pause-gateway ${JSON.stringify(logged.slice(-40))}`);
  await release(workspace);
  await removeWorkspace(workspace);
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
    for (const node of (await nodeAgents()).keys()) {
      await bootLogs(node);
      await onNode(node, `nohup k3s ctr -n k8s.io events > ${CTR_EVENTS} 2>&1 < /dev/null & echo following`);
    }
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 150 * 60_000 });

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

test("a small session's warm restore traced, from both its attaches to its first exec (E3, E10)", { skip: !PARTS.includes("restore"), timeout: 120 * 60_000 }, async () => {
  for (let run = 1; run <= RUNS; run++) await restoreOnce(run);
});

test("kube-router's rules for a new pod and a relabelled one, by the pods on the node (E4)", { skip: !PARTS.includes("density"), timeout: 120 * 60_000 }, async () => {
  await density();
});

test("a session paused and resumed through containerd's task API (E9)", { skip: !PARTS.includes("pause"), timeout: 120 * 60_000 }, async () => {
  await pauses();
});
