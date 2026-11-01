/**
 * Spike, not for main: the kernel-standby validation's restores into standby pods (the endpoint
 * package's E15). The gateway, given ALASIO_SPIKE_STANDBY=alternate, restores sessions in turns
 * of three: into a pod made for it, as it does today; into a standby pod of its node, kept ready
 * there, served once kubelet says its containers are ready; and into a standby served once it is
 * resumed and its node's agent saw it accept on its port (src/gateway/snapshots.ts). kubelet logs
 * at v=4, and each run keeps what it said of the restoring pod's probes. Each run makes a workspace, a Bun REPL in it
 * (session-repl.ts set), has the gateway release it into a snapshot, waits SETTLE_MS, then reads
 * the REPL back through the gateway (session-repl.ts trace), which restores it, and checks it
 * holds what it was set to. A run is a standby's where the pod it was restored into was made
 * before its release. Each run is printed as one `# standby-run <json>` line, with the restoring
 * sandbox's boot log lines.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=trace node --test test/e2e/restore-standby.test.ts
 */
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { FREE_LABEL } from "../../src/binder/names.ts";
import { NAMESPACE } from "../../cli/src/manifests/common.ts";
import { STANDBY_LABEL } from "../../src/kube/sandbox-shape.ts";
import { CONTAINERD, gvisorHandler } from "../../src/node-agent/names.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, inAlasio, kube, mainToken, nodeAgents, onNode, recordSession, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";
import type { ReplSet } from "./session-repl.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** Restores, in turns of three: into a pod made for it, into a standby served as kubelet says it is ready, into a standby served once resumed. */
const RUNS = Number(process.env["STANDBY_RUNS"] || 60);
/** How long after its release each session is restored: more than the gateway waits to make the next standby, and its boot. */
const SETTLE_MS = 25_000;
const RUNSC_LOGS = "/var/log/runsc";

/** The boot log lines this spike reads: the restore's timings. */
const SAID = /Kernel load took|Overall load took|Restore successful|VFS restored|Network stack restored|Kernel loaded|Starting sandbox|standby|Restore/u;

/** The sandbox of the pod `pod` on `node`, as the runtime names it: its boot log's directory. */
async function sandboxOf(node: string, pod: string): Promise<string> {
  return (await onNode(node, `(k3s crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q 2>/dev/null || crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q) | head -n 1`)).trim();
}

/** The lines of the sandbox `sandbox`'s boot log on `node` that SAID matches. */
async function bootLines(node: string, sandbox: string): Promise<string[]> {
  if (sandbox === "") return ["no sandbox"];
  const lines = (await onNode(node, `cat ${RUNSC_LOGS}/${sandbox}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
  return lines.filter((line) => SAID.test(line)).map((line) => line.slice(0, 600));
}

/** What kubelet logged of the pod `pod`'s probes and status on `node` since `since`, epoch ms: k3s's journal, kubelet at v=4 (raiseKubeletVerbosity). */
async function kubeletLines(node: string, pod: string, since: number): Promise<string[]> {
  const said = await onNode(node, `journalctl -u k3s --since @${Math.floor(since / 1000) - 5} --no-pager -o short-iso-precise | grep -F '${pod}' | grep -E 'Probe|probe|ContainersReady|Ready|status' | tail -n 80`).catch((error: unknown) => `could not be read: ${String(error)}`);
  return said.split("\n").filter((line) => line.length > 0).map((line) => line.slice(0, 500));
}

/** Has kubelet on `node` log at v=4, its probes' results among what it says, through its debug flags, by the API server's node proxy. */
async function raiseKubeletVerbosity(node: string): Promise<void> {
  const said = await onNode(node, `printf 4 | k3s kubectl replace --raw /api/v1/nodes/${node}/proxy/debug/flags/v -f - 2>&1 || true`);
  console.log(`# kubelet verbosity on ${node}: ${said.trim()}`);
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

async function runOnce(run: number): Promise<boolean> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  recordSession(workspace, "none");
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  await bindSession(workspace);
  const held = `held by ${workspace}`;
  const { sessionId } = await inAlasio<ReplSet>("./session-repl.ts", "set", workspace, held);
  await release(workspace);
  const releasedAt = Date.now();
  await sleep(SETTLE_MS);
  const standbys = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: STANDBY_LABEL });
  const client = await inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId);
  const restorer = await boundPodOf(workspace);
  if (!restorer) throw new Error(`${workspace} is bound to no pod after its restore`);
  const pod = await kube.get<V1Pod>(ref("Pod", restorer.name, SESSIONS));
  const made = new Date(pod?.metadata?.creationTimestamp ?? 0).getTime();
  const standby = made < releasedAt;
  const correct = client["live"] === held;
  await sleep(2_000);
  const restore = await bootLines(restorer.node, await sandboxOf(restorer.node, restorer.name));
  const ready = standbys.map((each) => ({ name: each.metadata?.name, phase: each.status?.phase }));
  const kubelet = await kubeletLines(restorer.node, restorer.name, releasedAt);
  const conditions = (pod?.status?.conditions ?? []).map(({ type, status, lastTransitionTime }) => ({ type, status, lastTransitionTime }));
  console.log(`# standby-run ${JSON.stringify({ run, target: TARGET, workspace, restorer, standby, correct, made, releasedAt, standbys: ready, client, restore, conditions, kubelet })}`);
  await release(workspace);
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
    const removed = await gatewayCall(await mainToken(workspace), "DELETE", "/workspace");
    if (removed.status !== 409) break;
  }
  if (!correct) throw new Error(`run ${run}: ${workspace} restored ${standby ? "into a standby" : "into a pod made for it"} holds ${String(client["live"])}, not ${held}`);
  return standby;
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
      await raiseKubeletVerbosity(node);
    }
  } catch (error) {
    await dumpClusterState();
    throw error;
  }
}, { timeout: 150 * 60_000 });

after(async () => {
  await tearDown();
}, { timeout: 30 * 60_000 });

/** What the sandbox gateway's and the node agents' pods logged of standbys, of each restore's phases, and of each resume. */
async function standbyLogs(): Promise<void> {
  const of = async (component: string, container: string) => {
    const pods = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: `app.kubernetes.io/component=${component}` });
    for (const pod of pods) {
      const logged = await kube.logs(NAMESPACE, pod.metadata?.name ?? "", container).catch((error: unknown) => `could not be read: ${String(error)}`);
      for (const line of logged.split("\n").filter((each) => /standby|restore phases|resumed the sandbox|probed /iu.test(each))) console.log(`# ${component} ${line.slice(0, 1500)}`);
    }
  };
  await of("sandbox-gateway", "sandbox-gateway");
  await of("node-agent", "node-agent");
  // What the shims said of standbys, from containerd's log, and the shims that run.
  for (const node of (await nodeAgents()).keys()) {
    const said = await onNode(node, "f=/var/lib/rancher/k3s/agent/containerd/containerd.log; grep -E 'standby|Created container|is not a standby|to container' \"$f\" | tail -n 300; pgrep -af containerd-shim-runsc | cut -c 1-300").catch((error: unknown) => `could not be read: ${String(error)}`);
    for (const line of said.split("\n")) console.log(`# shim ${line.slice(0, 1500)}`);
  }
}

test(`restores into standby pods and into pods made for them, ${RUNS} in all`, { timeout: 150 * 60_000 }, async () => {
  let intoStandbys = 0;
  try {
    for (let run = 1; run <= RUNS; run++) if (await runOnce(run)) intoStandbys++;
  } catch (error) {
    await dumpClusterState();
    throw error;
  } finally {
    await standbyLogs();
  }
  console.log(`# standby-summary ${JSON.stringify({ runs: RUNS, intoStandbys })}`);
  if (intoStandbys < (RUNS * 2) / 3 - 2) {
    await dumpClusterState();
    throw new Error(`only ${intoStandbys} of ${RUNS} restores were into standbys`);
  }
});
