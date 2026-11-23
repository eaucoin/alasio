/**
 * What the benchmark does on alasio's node, the runner itself (the host target), beside alasio's
 * own path: the sandboxes' boot logs, the pods' memory, the bytes the node agent sends the store
 * and receives from it, the disks' reads, the page cache, the store's profile put between the
 * agents and the store, and programs run in a node agent's network as its own clients of the
 * bucket.
 */
import { appendFileSync } from "node:fs";
import { join } from "node:path";

import type { V1Pod, V1Service } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { selectorOf } from "../../cli/src/kube/rollout.ts";
import { NAMESPACE, selectorLabels } from "../../cli/src/manifests/common.ts";
import { GATEWAY as STORE_GATEWAY } from "../../cli/src/manifests/object-store.ts";
import { FREE_LABEL } from "../../src/binder/names.ts";
import { CONTAINERD, gvisorHandler, NODE_AGENT_TOKEN_VARIABLE } from "../../src/node-agent/names.ts";
import { kube, nodeAgents, onNode, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { gone } from "./alasio.ts";
import { BENCH_DIR, COMMON, PROFILE, PROFILES, TOXIPROXY } from "./settings.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Where each sandbox's boot log is, on the node (bootLogs). */
const RUNSC_LOGS = "/var/log/runsc";
/** The CRI client on the node: k3s's own. */
const CRICTL = "k3s crictl";
/** The chain of rules, in each node agent's network namespace, that counts its traffic with the store (storeBytes). */
const STORE_CHAIN = "BENCH-STORE";
/** The user toxiproxy runs as in an agent's network namespace, whose connections are not sent to it again. */
const PROXY_UID = 65534;

/** The id of the pod sandbox of the pod `name`, as containerd names it, which its sandbox's boot log is under. */
export async function sandboxIdOf(node: string, name: string): Promise<string> {
  return (await onNode(node, `${CRICTL} pods --name '^${name}$' --namespace ${SESSIONS} -q | head -n 1`)).trim();
}

/** The boot log of the pod sandbox `sandboxId` on `node`: its lines. */
export async function bootLog(node: string, sandboxId: string): Promise<string[]> {
  if (sandboxId === "") return [];
  return (await onNode(node, `cat ${RUNSC_LOGS}/${sandboxId}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
}

/** The memory of the pod of uid `uid` on `node`, as its cgroup says: its peak and what it holds now. */
export async function podMemory(node: string, uid: string): Promise<{ readonly peak: number | null; readonly current: number | null; readonly stat: Record<string, number> }> {
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

/** Spike: the CPU time, in microseconds, that the pod of `uid` on `node` has used, as its cgroup's cpu.stat says; null where it has no cgroup. */
export async function podCpuUsec(node: string, uid: string): Promise<number | null> {
  const printed = await onNode(node, [
    `dir=$(find /sys/fs/cgroup -maxdepth 6 -type d \\( -name '*pod${uid}*' -o -name '*pod${uid.replaceAll("-", "_")}*' \\) | head -n 1)`,
    `[ -n "$dir" ] || exit 0`,
    `grep '^usage_usec ' "$dir/cpu.stat"`,
  ].join("; "));
  const value = printed.split(" ")[1];
  return value === undefined ? null : Number(value);
}

/**
 * The bytes that moved between the node agents' network and the store, as counted by
 * the rules countStore puts in the agent's network namespace on `node`: sent to it and received
 * from it, all the agent's clients of the bucket together (its uploads, its proxy of the restores'
 * reads, its fetches), and toxiproxy's on the emulated profiles, which carries them.
 */
export async function storeBytes(node: string): Promise<{ readonly sent: number; readonly received: number }> {
  const listed = await onNode(node, `nsenter --target ${await agentPid(node)} --net sh -c 'iptables -L ${STORE_CHAIN} -v -x -n'`);
  const bytes = (direction: string) => Number(listed.split("\n").find((line) => line.includes(`/* ${direction} */`))?.trim().split(/\s+/u)[1] ?? NaN);
  return { sent: bytes("sent"), received: bytes("received") };
}

/** Counts, in the agent's network namespace on `node`, what it sends to the store's Service and receives from it (storeBytes). */
export async function countStore(node: string): Promise<void> {
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

/** The bytes the node's disks have read, as /proc/diskstats counts their sectors. */
export async function diskReadBytes(node: string): Promise<number> {
  const listed = await onNode(node, "grep -E ' (sd[a-z]+|nvme[0-9]+n[0-9]+|vd[a-z]+) ' /proc/diskstats");
  return listed.split("\n").filter(Boolean).reduce((sum, line) => sum + Number(line.trim().split(/\s+/u)[5] ?? 0) * 512, 0);
}

/** Waits until the store has sent `node`'s agent nothing for 3 s, within 180 s, as the fetch a cold restore starts runs: what it received meanwhile. */
export async function storeQuiet(node: string): Promise<number> {
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
export const dropPageCache = (node: string): Promise<string> => onNode(node, "sync && echo 1 > /proc/sys/vm/drop_caches");

/**
 * Has the store's masters compact every volume of which a hundredth is deleted, between runs: a
 * run's snapshots, forgotten with its workspace, free their disk only once compacted, and the next
 * runs' saves need it.
 */
export async function vacuum(): Promise<void> {
  const [master] = await kube.list<V1Pod>("Pod", { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("seaweedfs-master")) });
  if (!master?.metadata?.name) throw new Error("no master of the store runs");
  await kube.execOk(NAMESPACE, master.metadata.name, ["wget", "-q", "-O", "-", "http://127.0.0.1:9333/vol/vacuum?garbageThreshold=0.01"], { container: "master" });
}

/** The bytes the workspace's snapshots hold in the bucket. */
export const bucketBytes = async (node: string, workspace: string): Promise<number> =>
  (JSON.parse(await inAgentNetwork(node, [new URL("dist/s3.mjs", COMMON).pathname, "bytes", `${SESSIONS}/${workspace}/`])) as { readonly bytes: number }).bytes;

/**
 * What the runner says of its memory, its kernel's last words and k3s's, as a run fails: in
 * logs/failure-<name>.txt. Said 10 s after the failure, so that a k3s that ended under it is seen
 * started again by systemd, or not (RestartSec 5 s), and how often it was.
 */
export async function sayRunner(name: string): Promise<void> {
  await sleep(10_000);
  const said = await onNode("", "free -m; echo; dmesg | tail -n 60; echo; systemctl show k3s --property=ActiveState,NRestarts; journalctl -u k3s --since '-10 min' --no-pager | tail -n 120")
    .catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
  appendFileSync(join(BENCH_DIR, "logs", `failure-${name}.txt`), said);
}

/**
 * What k3s's journal says of its ends and starts in the last hour, a fatal line among them (kine
 * ends k3s for a commit of its datastore that failed), as systemd started it again since the run
 * before: in logs/k3s-restarted-<name>.txt.
 */
export async function sayK3sRestarted(name: string): Promise<void> {
  const said = await onNode("", "systemctl show k3s --property=ActiveState,NRestarts,ExecMainStartTimestamp; journalctl -u k3s --since '-60 min' --no-pager | grep -E 'level=fatal|Transaction|Slow SQL|Main process exited|Started k3s|Stopped k3s|Scheduled restart' | tail -n 200")
    .catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
  appendFileSync(join(BENCH_DIR, "logs", `k3s-restarted-${name}.txt`), said);
}

/** How long k3s's API server may take to be ready again before a run, as one systemd starts again after it ended takes. */
const K3S_READY_WAIT_MS = 300_000;

/**
 * Waits until k3s's API server answers ready, within K3S_READY_WAIT_MS, before a run: a k3s that
 * ended under the run before (its datastore's commit failed, as kine ends k3s for one) is started
 * again by systemd, and the run waits for it rather than fail at once. How many times systemd has
 * started k3s again since it was installed, which a run's result keeps, so one it ended under is
 * told; throws where it is not ready in time.
 */
export async function k3sReady(): Promise<{ readonly restarts: number; readonly waited_ms: number }> {
  const started = Date.now();
  for (;;) {
    const said = await onNode("", "systemctl show k3s --property=NRestarts --value; k3s kubectl get --raw /readyz 2>&1 || true").catch((error: unknown) => (error instanceof Error ? error.message : String(error)));
    const [restarts = "", ...answer] = said.trim().split("\n");
    if (answer.join("\n").trim() === "ok") return { restarts: Number(restarts), waited_ms: Date.now() - started };
    if (Date.now() - started > K3S_READY_WAIT_MS) throw new Error(`k3s's API server was not ready within ${K3S_READY_WAIT_MS / 1000} s: ${answer.join(" ").slice(0, 400)}`);
    await sleep(2000);
  }
}

/**
 * Adds a boot log to each kept release's runsc options, for the boot command alone, which the
 * sandboxes' pause, pre-copy and restore are read from, and has every pool's free pods made
 * again, so that each runs with it.
 */
export async function bootLogs(node: string): Promise<void> {
  for (const { release } of NODE_PINS.gvisor.releases) {
    const options = `${CONTAINERD}/${gvisorHandler(release)}.toml`;
    // [runsc_config] is the options' last table (cli/src/cluster/k3s.ts).
    await onNode(node, `grep -q '^  debug-log' ${options} || printf '  debug-log = "${RUNSC_LOGS}/%%ID%%/"\\n  debug-command = "boot"\\n' >> ${options}; tail -n 4 ${options}`);
    // Spike: more runsc options, as name:value words separated by ',' (BENCH_RUNSC_OPTIONS).
    for (const word of (process.env["BENCH_RUNSC_OPTIONS"] ?? "").split(",").filter(Boolean)) {
      const [name, value] = word.split(":");
      await onNode(node, `grep -q '^  ${name} ' ${options} || printf '  ${name} = "${value}"\\n' >> ${options}; tail -n 6 ${options}`);
    }
  }
  const free = await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${FREE_LABEL}=true` });
  for (const pod of free) await kube.remove(ref("Pod", pod.metadata?.name ?? "", SESSIONS));
  for (const pod of free) await gone(pod.metadata?.name ?? "");
}

/** The process of the node agent on `node`, in the node's PID namespace, as its runtime says. */
export async function agentPid(node: string): Promise<string> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const agentId = (await kube.get<V1Pod>(ref("Pod", pod, NAMESPACE)))?.status?.containerStatuses?.find(({ name }) => name === "node-agent")?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";
  return (await onNode(node, `${CRICTL} inspect --output go-template --template '{{.info.pid}}' ${agentId}`)).trim();
}

/** The address of the store's Service, which the node agents reach the bucket at. */
export async function storeAddress(): Promise<string> {
  const address = (await kube.get<V1Service>(ref("Service", STORE_GATEWAY, NAMESPACE)))?.spec?.clusterIP;
  if (!address) throw new Error(`the store's Service ${STORE_GATEWAY} has no address`);
  return address;
}

/**
 * How the store answers through the profile, as the node agent on `node` reaches it: the probe
 * run in the agent's network namespace, as the agent's own client of the bucket, its key and
 * bucket, from the node, as the agent's pod has no room for its 256 MiB.
 */
export async function probeStore(node: string): Promise<Record<string, number>> {
  return JSON.parse(await inAgentNetwork(node, [new URL("dist/probe.mjs", COMMON).pathname, "--prefix", "bench-probe/"])) as Record<string, number>;
}

/**
 * Runs the Node program `argv` from the node, in the network namespace of the node agent on
 * `node`, with the agent's key and bucket, the store by its Service's address, and the agent's
 * token, as the agent's own client of the bucket and the gateway's of the agent: from the node, as
 * a program run in the agent's container is charged to its memory, which the agent's own work
 * fills (one such, 137, ended the agent under a save). Its last line printed.
 */
export async function inAgentNetwork(node: string, argv: readonly string[]): Promise<string> {
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
export async function shapeAgents(): Promise<void> {
  if (!PROFILES[PROFILE]?.emulated) return;
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
