/**
 * alasio's path, as the benchmark drives it: the fixtures' driver run in alasio's pod through the
 * sandbox gateway's bayma endpoint, as alasio reaches a session; the node agent asked as the
 * gateway asks it; the gateway's release; a restore's pod watched; the node's standby readied for
 * a restore's arm; a workspace's caches removed for a cold restore.
 */
import { randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import { join } from "node:path";

import type { V1Deployment, V1Pod } from "@kubernetes/client-node";

import { NAMESPACE, RELEASE } from "../../cli/src/manifests/common.ts";
import { GATEWAY, GATEWAY_PORT } from "../../cli/src/manifests/sandbox-gateway.ts";
import { BINDING_LABEL, keyHash, leaseName, POOL_LABEL } from "../../src/binder/names.ts";
import { hookOf, stateDirOf } from "../../src/gateway/snapshots.ts";
import { BAYMA_CONTAINER, RESTORE_ANNOTATION, STANDBY_ANNOTATION, STANDBY_LABEL } from "../../src/kube/sandbox-shape.ts";
import type { Manifest } from "../../src/node-agent/agent.ts";
import { LAYERS_DIR } from "../../src/node-agent/cache.ts";
import { gvisorDirectory, NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE, RUNSC_ROOT, SNAPSHOT_CACHE_DIR } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { bindSession, boundPodOf, gatewayCall, kube, mainToken, nodeAgents, onNode, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { dropPageCache, inAgentNetwork } from "./node.ts";
import { BENCH_DIR, HOST, readCommon } from "./settings.ts";

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
export const now = (): bigint => process.hrtime.bigint();
export const msBetween = (from: bigint, to: bigint): number => Number(to - from) / 1e6;

/** Where the benchmark's bundles go in the pods that run them. */
const IN_POD = "/tmp/bench";
/** The gateway's bayma endpoint, as alasio's own client of it names it (src/sandbox/gateway-client.ts). */
const MCP_URL = `http://${GATEWAY}.${NAMESPACE}.svc:${GATEWAY_PORT}/mcp`;

const alasioPod = () => kube.runningPod(NAMESPACE, RELEASE);

/** When the last restore on the node was asked, on the wall clock, which the gateway's standby is made after (standbyFor); 0 before the job's first. */
export const restores = { lastAt: 0 };

/** Copies `file` of bench/common/dist into the container of the pod, as IN_POD/<file>, renamed into place, so that a driver another run of it started meanwhile reads it whole. */
export async function copyIn(pod: string, container: string, file: string): Promise<void> {
  await kube.execOk(NAMESPACE, pod, ["sh", "-c", `mkdir -p ${IN_POD} && cat > ${IN_POD}/.${file}.$$ && mv ${IN_POD}/.${file}.$$ ${IN_POD}/${file}`], { container, stdin: readCommon(`dist/${file}`) });
}

/** Runs the driver's `command` in alasio's pod, of the workspace `workspace`, through the gateway: what it printed. */
export async function driver<T = Record<string, unknown>>(workspace: string, command: string, ...args: string[]): Promise<T> {
  const pod = await alasioPod();
  await copyIn(pod, "alasio", "driver.mjs");
  const token = await mainToken(workspace);
  const printed = await kube.execOk(NAMESPACE, pod, ["node", `${IN_POD}/driver.mjs`, command, "--url", MCP_URL, "--header", `Authorization: Bearer ${token}`, ...args], { container: "alasio" });
  return JSON.parse(printed.trim().split("\n").at(-1) ?? "null") as T;
}

/** Asks the node agent on `node` `method path`, as the gateway asks it: its status and what it answered. */
export async function agentApi(node: string, method: string, path: string, body?: unknown): Promise<{ readonly status: number; readonly said: string }> {
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

/** The node agent on `node` asked `method path`, which must answer 2xx: what it answered. */
export async function agentOk(node: string, method: string, path: string, body?: unknown): Promise<string> {
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

/** What a node agent's checkpoint is asked with (src/node-agent/server.ts POST /checkpoints). */
export interface CheckpointRequest {
  readonly environment: string;
  readonly workspace: string;
  readonly namespace: string;
  readonly pod: string;
  readonly containerId: string;
  readonly hook: readonly string[];
  readonly podTemplate: Record<string, unknown>;
  readonly live: { readonly stateDir: string };
}

/**
 * The live checkpoint of the session of `workspace`, bound to a pod of the sessions' pool, as the
 * gateway's keepLive asks it of the agent on its node: bayma's hook and state directory as the
 * pool's pod gives them, and the pool's template, which the gateway records with a snapshot.
 */
export async function checkpointRequest(workspace: string): Promise<{ readonly node: string; readonly request: CheckpointRequest }> {
  const bound = await boundPodOf(workspace);
  if (!bound) throw new Error(`no pod is bound to ${workspace}`);
  const pod = await podNamed(bound.name);
  const bayma = pod.spec?.containers.find(({ name }) => name === BAYMA_CONTAINER);
  const hook = bayma ? hookOf(bayma) : null;
  const stateDir = bayma ? stateDirOf(bayma) : null;
  if (!hook || !stateDir) throw new Error(`the pod ${bound.name} runs no bayma with a state directory`);
  return { node: bound.node, request: { environment: SESSIONS, workspace, namespace: SESSIONS, pod: bound.name, containerId: containerIdOf(pod), hook, podTemplate: await poolTemplate(pod), live: { stateDir } } };
}

/** The live checkpoint `request`, asked of the agent on `node` (CHECKPOINT): how long it took to its manifest committed, and the manifest; or why it failed. */
export async function liveCheckpoint(node: string, request: CheckpointRequest): Promise<{ readonly ms: number; readonly manifest: Manifest } | { readonly failed: string }> {
  const asking = join(BENCH_DIR, "checkpoint.cjs");
  const asked = join(BENCH_DIR, `checkpoint-${request.workspace}.json`);
  writeFileSync(asking, CHECKPOINT);
  writeFileSync(asked, JSON.stringify(request));
  return inAgentNetwork(node, [asking, String(NODE_AGENT_PORT), SESSIONS, request.workspace, asked]).then(
    (printed) => JSON.parse(printed) as { readonly ms: number; readonly manifest: Manifest },
    (error: unknown) => {
      const said = errorText(error);
      return { failed: /"error":"([^"]*)"/u.exec(said)?.[1] ?? said.trim().split("\n").at(-1) ?? "" };
    },
  );
}

/**
 * A session of its own: a new workspace made through the gateway and bound, as a first turn binds
 * it, and `fixture` of `sizeMib` set up in a session of its bayma: the workspace and the session.
 */
export async function newSession(fixture: string, sizeMib: number, runId: string): Promise<{ readonly workspace: string; readonly sessionId: string; readonly setup: Record<string, unknown> }> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  await bindSession(workspace);
  const setup = await driver<{ readonly session_id: string }>(workspace, "setup", "--fixture", fixture, "--size-mib", String(sizeMib), "--run-id", runId, "--rate-mib-s", String(HOST.copy_mib_s), "--workspace", "/workspace");
  return { workspace, sessionId: setup.session_id, setup };
}

/**
 * Forgets `workspace`: its pod ended unsaved where one is bound, then the workspace removed through
 * the gateway, its snapshots with it, in the bucket and every node's cache, asked again while the
 * gateway still lets the binding of the pod ended under it go (409), so no run's images outlive it.
 */
export async function forgetWorkspace(workspace: string): Promise<void> {
  const bound = await boundPodOf(workspace).catch(() => null);
  if (bound) await endUnsaved(workspace, bound.name).catch(() => {});
  let removed = { status: 0, said: "" };
  for (const deadline = Date.now() + 300_000; Date.now() < deadline; await sleep(3000)) {
    removed = await mainToken(workspace).then((token) => gatewayCall(token, "DELETE", "/workspace")).catch((error: unknown) => ({ status: 0, said: errorText(error) }));
    if (removed.status !== 409) break;
  }
  if (removed.status !== 204) console.error(`# ${workspace} was not removed: ${removed.status} ${removed.said}`);
}

/** The gateway's release of the workspace, asked from alasio's pod, as alasio asks it, timed by curl: its status and time. */
export async function release(workspace: string): Promise<{ readonly status: number; readonly ms: number; readonly said: string }> {
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
export async function podNamed(name: string): Promise<V1Pod> {
  const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
  if (!pod) throw new Error(`there is no pod ${SESSIONS}/${name}`);
  return pod;
}

export const containerIdOf = (pod: V1Pod): string => pod.status?.containerStatuses?.find(({ name }) => name === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "") ?? "";

/** Waits until the pod `name` is gone. */
export async function gone(name: string, timeoutMs = 180_000): Promise<void> {
  for (const deadline = Date.now() + timeoutMs; Date.now() < deadline; await sleep(500)) {
    if ((await kube.get(ref("Pod", name, SESSIONS))) === null) return;
  }
  throw new Error(`the pod ${SESSIONS}/${name} is still there after ${timeoutMs / 1000} s`);
}

/**
 * Watches the restore of `workspace`: once its restore's pod has its bayma container, waits on
 * runsc for its restore to complete, every page of it loaded (`runsc wait --restore`), and says
 * when, with the pod.
 */
export async function watchRestore(workspace: string, release: string): Promise<{ readonly residentNs: bigint | null; readonly pod: V1Pod; readonly error: string | null }> {
  const selector = `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}`;
  for (const deadline = Date.now() + 900_000; Date.now() < deadline; await sleep(50)) {
    const [pod] = (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: selector })).filter(({ metadata }) => !metadata?.deletionTimestamp && metadata?.name?.includes("-restore-"));
    const containerId = pod ? containerIdOf(pod) : "";
    if (!pod || containerId === "") continue;
    const node = pod.spec?.nodeName ?? "";
    const waited = await onNode(node, `${gvisorDirectory(release)}/runsc --root=${RUNSC_ROOT} wait --restore ${containerId}`).then(() => null, (error: unknown) => (error instanceof Error ? error.message : String(error)));
    // A standby claimed, its containers created long before, until the restore into it begins.
    if (waited !== null && pod.metadata?.annotations?.[STANDBY_ANNOTATION] !== undefined && /sandbox is not being restored, cannot wait for restore: state=created/u.test(waited)) continue;
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
export async function sourceOf(node: string, pod: V1Pod, prepared: string): Promise<"cache" | "bucket" | "unknown"> {
  // A standby's, which no annotation names, as its agent prepared it.
  const directory = pod.metadata?.annotations?.[RESTORE_ANNOTATION] ?? (pod.metadata?.annotations?.[STANDBY_ANNOTATION] === undefined ? "" : prepared);
  if (directory === "") return "unknown";
  const said = (await onNode(node, `if [ -e '${directory}/pages.img' ]; then echo cache; elif [ -d '${directory}' ]; then echo bucket; else echo unknown; fi`)).trim();
  return said === "cache" || said === "bucket" ? said : "unknown";
}

/** What the node agent of `node` and the sandbox gateway logged of `workspace`, timestamped: their last lines of it, which say where a restore spent its time. */
export async function loggedOf(node: string, workspace: string): Promise<{ readonly agent: string[]; readonly gateway: string[] }> {
  const of = async (pod: string, container: string) =>
    (await kube.logs(NAMESPACE, pod, container, { timestamps: true }).catch(() => "")).split("\n").filter((line) => line.includes(workspace)).slice(-25).map((line) => line.slice(0, 400));
  return { agent: await of((await nodeAgents()).get(node) ?? "", "node-agent"), gateway: await of(await kube.runningPod(NAMESPACE, GATEWAY), "sandbox-gateway") };
}

/**
 * What the agent of `node` logged of preparing a snapshot of `workspace`: its last line of it, and
 * the snapshot it names, which is the one restored: the release's `snapshot`, or the one a
 * compaction since made of it, which the workspace's current snapshot is then
 * (src/node-agent/agent.ts compactNow). The release's where none is logged.
 */
export async function preparedLine(node: string, workspace: string, snapshot: string): Promise<{ readonly line: string; readonly snapshot: string }> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const line = (await kube.logs(NAMESPACE, pod, "node-agent")).split("\n").findLast((each) => each.includes("prepared the snapshot ") && each.includes(` of ${SESSIONS}/${workspace} `));
  if (!line) return { line: "not logged", snapshot };
  const said = line.slice(line.indexOf("prepared"));
  return { line: said, snapshot: /^prepared the snapshot (\S+) /u.exec(said)?.[1] ?? snapshot };
}

/**
 * Whether a run's restores find a standby on their node (src/gateway/standbys.ts): an even run's
 * do, as a restore does that comes after the one before it on the node by more than the standby's
 * making, which is how the gateway serves most; an odd run's do not, as the first restore on a
 * node, or one that comes within the standby's making after another there (10 s, STANDBY_AFTER_MS), does not.
 */
export function standbyArm(run: number): "standby" | "none" {
  return run % 2 === 0 ? "standby" : "none";
}

/** How long a restore waits for the standby the gateway makes after the restore before it, before it goes on without. */
export const STANDBY_WAIT_MS = 120_000;

/** Whether `pod` runs, with its address, each of its containers started, and is not being deleted: a standby a restore claims (src/gateway/standbys.ts standing). */
export function standing(pod: V1Pod): boolean {
  return !pod.metadata?.deletionTimestamp && pod.status?.phase === "Running" && (pod.status.podIPs?.length ?? 0) > 0 &&
    (pod.status.containerStatuses ?? []).length > 0 && (pod.status.containerStatuses ?? []).every(({ state }) => state?.running !== undefined);
}

/**
 * The node's standby made ready for a restore of `arm`: where a restore ran on `node` before, the
 * standby the gateway makes 10 s after it waited for until it stands; then, for
 * `none`, deleted, and gone, so the restore makes its pod. What was found and done, for the
 * restore's details.
 */
export async function standbyFor(node: string, arm: "standby" | "none"): Promise<Record<string, unknown>> {
  const t0 = Date.now();
  const on = async () => (await kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector: `${STANDBY_LABEL}=${node}` })).filter((pod) => !pod.metadata?.deletionTimestamp);
  let found = await on();
  // None is made before the job's first restore on the node: nothing to wait for.
  for (const deadline = t0 + (restores.lastAt === 0 ? 0 : STANDBY_WAIT_MS); !found.some(standing) && Date.now() < deadline; found = await on()) await sleep(500);
  const stood = found.filter(standing).map((pod) => pod.metadata?.name ?? "");
  const deleted = arm === "none" ? found.map((pod) => pod.metadata?.name ?? "") : [];
  for (const name of deleted) await kube.remove(ref("Pod", name, SESSIONS));
  for (const name of deleted) await gone(name);
  return { arm, standing: stood, deleted, waited_ms: Date.now() - t0, since_last_restore_ms: restores.lastAt === 0 ? null : t0 - restores.lastAt };
}

/**
 * Ends the restored pod `name` of `workspace` without a save, as a crash ends it, and waits until it
 * is gone, and the gateway has let its binding go: its Lease gone, as the gateway records what a
 * binding whose pod went is released into while its Lease holds (src/gateway/sessions.ts
 * recordLost), which would otherwise write over the record a cold restore is given.
 */
export async function endUnsaved(workspace: string, name: string): Promise<void> {
  await kube.kill(SESSIONS, name);
  await gone(name);
  const lease = ref("Lease", leaseName(sessionKey(workspace)), SESSIONS);
  for (const deadline = Date.now() + 300_000; ; await sleep(1000)) {
    if ((await kube.get(lease)) === null) return;
    if (Date.now() > deadline) throw new Error(`the binding of ${workspace} was not let go within 300 s of its pod's end`);
  }
}

/**
 * Removes every node's cache of the workspace, by its agent, as the gateway removes a deleted
 * workspace's (src/gateway/snapshots.ts forgetDeleted), which spares a workspace used within the
 * cache's grace, as a restore may still read it (src/node-agent/cache.ts discard), as the warm
 * restore's was a minute before; asked again for 20 s, then, where the agent still spares it, by
 * hand, as the 10.0.4 baseline removed it, which no compaction is writing by then (the warm
 * restore's prepare stopped the release's); and the nodes' page cache, so a restore reads it from
 * the bucket. How each node's went.
 */
export async function coldCaches(workspace: string): Promise<Record<string, unknown>> {
  const how: Record<string, unknown> = {};
  for (const [node, pod] of await nodeAgents()) {
    const started = Date.now();
    let discarded = false;
    for (const deadline = started + 20_000; !discarded && Date.now() < deadline; await sleep(1000)) {
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

/** The Deployment of the pool the pod is of, whose template the gateway records with a snapshot (src/gateway/snapshots.ts take). */
export async function poolTemplate(pod: V1Pod): Promise<Record<string, unknown>> {
  const pool = pod.metadata?.labels?.[POOL_LABEL] ?? "";
  const deployment = await kube.get<V1Deployment>(ref("Deployment", pool, SESSIONS));
  if (!deployment) throw new Error(`the pool ${pool} of ${pod.metadata?.name} has no Deployment`);
  return { ...deployment.spec?.template } as Record<string, unknown>;
}

export const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** What of a promise's work came: its value or its error's text, and when it ended, in milliseconds from `t0`. */
export const settledFrom = <T>(work: Promise<T>, t0: number) =>
  work.then((value) => ({ ok: true, value, ended_ms: Date.now() - t0 }), (error: unknown) => ({ ok: false, value: errorText(error), ended_ms: Date.now() - t0 }));
