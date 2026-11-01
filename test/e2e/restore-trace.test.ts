/**
 * Spike, not for main: the kernel-standby validation's warm restores of small sessions (the
 * endpoint package's G-15). Each run makes a workspace, a Bun REPL in it (session-repl.ts set),
 * or one holding FILES files of the workspace open (session-repl.ts files), alternately; has the
 * gateway release it into a snapshot (POST /release), waits SETTLE_MS, then reads the REPL back
 * through the gateway (session-repl.ts trace), which binds the workspace and so restores it from
 * the node's cache. Then the boot logs of the sandbox that saved it and of the one that restored
 * it are read for the lines that say where the save's and the restore's kernel, netstack and VFS
 * time went. Each run is printed as one `# restore-run <json>` line.
 *
 *   ALASIO_E2E_TARGET=host ALASIO_E2E_SHARD=trace node --test test/e2e/restore-trace.test.ts
 */
import { randomBytes } from "node:crypto";
import { after, before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";

import { NODE_PINS } from "../../cli/src/cluster/node.ts";
import { FREE_LABEL } from "../../src/binder/names.ts";
import { CONTAINERD, gvisorHandler } from "../../src/node-agent/names.ts";
import { bindSession, boundPodOf, dumpClusterState, gatewayCall, inAlasio, kube, mainToken, nodeAgents, onNode, recordSession, ref, SESSIONS, setUp, tearDown, TARGET } from "./harness.ts";
import type { ReplSet } from "./session-repl.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** Runs of each kind of session. */
const RUNS = Number(process.env["TRACE_RUNS"] || 6);
/** How many files the second kind of session holds open. */
const FILES = Number(process.env["TRACE_FILES"] || 1000);
/** How long after its release each session is restored. */
const SETTLE_MS = 10_000;
const RUNSC_LOGS = "/var/log/runsc";

/** The lines of a boot log this spike reads: the save's and the restore's timings, and the timelines' summaries. */
const SAID = /gofer (save|restore) stats|VFS (PrepareSave|CompleteRestore)|Network stack restore|netstack:|Kernel (save|load) (took|stats)|CPUID load took|Overall load took|Restore successful|Tasks resumed after save|kernel load|Timeline|timeline|Starting sandbox|VFS restored|Network stack restored|Kernel loaded|MF metadata loaded/u;

/** The sandbox of the pod `pod` on `node`, as the runtime names it: its boot log's directory. */
async function sandboxOf(node: string, pod: string): Promise<string> {
  return (await onNode(node, `(k3s crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q 2>/dev/null || crictl pods --name '^${pod}$' --namespace ${SESSIONS} -q) | head -n 1`)).trim();
}

/** The lines of the sandbox `sandbox`'s boot log on `node` that SAID matches. */
async function bootLines(node: string, sandbox: string): Promise<string[]> {
  if (sandbox === "") return ["no sandbox"];
  const lines = (await onNode(node, `cat ${RUNSC_LOGS}/${sandbox}/runsc.log.*.boot.txt 2>/dev/null || true`)).split("\n");
  return lines.filter((line) => SAID.test(line)).map((line) => line.slice(0, 800));
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

async function runOnce(run: number, kind: "repl" | "files"): Promise<void> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  recordSession(workspace, "none");
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  const saver = await bindSession(workspace);
  const { sessionId } = kind === "repl"
    ? await inAlasio<ReplSet>("./session-repl.ts", "set", workspace, `held by ${workspace}`)
    : await inAlasio<ReplSet>("./session-repl.ts", "files", workspace, String(FILES));
  const saverSandbox = await sandboxOf(saver.node, saver.name);
  await release(workspace);
  await sleep(SETTLE_MS);
  const client = await inAlasio<Record<string, number | string | null>>("./session-repl.ts", "trace", workspace, sessionId);
  const restorer = await boundPodOf(workspace);
  if (!restorer) throw new Error(`${workspace} is bound to no pod after its restore`);
  await sleep(2_000);
  const save = await bootLines(saver.node, saverSandbox);
  const restore = await bootLines(restorer.node, await sandboxOf(restorer.node, restorer.name));
  console.log(`# restore-run ${JSON.stringify({ run, kind, target: TARGET, workspace, saver, restorer, client, save, restore })}`);
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

test(`small sessions' warm restores, ${RUNS} of each kind`, { timeout: 120 * 60_000 }, async () => {
  for (let run = 1; run <= RUNS; run++) {
    await runOnce(run, "repl");
    await runOnce(run, "files");
  }
});
