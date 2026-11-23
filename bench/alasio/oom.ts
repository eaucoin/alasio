/**
 * The two-sandbox OOM measure (BENCH_MODE=oom): does an OOM above two sessions' sandboxes, as
 * one session's memory grows past what the cgroup above them may hold, end the other session too?
 * Each trial binds two new sessions on the node, their pods' limits far above (test/e2e/harness.ts),
 * and holds the cgroup of the node's Burstable pods, theirs, to what it holds then and HEADROOM
 * more, so that the kernel's OOM is of that cgroup, above both sandboxes, and not of a pod's own
 * nor of the node, which a runner's memory filled whole starves before the kernel kills: B holds
 * 1.5 GiB and charges and frees 4 MiB every 20 ms beside it; A grows by 64 MiB every 80 ms until it
 * is killed. The cgroup's limit is given back after. Then whether B's pod runs on, the same pod, its
 * process alive, and what the kernel killed and kubelet evicted meanwhile, a line of
 * logs/oom.jsonl a trial. Both sessions are forgotten after.
 */
import { appendFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

import type { CoreV1Event, V1Pod } from "@kubernetes/client-node";

import { bindSession, gatewayCall, kube, mainToken, onNode, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { errorText, forgetWorkspace } from "./alasio.ts";
import { BENCH_DIR, RUNS } from "./settings.ts";

/** What the Burstable pods' cgroup is held to beyond what it holds as A begins: B's 1.5 GiB and as much again. */
const HEADROOM_BYTES = 3 * 1024 ** 3;
/** Where the node's Burstable pods' cgroup is, as kubelet makes it under systemd's driver or cgroupfs's. */
const BURSTABLE = "$(ls -d /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice /sys/fs/cgroup/kubepods/burstable 2>/dev/null | head -n 1)";

/** How long A may grow before the trial gives up on an OOM. */
const GROWTH_BOUND_MS = 120_000;
/** How long after A's end B is looked at: past kubelet's eviction checks, which run every 10 s. */
const SETTLE_MS = 20_000;

/** B: 1.5 GiB held, every page written, and 4 MiB written and dropped every 20 ms beside, its liveness stamped every 500 ms. */
const HOLDER = [
  "const fs = require('node:fs');",
  "const held = []; for (let i = 0; i < 24; i++) held.push(Buffer.alloc(64 << 20, 1));",
  "let n = 0; setInterval(() => Buffer.allocUnsafe(4 << 20).fill(n++ & 255), 20);",
  "setInterval(() => fs.writeFileSync('/tmp/holder.alive', String(Date.now())), 500);",
].join(" ");
/** A: 64 MiB more every 80 ms, every page written, until it is killed. */
const GROWER = "const held = []; setInterval(() => held.push(Buffer.alloc(64 << 20, 7)), 80);";

/** A new session's workspace, bound: its name, and its pod's. */
async function boundWorkspace(): Promise<{ readonly workspace: string; readonly pod: string; readonly uid: string; readonly node: string }> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  const { name, node } = await bindSession(workspace);
  const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
  return { workspace, pod: name, uid: pod?.metadata?.uid ?? "", node };
}

/** One trial, as the module says: its line. */
async function trial(number: number): Promise<Record<string, unknown>> {
  const started = Date.now();
  const b = await boundWorkspace();
  const a = await boundWorkspace();
  try {
    await kube.execOk(SESSIONS, b.pod, ["sh", "-c", `nohup node -e "${HOLDER}" > /tmp/holder.log 2>&1 & sleep 3`], { container: "bayma" });
    // The Burstable pods' cgroup held to what it holds now and HEADROOM_BYTES more: its limit before, to give back.
    const limited = (await onNode(b.node, `dir=${BURSTABLE}; before=$(cat "$dir/memory.max"); echo $(( $(cat "$dir/memory.current") + ${HEADROOM_BYTES} )) > "$dir/memory.max"; echo "$dir $before $(cat "$dir/memory.max")"`)).trim();
    const [cgroup = "", limitBefore = "max", limit = ""] = limited.split(" ");
    const grown = Date.now();
    const ran = await Promise.race([
      kube.exec(SESSIONS, a.pod, ["node", "-e", GROWER], { container: "bayma" }).then((done) => ({ code: done.code, said: done.stderr.slice(-400) }), (error: unknown) => ({ code: null, said: errorText(error).slice(-400) })),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), GROWTH_BOUND_MS)),
    ]);
    const aEndedMs = Date.now() - grown;
    // One not killed in time ended first, so that it grows no further once the limit is given back.
    if (ran === null) await kube.kill(SESSIONS, a.pod);
    await onNode(b.node, `echo ${limitBefore} > ${cgroup}/memory.max`);
    const events = (await onNode(b.node, `cat ${cgroup}/memory.events`)).trim().split("\n");
    await new Promise((resolve) => setTimeout(resolve, SETTLE_MS));
    const podOf = async (uid: string, name: string) => {
      const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
      return { there: pod?.metadata?.uid === uid && !pod.metadata.deletionTimestamp, phase: pod?.status?.phase ?? null, restarts: pod?.status?.containerStatuses?.map(({ restartCount }) => restartCount) ?? [] };
    };
    const bPod = await podOf(b.uid, b.pod);
    const aPod = await podOf(a.uid, a.pod);
    // B's process alive: its stamp younger than 2 s, as the sandbox's clock says.
    const stampAge = bPod.there
      ? await kube.exec(SESSIONS, b.pod, ["sh", "-c", "echo $(( $(date +%s%3N) - $(cat /tmp/holder.alive) ))"], { container: "bayma" }).then((done) => (done.code === 0 ? Number(done.stdout.trim()) : null), () => null)
      : null;
    const since = Math.floor(started / 1000);
    const kernel = (await onNode(b.node, `journalctl -k --since @${since} --no-pager -o short-iso-precise 2>/dev/null | grep -iE 'oom-kill|killed process|out of memory' || true`)).trim().split("\n").filter(Boolean);
    const evicted = (await kube.list<CoreV1Event>("Event", { namespace: SESSIONS }))
      .filter(({ reason, lastTimestamp, eventTime }) => reason === "Evicted" && new Date(String(lastTimestamp ?? eventTime ?? 0)).getTime() >= started)
      .map(({ involvedObject, message }) => `${involvedObject?.name ?? ""}: ${message ?? ""}`);
    return {
      trial: number,
      a: { pod: a.pod, ended: ran !== null, exec: ran, ended_ms: aEndedMs, ...aPod },
      b: { pod: b.pod, ...bPod, stamp_age_ms: stampAge },
      b_survived: bPod.there && bPod.phase === "Running" && stampAge !== null && stampAge < 2_000,
      cgroup: { path: cgroup, limit_before: limitBefore, limit: Number(limit), events },
      kernel,
      evicted,
      took_ms: Date.now() - started,
    };
  } finally {
    for (const { workspace } of [a, b]) await forgetWorkspace(workspace);
  }
}

/** The measure's trials, RUNS of them, each a line of logs/oom.jsonl. */
export async function oomTrials(): Promise<void> {
  for (let number = 1; number <= RUNS; number++) {
    const line = await trial(number).catch((error: unknown) => ({ trial: number, failure: errorText(error) }));
    appendFileSync(join(BENCH_DIR, "logs", "oom.jsonl"), `${JSON.stringify(line)}\n`);
    console.error(`# OOM trial ${number}: ${JSON.stringify(line).slice(0, 600)}`);
  }
}
