/**
 * The two-sandbox OOM measure (BENCH_MODE=oom): does an OOM above two sessions' sandboxes, as
 * memory grows past what the cgroup above them may hold, end a session it should not?
 * Each trial binds two new sessions on the node, their pods' limits far above (test/e2e/harness.ts),
 * and holds the cgroup of the node's Burstable pods, theirs, to what it holds then and
 * BENCH_OOM_HEADROOM_MIB (2048) more, so that the kernel's OOM is of that cgroup, above both
 * sandboxes, and not of a pod's own nor of the node, which a runner's memory filled whole starves
 * before the kernel kills. B holds 1.5 GiB and charges and frees 4 MiB every 20 ms beside it; then,
 * as BENCH_OOM_ARM says:
 * - `grow` (unless said): A grows by BENCH_OOM_GROWTH_MIB (64) every 80 ms until it is killed;
 * - `foreign`: A is left as it is, and a pod of no sandbox, runc's, Burstable, on the same node in
 *   a namespace of no sessions, grows as A would until it ends, so that the want is another pod's;
 * - `burst`: A takes 1 GiB at once and holds it 15 s, as a session's exec may, which the guard's
 *   limit of its sandbox, where it sets one, should leave room for.
 * The cgroup's limit is given back after. Then whether B's pod runs on, the same pod, its process
 * alive, and A's; what the kernel killed, the node agent ended or said of another pod's want and
 * kubelet evicted meanwhile; what the agent's guard set on each sandbox's cgroup, and says it did
 * of each; and whether A's session falls back, bound anew on its workspace: a line of
 * logs/oom.jsonl a trial. Both sessions are forgotten after.
 */
import { appendFileSync } from "node:fs";
import { randomBytes } from "node:crypto";
import { join } from "node:path";

import type { CoreV1Event, V1Pod } from "@kubernetes/client-node";

import { NAMESPACE } from "../../cli/src/manifests/common.ts";
import { NODE_AGENT_PORT, NODE_AGENT_TOKEN_VARIABLE } from "../../src/node-agent/names.ts";
import { bindSession, boundPodOf, gatewayCall, kube, mainToken, nodeAgents, onNode, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { errorText, forgetWorkspace, loggedOf } from "./alasio.ts";
import { BENCH_DIR, RUNS } from "./settings.ts";

/** What each trial grows: BENCH_OOM_ARM, `grow` unless said, as the module says. */
const ARM = process.env["BENCH_OOM_ARM"] || "grow";
if (!["grow", "foreign", "burst"].includes(ARM)) throw new Error(`BENCH_OOM_ARM is ${ARM}, not grow, foreign or burst`);

/** What the Burstable pods' cgroup is held to beyond what it holds as A begins, which A outgrows: BENCH_OOM_HEADROOM_MIB, 2048 unless said. */
const HEADROOM_BYTES = Number(process.env["BENCH_OOM_HEADROOM_MIB"] || 2048) * 1024 ** 2;
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
/**
 * A, or the foreign pod: GROWTH_MIB more every 80 ms, every page written, until it is killed, or
 * holds GROWER_MOST_BYTES, three times HEADROOM_BYTES, and stops: one not killed so never fills
 * the node, as the runner, starved, would end the job.
 */
const GROWER_MOST_BYTES = 3 * HEADROOM_BYTES;
/** What A grows by every 80 ms, in MiB: BENCH_OOM_GROWTH_MIB, 64 unless said. */
const GROWTH_MIB = Number(process.env["BENCH_OOM_GROWTH_MIB"] || 64);
const GROWER = `const held = []; const grow = setInterval(() => (held.length * ${GROWTH_MIB} * 1024 * 1024 >= ${GROWER_MOST_BYTES} ? clearInterval(grow) : held.push(Buffer.alloc(${GROWTH_MIB} << 20, 7))), 80);`;
/** A, of the burst arm: 1 GiB at once, every page written, held 15 s, then said. */
const BURST = "const b = Buffer.alloc(1 << 30, 7); setTimeout(() => console.log('held', b.length), 15000)";
/** How long the burst arm waits, once the Burstable pods' cgroup is held, for the guard to set what limits it sets. */
const GUARD_SETTLE_MS = 5_000;
/** How long the foreign pod may take to start, its image on the node already. */
const FOREIGN_START_MS = 60_000;

/** A new session's workspace, bound: its name, and its pod's. */
async function boundWorkspace(): Promise<{ readonly workspace: string; readonly pod: string; readonly uid: string; readonly node: string }> {
  const workspace = `fs-${randomBytes(6).toString("hex")}`;
  const created = await gatewayCall(await mainToken(workspace), "POST", "/workspace");
  if (created.status !== 204) throw new Error(`the gateway made no workspace: ${created.status} ${created.said}`);
  const { name, node } = await bindSession(workspace);
  const pod = await kube.get<V1Pod>(ref("Pod", name, SESSIONS));
  return { workspace, pod: name, uid: pod?.metadata?.uid ?? "", node };
}

/** The restarts of each container of the platform's pods (alasio's namespace), by pod and container. */
async function platformRestarts(): Promise<Record<string, number>> {
  const restarts: Record<string, number> = {};
  for (const pod of await kube.list<V1Pod>("Pod", { namespace: NAMESPACE })) {
    for (const status of pod.status?.containerStatuses ?? []) restarts[`${pod.metadata?.name ?? ""}/${status.name}`] = status.restartCount;
  }
  return restarts;
}

/** Whose each process the kernel's OOM killed was, by the pod its memory cgroup names: A's, B's, or another's (its name), the foreign pod's said so. */
const victimOf = (line: string, a: { readonly uid: string }, b: { readonly uid: string }, foreignUid: string | null): string => {
  const cgroup = /task_memcg=(\S+)/u.exec(line)?.[1] ?? "";
  const task = /task=([^,]+)/u.exec(line)?.[1] ?? "";
  const of = (uid: string) => cgroup.includes(uid.replaceAll("-", "_")) || cgroup.includes(uid);
  return of(a.uid) ? `A ${task}` : of(b.uid) ? `B ${task}` : foreignUid !== null && of(foreignUid) ? `other foreign ${task} ${cgroup}` : `other ${task} ${cgroup}`;
};

/** What the node's cgroups of the pod `uid` say: the pod's memory.events, and each of its scopes holding processes, its memory.max, memory.oom.group and memory.events. */
async function podCgroupsOf(node: string, uid: string): Promise<string[]> {
  const said = await onNode(node, [
    `pod=$(ls -d /sys/fs/cgroup/kubepods.slice/*/kubepods-*-pod${uid.replaceAll("-", "_")}.slice /sys/fs/cgroup/kubepods.slice/kubepods-pod${uid.replaceAll("-", "_")}.slice 2>/dev/null | head -n 1)`,
    `[ -n "$pod" ] || { echo gone; exit 0; }`,
    `echo "pod $(tr '\n' ' ' < "$pod/memory.events")"`,
    `for scope in "$pod"/*/; do [ -n "$(cat "$scope/cgroup.procs")" ] && echo "$(basename "$scope") max=$(cat "$scope/memory.max") group=$(cat "$scope/memory.oom.group") current=$(cat "$scope/memory.current") $(tr '\n' ' ' < "$scope/memory.events")"; done; true`,
  ].join("; ")).catch((error: unknown) => `unread: ${errorText(error)}`);
  return said.trim().split("\n").filter(Boolean);
}

/** What the agent of `node` logged that `said` matches since `since` (ms): its lines. */
async function agentSaid(node: string, since: number, said: RegExp): Promise<string[]> {
  const pod = (await nodeAgents()).get(node) ?? "";
  const logs = await kube.logs(NAMESPACE, pod, "node-agent", { timestamps: true }).catch(() => "");
  return logs.split("\n").filter((line) => said.test(line) && new Date(line.split(" ")[0] ?? "").getTime() >= since - 1_000).map((line) => line.slice(0, 500));
}

/** What the guard of `node`'s agent says it did of the pods `pods` (GET /memory/guard), or why it could not be asked. */
async function guardRecordsOf(node: string, pods: readonly string[]): Promise<unknown> {
  const agent = (await nodeAgents()).get(node) ?? "";
  try {
    const said = await kube.execOk(NAMESPACE, agent, ["sh", "-c", `curl -sS -H "authorization: Bearer $${NODE_AGENT_TOKEN_VARIABLE}" http://127.0.0.1:${NODE_AGENT_PORT}/memory/guard`], { container: "node-agent" });
    const { sandboxes } = JSON.parse(said) as { readonly sandboxes: readonly { readonly pod: string }[] };
    return sandboxes.filter(({ pod }) => pods.includes(pod));
  } catch (error) {
    return { unread: errorText(error).slice(0, 400) };
  }
}

/**
 * The foreign arm's pod: on `node`, in a namespace of no sessions, runc's (no runtime class),
 * Burstable, of `image`, growing as GROWER does until it ends or GROWTH_BOUND_MS has gone since it
 * started; deleted then. Its uid, and how it ended: its container's reason and exit code, as the
 * pod's status said, and how long it ran.
 */
async function foreignGrown(node: string, image: string): Promise<{ readonly uid: string; readonly ended: Record<string, unknown> }> {
  const name = `oom-foreign-${randomBytes(4).toString("hex")}`;
  const target = ref("Pod", name, "default");
  const pod: V1Pod = {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: "default" },
    spec: {
      nodeName: node,
      restartPolicy: "Never",
      terminationGracePeriodSeconds: 0,
      containers: [{ name: "grower", image, imagePullPolicy: "IfNotPresent", command: ["node", "-e", GROWER], resources: { requests: { memory: "64Mi" }, limits: { memory: "8Gi" } } }],
    },
  };
  await kube.apply(pod);
  try {
    const t0 = Date.now();
    let started: number | null = null;
    for (;;) {
      const now = await kube.get<V1Pod>(target);
      const state = now?.status?.containerStatuses?.[0]?.state;
      if (state?.terminated) {
        const { reason, exitCode, startedAt, finishedAt } = state.terminated;
        return { uid: now?.metadata?.uid ?? "", ended: { reason: reason ?? null, exit_code: exitCode, ran_ms: startedAt && finishedAt ? new Date(finishedAt).getTime() - new Date(startedAt).getTime() : null, phase: now?.status?.phase ?? null } };
      }
      if (state?.running && started === null) started = Date.now();
      if (started === null ? Date.now() - t0 > FOREIGN_START_MS : Date.now() - started > GROWTH_BOUND_MS) {
        return { uid: now?.metadata?.uid ?? "", ended: { reason: started === null ? "never started" : "still running", exit_code: null, phase: now?.status?.phase ?? null, waiting: now?.status?.containerStatuses?.[0]?.state?.waiting?.reason ?? null } };
      }
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
  } finally {
    await kube.kill("default", name).catch(() => {});
  }
}

/** One trial, as the module says: its line. */
async function trial(number: number): Promise<Record<string, unknown>> {
  const started = Date.now();
  const restartsBefore = await platformRestarts();
  const b = await boundWorkspace();
  const a = await boundWorkspace();
  try {
    await kube.execOk(SESSIONS, b.pod, ["sh", "-c", `nohup node -e "${HOLDER}" > /tmp/holder.log 2>&1 & sleep 3`], { container: "bayma" });
    // A's workspace marked, for its session's fallback to be told by.
    await kube.execOk(SESSIONS, a.pod, ["sh", "-c", 'echo kept > "$HOME/oom-marker"'], { container: "bayma" });
    // The Burstable pods' cgroup held to what it holds now and HEADROOM_BYTES more: its limit before, to give back.
    const limited = (await onNode(b.node, `dir=${BURSTABLE}; before=$(cat "$dir/memory.max"); echo $(( $(cat "$dir/memory.current") + ${HEADROOM_BYTES} )) > "$dir/memory.max"; echo "$dir $before $(cat "$dir/memory.max")"`)).trim();
    const [cgroup = "", limitBefore = "max", limit = ""] = limited.split(" ");
    // The burst arm's A takes its GiB once the guard has looked at the room left, and limited the sandboxes if it does.
    if (ARM === "burst") await new Promise((resolve) => setTimeout(resolve, GUARD_SETTLE_MS));
    const cgroupsBefore = ARM === "burst" ? { a: await podCgroupsOf(a.node, a.uid), b: await podCgroupsOf(b.node, b.uid) } : undefined;
    const grown = Date.now();
    // The foreign arm's pod, of the image the sessions' bayma runs.
    const image = ARM === "foreign" ? (await kube.get<V1Pod>(ref("Pod", b.pod, SESSIONS)))?.spec?.containers.find(({ name }) => name === "bayma")?.image ?? "" : "";
    const foreign = ARM === "foreign" ? await foreignGrown(b.node, image) : null;
    const ran = ARM === "foreign" ? null : await Promise.race([
      kube.exec(SESSIONS, a.pod, ["node", "-e", ARM === "burst" ? BURST : GROWER], { container: "bayma" }).then((done) => ({ code: done.code, out: done.stdout.trim().slice(-200), said: done.stderr.slice(-400) }), (error: unknown) => ({ code: null, out: "", said: errorText(error).slice(-400) })),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), GROWTH_BOUND_MS)),
    ]);
    const aEndedMs = Date.now() - grown;
    // As A, or the foreign pod, ended: what the guard set on each sandbox's scope, and the OOMs each cgroup had.
    const cgroupsAtEnd = { a: await podCgroupsOf(a.node, a.uid), b: await podCgroupsOf(b.node, b.uid) };
    // One not killed in time ended first, so that it grows no further once the limit is given back.
    if (ran === null && ARM !== "foreign") await kube.kill(SESSIONS, a.pod);
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
    // Of each OOM, the kernel's whole report, the tasks it weighed with their RSS and scores among it.
    const report = (await onNode(b.node, `journalctl -k --since @${since} --no-pager -o short-iso-precise 2>/dev/null | sed -n '/invoked oom-killer/,/Killed process/p' | tail -n 400 || true`)).trim().split("\n").filter(Boolean);
    const victims = kernel.filter((line) => line.includes("oom-kill:constraint")).map((line) => victimOf(line, a, b, foreign?.uid ?? null));
    const restartsAfter = await platformRestarts();
    const platformRestarted = Object.entries(restartsAfter).filter(([name, count]) => count > (restartsBefore[name] ?? 0)).map(([name]) => name);
    // A's session after: its pod gone and its binding let go, as a broken sandbox's is, and what the gateway said of it.
    const aBound = await boundPodOf(a.workspace);
    const aGateway = (await loggedOf(b.node, a.workspace)).gateway.filter((line) => /releas|broken|oom|memory/iu.test(line)).slice(-6);
    const ended = await agentSaid(b.node, started, /ended the sandbox|could not end the sandbox|could not be guarded/u);
    const foreignSaid = await agentSaid(b.node, started, /another pod's want/u);
    const guardRecords = await guardRecordsOf(b.node, [a.pod, b.pod]);
    // A ran on: its pod there, running, no container of it restarted, and, of the burst arm, its GiB held to the end.
    const aSurvived = aPod.there && aPod.phase === "Running" && aPod.restarts.every((count) => count === 0) && (ARM !== "burst" || (ran?.code === 0 && ran.out.includes("held 1073741824")));
    // A's session falls back: bound anew, of a fresh sandbox (it was never saved) on its workspace, which its marker is still in.
    const fallback = await (async () => {
      const t0 = Date.now();
      try {
        const { name } = await bindSession(a.workspace, 180_000);
        const marker = await kube.exec(SESSIONS, name, ["sh", "-c", 'cat "$HOME/oom-marker"'], { container: "bayma" });
        return { pod: name, anew: name !== a.pod, marker: marker.stdout.trim(), code: marker.code, ms: Date.now() - t0 };
      } catch (error) {
        return { failure: errorText(error).slice(0, 400), ms: Date.now() - t0 };
      }
    })();
    const evicted = (await kube.list<CoreV1Event>("Event", { namespace: SESSIONS }))
      .filter(({ reason, lastTimestamp, eventTime }) => reason === "Evicted" && new Date(String(lastTimestamp ?? eventTime ?? 0)).getTime() >= started)
      .map(({ involvedObject, message }) => `${involvedObject?.name ?? ""}: ${message ?? ""}`);
    return {
      trial: number,
      arm: ARM,
      a: { pod: a.pod, ended: ran !== null, exec: ran, ended_ms: aEndedMs, ...aPod },
      b: { pod: b.pod, ...bPod, stamp_age_ms: stampAge },
      b_survived: bPod.there && bPod.phase === "Running" && stampAge !== null && stampAge < 2_000,
      a_survived: aSurvived,
      foreign: foreign && { image, ...foreign.ended },
      victims,
      platform_restarted: platformRestarted,
      a_after: { bound: aBound?.name ?? null, gateway: aGateway },
      platform_killed: victims.filter((victim) => victim.startsWith("other") && !victim.startsWith("other foreign")).length,
      agent_ended: ended,
      agent_foreign: foreignSaid,
      guard_records: guardRecords,
      cgroups_before: cgroupsBefore,
      cgroups_at_end: cgroupsAtEnd,
      fallback,
      report,
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
