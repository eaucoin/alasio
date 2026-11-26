/**
 * The two-sandbox OOM measure (BENCH_MODE=oom): does an OOM above two sessions' sandboxes, as
 * memory grows past what the cgroup above them may hold, end a session it should not?
 * Each trial binds two new sessions on the node, their pods' limits far above (test/e2e/harness.ts),
 * and holds the cgroup of the node's Burstable pods, theirs, to what it holds then but its clean
 * page cache, which the kernel reclaims as it nears the limit (as the guard's reclaimableIn counts
 * it), what B is to hold, and BENCH_OOM_HEADROOM_MIB (2048) more, so that the kernel's OOM is of
 * that cgroup, above both sandboxes, and not of a pod's own nor of the node, which a runner's
 * memory filled whole starves before the kernel kills; and 12 s later, past the guard's windows,
 * so that what the room lost as it was held is no one's growth, B fills 1.5 GiB and charges and
 * frees 4 MiB every 20 ms beside it; 3 s after, as BENCH_OOM_ARM says:
 * - `grow` (unless said): A grows by BENCH_OOM_GROWTH_MIB (64) every 80 ms until it is killed;
 * - `foreign`: A is left as it is, and a pod of no sandbox, runc's, Burstable, on the same node in
 *   a namespace of no sessions, grows as A would until it ends, so that the want is another pod's;
 *   it grows only once its process is seen in a cgroup under the one held;
 * - `burst`: A takes 1 GiB at once and holds it 15 s, as a session's exec may, once the guard has
 *   limited its sandbox, which the limit should leave room for.
 * A trial whose arm's conditions did not come about (the foreign pod's process elsewhere, A's
 * sandbox not limited) is a line of its setup alone, `setup_ok` false, and no result.
 * The cgroup is held so anew every 50 ms, as kubelet sets its limit back as pods come and go,
 * and its limit given back after. Then whether B's pod runs on, the same pod, its process
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

/** What the Burstable pods' cgroup is held to beyond what it holds as A begins but its clean page cache, which A outgrows: BENCH_OOM_HEADROOM_MIB, 2048 unless said. */
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
/** What B's sandbox comes to hold more as it runs HOLDER, as trials measured it: its 1.5 GiB, and some 128 MiB of node's and gVisor's beside. */
const HOLDER_BYTES = 1.5 * 1024 ** 3 + 128 * 1024 ** 2;
/** How long after the Burstable pods' cgroup is held anything grows: past the guard's window of 10 s over which it measures what the room lost, and by whom. */
const CAP_SETTLE_MS = 12_000;
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
/** How long the burst arm waits, once the Burstable pods' cgroup is held, for the guard to limit A's sandbox. */
const GUARD_SETTLE_MS = 10_000;
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
 * Holds the Burstable pods' cgroup on `node` to what it holds but its clean page cache (active and
 * inactive file pages, but those dirty or being written back, as the guard's reclaimableIn counts
 * them), what B is to hold (HOLDER_BYTES), and HEADROOM_BYTES more: its path, its limit before, to
 * give back, what it held, of it what was clean page cache, and the limit set.
 */
async function capBurstable(node: string): Promise<{ readonly path: string; readonly limitBefore: string; readonly current: number; readonly reclaimable: number; readonly limit: number }> {
  const reclaimable = `awk '$1 == "active_file" || $1 == "inactive_file" { r += $2 } $1 == "file_dirty" || $1 == "file_writeback" { r -= $2 } END { printf "%.0f", (r > 0 ? r : 0) }' "$dir/memory.stat"`;
  const said = (await onNode(node, [
    `dir=${BURSTABLE}`,
    'before=$(cat "$dir/memory.max")',
    'current=$(cat "$dir/memory.current")',
    `reclaimable=$(${reclaimable})`,
    `echo $(( current - reclaimable + ${HOLDER_BYTES + HEADROOM_BYTES} )) > "$dir/memory.max"`,
    'echo "$dir $before $current $reclaimable $(cat "$dir/memory.max")"',
  ].join("; "))).trim();
  const [path = "", limitBefore = "max", current = "", held = "", limit = ""] = said.split(" ");
  return { path, limitBefore, current: Number(current), reclaimable: Number(held), limit: Number(limit) };
}

/**
 * Holds the cgroup `path` on `node` to `limit` every 50 ms until stopped, by a loop of the node's
 * own, as kubelet's QoS manager sets the Burstable pods' slice's limit back as pods come and go:
 * so seldom seen set back by the guard, which looks every 250 ms, and would take the room it then
 * finds and loses again for another pod's. Once stopped, each limit it found it set back to, a
 * time it was.
 */
async function keepCap(node: string, path: string, limit: number): Promise<{ readonly stop: () => Promise<readonly string[]> }> {
  const flag = `/run/alasio-oom-cap-${randomBytes(4).toString("hex")}`;
  const loop = `while [ -e ${flag} ]; do held=$(cat ${path}/memory.max); [ "$held" = ${limit} ] || { echo ${limit} > ${path}/memory.max; echo "$held" >> ${flag}.resets; }; sleep 0.05; done`;
  await onNode(node, `touch ${flag} ${flag}.resets; setsid nohup sh -c '${loop}' > /dev/null 2>&1 < /dev/null &`);
  return {
    stop: async () => {
      const said = await onNode(node, `rm -f ${flag}; sleep 0.3; cat ${flag}.resets; rm -f ${flag}.resets`).catch(() => "");
      return said.trim().split("\n").filter(Boolean);
    },
  };
}

/** The memory.max of A's sandbox's scope, as podCgroupsOf says it: its bytes, "max" for none, or null where it says none. */
const scopeMaxOf = (lines: readonly string[]): string | null => lines.map((line) => / max=(\S+)/u.exec(line)?.[1]).find((max) => max !== undefined) ?? null;

/**
 * The foreign arm's pod: on `node`, in a namespace of no sessions, runc's (no runtime class),
 * Burstable, its requests of CPU and memory under its limits, of `image`. Its process waits until
 * it is seen in a cgroup under `capped`, the Burstable pods' cgroup held, and that cgroup is held to
 * `limit` anew (kubelet sets the slice's limit back as it makes a pod's slice under it, as for
 * any pod that comes or goes), then grows as GROWER does until it ends or GROWTH_BOUND_MS has gone; the pod is deleted
 * then. Its uid; its setup: its QoS class and resources as admitted, its process's
 * /proc/<pid>/cgroup, whether that is under `capped`, and the limit `capped` had then and was held
 * to; the most its pod's cgroup held, read every second, its memory.peak, and each limit `capped`
 * had meanwhile; and how it ended: its container's reason and exit code, as the pod's status said,
 * and how long it ran; null where its setup failed.
 */
async function foreignGrown(node: string, image: string, capped: string, limit: number): Promise<{ readonly uid: string; readonly setup: Record<string, unknown>; readonly setupOk: boolean; readonly ended: Record<string, unknown> | null }> {
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
      containers: [{
        name: "grower",
        image,
        imagePullPolicy: "IfNotPresent",
        command: ["sh", "-c", 'while [ ! -e /tmp/go ]; do sleep 0.2; done; exec node -e "$GROWER"'],
        env: [{ name: "GROWER", value: GROWER }],
        resources: { requests: { cpu: "10m", memory: "64Mi" }, limits: { cpu: "1", memory: "8Gi" } },
      }],
    },
  };
  await kube.apply(pod);
  try {
    const t0 = Date.now();
    let now = await kube.get<V1Pod>(target);
    while (!now?.status?.containerStatuses?.[0]?.state?.running && Date.now() - t0 < FOREIGN_START_MS) {
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      now = await kube.get<V1Pod>(target);
    }
    const uid = now?.metadata?.uid ?? "";
    const under = uid.replaceAll("-", "_");
    // Its pod's cgroup, and the process of it that is not the pause container's, with its /proc/<pid>/cgroup.
    const located = (await onNode(node, [
      `pod=$(ls -d /sys/fs/cgroup/kubepods.slice/*/kubepods-*-pod${under}.slice /sys/fs/cgroup/kubepods.slice/kubepods-pod${under}.slice /sys/fs/cgroup/kubepods/*/pod${uid} /sys/fs/cgroup/kubepods/pod${uid} 2>/dev/null | head -n 1)`,
      'for pid in $(cat "$pod"/*/cgroup.procs 2>/dev/null); do [ "$(cat /proc/$pid/comm)" != pause ] && { echo "$pod $pid $(cat /proc/$pid/cgroup)"; break; }; done; true',
    ].join("; ")).catch((error: unknown) => `unread: ${errorText(error)}`)).trim();
    const [podCgroup = "", pid = "", procCgroup = ""] = located.split(" ");
    const cgroupPath = procCgroup.startsWith("0::") ? `/sys/fs/cgroup${procCgroup.slice(3)}` : "";
    // The held cgroup's limit as the pod's making left it, which kubelet may have set back as it made the pod's slice under it; and as held anew.
    const capFound = (await onNode(node, `cat ${capped}/memory.max`)).trim();
    const capSet = (await onNode(node, `echo ${limit} > ${capped}/memory.max; cat ${capped}/memory.max`)).trim();
    const setup = {
      pod: name,
      qos_class: now?.status?.qosClass ?? null,
      admitted_resources: now?.spec?.containers[0]?.resources ?? null,
      waiting: now?.status?.containerStatuses?.[0]?.state?.waiting?.reason ?? null,
      pod_cgroup: podCgroup,
      pid: pid || null,
      proc_cgroup: procCgroup || located,
      under_capped: cgroupPath.startsWith(`${capped}/`),
      cap_found: capFound,
      cap_set: capSet,
    };
    const setupOk = now?.status?.qosClass === "Burstable" && setup.under_capped && capSet === String(limit);
    if (!setupOk) return { uid, setup, setupOk, ended: null };
    await kube.execOk("default", name, ["touch", "/tmp/go"], { container: "grower" });
    const grown = Date.now();
    let peakCurrent = 0;
    // Each limit the held cgroup had as the pod grew, read every second.
    const capSeen = new Set<string>();
    for (;;) {
      const [current = "0", max = ""] = (await onNode(node, `cat ${podCgroup}/memory.current 2>/dev/null || echo 0; cat ${capped}/memory.max`).catch(() => "0")).trim().split("\n");
      peakCurrent = Math.max(peakCurrent, Number(current) || 0);
      if (max) capSeen.add(max);
      now = await kube.get<V1Pod>(target);
      const state = now?.status?.containerStatuses?.[0]?.state;
      if (state?.terminated || Date.now() - grown > GROWTH_BOUND_MS) {
        const memoryPeak = (await onNode(node, `cat ${podCgroup}/memory.peak 2>/dev/null || echo unread`).catch(() => "unread")).trim();
        const terminated = state?.terminated;
        const containerMs = terminated?.startedAt && terminated.finishedAt ? new Date(terminated.finishedAt).getTime() - new Date(terminated.startedAt).getTime() : null;
        return {
          uid,
          setup,
          setupOk,
          ended: { reason: terminated ? (terminated.reason ?? null) : "still running", exit_code: terminated?.exitCode ?? null, ran_ms: Date.now() - grown, container_ms: containerMs, phase: now?.status?.phase ?? null, peak_current: peakCurrent, memory_peak: memoryPeak, cap_seen: [...capSeen] },
        };
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
  // What gives the Burstable pods' cgroup its limit back, once held; called again, it does nothing.
  let giveBack = async (): Promise<void> => {};
  try {
    // The Burstable pods' cgroup held to what it holds now but its clean page cache, what B is to hold and HEADROOM_BYTES more; its limit before given back once A, or the foreign pod, has ended.
    const cap = await capBurstable(b.node);
    const cgroup = cap.path;
    const capOk = cgroup !== "" && Number.isFinite(cap.limit) && cap.limit > 0;
    // Held so until given back, as kubelet sets the slice's limit back as pods come and go.
    const kept = capOk ? await keepCap(b.node, cgroup, cap.limit) : null;
    let givenBack = false;
    let capResets: readonly string[] = [];
    giveBack = async () => {
      if (givenBack) return;
      givenBack = true;
      capResets = (await kept?.stop()) ?? [];
      await onNode(b.node, `echo ${cap.limitBefore} > ${cgroup}/memory.max`);
    };
    // A setup that failed: its line, of what was set up alone, the limit given back.
    const setupFailed = async (setup: Record<string, unknown>) => {
      await giveBack();
      return { trial: number, arm: ARM, setup_ok: false, cap, cap_resets: capResets.length, setup, took_ms: Date.now() - started };
    };
    if (!capOk) return await setupFailed({ cap: "not set" });
    // What the room lost as it was held is past the guard's window before anything grows: no other pod's want of its, nor of the pool's pods made for A's and B's.
    await new Promise((resolve) => setTimeout(resolve, CAP_SETTLE_MS));
    // B fills its 1.5 GiB 3 s before anything grows, as a session's REPL loading a dataset beside it.
    await kube.execOk(SESSIONS, b.pod, ["sh", "-c", `nohup node -e "${HOLDER}" > /tmp/holder.log 2>&1 & sleep 3`], { container: "bayma" });
    // A's workspace marked, for its session's fallback to be told by.
    await kube.execOk(SESSIONS, a.pod, ["sh", "-c", 'echo kept > "$HOME/oom-marker"'], { container: "bayma" });
    // The burst arm's A takes its GiB once the guard has limited its sandbox, its scope's memory.max a number.
    let aLimitBefore: string | null = null;
    if (ARM === "burst") {
      const until = Date.now() + GUARD_SETTLE_MS;
      do {
        await new Promise((resolve) => setTimeout(resolve, 500));
        aLimitBefore = scopeMaxOf(await podCgroupsOf(a.node, a.uid));
      } while (!/^\d+$/u.test(aLimitBefore ?? "") && Date.now() < until);
      if (!/^\d+$/u.test(aLimitBefore ?? "")) return await setupFailed({ a_limit_before: aLimitBefore, cgroups: { a: await podCgroupsOf(a.node, a.uid), b: await podCgroupsOf(b.node, b.uid) } });
    }
    const cgroupsBefore = ARM === "burst" ? { a: await podCgroupsOf(a.node, a.uid), b: await podCgroupsOf(b.node, b.uid) } : undefined;
    const grown = Date.now();
    // The foreign arm's pod, of the image the sessions' bayma runs.
    const image = ARM === "foreign" ? (await kube.get<V1Pod>(ref("Pod", b.pod, SESSIONS)))?.spec?.containers.find(({ name }) => name === "bayma")?.image ?? "" : "";
    const foreign = ARM === "foreign" ? await foreignGrown(b.node, image, cgroup, cap.limit) : null;
    if (foreign && !foreign.setupOk) return await setupFailed({ foreign: { image, ...foreign.setup } });
    const ran = ARM === "foreign" ? null : await Promise.race([
      kube.exec(SESSIONS, a.pod, ["node", "-e", ARM === "burst" ? BURST : GROWER], { container: "bayma" }).then((done) => ({ code: done.code, out: done.stdout.trim().slice(-200), said: done.stderr.slice(-400) }), (error: unknown) => ({ code: null, out: "", said: errorText(error).slice(-400) })),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), GROWTH_BOUND_MS)),
    ]);
    const aEndedMs = Date.now() - grown;
    // As A, or the foreign pod, ended: what the guard set on each sandbox's scope, and the OOMs each cgroup had.
    const cgroupsAtEnd = { a: await podCgroupsOf(a.node, a.uid), b: await podCgroupsOf(b.node, b.uid) };
    // One not killed in time ended first, so that it grows no further once the limit is given back.
    if (ran === null && ARM !== "foreign") await kube.kill(SESSIONS, a.pod);
    await giveBack();
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
      setup_ok: true,
      a: { pod: a.pod, ended: ran !== null, exec: ran, ended_ms: aEndedMs, ...aPod },
      b: { pod: b.pod, ...bPod, stamp_age_ms: stampAge },
      b_survived: bPod.there && bPod.phase === "Running" && stampAge !== null && stampAge < 2_000,
      a_survived: aSurvived,
      a_limit: ARM === "burst" ? { before: aLimitBefore, after: scopeMaxOf(cgroupsAtEnd.a) } : undefined,
      foreign: foreign && { image, ...foreign.setup, ...foreign.ended },
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
      cgroup: { path: cgroup, limit_before: cap.limitBefore, current: cap.current, reclaimable: cap.reclaimable, limit: cap.limit, events, resets: capResets.length, reset_to: [...new Set(capResets)] },
      kernel,
      evicted,
      took_ms: Date.now() - started,
    };
  } finally {
    // A trial that threw gives the limit back too.
    await giveBack().catch(() => {});
    for (const { workspace } of [a, b]) await forgetWorkspace(workspace);
  }
}

/** The measure's trials, RUNS of them, each a line of logs/oom.jsonl. */
export async function oomTrials(): Promise<void> {
  for (let number = 1; number <= RUNS; number++) {
    const line = await trial(number).catch((error: unknown) => ({ trial: number, arm: ARM, setup_ok: false, failure: errorText(error) }));
    appendFileSync(join(BENCH_DIR, "logs", "oom.jsonl"), `${JSON.stringify(line)}\n`);
    console.error(`# OOM trial ${number}: ${JSON.stringify(line).slice(0, 600)}`);
  }
}
