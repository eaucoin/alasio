/**
 * A restore as the benchmark measures it: the probe's first request through the gateway binds
 * the released session, which restores it, and the probe's first exec answers once it is
 * serving; beside it, the restore's pod watched until runsc has every page loaded, the node's
 * counters, the sandbox's boot log, and, of the `trace` variant, the pod's own steps as kubelet
 * says them. What alasio's spans say of it is read once the run is over (withSpans).
 */
import type { V1Pod } from "@kubernetes/client-node";

import { BINDING_LABEL, keyHash } from "../../src/binder/names.ts";
import { STANDBY_ANNOTATION, STANDBY_LABEL } from "../../src/kube/sandbox-shape.ts";
import type { Manifest } from "../../src/node-agent/agent.ts";
import type { NodeMemory, SandboxState } from "../../src/node-agent/memory.ts";
import { SNAPSHOT_CACHE_DIR } from "../../src/node-agent/names.ts";
import { sessionKey } from "../../src/sandbox/names.ts";
import { boundPodOf, kube, onNode, ref, SESSIONS } from "../../test/e2e/harness.ts";
import { agentOk, driver, gone, loggedOf, msBetween, preparedLine, release, restores, sleep, sourceOf, watchRestore } from "./alasio.ts";
import { bootLog, diskReadBytes, layerCacheBytes, podMemory, sandboxIdOf, storeBytes, storeQuiet } from "./node.ts";
import type { Metrics } from "./results.ts";
import { restoreLogged } from "./runsc-log.ts";
import { compactionAtT0, resumeSteps, type Span } from "./spans.ts";
import { PAGE_CACHE, VARIANT } from "./settings.ts";

/** What the driver's probe says of a restore (bench/common/driver/driver.ts probe). */
export interface Probed {
  readonly t0_ns: string;
  readonly t0_wall_ms: number;
  readonly initialized_ns: string | null;
  readonly first_exec_end_ns: string;
  readonly first_exec_ms: number;
  readonly verify_ms: number;
  readonly ok: boolean;
  readonly failure: string | null;
  readonly correctness: Record<string, unknown>;
  readonly workload_rss_bytes: number;
  readonly notice: string | null;
  readonly refusals: readonly string[];
}

/** The probe's own part of its first exec: from its MCP session initialized, the gateway serving, to its first exec's answer. */
export const probeExecMs = (probed: Probed): number | null =>
  probed.initialized_ns === null ? null : msBetween(BigInt(probed.initialized_ns), BigInt(probed.first_exec_end_ns));

/** How long a restore may take to its first exec's answer, and to every page resident, from its t0. */
const RESTORE_BOUND_MS = 300_000;

/**
 * A restore of the workspace from its snapshot `manifest` through the gateway, its first request
 * the probe's, on `node`, whose agent restores it: its result's measures, checks and details, its
 * pod, and its window on the wall clock, from its t0 to its first exec's answer. A cold one's bytes
 * from the store are counted until the fetch it starts is through. `during`, where given, runs once
 * every page of it is resident, before that fetch is through: what it answers, as `during`.
 */
export async function restore<D = null>(node: string, workspace: string, sessionId: string, fixture: string, runId: string, manifest: Manifest, kind: "warm" | "cold", during?: () => Promise<D>) {
  const storeBefore = await storeBytes(node);
  const diskBefore = await diskReadBytes(node);
  // What the node's standbys hold idle, one of which the restore may claim.
  const memoryBefore = await agentOk(node, "GET", "/memory").then((said) => JSON.parse(said) as NodeMemory, () => null);
  // Settled whatever comes of the probe, which fails a restore that never answers before its pod is seen.
  const watching = watchRestore(workspace, manifest.release).then((watched) => ({ watched }), (error: unknown) => ({ error }));
  const timeline = VARIANT === "trace" ? podTimeline(workspace) : null;
  const probed = await driver<Probed>(workspace, "probe", "--session", sessionId, "--fixture", fixture, "--run-id", runId);
  restores.lastAt = probed.t0_wall_ms;
  const settled = await watching;
  if ("error" in settled) throw settled.error;
  const { residentNs, pod, error } = settled.watched;
  const duringIt = during === undefined ? null : await during();
  const diskRead = (await diskReadBytes(node)) - diskBefore;
  const storeReceived = (kind === "cold" ? await storeQuiet(node) : (await storeBytes(node)).received) - storeBefore.received;
  const name = pod.metadata?.name ?? "";
  if (pod.spec?.nodeName !== node) throw new Error(`the restore's pod ${name} runs on ${pod.spec?.nodeName}, not on ${node}, whose counters the run reads`);
  const memory = await podMemory(node, pod.metadata?.uid ?? "");
  // What the node agent reads of the node's memory (src/node-agent/memory.ts): this sandbox's, as its pod's cgroup and its Sentry
  // say it, and the node's budget of what is held ahead; none from an agent that does not answer it.
  const nodeMemory = await agentOk(node, "GET", "/memory").then((said) => JSON.parse(said) as NodeMemory, () => null);
  // Its node's sandboxes', by their pods' state: the states alasio has, its tiers (frozen, reclaimed) yet to come with their control.
  const byState = (state: SandboxState) => nodeMemory?.sandboxes.filter((sandbox) => sandbox.state === state).reduce((sum, { bytes }) => sum + (bytes ?? 0), 0) ?? null;
  const agentMemory = nodeMemory === null ? null : {
    sandbox: nodeMemory.sandboxes.find((sandbox) => sandbox.pod === name) ?? null,
    available_bytes: nodeMemory.availableBytes,
    agent_bytes: nodeMemory.agentBytes,
    agent_anon_bytes: nodeMemory.agentAnonBytes,
    agent_file_bytes: nodeMemory.agentFileBytes,
    agent_active_file_bytes: nodeMemory.agentActiveFileBytes,
    agent_inactive_file_bytes: nodeMemory.agentInactiveFileBytes,
    agent_dirty_bytes: nodeMemory.agentDirtyBytes,
    agent_events: nodeMemory.agentEvents,
    budget_bytes: nodeMemory.budgetBytes,
    budgeted_bytes: nodeMemory.budgetedBytes,
  };
  // Restored into a standby of its node's (src/gateway/standbys.ts), or into a pod made for it.
  const standby = pod.metadata?.annotations?.[STANDBY_ANNOTATION] !== undefined;
  // What the standby it claimed held, idle, before.
  const standbyIdle = standby ? (memoryBefore?.sandboxes.find((sandbox) => sandbox.pod === name && sandbox.state === "standby")?.bytes ?? null) : null;
  const layerCache = await layerCacheBytes(node);
  const logged = restoreLogged(await bootLog(node, await sandboxIdOf(node, name)), standby);
  const { line: prepared, snapshot: restoredSnapshot } = await preparedLine(node, workspace, manifest.snapshot);
  const source = await sourceOf(node, pod, `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}/${restoredSnapshot}`);
  const logs = await loggedOf(node, workspace);
  const resident = residentNs === null ? null : msBetween(BigInt(probed.t0_ns), residentNs);
  const loaded = logged.loadedBytes;
  const metrics: Metrics = {
    first_exec_ms: probed.first_exec_ms,
    verify_ms: probed.verify_ms,
    resident_ms: resident,
    resident_how: resident === null ? null : "signal",
    memory_peak_bytes: memory.peak,
    memory_bytes: memory.current,
    workload_rss_bytes: probed.workload_rss_bytes,
    memory_file_bytes: agentMemory?.sandbox?.shmemBytes ?? null,
    sandbox_overhead_bytes: agentMemory?.sandbox?.bytes == null || agentMemory.sandbox.shmemBytes === null ? null : agentMemory.sandbox.bytes - agentMemory.sandbox.shmemBytes,
    node_agent_memory_bytes: agentMemory?.agent_bytes ?? null,
    node_agent_anon_bytes: agentMemory?.agent_anon_bytes ?? null,
    node_agent_file_bytes: agentMemory?.agent_file_bytes ?? null,
    gofer_bytes: agentMemory?.sandbox?.goferBytes ?? null,
    // Resident, its MemoryFile's, against what it touched before its pages were read, which runsc says; what it touched after is not said.
    awaited_bytes: logged.awaitedBytes,
    standby_idle_bytes: standbyIdle,
    sandboxes_bound_bytes: byState("bound"),
    sandboxes_standby_bytes: byState("standby"),
    sandboxes_free_bytes: byState("free"),
    layer_cache_bytes: layerCache,
    restore_kernel_load_ms: logged.kernelLoadMs,
    restore_memory_load_ms: logged.memoryLoadMs,
    restore_to_running_ms: logged.toRunningMs,
    probe_exec_ms: probeExecMs(probed),
    loaded_bytes: loaded,
    store_received_bytes: storeReceived,
    disk_read_bytes: diskRead,
    // Of a cold restore, what the store sent the node for it, its fetch included; of a warm one, what the node's disks read.
    read_amplification: loaded === null || loaded === 0 ? null : (kind === "cold" ? storeReceived : diskRead) / loaded,
    page_cache: kind === "cold" ? "dropped" : PAGE_CACHE,
  };
  const window = { fromMs: probed.t0_wall_ms, toMs: probed.t0_wall_ms + probed.first_exec_ms };
  const details = {
    pod: name, node, standby, source, prepared, restored_snapshot: restoredSnapshot, chain: manifest.layers.length, runsc_restore_ms: logged.runscRestoreMs, logged: logged.said,
    memory_stat: memory.stat, agent_memory: agentMemory, resident_error: error, logs, refusals: probed.refusals, t0_wall_ms: probed.t0_wall_ms, end_wall_ms: Date.now(),
    ...(timeline === null ? {} : { pod_timeline: await timeline.stop(pod) }),
  };
  return { probed, metrics, details, pod: name, window, during: duringIt };
}

/**
 * What alasio's spans of the run say of a restore whose probe began at `t0Ms`: runsc's resume with
 * bayma's hook, the wait for the pod's network policies, whether the release's compaction ran as it
 * began, and what of the first exec is the product's orchestration, neither runsc's restore to
 * runnable, its resume, nor the probe's own MCP session and exec.
 */
export function restoreSpanMetrics(spans: readonly Span[], workspace: string, t0Ms: number, metrics: Metrics): Metrics {
  const { runscResumeMs, policyWaitMs } = resumeSteps(spans, workspace, t0Ms);
  const toRunning = typeof metrics.restore_to_running_ms === "number" ? metrics.restore_to_running_ms : null;
  const probeExec = typeof metrics.probe_exec_ms === "number" ? metrics.probe_exec_ms : null;
  const first = typeof metrics.first_exec_ms === "number" ? metrics.first_exec_ms : null;
  return {
    runsc_resume_ms: runscResumeMs,
    policy_wait_ms: policyWaitMs,
    orchestration_ms: first !== null && toRunning !== null && runscResumeMs !== null && probeExec !== null ? first - toRunning - runscResumeMs - probeExec : null,
    compaction_at_t0: compactionAtT0(spans, workspace, t0Ms),
  };
}

/** Whether a restore passed: its probe's checks, its image read from `source`, within RESTORE_BOUND_MS; or why not. */
export function restoreOutcome(restored: Awaited<ReturnType<typeof restore>>, source: "cache" | "bucket"): { readonly ok: boolean; readonly failure: string | null } {
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

/** How long a `restored` session runs after its restore before its live save: past runsc's working-set-window (3 s). */
const WORKING_SET_HELD_MS = 6000;

/**
 * `restored`'s release and restore of the session before its saves, the restore asked by the
 * fixture's first exec as alasio's next request asks it, and the session then left to run past the
 * working-set-window: what runsc logged of the working set it recorded, and of the one it read first.
 */
export async function restoreBefore(workspace: string, sessionId: string, fixture: string): Promise<Record<string, unknown>> {
  const bound = await boundPodOf(workspace);
  if (!bound) throw new Error(`no pod is bound to ${workspace}`);
  const released = await release(workspace);
  if (released.status < 200 || released.status >= 300) throw new Error(`the gateway's release before the saves answered ${released.status}: ${released.said}`);
  await gone(bound.name);
  // Its first exec alone, as the probe's, without the verify, which reads every page.
  const asked = Date.now();
  // A restore on the node, after which the gateway makes its standby: the run's own restores wait for that one (standbyFor).
  restores.lastAt = asked;
  await driver(workspace, "first", "--session", sessionId, "--fixture", fixture);
  const firstMs = Date.now() - asked;
  await sleep(WORKING_SET_HELD_MS);
  const restored = await boundPodOf(workspace);
  if (!restored) throw new Error(`no pod is bound to ${workspace} after its restore before the saves`);
  const lines = await bootLog(restored.node, await sandboxIdOf(restored.node, restored.name));
  return { release_ms: released.ms, first_exec_ms: firstMs, working_set: lines.filter((line) => /Working set: |working set holds|recording the working set|working sets/u.test(line)) };
}

/** How often the `trace` variant reads a restore's pod. */
const POD_WATCH_MS = 50;

/**
 * The `trace` variant's watch of the restore of `workspace`, from now until stopped: its pod read
 * every POD_WATCH_MS, each change of its phase, conditions and containers' states said with the
 * wall clock's millisecond it was seen at; and, once stopped with the pod, what kubelet logged of it
 * in k3s's journal meanwhile: the steps between the pod's creation and runsc's start, which no span
 * covers and the pod's own timestamps say to the second alone.
 */
function podTimeline(workspace: string): { readonly stop: (pod: V1Pod) => Promise<Record<string, unknown>> } {
  const started = Date.now();
  const seen: { readonly at_ms: number; readonly pod: string; readonly state: string }[] = [];
  const bound = `${BINDING_LABEL}=${keyHash(sessionKey(workspace))}`;
  const standbys = STANDBY_LABEL;
  let watching = true;
  const watch = (async () => {
    const last = new Map<string, string>();
    while (watching) {
      // Its restore's pod, once bound to it, and the node's standbys, one of which its restore may claim.
      const pods = (await Promise.all([bound, standbys].map((labelSelector) => kube.list<V1Pod>("Pod", { namespace: SESSIONS, labelSelector }).catch(() => [] as V1Pod[])))).flat();
      for (const pod of pods) {
        const state = JSON.stringify({
          phase: pod.status?.phase,
          conditions: pod.status?.conditions?.map(({ type, status }) => `${type}=${status}`),
          containers: pod.status?.containerStatuses?.map(({ name, state: each }) => `${name}=${Object.keys(each ?? {}).join(",")}`),
        });
        const name = pod.metadata?.name ?? "";
        if (last.get(name) !== state) seen.push({ at_ms: Date.now() - started, pod: name, state });
        last.set(name, state);
      }
      await sleep(POD_WATCH_MS);
    }
  })();
  return {
    stop: async (pod) => {
      watching = false;
      await watch;
      const name = pod.metadata?.name ?? "";
      const since = new Date(started - 1000).toISOString().replace("T", " ").slice(0, 19);
      const kubelet = (await onNode("", `journalctl -u k3s --since '${since}' --utc --no-pager -o short-iso-precise | grep -F '${name}' | tail -n 200 || true`).catch(() => "")).trim().split("\n").filter(Boolean);
      return { started_wall_ms: started, seen, kubelet, pod_uid: pod.metadata?.uid ?? null, kept: (await kube.get(ref("Pod", name, SESSIONS))) !== null };
    },
  };
}
