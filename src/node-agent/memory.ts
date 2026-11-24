/**
 * What the node's memory holds, as its node agent reads it: each gVisor sandbox's, by the state
 * of its pod (`bound` to a session or harness's binding, its workspace's token written beside it;
 * a `standby`, which a restore may claim, its runtime told so by its pod's annotation; or `free`,
 * a pool's pod waiting for a binding), as its pod's cgroup counts it (memory.current, and of it
 * its anonymous and file-backed memory and what zswap holds compressed, memory.stat and
 * memory.zswap.current), and of it what its MemoryFile holds, the memfd its Sentry keeps all of
 * its application's memory in, its page cache and its tmpfs with it (memory.stat's shmem), the
 * rest being the sandbox's own: its Sentry's, its gofer's and its shims' (overhead). So counted,
 * its application's memory is the Sentry's own accounting of it, which no process's RSS in the
 * sandbox is, as each counts the pages it shares once more. Beside them the node's memory, as its
 * /proc/meminfo says, and the agent's own, its cgroup's.
 *
 * And the budget of what is held ahead of a session's need, standbys and free pods now (frozen
 * and reclaimed sessions as alasio's tiers come): half of what the node's memory leaves once
 * kubelet's reservations (NODE_MEMORY_RESERVED_BYTES), its soft eviction threshold
 * (NODE_MEMORY_SOFT_EVICTION_BYTES) and the bound sandboxes are counted, so that what is held
 * ahead is given up before kubelet evicts what runs, with as much again to spare.
 *
 * All of it as the agent's gauges (observeNodeMemory), read as metrics are exported, and as
 * nodeMemory answers it, which the agent serves (`GET /memory`). A pod whose cgroup cannot be
 * found is counted for what is known of it.
 */
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import type { BatchObservableResult } from "@opentelemetry/api";
import { Effect, type Scope } from "effect";

import { STANDBY_ANNOTATION } from "../kube/sandbox-shape.ts";
import { meter } from "../telemetry/index.ts";
import { NODE_MEMORY_RESERVED_BYTES, NODE_MEMORY_SOFT_EVICTION_BYTES } from "./names.ts";
import type { ListedContainer, Runsc } from "./runsc.ts";

/** The state of a sandbox's pod, as the module says. */
export type SandboxState = "bound" | "standby" | "free";
const STATES: readonly SandboxState[] = ["bound", "standby", "free"];

/** What a sandbox holds of the node's memory, in bytes, as the module says. */
export interface SandboxMemory {
  readonly namespace: string;
  readonly pod: string;
  readonly state: SandboxState;
  /**
   * Its pod's cgroup's: all of it; its anonymous and file-backed memory; of the file-backed, its
   * MemoryFile's (shmem); and what zswap holds of it. Null where its cgroup was not found.
   */
  readonly bytes: number | null;
  readonly anonBytes: number | null;
  readonly fileBytes: number | null;
  readonly shmemBytes: number | null;
  readonly zswapBytes: number | null;
}

/** The node's memory and its sandboxes', in bytes, as the module says. */
export interface NodeMemory {
  readonly totalBytes: number;
  readonly availableBytes: number;
  /** The agent's own, as its cgroup counts it; null where it cannot be read. */
  readonly agentBytes: number | null;
  /** What may be held ahead of a session's need, and what is. */
  readonly budgetBytes: number;
  readonly budgetedBytes: number;
  readonly sandboxes: readonly SandboxMemory[];
}

/** What nodeMemory reads the node with. */
export interface MemoryOptions {
  readonly runsc: Pick<Runsc, "containers">;
  /** Where the node's filesystem is, as the agent sees it: its cgroups under sys/fs/cgroup, its /proc/meminfo. */
  readonly nodeRoot: string;
  /** Where its pods' directories are (./workspaces.ts), each bound one's with its binding's token. */
  readonly attachRoot: string;
  /** Where the agent's process says its cgroup is (its /proc/self/cgroup), and where it sees the cgroups' root. */
  readonly ownProc?: string;
  readonly ownCgroups?: string;
}

/** Where a process says its cgroup is. */
const OWN_PROC = "/proc/self/cgroup";
/** Where a container sees the cgroups' root (cgroup v2). */
const OWN_CGROUPS = "/sys/fs/cgroup";

const POD_NAMESPACE = "io.kubernetes.cri.sandbox-namespace";
const POD_NAME = "io.kubernetes.cri.sandbox-name";
const POD_UID = "io.kubernetes.cri.sandbox-uid";

/** A file's text, or null where it cannot be read. */
const text = (path: string): Promise<string | null> => readFile(path, "utf8").catch(() => null);

/** A cgroup's file of one number, or null. */
export const numberIn = async (path: string): Promise<number | null> => {
  const read = (await text(path))?.trim();
  return read !== undefined && /^[0-9]+$/u.test(read) ? Number(read) : null;
};

/** The value of `key` in a file of `key value` lines (memory.stat), or null. */
export function statOf(stat: string | null, key: string): number | null {
  const line = stat?.split("\n").find((each) => each.startsWith(`${key} `));
  const value = line?.slice(key.length + 1).trim();
  return value !== undefined && /^[0-9]+$/u.test(value) ? Number(value) : null;
}

/** The value, in bytes, of `key` of /proc/meminfo's text (`MemAvailable:  123 kB`), or null. */
export function meminfoOf(meminfo: string | null, key: string): number | null {
  const value = new RegExp(`^${key}:\\s+([0-9]+) kB$`, "mu").exec(meminfo ?? "")?.[1];
  return value === undefined ? null : Number(value) * 1024;
}

/**
 * The cgroup of the pod `uid` under the node's cgroup root `cgroups`, as kubelet makes it: under
 * systemd's driver, `kubepods.slice/kubepods[-<qos>].slice/kubepods[-<qos>]-pod<uid, its dashes
 * underscores>.slice`; under cgroupfs's, `kubepods[/<qos>]/pod<uid>`. Null where none is there.
 */
export async function podCgroup(cgroups: string, uid: string): Promise<string | null> {
  const underscored = uid.replaceAll("-", "_");
  const candidates = [
    join(cgroups, "kubepods.slice", `kubepods-pod${underscored}.slice`),
    ...["burstable", "besteffort"].map((qos) => join(cgroups, "kubepods.slice", `kubepods-${qos}.slice`, `kubepods-${qos}-pod${underscored}.slice`)),
    join(cgroups, "kubepods", `pod${uid}`),
    ...["burstable", "besteffort"].map((qos) => join(cgroups, "kubepods", qos, `pod${uid}`)),
  ];
  for (const candidate of candidates) if ((await numberIn(join(candidate, "memory.current"))) !== null) return candidate;
  return null;
}

/**
 * The memory of the agent's own cgroup, as `proc` (its /proc/self/cgroup, `0::<path>`) names it
 * under the cgroups' root `cgroups`: of the path as it is, where the agent shares the node's
 * cgroup namespace, or of the root itself, where its own namespace's root is its cgroup. Null
 * where neither is read.
 */
export async function ownMemory(proc: string, cgroups: string): Promise<number | null> {
  const path = /^0::(\/.*)$/mu.exec((await text(proc)) ?? "")?.[1];
  return (path === undefined ? null : await numberIn(join(cgroups, path, "memory.current"))) ?? (await numberIn(join(cgroups, "memory.current")));
}

/** A sandbox's memory, as the module says, with its pod's cgroup, under the node's cgroup root as the agent sees it; null where it was not found. */
export interface SandboxCgroup extends SandboxMemory {
  readonly cgroup: string | null;
}

/** The node's sandboxes' memory, and their pods' cgroups, as the module says. */
export const sandboxesOf = ({ runsc, nodeRoot, attachRoot }: Pick<MemoryOptions, "runsc" | "nodeRoot" | "attachRoot">): Effect.Effect<readonly SandboxCgroup[]> =>
  Effect.gen(function*() {
    const cgroups = join(nodeRoot, "sys", "fs", "cgroup");
    const listed = yield* runsc.containers.pipe(
      Effect.catch((error) => Effect.as(Effect.logWarning(`the node's sandboxes' memory could not be read, as runsc could not list them: ${error.message}`), [] as readonly ListedContainer[])),
    );
    // Each pod's containers, by its uid, of the pods some container of which runs, as a standby's root does while its others
    // wait for their restore.
    const pods = new Map<string, ListedContainer[]>();
    for (const container of listed) {
      const uid = container.annotations[POD_UID];
      if (uid !== undefined) pods.set(uid, [...(pods.get(uid) ?? []), container]);
    }
    const running = [...pods].filter(([, containers]) => containers.some(({ status }) => status === "running"));
    const sandboxes = yield* Effect.forEach(running, ([uid, containers]) =>
      Effect.gen(function*() {
        const [first] = containers;
        const namespace = first?.annotations[POD_NAMESPACE] ?? "";
        const pod = first?.annotations[POD_NAME] ?? "";
        const bound = (yield* Effect.promise(() => text(join(attachRoot, namespace, pod, "token")))) !== null;
        const state: SandboxState = bound ? "bound" : containers.some(({ annotations }) => annotations[STANDBY_ANNOTATION] !== undefined) ? "standby" : "free";
        const cgroup = yield* Effect.promise(() => podCgroup(cgroups, uid));
        const stat = cgroup === null ? null : yield* Effect.promise(() => text(join(cgroup, "memory.stat")));
        return {
          namespace,
          pod,
          state,
          bytes: cgroup === null ? null : yield* Effect.promise(() => numberIn(join(cgroup, "memory.current"))),
          anonBytes: statOf(stat, "anon"),
          fileBytes: statOf(stat, "file"),
          shmemBytes: statOf(stat, "shmem"),
          // A kernel without zswap's accounting counts none.
          zswapBytes: cgroup === null ? null : (yield* Effect.promise(() => numberIn(join(cgroup, "memory.zswap.current")))) ?? 0,
          cgroup,
        } satisfies SandboxCgroup;
      }), { concurrency: 4 });
    return sandboxes.toSorted((a, b) => `${a.namespace}/${a.pod}`.localeCompare(`${b.namespace}/${b.pod}`));
  });

/** The node's memory and its sandboxes', as the module says. */
export const nodeMemory = ({ runsc, nodeRoot, attachRoot, ownProc = OWN_PROC, ownCgroups = OWN_CGROUPS }: MemoryOptions): Effect.Effect<NodeMemory> =>
  Effect.gen(function*() {
    const sandboxes = (yield* sandboxesOf({ runsc, nodeRoot, attachRoot })).map(({ cgroup: _cgroup, ...memory }) => memory);
    const meminfo = yield* Effect.promise(() => text(join(nodeRoot, "proc", "meminfo")));
    const totalBytes = meminfoOf(meminfo, "MemTotal") ?? 0;
    const sum = (state: SandboxState | readonly SandboxState[]) =>
      sandboxes.filter((sandbox) => (typeof state === "string" ? [state] : state).includes(sandbox.state)).reduce((held, { bytes }) => held + (bytes ?? 0), 0);
    return {
      totalBytes,
      availableBytes: meminfoOf(meminfo, "MemAvailable") ?? 0,
      agentBytes: yield* Effect.promise(() => ownMemory(ownProc, ownCgroups)),
      budgetBytes: Math.max(0, Math.floor((totalBytes - NODE_MEMORY_RESERVED_BYTES - NODE_MEMORY_SOFT_EVICTION_BYTES - sum("bound")) / 2)),
      budgetedBytes: sum(["standby", "free"]),
      sandboxes,
    };
  });

const sandboxMemory = meter.createObservableGauge("alasio.node_agent.sandboxes.memory", {
  description: "The memory the node's gVisor sandboxes hold, by their pods' state (bound, standby, free) and part: as their pods' cgroups count it (cgroup); of it anonymous (anon) and file-backed (file); their MemoryFiles', their applications' memory with its page cache and tmpfs (memoryfile), and the rest, their Sentries', gofers' and shims' (overhead); and zswap's (zswap)",
  unit: "By",
});
const sandboxCount = meter.createObservableGauge("alasio.node_agent.sandboxes", {
  description: "The node's gVisor sandboxes that run, by their pods' state (bound, standby, free)",
  unit: "{sandbox}",
});
const nodeTotal = meter.createObservableGauge("alasio.node.memory.total", { description: "The node's memory, as its /proc/meminfo says (MemTotal)", unit: "By" });
const nodeAvailable = meter.createObservableGauge("alasio.node.memory.available", { description: "The node's memory available, as its /proc/meminfo says (MemAvailable)", unit: "By" });
const agentMemory = meter.createObservableGauge("alasio.node_agent.memory", { description: "The node agent's own memory, as its cgroup counts it", unit: "By" });
const budget = meter.createObservableGauge("alasio.node_agent.memory.budget", {
  description: "What of the node's memory may be held ahead of a session's need: half of what kubelet's reservations, its soft eviction threshold and the bound sandboxes leave",
  unit: "By",
});
const budgeted = meter.createObservableGauge("alasio.node_agent.memory.budgeted", { description: "What of the node's memory is held ahead of a session's need: its standbys' and free pods' sandboxes'", unit: "By" });

/** Observes nodeMemory's as the agent's gauges, read as metrics are exported, for as long as the scope is open. */
export const observeNodeMemory = (options: MemoryOptions): Effect.Effect<void, never, Scope.Scope> =>
  Effect.gen(function*() {
    // The gauges are read as metrics are exported, outside every fiber of the agent's.
    const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
    const gauges = [sandboxMemory, sandboxCount, nodeTotal, nodeAvailable, agentMemory, budget, budgeted];
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const observe = (result: BatchObservableResult) =>
          runPromise(Effect.map(nodeMemory(options), (memory) => {
            for (const state of STATES) {
              const of = memory.sandboxes.filter((sandbox) => sandbox.state === state);
              result.observe(sandboxCount, of.length, { "alasio.sandbox.state": state });
              const total = (part: (sandbox: SandboxMemory) => number | null, among: readonly SandboxMemory[] = of) => among.reduce((sum, sandbox) => sum + (part(sandbox) ?? 0), 0);
              const split = of.filter(({ bytes, shmemBytes }) => bytes !== null && shmemBytes !== null);
              const parts: Readonly<Record<string, number>> = {
                cgroup: total(({ bytes }) => bytes),
                anon: total(({ anonBytes }) => anonBytes),
                file: total(({ fileBytes }) => fileBytes),
                memoryfile: total(({ shmemBytes }) => shmemBytes, split),
                overhead: total(({ bytes, shmemBytes }) => (bytes ?? 0) - (shmemBytes ?? 0), split),
                zswap: total(({ zswapBytes }) => zswapBytes),
              };
              for (const [part, bytes] of Object.entries(parts)) result.observe(sandboxMemory, bytes, { "alasio.sandbox.state": state, "alasio.memory.part": part });
            }
            result.observe(nodeTotal, memory.totalBytes);
            result.observe(nodeAvailable, memory.availableBytes);
            if (memory.agentBytes !== null) result.observe(agentMemory, memory.agentBytes);
            result.observe(budget, memory.budgetBytes);
            result.observe(budgeted, memory.budgetedBytes);
          }));
        meter.addBatchObservableCallback(observe, gauges);
        return observe;
      }),
      (observe) => Effect.sync(() => meter.removeBatchObservableCallback(observe, gauges)),
    );
  });
