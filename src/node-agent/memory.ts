/**
 * What the node's memory holds, as its node agent reads it: each gVisor sandbox's, by the state
 * of its pod (`bound` to a session or harness's binding, its workspace's token written beside it;
 * a `standby`, which a restore may claim, found by its shim's socket, as runsc lists none of a
 * standby's containers, created and none started; or `free`, a pool's pod waiting for a binding),
 * as its pod's cgroup counts it (memory.current, and of it
 * its anonymous and file-backed memory and what zswap holds compressed, memory.stat and
 * memory.zswap.current), and of it what its MemoryFile holds, the memfd its Sentry keeps all of
 * its application's memory in, its page cache and its tmpfs with it (memory.stat's shmem), the
 * rest being the sandbox's own: its Sentry's, its gofer's and its shims' (overhead). So counted,
 * its application's memory is the Sentry's own accounting of it, which no process's RSS in the
 * sandbox is, as each counts the pages it shares once more. Beside them the node's memory, as its
 * /proc/meminfo says, and the agent's own, its cgroup's, split as a sandbox's is: its anonymous
 * memory, which only the agent's own work holds, and its page cache, which the kernel takes back
 * as the agent nears its limit; and its cgroup's events.
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
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

import type { BatchObservableResult } from "@opentelemetry/api";
import { Effect, type Scope } from "effect";

import { STANDBY_ANNOTATION, STANDBY_SOCKETS } from "../kube/sandbox-shape.ts";
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
  /** Of the overhead, its gofers' own (goferOf); null where none was found or read. */
  readonly goferBytes: number | null;
}

/** How often the kernel has said the agent's cgroup reached its memory.high, its memory.max, ran out of memory, and killed in it so (memory.events). */
export interface MemoryEvents {
  readonly high: number;
  readonly max: number;
  readonly oom: number;
  readonly oomKill: number;
}

/** The node's memory and its sandboxes', in bytes, as the module says. */
export interface NodeMemory {
  readonly totalBytes: number;
  readonly availableBytes: number;
  /**
   * The agent's own, as its cgroup counts it: all of it; its anonymous memory, which the kernel
   * reclaims none of without swap; its page cache, mostly its store proxy's reads, of it what is
   * active and inactive, and dirty; and its cgroup's events. Null where they cannot be read.
   */
  readonly agentBytes: number | null;
  readonly agentAnonBytes: number | null;
  readonly agentFileBytes: number | null;
  readonly agentActiveFileBytes: number | null;
  readonly agentInactiveFileBytes: number | null;
  readonly agentDirtyBytes: number | null;
  readonly agentEvents: MemoryEvents | null;
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

/** The agent's own memory, as NodeMemory says it. */
export type OwnMemory = { readonly [K in "agentBytes" | "agentAnonBytes" | "agentFileBytes" | "agentActiveFileBytes" | "agentInactiveFileBytes" | "agentDirtyBytes" | "agentEvents"]: NodeMemory[K] };

/**
 * The memory of the agent's own cgroup, as `proc` (its /proc/self/cgroup, `0::<path>`) names it
 * under the cgroups' root `cgroups`: of the path as it is, where the agent shares the node's
 * cgroup namespace, or of the root itself, where its own namespace's root is its cgroup. None
 * where neither is read.
 */
export async function ownMemory(proc: string, cgroups: string): Promise<OwnMemory> {
  const path = /^0::(\/.*)$/mu.exec((await text(proc)) ?? "")?.[1];
  const own = path !== undefined && (await numberIn(join(cgroups, path, "memory.current"))) !== null ? join(cgroups, path) : cgroups;
  const bytes = await numberIn(join(own, "memory.current"));
  const [stat, events] = bytes === null ? [null, null] : await Promise.all([text(join(own, "memory.stat")), text(join(own, "memory.events"))]);
  const event = (key: string) => statOf(events, key) ?? 0;
  return {
    agentBytes: bytes,
    agentAnonBytes: statOf(stat, "anon"),
    agentFileBytes: statOf(stat, "file"),
    agentActiveFileBytes: statOf(stat, "active_file"),
    agentInactiveFileBytes: statOf(stat, "inactive_file"),
    agentDirtyBytes: statOf(stat, "file_dirty"),
    agentEvents: events === null ? null : { high: event("high"), max: event("max"), oom: event("oom"), oomKill: event("oom_kill") },
  };
}

/** What runsc names a gofer's process, its argv[0], beside its sandbox's runsc-sandbox. */
const GOFER = "runsc-gofer";

/**
 * What the gofers of the sandbox whose pod's cgroup is `cgroup` hold, a gofer to each of its
 * containers: their processes, among those of the cgroup and of its children (its pod sandbox's
 * scope), named so, as the node's processes say under `proc`; of each, its proportional set size
 * (smaps_rollup's Pss), so the runsc binary it maps with its sandbox is counted once between them.
 * Null where none is found or read.
 */
export async function goferOf(cgroup: string, proc: string): Promise<number | null> {
  const children = (await readdir(cgroup, { withFileTypes: true }).catch(() => [])).filter((entry) => entry.isDirectory()).map((entry) => join(cgroup, entry.name));
  let held: number | null = null;
  for (const dir of [cgroup, ...children]) {
    for (const pid of ((await text(join(dir, "cgroup.procs"))) ?? "").split("\n").filter((each) => /^[0-9]+$/u.test(each))) {
      if ((await text(join(proc, pid, "cmdline")))?.split("\0")[0] !== GOFER) continue;
      const pss = /^Pss:\s+([0-9]+) kB$/mu.exec((await text(join(proc, pid, "smaps_rollup"))) ?? "")?.[1];
      if (pss !== undefined) held = (held ?? 0) + Number(pss) * 1024;
    }
  }
  return held;
}

/** Where kubelet keeps each pod's logs on its node, in a directory named `<namespace>_<pod>_<uid>`. */
const POD_LOGS = join("var", "log", "pods");

/**
 * The node's standbys, under its root `nodeRoot`: each by its shim's socket in STANDBY_SOCKETS,
 * `<namespace>.<pod>.sock` (standbySocket), not its containers' beside it; its pod's uid by the
 * directory kubelet keeps its logs in; of those whose pod's cgroup under `cgroups` is there, as a
 * socket a standby left as it went may outlive it.
 */
export async function standbysOf(nodeRoot: string, cgroups: string): Promise<readonly { readonly uid: string; readonly namespace: string; readonly pod: string }[]> {
  const sockets = await readdir(join(nodeRoot, STANDBY_SOCKETS)).catch(() => [] as string[]);
  const logs = await readdir(join(nodeRoot, POD_LOGS)).catch(() => [] as string[]);
  const found = [];
  for (const socket of sockets) {
    const named = /^([a-z0-9-]+)\.([a-z0-9-]+)\.sock$/u.exec(socket);
    if (!named) continue;
    const [, namespace = "", pod = ""] = named;
    const uid = logs.find((dir) => dir.startsWith(`${namespace}_${pod}_`))?.slice(`${namespace}_${pod}_`.length);
    if (uid !== undefined && (await podCgroup(cgroups, uid)) !== null) found.push({ uid, namespace, pod });
  }
  return found;
}

/** A sandbox's memory, as the module says, with its pod's cgroup, under the node's cgroup root as the agent sees it; null where it was not found. */
export interface SandboxCgroup extends SandboxMemory {
  readonly cgroup: string | null;
}

/**
 * The node's sandboxes' memory, and their pods' cgroups, as the module says; of each only what its
 * cgroup holds where `split` is false, its memory.stat not read, as each read of it has the kernel
 * gather its counts from every processor.
 */
export const sandboxesOf = ({ runsc, nodeRoot, attachRoot }: Pick<MemoryOptions, "runsc" | "nodeRoot" | "attachRoot">, split = true): Effect.Effect<readonly SandboxCgroup[]> =>
  Effect.gen(function*() {
    const cgroups = join(nodeRoot, "sys", "fs", "cgroup");
    const listed = yield* runsc.containers.pipe(
      Effect.catch((error) => Effect.as(Effect.logWarning(`the node's sandboxes' memory could not be read, as runsc could not list them: ${error.message}`), [] as readonly ListedContainer[])),
    );
    // Each pod's containers, by its uid, of the pods some container of which runs.
    const pods = new Map<string, ListedContainer[]>();
    for (const container of listed) {
      const uid = container.annotations[POD_UID];
      if (uid !== undefined) pods.set(uid, [...(pods.get(uid) ?? []), container]);
    }
    const running = yield* Effect.forEach([...pods].filter(([, containers]) => containers.some(({ status }) => status === "running")), ([uid, containers]) =>
      Effect.gen(function*() {
        const [first] = containers;
        const namespace = first?.annotations[POD_NAMESPACE] ?? "";
        const pod = first?.annotations[POD_NAME] ?? "";
        const bound = (yield* Effect.promise(() => text(join(attachRoot, namespace, pod, "token")))) !== null;
        const state: SandboxState = bound ? "bound" : containers.some(({ annotations }) => annotations[STANDBY_ANNOTATION] !== undefined) ? "standby" : "free";
        return { uid, namespace, pod, state };
      }));
    // And the standbys, which runsc lists none of, their shims having booted their sandboxes with none of their containers
    // started: each by its shim's socket, of those whose pod's cgroup is there.
    const standbys = yield* Effect.forEach((yield* Effect.promise(() => standbysOf(nodeRoot, cgroups))).filter(({ uid }) => !pods.has(uid)), (standby) =>
      Effect.map(Effect.promise(() => text(join(attachRoot, standby.namespace, standby.pod, "token"))), (token): typeof running[number] => ({ ...standby, state: token === null ? "standby" : "bound" })));
    const sandboxes = yield* Effect.forEach([...running, ...standbys], ({ uid, namespace, pod, state }) =>
      Effect.gen(function*() {
        const cgroup = yield* Effect.promise(() => podCgroup(cgroups, uid));
        const stat = cgroup === null || !split ? null : yield* Effect.promise(() => text(join(cgroup, "memory.stat")));
        return {
          namespace,
          pod,
          state,
          bytes: cgroup === null ? null : yield* Effect.promise(() => numberIn(join(cgroup, "memory.current"))),
          anonBytes: statOf(stat, "anon"),
          fileBytes: statOf(stat, "file"),
          shmemBytes: statOf(stat, "shmem"),
          // A kernel without zswap's accounting counts none.
          zswapBytes: cgroup === null || !split ? null : (yield* Effect.promise(() => numberIn(join(cgroup, "memory.zswap.current")))) ?? 0,
          goferBytes: cgroup === null || !split ? null : yield* Effect.promise(() => goferOf(cgroup, join(nodeRoot, "proc"))),
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
      ...(yield* Effect.promise(() => ownMemory(ownProc, ownCgroups))),
      budgetBytes: Math.max(0, Math.floor((totalBytes - NODE_MEMORY_RESERVED_BYTES - NODE_MEMORY_SOFT_EVICTION_BYTES - sum("bound")) / 2)),
      budgetedBytes: sum(["standby", "free"]),
      sandboxes,
    };
  });

const sandboxMemory = meter.createObservableGauge("alasio.node_agent.sandboxes.memory", {
  description: "The memory the node's gVisor sandboxes hold, by their pods' state (bound, standby, free) and part: as their pods' cgroups count it (cgroup); of it anonymous (anon) and file-backed (file); their MemoryFiles', their applications' memory with its page cache and tmpfs (memoryfile), and the rest, their Sentries', gofers' and shims' (overhead), of it their gofers' own, as their proportional set size (gofer); and zswap's (zswap)",
  unit: "By",
});
const sandboxCount = meter.createObservableGauge("alasio.node_agent.sandboxes", {
  description: "The node's gVisor sandboxes that run, by their pods' state (bound, standby, free)",
  unit: "{sandbox}",
});
const nodeTotal = meter.createObservableGauge("alasio.node.memory.total", { description: "The node's memory, as its /proc/meminfo says (MemTotal)", unit: "By" });
const nodeAvailable = meter.createObservableGauge("alasio.node.memory.available", { description: "The node's memory available, as its /proc/meminfo says (MemAvailable)", unit: "By" });
const agentMemory = meter.createObservableGauge("alasio.node_agent.memory", {
  description: "The node agent's own memory, as its cgroup counts it, by part: all of it (cgroup); of it anonymous (anon), which the kernel reclaims none of without swap, and file-backed (file), its page cache, which the kernel reclaims as the agent nears its limit",
  unit: "By",
});
const agentEvents = meter.createObservableCounter("alasio.node_agent.memory.events", {
  description: "How often the kernel has said the node agent's cgroup reached its memory.high (high) or its limit (max), ran out of memory (oom), and killed in it so (oom_kill), as its memory.events says",
  unit: "{event}",
});
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
    const gauges = [sandboxMemory, sandboxCount, nodeTotal, nodeAvailable, agentMemory, agentEvents, budget, budgeted];
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
                gofer: total(({ goferBytes }) => goferBytes),
              };
              for (const [part, bytes] of Object.entries(parts)) result.observe(sandboxMemory, bytes, { "alasio.sandbox.state": state, "alasio.memory.part": part });
            }
            result.observe(nodeTotal, memory.totalBytes);
            result.observe(nodeAvailable, memory.availableBytes);
            const own: Readonly<Record<string, number | null>> = { cgroup: memory.agentBytes, anon: memory.agentAnonBytes, file: memory.agentFileBytes };
            for (const [part, bytes] of Object.entries(own)) if (bytes !== null) result.observe(agentMemory, bytes, { "alasio.memory.part": part });
            const events: Readonly<Record<string, number>> = memory.agentEvents === null ? {} : { high: memory.agentEvents.high, max: memory.agentEvents.max, oom: memory.agentEvents.oom, oom_kill: memory.agentEvents.oomKill };
            for (const [event, count] of Object.entries(events)) result.observe(agentEvents, count, { "alasio.memory.event": event });
            result.observe(budget, memory.budgetBytes);
            result.observe(budgeted, memory.budgetedBytes);
          }));
        meter.addBatchObservableCallback(observe, gauges);
        return observe;
      }),
      (observe) => Effect.sync(() => meter.removeBatchObservableCallback(observe, gauges)),
    );
  });
