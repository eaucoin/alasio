/**
 * The node agent's guard of its node's memory: what a sandbox that grows past what the node has
 * ends, and that it ends that sandbox alone, whole, never another session's nor the platform's
 * pods. The kernel's OOM killer cannot be left to choose: it takes the process of the greatest
 * RSS, and a gVisor sandbox's memory is the MemoryFile its Sentry and stubs map, charged to its
 * pod's cgroup but counted in no process's RSS, so an OOM above the node's pods takes the object
 * store's or the stack's processes first, as two sandboxes grown at once past the node measured.
 *
 * So, every GUARD_MS, the agent reads what each sandbox's pod holds (./memory.ts sandboxesOf) and
 * what the node has left for them: the least of what /proc/meminfo says is available over
 * kubelet's soft eviction threshold, and what each cgroup above the pods (kubepods' and their
 * QoS classes') has under its memory.max. Each bound sandbox's cgroup, the one its processes are
 * in (its pod sandbox's scope), it limits to what it holds and its share of that room, an equal
 * share of each bound one's, SANDBOX_MEMORY_SLACK_BYTES at the least, and gives memory.oom.group,
 * so a sandbox that grows past its share is ended by its own cgroup's OOM, whole, as the room it
 * grows into is what the others are kept from. A sandbox not bound, a pool's free pod or a
 * standby, holds little and is left as kubelet makes it.
 *
 * Where the node nonetheless comes near its end, what is available over kubelet's hard eviction
 * threshold or under a cgroup above the pods less than SANDBOX_MEMORY_SLACK_BYTES (the shares'
 * least overlapping, as two sandboxes grow at once), the agent ends the sandbox of its node's
 * pods that holds the most, killing its pod's processes at once (cgroup.kill), before the
 * kernel must choose; the kernel's OOM killer is left for what comes faster than a look. A
 * sandbox ended so, or by its own cgroup's OOM, is one whose bayma ended: its pod's gateway
 * releases its binding as a broken sandbox's, and its session comes back from its last save.
 */
import { readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { Effect, Exit, Schedule, type Scope } from "effect";

import { meter } from "../telemetry/index.ts";
import { meminfoOf, type MemoryOptions, numberIn, type SandboxCgroup, type SandboxState, sandboxesOf } from "./memory.ts";
import { NODE_MEMORY_HARD_EVICTION_BYTES, NODE_MEMORY_SOFT_EVICTION_BYTES } from "./names.ts";

/** How often the guard looks at the node's memory. */
export const GUARD_MS = 250;

/** How long the guard takes runsc's list of the node's sandboxes for theirs, a pod made since waiting as long to be guarded. */
const LIST_MS = 2_000;

/**
 * The least a bound sandbox may grow by past what it holds, whatever is left: room for a turn's
 * allocations between two looks, and for a sandbox that writes and frees as it runs; and what
 * the node keeps left before the guard ends its largest sandbox.
 */
export const SANDBOX_MEMORY_SLACK_BYTES = 256 * 1024 ** 2;

/** By how much a sandbox's limit must move before it is written again. */
const LIMIT_STEP_BYTES = 16 * 1024 ** 2;

/** A sandbox as the guard plans for it: its key, its pod's state, what its pod and its own cgroup hold. */
export interface Guarded {
  readonly key: string;
  readonly state: SandboxState;
  readonly bytes: number;
  readonly scopeBytes: number;
}

/** What the guard does at a look: the limit of each bound sandbox's cgroup, by its key; and the sandbox it ends, if any. */
export interface GuardPlan {
  readonly limits: ReadonlyMap<string, number>;
  readonly end: string | null;
}

/**
 * What the guard does, as the module says, given `room`, what the node has left for its
 * sandboxes before kubelet evicts or a cgroup above them reaches its limit; `lastRoom`, what it
 * has left before kubelet evicts at once or that limit; `sandboxes`; and those it ended already,
 * while one of which is there it ends none.
 */
export function guardPlan(room: number, lastRoom: number, sandboxes: readonly Guarded[], ended: ReadonlySet<string>): GuardPlan {
  const bound = sandboxes.filter(({ state }) => state === "bound");
  const share = Math.max(SANDBOX_MEMORY_SLACK_BYTES, Math.floor(Math.max(0, room) / Math.max(1, bound.length)));
  const limits = new Map(bound.map(({ key, scopeBytes }) => [key, scopeBytes + share]));
  // None while one ended is still there, its memory being freed (some 0.5 s for 2.5 GiB), which would end another for nothing.
  if (lastRoom >= SANDBOX_MEMORY_SLACK_BYTES || sandboxes.some(({ key }) => ended.has(key))) return { limits, end: null };
  return { limits, end: sandboxes.toSorted((a, b) => b.bytes - a.bytes)[0]?.key ?? null };
}

/** A file's text, or null where it cannot be read. */
const text = (path: string): Promise<string | null> => readFile(path, "utf8").catch(() => null);

/** Writes `value` into the cgroup file `path`: whether it could. */
const write = (path: string, value: string): Promise<boolean> => writeFile(path, value).then(() => true, () => false);

/**
 * The cgroup a pod's sandbox's processes are in, under its pod's cgroup `pod`: the one cgroup of
 * the pod's with processes, its pod sandbox's scope; the pod's own where it holds them, or where
 * none does.
 */
export async function sandboxCgroup(pod: string): Promise<string> {
  const held = async (dir: string) => ((await text(join(dir, "cgroup.procs"))) ?? "").trim() !== "";
  if (await held(pod)) return pod;
  const children = (await readdir(pod, { withFileTypes: true }).catch(() => [])).filter((entry) => entry.isDirectory()).map((entry) => join(pod, entry.name));
  const holding = [];
  for (const child of children) if (await held(child)) holding.push(child);
  return holding.length === 1 ? (holding[0] ?? pod) : pod;
}

/** What each cgroup above the pods `pods`, under the root `cgroups`, has left under its memory.max: the least, or Infinity where none has one. */
export async function aboveRoom(cgroups: string, pods: readonly string[]): Promise<number> {
  const above = new Set<string>();
  for (const pod of pods) for (let dir = dirname(pod); dir.startsWith(`${cgroups}/`); dir = dirname(dir)) above.add(dir);
  let room = Number.POSITIVE_INFINITY;
  for (const dir of above) {
    const max = await numberIn(join(dir, "memory.max"));
    const current = await numberIn(join(dir, "memory.current"));
    if (max !== null && current !== null) room = Math.min(room, max - current);
  }
  return room;
}

const ended = meter.createCounter("alasio.node_agent.sandboxes.ended", {
  description: "The sandboxes the node agent ended, the largest on its node, as its node's memory came near its end",
  unit: "{sandbox}",
});

/** The key of a sandbox: its pod's namespace and name. */
const keyOf = ({ namespace, pod }: Pick<SandboxCgroup, "namespace" | "pod">): string => `${namespace}/${pod}`;

/** How many MiB `bytes` is, for a log. */
const mib = (bytes: number): string => `${Math.round(bytes / 1024 ** 2)} MiB`;

/** What guardNodeMemory looks at the node with, and how often. */
export interface GuardOptions extends Pick<MemoryOptions, "runsc" | "nodeRoot" | "attachRoot"> {
  readonly everyMs?: number;
}

/** One look of the guard's, as the module says; the sandboxes it ended so far, by their pods' cgroups, kept and forgotten as they go. */
export const guardOnce = ({ runsc, nodeRoot, attachRoot }: GuardOptions, endedBefore: Set<string>): Effect.Effect<void> =>
  Effect.gen(function*() {
    const cgroups = join(nodeRoot, "sys", "fs", "cgroup");
    const sandboxes = (yield* sandboxesOf({ runsc, nodeRoot, attachRoot })).filter((sandbox): sandbox is SandboxCgroup & { readonly cgroup: string } => sandbox.cgroup !== null);
    // Those ended that are gone are forgotten, as a pod's cgroup goes with it.
    for (const cgroup of endedBefore) if (!sandboxes.some((sandbox) => sandbox.cgroup === cgroup)) endedBefore.delete(cgroup);
    const available = meminfoOf(yield* Effect.promise(() => text(join(nodeRoot, "proc", "meminfo"))), "MemAvailable");
    const above = yield* Effect.promise(() => aboveRoom(cgroups, sandboxes.map(({ cgroup }) => cgroup)));
    const room = Math.min(available === null ? Number.POSITIVE_INFINITY : available - NODE_MEMORY_SOFT_EVICTION_BYTES, above);
    const lastRoom = Math.min(available === null ? Number.POSITIVE_INFINITY : available - NODE_MEMORY_HARD_EVICTION_BYTES, above);
    const scopes = new Map<string, string>();
    const guarded: Guarded[] = [];
    for (const sandbox of sandboxes) {
      const scope = yield* Effect.promise(() => sandboxCgroup(sandbox.cgroup));
      scopes.set(keyOf(sandbox), scope);
      const scopeBytes = (yield* Effect.promise(() => numberIn(join(scope, "memory.current")))) ?? sandbox.bytes ?? 0;
      guarded.push({ key: keyOf(sandbox), state: sandbox.state, bytes: sandbox.bytes ?? 0, scopeBytes });
    }
    const endedKeys = new Set(sandboxes.filter(({ cgroup }) => endedBefore.has(cgroup)).map(keyOf));
    const plan = guardPlan(room, lastRoom, guarded, endedKeys);
    for (const [key, limit] of plan.limits) {
      const scope = scopes.get(key);
      if (scope === undefined) continue;
      const held = yield* Effect.promise(() => numberIn(join(scope, "memory.max")));
      if (held === null || Math.abs(held - limit) >= LIMIT_STEP_BYTES) yield* Effect.promise(() => write(join(scope, "memory.max"), `${limit}\n`));
      // Set again as systemd may set it back, as it sets its units' cgroups' attributes.
      if ((yield* Effect.promise(() => text(join(scope, "memory.oom.group"))))?.trim() !== "1") yield* Effect.promise(() => write(join(scope, "memory.oom.group"), "1\n"));
    }
    const end = sandboxes.find((sandbox) => keyOf(sandbox) === plan.end);
    if (end === undefined) return;
    endedBefore.add(end.cgroup);
    if (yield* Effect.promise(() => write(join(end.cgroup, "cgroup.kill"), "1\n"))) {
      ended.add(1, { "alasio.sandbox.state": end.state });
      yield* Effect.logWarning(
        `ended the sandbox of ${keyOf(end)} (${end.state}), the largest on its node at ${mib(end.bytes ?? 0)}, as its node had ${mib(Math.max(0, lastRoom))} left before kubelet evicts at once or its pods reach their limit`,
      );
    } else {
      yield* Effect.logWarning(`could not end the sandbox of ${keyOf(end)}, the largest on its node, as its pod's cgroup takes no kill`);
    }
  });

/**
 * Guards the node's memory, as the module says, every `everyMs` (GUARD_MS), for as long as the
 * scope is open; its sandboxes listed by runsc every LIST_MS at most, their pods' cgroups read at
 * every look.
 */
export const guardNodeMemory = (options: GuardOptions): Effect.Effect<void, never, Scope.Scope> =>
  Effect.gen(function*() {
    const containers = yield* Effect.cachedWithTTL(options.runsc.containers, (exit) => (Exit.isSuccess(exit) ? LIST_MS : 0));
    const endedBefore = new Set<string>();
    yield* guardOnce({ ...options, runsc: { containers } }, endedBefore).pipe(
      Effect.catchDefect((defect) => Effect.logWarning(`the node's memory could not be guarded: ${String(defect)}`)),
      Effect.repeat(Schedule.spaced(options.everyMs ?? GUARD_MS)),
      Effect.forkScoped,
    );
  });
