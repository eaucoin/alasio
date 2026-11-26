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
 * QoS classes') has under its memory.max, its clean page cache counted as room, as the kernel
 * reclaims it before it must kill (aboveRoom). Of each sandbox it guards the cgroup its processes
 * are in, its pod sandbox's scope, the one cgroup of its pod's that holds any (sandboxCgroup); a
 * sandbox whose pod has none such, or more, it leaves be, saying so, and never writes to its pod's
 * own cgroup. Each bound sandbox's it gives memory.oom.group, and, where its equal share of that
 * room is short of SANDBOX_MEMORY_LIMITED_BELOW_BYTES, limits to what it holds and its headroom:
 * the room, up to SANDBOX_MEMORY_HEADROOM_BYTES, so more than its share, and never less than
 * SANDBOX_MEMORY_SLACK_BYTES; of two that grow at once past the room, the guard ends one below. A
 * sandbox that grows past its limit is ended by its own cgroup's OOM, whole. Where each one's
 * share is more, none is limited, as none grows by as much between two looks, and a limit costs
 * what it holds: the kernel bounds what a cgroup may dirty of the page cache by its limit, which
 * slowed a sandbox's stop's save of 3 GiB by some 200 ms under a limit of GiBs. A sandbox not
 * bound, a pool's free pod or a standby, holds little and is left as kubelet makes it. What it
 * last set of each, and what it ended, it keeps (GuardRecords), for the gateway to tell a session
 * that ran out of memory what it ran out of.
 *
 * Where the node nonetheless comes near its end, what is available over kubelet's hard eviction
 * threshold or under a cgroup above the pods less than SANDBOX_MEMORY_SLACK_BYTES, the agent ends
 * a sandbox only where sandboxes are the cause: the one that grew the most over the last
 * GROWTH_WINDOW_MS, by GROWN_BYTES at least, of one being restored past what its restore brings in
 * (Restores), its scope's processes killed at once (cgroup.kill); then none more for
 * END_WINDOW_MS, nor while one ended is still there, its memory being freed, so the room is
 * measured anew after each. And only where the sandboxes took the most of what the node is losing
 * now, over the last ATTRIBUTION_WINDOW_MS, the room at its most then less the room now: their
 * growth together over as long, no less than what is left of that loss, the other pods'; and none
 * while the room has stopped falling. A sandbox that grew as another pod grows more, as a
 * session's REPL loading a dataset while a pod beside it fills the node, is not the cause: ended,
 * it frees what the other takes again, and a session is lost for none of its own doing, as two
 * sandboxes beside a pod that grew by 6 GiB measured, the one that had grown ended in 17 trials of
 * 20. Where no sandbox grew so, or the other pods take the most, the pressure is another pod's (the
 * folder instances', the platform's): it ends nothing, says so once a while with what each took
 * and counts it, and leaves kubelet's eviction and the kernel to act. A sandbox ended, by the
 * agent or by its own cgroup's OOM, is one whose bayma ended: its pod's gateway releases its
 * binding as one that ran out of memory, and its session comes back from its last save.
 */
import { readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { Effect, Exit, Schedule, type Scope } from "effect";

import { meter } from "../telemetry/index.ts";
import { meminfoOf, type MemoryOptions, numberIn, type SandboxCgroup, type SandboxState, sandboxesOf, statOf } from "./memory.ts";
import { NODE_MEMORY_HARD_EVICTION_BYTES, NODE_MEMORY_SOFT_EVICTION_BYTES } from "./names.ts";

/** How often the guard looks at the node's memory. */
export const GUARD_MS = 250;

/** How long the guard takes runsc's list of the node's sandboxes for theirs, a pod made since waiting as long to be guarded. */
const LIST_MS = 2_000;

/**
 * The least a limited sandbox may grow by past what it holds, whatever is left: room for a
 * sandbox that writes and frees as it runs; and what the node keeps left before the guard ends a
 * sandbox that grew into it.
 */
export const SANDBOX_MEMORY_SLACK_BYTES = 256 * 1024 ** 2;

/**
 * The share of the room below which bound sandboxes are limited, as the module says: more than
 * any grows by between two looks.
 */
export const SANDBOX_MEMORY_LIMITED_BELOW_BYTES = 1024 ** 3;

/**
 * The headroom a limited sandbox has over what it holds while the room holds as much, whatever its
 * share: a turn's sudden allocation of 1 GiB, as a REPL loading a dataset makes, is not ended
 * while the node has room for it; the share of SANDBOX_MEMORY_LIMITED_BELOW_BYTES above which a
 * sandbox is not limited at all, so a limit never leaves one less than that share would.
 */
export const SANDBOX_MEMORY_HEADROOM_BYTES = 1024 ** 3;

/** How far back a sandbox's growth is measured, the one that grew most being the one ended. */
export const GROWTH_WINDOW_MS = 10_000;

/**
 * How far back what the node is losing, and who takes it, is measured, as the module says: 2 s,
 * 8 looks. Short, so that a fall of the room another pod caused before, past, holds back no end of
 * a sandbox growing now, while the kernel nears its OOM, which takes the platform's processes
 * first: a pod beside the sandboxes grew by 6 GiB in some 8 s, and a REPL grows at as much, 800
 * MiB a second, so either is GiBs over 2 s, far more than the 4 MiB a look of a sandbox that frees
 * as it writes, and one that filled its memory 3 s before is past it as the other grows.
 */
export const ATTRIBUTION_WINDOW_MS = 2_000;

/** By how much a sandbox must have grown over GROWTH_WINDOW_MS to be taken for the cause of its node's want of memory. */
export const GROWN_BYTES = 256 * 1024 ** 2;

/** How long after it ends a sandbox the guard ends none more: room for its memory to be freed and the node's to be measured anew. */
export const END_WINDOW_MS = 10_000;

/** How often the guard says that its node's want of memory is another pod's, while it lasts. */
const FOREIGN_SAID_MS = 60_000;

/** By how much a sandbox's limit must move before it is written again. */
const LIMIT_STEP_BYTES = 16 * 1024 ** 2;

/** A sandbox as the guard plans for it: its key, its pod's state, what its pod and its own cgroup hold, and by how much it grew over GROWTH_WINDOW_MS, and over ATTRIBUTION_WINDOW_MS. */
export interface Guarded {
  readonly key: string;
  readonly state: SandboxState;
  readonly bytes: number;
  readonly scopeBytes: number;
  readonly grown: number;
  readonly grownRecently: number;
}

/**
 * What the guard does at a look: the limit of each bound sandbox's cgroup, by its key, null where
 * it has none; the sandbox it ends, if any; whether its node wants memory that no sandbox is the
 * cause of; and of what the node lost over ATTRIBUTION_WINDOW_MS, what the sandboxes took by their
 * growth together, and what the other pods took, the rest.
 */
export interface GuardPlan {
  readonly limits: ReadonlyMap<string, number | null>;
  readonly end: string | null;
  readonly foreign: boolean;
  readonly sandboxesTook: number;
  readonly othersTook: number;
}

/**
 * What the guard does, as the module says, given `room`, what the node has left for its
 * sandboxes before kubelet evicts or a cgroup above them reaches its limit; `lastRoom`, what it
 * has left before kubelet evicts at once or that limit; `sandboxes`; whether one it ended is still
 * there, or it ended one within END_WINDOW_MS (`ending`), while which it ends none; and `lost`,
 * what the room lost over ATTRIBUTION_WINDOW_MS, its most then less what it is now.
 */
export function guardPlan(room: number, lastRoom: number, sandboxes: readonly Guarded[], ending: boolean, lost: number): GuardPlan {
  const bound = sandboxes.filter(({ state }) => state === "bound");
  const share = Math.floor(Math.max(0, room) / Math.max(1, bound.length));
  // No less than its share, which is less than SANDBOX_MEMORY_HEADROOM_BYTES where it is limited, and the room.
  const headroom = Math.max(SANDBOX_MEMORY_SLACK_BYTES, Math.min(SANDBOX_MEMORY_HEADROOM_BYTES, room));
  const limits = new Map(bound.map(({ key, scopeBytes }) => [key, share < SANDBOX_MEMORY_LIMITED_BELOW_BYTES ? scopeBytes + headroom : null]));
  const sandboxesTook = sandboxes.reduce((sum, { grownRecently }) => sum + grownRecently, 0);
  const othersTook = Math.max(0, lost - sandboxesTook);
  if (lastRoom >= SANDBOX_MEMORY_SLACK_BYTES) return { limits, end: null, foreign: false, sandboxesTook, othersTook };
  const grower = sandboxes.filter(({ grown }) => grown >= GROWN_BYTES).toSorted((a, b) => b.grown - a.grown)[0];
  // None grew into it, or the other pods take the most of what the node is losing: theirs the want.
  if (grower === undefined || othersTook > sandboxesTook) return { limits, end: null, foreign: true, sandboxesTook, othersTook };
  // The room no longer falling: none ended for it, until it falls again or the one grown is past the window.
  if (lost <= 0) return { limits, end: null, foreign: false, sandboxesTook, othersTook };
  return { limits, end: ending ? null : grower.key, foreign: false, sandboxesTook, othersTook };
}

/** A file's text, or null where it cannot be read. */
const text = (path: string): Promise<string | null> => readFile(path, "utf8").catch(() => null);

/** Writes `value` into the cgroup file `path`: whether it could. */
const write = (path: string, value: string): Promise<boolean> => writeFile(path, value).then(() => true, () => false);

/**
 * The cgroup a pod's sandbox's processes are in, under its pod's cgroup `pod`: the one cgroup of
 * the pod's with processes, its pod sandbox's scope; null where the pod holds processes of its
 * own, or no cgroup of it does, or more than one, as then none is the sandbox's alone.
 */
export async function sandboxCgroup(pod: string): Promise<string | null> {
  const held = async (dir: string) => ((await text(join(dir, "cgroup.procs"))) ?? "").trim() !== "";
  if (await held(pod)) return null;
  const children = (await readdir(pod, { withFileTypes: true }).catch(() => [])).filter((entry) => entry.isDirectory()).map((entry) => join(pod, entry.name));
  const holding = [];
  for (const child of children) if (await held(child)) holding.push(child);
  return holding.length === 1 ? (holding[0] ?? null) : null;
}

/** Of what the cgroup `dir` holds, its page cache the kernel reclaims as it nears its limit, clean: its active and inactive file pages, but those dirty or being written back. */
export async function reclaimableIn(dir: string): Promise<number> {
  const stat = await text(join(dir, "memory.stat"));
  const of = (key: string) => statOf(stat, key) ?? 0;
  return Math.max(0, of("active_file") + of("inactive_file") - of("file_dirty") - of("file_writeback"));
}

/**
 * What each cgroup above the pods `pods`, under the root `cgroups`, has left under its
 * memory.max: what it does not hold, and of what it holds what `reclaimable` says the kernel
 * reclaims as it nears its limit (reclaimableIn). The least, or Infinity where none has a limit.
 */
export async function aboveRoom(cgroups: string, pods: readonly string[], reclaimable: (dir: string) => Promise<number> = reclaimableIn): Promise<number> {
  const above = new Set<string>();
  for (const pod of pods) for (let dir = dirname(pod); dir.startsWith(`${cgroups}/`); dir = dirname(dir)) above.add(dir);
  let room = Number.POSITIVE_INFINITY;
  for (const dir of above) {
    const max = await numberIn(join(dir, "memory.max"));
    const current = await numberIn(join(dir, "memory.current"));
    if (max !== null && current !== null) room = Math.min(room, max - current + (await reclaimable(dir)));
  }
  return room;
}

const ended = meter.createCounter("alasio.node_agent.sandboxes.ended", {
  description: "The sandboxes the node agent ended, the one that grew the most on its node, as its node's memory came near its end",
  unit: "{sandbox}",
});
const spared = meter.createCounter("alasio.node_agent.memory.foreign_pressure", {
  description: "The node agent's looks that found its node's memory near its end with no sandbox grown into it, or other pods having taken the most of it, the want another pod's, which it left to kubelet and the kernel",
  unit: "{look}",
});
const unguarded = meter.createCounter("alasio.node_agent.sandboxes.unguarded", {
  description: "The sandboxes the node agent left unguarded, as no one cgroup of their pod's held their processes",
  unit: "{sandbox}",
});

/** The key of a sandbox: its pod's namespace and name. */
const keyOf = ({ namespace, pod }: Pick<SandboxCgroup, "namespace" | "pod">): string => `${namespace}/${pod}`;

/** How many MiB `bytes` is, for a log. */
const mib = (bytes: number): string => `${Math.round(bytes / 1024 ** 2)} MiB`;

/**
 * What the guard last did of a sandbox's: the limit in force since it last set its cgroup's, null
 * for none of its own, its pod's limit alone then; and, where it ended it, what its pod held then;
 * as the agent answers it (`GET /memory/guard`).
 */
export interface GuardRecord {
  readonly namespace: string;
  readonly pod: string;
  readonly limitBytes: number | null;
  readonly endedBytes: number | null;
  readonly at: number;
}

/** How long the guard keeps what it did of a sandbox's after its pod has gone: as long as its gateway takes to release its binding, and more. */
const RECORD_KEPT_MS = 10 * 60_000;

/** What the guard did of the node's sandboxes, by their keys (GuardRecord), as the agent answers it. */
export class GuardRecords {
  readonly #records = new Map<string, GuardRecord>();

  /** Notes what the guard did of the sandbox of `namespace`/`pod`, besides what it did before. */
  note(namespace: string, pod: string, change: Partial<Pick<GuardRecord, "limitBytes" | "endedBytes">>): void {
    const key = `${namespace}/${pod}`;
    const before = this.#records.get(key) ?? { namespace, pod, limitBytes: null, endedBytes: null, at: 0 };
    this.#records.set(key, { ...before, ...change, at: Date.now() });
  }

  /** What the guard did, of the sandboxes it did anything of within RECORD_KEPT_MS. */
  all(): readonly GuardRecord[] {
    for (const [key, record] of this.#records) if (Date.now() - record.at > RECORD_KEPT_MS) this.#records.delete(key);
    return [...this.#records.values()];
  }
}

/**
 * The node's sandboxes being restored, by their pods' keys, as the agent restores them (./agent.ts
 * resume, ./standby.ts): what their restores bring in, their images' bytes together, null where it
 * is not known; and when each had read its images, or ended, null while it reads them. What a
 * sandbox fills its memory with from its image is not what its node wants memory for: the guard
 * counts of a sandbox's growth while it restores only what is past its image, and
 * SANDBOX_MEMORY_SLACK_BYTES beside it for its sandbox's own; of one whose image's bytes are not
 * known, none. And for GROWTH_WINDOW_MS after, as its growth is measured over as long. One that has
 * not read its images within RECORD_KEPT_MS is forgotten, as a restore whose pod went is.
 */
export class Restores {
  readonly #restores = new Map<string, { readonly bytes: number | null; readonly since: number; readonly readAt: number | null }>();

  /** Notes that the sandbox of `namespace`/`pod` is restored from images of `bytes` together, null where not known. */
  restoring(namespace: string, pod: string, bytes: number | null): void {
    this.#restores.set(`${namespace}/${pod}`, { bytes, since: Date.now(), readAt: null });
  }

  /** Notes that the restore of the sandbox of `namespace`/`pod` has read its images, or ended. */
  read(namespace: string, pod: string): void {
    const restore = this.#restores.get(`${namespace}/${pod}`);
    if (restore !== undefined) this.#restores.set(`${namespace}/${pod}`, { ...restore, readAt: Date.now() });
  }

  /**
   * What the guard counts the sandbox of the key `key` as holding at the least, as the class says:
   * what its restore brings in, and SANDBOX_MEMORY_SLACK_BYTES; Infinity where that is not known;
   * null where it is not being restored.
   */
  floorOf(key: string, now: number): number | null {
    const restore = this.#restores.get(key);
    if (restore === undefined) return null;
    if (restore.readAt === null ? now - restore.since > RECORD_KEPT_MS : now - restore.readAt > GROWTH_WINDOW_MS) {
      this.#restores.delete(key);
      return null;
    }
    return restore.bytes === null ? Number.POSITIVE_INFINITY : restore.bytes + SANDBOX_MEMORY_SLACK_BYTES;
  }
}

/**
 * What guardNodeMemory looks at the node with, and how often; what each cgroup above the pods
 * holds that the kernel reclaims (reclaimableIn unless given); where it keeps what it does; and the
 * sandboxes being restored (none unless given).
 */
export interface GuardOptions extends Pick<MemoryOptions, "runsc" | "nodeRoot" | "attachRoot"> {
  readonly everyMs?: number;
  readonly reclaimable?: (dir: string) => Promise<number>;
  readonly records?: GuardRecords;
  readonly restores?: Restores;
}

/** What the guard keeps from look to look: each sandbox's bytes as each look read them, by its key; the room as each look over ATTRIBUTION_WINDOW_MS found it; when it last ended one, and the cgroups of those it ended, kept until they go; and when it last said its node's want was another pod's. */
export interface GuardState {
  readonly seen: Map<string, readonly { readonly at: number; readonly bytes: number }[]>;
  rooms: readonly { readonly at: number; readonly room: number }[];
  readonly endedCgroups: Set<string>;
  lastEndAt: number;
  lastForeignAt: number;
  readonly unguarded: Set<string>;
}

/** A guard's state before its first look. */
export const newGuardState = (): GuardState => ({ seen: new Map(), rooms: [], endedCgroups: new Set(), lastEndAt: 0, lastForeignAt: 0, unguarded: new Set() });

/** One look of the guard's, as the module says, with what it keeps from look to look. */
export const guardOnce = ({ runsc, nodeRoot, attachRoot, reclaimable = reclaimableIn, records = new GuardRecords(), restores = new Restores() }: GuardOptions, state: GuardState): Effect.Effect<void> =>
  Effect.gen(function*() {
    const now = Date.now();
    const cgroups = join(nodeRoot, "sys", "fs", "cgroup");
    const listed = (yield* sandboxesOf({ runsc, nodeRoot, attachRoot }, false)).filter((sandbox): sandbox is SandboxCgroup & { readonly cgroup: string } => sandbox.cgroup !== null);
    // Each sandbox's own cgroup; one whose pod has none such left be, said once.
    const sandboxes: (SandboxCgroup & { readonly cgroup: string; readonly scope: string })[] = [];
    for (const sandbox of listed) {
      const scope = yield* Effect.promise(() => sandboxCgroup(sandbox.cgroup));
      if (scope === null) {
        if (!state.unguarded.has(keyOf(sandbox))) {
          state.unguarded.add(keyOf(sandbox));
          unguarded.add(1);
          yield* Effect.logWarning(`left the sandbox of ${keyOf(sandbox)} unguarded: no one cgroup of its pod's holds its processes`);
        }
        continue;
      }
      sandboxes.push({ ...sandbox, scope });
    }
    // What is gone is forgotten: an ended sandbox once its scope is, a sandbox's bytes once it is.
    for (const cgroup of state.endedCgroups) if (!sandboxes.some(({ scope }) => scope === cgroup)) state.endedCgroups.delete(cgroup);
    for (const key of state.seen.keys()) if (!sandboxes.some((sandbox) => keyOf(sandbox) === key)) state.seen.delete(key);
    for (const key of state.unguarded) if (!listed.some((sandbox) => keyOf(sandbox) === key)) state.unguarded.delete(key);
    const available = meminfoOf(yield* Effect.promise(() => text(join(nodeRoot, "proc", "meminfo"))), "MemAvailable");
    const above = yield* Effect.promise(() => aboveRoom(cgroups, sandboxes.map(({ cgroup }) => cgroup), reclaimable));
    const room = Math.min(available === null ? Number.POSITIVE_INFINITY : available - NODE_MEMORY_SOFT_EVICTION_BYTES, above);
    const lastRoom = Math.min(available === null ? Number.POSITIVE_INFINITY : available - NODE_MEMORY_HARD_EVICTION_BYTES, above);
    // What the room lost over ATTRIBUTION_WINDOW_MS: its most then less what it is now; none where it is or was unbounded.
    state.rooms = [...state.rooms.filter(({ at }) => now - at <= ATTRIBUTION_WINDOW_MS), { at: now, room }];
    const most = Math.max(...state.rooms.map((each) => each.room));
    const lost = Number.isFinite(most) && Number.isFinite(room) ? Math.max(0, most - room) : 0;
    const guarded: Guarded[] = [];
    for (const sandbox of sandboxes) {
      const key = keyOf(sandbox);
      const scopeBytes = (yield* Effect.promise(() => numberIn(join(sandbox.scope, "memory.current")))) ?? sandbox.bytes ?? 0;
      const seen = [...(state.seen.get(key) ?? []).filter(({ at }) => now - at <= GROWTH_WINDOW_MS), { at: now, bytes: scopeBytes }];
      state.seen.set(key, seen);
      // Of one being restored, what is past what its restore brings in alone.
      const floor = restores.floorOf(key, now) ?? 0;
      const least = Math.max(Math.min(...seen.map(({ bytes }) => bytes)), floor);
      const leastRecently = Math.max(Math.min(...seen.filter(({ at }) => now - at <= ATTRIBUTION_WINDOW_MS).map(({ bytes }) => bytes)), floor);
      guarded.push({ key, state: sandbox.state, bytes: sandbox.bytes ?? 0, scopeBytes, grown: Math.max(0, scopeBytes - least), grownRecently: Math.max(0, scopeBytes - leastRecently) });
    }
    const ending = state.endedCgroups.size > 0 || now - state.lastEndAt < END_WINDOW_MS;
    const plan = guardPlan(room, lastRoom, guarded, ending, lost);
    for (const [key, limit] of plan.limits) {
      const sandbox = sandboxes.find((each) => keyOf(each) === key);
      if (sandbox === undefined) continue;
      const held = yield* Effect.promise(() => numberIn(join(sandbox.scope, "memory.max")));
      if (limit === null ? held !== null : held === null || Math.abs(held - limit) >= LIMIT_STEP_BYTES) {
        if (yield* Effect.promise(() => write(join(sandbox.scope, "memory.max"), limit === null ? "max\n" : `${limit}\n`))) {
          // The limit in force: its pod's own where that is the less, which kubelet set and the pod's own OOM then ends it at.
          const pods = limit === null ? null : yield* Effect.promise(() => numberIn(join(sandbox.cgroup, "memory.max")));
          records.note(sandbox.namespace, sandbox.pod, { limitBytes: limit === null ? null : Math.min(limit, pods ?? limit) });
        }
      }
      // Set again as systemd may set it back, as it sets its units' cgroups' attributes.
      if ((yield* Effect.promise(() => text(join(sandbox.scope, "memory.oom.group"))))?.trim() !== "1") yield* Effect.promise(() => write(join(sandbox.scope, "memory.oom.group"), "1\n"));
    }
    if (plan.foreign) {
      spared.add(1);
      if (now - state.lastForeignAt >= FOREIGN_SAID_MS) {
        state.lastForeignAt = now;
        yield* Effect.logWarning(
          `its node has ${mib(Math.max(0, lastRoom))} left before kubelet evicts at once or its pods reach their limit, and of the ${mib(lost)} it lost in ${ATTRIBUTION_WINDOW_MS / 1000} s sandboxes took ${mib(plan.sandboxesTook)}, other pods ${mib(plan.othersTook)}: another pod's want, left to kubelet's eviction and the kernel`,
        );
      }
      return;
    }
    const end = sandboxes.find((sandbox) => keyOf(sandbox) === plan.end);
    if (end === undefined) return;
    const grown = guarded.find(({ key }) => key === plan.end)?.grown ?? 0;
    state.endedCgroups.add(end.scope);
    state.lastEndAt = now;
    if (yield* Effect.promise(() => write(join(end.scope, "cgroup.kill"), "1\n"))) {
      ended.add(1, { "alasio.sandbox.state": end.state });
      records.note(end.namespace, end.pod, { endedBytes: end.bytes ?? 0 });
      yield* Effect.logWarning(
        `ended the sandbox of ${keyOf(end)} (${end.state}) at ${mib(end.bytes ?? 0)}, grown by ${mib(grown)} in ${GROWTH_WINDOW_MS / 1000} s, the most on its node, as its node had ${mib(Math.max(0, lastRoom))} left before kubelet evicts at once or its pods reach their limit`,
      );
    } else {
      yield* Effect.logWarning(`could not end the sandbox of ${keyOf(end)}, the one grown the most on its node, as its cgroup takes no kill`);
    }
  });

/**
 * Guards the node's memory, as the module says, every `everyMs` (GUARD_MS), for as long as the
 * scope is open: what each cgroup holds read at every look; its sandboxes listed by runsc every
 * LIST_MS at most; and what the cgroups above the pods hold that the kernel reclaims (their
 * memory.stat) every LIST_MS at most, and at the next look once one has reached its limit since
 * (its memory.events' max), as the kernel then reclaims its page cache for the limit, which a count
 * read before would overstate its room by. Page cache the kernel reclaims for the node's own want
 * a count up to LIST_MS old overstates too, the room under the node's (MemAvailable) read at every
 * look then the lesser. A cgroup's memory.stat has the kernel gather its counts from every
 * processor as it is read: every look's, the pods' and those above, slowed a stop's save of 3 GiB
 * by some 250 ms (memory files' save 380 to 628 ms at the median).
 */
export const guardNodeMemory = (options: GuardOptions): Effect.Effect<void, never, Scope.Scope> =>
  Effect.gen(function*() {
    const containers = yield* Effect.cachedWithTTL(options.runsc.containers, (exit) => (Exit.isSuccess(exit) ? LIST_MS : 0));
    const reclaimable = options.reclaimable ?? reclaimableIn;
    const read = new Map<string, { readonly at: number; readonly reached: number | null; readonly bytes: number }>();
    const reclaimableNow = async (dir: string) => {
      // How often it has reached its limit: a read of its events, which has the kernel gather nothing.
      const reached = statOf(await text(join(dir, "memory.events")), "max");
      const known = read.get(dir);
      if (known !== undefined && known.reached === reached && Date.now() - known.at < LIST_MS) return known.bytes;
      const bytes = await reclaimable(dir);
      read.set(dir, { at: Date.now(), reached, bytes });
      return bytes;
    };
    const state = newGuardState();
    yield* guardOnce({ ...options, runsc: { containers }, reclaimable: reclaimableNow }, state).pipe(
      Effect.catchDefect((defect) => Effect.logWarning(`the node's memory could not be guarded: ${String(defect)}`)),
      Effect.repeat(Schedule.spaced(options.everyMs ?? GUARD_MS)),
      Effect.forkScoped,
    );
  });
