/**
 * Whether the kernel killed a process of the container alasio runs in for its memory, as the
 * container's cgroup (v2) counts its kills (memory.events, oom_kill), and the limit it ran out of
 * (memory.max): what tells a harness's process killed so from one killed otherwise, as the
 * kernel, the container out of its memory, kills its largest process alone
 * (cli/src/cluster/k3s.ts singleProcessOOMKill), the container living on. Its kills are counted
 * as a process starts and again as it dies, so a kill before is not taken for its own, and each
 * kill seen is told once. The count is the container's, so a process something else killed
 * after another of the container's ran out of its memory is taken for one the kernel killed:
 * rare. Where the cgroup's files cannot be read, as under cgroup v1, nothing is said of memory.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";

/** Where a container's cgroup is, as it sees it. */
export const CGROUP_DIR = "/sys/fs/cgroup";

/** A kill for memory: the limit the container ran out of, as a quantity, or null where it has none. */
export interface MemoryKill {
  readonly limit: string | null;
}

/** What a process is told, as it dies, of whether it was killed for its memory: the kill, or null where it was not, or the cgroup says nothing. */
export type MemoryKills = () => MemoryKill | null;

/** The kills for memory the cgroup at `dir` counts now: null where it cannot be read. */
function killsIn(dir: string): number | null {
  try {
    const line = readFileSync(join(dir, "memory.events"), "utf8").split("\n").find((each) => each.startsWith("oom_kill "));
    const kills = Number(line?.slice("oom_kill ".length));
    return line === undefined || !Number.isSafeInteger(kills) ? null : kills;
  } catch {
    return null;
  }
}

/** `bytes` as the quantity Kubernetes would write it: in whole GiB or MiB where it is one. */
export function asQuantity(bytes: number): string {
  if (bytes % 2 ** 30 === 0) return `${bytes / 2 ** 30}Gi`;
  if (bytes % 2 ** 20 === 0) return `${bytes / 2 ** 20}Mi`;
  return String(bytes);
}

/** The memory limit of the cgroup at `dir`: null where it has none, or it cannot be read. */
function limitIn(dir: string): string | null {
  try {
    const max = readFileSync(join(dir, "memory.max"), "utf8").trim();
    return /^[0-9]+$/u.test(max) ? asQuantity(Number(max)) : null;
  } catch {
    return null;
  }
}

/** Counts the kills for memory of the cgroup at `dir` now, as a process starts: what tells, as it dies, whether one was its own. */
export function watchMemoryKills(dir: string = CGROUP_DIR): MemoryKills {
  let before = killsIn(dir);
  return () => {
    if (before === null) return null;
    const now = killsIn(dir);
    if (now === null || now <= before) return null;
    // Each kill told once: one counted already is no later death's.
    before = now;
    return { limit: limitIn(dir) };
  };
}

/** What a process killed for its memory did, as a sentence's end: "ran out of its memory (1536Mi)". */
export function ranOutOf({ limit }: MemoryKill): string {
  return limit === null ? "ran out of memory" : `ran out of its memory (${limit})`;
}
