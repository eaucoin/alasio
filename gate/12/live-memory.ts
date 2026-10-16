/**
 * How 12's first gate (live.test.ts) judges the memory its checkpoints cost: the trend of the
 * Sentry's own memory over a run's cycles, the checkpoints' cost what a run that checkpoints
 * grows by beyond a baseline run of the same cycles that does not (live-judge.test.ts). What
 * the workload holds, as a session's runtime that keeps what its cells compile does, grows
 * both alike and is no checkpoint's cost.
 */

/** The cycles the Sentry's memory settles over, which its trend leaves out. */
export const WARM_UP = 10;

/**
 * The most the Sentry's own memory may grow by a checkpoint beyond the baseline's growth, as
 * their trends over the rest say: one that keeps what a checkpoint took ends the sandbox by
 * the host's OOM killer (160 MB a checkpoint, run 38744966370).
 */
export const MAX_SENTRY_GROWTH_BYTES = 2 * 1024 ** 2;

/** The slope of the least-squares line through `points`. */
export function slope(points: readonly (readonly [number, number])[]): number {
  const n = points.length;
  const mean = (values: readonly number[]) => values.reduce((sum, value) => sum + value, 0) / n;
  const x = mean(points.map(([px]) => px));
  const y = mean(points.map(([, py]) => py));
  return points.reduce((sum, [px, py]) => sum + (px - x) * (py - y), 0) / points.reduce((sum, [px]) => sum + (px - x) ** 2, 0);
}

/** What a run records of each cycle that the judge reads. */
export interface CycleMemory {
  readonly index: number;
  /** The Sentry's own (anonymous) memory after the cycle, in bytes. */
  readonly sentry: { readonly anonBytes: number };
  /** The sandbox's memory by kind after the cycle, as its /proc/meminfo says it, in bytes. */
  readonly sandbox: Readonly<Record<string, number>>;
}

/** The trend of `bytes` of each cycle past WARM_UP, in bytes a cycle. */
export function trend(cycles: readonly CycleMemory[], bytes: (cycle: CycleMemory) => number): number {
  return slope(cycles.slice(WARM_UP).map((cycle) => [cycle.index, bytes(cycle)]));
}
