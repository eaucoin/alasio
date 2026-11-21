/**
 * The restores on the node whose images are still being read, which the agent's background
 * work yields to: a compaction reads and writes the node's disk, which a restore reads its
 * image from, and a restore that shares it waits on its page loads as its sandbox resumes. A
 * restore loads from its prepare until runsc says it has read every page, or it is abandoned,
 * or for as long as it was given at most, should neither be said.
 *
 * Work yields either by waiting until no restore loads (`quiet`), between the reads and writes
 * it does itself, or by being paused and resumed as restores begin and end loading
 * (`pausing`), as a process it runs is. Neither CPU nor I/O priority makes it yield: a disk
 * served without an I/O scheduler, as virtual disks often are, serves every priority alike.
 *
 * How long work yielded so while a restore loaded, to it or to another beside it, is told as its
 * load ends: what its load cost what yields.
 */
import { Effect, FiberMap, Latch, type Scope } from "effect";

/** The restores loading on the node; see makeLoading. */
export interface Loading {
  /** The restore `key` loads, from now until `end` says it has ended, or for `atMostMs`. */
  readonly begin: (key: string, atMostMs: number) => Effect.Effect<void>;
  /**
   * The restore `key` has ended loading: how long, in milliseconds, work yielded to restores
   * while it loaded; null for one that does not load, which is ended already.
   */
  readonly end: (key: string) => Effect.Effect<number | null>;
  /** Waits until no restore loads. */
  readonly quiet: Effect.Effect<void>;
  /**
   * Tells `pause` true as restores begin loading, at once where one does, and false once none
   * does, until the function it answers is called.
   */
  readonly pausing: (pause: (paused: boolean) => void) => () => void;
}

export const makeLoading: Effect.Effect<Loading, never, Scope.Scope> = Effect.gen(function*() {
  /** Each restore loading, by its key, as the fiber that ends it once it has loaded for as long as it was given. */
  const loading = yield* FiberMap.make<string>();
  const loads = new Set<string>();
  const quiet = yield* Latch.make(true);
  const pausers = new Set<(paused: boolean) => void>();
  const clock = yield* Effect.clockWith(Effect.succeed);
  /** How many wait for quiet now. */
  let waiting = 0;
  /** How long, in milliseconds, work yielded to restores in all, as of `settledAt`: paused, or waiting for quiet, while any loaded. */
  let yielded = 0;
  let settledAt = clock.currentTimeMillisUnsafe();
  /** Of each restore loading, how long work had yielded in all as it began. */
  const yieldedAsBegun = new Map<string, number>();

  /** Counts how long work has yielded since it was last counted, before what yields or loads changes. */
  const settle = (): void => {
    const now = clock.currentTimeMillisUnsafe();
    if (loads.size > 0 && pausers.size + waiting > 0) yielded += now - settledAt;
    settledAt = now;
  };

  /** Says whether the restore `key` loads, and tells what yields to restores where that makes any load, or none. */
  const mark = (key: string, now: boolean): void => {
    settle();
    const before = loads.size > 0;
    if (now) {
      if (!loads.has(key)) yieldedAsBegun.set(key, yielded);
      loads.add(key);
    } else {
      loads.delete(key);
      yieldedAsBegun.delete(key);
    }
    const after = loads.size > 0;
    if (before === after) return;
    if (after) quiet.closeUnsafe();
    else quiet.openUnsafe();
    for (const pause of pausers) pause(after);
  };

  return {
    begin: (key, atMostMs) =>
      Effect.sync(() => mark(key, true)).pipe(
        Effect.andThen(FiberMap.run(loading, key, Effect.sleep(atMostMs).pipe(Effect.andThen(Effect.sync(() => mark(key, false)))))),
        Effect.asVoid,
      ),
    end: (key) =>
      FiberMap.remove(loading, key).pipe(Effect.andThen(Effect.sync(() => {
        const asBegun = yieldedAsBegun.get(key);
        mark(key, false);
        return asBegun === undefined ? null : yielded - asBegun;
      }))),
    quiet: Effect.acquireUseRelease(
      Effect.sync(() => {
        settle();
        waiting += 1;
      }),
      () => quiet.await,
      () =>
        Effect.sync(() => {
          settle();
          waiting -= 1;
        }),
    ),
    pausing: (pause) => {
      settle();
      pausers.add(pause);
      if (loads.size > 0) pause(true);
      return () => {
        settle();
        pausers.delete(pause);
      };
    },
  };
});
