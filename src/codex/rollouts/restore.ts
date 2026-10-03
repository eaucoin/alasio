/**
 * Writing rollouts back: every file the given threads need that is missing
 * here, their own and those their history starts in, byte for byte, where it
 * was and with its modification time, each written whole or not at all.
 * Codex rebuilds its indexes from them itself. A file present here, or
 * compressed here, is never touched, and only threads asked for are written
 * back, so a thread deleted locally stays deleted unless alasio points at it.
 */
import { mkdir, rename, utimes, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { Effect, Schema } from "effect";

import { withLogScope } from "../../shared/log.ts";
import { listRolloutFiles } from "./files.ts";
import type { NeonRolloutStore, RestorableRollout } from "./store.ts";

/** What writing rollouts back failed with: the store, or the file system, as it said. */
export class RolloutRestoreError extends Schema.TaggedError<RolloutRestoreError>()("RolloutRestoreError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** The threads to write back, the store they are kept in, and the Codex home they go to. */
export interface RestoreRolloutsOptions {
  readonly store: NeonRolloutStore;
  readonly threadIds: readonly string[];
  readonly home: string;
}

/** `work`, its failure a RolloutRestoreError. */
const restoring = <A>(work: () => A | Promise<A>): Effect.Effect<A, RolloutRestoreError> =>
  Effect.tryPromise({ try: async () => await work(), catch: (cause) => new RolloutRestoreError({ cause }) });

async function writeBack(store: NeonRolloutStore, home: string, rollout: RestorableRollout): Promise<void> {
  const bytes = await store.read(rollout.name);
  if (bytes.length !== rollout.size) {
    throw new Error(`the store holds ${bytes.length} of its ${rollout.size} bytes`);
  }
  const target = join(home, rollout.path);
  const partial = `${target}.restoring`;
  await mkdir(dirname(target), { recursive: true });
  await writeFile(partial, bytes);
  const modified = new Date(rollout.modifiedMs);
  await utimes(partial, modified, modified);
  await rename(partial, target);
}

/**
 * Writes back what `threadIds` need under `home` from `store`. Succeeds with the
 * paths written; a file that cannot be is logged, and the rest still are.
 */
export const restoreRollouts = Effect.fnUntraced(
  function*({ store, threadIds, home }: RestoreRolloutsOptions): Effect.fn.Return<string[], RolloutRestoreError> {
    if (threadIds.length === 0) return [];
    const present = new Set((yield* restoring(() => listRolloutFiles(home))).map((file) => file.name));
    const written: string[] = [];
    for (const rollout of yield* restoring(() => store.lineage(threadIds))) {
      if (present.has(rollout.name)) continue;
      yield* restoring(() => writeBack(store, home, rollout)).pipe(
        Effect.andThen(Effect.logInfo(`wrote back ${rollout.path}`)),
        Effect.andThen(Effect.sync(() => written.push(rollout.path))),
        Effect.catchTag("RolloutRestoreError", (error) => Effect.logError(`could not write back ${rollout.path}: ${error.message}`)),
      );
    }
    return written;
  },
  withLogScope("rollout-restore"),
);
