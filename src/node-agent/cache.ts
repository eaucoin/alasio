/**
 * The node's snapshot cache: the images of the snapshots taken or prepared on this node,
 * each in `<root>/<environment>/<workspace>/<snapshot>/`, where a restore on this node
 * reads them. A snapshot is written in a directory of its own beside that one, whose name
 * begins with ".", and moved there once it is whole, so what the cache holds is always
 * whole; what a stopped agent left half written is removed as the next one starts.
 *
 * The cache holds at most `budget` bytes, its least recently used snapshots evicted
 * first, but for any used within `grace`, which a restore may still be reading. When a
 * snapshot was last used is its directory's modification time, so it outlasts the agent.
 * Whatever the budget, nothing is written to it that would leave the node's disk with
 * less than RESERVE_BYTES free: room is made first, snapshots evicted as they would be
 * over the budget, and what it cannot be made for is refused.
 */
import { mkdir, readdir, rename, rm, stat, statfs, utimes } from "node:fs/promises";
import { join } from "node:path";

import { Clock, Effect, Schema } from "effect";

import { NAME, SNAPSHOT_ID } from "../kube/sandbox-shape.ts";

/** What the cache is given: its directory, its byte budget, how long a snapshot used is kept regardless, and how many bytes its disk has free. */
export interface CacheOptions {
  readonly root: string;
  readonly budget: number;
  readonly graceMs: number;
  /** The bytes free on the cache's disk, as statfs says unless told. */
  readonly free?: (root: string) => Promise<number>;
}

/** What the cache leaves free on the node's disk whatever it writes: the node's own room to run. */
export const RESERVE_BYTES = 2 * 1024 ** 3;

/** The bytes free on the disk of `directory`, to anyone. */
async function freeOnDisk(directory: string): Promise<number> {
  await mkdir(directory, { recursive: true });
  const { bavail, bsize } = await statfs(directory);
  return bavail * bsize;
}

/** A snapshot the cache holds. */
export interface CachedSnapshot {
  readonly environment: string;
  readonly workspace: string;
  readonly snapshot: string;
  readonly bytes: number;
  /** When it was last used, in milliseconds since the epoch. */
  readonly usedAt: number;
}

/** The cache's directory could not be read or written. */
export class CacheError extends Schema.TaggedError<CacheError>()("CacheError", {
  reason: Schema.String,
}) {
  override get message(): string {
    return `the node's snapshot cache failed: ${this.reason}`;
  }
}

/** The node's snapshot cache; see makeSnapshotCache. */
export interface SnapshotCache {
  /** Where the snapshot's images are once it is whole. */
  readonly path: (environment: string, workspace: string, snapshot: string) => string;
  /** Where the snapshot is written until it is whole, emptied first. */
  readonly stage: (environment: string, workspace: string, snapshot: string) => Effect.Effect<string, CacheError>;
  /** Moves the staged snapshot where it is whole, used now. */
  readonly commit: (environment: string, workspace: string, snapshot: string) => Effect.Effect<void, CacheError>;
  /** Whether the snapshot is whole in the cache. */
  readonly holds: (environment: string, workspace: string, snapshot: string) => Effect.Effect<boolean, CacheError>;
  /** Records the snapshot as used now. */
  readonly use: (environment: string, workspace: string, snapshot: string) => Effect.Effect<void, CacheError>;
  /** Removes everything of the workspace but `keep`, staged or whole; all of it for null. */
  readonly clear: (environment: string, workspace: string, keep: string | null) => Effect.Effect<void, CacheError>;
  /** Removes the whole snapshots of the workspace but `keep` that were not used within the grace, which a restore may still be reading: those removed. */
  readonly discard: (environment: string, workspace: string, keep: string | null) => Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /** The whole snapshots the cache holds. */
  readonly list: Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /** Evicts snapshots, least recently used first, until the cache is within its budget: those evicted. */
  readonly evict: Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /**
   * Makes room on the disk for `bytes` more, RESERVE_BYTES left beside them, evicting
   * snapshots as `evict` does but for any of `keep`'s workspace's: those evicted. Fails,
   * saying so, where it cannot.
   */
  readonly room: (bytes: number, keep: { readonly environment: string; readonly workspace: string }) => Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /** Removes what was left half written. */
  readonly clean: Effect.Effect<void, CacheError>;
}

const SNAPSHOT = new RegExp(`^${SNAPSHOT_ID}$`, "u");

/**
 * `names`, each the name the agent gives what it is: the cache's paths are built from
 * nothing else, whatever asks for one. Any other is a bug, as the agent checks what it is
 * given first.
 */
function checked(environment: string, workspace: string, snapshot: string | null = null): void {
  if (!NAME.test(environment) || !NAME.test(workspace) || (snapshot !== null && !SNAPSHOT.test(snapshot))) {
    throw new Error(`the snapshot cache names no ${JSON.stringify([environment, workspace, snapshot])}`);
  }
}

/** The directory a snapshot is written in until it is whole. */
const staged = (snapshot: string): string => `.${snapshot}`;

/** The names of the directories in `directory`, but those being written; none when it is not there. */
async function directories(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  return entries.filter((entry) => entry.isDirectory() && !entry.name.startsWith(".")).map((entry) => entry.name).sort();
}

/** The bytes of the files under `directory`. */
async function size(directory: string): Promise<number> {
  let bytes = 0;
  for (const entry of await readdir(directory, { withFileTypes: true, recursive: true })) {
    if (entry.isFile()) bytes += (await stat(join(entry.parentPath, entry.name))).size;
  }
  return bytes;
}

/** The node's snapshot cache in `root`, as the module says. */
export function makeSnapshotCache({ root, budget, graceMs, free = freeOnDisk }: CacheOptions): SnapshotCache {
  const attempt = <A>(run: () => Promise<A>): Effect.Effect<A, CacheError> =>
    Effect.tryPromise({ try: run, catch: (cause) => new CacheError({ reason: cause instanceof Error ? cause.message : String(cause) }) });

  const workspaceDir = (environment: string, workspace: string): string => {
    checked(environment, workspace);
    return join(root, environment, workspace);
  };
  const path = (environment: string, workspace: string, snapshot: string): string => {
    checked(environment, workspace, snapshot);
    return join(workspaceDir(environment, workspace), snapshot);
  };
  const stagePath = (environment: string, workspace: string, snapshot: string): string => {
    checked(environment, workspace, snapshot);
    return join(workspaceDir(environment, workspace), staged(snapshot));
  };

  const use = (environment: string, workspace: string, snapshot: string): Effect.Effect<void, CacheError> =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) => attempt(() => utimes(path(environment, workspace, snapshot), now / 1000, now / 1000))),
    );

  const list = attempt(async () => {
    const found: CachedSnapshot[] = [];
    // What else is there is no snapshot of the agent's.
    for (const environment of (await directories(root)).filter((name) => NAME.test(name))) {
      for (const workspace of (await directories(join(root, environment))).filter((name) => NAME.test(name))) {
        for (const snapshot of (await directories(workspaceDir(environment, workspace))).filter((name) => SNAPSHOT.test(name))) {
          const directory = path(environment, workspace, snapshot);
          found.push({ environment, workspace, snapshot, bytes: await size(directory), usedAt: (await stat(directory)).mtimeMs });
        }
      }
    }
    return found;
  });

  return {
    path,

    stage: (environment, workspace, snapshot) => {
      const directory = stagePath(environment, workspace, snapshot);
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await mkdir(directory, { recursive: true });
      }).pipe(Effect.as(directory));
    },

    commit: (environment, workspace, snapshot) =>
      attempt(() => rename(stagePath(environment, workspace, snapshot), path(environment, workspace, snapshot))).pipe(
        Effect.andThen(use(environment, workspace, snapshot)),
      ),

    holds: (environment, workspace, snapshot) =>
      attempt(() => directories(workspaceDir(environment, workspace))).pipe(Effect.map((snapshots) => snapshots.includes(snapshot))),

    use,

    clear: (environment, workspace, keep) =>
      attempt(async () => {
        const directory = workspaceDir(environment, workspace);
        if (keep === null) return rm(directory, { recursive: true, force: true });
        const entries = await readdir(directory).catch(() => [] as string[]);
        await Promise.all(entries.filter((name) => name !== keep).map((name) => rm(join(directory, name), { recursive: true, force: true })));
      }),

    list,

    discard: (environment, workspace, keep) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        const unused = (yield* list).filter((cached) =>
          cached.environment === environment && cached.workspace === workspace && cached.snapshot !== keep && now - cached.usedAt >= graceMs
        );
        for (const cached of unused) yield* attempt(() => rm(path(cached.environment, cached.workspace, cached.snapshot), { recursive: true, force: true }));
        return unused;
      }),

    evict: Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const held = [...(yield* list)].sort((a, b) => a.usedAt - b.usedAt);
      let total = held.reduce((sum, { bytes }) => sum + bytes, 0);
      const evicted: CachedSnapshot[] = [];
      for (const cached of held) {
        if (total <= budget) break;
        if (now - cached.usedAt < graceMs) continue;
        yield* attempt(() => rm(path(cached.environment, cached.workspace, cached.snapshot), { recursive: true, force: true }));
        total -= cached.bytes;
        evicted.push(cached);
      }
      return evicted;
    }),

    room: (bytes, keep) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        let available = yield* attempt(() => free(root));
        const evicted: CachedSnapshot[] = [];
        const evictable = [...(yield* list)]
          .filter((cached) => (cached.environment !== keep.environment || cached.workspace !== keep.workspace) && now - cached.usedAt >= graceMs)
          .sort((a, b) => a.usedAt - b.usedAt);
        for (const cached of evictable) {
          if (available >= bytes + RESERVE_BYTES) break;
          yield* attempt(() => rm(path(cached.environment, cached.workspace, cached.snapshot), { recursive: true, force: true }));
          available += cached.bytes;
          evicted.push(cached);
        }
        if (evicted.length > 0) available = yield* attempt(() => free(root));
        if (available < bytes + RESERVE_BYTES) {
          return yield* new CacheError({ reason: `its disk has ${available} bytes free, and ${bytes} more would leave less than the ${RESERVE_BYTES} it keeps free` });
        }
        return evicted;
      }),

    clean: attempt(async () => {
      for (const environment of (await directories(root)).filter((name) => NAME.test(name))) {
        for (const workspace of (await directories(join(root, environment))).filter((name) => NAME.test(name))) {
          const directory = workspaceDir(environment, workspace);
          const entries = await readdir(directory);
          await Promise.all(entries.filter((name) => name.startsWith(".")).map((name) => rm(join(directory, name), { recursive: true, force: true })));
        }
      }
    }),
  };
}
