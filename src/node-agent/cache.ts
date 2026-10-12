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
 */
import { mkdir, readdir, rename, rm, stat, utimes } from "node:fs/promises";
import { join } from "node:path";

import { Clock, Effect, Schema } from "effect";

/** What the cache is given: its directory, its byte budget, and how long a snapshot used is kept regardless. */
export interface CacheOptions {
  readonly root: string;
  readonly budget: number;
  readonly graceMs: number;
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
  /** The whole snapshots the cache holds. */
  readonly list: Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /** Evicts snapshots, least recently used first, until the cache is within its budget: those evicted. */
  readonly evict: Effect.Effect<readonly CachedSnapshot[], CacheError>;
  /** Removes what was left half written. */
  readonly clean: Effect.Effect<void, CacheError>;
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
export function makeSnapshotCache({ root, budget, graceMs }: CacheOptions): SnapshotCache {
  const attempt = <A>(run: () => Promise<A>): Effect.Effect<A, CacheError> =>
    Effect.tryPromise({ try: run, catch: (cause) => new CacheError({ reason: cause instanceof Error ? cause.message : String(cause) }) });

  const workspaceDir = (environment: string, workspace: string): string => join(root, environment, workspace);
  const path = (environment: string, workspace: string, snapshot: string): string => join(workspaceDir(environment, workspace), snapshot);
  const stagePath = (environment: string, workspace: string, snapshot: string): string => join(workspaceDir(environment, workspace), staged(snapshot));

  const use = (environment: string, workspace: string, snapshot: string): Effect.Effect<void, CacheError> =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) => attempt(() => utimes(path(environment, workspace, snapshot), now / 1000, now / 1000))),
    );

  const list = attempt(async () => {
    const found: CachedSnapshot[] = [];
    for (const environment of await directories(root)) {
      for (const workspace of await directories(join(root, environment))) {
        for (const snapshot of await directories(workspaceDir(environment, workspace))) {
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

    clean: attempt(async () => {
      for (const environment of await directories(root)) {
        for (const workspace of await directories(join(root, environment))) {
          const directory = workspaceDir(environment, workspace);
          const entries = await readdir(directory);
          await Promise.all(entries.filter((name) => name.startsWith(".")).map((name) => rm(join(directory, name), { recursive: true, force: true })));
        }
      }
    }),
  };
}
