/**
 * The node's snapshot cache: the images of the snapshots taken or prepared on this node, by
 * workspace, each image once, by its digest, as runsc names it, in
 * `<root>/<environment>/<workspace>/layers/<digest>/`; and, for a snapshot prepared for a
 * restore on this node, `<root>/<environment>/<workspace>/<snapshot>/`, where the restore
 * reads it: its image's files, linked to the cache's, and in its `layers/<digest>/` each
 * image it holds pages of besides its own, its layers, linked to the cache's too, as runsc
 * finds an image's layers. An image is written in a directory of its own beside the layers,
 * whose name begins with ".", and moved among them once it is whole and its digest known, so
 * what the cache holds is always whole; what a stopped agent left half written is removed as
 * the next one starts.
 *
 * The cache holds at most `budget` bytes, its least recently used workspaces evicted whole
 * first, but for any used within `grace`, which a restore may still be reading. When a
 * workspace was last used is its directory's modification time, so it outlasts the agent.
 * Whatever the budget, nothing is written to it that would leave the node's disk with less
 * than RESERVE_BYTES free: room is made first, workspaces evicted as they would be over the
 * budget, and what it cannot be made for is refused.
 */
import { link, mkdir, readdir, rename, rm, stat, statfs, symlink, utimes } from "node:fs/promises";
import { join, relative, sep } from "node:path";

import { Clock, Effect, Schema } from "effect";

import { NAME, SNAPSHOT_ID } from "../kube/sandbox-shape.ts";

/** What the cache is given: its directory, its byte budget, how long a workspace used is kept regardless, and how many bytes its disk has free. */
export interface CacheOptions {
  readonly root: string;
  readonly budget: number;
  readonly graceMs: number;
  /** The bytes free on the cache's disk, as statfs says unless told. */
  readonly free?: (root: string) => Promise<number>;
}

/** What the cache leaves free on the node's disk whatever it writes: the node's own room to run. */
export const RESERVE_BYTES = 2 * 1024 ** 3;

/** The directory of a workspace's images, each in one named by its digest; and of a snapshot's, its layers. */
export const LAYERS_DIR = "layers";

/** An image's digest, as runsc names it: the SHA-256 of its pages metadata, in hex. */
export const DIGEST = /^[0-9a-f]{64}$/u;

/** The bytes free on the disk of `directory`, to anyone. */
async function freeOnDisk(directory: string): Promise<number> {
  await mkdir(directory, { recursive: true });
  const { bavail, bsize } = await statfs(directory);
  return bavail * bsize;
}

/** A workspace the cache holds images of: their digests, their bytes, and when it was last used. */
export interface CachedWorkspace {
  readonly environment: string;
  readonly workspace: string;
  readonly images: readonly string[];
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
  /** The directory of the workspace's images, where the agent keeps beside them what they are. */
  readonly workspace: (environment: string, workspace: string) => string;
  /** Where the image `digest` of the workspace is once it is whole. */
  readonly image: (environment: string, workspace: string, digest: string) => string;
  /** Where an image is written until it is whole and its digest known, `name` the snapshot it is of, emptied first. */
  readonly stage: (environment: string, workspace: string, name: string) => Effect.Effect<string, CacheError>;
  /** Removes the image staged as `name`, as a save that failed left it. */
  readonly unstage: (environment: string, workspace: string, name: string) => Effect.Effect<void, CacheError>;
  /** Moves the image staged as `name` where an image of `digest` is, the workspace used now; one there already is kept, the staged one removed. */
  readonly commit: (environment: string, workspace: string, name: string, digest: string) => Effect.Effect<void, CacheError>;
  /** Whether the image `digest` of the workspace is whole in the cache. */
  readonly holds: (environment: string, workspace: string, digest: string) => Effect.Effect<boolean, CacheError>;
  /**
   * The directory a restore of the snapshot reads, made anew: the files of the image
   * `digests[0]`, then each of the others as its layer, every one of which the cache must
   * hold. The workspace is used now.
   */
  readonly prepare: (environment: string, workspace: string, snapshot: string, digests: readonly string[]) => Effect.Effect<string, CacheError>;
  /** Removes everything of the workspace. */
  readonly clear: (environment: string, workspace: string) => Effect.Effect<void, CacheError>;
  /**
   * Removes the workspace's images but `keep`, and the snapshots prepared of it, if it was
   * not used within the grace, as a restore may still be reading it: whether it did.
   */
  readonly discard: (environment: string, workspace: string, keep: readonly string[]) => Effect.Effect<boolean, CacheError>;
  /** The workspaces the cache holds images of. */
  readonly list: Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /** Evicts workspaces, least recently used first, until the cache is within its budget: those evicted. */
  readonly evict: Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /**
   * Makes room on the disk for `bytes` more, RESERVE_BYTES left beside them, evicting
   * workspaces as `evict` does but `keep`: those evicted. Fails, saying so, where it cannot.
   */
  readonly room: (bytes: number, keep: { readonly environment: string; readonly workspace: string }) => Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /** Removes what was left half written. */
  readonly clean: Effect.Effect<void, CacheError>;
}

const SNAPSHOT = new RegExp(`^${SNAPSHOT_ID}$`, "u");

/**
 * `names`, each the name the agent gives what it is: the cache's paths are built from
 * nothing else, whatever asks for one. Any other is a bug, as the agent checks what it is
 * given first.
 */
function checked(environment: string, workspace: string, snapshot: string | null = null, digests: readonly string[] = []): void {
  if (!NAME.test(environment) || !NAME.test(workspace) || (snapshot !== null && !SNAPSHOT.test(snapshot)) || !digests.every((digest) => DIGEST.test(digest))) {
    throw new Error(`the snapshot cache names no ${JSON.stringify([environment, workspace, snapshot, digests])}`);
  }
}

/** The directory an image is written in until it is whole. */
const staged = (name: string): string => `.${name}`;

/** The names of the directories in `directory`, but those being written; none when it is not there. */
async function directories(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  return entries.filter((entry) => entry.isDirectory() && !entry.name.startsWith(".")).map((entry) => entry.name).sort();
}

/** The bytes of the files under `directory`, each once however many links it has there, but those being written. */
async function size(directory: string): Promise<number> {
  const seen = new Set<number>();
  let bytes = 0;
  for (const entry of await readdir(directory, { withFileTypes: true, recursive: true })) {
    if (!entry.isFile() || relative(directory, entry.parentPath).split(sep).some((name) => name.startsWith("."))) continue;
    const { ino, size: fileBytes } = await stat(join(entry.parentPath, entry.name));
    if (seen.has(ino)) continue;
    seen.add(ino);
    bytes += fileBytes;
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
  const image = (environment: string, workspace: string, digest: string): string => {
    checked(environment, workspace, null, [digest]);
    return join(workspaceDir(environment, workspace), LAYERS_DIR, digest);
  };

  const use = (environment: string, workspace: string): Effect.Effect<void, CacheError> =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) => attempt(() => utimes(workspaceDir(environment, workspace), now / 1000, now / 1000))),
    );

  const holds = (environment: string, workspace: string, digest: string): Effect.Effect<boolean, CacheError> =>
    attempt(() => directories(join(workspaceDir(environment, workspace), LAYERS_DIR))).pipe(Effect.map((digests) => digests.includes(digest)));

  const list = attempt(async () => {
    const found: CachedWorkspace[] = [];
    // What else is there is no workspace of the agent's.
    for (const environment of (await directories(root)).filter((name) => NAME.test(name))) {
      for (const workspace of (await directories(join(root, environment))).filter((name) => NAME.test(name))) {
        const directory = workspaceDir(environment, workspace);
        const images = (await directories(join(directory, LAYERS_DIR))).filter((name) => DIGEST.test(name));
        found.push({ environment, workspace, images, bytes: await size(directory), usedAt: (await stat(directory)).mtimeMs });
      }
    }
    return found;
  });

  /** Removes `cached` whole. */
  const remove = (cached: CachedWorkspace): Effect.Effect<void, CacheError> =>
    attempt(() => rm(workspaceDir(cached.environment, cached.workspace), { recursive: true, force: true }));

  return {
    workspace: workspaceDir,
    image,

    stage: (environment, workspace, name) => {
      checked(environment, workspace, name);
      const directory = join(workspaceDir(environment, workspace), LAYERS_DIR, staged(name));
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await mkdir(directory, { recursive: true });
      }).pipe(Effect.as(directory));
    },

    unstage: (environment, workspace, name) => {
      checked(environment, workspace, name);
      return attempt(() => rm(join(workspaceDir(environment, workspace), LAYERS_DIR, staged(name)), { recursive: true, force: true }));
    },

    commit: (environment, workspace, name, digest) => {
      checked(environment, workspace, name, [digest]);
      const from = join(workspaceDir(environment, workspace), LAYERS_DIR, staged(name));
      return holds(environment, workspace, digest).pipe(
        Effect.flatMap((held) => attempt(() => (held ? rm(from, { recursive: true, force: true }) : rename(from, image(environment, workspace, digest))))),
        Effect.andThen(use(environment, workspace)),
      );
    },

    holds,

    prepare: (environment, workspace, snapshot, digests) => {
      checked(environment, workspace, snapshot, digests);
      const directory = join(workspaceDir(environment, workspace), snapshot);
      const [own, ...layers] = digests;
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await mkdir(join(directory, LAYERS_DIR), { recursive: true });
        const ownDir = image(environment, workspace, own ?? "");
        for (const entry of await readdir(ownDir, { withFileTypes: true })) {
          if (entry.isFile()) await link(join(ownDir, entry.name), join(directory, entry.name));
        }
        // Relative, so the link holds wherever the cache is mounted.
        for (const digest of layers) await symlink(join("..", "..", LAYERS_DIR, digest), join(directory, LAYERS_DIR, digest));
      }).pipe(Effect.andThen(use(environment, workspace)), Effect.as(directory));
    },

    clear: (environment, workspace) => attempt(() => rm(workspaceDir(environment, workspace), { recursive: true, force: true })),

    discard: (environment, workspace, keep) =>
      Effect.gen(function*() {
        checked(environment, workspace, null, keep);
        const now = yield* Clock.currentTimeMillis;
        const directory = workspaceDir(environment, workspace);
        const usedAt = yield* attempt(() => stat(directory).then(({ mtimeMs }) => mtimeMs, () => null));
        if (usedAt === null || now - usedAt < graceMs) return false;
        yield* attempt(async () => {
          for (const snapshot of (await directories(directory)).filter((name) => SNAPSHOT.test(name))) await rm(join(directory, snapshot), { recursive: true, force: true });
          for (const digest of await directories(join(directory, LAYERS_DIR))) {
            if (!keep.includes(digest)) await rm(join(directory, LAYERS_DIR, digest), { recursive: true, force: true });
          }
          // Removing them used it, which it was not.
          await utimes(directory, usedAt / 1000, usedAt / 1000);
        });
        return true;
      }),

    list,

    evict: Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const held = [...(yield* list)].sort((a, b) => a.usedAt - b.usedAt);
      let total = held.reduce((sum, { bytes }) => sum + bytes, 0);
      const evicted: CachedWorkspace[] = [];
      for (const cached of held) {
        if (total <= budget) break;
        if (now - cached.usedAt < graceMs) continue;
        yield* remove(cached);
        total -= cached.bytes;
        evicted.push(cached);
      }
      return evicted;
    }),

    room: (bytes, keep) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        let available = yield* attempt(() => free(root));
        const evicted: CachedWorkspace[] = [];
        const evictable = [...(yield* list)]
          .filter((cached) => (cached.environment !== keep.environment || cached.workspace !== keep.workspace) && now - cached.usedAt >= graceMs)
          .sort((a, b) => a.usedAt - b.usedAt);
        for (const cached of evictable) {
          if (available >= bytes + RESERVE_BYTES) break;
          yield* remove(cached);
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
          const layers = join(workspaceDir(environment, workspace), LAYERS_DIR);
          const entries = await readdir(layers).catch(() => [] as string[]);
          await Promise.all(entries.filter((name) => name.startsWith(".")).map((name) => rm(join(layers, name), { recursive: true, force: true })));
        }
      }
    }),
  };
}
