/**
 * The node's snapshot cache: the images of the snapshots taken or prepared on this node, by
 * workspace, each image once, by its digest, as runsc names it, in
 * `<root>/<environment>/<workspace>/layers/<digest>/`; and, for a snapshot prepared for a
 * restore on this node, `<root>/<environment>/<workspace>/<snapshot>/`, where the restore
 * reads it: its image's files, linked to the cache's, and in its `layers/<digest>/` each
 * image it holds pages of besides its own, its layers, linked to the cache's too, as runsc
 * finds an image's layers; or, for one read from the bucket, the checkpoint gofer's options
 * file alone, until its restore has read it, and which runsc describes it from a copy of, in
 * `<root>/.inspect/`, apart from every workspace, so that no clear or eviction of the
 * workspace removes it while runsc reads it (remoteCopy), as runsc verifies a chain of images
 * the cache stages from a copy of its own there (chainCopy). An image is written in a directory
 * of its own beside the layers, whose name begins with ".", and moved among them once it is
 * whole and its digest known, so what the cache holds is always whole; what a stopped agent
 * left half written is removed as the next one starts.
 *
 * Beside the images, in `<root>/<environment>/<workspace>/stamps/<digest>.json`, each
 * image's stamp: its files as they were checked as they were written, each by its SHA-256,
 * and its size, inode and modification time then. An image whose files are of those still
 * is taken as the one checked, without reading it again (stamped). An image whose pages file
 * a restore from the bucket wrote as it read it (stagePages) holds only the pages one image
 * refers to, which its stamp names (pagesFor): its pages file was checked page by page, not
 * by its SHA-256, and its other files are whole. A modification time is of
 * the filesystem's granularity, so a file put in its place of the same size, on an inode used
 * again, within one tick of it, is not seen: what holds the image whole there is runsc's
 * verify of it as it is staged or prepared and the SHA-256 of each file as it is downloaded.
 *
 * The cache holds at most `budget` bytes, its least recently used workspaces evicted whole
 * first, but for any used within `grace`, which a restore may still be reading, and any its
 * caller spares, as the agent spares those an operation of its runs on. When a workspace was
 * last used is its directory's modification time, so it outlasts the agent; a workspace is
 * used as an image is staged in it, as it is committed, and as a snapshot of it is prepared.
 * Whatever the budget, nothing is written to it that would leave the node's disk with less
 * than RESERVE_BYTES free: room is made first, workspaces evicted as they would be over the
 * budget, and what it cannot be made for is refused.
 */
import { link, mkdir, mkdtemp, readdir, readFile, rename, rm, stat, statfs, symlink, truncate, utimes, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";

import { Clock, Effect, Schema, type Scope } from "effect";

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

/** The directory, under the cache's root and of no workspace, of the copies of options runsc inspects a snapshot in the bucket by (remoteCopy), and of the chains it verifies as the cache stages them (chainCopy): its name begins with ".", as no workspace's environment does. */
const INSPECT_DIR = ".inspect";

/** The directory of a workspace's images' stamps, each named by its image's digest. */
export const STAMPS_DIR = "stamps";

/** A file of an image as it was checked: its path in the image, its size, and its SHA-256, in hex. */
export interface CheckedFile {
  readonly path: string;
  readonly bytes: number;
  readonly sha256: string;
}

/**
 * An image's stamp, as the cache keeps it: each of its files as checked, and its inode and
 * modification time then, in nanoseconds, in decimal; and, for an image whose pages file holds
 * only the pages one image refers to, that image's digest.
 */
const Stamp = Schema.fromJsonString(Schema.Struct({
  files: Schema.Array(Schema.Struct({ path: Schema.String, bytes: Schema.Number, sha256: Schema.String, inode: Schema.String, modifiedNs: Schema.String })),
  pagesFor: Schema.optional(Schema.String),
}));

/**
 * An image as its stamp records it: its files as checked, each as its manifest says it, and,
 * where its pages file holds only the pages the image `pagesFor` refers to, as a restore from
 * the bucket wrote it, that image's digest; null for a whole copy.
 */
export interface StampedImage {
  readonly files: readonly CheckedFile[];
  readonly pagesFor: string | null;
}

/** The name of an image's pages file, as runsc names it. */
export const PAGES_FILE = "pages.img";

/** The bytes of a page of a pages file, as the checkpoint gofer's marks count them. */
export const PAGE_BYTES = 4096;

/** The directory, in a snapshot's directory, of the pages files a restore from the bucket writes as it reads them (stagePages). */
export const PAGES_CACHE_DIR = "pages-cache";

/** The suffix of the marks the checkpoint gofer writes beside a pages file it wrote (pagesMarks). */
const MARKS_SUFFIX = ".marks";

/** The marks' magic, and the bytes of their header: magic, version, flags, pages, digest. */
const MARKS_MAGIC = "gvcmarks";
const MARKS_HEADER_BYTES = MARKS_MAGIC.length + 4 + 4 + 8 + 32;

/**
 * What the checkpoint gofer says of a pages file it wrote as its restore read it, in its marks'
 * header: their version, whether every page its image refers to is checked, how many pages
 * the file holds, and the digest of the image whose page hashes they were checked against.
 */
export interface PagesMarks {
  readonly version: number;
  readonly complete: boolean;
  readonly pages: number;
  readonly digest: string;
}

/**
 * A pages file staged for a restore from the bucket to write as it reads it (stagePages): its
 * directory, of its own, which the image is committed from once whole, and the path, in the
 * snapshot's PAGES_CACHE_DIR, linked to its pages file, where the checkpoint gofer writes it.
 */
export interface StagedPages {
  readonly environment: string;
  readonly workspace: string;
  readonly digest: string;
  readonly directory: string;
  readonly link: string;
}

/** A restore's own name for what it stages (stagePages), as the agent makes one: 16 hex digits. */
export const STAGING_NONCE = /^[0-9a-f]{16}$/u;

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

/** Whether the cache is to spare a workspace, whatever its budget, as one being written or read is. */
export type Spared = (workspace: { readonly environment: string; readonly workspace: string }) => boolean;

/** The node's snapshot cache; see makeSnapshotCache. */
export interface SnapshotCache {
  /** The directory of the workspace's images, where the agent keeps beside them what they are. */
  readonly workspace: (environment: string, workspace: string) => string;
  /** Where the image `digest` of the workspace is once it is whole. */
  readonly image: (environment: string, workspace: string, digest: string) => string;
  /** Where an image is written until it is whole and its digest known, `name` the snapshot it is of, emptied first. The workspace is used now. */
  readonly stage: (environment: string, workspace: string, name: string) => Effect.Effect<string, CacheError>;
  /** Removes the image staged as `name`, as a save that failed left it. */
  readonly unstage: (environment: string, workspace: string, name: string) => Effect.Effect<void, CacheError>;
  /**
   * Moves the image staged as `name` where an image of `digest` is, the workspace used now; one
   * there already is kept, the staged one removed: whether it moved the staged one.
   */
  readonly commit: (environment: string, workspace: string, name: string, digest: string) => Effect.Effect<boolean, CacheError>;
  /**
   * Records the image `digest` of the workspace as `files`, its files as they were checked
   * as they were written: each by its SHA-256, and its size, inode and modification time now;
   * and, where given, `pagesFor`, the image whose referenced pages alone its pages file holds.
   */
  readonly stamp: (environment: string, workspace: string, digest: string, files: readonly CheckedFile[], pagesFor?: string) => Effect.Effect<void, CacheError>;
  /**
   * The image `digest` of the workspace as its stamp records it, if its files are the files it
   * holds, each of the size, inode and modification time recorded; null where it has no stamp,
   * or one of them has changed since, as what changes a file does. None of its files is read.
   */
  readonly stamped: (environment: string, workspace: string, digest: string) => Effect.Effect<StampedImage | null, CacheError>;
  /**
   * Stages the pages file of the image `digest` of the workspace, `bytes` long and sparse, for
   * the restore of the snapshot prepared from the bucket (prepareRemote, which it follows), named
   * `nonce` by it, to write as it reads it: in a directory of its own, whose name begins with
   * ".", linked into the snapshot's PAGES_CACHE_DIR at the path its checkpoint gofer reads it
   * at, the snapshot's own image's (`top`) or a layer's.
   */
  readonly stagePages: (environment: string, workspace: string, snapshot: string, nonce: string, digest: string, bytes: number, top: boolean) => Effect.Effect<StagedPages, CacheError>;
  /**
   * The marks the checkpoint gofer wrote of `staged`, their header, where the file it wrote them
   * of is `staged`'s (of its inode): null where there are none, as a restore that did not end
   * leaves, or of another file, as another restore of the snapshot prepared since stages.
   */
  readonly pagesMarks: (staged: StagedPages) => Effect.Effect<PagesMarks | null, CacheError>;
  /** Removes `staged`, both its names: its link only where it is still `staged`'s. */
  readonly unstagePages: (staged: StagedPages) => Effect.Effect<void, CacheError>;
  /**
   * Moves `staged`'s directory, the image it holds whole but its pages file, among the
   * workspace's images, as commit does, once its link and marks are removed: whether it moved
   * it, one there already kept.
   */
  readonly commitPages: (staged: StagedPages) => Effect.Effect<boolean, CacheError>;
  /** Whether the image `digest` of the workspace is whole in the cache. */
  readonly holds: (environment: string, workspace: string, digest: string) => Effect.Effect<boolean, CacheError>;
  /**
   * The directory a restore of the snapshot reads, made anew: the files of the image
   * `digests[0]`, then each of the others as its layer, every one of which the cache must
   * hold. The workspace is used now.
   */
  readonly prepare: (environment: string, workspace: string, snapshot: string, digests: readonly string[]) => Effect.Effect<string, CacheError>;
  /** Removes the image `digest` of the workspace, as one that is not what its manifest says, and nothing else of it. */
  readonly drop: (environment: string, workspace: string, digest: string) => Effect.Effect<void, CacheError>;
  /**
   * The directory a restore of the snapshot reads from the bucket, made anew: the checkpoint
   * gofer's options file `name` alone, holding `options`, which name the agent's proxy of the
   * bucket and the restore's token for it, readable and writable by its owner only, as the
   * gofer requires of a file of credentials.
   * The workspace is used now.
   */
  readonly prepareRemote: (environment: string, workspace: string, snapshot: string, name: string, options: string) => Effect.Effect<string, CacheError>;
  /** Removes the options file `name` of each snapshot prepared of the workspace from the bucket, as prepareRemote wrote it. */
  readonly unprepareRemote: (environment: string, workspace: string, name: string) => Effect.Effect<void, CacheError>;
  /**
   * A directory of its own holding the checkpoint gofer's options file `name`, holding
   * `options`, as prepareRemote writes it, from which runsc reads the snapshot from the bucket
   * as its restore does, whatever becomes of the restore's directory meanwhile: its options
   * removed once its restore has read them, or the directory made anew for the next restore,
   * or the workspace cleared or evicted whole. Apart from every workspace (INSPECT_DIR); one a
   * stopped agent left is removed as the next starts; removed as the scope ends.
   */
  readonly remoteCopy: (environment: string, workspace: string, snapshot: string, name: string, options: string) => Effect.Effect<string, CacheError, Scope.Scope>;
  /**
   * A directory of its own holding the chain of images `digests` of the workspace as prepare
   * makes one, linked to the cache's, every one of which the cache must hold: what runsc
   * verifies an image by as the cache stages it, apart from every workspace (INSPECT_DIR), so
   * that no restore's directory is touched; one a stopped agent left is removed as the next
   * starts; removed as the scope ends. The workspace is not used by it.
   */
  readonly chainCopy: (environment: string, workspace: string, digests: readonly string[]) => Effect.Effect<string, CacheError, Scope.Scope>;
  /** Removes everything of the workspace. */
  readonly clear: (environment: string, workspace: string) => Effect.Effect<void, CacheError>;
  /**
   * Removes the workspace's images but `keep`, and the snapshots prepared of it, if it was
   * not used within the grace, as a restore may still be reading it: whether it did.
   */
  readonly discard: (environment: string, workspace: string, keep: readonly string[]) => Effect.Effect<boolean, CacheError>;
  /** The workspaces the cache holds images of. */
  readonly list: Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /** Evicts workspaces, least recently used first, but those `spared` says to spare, until the cache is within its budget: those evicted. */
  readonly evict: (spared: Spared) => Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /**
   * Makes room on the disk for `bytes` more, RESERVE_BYTES left beside them, evicting
   * workspaces as `evict` does: those evicted. Fails, saying so, where it cannot.
   */
  readonly room: (bytes: number, spared: Spared) => Effect.Effect<readonly CachedWorkspace[], CacheError>;
  /** Removes what was left half written, the pages files staged for restores (stagePages), and the copies left (remoteCopy, chainCopy). */
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

/** The directory a restore named `nonce` stages the pages file of the image `digest` of the snapshot in (stagePages). */
const stagedPages = (snapshot: string, nonce: string, digest: string): string => `.pages-${snapshot}-${nonce}-${digest}`;

/** The names of the directories in `directory`, but those being written; none when it is not there. */
async function directories(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return [];
    throw error;
  });
  return entries.filter((entry) => entry.isDirectory() && !entry.name.startsWith(".")).map((entry) => entry.name).sort();
}

/** `found`, or null where what it was asked of is gone, as what the agent removes meanwhile is. */
async function unlessGone<A>(found: Promise<A>): Promise<A | null> {
  return found.catch((error: NodeJS.ErrnoException) => {
    if (error.code === "ENOENT") return null;
    throw error;
  });
}

/**
 * The bytes of the files under `directory`, each once however many links it has there, but
 * those being written; what is removed as it is walked, as the agent moves an image or
 * removes a workspace's meanwhile, counts for nothing.
 */
async function size(directory: string): Promise<number> {
  const seen = new Set<number>();
  let bytes = 0;
  const walk = async (under: string): Promise<void> => {
    for (const entry of (await unlessGone(readdir(under, { withFileTypes: true }))) ?? []) {
      const path = join(under, entry.name);
      if (entry.isDirectory()) {
        if (!entry.name.startsWith(".")) await walk(path);
        continue;
      }
      if (!entry.isFile()) continue;
      const found = await unlessGone(stat(path));
      if (found === null || seen.has(found.ino)) continue;
      seen.add(found.ino);
      bytes += found.size;
    }
  };
  await walk(directory);
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

  const stampOf = (environment: string, workspace: string, digest: string): string => {
    checked(environment, workspace, null, [digest]);
    return join(workspaceDir(environment, workspace), STAMPS_DIR, `${digest}.json`);
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
        const bytes = await size(directory);
        // One removed meanwhile, as an eviction removes it, holds nothing.
        const used = await unlessGone(stat(directory));
        if (used !== null) found.push({ environment, workspace, images, bytes, usedAt: used.mtimeMs });
      }
    }
    return found;
  });

  /**
   * Makes `directory` the chain of images `digests` of the workspace, as a restore reads one: the
   * files of the first, linked to the cache's, and in its `layers/` each of the others, a link
   * to the cache's.
   */
  const linkChain = async (directory: string, environment: string, workspace: string, digests: readonly string[]): Promise<void> => {
    const [own, ...layers] = digests;
    await mkdir(join(directory, LAYERS_DIR), { recursive: true });
    const ownDir = image(environment, workspace, own ?? "");
    for (const entry of await readdir(ownDir, { withFileTypes: true })) {
      if (entry.isFile()) await link(join(ownDir, entry.name), join(directory, entry.name));
    }
    // Relative, so the link holds wherever the cache is mounted.
    for (const digest of layers) await symlink(relative(join(directory, LAYERS_DIR), image(environment, workspace, digest)), join(directory, LAYERS_DIR, digest));
  };

  /**
   * Moves the image written in `from` where an image of `digest` is, the workspace used now; one
   * there already is kept, `from` removed: whether it moved it.
   */
  const moveIn = (environment: string, workspace: string, from: string, digest: string): Effect.Effect<boolean, CacheError> =>
    holds(environment, workspace, digest).pipe(
      Effect.flatMap((held) => attempt(async () => {
        if (held) await rm(from, { recursive: true, force: true });
        else await rename(from, image(environment, workspace, digest));
        return !held;
      })),
      Effect.tap(() => use(environment, workspace)),
    );

  /** Removes the link of `staged` and the marks beside it, where it is still `staged`'s, as another restore's may be in its place. */
  const unlinkStaged = async ({ directory, link: linked }: StagedPages): Promise<void> => {
    const [at, own] = await Promise.all([unlessGone(stat(linked)), unlessGone(stat(join(directory, PAGES_FILE)))]);
    if (at === null || own === null || at.ino !== own.ino || at.dev !== own.dev) return;
    await rm(linked, { force: true });
    await rm(`${linked}${MARKS_SUFFIX}`, { force: true });
  };

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
      }).pipe(Effect.andThen(use(environment, workspace)), Effect.as(directory));
    },

    unstage: (environment, workspace, name) => {
      checked(environment, workspace, name);
      return attempt(() => rm(join(workspaceDir(environment, workspace), LAYERS_DIR, staged(name)), { recursive: true, force: true }));
    },

    commit: (environment, workspace, name, digest) => {
      checked(environment, workspace, name, [digest]);
      return moveIn(environment, workspace, join(workspaceDir(environment, workspace), LAYERS_DIR, staged(name)), digest);
    },

    stamp: (environment, workspace, digest, files, pagesFor) =>
      attempt(async () => {
        const directory = image(environment, workspace, digest);
        const stamped = await Promise.all(files.map(async (file) => {
          const { ino, mtimeNs } = await stat(join(directory, file.path), { bigint: true });
          return { ...file, inode: String(ino), modifiedNs: String(mtimeNs) };
        }));
        const path = stampOf(environment, workspace, digest);
        // Written beside it, then moved in, so a stamp is always whole.
        const writing = join(workspaceDir(environment, workspace), STAMPS_DIR, `.${digest}.json`);
        await mkdir(join(workspaceDir(environment, workspace), STAMPS_DIR), { recursive: true });
        await writeFile(writing, JSON.stringify({ files: stamped, ...(pagesFor === undefined ? {} : { pagesFor }) }));
        await rename(writing, path);
      }),

    stamped: (environment, workspace, digest) =>
      attempt(async () => {
        const text = await readFile(stampOf(environment, workspace, digest), "utf8").catch(() => null);
        const decoded = text === null ? null : Schema.decodeUnknownOption(Stamp)(text);
        if (decoded === null || decoded._tag === "None") return null;
        const { files, pagesFor } = decoded.value;
        const directory = image(environment, workspace, digest);
        // The files it holds, every one of them the stamp's.
        const held = (await readdir(directory, { withFileTypes: true, recursive: true }).catch(() => []))
          .filter((entry) => !entry.isDirectory())
          .map((entry) => relative(directory, join(entry.parentPath, entry.name)))
          .sort();
        if (JSON.stringify(held) !== JSON.stringify(files.map(({ path }) => path).toSorted())) return null;
        for (const { path, bytes, inode, modifiedNs } of files) {
          const found = await stat(join(directory, path), { bigint: true }).catch(() => null);
          if (found === null || found.size !== BigInt(bytes) || String(found.ino) !== inode || String(found.mtimeNs) !== modifiedNs) return null;
        }
        return { files: files.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })), pagesFor: pagesFor ?? null };
      }),

    stagePages: (environment, workspace, snapshot, nonce, digest, bytes, top) => {
      checked(environment, workspace, snapshot, [digest]);
      if (!STAGING_NONCE.test(nonce)) throw new Error(`the snapshot cache stages no pages as ${JSON.stringify(nonce)}`);
      const directory = join(workspaceDir(environment, workspace), LAYERS_DIR, stagedPages(snapshot, nonce, digest));
      // Where the checkpoint gofer reads it, as runsc names a layer's files.
      const servedAt = join(workspaceDir(environment, workspace), snapshot, PAGES_CACHE_DIR, ...(top ? [] : [LAYERS_DIR, digest]), PAGES_FILE);
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await mkdir(directory, { recursive: true });
        await writeFile(join(directory, PAGES_FILE), "", { flag: "wx" });
        await truncate(join(directory, PAGES_FILE), bytes);
        await mkdir(dirname(servedAt), { recursive: true });
        // A hard link: the gofer opens nothing through a symbolic one.
        await link(join(directory, PAGES_FILE), servedAt);
      }).pipe(Effect.andThen(use(environment, workspace)), Effect.as({ environment, workspace, digest, directory, link: servedAt }));
    },

    pagesMarks: ({ directory, link }) =>
      attempt(async () => {
        const [linked, own] = await Promise.all([unlessGone(stat(link, { bigint: true })), unlessGone(stat(join(directory, PAGES_FILE), { bigint: true }))]);
        if (linked === null || own === null || linked.ino !== own.ino || linked.dev !== own.dev) return null;
        const header = await unlessGone(readFile(`${link}${MARKS_SUFFIX}`));
        if (header === null || header.length < MARKS_HEADER_BYTES || header.toString("latin1", 0, MARKS_MAGIC.length) !== MARKS_MAGIC) return null;
        let at = MARKS_MAGIC.length;
        const version = header.readUInt32LE(at);
        const flags = header.readUInt32LE((at += 4));
        const pages = Number(header.readBigUInt64LE((at += 4)));
        const digest = header.toString("hex", (at += 8), at + 32);
        return { version, complete: (flags & 1) === 1, pages, digest };
      }),

    unstagePages: (staged) =>
      attempt(async () => {
        await unlinkStaged(staged);
        await rm(staged.directory, { recursive: true, force: true });
      }),

    commitPages: (staged) => attempt(() => unlinkStaged(staged)).pipe(Effect.andThen(moveIn(staged.environment, staged.workspace, staged.directory, staged.digest))),

    holds,

    prepare: (environment, workspace, snapshot, digests) => {
      checked(environment, workspace, snapshot, digests);
      const directory = join(workspaceDir(environment, workspace), snapshot);
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await linkChain(directory, environment, workspace, digests);
      }).pipe(Effect.andThen(use(environment, workspace)), Effect.as(directory));
    },

    drop: (environment, workspace, digest) =>
      attempt(async () => {
        await rm(stampOf(environment, workspace, digest), { force: true });
        await rm(image(environment, workspace, digest), { recursive: true, force: true });
      }),
    prepareRemote: (environment, workspace, snapshot, name, options) => {
      checked(environment, workspace, snapshot);
      if (!/^[a-z0-9_]+\.json$/u.test(name)) throw new Error(`the snapshot cache writes no options file ${JSON.stringify(name)}`);
      const directory = join(workspaceDir(environment, workspace), snapshot);
      return attempt(async () => {
        await rm(directory, { recursive: true, force: true });
        await mkdir(directory, { recursive: true, mode: 0o700 });
        // Owner-only from its making: it holds the restore's token.
        await writeFile(join(directory, name), options, { mode: 0o600, flag: "wx" });
      }).pipe(Effect.andThen(use(environment, workspace)), Effect.as(directory));
    },

    remoteCopy: (environment, workspace, snapshot, name, options) => {
      checked(environment, workspace, snapshot);
      if (!/^[a-z0-9_]+\.json$/u.test(name)) throw new Error(`the snapshot cache writes no options file ${JSON.stringify(name)}`);
      const copies = join(root, INSPECT_DIR);
      return Effect.acquireRelease(
        attempt(async () => {
          await mkdir(copies, { recursive: true, mode: 0o700 });
          // One of its own however many are made of the snapshot at once, owner-only from its making.
          const directory = await mkdtemp(join(copies, `${environment}.${workspace}.${snapshot}-`));
          await writeFile(join(directory, name), options, { mode: 0o600, flag: "wx" });
          return directory;
        }),
        (directory) => attempt(() => rm(directory, { recursive: true, force: true })).pipe(Effect.ignore),
      );
    },

    chainCopy: (environment, workspace, digests) => {
      checked(environment, workspace, null, digests);
      const copies = join(root, INSPECT_DIR);
      return Effect.acquireRelease(
        attempt(async () => {
          await mkdir(copies, { recursive: true, mode: 0o700 });
          const directory = await mkdtemp(join(copies, `${environment}.${workspace}.chain-`));
          await linkChain(directory, environment, workspace, digests);
          return directory;
        }),
        (directory) => attempt(() => rm(directory, { recursive: true, force: true })).pipe(Effect.ignore),
      );
    },

    unprepareRemote: (environment, workspace, name) => {
      if (!/^[a-z0-9_]+\.json$/u.test(name)) throw new Error(`the snapshot cache writes no options file ${JSON.stringify(name)}`);
      const directory = workspaceDir(environment, workspace);
      return attempt(async () => {
        for (const snapshot of (await directories(directory)).filter((each) => SNAPSHOT.test(each))) await rm(join(directory, snapshot, name), { force: true });
      });
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
            if (keep.includes(digest)) continue;
            await rm(join(directory, STAMPS_DIR, `${digest}.json`), { force: true });
            await rm(join(directory, LAYERS_DIR, digest), { recursive: true, force: true });
          }
          // Removing them used it, which it was not.
          await utimes(directory, usedAt / 1000, usedAt / 1000);
        });
        return true;
      }),

    list,

    evict: (spared) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        const held = [...(yield* list)].sort((a, b) => a.usedAt - b.usedAt);
        let total = held.reduce((sum, { bytes }) => sum + bytes, 0);
        const evicted: CachedWorkspace[] = [];
        for (const cached of held) {
          if (total <= budget) break;
          if (now - cached.usedAt < graceMs || spared(cached)) continue;
          yield* remove(cached);
          total -= cached.bytes;
          evicted.push(cached);
        }
        return evicted;
      }),

    room: (bytes, spared) =>
      Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        let available = yield* attempt(() => free(root));
        const evicted: CachedWorkspace[] = [];
        const evictable = [...(yield* list)]
          .filter((cached) => !spared(cached) && now - cached.usedAt >= graceMs)
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
      await rm(join(root, INSPECT_DIR), { recursive: true, force: true });
      for (const environment of (await directories(root)).filter((name) => NAME.test(name))) {
        for (const workspace of (await directories(join(root, environment))).filter((name) => NAME.test(name))) {
          for (const under of [LAYERS_DIR, STAMPS_DIR]) {
            const directory = join(workspaceDir(environment, workspace), under);
            const entries = await readdir(directory).catch(() => [] as string[]);
            await Promise.all(entries.filter((name) => name.startsWith(".")).map((name) => rm(join(directory, name), { recursive: true, force: true })));
          }
          // The pages files staged for restores (stagePages), which no fetch of this agent's will commit.
          for (const snapshot of (await directories(workspaceDir(environment, workspace))).filter((name) => SNAPSHOT.test(name))) {
            await rm(join(workspaceDir(environment, workspace), snapshot, PAGES_CACHE_DIR), { recursive: true, force: true });
          }
        }
      }
    }),
  };
}
