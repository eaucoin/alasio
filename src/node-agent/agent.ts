/**
 * What a node agent does with snapshots: whole-sandbox gVisor checkpoints of session
 * pods on its node, kept in the snapshots bucket and in the node's cache, one per
 * workspace, the latest. A workspace is one session's, of one environment (main, or a
 * branch environment), and its snapshot is `<environment>/<workspace>/` in the bucket:
 *
 * - `<snapshot>/<file>`: the image's files, as runsc wrote them;
 * - `<snapshot>/manifest.json`: what the snapshot is (Manifest), written after its files;
 * - `current.json`: the manifest of the workspace's current snapshot, written last, which
 *   commits it. A workspace without one has no snapshot.
 *
 * A checkpoint ends the sandbox it saves. Once it has begun, the workspace's current
 * snapshot no longer holds what the workspace is, whose files and processes have moved
 * on since it was taken, so a checkpoint that fails forgets it rather than leave it to be
 * restored. A copy gives a workspace another's current snapshot, as a session forked
 * from another restores what the other was: its files copied in the bucket under the
 * same snapshot id, then its manifest, then its commit, as a checkpoint writes them. One
 * operation runs on a workspace at a time.
 */
import { createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readdir, stat, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { pipeline } from "node:stream/promises";

import { Clock, Effect, RcMap, Schema, type Scope, Semaphore } from "effect";

import { meter, withAlasioSpan } from "../telemetry/index.ts";
import { withLogScope } from "../shared/log.ts";
import { CacheError, type SnapshotCache } from "./cache.ts";
import { runscVersion } from "./names.ts";
import type { ReleaseNotInstalled, Runsc, RunscError } from "./runsc.ts";
import type { S3Bucket, S3Error } from "./s3.ts";

/** An environment's or a workspace's name: a DNS label, as their namespaces' and Sandboxes' names are. */
const Name = Schema.String.check(Schema.isPattern(/^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u));

/** A file of a snapshot's image: its path in the image, its size and its SHA-256, in hex. */
const SnapshotFile = Schema.Struct({ path: Schema.String, bytes: Schema.Number, sha256: Schema.String });
export type SnapshotFile = typeof SnapshotFile.Type;

/** What a snapshot is, as the bucket and the cache keep it beside its image. */
export const Manifest = Schema.Struct({
  environment: Schema.String,
  workspace: Schema.String,
  snapshot: Schema.String,
  /** The gVisor release that saved it, the only one that restores it. */
  release: Schema.String,
  /** The image's identity, as runsc gives it. */
  image: Schema.String,
  /** The CPU features the sandbox was saved with, which a node restoring it must have. */
  cpuFeatures: Schema.Array(Schema.String),
  /** The pod template the sandbox ran, as alasio gave it, which its restore runs again. */
  podTemplate: Schema.Record(Schema.String, Schema.Unknown),
  files: Schema.Array(SnapshotFile),
  bytes: Schema.Number,
  takenAt: Schema.String,
  /** How long runsc took to checkpoint the sandbox: its hook run in it, then its state saved, the sandbox paused. */
  pauseMs: Schema.Number,
  /** How long its files took to upload. */
  uploadMs: Schema.Number,
});
export type Manifest = typeof Manifest.Type;

/** A workspace, as each request names it. */
export const WorkspaceRef = Schema.Struct({ environment: Name, workspace: Name });
export type WorkspaceRef = typeof WorkspaceRef.Type;

/** A request to copy a workspace's current snapshot to another. */
export const CopyRequest = Schema.Struct({ from: WorkspaceRef, to: WorkspaceRef });
export type CopyRequest = typeof CopyRequest.Type;

/** A request to checkpoint a workspace's sandbox. */
export const CheckpointRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The gVisor release the sandbox runs under. */
  release: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u)),
  /** The container whose sandbox is saved, by containerd's id, in which the hook runs. */
  containerId: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u)),
  /** The program run in the container before the save: its arguments, none holding whitespace, as runsc splits them at spaces. */
  hook: Schema.Array(Schema.String.check(Schema.isPattern(/^\S+$/u))).check(Schema.isMinLength(1)),
  podTemplate: Schema.Record(Schema.String, Schema.Unknown),
});
export type CheckpointRequest = typeof CheckpointRequest.Type;

/** A snapshot prepared on this node: what it is, and where its image is. */
export interface Prepared {
  readonly manifest: Manifest;
  readonly imagePath: string;
}

/** What this node holds: its gVisor releases with the CPU features each sees here, and the snapshots in its cache. */
export interface Holdings {
  readonly node: string;
  readonly releases: readonly { readonly release: string; readonly cpuFeatures: readonly string[] }[];
  readonly cached: readonly { readonly environment: string; readonly workspace: string; readonly snapshot: string; readonly bytes: number; readonly usedAt: string }[];
}

/** A workspace has no current snapshot. */
export class NoSnapshot extends Schema.TaggedError<NoSnapshot>()("NoSnapshot", {
  environment: Schema.String,
  workspace: Schema.String,
}) {
  override get message(): string {
    return `${this.environment}/${this.workspace} has no snapshot`;
  }
}

/** This node cannot restore a workspace's snapshot: why. */
export class Unrestorable extends Schema.TaggedError<Unrestorable>()("Unrestorable", {
  environment: Schema.String,
  workspace: Schema.String,
  snapshot: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `this node cannot restore the snapshot ${this.snapshot} of ${this.environment}/${this.workspace}: ${this.reason}`;
  }
}

/** A workspace's snapshot is not what its manifest says: which part, and how. */
export class SnapshotCorrupt extends Schema.TaggedError<SnapshotCorrupt>()("SnapshotCorrupt", {
  environment: Schema.String,
  workspace: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the snapshot of ${this.environment}/${this.workspace} is corrupt: ${this.reason}`;
  }
}

/** A checkpoint failed: why, and what became of the workspace's snapshot. */
export class CheckpointFailed extends Schema.TaggedError<CheckpointFailed>()("CheckpointFailed", {
  environment: Schema.String,
  workspace: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `checkpointing ${this.environment}/${this.workspace} failed: ${this.reason}`;
  }
}

/** How preparing a snapshot fails. */
export type PrepareError = NoSnapshot | Unrestorable | SnapshotCorrupt | S3Error | CacheError | RunscError;

/** What a node agent does; see makeNodeAgent. */
export interface NodeAgent {
  /** Checkpoints a workspace's sandbox and makes it the workspace's snapshot: its manifest. */
  readonly checkpoint: (request: CheckpointRequest) => Effect.Effect<Manifest, CheckpointFailed>;
  /** The workspace's current snapshot in this node's cache, checked against its manifest, for a restore on this node. */
  readonly prepare: (workspace: WorkspaceRef) => Effect.Effect<Prepared, PrepareError>;
  /** The manifest of the workspace's current snapshot, or null. */
  readonly current: (workspace: WorkspaceRef) => Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt>;
  /** Makes the current snapshot of `from` that of `to` too: `to`'s manifest. One that fails as it writes leaves `to` without a snapshot. */
  readonly copy: (from: WorkspaceRef, to: WorkspaceRef) => Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError>;
  /** Forgets the workspace's snapshot, in the bucket and in the cache. */
  readonly forget: (workspace: WorkspaceRef) => Effect.Effect<void, S3Error | CacheError>;
  readonly holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError>;
}

/** What makeNodeAgent is given: this node's name, its releases, its cache, and the bucket. */
export interface NodeAgentOptions {
  readonly node: string;
  readonly runsc: Runsc;
  readonly cache: SnapshotCache;
  readonly bucket: S3Bucket;
}

/** The file of a snapshot's manifest, in the bucket and in the cache. */
export const MANIFEST_FILE = "manifest.json";
/** The object of a workspace's current snapshot. */
export const CURRENT_FILE = "current.json";
/** How long the hook may take to ready the sandbox for its save. */
const HOOK_TIMEOUT = "30s";

const checkpoints = meter.createCounter("alasio.node_agent.checkpoints", {
  description: "Whole-sandbox checkpoints taken by the node agent, by outcome",
  unit: "{checkpoint}",
});
const pauses = meter.createHistogram("alasio.node_agent.checkpoint.pause", {
  description: "How long a checkpoint held its sandbox: from the hook's start to the sandbox saved",
  unit: "s",
});
const uploads = meter.createHistogram("alasio.node_agent.checkpoint.upload", {
  description: "How long a checkpoint's image took to upload to the snapshots bucket",
  unit: "s",
});
const sizes = meter.createHistogram("alasio.node_agent.snapshot.size", {
  description: "The size of a checkpoint's image",
  unit: "By",
});
const copies = meter.createCounter("alasio.node_agent.copies", {
  description: "Snapshots copied from one workspace to another by the node agent, by outcome",
  unit: "{copy}",
});
const prepares = meter.createCounter("alasio.node_agent.prepares", {
  description: "Snapshots prepared for a restore by the node agent, by where their image came from, or why not",
  unit: "{prepare}",
});

/** A snapshot's id: when it was taken, sortable, and a random part. */
function snapshotId(takenAt: number): string {
  return `${new Date(takenAt).toISOString().replace(/[-:]|\.\d{3}/gu, "")}-${randomBytes(4).toString("hex")}`;
}

/** The SHA-256 of a file, in hex. */
async function fileSha256(path: string): Promise<string> {
  const hash = createHash("sha256");
  await pipeline(createReadStream(path), hash);
  return hash.digest("hex");
}

/** The files of an image directory, each with its size and SHA-256, by their paths in it, in order. */
async function imageFiles(directory: string): Promise<SnapshotFile[]> {
  const entries = await readdir(directory, { withFileTypes: true, recursive: true });
  const paths = entries.filter((entry) => entry.isFile()).map((entry) => relative(directory, join(entry.parentPath, entry.name))).sort();
  return Promise.all(paths.map(async (path) => {
    const file = join(directory, path);
    return { path, bytes: (await stat(file)).size, sha256: await fileSha256(file) };
  }));
}

/** `run`, on the node's disk, failing as the cache does. */
const onDisk = <A>(run: () => Promise<A>): Effect.Effect<A, CacheError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new CacheError({ reason: cause instanceof Error ? cause.message : String(cause) }) });

/** The node agent of `options`, whose operations on a workspace run one at a time while the scope lasts. */
export const makeNodeAgent = Effect.fnUntraced(function*({ node, runsc, cache, bucket }: NodeAgentOptions): Effect.fn.Return<NodeAgent, never, Scope.Scope> {
  const locks = yield* RcMap.make({ lookup: (_workspace: string) => Semaphore.make(1) });

  const prefixOf = ({ environment, workspace }: WorkspaceRef): string => `${environment}/${workspace}`;

  /** `effect` once no other operation runs on the workspaces, each held in the order of its name, so that two operations never wait on each other. */
  const exclusive = <A, E>(workspaces: readonly WorkspaceRef[], effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.scoped(
      [...new Set(workspaces.map(prefixOf))].sort().reduceRight<Effect.Effect<A, E, Scope.Scope>>(
        (held, name) => RcMap.get(locks, name).pipe(Effect.flatMap((lock) => held.pipe(Semaphore.withPermit(lock)))),
        effect,
      ),
    );

  /** How long `effect` took, in milliseconds. */
  const timed = <E>(effect: Effect.Effect<unknown, E>): Effect.Effect<number, E> =>
    Effect.gen(function*() {
      const start = yield* Clock.currentTimeMillis;
      yield* effect;
      return (yield* Clock.currentTimeMillis) - start;
    });

  const current = (ref: WorkspaceRef): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> =>
    bucket.getJson(`${prefixOf(ref)}/${CURRENT_FILE}`).pipe(
      Effect.flatMap((found) =>
        found === null
          ? Effect.succeed(null)
          : Schema.decodeUnknownEffect(Manifest)(found).pipe(
            Effect.mapError((error) => new SnapshotCorrupt({ ...ref, reason: `its ${CURRENT_FILE} is not a manifest: ${error.message}` })),
          )
      ),
    );

  /** Forgets the workspace's snapshot: the cache's copy, then in the bucket, uncommitted first and then its objects deleted. */
  const forgetNow = (ref: WorkspaceRef): Effect.Effect<void, S3Error | CacheError> =>
    Effect.gen(function*() {
      const prefix = prefixOf(ref);
      yield* cache.clear(ref.environment, ref.workspace, null);
      yield* bucket.remove(`${prefix}/${CURRENT_FILE}`);
      yield* Effect.forEach(yield* bucket.list(`${prefix}/`), bucket.remove, { discard: true });
    });

  /** Takes the snapshot and commits it, as the module says: its manifest. */
  const take = (request: CheckpointRequest): Effect.Effect<Manifest, RunscError | ReleaseNotInstalled | S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace, release, containerId, hook, podTemplate } = request;
      const takenAt = yield* Clock.currentTimeMillis;
      const snapshot = snapshotId(takenAt);
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot });
      const directory = yield* cache.stage(environment, workspace, snapshot);
      const pauseMs = yield* timed(runsc.checkpoint(release, { containerId, imagePath: directory, hook, hookTimeout: HOOK_TIMEOUT }));
      const image = yield* runsc.inspect(release, directory);
      if (image.version !== runscVersion(release)) {
        return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image says runsc ${image.version} saved it, not ${runscVersion(release)}` });
      }
      const files = yield* onDisk(() => imageFiles(directory));
      const prefix = `${prefixOf(request)}/${snapshot}`;
      const uploadMs = yield* timed(Effect.forEach(files, ({ path }) => bucket.putFile(`${prefix}/${path}`, join(directory, path)), { discard: true }));
      const manifest: Manifest = {
        environment,
        workspace,
        snapshot,
        release,
        image: image.digest,
        cpuFeatures: image.cpuFeatures,
        podTemplate,
        files,
        bytes: files.reduce((sum, { bytes }) => sum + bytes, 0),
        takenAt: new Date(takenAt).toISOString(),
        pauseMs,
        uploadMs,
      };
      yield* onDisk(() => writeFile(join(directory, MANIFEST_FILE), JSON.stringify(manifest)));
      yield* bucket.putJson(`${prefix}/${MANIFEST_FILE}`, manifest);
      yield* bucket.putJson(`${prefixOf(request)}/${CURRENT_FILE}`, manifest);
      return manifest;
    });

  /**
   * After a snapshot is committed: every other object of the workspace's deleted (its
   * previous snapshot's, and any an operation that failed left), then `cached`, which
   * makes the cache hold what it should of the workspace. What fails here leaves the
   * snapshot committed.
   */
  const settle = (manifest: Manifest, cached: Effect.Effect<void, CacheError>): Effect.Effect<void> =>
    Effect.gen(function*() {
      const prefix = prefixOf(manifest);
      const stale = (yield* bucket.list(`${prefix}/`)).filter((key) => key !== `${prefix}/${CURRENT_FILE}` && !key.startsWith(`${prefix}/${manifest.snapshot}/`));
      yield* Effect.forEach(stale, bucket.remove, { discard: true });
      yield* cached;
    }).pipe(Effect.catch((error) => Effect.logWarning(`the snapshot ${manifest.snapshot} of ${prefixOf(manifest)} is committed, but ${error.message}`)));

  const evict: Effect.Effect<void, CacheError> = cache.evict.pipe(
    Effect.flatMap((evicted) =>
      Effect.forEach(evicted, ({ environment, workspace, snapshot, bytes }) =>
        Effect.logInfo(`evicted the snapshot ${snapshot} of ${environment}/${workspace} (${bytes} bytes) from the node's cache`), { discard: true })
    ),
  );

  const checkpoint = (request: CheckpointRequest): Effect.Effect<Manifest, CheckpointFailed> => {
    const failed = (reason: string) => new CheckpointFailed({ environment: request.environment, workspace: request.workspace, reason });
    const moved = "as the workspace has moved on since it was taken";
    return exclusive(
      [request],
      take(request).pipe(
        Effect.catch((error) =>
          forgetNow(request).pipe(
            Effect.matchEffect({
              onSuccess: () => Effect.fail(failed(`${error.message}; its snapshot is forgotten, ${moved}`)),
              onFailure: (forgetting) => Effect.fail(failed(`${error.message}; forgetting its snapshot, ${moved}, failed too: ${forgetting.message}`)),
            }),
          )
        ),
        Effect.onInterrupt(() => forgetNow(request).pipe(Effect.ignore)),
        // Its image is the one the cache holds of the workspace.
        Effect.tap((manifest) =>
          settle(
            manifest,
            cache.commit(manifest.environment, manifest.workspace, manifest.snapshot).pipe(
              Effect.andThen(cache.clear(manifest.environment, manifest.workspace, manifest.snapshot)),
              Effect.andThen(evict),
            ),
          )
        ),
      ),
    ).pipe(
      Effect.tap((manifest) =>
        Effect.sync(() => {
          pauses.record(manifest.pauseMs / 1000);
          uploads.record(manifest.uploadMs / 1000);
          sizes.record(manifest.bytes);
          checkpoints.add(1, { outcome: "taken" });
        }).pipe(
          Effect.andThen(Effect.annotateCurrentSpan({ "alasio.snapshot.bytes": manifest.bytes, "alasio.snapshot.pause_ms": manifest.pauseMs, "alasio.snapshot.upload_ms": manifest.uploadMs })),
          Effect.andThen(Effect.logInfo(`took the snapshot ${manifest.snapshot} of ${prefixOf(manifest)}: ${manifest.bytes} bytes, paused ${manifest.pauseMs} ms, uploaded in ${manifest.uploadMs} ms`)),
        )
      ),
      Effect.tapError((error) => Effect.sync(() => checkpoints.add(1, { outcome: "failed" })).pipe(Effect.andThen(Effect.logWarning(error.message)))),
      withAlasioSpan("alasio.node_agent.checkpoint", { attributes: spanAttributes(request) }),
    );
  };

  /**
   * Copies the current snapshot of `from` to `to` and commits it, as the module says:
   * `to`'s manifest. Once it has begun writing, one that fails forgets `to`'s snapshot,
   * rather than leave part of one, or one `to` was to move on from.
   */
  const copyNow = (from: WorkspaceRef, to: WorkspaceRef): Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError> =>
    Effect.gen(function*() {
      const source = yield* current(from);
      if (source === null) return yield* new NoSnapshot(from);
      // A workspace's snapshot is its own already.
      if (prefixOf(from) === prefixOf(to)) return source;
      const { snapshot } = source;
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot });
      const manifest: Manifest = { ...source, environment: to.environment, workspace: to.workspace };
      yield* Effect.gen(function*() {
        yield* Effect.forEach(source.files, ({ path }) => bucket.copy(`${prefixOf(from)}/${snapshot}/${path}`, `${prefixOf(to)}/${snapshot}/${path}`), { discard: true });
        yield* bucket.putJson(`${prefixOf(to)}/${snapshot}/${MANIFEST_FILE}`, manifest);
        yield* bucket.putJson(`${prefixOf(to)}/${CURRENT_FILE}`, manifest);
      }).pipe(
        Effect.onError(() =>
          forgetNow(to).pipe(Effect.catch((error) => Effect.logWarning(`copying a snapshot to ${prefixOf(to)} failed, and forgetting what it wrote failed too: ${error.message}`)))
        ),
      );
      // This node's cache holds nothing of `to` until it is prepared here.
      yield* settle(manifest, cache.clear(to.environment, to.workspace, null));
      return manifest;
    });

  const copy = (from: WorkspaceRef, to: WorkspaceRef): Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError> =>
    exclusive([from, to], copyNow(from, to)).pipe(
      Effect.tap((manifest) =>
        Effect.sync(() => copies.add(1, { outcome: "copied" })).pipe(
          Effect.andThen(Effect.logInfo(`copied the snapshot ${manifest.snapshot} of ${prefixOf(from)} to ${prefixOf(to)}`)),
        )
      ),
      Effect.tapError((error) => Effect.sync(() => copies.add(1, { outcome: error._tag }))),
      withAlasioSpan("alasio.node_agent.copy", {
        attributes: { ...spanAttributes(to), "alasio.source.environment": from.environment, "alasio.source.workspace": from.workspace },
      }),
    );

  /** The snapshot's image, from the bucket into the cache, each file checked against its manifest. */
  const download = (manifest: Manifest): Effect.Effect<void, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace, snapshot } = manifest;
      const directory = yield* cache.stage(environment, workspace, snapshot);
      for (const file of manifest.files) {
        const target = join(directory, file.path);
        yield* onDisk(() => mkdir(dirname(target), { recursive: true }));
        const received = yield* bucket.getFile(`${prefixOf(manifest)}/${snapshot}/${file.path}`, target);
        if (received.bytes !== file.bytes || received.sha256 !== file.sha256) {
          return yield* new SnapshotCorrupt({
            environment,
            workspace,
            reason: `its file ${file.path} is ${received.bytes} bytes of SHA-256 ${received.sha256}, where its manifest says ${file.bytes} bytes of ${file.sha256}`,
          });
        }
      }
      yield* onDisk(() => writeFile(join(directory, MANIFEST_FILE), JSON.stringify(manifest)));
      yield* cache.commit(environment, workspace, snapshot);
    }).pipe(Effect.onError(() => cache.clear(manifest.environment, manifest.workspace, null).pipe(Effect.ignore)));

  /** Why this node cannot restore `manifest`, or null when it can. */
  const unrestorable = (manifest: Manifest): Effect.Effect<string | null, RunscError> =>
    Effect.gen(function*() {
      const here = yield* runsc.cpuFeatures(manifest.release).pipe(
        Effect.map((names) => new Set(names)),
        Effect.catchTag("ReleaseNotInstalled", () => Effect.succeed(null)),
      );
      if (here === null) return `its gVisor ${manifest.release} is not installed here`;
      const missing = manifest.cpuFeatures.filter((feature) => !here.has(feature));
      return missing.length > 0 ? `this node's CPU lacks features it was saved with: ${missing.join(", ")}` : null;
    });

  const prepare = (ref: WorkspaceRef): Effect.Effect<Prepared, PrepareError> =>
    exclusive(
      [ref],
      Effect.gen(function*() {
        const manifest = yield* current(ref);
        if (manifest === null) return yield* new NoSnapshot(ref);
        const { environment, workspace, snapshot } = manifest;
        yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot });
        const why = yield* unrestorable(manifest);
        if (why !== null) return yield* new Unrestorable({ environment, workspace, snapshot, reason: why });
        const cached = yield* cache.holds(environment, workspace, snapshot);
        if (cached) yield* cache.use(environment, workspace, snapshot);
        else yield* download(manifest);
        yield* cache.clear(environment, workspace, snapshot);
        yield* evict.pipe(Effect.catch((error) => Effect.logWarning(error.message)));
        const source = cached ? "cache" : "bucket";
        yield* Effect.sync(() => prepares.add(1, { outcome: "prepared", source }));
        yield* Effect.annotateCurrentSpan({ "alasio.snapshot.source": source, "alasio.snapshot.bytes": manifest.bytes });
        return { manifest, imagePath: cache.path(environment, workspace, snapshot) };
      }),
    ).pipe(
      Effect.tapError((error) => Effect.sync(() => prepares.add(1, { outcome: error._tag }))),
      withAlasioSpan("alasio.node_agent.prepare", { attributes: spanAttributes(ref) }),
    );

  const holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError> = Effect.gen(function*() {
    const releases = yield* Effect.forEach(yield* runsc.releases, (release) => runsc.cpuFeatures(release).pipe(Effect.map((cpuFeatures) => ({ release, cpuFeatures }))));
    const cached = (yield* cache.list).map(({ usedAt, ...rest }) => ({ ...rest, usedAt: new Date(usedAt).toISOString() }));
    return { node, releases, cached };
  });

  return {
    checkpoint: (request) => checkpoint(request).pipe(withLogScope("node-agent")),
    prepare: (ref) => prepare(ref).pipe(withLogScope("node-agent")),
    current,
    copy: (from, to) => copy(from, to).pipe(withLogScope("node-agent")),
    forget: (ref) =>
      exclusive([ref], forgetNow(ref)).pipe(
        Effect.tap(() => Effect.logInfo(`forgot the snapshot of ${prefixOf(ref)}`)),
        withAlasioSpan("alasio.node_agent.forget", { attributes: spanAttributes(ref) }),
        withLogScope("node-agent"),
      ),
    holdings,
  };
});

/** The attributes of a span of an operation on a workspace. */
function spanAttributes({ environment, workspace }: WorkspaceRef): Record<string, string> {
  return { "alasio.environment": environment, "alasio.workspace": workspace };
}
