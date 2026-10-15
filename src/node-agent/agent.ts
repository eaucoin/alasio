/**
 * What a node agent does with snapshots: whole-sandbox gVisor checkpoints of session
 * pods on its node, kept in the snapshots bucket and in the node's cache. A workspace is
 * one session's, of one environment (main, or a branch environment), and its snapshots are
 * `<environment>/<workspace>/` in the bucket:
 *
 * - `layers/<digest>/<file>`: each image of the workspace's, once, by its digest, as runsc
 *   wrote its files. A snapshot's image holds the pages its sandbox wrote since the image
 *   it was last saved to or restored from, when that one is in this node's cache and the
 *   sandbox tracks its writes, and refers to that image and its layers for the rest; it
 *   holds them all otherwise;
 * - `manifests/<snapshot>.json`: what each snapshot is (Manifest): its image and its
 *   layers, each of whose files is in the bucket as the manifest says before it is written;
 * - `current.json`: the manifest of the workspace's current snapshot, written last, which
 *   commits it. A workspace without one has no snapshot to restore.
 *
 * A checkpoint ends the sandbox it saves. Once it has begun, the workspace's current
 * snapshot no longer holds what the workspace is, whose files and processes have moved
 * on since it was taken, so a checkpoint that fails forgets it rather than leave it to be
 * restored. A snapshot restored is spent: no longer current, its images go on as the layers
 * of the next. A copy gives a workspace another's current snapshot, as a session forked
 * from another restores what the other was: its images copied in the bucket, then its
 * manifest, then its commit, as a checkpoint writes them.
 *
 * A snapshot a release's checkpoint commits whose chain of images is due (compactionDue)
 * is compacted, after its commit and beside whatever else the agent does, one at a time on
 * the node at its lowest priority: its images into one, the image of a new snapshot
 * `compactedFrom` it, committed as the current one only if the workspace has not moved on
 * from it meanwhile. It is the same sandbox at the same moment, so a Sandbox released into
 * the one is restored from the other, which is the next delta's base.
 *
 * What no snapshot needs any more is collected, by one caller (alasio's collection): the
 * images and manifests that neither the current snapshot, nor the latest, which a sandbox
 * restored from it may still extend, nor one superseded within COLLECT_GRACE_MS names, nor
 * written within it, as a commit writes its images before its manifest. A restore or a
 * copy that read a manifest just before it was superseded reads its images within that.
 *
 * A sandbox is restored paused, in a pod of a new address, whose network policies
 * kube-router puts in force on the node as it next syncs (./kube-router.ts): until then
 * what the pod sends goes through, whatever they say. Its resume waits for them, then
 * forgets the connections tracked from its addresses, then resumes its containers; one
 * whose policies are not in force within POLICY_WAIT_MS leaves it paused.
 *
 * A restore alasio refuses before it resumes the sandbox is abandoned: the agent kills
 * what runsc runs for the pod's containers, their sandbox and gofers among it, so that the
 * pod's teardown waits on none of it: a `runsc restore` that failed has left them running,
 * its shim then tearing none of them down. A process is a container's only where its
 * arguments name runsc's root, the container's bundle and its id, and is killed only while
 * its start time is the one it was found with, so a pid the node has given another process
 * since is left be (./runsc.ts). This stays as defence should a restore leave runsc's
 * processes behind.
 *
 * One operation on a workspace's snapshots runs at a time.
 */
import { createHash, randomBytes } from "node:crypto";
import { createReadStream } from "node:fs";
import { mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { isIP, isIPv6 } from "node:net";
import { dirname, join, posix, relative } from "node:path";
import { pipeline } from "node:stream/promises";

import type { BatchObservableResult } from "@opentelemetry/api";
import { Clock, type Duration, Effect, RcMap, Schedule, Schema, type Scope, Semaphore } from "effect";

import { NAME, SNAPSHOT_ID } from "../kube/sandbox-shape.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";
import { withLogScope } from "../shared/log.ts";
import { CacheError, type CachedWorkspace, DIGEST, LAYERS_DIR, type SnapshotCache } from "./cache.ts";
import { type HostCommandError, type HostCommands, onNode } from "./host.ts";
import { egressGap, type PodAddress } from "./kube-router.ts";
import { runscVersion } from "./names.ts";
import type { ReleaseNotInstalled, Runsc, RunscError, RunningContainer } from "./runsc.ts";
import type { S3Bucket, S3Error } from "./s3.ts";
import { hostPathOf, readUnder, type UnderRoot } from "./volume.ts";

/** An environment's or a workspace's name: a DNS label, as their namespaces' and Sandboxes' names are. */
const Name = Schema.String.check(Schema.isPattern(NAME));

/** A snapshot's id, as snapshotId makes one. */
const SnapshotId = Schema.String.check(Schema.isPattern(new RegExp(`^${SNAPSHOT_ID}$`, "u")));

/** A gVisor release. */
const Release = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u));

/**
 * A file's path in a snapshot's image, as imageFiles gives it: relative, of names that
 * neither begin with a dot nor hold a separator, so it is under the image's directory
 * however a bucket's manifest writes it.
 */
const ImagePath = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_][A-Za-z0-9._-]*(\/[A-Za-z0-9_][A-Za-z0-9._-]*)*$/u), Schema.isMaxLength(1024));

/** A count of bytes, or of milliseconds. */
const Count = Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));

/** An ID of bayma's, a session's or an exec's, as bayma makes them. */
const BaymaId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/u));

/** An absolute path, of names, none of them `.` or `..`. */
const AbsolutePath = Schema.String.check(
  Schema.isPattern(/^(\/[^/\0]+)+$/u),
  Schema.makeFilter((path) => path.split("/").every((name) => name !== "." && name !== "..") || "must name no . or .."),
);

/** How a checkpoint saves a sandbox live: the state directory, in the container, of the bayma it runs. */
export const LiveSave = Schema.Struct({ stateDir: AbsolutePath });
export type LiveSave = typeof LiveSave.Type;

/** The execs a save caught running, each by its session's and its own ID, as bayma names them. */
const Running = Schema.Array(Schema.Struct({ sessionId: BaymaId, execId: BaymaId }));

/**
 * What a snapshot taken live caught, as bayma's `sandbox-hook save` said it in its state
 * directory's last-save.json: the state generation, which the snapshot may be restored at
 * only while it is the one on the disk (bayma's generation.json), and the execs running,
 * for whoever reads the manifest. So the snapshot holds what the sandbox is only while
 * nothing its clients were told of has happened since.
 */
export const Caught = Schema.Struct({
  stateDir: AbsolutePath,
  generation: Count,
  running: Running,
});
export type Caught = typeof Caught.Type;


/**
 * A file of an image: its path in the image, its size, its SHA-256, in hex, and the ETag
 * the bucket answered its upload with, which names what the bucket holds of it.
 */
const SnapshotFile = Schema.Struct({ path: ImagePath, bytes: Count, sha256: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u)), etag: Schema.String });
export type SnapshotFile = typeof SnapshotFile.Type;

/** An image, as runsc names it, by its digest, and its files, which the bucket keeps under `layers/<digest>/`. */
const Layer = Schema.Struct({ digest: Schema.String.check(Schema.isPattern(DIGEST)), files: Schema.Array(SnapshotFile) });
export type Layer = typeof Layer.Type;

/**
 * What a snapshot is, as the bucket keeps it, and the cache beside its images. Whatever can
 * write the bucket writes it, so what names a place on a node is held to the names the
 * agent makes, the workspace it names to the one asked about (current), and its size, which
 * the node's disk is made room for, to its images'.
 */
export const Manifest = Schema.Struct({
  environment: Name,
  workspace: Name,
  snapshot: SnapshotId,
  /** The gVisor release that saved it, the only one that restores it. */
  release: Release,
  /** The CPU features the sandbox was saved with, which a node restoring it must have. */
  cpuFeatures: Schema.Array(Schema.String),
  /** The pod template the sandbox ran, as alasio gave it, which its restore runs again. */
  podTemplate: Schema.Record(Schema.String, Schema.Unknown),
  /** Its image, then each image that one holds pages of, its layers, as runsc lists them: what a restore of it reads. */
  layers: Schema.Array(Layer).check(Schema.isMinLength(1)),
  /** The bytes of its images together. */
  bytes: Count,
  takenAt: Schema.String,
  /** How long runsc took to checkpoint the sandbox: its hook run in it, then its state saved, the sandbox paused. */
  pauseMs: Count,
  /** How long its image took to upload. */
  uploadMs: Count,
  /** For a snapshot taken live, what it caught: it holds the session only while bayma's state generation is that one. */
  live: Schema.optional(Caught),
  /**
   * For a snapshot of a compacted image, the snapshot whose memory it holds, as a Sandbox
   * released into that one names it: the same sandbox, at the same moment, in one image.
   */
  compactedFrom: Schema.optional(SnapshotId),
}).check(Schema.makeFilter((manifest) => manifest.bytes === bytesOf(manifest.layers) || "its bytes must be its images' together"));
export type Manifest = typeof Manifest.Type;

/** The bytes of `layers`' files together. */
function bytesOf(layers: readonly Layer[]): number {
  return layers.reduce((sum, { files }) => sum + files.reduce((each, { bytes }) => each + bytes, 0), 0);
}

/** The most images a snapshot reads from, its own and its layers, before it is compacted. */
export const MAX_CHAIN_DEPTH = 16;

/**
 * Why a snapshot of `layers`, its image and those it holds pages of, is compacted, or null
 * where it is not. Its base is the largest of them, as a chain's first image, saved whole,
 * is, and the others its deltas: it is compacted once its deltas together are as large as
 * its base, so that the chain stays within about twice the base on the disks that keep it;
 * once one delta is larger than half its base, as one is of a sandbox that wrote over much
 * of its memory, which the base then holds stale; or once it reads from more than
 * MAX_CHAIN_DEPTH images, each a file a restore opens and looks pages up in.
 */
export function compactionDue(layers: readonly Layer[]): string | null {
  if (layers.length < 2) return null;
  if (layers.length > MAX_CHAIN_DEPTH) return `it reads from ${layers.length} images, more than ${MAX_CHAIN_DEPTH}`;
  const sizes = layers.map((layer) => bytesOf([layer]));
  const base = Math.max(...sizes);
  const deltas = sizes.toSpliced(sizes.indexOf(base), 1);
  const together = deltas.reduce((sum, bytes) => sum + bytes, 0);
  if (together >= base) return `its deltas are ${together} bytes together, as large as its base's ${base}`;
  const largest = Math.max(...deltas);
  if (largest * 2 > base) return `a delta of ${largest} bytes is more than half its base's ${base}`;
  return null;
}

/** A workspace, as each request names it. */
export const WorkspaceRef = Schema.Struct({ environment: Name, workspace: Name });
export type WorkspaceRef = typeof WorkspaceRef.Type;

/** A request to copy a workspace's current snapshot to another. */
export const CopyRequest = Schema.Struct({ from: WorkspaceRef, to: WorkspaceRef });
export type CopyRequest = typeof CopyRequest.Type;

/** A container, by containerd's id. */
const ContainerId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u));

/** A pod's name: a DNS subdomain. */
const PodName = Schema.String.check(Schema.isPattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/u), Schema.isMaxLength(253));

/** A request to checkpoint a workspace's sandbox. */
export const CheckpointRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The pod whose sandbox is saved, which the container must be of. */
  namespace: Name,
  pod: PodName,
  /** The container whose sandbox is saved, in which the hook runs: the release that runs it is the one that saves it. */
  containerId: ContainerId,
  /** The program run in the container before the save: its arguments, none holding whitespace, as runsc splits them at spaces. */
  hook: Schema.Array(Schema.String.check(Schema.isPattern(/^\S+$/u))).check(Schema.isMinLength(1)),
  podTemplate: Schema.Record(Schema.String, Schema.Unknown),
  /** Where the sandbox is saved live, running on: never at a release, which ends it. */
  live: Schema.optional(LiveSave),
});
export type CheckpointRequest = typeof CheckpointRequest.Type;

/** A request to resume a workspace's sandbox, restored paused in a pod on this node. */
export const ResumeRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The pod's namespace and name, as its network policies' rules name it, which its containers must be of. */
  namespace: Name,
  pod: PodName,
  /** The pod's addresses, one of each family it has, in their canonical form, as Kubernetes gives them. */
  podIPs: Schema.Array(Schema.String.check(Schema.makeFilter((ip) => isIP(ip) !== 0 || "must be an IP address"))).check(Schema.isMinLength(1)),
  /** The pod's containers, each resumed, by the release that runs it. */
  containerIds: Schema.Array(ContainerId).check(Schema.isMinLength(1)),
  /** The network policies of its namespace that select the pod, by name, which its rules must run what it sends through. */
  policies: Schema.Array(PodName).check(Schema.isMaxLength(64)),
});
export type ResumeRequest = typeof ResumeRequest.Type;

/** A request to abandon a workspace's restore, which alasio refused before it resumed its sandbox, in a pod on this node. */
export const AbandonRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The pod restored into, whose containers' processes of runsc's are killed. */
  namespace: Name,
  pod: PodName,
});
export type AbandonRequest = typeof AbandonRequest.Type;

/** A process of runsc's an abandon killed: the container it ran for, by containerd's id, and its pid. */
export interface Killed {
  readonly containerId: string;
  readonly pid: number;
}

/** A snapshot prepared on this node: what it is, and where its image is. */
export interface Prepared {
  readonly manifest: Manifest;
  readonly imagePath: string;
}

/** What this node holds: its gVisor releases with the CPU features each sees here, and the workspaces whose images its cache holds, by their digests. */
export interface Holdings {
  readonly node: string;
  readonly releases: readonly { readonly release: string; readonly cpuFeatures: readonly string[] }[];
  readonly cached: readonly { readonly environment: string; readonly workspace: string; readonly images: readonly string[]; readonly bytes: number; readonly usedAt: string }[];
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

/**
 * A sandbox restored from a snapshot taken live is not let run, its snapshot holding the
 * session no more: bayma's state generation has moved on since, or cannot be told.
 */
export class SnapshotOutlived extends Schema.TaggedError<SnapshotOutlived>()("SnapshotOutlived", {
  environment: Schema.String,
  workspace: Schema.String,
  snapshot: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the snapshot ${this.snapshot} of ${this.environment}/${this.workspace}, taken live, holds the session no more: ${this.reason}; its sandbox is left paused`;
  }
}

/**
 * How long a resume waits for a restored pod's network policies to be in force on its
 * node. kube-router syncs as the pod gets its address, a sync of the node's whole filter
 * table, which takes it well under a second on the nodes alasio makes; one that has not
 * within this is behind or down, and the prompt waiting on the restore is told so rather
 * than wait on.
 */
export const POLICY_WAIT_MS = 30_000;

/** A pod's network policies were not in force on this node within POLICY_WAIT_MS: which address of its lacked what. Its sandbox stays paused. */
export class PoliciesNotInForce extends Schema.TaggedError<PoliciesNotInForce>()("PoliciesNotInForce", {
  namespace: Schema.String,
  pod: Schema.String,
  ip: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the network policies of ${this.namespace}/${this.pod} are not in force on this node after ${POLICY_WAIT_MS / 1000} s, its sandbox left paused: for ${this.ip}, ${this.reason}`;
  }
}

/** A container is not the named pod's: whose it is. */
export class ContainerElsewhere extends Schema.TaggedError<ContainerElsewhere>()("ContainerElsewhere", {
  containerId: Schema.String,
  namespace: Schema.String,
  pod: Schema.String,
  found: Schema.String,
}) {
  override get message(): string {
    return `the container ${this.containerId} is not of the pod ${this.namespace}/${this.pod}, but of ${this.found}`;
  }
}

/** How preparing a snapshot fails. */
export type PrepareError = NoSnapshot | Unrestorable | SnapshotCorrupt | S3Error | CacheError | RunscError;

/** How resuming a sandbox fails. */
export type ResumeError = PoliciesNotInForce | ContainerElsewhere | SnapshotOutlived | HostCommandError | RunscError | ReleaseNotInstalled | S3Error | SnapshotCorrupt;

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
  /**
   * Makes the workspace's current snapshot current no more, if it is `snapshot` still: one
   * its sandbox was restored from, or one taken live whose exec has ended. Its images go on
   * as the layers of its next.
   */
  readonly spend: (workspace: WorkspaceRef, snapshot: string) => Effect.Effect<void, S3Error | SnapshotCorrupt>;
  /** Forgets the workspace's snapshots, in the bucket and in the cache. */
  readonly forget: (workspace: WorkspaceRef) => Effect.Effect<void, S3Error | CacheError>;
  /** Collects what of the workspace's in the bucket no snapshot needs any more, as the module says: the digests of the images it keeps. */
  readonly collect: (workspace: WorkspaceRef) => Effect.Effect<readonly string[], S3Error | SnapshotCorrupt>;
  /** The workspaces of `environment` the bucket holds anything of. */
  readonly workspaces: (environment: string) => Effect.Effect<readonly string[], S3Error>;
  /** Removes from this node's cache the workspace's images but `keep`, unless a restore may still be reading them: whether it did. */
  readonly discard: (workspace: WorkspaceRef, keep: readonly string[]) => Effect.Effect<boolean, CacheError>;
  /** Resumes the workspace's sandbox, restored paused, once its pod's network policies are in force on this node, as the module says. */
  readonly resume: (request: ResumeRequest) => Effect.Effect<void, ResumeError>;
  /** Abandons the workspace's restore into a pod on this node, alasio having refused it before its resume, as the module says: what it killed. */
  readonly abandon: (request: AbandonRequest) => Effect.Effect<readonly Killed[], RunscError>;
  readonly holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError>;
}

/**
 * What makeNodeAgent is given: this node's name, its releases, its cache, the bucket, the
 * commands it runs on the node, and where it sees the node's filesystem.
 */
export interface NodeAgentOptions {
  readonly node: string;
  readonly nodeRoot: string;
  readonly runsc: Runsc;
  readonly cache: SnapshotCache;
  readonly bucket: S3Bucket;
  readonly host: HostCommands;
}

/** The directory of a workspace's manifests, in the bucket. */
export const MANIFESTS_DIR = "manifests";
/** The file of the manifest of the snapshot a cache last took or prepared of a workspace, beside its images. */
export const MANIFEST_FILE = "manifest.json";
/** The object of a workspace's current snapshot. */
export const CURRENT_FILE = "current.json";
/**
 * How long what a snapshot superseded needs, and what a commit wrote, is kept: longer than
 * a restore or a copy reads the images of a manifest it read, and than a commit takes.
 */
export const COLLECT_GRACE_MS = 60 * 60_000;
/**
 * The most of a file of bayma's state directory the agent reads: one larger is not read,
 * and what it says is not told.
 */
export const STATE_FILE_BYTES = 1024 * 1024;
/** bayma's state generation, as its state directory's generation.json says it. */
const StateGenerationFile = Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1), generation: Count }));
/** What bayma's last save caught, as its state directory's last-save.json says it. */
const LastSaveFile = Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1), generation: Count, running: Running }));
/** How the agent reads a state file's bytes as text: those that are not UTF-8 fail it. */
const UTF8 = new TextDecoder("utf-8", { fatal: true });
/** How long the hook may take to ready the sandbox for its save. */
const HOOK_TIMEOUT = "30s";
/** How often the cache is looked at for what to evict, besides as checkpoints and prepares write to it. */
const EVICT_POLL: Duration.Input = "1 minute";
/** How often a resume looks for a restored pod's rules. */
const POLICY_POLL = "250 millis";

const checkpoints = meter.createCounter("alasio.node_agent.checkpoints", {
  description: "Whole-sandbox checkpoints taken by the node agent, by outcome: taken, or what failed",
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
const resumes = meter.createCounter("alasio.node_agent.resumes", {
  description: "Restored sandboxes resumed by the node agent once their pods' network policies were in force, by outcome",
  unit: "{resume}",
});
const abandons = meter.createCounter("alasio.node_agent.abandons", {
  description: "Restores alasio refused before their sandboxes resumed, abandoned by the node agent, what runsc ran for their pods killed, by outcome",
  unit: "{abandon}",
});
const compactions = meter.createCounter("alasio.node_agent.compactions", {
  description: "Snapshots whose chains of images the node agent compacted into one, after a release's checkpoint committed them, by outcome: compacted, superseded as the workspace moved on meanwhile, or what failed",
  unit: "{compaction}",
});
const compactionDuration = meter.createHistogram("alasio.node_agent.compaction.duration", {
  description: "How long runsc took to compact a snapshot's chain of images into one",
  unit: "s",
});
const keptReleases = meter.createObservableGauge("alasio.node_agent.releases", {
  description: "The gVisor releases kept on the node, each of which restores the snapshots it took",
  unit: "{release}",
});
const cachedSnapshots = meter.createObservableGauge("alasio.node_agent.cache.snapshots", {
  description: "The snapshots the node's cache holds, by the gVisor release that took them, none for a release kept on the node that took none",
  unit: "{snapshot}",
});

/** The release a cached manifest names, of all it says. */
const CachedRelease = Schema.fromJsonString(Schema.Struct({ release: Schema.String }));

/** A snapshot's id, as SNAPSHOT_ID says: when it was taken, to the millisecond, and a random part. */
function snapshotId(takenAt: number): string {
  return `${new Date(takenAt).toISOString().replace(/[-:.]/gu, "")}-${randomBytes(4).toString("hex")}`;
}

/** The SHA-256 of a file, in hex. */
async function fileSha256(path: string): Promise<string> {
  const hash = createHash("sha256");
  await pipeline(createReadStream(path), hash);
  return hash.digest("hex");
}

/** A file of an image as it is on the disk, before the bucket holds it. */
type ImageFile = Omit<SnapshotFile, "etag">;

/** The files of an image directory, each with its size and SHA-256, by their paths in it, in order. */
async function imageFiles(directory: string): Promise<ImageFile[]> {
  const entries = await readdir(directory, { withFileTypes: true, recursive: true });
  const paths = entries.filter((entry) => entry.isFile()).map((entry) => relative(directory, join(entry.parentPath, entry.name))).sort();
  return Promise.all(paths.map(async (path) => {
    const file = join(directory, path);
    return { path, bytes: (await stat(file)).size, sha256: await fileSha256(file) };
  }));
}

/** Whether `found`, an image's files on the disk, are `layer`'s. */
const sameFiles = (found: readonly ImageFile[], layer: Layer): boolean =>
  JSON.stringify(found) === JSON.stringify(layer.files.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })));

/** What each suffix of a Kubernetes quantity multiplies it by. */
const QUANTITY_UNITS: Readonly<Record<string, number>> = { Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, Pi: 2 ** 50, Ei: 2 ** 60, k: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15, E: 1e18 };

/**
 * The most memory the pod of `podTemplate` may hold, as its containers' limits say, which
 * bounds its sandbox's image: what a checkpoint makes room for in the cache. A container of
 * no limit, or of one not read, adds nothing.
 */
export function memoryLimitOf(podTemplate: Readonly<Record<string, unknown>>): number {
  const spec = podTemplate["spec"] as { readonly containers?: readonly { readonly resources?: { readonly limits?: Readonly<Record<string, unknown>> } }[] } | undefined;
  return (spec?.containers ?? []).reduce((sum, { resources }) => {
    const limit = resources?.limits?.["memory"];
    const [, amount = "", unit = ""] = /^([0-9]+(?:\.[0-9]+)?)([A-Za-z]*)$/u.exec(typeof limit === "string" ? limit : String(limit ?? "")) ?? [];
    const bytes = Number(amount) * (unit === "" ? 1 : QUANTITY_UNITS[unit] ?? Number.NaN);
    return sum + (Number.isFinite(bytes) ? Math.ceil(bytes) : 0);
  }, 0);
}

/** `run`, on the node's disk, failing as the cache does. */
const onDisk = <A>(run: () => Promise<A>): Effect.Effect<A, CacheError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new CacheError({ reason: cause instanceof Error ? cause.message : String(cause) }) });

/** The node agent of `options`, whose operations on a workspace run one at a time while the scope lasts. */
export const makeNodeAgent = Effect.fnUntraced(function*({ node, nodeRoot, runsc, cache, bucket, host }: NodeAgentOptions): Effect.fn.Return<NodeAgent, never, Scope.Scope> {
  const locks = yield* RcMap.make({ lookup: (_workspace: string) => Semaphore.make(1) });
  /**
   * The snapshot each sandbox running on this node, by the container checkpoints name, was
   * last saved to or restored from, whose image the next checkpoint of it saves what changed
   * since: in memory, as an agent that starts again takes a full one first.
   */
  const bases = new Map<string, Manifest>();
  /** The snapshot each workspace's restore on this node was last prepared from, which its resume makes its sandbox's base. */
  const prepared = new Map<string, Manifest>();
  /** Compactions run in the agent's scope, one at a time on the node, beside whatever else it does. */
  const scope = yield* Effect.scope;
  const compacting = yield* Semaphore.make(1);

  /** How many workspaces the cache holds the images of under each release, as the manifests beside them say. */
  const cachedByRelease: Effect.Effect<ReadonlyMap<string, number>, CacheError | Schema.SchemaError> = Effect.gen(function*() {
    const held = new Map<string, number>();
    for (const { environment, workspace } of yield* cache.list) {
      const text = yield* onDisk(() => readFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), "utf8").catch(() => null));
      if (text === null) continue;
      const { release } = yield* Schema.decodeUnknownEffect(CachedRelease)(text);
      held.set(release, (held.get(release) ?? 0) + 1);
    }
    return held;
  });

  // The gauges are read as metrics are exported, outside every fiber of the agent's.
  const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
  const gauges = [keptReleases, cachedSnapshots];
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      const observe = (result: BatchObservableResult) =>
        runPromise(Effect.gen(function*() {
          const installed = yield* runsc.releases;
          const held = yield* cachedByRelease;
          result.observe(keptReleases, installed.length);
          for (const release of new Set([...installed, ...held.keys()])) {
            result.observe(cachedSnapshots, held.get(release) ?? 0, { "alasio.gvisor.release": release });
          }
        }).pipe(Effect.catch((error) => Effect.logWarning(`could not count the snapshots the node's cache holds: ${error.message}`))));
      meter.addBatchObservableCallback(observe, gauges);
      return observe;
    }),
    (observe) => Effect.sync(() => meter.removeBatchObservableCallback(observe, gauges)),
  );

  const prefixOf = ({ environment, workspace }: WorkspaceRef): string => `${environment}/${workspace}`;
  /** The object of the file `path` of the workspace's image `digest`. */
  const layerKey = (ref: WorkspaceRef, digest: string, path: string): string => `${prefixOf(ref)}/${LAYERS_DIR}/${digest}/${path}`;
  /** The object of the manifest of the workspace's snapshot `snapshot`. */
  const manifestKey = (ref: WorkspaceRef, snapshot: string): string => `${prefixOf(ref)}/${MANIFESTS_DIR}/${snapshot}.json`;

  /** `effect` once no other operation runs on the workspaces, each held in the order of its name, so that two operations never wait on each other. */
  const exclusive = <A, E>(workspaces: readonly WorkspaceRef[], effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
    Effect.scoped(
      [...new Set(workspaces.map(prefixOf))].sort().reduceRight<Effect.Effect<A, E, Scope.Scope>>(
        (held, name) => RcMap.get(locks, name).pipe(Effect.flatMap((lock) => held.pipe(Semaphore.withPermit(lock)))),
        effect,
      ),
    );

  /** How long `effect` took, in milliseconds, and what it came to. */
  const timed = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<readonly [number, A], E> =>
    Effect.gen(function*() {
      const start = yield* Clock.currentTimeMillis;
      const value = yield* effect;
      return [(yield* Clock.currentTimeMillis) - start, value] as const;
    });

  /** The manifest the object `key` holds, of the workspace `ref`'s: one of another workspace's is none of it. */
  const manifestAt = (ref: WorkspaceRef, key: string): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> =>
    bucket.getJson(key).pipe(
      Effect.flatMap((found) =>
        found === null
          ? Effect.succeed(null)
          : Schema.decodeUnknownEffect(Manifest)(found).pipe(
            Effect.mapError((error) => new SnapshotCorrupt({ ...ref, reason: `its ${key.slice(prefixOf(ref).length + 1)} is not a manifest: ${error.message}` })),
            Effect.filterOrFail(
              (manifest) => prefixOf(manifest) === prefixOf(ref),
              (manifest) => new SnapshotCorrupt({ ...ref, reason: `its ${key.slice(prefixOf(ref).length + 1)} names ${prefixOf(manifest)}` }),
            ),
          )
      ),
    );

  /** The workspace's current snapshot, as its `current.json` says. */
  const current = (ref: WorkspaceRef): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> => manifestAt(ref, `${prefixOf(ref)}/${CURRENT_FILE}`);

  /** Forgets the workspace's snapshots: the cache's copy, then in the bucket, uncommitted first and then its objects deleted. */
  const forgetNow = (ref: WorkspaceRef): Effect.Effect<void, S3Error | CacheError> =>
    Effect.gen(function*() {
      const prefix = prefixOf(ref);
      yield* cache.clear(ref.environment, ref.workspace);
      yield* bucket.remove(`${prefix}/${CURRENT_FILE}`);
      yield* Effect.forEach(yield* bucket.list(`${prefix}/`), ({ key }) => bucket.remove(key), { discard: true });
    });

  /**
   * The release that runs the container, which must be of the pod `namespace`/`pod`, as
   * the annotations containerd gives its spec say: a request names no container of
   * another's; and whether its sandbox tracks its writes.
   */
  const ofPod = (containerId: string, namespace: string, pod: string): Effect.Effect<RunningContainer, RunscError | ContainerElsewhere> =>
    runsc.container(containerId).pipe(
      Effect.flatMap((running) => {
        const found = podOf(running.annotations);
        return found === `${namespace}/${pod}` ? Effect.succeed(running) : Effect.fail(new ContainerElsewhere({ containerId, namespace, pod, found }));
      }),
    );

  /** The files of the image `digest` the cache holds of the workspace, uploaded: each with the ETag its upload was answered with. */
  const upload = (ref: WorkspaceRef, digest: string, files: readonly ImageFile[]): Effect.Effect<Layer, S3Error> =>
    Effect.forEach(files, (file) =>
      bucket.putFile(layerKey(ref, digest, file.path), join(cache.image(ref.environment, ref.workspace, digest), file.path)).pipe(
        Effect.map((etag) => ({ ...file, etag })),
      )).pipe(Effect.map((uploaded) => ({ digest, files: uploaded })));

  /** Whether every file of `layer` of the workspace's is in the bucket as it was put: of its size, and of the ETag its upload was answered with. */
  const inBucket = (ref: WorkspaceRef, layer: Layer): Effect.Effect<boolean, S3Error> =>
    Effect.forEach(layer.files, ({ path, bytes, etag }) =>
      bucket.head(layerKey(ref, layer.digest, path)).pipe(
        Effect.map((stored) => stored !== null && stored.bytes === bytes && stored.etag === etag),
      )).pipe(Effect.map((each) => each.every(Boolean)));

  /**
   * `layer`, an image of the workspace's that a new snapshot refers to, as the bucket holds
   * it: as it was put, checked; or put again from the cache, whose files must be the
   * manifest's, where the bucket lost it or holds another of its digest.
   */
  const layerInBucket = (ref: WorkspaceRef, layer: Layer): Effect.Effect<Layer, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      if (yield* inBucket(ref, layer)) return layer;
      const reason = `its image ${layer.digest} is not in the bucket as it was put`;
      if (!(yield* cache.holds(ref.environment, ref.workspace, layer.digest))) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, nor in this node's cache` });
      const files = yield* onDisk(() => imageFiles(cache.image(ref.environment, ref.workspace, layer.digest)));
      if (!sameFiles(files, layer)) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, and this node's cache holds other files of it` });
      yield* Effect.logWarning(`${reason}: it is put again from this node's cache`);
      const again = yield* upload(ref, layer.digest, files);
      if (!(yield* inBucket(ref, again))) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, nor once put again` });
      return again;
    });

  /**
   * The snapshot of the sandbox `request` names, taken and committed as the module says:
   * its manifest. A delta of its base, its sandbox tracking its writes and the base's image
   * in the cache; whole otherwise.
   */
  const take = (request: CheckpointRequest): Effect.Effect<Manifest, RunscError | ReleaseNotInstalled | ContainerElsewhere | S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace, containerId, podTemplate } = request;
      const running = yield* ofPod(containerId, request.namespace, request.pod);
      const { tracksWrites } = running;
      const takenAt = yield* Clock.currentTimeMillis;
      const snapshot = snapshotId(takenAt);
      const base = bases.get(containerId);
      const [own] = base?.layers ?? [];
      const parent = base !== undefined && own !== undefined && tracksWrites && prefixOf(base) === prefixOf(request) && (yield* cache.holds(environment, workspace, own.digest)) ? base : null;
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot, "alasio.snapshot.parent": parent?.snapshot ?? "" });
      yield* cache.room(memoryLimitOf(podTemplate), request).pipe(Effect.flatMap(logEvicted));
      const staged = yield* cache.stage(environment, workspace, snapshot);
      return yield* commit(request, snapshot, takenAt, staged, parent, running);
    });

  /**
   * Saves the sandbox `request` names into `staged`, a delta of `parent`'s image if given,
   * and commits it as the snapshot `snapshot`: its manifest. What it staged goes if it fails.
   */
  const commit = (request: CheckpointRequest, snapshot: string, takenAt: number, staged: string, parent: Manifest | null, container: RunningContainer): Effect.Effect<Manifest, RunscError | ReleaseNotInstalled | S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace, containerId, hook, podTemplate, live } = request;
      const { release } = container;
      const [own] = parent?.layers ?? [];
      const [pauseMs] = yield* timed(runsc.checkpoint(release, {
        containerId,
        imagePath: staged,
        hook,
        hookTimeout: HOOK_TIMEOUT,
        ...(own === undefined ? {} : { parentImagePath: cache.image(environment, workspace, own.digest) }),
        live: live !== undefined,
      }));
      // What the save caught, as bayma's hook wrote it as the sandbox was saved.
      const caught = live === undefined ? undefined : yield* lastSave(request, live, [container]);
      const image = yield* runsc.inspect(release, staged);
      if (image.version !== runscVersion(release)) {
        return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image says runsc ${image.version} saved it, not ${runscVersion(release)}` });
      }
      // Each image it holds pages of is one of its parent's, whose files its manifest says.
      const known = new Map((parent?.layers ?? []).map((layer) => [layer.digest, layer]));
      const referred = yield* Effect.forEach(yield* runsc.layers(release, staged), (digest) => {
        const layer = known.get(digest);
        return layer === undefined ? Effect.fail(new SnapshotCorrupt({ environment, workspace, reason: `its image refers to the image ${digest}, which is none of its parent's` })) : Effect.succeed(layer);
      });
      yield* cache.commit(environment, workspace, snapshot, image.digest);
      const files = yield* onDisk(() => imageFiles(cache.image(environment, workspace, image.digest)));
      const [uploadMs, layers] = yield* timed(Effect.gen(function*() {
        const uploaded = yield* upload(request, image.digest, files);
        if (!(yield* inBucket(request, uploaded))) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image ${image.digest} is not in the bucket as it was put` });
        return [uploaded, ...yield* Effect.forEach(referred, (layer) => layerInBucket(request, layer))];
      }));
      const manifest: Manifest = {
        environment,
        workspace,
        snapshot,
        release,
        cpuFeatures: image.cpuFeatures,
        podTemplate,
        layers,
        bytes: bytesOf(layers),
        takenAt: new Date(takenAt).toISOString(),
        pauseMs,
        uploadMs,
        ...(caught === undefined ? {} : { live: caught }),
      };
      yield* onDisk(() => writeFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), JSON.stringify(manifest)));
      yield* bucket.putJson(manifestKey(request, snapshot), manifest);
      yield* bucket.putJson(`${prefixOf(request)}/${CURRENT_FILE}`, manifest);
      return manifest;
    }).pipe(Effect.onError(() => cache.unstage(request.environment, request.workspace, snapshot).pipe(Effect.ignore)));

  /**
   * Compacts the chain of images of `committed`, the snapshot a release's checkpoint has just
   * committed, where compactionDue says to: its image and its layers into one image, the
   * snapshot `compactedFrom` it, which becomes the workspace's current snapshot, and so the
   * base of the next delta of a sandbox restored from it, if the workspace has not moved on
   * from `committed` meanwhile. Its new snapshot, or null where it did not compact it.
   * Nothing waits on it: it writes and uploads its image before it holds the workspace, then
   * commits it once it holds it, as a checkpoint commits its own.
   */
  const compactNow = (committed: Manifest): Effect.Effect<Manifest | null, RunscError | ReleaseNotInstalled | S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const why = compactionDue(committed.layers);
      if (why === null) return null;
      const { environment, workspace, release } = committed;
      const [own] = committed.layers;
      if (own === undefined) return null;
      for (const { digest } of committed.layers) {
        if (!(yield* cache.holds(environment, workspace, digest))) {
          yield* Effect.logInfo(`not compacting the snapshot ${committed.snapshot} of ${prefixOf(committed)}, though ${why}: this node's cache no longer holds its image ${digest}`);
          return null;
        }
      }
      const snapshot = snapshotId(yield* Clock.currentTimeMillis);
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot, "alasio.snapshot.compacted_from": committed.snapshot, "alasio.snapshot.chain_depth": committed.layers.length });
      // At most as large as the images it is compacted from.
      yield* cache.room(committed.bytes, committed).pipe(Effect.flatMap(logEvicted));
      const staged = yield* cache.stage(environment, workspace, snapshot);
      return yield* Effect.gen(function*() {
        // runsc writes the image into a directory it makes.
        yield* onDisk(() => rm(staged, { recursive: true, force: true }));
        const [compactMs] = yield* timed(runsc.compact(release, { imagePath: cache.image(environment, workspace, own.digest), layerPath: join(cache.workspace(environment, workspace), LAYERS_DIR), output: staged }));
        yield* Effect.sync(() => compactionDuration.record(compactMs / 1000));
        const image = yield* runsc.inspect(release, staged);
        if (image.version !== runscVersion(release)) {
          return yield* new SnapshotCorrupt({ environment, workspace, reason: `its compacted image says runsc ${image.version} wrote it, not ${runscVersion(release)}` });
        }
        const referred = yield* runsc.layers(release, staged);
        if (referred.length > 0) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its compacted image refers to the images ${referred.join(", ")}, where it is to refer to none` });
        yield* cache.commit(environment, workspace, snapshot, image.digest);
        const files = yield* onDisk(() => imageFiles(cache.image(environment, workspace, image.digest)));
        const [uploadMs, uploaded] = yield* timed(upload(committed, image.digest, files));
        if (!(yield* inBucket(committed, uploaded))) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its compacted image ${image.digest} is not in the bucket as it was put` });
        const manifest: Manifest = { ...committed, snapshot, layers: [uploaded], bytes: bytesOf([uploaded]), uploadMs, compactedFrom: committed.snapshot };
        // What it uploaded, should the workspace have moved on meanwhile, no manifest names, and is collected as any such image is.
        const written = yield* exclusive(
          [committed],
          Effect.gen(function*() {
            if ((yield* current(committed))?.snapshot !== committed.snapshot) return false;
            yield* onDisk(() => writeFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), JSON.stringify(manifest)));
            yield* bucket.putJson(manifestKey(committed, snapshot), manifest);
            yield* bucket.putJson(`${prefixOf(committed)}/${CURRENT_FILE}`, manifest);
            return true;
          }),
        );
        if (!written) {
          yield* Effect.sync(() => compactions.add(1, { "alasio.node_agent.compaction.outcome": "superseded" }));
          yield* Effect.logInfo(`compacted the snapshot ${committed.snapshot} of ${prefixOf(committed)}, as ${why}, but the workspace moved on from it meanwhile: the compacted image is left to be collected`);
          return null;
        }
        yield* Effect.sync(() => compactions.add(1, { "alasio.node_agent.compaction.outcome": "compacted" }));
        yield* Effect.logInfo(
          `compacted the snapshot ${committed.snapshot} of ${prefixOf(committed)}, as ${why}: its ${committed.layers.length} images, ${committed.bytes} bytes, are the image of its snapshot ${snapshot}, ${manifest.bytes} bytes, in ${compactMs} ms, uploaded in ${uploadMs} ms`,
        );
        return manifest;
      }).pipe(Effect.onError(() => cache.unstage(environment, workspace, snapshot).pipe(Effect.ignore)));
    });

  /** Compacts `committed` as compactNow does, once no other compaction runs on the node, in the agent's scope: what fails is said, and changes nothing. */
  const compactLater = (committed: Manifest): Effect.Effect<void> =>
    compactNow(committed).pipe(
      compacting.withPermits(1),
      Effect.tapError((error) => Effect.sync(() => compactions.add(1, { "alasio.node_agent.compaction.outcome": error._tag }))),
      Effect.catch((error) => Effect.logWarning(`compacting the snapshot ${committed.snapshot} of ${prefixOf(committed)} failed, which changes nothing of it: ${error.message}`)),
      withAlasioSpan("alasio.node_agent.compaction", { attributes: { ...spanAttributes(committed), "alasio.snapshot": committed.snapshot } }),
      withLogScope("node-agent"),
      Effect.forkIn(scope),
      Effect.asVoid,
    );

  /** Says which workspaces were evicted from the node's cache. */
  const logEvicted = (evicted: readonly CachedWorkspace[]) =>
    Effect.forEach(evicted, ({ environment, workspace, bytes }) =>
      Effect.logInfo(`evicted the images of ${environment}/${workspace} (${bytes} bytes) from the node's cache`), { discard: true });

  const evict: Effect.Effect<void, CacheError> = cache.evict.pipe(Effect.flatMap(logEvicted));

  const checkpoint = (request: CheckpointRequest): Effect.Effect<Manifest, CheckpointFailed> => {
    const failed = (reason: string) => new CheckpointFailed({ environment: request.environment, workspace: request.workspace, reason });
    const moved = "as the workspace has moved on since it was taken";
    const taken = take(request).pipe(Effect.tapError((error) => Effect.sync(() => checkpoints.add(1, { "alasio.node_agent.checkpoint.outcome": error._tag }))));
    const committed = request.live === undefined
      ? taken.pipe(
        Effect.catch((error) =>
          forgetNow(request).pipe(
            Effect.matchEffect({
              onSuccess: () => Effect.fail(failed(`${error.message}; its snapshot is forgotten, ${moved}`)),
              onFailure: (forgetting) => Effect.fail(failed(`${error.message}; forgetting its snapshot, ${moved}, failed too: ${forgetting.message}`)),
            }),
          )
        ),
        Effect.onInterrupt(() => forgetNow(request).pipe(Effect.ignore)),
        // The sandbox it saved ended.
        Effect.ensuring(Effect.sync(() => bases.delete(request.containerId))),
      )
      : taken.pipe(
        // The sandbox runs on from the image it was saved to, which its next checkpoint saves what changed since.
        Effect.tap((manifest) => Effect.sync(() => bases.set(request.containerId, manifest))),
        // What runsc saved of the writes since its base, which the next would build on, is gone with it.
        Effect.tapError(() => Effect.sync(() => bases.delete(request.containerId))),
        Effect.mapError((error) => failed(`${error.message}; its snapshot is kept, as a live checkpoint that fails changes nothing`)),
      );
    return exclusive([request], committed.pipe(Effect.tap(() => evict.pipe(Effect.catch((error) => Effect.logWarning(error.message)))))).pipe(
      // A release's: compacted once it no longer holds the workspace, where its chain is due.
      Effect.tap((manifest) => (request.live === undefined ? compactLater(manifest) : Effect.void)),
      Effect.tap((manifest) =>
        Effect.sync(() => {
          pauses.record(manifest.pauseMs / 1000);
          uploads.record(manifest.uploadMs / 1000);
          sizes.record(manifest.bytes);
          checkpoints.add(1, { "alasio.node_agent.checkpoint.outcome": "taken" });
        }).pipe(
          Effect.andThen(Effect.annotateCurrentSpan({ "alasio.snapshot.bytes": manifest.bytes, "alasio.snapshot.pause_ms": manifest.pauseMs, "alasio.snapshot.upload_ms": manifest.uploadMs })),
          Effect.andThen(Effect.logInfo(`took the snapshot ${manifest.snapshot} of ${prefixOf(manifest)}${manifest.live === undefined ? "" : ` live, at bayma's generation ${manifest.live.generation}`}: ${manifest.bytes} bytes, paused ${manifest.pauseMs} ms, uploaded in ${manifest.uploadMs} ms`)),
        )
      ),
      Effect.tapError((error) => Effect.logWarning(error.message)),
      withAlasioSpan("alasio.node_agent.checkpoint", { attributes: { ...spanAttributes(request), "alasio.snapshot.live": request.live !== undefined } }),
    );
  };

  /**
   * Copies the current snapshot of `from` to `to` and commits it, as the module says:
   * `to`'s manifest. Once it has begun writing, one that fails forgets `to`'s snapshots,
   * rather than leave part of one, or one `to` was to move on from.
   */
  const copyNow = (from: WorkspaceRef, to: WorkspaceRef): Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError> =>
    Effect.gen(function*() {
      const source = yield* current(from);
      if (source === null) return yield* new NoSnapshot(from);
      // A workspace's snapshot is its own already.
      if (prefixOf(from) === prefixOf(to)) return source;
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": source.snapshot });
      const written = yield* Effect.gen(function*() {
        yield* cache.clear(to.environment, to.workspace);
        const layers = yield* Effect.forEach(source.layers, (layer) =>
          Effect.forEach(layer.files, (file) =>
            Effect.gen(function*() {
              const key = layerKey(to, layer.digest, file.path);
              yield* bucket.copy(layerKey(from, layer.digest, file.path), key, file.bytes);
              const stored = yield* bucket.head(key);
              if (stored === null || stored.bytes !== file.bytes) return yield* new SnapshotCorrupt({ ...to, reason: `its copy of ${key} is not in the bucket as it was copied` });
              return { ...file, etag: stored.etag };
            })).pipe(Effect.map((files) => ({ digest: layer.digest, files }))));
        const manifest: Manifest = { ...source, environment: to.environment, workspace: to.workspace, layers };
        yield* bucket.putJson(manifestKey(to, manifest.snapshot), manifest);
        yield* bucket.putJson(`${prefixOf(to)}/${CURRENT_FILE}`, manifest);
        return manifest;
      }).pipe(
        Effect.onError(() =>
          forgetNow(to).pipe(Effect.catch((error) => Effect.logWarning(`copying a snapshot to ${prefixOf(to)} failed, and forgetting what it wrote failed too: ${error.message}`)))
        ),
      );
      return written;
    });

  const copy = (from: WorkspaceRef, to: WorkspaceRef): Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError> =>
    exclusive([from, to], copyNow(from, to)).pipe(
      Effect.tap((manifest) =>
        Effect.sync(() => copies.add(1, { "alasio.node_agent.copy.outcome": "copied" })).pipe(
          Effect.andThen(Effect.logInfo(`copied the snapshot ${manifest.snapshot} of ${prefixOf(from)} to ${prefixOf(to)}`)),
        )
      ),
      Effect.tapError((error) => Effect.sync(() => copies.add(1, { "alasio.node_agent.copy.outcome": error._tag }))),
      withAlasioSpan("alasio.node_agent.copy", {
        attributes: { ...spanAttributes(to), "alasio.source.environment": from.environment, "alasio.source.workspace": from.workspace },
      }),
    );

  /** The image `layer` of the workspace's, from the bucket into the cache, each file checked against the manifest's. */
  const download = (ref: WorkspaceRef, snapshot: string, layer: Layer): Effect.Effect<void, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace } = ref;
      const directory = yield* cache.stage(environment, workspace, snapshot);
      for (const file of layer.files) {
        const target = join(directory, file.path);
        yield* onDisk(() => mkdir(dirname(target), { recursive: true }));
        const received = yield* bucket.getFile(layerKey(ref, layer.digest, file.path), target, file.bytes);
        if (received.bytes !== file.bytes || received.sha256 !== file.sha256) {
          return yield* new SnapshotCorrupt({
            environment,
            workspace,
            reason: `its image ${layer.digest}'s file ${file.path} is ${received.bytes} bytes of SHA-256 ${received.sha256}, where its manifest says ${file.bytes} bytes of ${file.sha256}`,
          });
        }
      }
      yield* cache.commit(environment, workspace, snapshot, layer.digest);
    });

  /** Whether the cache's copy of the image `layer` of the workspace's is what the manifest says, file by file; one that is not is removed, said so. */
  const intact = (ref: WorkspaceRef, layer: Layer): Effect.Effect<boolean, CacheError> =>
    Effect.gen(function*() {
      const { environment, workspace } = ref;
      if (!(yield* cache.holds(environment, workspace, layer.digest))) return false;
      if (sameFiles(yield* onDisk(() => imageFiles(cache.image(environment, workspace, layer.digest))), layer)) return true;
      yield* Effect.logWarning(`the node's cached copy of the image ${layer.digest} of ${prefixOf(ref)} is not what its manifest says: it is read again from the bucket`);
      yield* cache.clear(environment, workspace);
      return false;
    });

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
        const started = yield* Clock.currentTimeMillis;
        // A copy the cache holds is read again against its manifest: what the disk keeps may have changed since it was written.
        const missing: Layer[] = [];
        for (const layer of manifest.layers) if (!(yield* intact(ref, layer))) missing.push(layer);
        if (missing.length > 0) yield* cache.room(bytesOf(missing), ref).pipe(Effect.flatMap(logEvicted));
        yield* Effect.forEach(missing, (layer) => download(ref, snapshot, layer), { discard: true }).pipe(
          Effect.onError(() => cache.clear(environment, workspace).pipe(Effect.ignore)),
        );
        const imagePath = yield* cache.prepare(environment, workspace, snapshot, manifest.layers.map(({ digest }) => digest));
        yield* onDisk(() => writeFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), JSON.stringify(manifest)));
        prepared.set(prefixOf(ref), manifest);
        const preparedMs = (yield* Clock.currentTimeMillis) - started;
        yield* evict.pipe(Effect.catch((error) => Effect.logWarning(error.message)));
        const source = missing.length === 0 ? "cache" : "bucket";
        yield* Effect.sync(() => prepares.add(1, { "alasio.node_agent.prepare.outcome": "prepared", "alasio.node_agent.prepare.source": source }));
        yield* Effect.annotateCurrentSpan({ "alasio.snapshot.source": source, "alasio.snapshot.bytes": manifest.bytes, "alasio.snapshot.prepare_ms": preparedMs });
        yield* Effect.logInfo(`prepared the snapshot ${snapshot} of ${prefixOf(manifest)} from the ${source}: ${manifest.bytes} bytes in ${preparedMs} ms`);
        return { manifest, imagePath };
      }),
    ).pipe(
      Effect.tapError((error) => Effect.sync(() => prepares.add(1, { "alasio.node_agent.prepare.outcome": error._tag }))),
      withAlasioSpan("alasio.node_agent.prepare", { attributes: spanAttributes(ref) }),
    );

  /**
   * Collects what of the workspace's no snapshot needs any more, as the module says: the
   * digests of the images it keeps. The manifests, by their snapshots' ids, sort as they
   * were taken; each is superseded as the one after it is written.
   */
  const collectNow = (ref: WorkspaceRef): Effect.Effect<readonly string[], S3Error | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const prefix = prefixOf(ref);
      const now = yield* Clock.currentTimeMillis;
      const listed = yield* bucket.list(`${prefix}/`);
      const recent = ({ writtenAt }: { readonly writtenAt: number }) => now - writtenAt < COLLECT_GRACE_MS;
      const manifests = listed.filter(({ key }) => key.startsWith(`${prefix}/${MANIFESTS_DIR}/`));
      const latest = manifests.at(-1);
      const kept = manifests.filter((each, index) => each === latest || recent(manifests[index + 1] ?? each));
      const named = yield* Effect.forEach(kept, ({ key }) => manifestAt(ref, key));
      const committed = yield* current(ref);
      const images = new Set([...named, committed].flatMap((manifest) => manifest?.layers.map(({ digest }) => digest) ?? []));
      const stale = listed.filter((each) => {
        if (each.key.startsWith(`${prefix}/${MANIFESTS_DIR}/`)) return !kept.includes(each);
        const [, digest = ""] = each.key.slice(prefix.length + 1).split("/");
        return each.key.startsWith(`${prefix}/${LAYERS_DIR}/`) && !images.has(digest) && !recent(each);
      });
      yield* Effect.forEach(stale, ({ key }) => bucket.remove(key), { discard: true });
      if (stale.length > 0) yield* Effect.logInfo(`collected ${stale.length} objects of ${prefix} that no snapshot needs any more`);
      return [...images].sort();
    });

  /** What of its rules a pod's address lacks in its family's filter table on the node, as ./kube-router.ts says, or null. */
  const policyGap = (address: PodAddress): Effect.Effect<string | null, HostCommandError> =>
    onNode(host, [isIPv6(address.ip) ? "ip6tables-save" : "iptables-save", "-t", "filter"]).pipe(Effect.map(({ stdout }) => egressGap(stdout, address)));

  /** Waits for every address of the pod's to have its rules, POLICY_WAIT_MS at most: how long it waited, in milliseconds. */
  const policiesInForce = ({ namespace, pod, podIPs, policies }: ResumeRequest): Effect.Effect<number, PoliciesNotInForce | HostCommandError> =>
    Effect.gen(function*() {
      const start = yield* Clock.currentTimeMillis;
      for (;;) {
        const gaps = yield* Effect.forEach(podIPs, (ip) => policyGap({ namespace, pod, ip, policies }).pipe(Effect.map((reason) => (reason === null ? [] : [{ ip, reason }]))));
        const [gap] = gaps.flat();
        const waited = (yield* Clock.currentTimeMillis) - start;
        if (gap === undefined) return waited;
        if (waited >= POLICY_WAIT_MS) return yield* new PoliciesNotInForce({ namespace, pod, ...gap });
        yield* Effect.sleep(POLICY_POLL);
      }
    });

  /**
   * Forgets the connections tracked from `ip` on the node. kube-router lets the packets of
   * a tracked connection through before any policy (KUBE-NWPLCY-COMMON), so one tracked
   * from the address before its rules were in force, of a pod that had it before among
   * them, would go on through them. conntrack exits 1 where it deleted none, which is no
   * failure.
   */
  const forgetConnections = (ip: string): Effect.Effect<void, HostCommandError> =>
    onNode(
      host,
      ["conntrack", "-D", "-s", ip, ...(isIPv6(ip) ? ["-f", "ipv6"] : [])],
      ({ exitCode, stderr }) => exitCode === 0 || (exitCode === 1 && stderr.includes("0 flow entries have been deleted")),
    ).pipe(Effect.asVoid);

  /**
   * The manifest of the snapshot this node's cache last took or prepared of the workspace,
   * as it keeps it beside its images, for a resume an agent started since its prepare runs:
   * null where it holds none, or one that is no manifest of the workspace's.
   */
  const lastCached = (ref: WorkspaceRef): Effect.Effect<Manifest | null> =>
    Effect.promise(() => readFile(join(cache.workspace(ref.environment, ref.workspace), MANIFEST_FILE), "utf8").catch(() => null)).pipe(
      Effect.flatMap((text) => (text === null ? Effect.succeed(null) : Schema.decodeUnknownEffect(Schema.fromJsonString(Manifest))(text))),
      Effect.map((manifest) => (manifest !== null && prefixOf(manifest) === prefixOf(ref) ? manifest : null)),
      Effect.orElseSucceed(() => null),
    );

  /**
   * The file `name` of bayma's state directory `stateDir`, in the container, as `schema`
   * decodes it, read on the volume of `containers` that holds it, as an untrusted file
   * (./volume.ts), of STATE_FILE_BYTES at most, and of no property but the schema's.
   */
  const stateFile = <A>(stateDir: string, name: string, schema: Schema.Decoder<A>, containers: readonly RunningContainer[]): Promise<A> =>
    (async () => {
      const path = posix.join(stateDir, name);
      const held = (await Promise.all(containers.map(({ bundle }) => hostPathOf(nodeRoot, bundle, path)))).find((found): found is UnderRoot => found !== null);
      if (held === undefined) throw new Error(`no volume of its pod holds ${path}`);
      const bytes = await readUnder(held, STATE_FILE_BYTES);
      try {
        return Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(UTF8.decode(bytes));
      } catch (error) {
        throw new Error(`${path} is none of bayma's: ${(error instanceof Error ? error.message : String(error)).replace(/\s*\n\s*/gu, " ")}`);
      }
    })();

  /** What the live save of the sandbox `request` names caught, as bayma's last-save.json says. */
  const lastSave = (request: CheckpointRequest, { stateDir }: LiveSave, containers: readonly RunningContainer[]): Effect.Effect<Caught, SnapshotCorrupt> =>
    Effect.tryPromise({
      try: async () => {
        const { generation, running } = await stateFile(stateDir, "last-save.json", LastSaveFile, containers);
        return { stateDir, generation, running };
      },
      catch: (cause) =>
        new SnapshotCorrupt({ environment: request.environment, workspace: request.workspace, reason: `what its live save caught cannot be told: ${cause instanceof Error ? cause.message : String(cause)}` }),
    });

  /**
   * Whether `manifest`, taken live, holds the session still, a restored sandbox let run from
   * it: bayma's state generation is the one the save caught, as its generation.json says on
   * the volume of the pod's containers that holds it. Fails where it has moved on, or where
   * it cannot be told.
   */
  const holdsStill = (manifest: Manifest, caught: Caught, containers: readonly RunningContainer[]): Effect.Effect<void, SnapshotOutlived> =>
    Effect.tryPromise({
      try: async () => {
        const { generation } = await stateFile(caught.stateDir, "generation.json", StateGenerationFile, containers);
        if (generation !== caught.generation) throw new Error(`bayma's state generation is ${generation}, and was ${caught.generation} as it was saved`);
      },
      catch: (cause) =>
        new SnapshotOutlived({
          environment: manifest.environment,
          workspace: manifest.workspace,
          snapshot: manifest.snapshot,
          reason: cause instanceof Error ? cause.message : String(cause),
        }),
    });

  const resume = (request: ResumeRequest): Effect.Effect<void, ResumeError> =>
    Effect.gen(function*() {
      const waited = yield* policiesInForce(request);
      yield* Effect.annotateCurrentSpan({ "alasio.resume.policy_wait_ms": waited });
      const containers = yield* Effect.forEach(request.containerIds, (id) => ofPod(id, request.namespace, request.pod));
      const restoredFrom = prepared.get(prefixOf(request)) ?? (yield* lastCached(request));
      if (restoredFrom?.live !== undefined) yield* holdsStill(restoredFrom, restoredFrom.live, containers);
      yield* Effect.forEach(request.podIPs, forgetConnections, { discard: true });
      yield* Effect.forEach(request.containerIds, (id, index) => runsc.resume(containers[index]?.release ?? "", id), { discard: true });
      // Its sandbox runs from the snapshot prepared of it, which its next checkpoint saves what changed since.
      if (restoredFrom !== null) for (const id of request.containerIds) bases.set(id, restoredFrom);
      return waited;
    }).pipe(
      Effect.tap((waited) =>
        Effect.sync(() => resumes.add(1, { "alasio.node_agent.resume.outcome": "resumed" })).pipe(
          Effect.andThen(Effect.logInfo(`resumed the sandbox of ${prefixOf(request)} in ${request.namespace}/${request.pod}, its network policies in force after ${waited} ms`)),
        )
      ),
      Effect.tapError((error) => Effect.sync(() => resumes.add(1, { "alasio.node_agent.resume.outcome": error._tag })).pipe(Effect.andThen(Effect.logWarning(error.message)))),
      Effect.asVoid,
      withAlasioSpan("alasio.node_agent.resume", {
        attributes: { ...spanAttributes(request), "k8s.namespace.name": request.namespace, "k8s.pod.name": request.pod },
      }),
    );

  /** Kills what runsc runs for the containers of the pod `request` names, as the module says: what it killed, of how many it found. */
  const abandonNow = (request: AbandonRequest): Effect.Effect<readonly [readonly Killed[], number], RunscError> =>
    Effect.gen(function*() {
      const containers = (yield* runsc.containers).filter(({ annotations }) => podOf(annotations) === `${request.namespace}/${request.pod}`);
      const found = yield* runsc.processes(containers);
      const killed = yield* Effect.forEach(found, (each) => runsc.kill(each).pipe(Effect.map((done) => (done ? [{ containerId: each.containerId, pid: each.pid }] : []))));
      return [killed.flat(), found.length] as const;
    });

  const abandon = (request: AbandonRequest): Effect.Effect<readonly Killed[], RunscError> =>
    exclusive([request], abandonNow(request)).pipe(
      Effect.tap(([killed, found]) =>
        Effect.sync(() => abandons.add(1, { "alasio.node_agent.abandon.outcome": "abandoned" })).pipe(
          Effect.andThen(Effect.annotateCurrentSpan({ "alasio.abandon.killed": killed.length })),
          Effect.andThen(Effect.logInfo(
            `abandoned the restore of ${prefixOf(request)} in ${request.namespace}/${request.pod}: ${
              killed.length === 0 ? "runsc ran nothing for it" : `killed ${killed.map(({ containerId, pid }) => `${pid} of ${containerId}`).join(", ")}`
            }${found > killed.length ? `; of those found, ${found - killed.length} had ended since, their pids gone or another process's` : ""}`,
          )),
        )
      ),
      Effect.map(([killed]) => killed),
      Effect.tapError((error) => Effect.sync(() => abandons.add(1, { "alasio.node_agent.abandon.outcome": error._tag })).pipe(Effect.andThen(Effect.logWarning(error.message)))),
      withAlasioSpan("alasio.node_agent.abandon", {
        attributes: { ...spanAttributes(request), "k8s.namespace.name": request.namespace, "k8s.pod.name": request.pod },
      }),
    );

  const holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError> = Effect.gen(function*() {
    const releases = yield* Effect.forEach(yield* runsc.releases, (release) => runsc.cpuFeatures(release).pipe(Effect.map((cpuFeatures) => ({ release, cpuFeatures }))));
    const cached = (yield* cache.list).map(({ usedAt, ...rest }) => ({ ...rest, usedAt: new Date(usedAt).toISOString() }));
    return { node, releases, cached };
  });

  // The cache returns within its budget on its own too, as what was used within the grace passes it, though no checkpoint or prepare runs.
  yield* evict.pipe(
    Effect.catch((error) => Effect.logWarning(error.message)),
    Effect.repeat(Schedule.spaced(EVICT_POLL)),
    withLogScope("node-agent"),
    Effect.forkScoped,
  );

  return {
    checkpoint: (request) => checkpoint(request).pipe(withLogScope("node-agent")),
    prepare: (ref) => prepare(ref).pipe(withLogScope("node-agent")),
    current,
    copy: (from, to) => copy(from, to).pipe(withLogScope("node-agent")),
    spend: (ref, snapshot) =>
      exclusive(
        [ref],
        current(ref).pipe(
          Effect.flatMap((manifest) => (manifest?.snapshot === snapshot ? bucket.remove(`${prefixOf(ref)}/${CURRENT_FILE}`).pipe(Effect.as(true)) : Effect.succeed(false))),
        ),
      ).pipe(
        Effect.tap((spent) => (spent ? Effect.logInfo(`the snapshot ${snapshot} of ${prefixOf(ref)} is spent`) : Effect.void)),
        Effect.asVoid,
        withAlasioSpan("alasio.node_agent.spend", { attributes: spanAttributes(ref) }),
        withLogScope("node-agent"),
      ),
    forget: (ref) =>
      exclusive([ref], forgetNow(ref)).pipe(
        Effect.tap(() => Effect.logInfo(`forgot the snapshots of ${prefixOf(ref)}`)),
        withAlasioSpan("alasio.node_agent.forget", { attributes: spanAttributes(ref) }),
        withLogScope("node-agent"),
      ),
    collect: (ref) =>
      exclusive([ref], collectNow(ref)).pipe(
        withAlasioSpan("alasio.node_agent.collect", { attributes: spanAttributes(ref) }),
        withLogScope("node-agent"),
      ),
    resume: (request) => resume(request).pipe(withLogScope("node-agent")),
    abandon: (request) => abandon(request).pipe(withLogScope("node-agent")),
    holdings,
    workspaces: (environment) =>
      bucket.list(`${environment}/`).pipe(
        // What else the bucket holds under it is no workspace's of the agent's.
        Effect.map((listed) => [...new Set(listed.map(({ key }) => key.slice(environment.length + 1).split("/")[0] ?? ""))].filter((name) => NAME.test(name)).sort()),
      ),
    discard: (ref, keep) =>
      exclusive([ref], cache.discard(ref.environment, ref.workspace, keep)).pipe(
        Effect.tap((discarded) => (discarded ? Effect.logInfo(`discarded from the node's cache the images of ${prefixOf(ref)} no snapshot needs any more`) : Effect.void)),
        withLogScope("node-agent"),
      ),
  };
});

/** The pod a container is of, `<namespace>/<name>`, as the annotations containerd gives its spec say, `?` for what they do not. */
function podOf(annotations: Readonly<Record<string, string>>): string {
  return `${annotations["io.kubernetes.cri.sandbox-namespace"] ?? "?"}/${annotations["io.kubernetes.cri.sandbox-name"] ?? "?"}`;
}

/** The attributes of a span of an operation on a workspace. */
function spanAttributes({ environment, workspace }: WorkspaceRef): Record<string, string> {
  return { "alasio.environment": environment, "alasio.workspace": workspace };
}
