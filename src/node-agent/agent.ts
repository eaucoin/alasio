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
 * - `current.json`: the manifest of the workspace's current snapshot, written last, or, of a
 *   checkpoint taken in one pass, beside its manifest, which commits it. A workspace without
 *   one has no snapshot to restore.
 *
 * A checkpoint ends the sandbox it saves. Once it has begun, the workspace's current
 * snapshot no longer holds what the workspace is, whose files and processes have moved
 * on since it was taken, so a checkpoint that fails forgets it rather than leave it to be
 * restored. A snapshot restored is spent: no longer current, its images go on as the layers
 * of the next; its commit is removed only as the bucket holds it as it was read, by its ETag,
 * so a commit another node's agent writes meanwhile stands. A copy gives a workspace another's
 * current snapshot, as a session forked from another restores what the other was: its images
 * copied in the bucket, then its manifest, then its commit, as a checkpoint writes them. A
 * checkpoint's commit, and a copy's, are written whatever the bucket holds, as each is the
 * workspace's latest, of its one sandbox or of a fork none runs from yet, and read nothing it
 * holds; a compaction's, which compacts the snapshot it read, only as the bucket holds that one.
 * A release's checkpoint of a sandbox whose restore from the bucket still reads it waits for
 * the pages it saves as they load, as long as the restore asks, which one of a store away does
 * as long as its sandbox runs: once those reads have stalled, or go too slowly to end in time
 * (./names.ts LOADING_SAVE_FLOOR_SECONDS), that restore's token is revoked, so its loading
 * fails, and the checkpoint with it (boundWhileLoading). A live checkpoint of such a sandbox is
 * not taken (LiveSaveDeferred): it would pause an exec that runs for as long as the pages it
 * saves take to load, for a snapshot its release takes anyway.
 *
 * A snapshot a release's checkpoint commits whose chain of images is due (compactionDue)
 * is compacted, after its commit and beside whatever else the agent does, one at a time on
 * the node at its lowest priority, yielding to the restores on the node that read their
 * images (./loading.ts): its images into one, the image of a new snapshot
 * `compactedFrom` it, committed as the current one only if the workspace has not moved on
 * from it meanwhile, then recorded beside the workspace's files (NodeAgentOptions compacted),
 * as the gateway restores a workspace released into the one from that record, reading nothing
 * of the bucket; a restore of the workspace on this node stops it, as the workspace moves on
 * from it with the restore, its runsc killed and what it wrote of its image removed from the
 * cache and the bucket. It is the same sandbox at the same moment, so a workspace released into
 * the one is restored from the other, which is the next delta's base. A snapshot taken live
 * is never compacted, its sandbox running on from it: one whose parent's chain would be due
 * with it is taken whole instead, which cuts the chain the sandbox goes on extending.
 *
 * What no snapshot needs any more is collected, by one caller (alasio's collection): the
 * images and manifests that neither the current snapshot, nor the latest, which a sandbox
 * restored from it may still extend, nor the one the latest was compacted from, which one
 * restored from it may extend as well, nor one superseded within COLLECT_GRACE_MS names, nor
 * written within it, as a commit writes its images before its manifest: one whose first
 * upload began that long ago, less CLOCK_SKEW_MS, is refused. A restore or a copy that read
 * a manifest just before it was superseded reads its images within that.
 *
 * A sandbox is restored paused, in a pod of a new address, whose network policies
 * kube-router puts in force on the node as it next syncs (./kube-router.ts): until then
 * what the pod sends goes through, whatever they say. Its resume, asked as its pod is made,
 * waits for the pod to run its containers, as the API server says it with its addresses,
 * which kube-router reads; then for its policies, then forgets the connections tracked from
 * its addresses, then resumes its containers; then, before it is answered, waits for the
 * pod to accept connections on the port its bayma serves on, from the node, as kubelet's
 * readiness probe finds it ready, so that what serves the pod once its resume is answered
 * waits on no probe of kubelet's. One whose pod does not run within
 * RESTORE_RUNNING_WAIT_MS, or whose policies are not in force within POLICY_WAIT_MS, leaves
 * it paused.
 *
 * A restore alasio refuses before it resumes the sandbox is abandoned: the agent ends what
 * of it runs, so that the pod's teardown waits on none of it, its grace among it, and the
 * node's room it holds is freed at once. A container runsc holds paused, as each of a sandbox
 * restored whole is until its resume, is killed through runsc, as its shim would kill it
 * (`runsc kill`, SIGKILL), which ends every process of the sandbox and says each of its
 * containers ended, as kubelet then finds them: killed past runsc, its sandbox would end with
 * runsc saying its containers paused still, until a command of runsc's next reads their state
 * and finds the sandbox gone; and left to the pod's teardown, it would end only at its grace's
 * end, as kubelet's SIGTERM waits for a resume that never comes. Then the agent kills what
 * runsc still runs for the pod's containers, their sandbox and gofers among it: a
 * `runsc restore` that failed has left them running, its shim then tearing none of them down.
 * A container is the pod's only where the annotations containerd gives its spec name the pod
 * and its uid, so that the containers of a pod of its name made since are left be. A process
 * is a container's only where its arguments name runsc's root, the container's bundle and its
 * id, and is killed only while its start time is the one it was found with, so a pid the node
 * has given another process since is left be (./runsc.ts). One forked meanwhile by a process
 * the agent kills has the arguments of the one it forked from until it execs, so the agent
 * scans again after its kills, until a scan kills nothing new. This stays as defence should a
 * restore leave runsc's processes behind.
 *
 * One operation on a workspace's snapshots runs at a time. The node's cache evicts nothing of
 * a workspace an operation runs on or waits to, nor of one a compaction writes, nor discards
 * it: what they read and write there, an image staged or the parent a delta is saved from,
 * stays until they end.
 */
import { createHash, randomBytes } from "node:crypto";
import { constants, createReadStream } from "node:fs";
import { mkdir, open, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { isIP, isIPv6 } from "node:net";
import { dirname, join, posix, relative } from "node:path";
import { pipeline } from "node:stream/promises";
import { Worker } from "node:worker_threads";

import type { V1Pod } from "@kubernetes/client-node";
import type { BatchObservableResult } from "@opentelemetry/api";
import { Cause, Clock, type Duration, Effect, Exit, FiberMap, Option, RcMap, Schedule, Schema, type Scope, Semaphore, Stream, SubscriptionRef } from "effect";

import type { KubeApiError } from "../kube/client.ts";
import { quantityOf } from "../kube/quantity.ts";
import { NAME, SNAPSHOT_ID } from "../kube/sandbox-shape.ts";
import { currentTraceparent, meter, withAlasioSpan, withWaitSpan } from "../telemetry/index.ts";
import { withLogScope } from "../shared/log.ts";
import { CacheError, type CachedWorkspace, DIGEST, LAYERS_DIR, type SnapshotCache } from "./cache.ts";
import { type HostCommandError, type HostCommands, onNode } from "./host.ts";
import { type Listing, listedEgressGap, type PodAddress } from "./kube-router.ts";
import { makeLoading } from "./loading.ts";
import {
  backgroundReadSeconds,
  LOAD_WAIT_WATCHDOG_MARGIN_SECONDS,
  LOADING_SAVE_FLOOR_SECONDS,
  LOADING_SAVE_LATEST_SECONDS,
  LOADING_STALL_SECONDS,
  RESTORE_RUNNING_WAIT_MS,
  restoresFromBucket,
  runscVersion,
  SAVE_HOOK_SECONDS,
  SAVE_LOAD_WAIT_SECONDS,
  SAVES_DEFERRED_FOR_SECONDS,
  savesSummary,
  savesWithLoadWait,
  STORE_SOCKET_ENDPOINT,
} from "./names.ts";
import { type ListedContainer, type ReleaseNotInstalled, type Runsc, RunscError, type RunningContainer, summaryOf, VERIFY_STATUSES } from "./runsc.ts";
import { type S3Bucket, S3Error } from "./s3.ts";
import type { StoreProxy, StoreToken, Transport } from "./store-proxy.ts";
import { hostPathOf, readUnder, type UnderRoot } from "./volume.ts";

/** An environment's or a workspace's name: a DNS label, as their namespaces' names are. */
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
  /**
   * How durable it was as it was committed: `store`, its images and its commit in the store, and
   * its images synced to the node's disk before the commit. Absent: it was saved before durability
   * was recorded, and its images were not synced to the node's disk; in the store all the same.
   */
  durability: Schema.optional(Schema.Literal("store")),
  /** For a snapshot taken live, what it caught: it holds the session only while bayma's state generation is that one. */
  live: Schema.optional(Caught),
  /**
   * For a snapshot a release took, what its save caught: it holds the session only while bayma's
   * state generation is that one, as a live one's does. Absent: it was taken before what a
   * release caught was recorded, of a sandbox whose request named no state directory, or by a
   * save whose last-save.json could not be read once it had ended its sandbox.
   */
  caught: Schema.optional(Caught),
  /**
   * For a snapshot of a compacted image, the snapshot whose memory it holds, as a workspace
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
 * MAX_CHAIN_DEPTH images, each a file a restore opens and looks pages up in. With `adding`,
 * the images to be added to the chain, not yet written, which count to how many it reads
 * from alone: whether a snapshot of them and `layers` would be.
 */
export function compactionDue(layers: readonly Layer[], adding = 0): string | null {
  const depth = layers.length + adding;
  if (depth > MAX_CHAIN_DEPTH) return `it reads from ${depth} images, more than ${MAX_CHAIN_DEPTH}`;
  if (layers.length < 2) return null;
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

/** How a restore's checkpoint gofer reads the snapshots bucket, as alasio asks it (./names.ts Gofer, goferOf). */
export const GoferRequest = Schema.Struct({ transport: Schema.Literals(["socket", "tcp"]), pagesRetry: Schema.Boolean });

/**
 * A request to prepare a workspace's snapshot for a restore: its manifest, the workspace's
 * current one as alasio read it, which the restore waits on no second read of; and how its
 * checkpoint gofer reads the bucket, if it does: over TCP, its reads tried as many times as
 * the gofer tries them, unless asked otherwise.
 */
export const PrepareRequest = Schema.Struct({ ...WorkspaceRef.fields, manifest: Manifest, gofer: Schema.optional(GoferRequest) }).check(
  Schema.makeFilter(({ environment, workspace, manifest }) => (manifest.environment === environment && manifest.workspace === workspace) || "its manifest must be the workspace's"),
);
export type PrepareRequest = typeof PrepareRequest.Type;

/** A request to copy a workspace's current snapshot to another. */
export const CopyRequest = Schema.Struct({ from: WorkspaceRef, to: WorkspaceRef });
export type CopyRequest = typeof CopyRequest.Type;

/** A container, by containerd's id. */
const ContainerId = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/u));

/** A pod's name: a DNS subdomain. */
const PodName = Schema.String.check(Schema.isPattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$/u), Schema.isMaxLength(253));

/** A pod's uid, as Kubernetes gives one: a UUID. */
const PodUid = Schema.String.check(Schema.isPattern(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u));

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
  /**
   * Of a release's save, the state directory, in the container, of the bayma it runs, where
   * bayma's hook says what the save caught (last-save.json), which the snapshot records
   * (Manifest caught): never of a live save, whose `live` says it.
   */
  stateDir: Schema.optional(AbsolutePath),
  /**
   * Whether the pod ends as its sandbox is saved, its bayma holding its stop for the save: a
   * release whose save is not deferred (loadWaitOf), as the pod's end ends the sandbox that a
   * deferral would leave running, but bounded while its sandbox loads (boundWhileLoading).
   */
  ending: Schema.optional(Schema.Boolean),
});
export type CheckpointRequest = typeof CheckpointRequest.Type;

/** A request to resume a workspace's sandbox, restored paused in a pod on this node, once the pod runs. */
export const ResumeRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The pod's namespace and name, as its network policies' rules name it, which its containers must be of. */
  namespace: Name,
  pod: PodName,
  /** The network policies of its namespace that select the pod, by name, which its rules must run what it sends through. */
  policies: Schema.Array(PodName).check(Schema.isMaxLength(64)),
  /** The snapshot its sandbox was restored from, whose manifest says whether it may run. */
  snapshot: SnapshotId,
  /** The port its bayma serves on, which the pod accepts connections on once it is ready. */
  port: Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 65_535 })),
});
export type ResumeRequest = typeof ResumeRequest.Type;

/** A pod restored into, running: its addresses, one of each family it has, in their canonical form, and its containers, by containerd's ids, as the API server says them. */
interface RunningPod {
  readonly podIPs: readonly string[];
  readonly containerIds: readonly string[];
}

/** A request to abandon a workspace's restore, which alasio refused before it resumed its sandbox, in a pod on this node. */
export const AbandonRequest = Schema.Struct({
  environment: Name,
  workspace: Name,
  /** The pod restored into, whose containers' processes of runsc's are killed. */
  namespace: Name,
  pod: PodName,
  /** That pod's uid, which tells it from a pod of its name made since. */
  uid: PodUid,
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
  /** Where the restore reads the image: the node's cache, or the bucket, as it goes. */
  readonly source: "cache" | "bucket";
}

/** What this node holds: its gVisor releases with the CPU features each sees here, and the workspaces whose images its cache holds, by their digests. */
export interface Holdings {
  readonly node: string;
  readonly releases: readonly { readonly release: string; readonly cpuFeatures: readonly string[] }[];
  readonly cached: readonly { readonly environment: string; readonly workspace: string; readonly images: readonly string[]; readonly bytes: number; readonly usedAt: string }[];
}

/** The tag of what a node holds, `holdings`, which changes as it does: its SHA-256, in hex. Pure, for tests. */
export const holdingsTag = (holdings: Holdings): string => createHash("sha256").update(JSON.stringify(holdings)).digest("hex");

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

/**
 * The agent is stopping, so it prepares no restore of the workspace from the bucket, which
 * would read through it after it stopped (./store-proxy.ts): the next agent of its node does.
 */
export class AgentStopping extends Schema.TaggedError<AgentStopping>()("AgentStopping", {
  environment: Schema.String,
  workspace: Schema.String,
}) {
  override get message(): string {
    return `the node agent is stopping, so it prepares no restore of ${this.environment}/${this.workspace} from the bucket: its next one does`;
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

/**
 * A snapshot of a workspace's is not committed, its first upload having begun so long ago,
 * COLLECT_GRACE_MS less CLOCK_SKEW_MS or longer, that a collection may have taken what it
 * uploaded, which no manifest names until the commit.
 */
export class CommitOverdue extends Schema.TaggedError<CommitOverdue>()("CommitOverdue", {
  environment: Schema.String,
  workspace: Schema.String,
  snapshot: Schema.String,
  uploadedMsAgo: Schema.Number,
}) {
  override get message(): string {
    return `the snapshot ${this.snapshot} of ${this.environment}/${this.workspace} is not committed: its upload began ${Math.floor(this.uploadedMsAgo / 1000)} s ago, ${(COLLECT_GRACE_MS - CLOCK_SKEW_MS) / 60_000} min or more, by when a collection may have taken what it uploaded`;
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
 * A sandbox gVisor saves no more, as runsc says (PAGES_UNREAD): restored from the bucket, its
 * restore failed to read some of its pages in the background, which it never will, and so
 * every checkpoint of it fails. Its snapshot is as a checkpoint that failed leaves it.
 */
export class SandboxUnsaveable extends Schema.TaggedError<SandboxUnsaveable>()("SandboxUnsaveable", {
  environment: Schema.String,
  workspace: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the sandbox of ${this.environment}/${this.workspace} can be saved no more, its restore having failed to read some of its pages: ${this.reason}`;
  }
}

/**
 * A live checkpoint not taken, its sandbox's restore from the bucket reading it still: the save
 * would pause the exec that runs for as long as the pages it saves take to load, for a snapshot
 * its release takes anyway. Nothing of the workspace's snapshot changes.
 */
export class LiveSaveDeferred extends Schema.TaggedError<LiveSaveDeferred>()("LiveSaveDeferred", {
  environment: Schema.String,
  workspace: Schema.String,
  served: Schema.Number,
  expected: Schema.NullOr(Schema.Number),
}) {
  override get message(): string {
    const of = this.expected === null || !Number.isFinite(this.expected) ? "" : ` of ${this.expected}`;
    return `the sandbox of ${this.environment}/${this.workspace} is not saved live while its restore from the bucket reads it still, ${this.served}${of} bytes of its pages served: its release saves it`;
  }
}

/**
 * A save not taken, as runsc's wait for the pages that its sandbox's restore from the bucket has
 * not loaded yet expired (LOAD_WAIT_EXPIRED): the pages it saves, or those bayma's hook touched
 * as it ran for the save. What it wrote, incomplete, is unstaged with it, and the sandbox runs
 * on as it was, even where the save was to end it, its restore reading on. Nothing of the
 * workspace's snapshot changes, which the sandbox's next save builds on as before.
 */
export class SaveDeferred extends Schema.TaggedError<SaveDeferred>()("SaveDeferred", {
  environment: Schema.String,
  workspace: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the save of ${this.environment}/${this.workspace} is deferred, the pages it saves still loading from the bucket past its wait: ${this.reason}; its snapshot is kept, and its sandbox runs on`;
  }
}

/**
 * How long a save whose waits for pages still loading runsc bounds by `waitSeconds`
 * (./names.ts savesWithLoadWait) runs at least before the agent bounds it while its sandbox loads
 * all the same (boundWhileLoading), as a watchdog, in ms: its load wait; SAVE_HOOK_SECONDS, what
 * bayma's save hook bounds its own work to, the time it waits for stalled pages not counted, as
 * runsc kills it at the first stalled tick past the load wait; and
 * LOAD_WAIT_WATCHDOG_MARGIN_SECONDS for the rest, which runsc does not bound. Not the timeout
 * runsc gives the hook (restoreHookSeconds), which allows for a hook that waits on its pages as
 * they trickle in: one that trickles past SAVE_HOOK_SECONDS after the load wait may be revoked so, at
 * the latest at LOADING_SAVE_LATEST_SECONDS, and its session falls back as before.
 */
export const loadWaitWatchdogMs = (waitSeconds: number): number => (waitSeconds + SAVE_HOOK_SECONDS + LOAD_WAIT_WATCHDOG_MARGIN_SECONDS) * 1000;

/**
 * What runsc says of a save whose wait for pages still loading expired, at the end of its error,
 * from 20261102.0-eaucoin.10 on (./names.ts savesWithLoadWait): the wait for the pages it saves,
 * or bayma's hook's for those it touches, which runsc then killed.
 */
export const LOAD_WAIT_EXPIRED = "the save's load wait expired";

/** How a release's save of a sandbox whose restore reads the bucket still has gone, as boundWhileLoading looks at it: times in ms since the epoch, sizes in bytes. */
export interface LoadingSave {
  /** When the checkpoint was asked, and when runsc's save of it started, the workspace's lock held. */
  readonly askedAt: number;
  readonly startedAt: number;
  /** Now, and when the restore was last served a byte of its pages, or the save started, if later. */
  readonly now: number;
  readonly movedAt: number;
  /** The bytes of pages it had been served as the save started, and has been now; and those it reads, null or Infinity where not known. */
  readonly servedAtStart: number;
  readonly served: number;
  readonly expected: number | null;
  /** When the window its recent rate is measured over began (recentReads), and the bytes it had been served then. */
  readonly recentFrom: number;
  readonly servedAtRecentFrom: number;
}

/**
 * Why the token of the restore `save` is of is to be revoked now, as boundWhileLoading says;
 * null where it is not: its save asked `latestMs` ago or longer; or, once it has run `floorMs`,
 * no byte of the restore's pages served for `stallMs`, or those left not served before
 * `latestMs` from its ask at the rate they were served since it started, nor at the rate of
 * its recent window (recentReads), so that a pause shorter than `stallMs`, which drags the
 * one down, does not have a store that serves the rest in time since cut.
 */
export function loadingSaveRevocation(save: LoadingSave, { floorMs, stallMs, latestMs }: { readonly floorMs: number; readonly stallMs: number; readonly latestMs: number }): string | null {
  const { askedAt, startedAt, now, movedAt, servedAtStart, served, expected, recentFrom, servedAtRecentFrom } = save;
  const seconds = (ms: number) => `${Math.round(ms / 1000)} s`;
  if (now - askedAt >= latestMs) return `its release's save, asked ${seconds(now - askedAt)} ago, may wait on it no longer, ${seconds(latestMs)} at the most`;
  if (now - startedAt < floorMs) return null;
  if (now - movedAt >= stallMs) return `no byte of its pages has been served for ${seconds(now - movedAt)} while its release's save, running ${seconds(now - startedAt)}, waits`;
  if (expected === null || !Number.isFinite(expected) || served >= expected || now <= recentFrom) return null;
  const left = expected - served;
  const late = (rate: number) => rate <= 0 || now + left / rate > askedAt + latestMs;
  const rate = (served - servedAtStart) / (now - startedAt);
  const recent = (served - servedAtRecentFrom) / (now - recentFrom);
  if (rate <= 0 || !late(rate) || !late(recent)) return null;
  const takes = (at: number) => (at > 0 ? `in ${seconds(left / at)}` : "never");
  return `its ${left} bytes of pages left would be served only past the ${seconds(askedAt + latestMs - now)} its release's save may wait on them still: ${takes(rate)} at the ${Math.round(rate * 1000)} bytes a second served since the save started ${seconds(now - startedAt)} ago, ${takes(recent)} at the ${Math.round(recent * 1000)} bytes a second of the last ${seconds(now - recentFrom)}`;
}

/** What boundWhileLoading saw of a restore's reads at one of its looks: when, and the bytes of pages it had been served. */
export interface ReadsSeen {
  readonly at: number;
  readonly served: number;
}

/**
 * The share of the stall window a restore's recent window spans at least once its reads have
 * gone on after a pause (recentReads): a third, 30 s of 90 s, so that the burst one look finds
 * after a short pause is not taken for the rate of a store that serves in bursts, which would
 * hold the rate rule off for as long as it keeps pausing.
 */
const RECENT_LEAST_SHARE = 1 / 3;

/**
 * Where the window a restore's recent rate is measured over begins, of `seen`, its looks in
 * order, the first as its save started: the look it begins at. It begins as its last pause
 * ended, at the last look that found no more served than the one before, but no further back
 * than the last look `stallMs` or more before the last, nor later than the last look a third
 * of that (RECENT_LEAST_SHARE) or more before the last, or the first. Where the last look
 * found no more served, the window is that look alone, empty: a pause is the stall rule's.
 */
export function recentReads(seen: readonly ReadsSeen[], stallMs: number): ReadsSeen {
  const first = seen[0];
  const last = seen.at(-1);
  if (first === undefined || last === undefined) throw new Error("recentReads needs a look");
  let resumed = first;
  for (let index = seen.length - 1; index > 0; index -= 1) {
    if (seen[index]!.served <= seen[index - 1]!.served) {
      resumed = seen[index]!;
      break;
    }
  }
  if (resumed === last) return last;
  const windowed = seen.findLast((look) => look.at <= last.at - stallMs);
  const begins = windowed !== undefined && windowed.at > resumed.at ? windowed : resumed;
  const least = seen.findLast((look) => look.at <= last.at - stallMs * RECENT_LEAST_SHARE) ?? first;
  return begins.at > least.at ? least : begins;
}

/**
 * What runsc says of a checkpoint of a sandbox whose restore failed to read some of its pages
 * in the background: as a whole save waits for them all (pgalloc's AwaitLoadAll), and, from
 * 20261102.0-eaucoin.9 on, as a delta save waits for those of them it reads, the pages dirtied
 * as they loaded.
 */
export const PAGES_UNREAD = "previous async page loading failed";

/**
 * A sandbox restored from a snapshot whose save caught bayma's state generation, taken live or
 * by a release, is not let run, its snapshot holding the session no more: bayma's state
 * generation has moved on since, or cannot be told.
 */
export class SnapshotOutlived extends Schema.TaggedError<SnapshotOutlived>()("SnapshotOutlived", {
  environment: Schema.String,
  workspace: Schema.String,
  snapshot: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the snapshot ${this.snapshot} of ${this.environment}/${this.workspace} holds the session no more: ${this.reason}; its sandbox is left paused`;
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

/** A restored pod did not run its containers, with its addresses, within RESTORE_RUNNING_WAIT_MS, or went: which, and why. Its sandbox stays paused. */
export class PodNotRunning extends Schema.TaggedError<PodNotRunning>()("PodNotRunning", {
  namespace: Schema.String,
  pod: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.namespace}/${this.pod} does not run its containers, so its sandbox is left paused: ${this.reason}`;
  }
}

/**
 * How long a resumed sandbox's pod is waited for to accept connections on its bayma's port,
 * how long each try is given, as kubelet's probe gives one by default, and how long after one
 * that fails the next is: its netstack, restored whole, accepts at once, as it does for
 * kubelet's probe even while paused, so one that has not by then has no listener.
 */
export const ACCEPT_WAIT_MS = 10_000;
const ACCEPT_TRY_SECONDS = 1;
const ACCEPT_POLL_MS = 10;

/** A resumed sandbox's pod did not accept connections on its bayma's port within ACCEPT_WAIT_MS: which, on which address and port. */
export class NotAccepting extends Schema.TaggedError<NotAccepting>()("NotAccepting", {
  namespace: Schema.String,
  pod: Schema.String,
  address: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `${this.namespace}/${this.pod} did not accept connections on ${this.address} within ${ACCEPT_WAIT_MS / 1000} s of its resume, as kubelet's readiness probe would find it: ${this.reason}`;
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

/** A workspace's commit was written again between its read and a removal of it as read (spendNow). */
class WrittenMeanwhile extends Schema.TaggedError<WrittenMeanwhile>()("WrittenMeanwhile", {}) {}

/** How preparing a snapshot fails. */
export type PrepareError = Unrestorable | SnapshotCorrupt | AgentStopping | S3Error | CacheError | RunscError;

/** How resuming a sandbox fails. */
export type ResumeError = PodNotRunning | KubeApiError | PoliciesNotInForce | ContainerElsewhere | SnapshotOutlived | NotAccepting | HostCommandError | RunscError | ReleaseNotInstalled | S3Error | SnapshotCorrupt;

/** What a node agent does; see makeNodeAgent. */
export interface NodeAgent {
  /** Checkpoints a workspace's sandbox and makes it the workspace's snapshot: its manifest. */
  readonly checkpoint: (request: CheckpointRequest) => Effect.Effect<Manifest, CheckpointFailed | SandboxUnsaveable | LiveSaveDeferred | SaveDeferred>;
  /** The workspace's snapshot of the request's manifest in this node's cache, checked against it, for a restore on this node. */
  readonly prepare: (request: PrepareRequest) => Effect.Effect<Prepared, PrepareError>;
  /** The manifest of the workspace's current snapshot, or null. */
  readonly current: (workspace: WorkspaceRef) => Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt>;
  /** Makes the current snapshot of `from` that of `to` too: `to`'s manifest. One that fails as it writes leaves `to` without a snapshot. */
  readonly copy: (from: WorkspaceRef, to: WorkspaceRef) => Effect.Effect<Manifest, NoSnapshot | SnapshotCorrupt | S3Error | CacheError>;
  /**
   * Makes the workspace's current snapshot current no more, if it is `snapshot` still, or one
   * compacted from it, which is the same: one its sandbox was restored from, or one taken
   * live whose exec has ended. Its images go on as the layers of its next. One committed since
   * by any node's agent stands (spendNow).
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
  readonly abandon: (request: AbandonRequest) => Effect.Effect<readonly Killed[], RunscError | ReleaseNotInstalled>;
  readonly holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError>;
  /**
   * What this node holds, once it is other than what the tag `seen` names (holdingsTag), as
   * its cache changes, or at once where it is; null where it is not within `waitMs`.
   */
  readonly holdingsAfter: (seen: string, waitMs: number) => Effect.Effect<Holdings | null, RunscError | ReleaseNotInstalled | CacheError>;
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
  /**
   * The bucket as the checkpoint gofer of a restore reads it, from the node: through the
   * agent, by a token of the restore's own (./store-proxy.ts).
   */
  readonly storeProxy: StoreProxy;
  readonly host: HostCommands;
  /** The first gVisor release that restores straight from the bucket (restoresFromBucket); none where empty. */
  readonly restoresFromBucketSince: string;
  /** The first gVisor release that bounds a save's wait for pages still loading (savesWithLoadWait); none where empty. */
  readonly savesWithLoadWaitSince: string;
  /** The first gVisor release that writes a checkpoint's summary (savesSummary), which describes its image; none where empty. */
  readonly savesSummarySince: string;
  /**
   * How long a save of a release that bounds it waits for pages still loading, in seconds
   * (SAVE_LOAD_WAIT_SECONDS unless told), and how long a workspace's release saves may have been
   * deferred so before the next is bounded while its sandbox loads instead, in ms
   * (SAVES_DEFERRED_FOR_SECONDS unless told); and how long such a save runs at least before it is
   * bounded while its sandbox loads all the same, in ms (loadWaitWatchdogMs unless told).
   */
  readonly loadWait?: { readonly waitSeconds?: number; readonly deferredForMs?: number; readonly watchdogMs?: number };
  /**
   * How long a restore from the bucket, its sandbox resumed, may read the bucket through the
   * agent for what its sandbox did not touch first, before its token is revoked: its next
   * checkpoint, abandon or prepare revokes it before, and an agent that starts again holds
   * none. As long as the largest of the node's sandboxes takes to read whole
   * (./names.ts backgroundReadSeconds).
   */
  readonly backgroundReadMs: number;
  /**
   * How a release's save of a sandbox whose restore from the bucket still reads it is bounded
   * (boundWhileLoading): how long no byte of the restore's pages may be served
   * (LOADING_STALL_SECONDS unless told), how long the save runs at least before that or a rate
   * too slow counts (LOADING_SAVE_FLOOR_SECONDS), how long from its ask it may wait at the most
   * (LOADING_SAVE_LATEST_SECONDS), and how often the restore's reads are looked at (5 s), in ms.
   */
  readonly loadingSave?: { readonly stallMs?: number; readonly floorMs?: number; readonly latestMs?: number; readonly pollMs?: number };
  /** How long runsc may take to say how many bytes of pages a restore from the bucket reads, INSPECT_TIMEOUT_MS unless told. */
  readonly inspectTimeoutMs?: number;
  /**
   * The pod `name` of `namespace` as the API server says it each time it changes, from as it
   * is now; null where it is gone: what a resume waits on for its pod to run (./server.ts, by
   * the agent's ServiceAccount, which reads the sessions' pods).
   */
  readonly pods: (namespace: string, name: string) => Stream.Stream<V1Pod | null, KubeApiError>;
  /**
   * Whether a checkpoint uploads its image in one pass, its files hashed as they upload, all at
   * once (hashedAndPut), writes its manifest beside its commit, and looks at the images it refers
   * to in the bucket only once it is committed (checkLater); not as one that hashes its files,
   * then uploads them one after another, then looks at them and the images it refers to in the
   * bucket, then writes its manifest and its commit one after the other: no unless told.
   */
  readonly onePassSaves?: boolean;
  /**
   * Records, beside the workspace's files, the compaction of its snapshot `from` this agent
   * committed, `manifest` (./workspaces.ts setCompacted): what a restore of a workspace released
   * into `from` restores instead. One that fails leaves that restore to the snapshot compacted
   * from, which collection keeps while the compaction's manifest is the workspace's latest.
   */
  readonly compacted: (ref: WorkspaceRef, compacted: { readonly from: string; readonly manifest: Manifest }) => Effect.Effect<void, { readonly message: string }>;
}

/** The checkpoint gofer's options file for an image in an S3 store (runsc's checkpoint_restore.md). */
export const GOFER_OPTIONS_FILE = "s3_opts.json";
/**
 * How many reads of an image's pages the checkpoint gofer serves at once, each of 16 MiB on a
 * connection of its own to the agent's proxy of the bucket: an S3 connection delivers some
 * 85 MB/s, and a restore loading its pages in the background keeps all of them but one in
 * flight from gVisor eaucoin.15 on, where an earlier loader kept the one its budget found.
 */
const GOFER_READ_PARALLEL = 16;
/**
 * How many files of images the agent looks at in the bucket at once, as a checkpoint checks
 * what it refers to, and a restore from the bucket every image of its chain, three files an
 * image: each look a round trip to the store, some 150 ms on S3 Standard.
 */
const BUCKET_HEADS = 16;

/** The directory of a workspace's manifests, in the bucket. */
export const MANIFESTS_DIR = "manifests";
/** The file of the manifest of the snapshot a cache last took or prepared of a workspace, beside its images. */
export const MANIFEST_FILE = "manifest.json";
/** The object of a workspace's current snapshot. */
export const CURRENT_FILE = "current.json";
/**
 * The file, beside a workspace's images in the cache, of the chain of its images runsc last
 * verified there as the cache staged them (verifiedStaged), a VerifiedChain: a prepare of a
 * snapshot of that chain, under that release, its images intact since, takes it as verified.
 */
export const VERIFIED_FILE = "verified.json";
/**
 * The file, beside a workspace's images in the cache, of the compaction that writes one: the snapshot it compacts into, and
 * its image's digest once the cache holds it; there until it has ended, so that an agent that starts again after one that
 * stopped as it compacted removes what that one wrote (recoverCompactions).
 */
export const COMPACTING_FILE = "compacting.json";
/**
 * How long what a snapshot superseded needs, and what a commit wrote, is kept: longer than
 * a restore or a copy reads the images of a manifest it read, and than a commit takes, one
 * that takes longer, less CLOCK_SKEW_MS, refused.
 */
export const COLLECT_GRACE_MS = 60 * 60_000;
/**
 * How far apart the clocks that time a commit and its collection may stand: the agent's that
 * commits, the store's that says when each object was written, and the collecting agent's.
 * A commit whose first upload began COLLECT_GRACE_MS less this ago or longer is refused
 * (CommitOverdue), as a collection may have taken what it uploaded and no manifest names yet.
 */
export const CLOCK_SKEW_MS = 5 * 60_000;
/**
 * The most of a file of bayma's state directory the agent reads: one larger is not read,
 * and what it says is not told.
 */
export const STATE_FILE_BYTES = 1024 * 1024;
/**
 * How long the agent waits for a file of bayma's state directory to be read: a file of
 * STATE_FILE_BYTES at most, on a volume of the node's, is read well within it.
 */
export const STATE_READ_TIMEOUT_MS = 5_000;

/**
 * How long runsc may take to describe an image in the bucket, reading its metadata through
 * the node agent: well within it, as a restore reads the same before it starts. Past it, its
 * process is killed, and the restore's reads are waited on by their quiet.
 */
export const INSPECT_TIMEOUT_MS = 60_000;
/** bayma's state generation, as its state directory's generation.json says it. */
const StateGenerationFile = Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1), generation: Count }));
/** What bayma's last save caught, as its state directory's last-save.json says it. */
const LastSaveFile = Schema.fromJsonString(Schema.Struct({ schemaVersion: Schema.Literal(1), generation: Count, running: Running }));
/** How the agent reads a state file's bytes as text: those that are not UTF-8 fail it. */
const UTF8 = new TextDecoder("utf-8", { fatal: true });
/**
 * How long bayma's hook may take as runsc bounds it, of a sandbox whose restore from the
 * bucket reads its pages through the agent for up to `backgroundReadMs`: runsc keeps the
 * bound the save gave the hook with the sandbox's state, and holds the hook to it as the
 * sandbox is restored, when the hook starts while its pages are still read lazily, each
 * page its process touches read as slowly as the background reads may be. So as long as
 * those may take, and the hook's own work beside; a save, and the resume after one taken
 * live, the hook bounds within SAVE_HOOK_SECONDS itself. A hook that hangs as its sandbox is
 * restored is killed only past all of it, its restore held meanwhile: bounded still, as the
 * background reads' budget is of the largest sandbox's memory (./names.ts
 * backgroundReadSeconds).
 */
export const restoreHookSeconds = (backgroundReadMs: number): number => Math.ceil(backgroundReadMs / 1000) + SAVE_HOOK_SECONDS;
/**
 * How long a resume of a sandbox restored from images of `bytes` together may take, in
 * milliseconds: its wait for its pod to run, then for the pod's network policies, then
 * runsc's resume, which runs bayma's hook while the sandbox's pages still load, each page the
 * hook touches waited for, as long as runsc would bound that hook of a sandbox of so many bytes
 * (restoreHookSeconds), read at the slowest (./names.ts backgroundReadSeconds), then its wait
 * for the pod to accept connections. One of a large image, or of a chain whose parent's pages a
 * delta left scattered, on a slow store, is waited for; one stuck still ends.
 */
export const resumeMs = (bytes: number): number => RESTORE_RUNNING_WAIT_MS + POLICY_WAIT_MS + restoreHookSeconds(backgroundReadSeconds(bytes) * 1000) * 1000 + ACCEPT_WAIT_MS;
/** How often the cache is looked at for what to evict, besides as checkpoints and prepares write to it. */
const EVICT_POLL: Duration.Input = "1 minute";
/**
 * How a spend whose workspace's commit was written again as it was spent reads it again: three times more, 50 ms after,
 * then twice as long each time, its workspace held meanwhile; a commit written again that often is of a workspace whose
 * spend its caller asks again later.
 */
const SPEND_READS = { times: 3, schedule: Schedule.exponential("50 millis") };
/**
 * How often a resume looks for a restored pod's rules: kube-router writes them a few hundred
 * milliseconds after the pod's address reaches the API server, which the restore waits on.
 * A look lists a chain or two of the node's filter table, some 5 ms each.
 */
const POLICY_POLL = "50 millis";
/**
 * How a resume's watch of its pod that fails, as one whose connection drops does, is asked again:
 * five times more in all, 100 ms after it failed, then twice as long each time.
 */
const WATCH_TRIES = { times: 5, schedule: Schedule.exponential("100 millis") };
/** The most scans an abandon runs for what runsc runs for a pod, each after the kills of the one before (abandonNow). */
const ABANDON_SCANS = 10;

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
const layerChecks = meter.createCounter("alasio.node_agent.checkpoint.layer_checks", {
  description: "The images a checkpoint taken in one pass refers to, looked at in the bucket once it is committed, by outcome: held, put again from the node's cache, or what failed",
  unit: "{image}",
});
const uncaught = meter.createCounter("alasio.node_agent.checkpoint.uncaught", {
  description: "Release saves whose bayma's last-save.json could not be read once runsc's save had ended the sandbox, each kept recording no generation of bayma's state",
  unit: "{checkpoint}",
});
const copies = meter.createCounter("alasio.node_agent.copies", {
  description: "Snapshots copied from one workspace to another by the node agent, by outcome",
  unit: "{copy}",
});
const fetches = meter.createCounter("alasio.node_agent.fetches", {
  description: "Images of a snapshot restored from the bucket fetched into the node's cache once its restore read them, for the next checkpoint's delta, by outcome",
  unit: "{image}",
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

/** A chain of images runsc verified, as VERIFIED_FILE says it: the release that verified it, and its images' digests, in the chain's order. */
const VerifiedChain = Schema.fromJsonString(Schema.Struct({ release: Release, digests: Schema.Array(Schema.String.check(Schema.isPattern(DIGEST))) }));

/** A compaction under way, as COMPACTING_FILE says it. */
const Compacting = Schema.fromJsonString(Schema.Struct({ snapshot: SnapshotId, digest: Schema.optional(Schema.String.check(Schema.isPattern(DIGEST))) }));

/** A snapshot's id, as SNAPSHOT_ID says: when it was taken, to the millisecond, and a random part. */
function snapshotId(takenAt: number): string {
  return `${new Date(takenAt).toISOString().replace(/[-:.]/gu, "")}-${randomBytes(4).toString("hex")}`;
}

/** The SHA-256 of a file, in hex; each chunk of it read once `ready` has resolved, where given. */
async function fileSha256(path: string, ready?: () => Promise<void>): Promise<string> {
  const hash = createHash("sha256");
  if (ready === undefined) await pipeline(createReadStream(path), hash);
  else {
    await pipeline(createReadStream(path), async function*(chunks: AsyncIterable<Buffer>) {
      for await (const chunk of chunks) {
        await ready();
        yield chunk;
      }
    }, hash);
  }
  return hash.digest("hex");
}

/** A file of an image as it is on the disk, before the bucket holds it. */
type ImageFile = Omit<SnapshotFile, "etag">;

/** The paths of the files of an image directory, in it, in order. */
async function imagePaths(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true, recursive: true });
  return entries.filter((entry) => entry.isFile()).map((entry) => relative(directory, join(entry.parentPath, entry.name))).sort();
}

/** The files of an image directory, each with its size and SHA-256, by their paths in it, in order; read as fileSha256 reads them. */
async function imageFiles(directory: string, ready?: () => Promise<void>): Promise<ImageFile[]> {
  return Promise.all((await imagePaths(directory)).map(async (path) => {
    const file = join(directory, path);
    return { path, bytes: (await stat(file)).size, sha256: await fileSha256(file, ready) };
  }));
}

/**
 * What hashAhead runs in a thread of its own: the file `workerData.path` read once from its
 * start, its SHA-256 updated as it is, how far it has read posted every HASH_PROGRESS_BYTES,
 * and at its end the SHA-256, in hex; or why it failed.
 */
const HASH_WORKER = `
const { parentPort, workerData } = require("node:worker_threads");
const { createHash } = require("node:crypto");
const { createReadStream } = require("node:fs");
const hash = createHash("sha256");
let read = 0;
let told = 0;
createReadStream(workerData.path, { highWaterMark: 1 << 20 })
  .on("data", (chunk) => {
    hash.update(chunk);
    read += chunk.length;
    if (read - told >= workerData.progressBytes) {
      told = read;
      parentPort.postMessage({ read });
    }
  })
  .on("end", () => parentPort.postMessage({ read, sha256: hash.digest("hex") }))
  .on("error", (error) => parentPort.postMessage({ error: error.message }));
`;

/** How often a hash ahead of an upload says how far it has read: 8 MiB, a quarter of a part (./names.ts SNAPSHOT_PART_BYTES). */
const HASH_PROGRESS_BYTES = 8 * 1024 ** 2;

/**
 * The SHA-256 of the file at `path`, in hex, as it is read once from its start, and what
 * resolves once that read has passed the byte `end`, or fails as it does: what each part of an
 * upload of the file waits for (hashedAndPut), so that the file is read from the disk once, by
 * the hash, its upload reading the same bytes after it, from the page cache, as runsc writes an
 * image past it (`--direct`). Read and hashed in a thread of its own (HASH_WORKER), as a hash in
 * the agent's own would hold the thread its uploads send from. Its failure is told by both, and
 * by neither left unhandled.
 */
export function hashAhead(path: string): { readonly sha256: Promise<string>; readonly through: (end: number) => Promise<void> } {
  let read = 0;
  let failed: { readonly cause: unknown } | null = null;
  let waiting: { readonly end: number; readonly resolve: () => void; readonly reject: (cause: unknown) => void }[] = [];
  const settle = () => {
    const due = waiting.filter(({ end }) => failed !== null || end <= read);
    waiting = waiting.filter((each) => !due.includes(each));
    for (const { resolve, reject } of due) {
      if (failed === null) resolve();
      else reject(failed.cause);
    }
  };
  const sha256 = new Promise<string>((resolve, reject) => {
    const worker = new Worker(HASH_WORKER, { eval: true, workerData: { path, progressBytes: HASH_PROGRESS_BYTES } });
    const fail = (cause: unknown) => {
      failed = { cause };
      settle();
      reject(cause);
      void worker.terminate();
    };
    worker.on("message", (said: { readonly read: number; readonly sha256?: string; readonly error?: string }) => {
      if (said.error !== undefined) return fail(new Error(said.error));
      read = said.sha256 === undefined ? said.read : Number.POSITIVE_INFINITY;
      settle();
      if (said.sha256 !== undefined) {
        resolve(said.sha256);
        void worker.terminate();
      }
    });
    worker.on("error", fail);
  });
  sha256.catch(() => {});
  return {
    sha256,
    through: (end) =>
      new Promise<void>((resolve, reject) => {
        waiting.push({ end, resolve, reject });
        settle();
      }),
  };
}

/** Syncs the file or directory at `path` to the disk. */
async function syncPath(path: string, directory: boolean): Promise<void> {
  const handle = await open(path, directory ? constants.O_RDONLY | constants.O_DIRECTORY : constants.O_RDONLY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * Syncs an image directory to the disk: each of its files, then each directory in it, itself,
 * and the one it was moved into, which holds its name. What runsc writes with `O_DIRECT` is past
 * the page cache but not past the device's, nor is any file's size or name.
 */
async function syncImage(directory: string): Promise<void> {
  const entries = await readdir(directory, { withFileTypes: true, recursive: true });
  await Promise.all(entries.filter((entry) => entry.isFile()).map((entry) => syncPath(join(entry.parentPath, entry.name), false)));
  const directories = entries.filter((entry) => entry.isDirectory()).map((entry) => join(entry.parentPath, entry.name));
  await Promise.all(directories.map((each) => syncPath(each, true)));
  await syncPath(directory, true);
  await syncPath(dirname(directory), true);
}

/** Whether `found`, an image's files on the disk, are `layer`'s. */
const sameFiles = (found: readonly ImageFile[], layer: Layer): boolean =>
  JSON.stringify(found) === JSON.stringify(layer.files.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })));

/**
 * The most memory the pod of `podTemplate` may hold, as its containers' limits say, which
 * bounds its sandbox's image: what a checkpoint makes room for in the cache. A container of
 * no limit, or of one not read, adds nothing.
 */
export function memoryLimitOf(podTemplate: Readonly<Record<string, unknown>>): number {
  const spec = podTemplate["spec"] as { readonly containers?: readonly { readonly resources?: { readonly limits?: Readonly<Record<string, unknown>> } }[] } | undefined;
  return (spec?.containers ?? []).reduce((sum, { resources }) => {
    const bytes = quantityOf(resources?.limits?.["memory"]);
    return sum + (Number.isFinite(bytes) ? Math.ceil(bytes) : 0);
  }, 0);
}

/**
 * `cache`, `changed` run after each of its calls that may change what it holds, the images of
 * its workspaces, their bytes, or when each was used, whatever came of the call.
 */
function telling(cache: SnapshotCache, changed: Effect.Effect<void>): SnapshotCache {
  const told = <Args extends readonly unknown[], A, E>(call: (...args: Args) => Effect.Effect<A, E>) => (...args: Args) => call(...args).pipe(Effect.ensuring(changed));
  return {
    ...cache,
    stage: told(cache.stage),
    commit: told(cache.commit),
    prepare: told(cache.prepare),
    prepareRemote: told(cache.prepareRemote),
    drop: told(cache.drop),
    clear: told(cache.clear),
    discard: told(cache.discard),
    evict: told(cache.evict),
    room: told(cache.room),
  };
}

/** `run`, on the node's disk, failing as the cache does. */
const onDisk = <A>(run: () => Promise<A>): Effect.Effect<A, CacheError> =>
  Effect.tryPromise({ try: run, catch: (cause) => new CacheError({ reason: cause instanceof Error ? cause.message : String(cause) }) });

/** The node agent of `options`, whose operations on a workspace run one at a time while the scope lasts. */
export const makeNodeAgent = Effect.fnUntraced(function*({ node, nodeRoot, runsc, cache: given, bucket, storeProxy, host, restoresFromBucketSince, savesWithLoadWaitSince, savesSummarySince, loadWait = {}, backgroundReadMs, loadingSave = {}, inspectTimeoutMs = INSPECT_TIMEOUT_MS, pods, onePassSaves = false, compacted: recordCompacted }: NodeAgentOptions): Effect.fn.Return<NodeAgent, never, Scope.Scope> {
  /** How many calls of the cache's may have changed what it holds: what holdingsAfter waits on, and what holdings reads its listing as of. */
  const cacheChanges = yield* SubscriptionRef.make(0);
  const cache = telling(given, SubscriptionRef.update(cacheChanges, (count) => count + 1));
  const locks = yield* RcMap.make({ lookup: (_workspace: string) => Semaphore.make(1) });
  /** One fetch of each workspace's images at a time (fetchLater), beside its operations. */
  const fetchLocks = yield* RcMap.make({ lookup: (_workspace: string) => Semaphore.make(1) });
  /** How many operations run on, or wait to run on, each workspace, by its prefix, and compactions write to it: the cache spares those of any. */
  const inFlight = new Map<string, number>();
  /**
   * The snapshot each sandbox running on this node, by the container checkpoints name, was
   * last saved to or restored from, whose image the next checkpoint of it saves what changed
   * since: in memory, as an agent that starts again takes a full one first.
   */
  const bases = new Map<string, Manifest>();
  /** The snapshot each workspace's restore on this node was last prepared from, which its resume makes its sandbox's base. */
  const prepared = new Map<string, Manifest>();
  /**
   * Since when each workspace's release saves on this node have been deferred (SaveDeferred), by
   * its prefix, from the first that was, and the container whose sandbox they were of: in memory,
   * as an agent that starts again begins a window anew. A save of another container's, as of a
   * sandbox resumed since, or of none, begins none.
   */
  const deferredSince = new Map<string, { readonly since: number; readonly containerId: string }>();
  /** Compactions run in the agent's scope, one at a time on the node, beside whatever else it does. */
  const scope = yield* Effect.scope;
  const compacting = yield* Semaphore.make(1);
  /** Each workspace's compaction, by its prefix, running or waiting to: a restore of the workspace on this node stops it. */
  const compactionsRunning = yield* FiberMap.make<string>();
  /** The restores on this node still reading their images, by workspace prefix, which compactions yield to. */
  const loading = yield* makeLoading;
  /** Resolves once no restore on this node reads its image. */
  const quietly = (): Promise<void> => Effect.runPromise(loading.quiet);

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

  /** `effect`, the workspaces in flight while it runs, as the cache spares them. */
  const inFlightWhile = <A, E>(workspaces: readonly WorkspaceRef[], effect: Effect.Effect<A, E>): Effect.Effect<A, E> => {
    const names = [...new Set(workspaces.map(prefixOf))];
    return Effect.acquireUseRelease(
      Effect.sync(() => names.forEach((name) => inFlight.set(name, (inFlight.get(name) ?? 0) + 1))),
      () => effect,
      () => Effect.sync(() => names.forEach((name) => ((inFlight.get(name) ?? 1) > 1 ? inFlight.set(name, (inFlight.get(name) ?? 1) - 1) : inFlight.delete(name)))),
    );
  };

  /** Whether the cache is to spare a workspace: one in flight. */
  const spared = (workspace: WorkspaceRef): boolean => inFlight.has(prefixOf(workspace));

  /**
   * `effect` once no other operation runs on the workspaces, each held in the order of its
   * name, so that two operations never wait on each other; the workspaces in flight from
   * its call on, as it waits too; its wait for them a step of its own, `waited`, where given.
   */
  const exclusive = <A, E>(workspaces: readonly WorkspaceRef[], effect: Effect.Effect<A, E>, waited?: string): Effect.Effect<A, E> => {
    const holding = (held: Effect.Effect<A, E>) =>
      Effect.scoped(
        [...new Set(workspaces.map(prefixOf))].sort().reduceRight<Effect.Effect<A, E, Scope.Scope>>(
          (inner, name) => RcMap.get(locks, name).pipe(Effect.flatMap((lock) => inner.pipe(Semaphore.withPermit(lock)))),
          held,
        ),
      );
    const [one] = workspaces;
    return inFlightWhile(workspaces, waited === undefined ? holding(effect) : withWaitSpan(waited, workspaces.length === 1 && one ? spanAttributes(one) : {})(holding, effect));
  };

  /** How long `effect` took, in milliseconds, and what it came to. */
  const timed = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<readonly [number, A], E> =>
    Effect.gen(function*() {
      const start = yield* Clock.currentTimeMillis;
      const value = yield* effect;
      return [(yield* Clock.currentTimeMillis) - start, value] as const;
    });

  /** The manifest the object `key` holds, of the workspace `ref`'s, and the ETag the bucket holds it by: one of another workspace's is none of it. */
  const taggedManifestAt = (ref: WorkspaceRef, key: string): Effect.Effect<{ readonly manifest: Manifest; readonly etag: string } | null, S3Error | SnapshotCorrupt> =>
    bucket.getTaggedJson(key).pipe(
      Effect.flatMap((found) =>
        found === null
          ? Effect.succeed(null)
          : Schema.decodeUnknownEffect(Manifest)(found.value).pipe(
            Effect.mapError((error) => new SnapshotCorrupt({ ...ref, reason: `its ${key.slice(prefixOf(ref).length + 1)} is not a manifest: ${error.message}` })),
            Effect.filterOrFail(
              (manifest) => prefixOf(manifest) === prefixOf(ref),
              (manifest) => new SnapshotCorrupt({ ...ref, reason: `its ${key.slice(prefixOf(ref).length + 1)} names ${prefixOf(manifest)}` }),
            ),
            Effect.map((manifest) => ({ manifest, etag: found.etag })),
          )
      ),
    );

  /** The manifest the object `key` holds, of the workspace `ref`'s, as taggedManifestAt reads it. */
  const manifestAt = (ref: WorkspaceRef, key: string): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> =>
    Effect.map(taggedManifestAt(ref, key), (found) => found?.manifest ?? null);

  /** The workspace's current snapshot, as its `current.json` says. */
  const current = (ref: WorkspaceRef): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> => manifestAt(ref, `${prefixOf(ref)}/${CURRENT_FILE}`);

  /**
   * Makes the workspace's current snapshot current no more where it is `snapshot`, or one compacted from it, which is the
   * same: its manifest, or null where it is neither. Its `current.json` is removed only as the bucket holds it as it was read
   * (removeIf, by its ETag): the agent of another node may commit the workspace's next snapshot meanwhile, a release of its
   * sandbox restored there, or a compaction there, which no lock of this agent's holds back. One written since is read again,
   * and stands where it is another: as often as SPEND_READS says, the workspace held meanwhile, after which the spend fails,
   * for its caller to ask again.
   */
  const spendNow = (ref: WorkspaceRef, snapshot: string): Effect.Effect<Manifest | null, S3Error | SnapshotCorrupt> => {
    const key = `${prefixOf(ref)}/${CURRENT_FILE}`;
    const once = Effect.gen(function*() {
      const found = yield* taggedManifestAt(ref, key);
      if (found === null || (found.manifest.snapshot !== snapshot && found.manifest.compactedFrom !== snapshot)) return null;
      if (yield* bucket.removeIf(key, found.etag)) return found.manifest;
      return yield* new WrittenMeanwhile();
    });
    return once.pipe(
      Effect.retry({ while: (error) => error._tag === "WrittenMeanwhile", ...SPEND_READS }),
      Effect.catchTag("WrittenMeanwhile", () => Effect.fail(new S3Error({ operation: "deletion", key, reason: `it was written again each of the ${SPEND_READS.times + 1} times it was read` }))),
    );
  };

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
      Effect.catchTag("SandboxGone", ({ reason }) => Effect.fail(new RunscError({ release: "the node's", command: `state ${containerId}`, reason }))),
      Effect.flatMap((running) => {
        const found = podOf(running.annotations);
        return found === `${namespace}/${pod}` ? Effect.succeed(running) : Effect.fail(new ContainerElsewhere({ containerId, namespace, pod, found }));
      }),
    );

  /**
   * The files of the image `digest` the cache holds of the workspace, uploaded, as putFile puts them: each with the ETag its
   * upload was answered with, and in a span of its own, `put`, of its bytes.
   */
  const upload = (ref: WorkspaceRef, digest: string, files: readonly ImageFile[], put: string, ready?: () => Promise<void>): Effect.Effect<Layer, S3Error> =>
    Effect.forEach(files, (file) =>
      bucket.putFile(layerKey(ref, digest, file.path), join(cache.image(ref.environment, ref.workspace, digest), file.path), ready).pipe(
        Effect.map((etag) => ({ ...file, etag })),
        withAlasioSpan(put, { attributes: { ...spanAttributes(ref), "alasio.snapshot.file": file.path, "alasio.snapshot.bytes": file.bytes } }),
      )).pipe(Effect.map((uploaded) => ({ digest, files: uploaded })));

  /**
   * The files of the image `digest` the cache holds of the workspace, each hashed and uploaded in one read of it (hashAhead),
   * all at once: its layer, each file with its size, its SHA-256, and the ETag its upload was answered with, in a span of
   * its own, `put`, of its bytes.
   */
  const hashedAndPut = (ref: WorkspaceRef, digest: string, put: string): Effect.Effect<Layer, S3Error | CacheError> =>
    Effect.gen(function*() {
      const directory = cache.image(ref.environment, ref.workspace, digest);
      const paths = yield* onDisk(() => imagePaths(directory));
      const files = yield* Effect.forEach(paths, (path) =>
        Effect.gen(function*() {
          const file = join(directory, path);
          const { size: bytes } = yield* onDisk(() => stat(file));
          yield* Effect.annotateCurrentSpan({ "alasio.snapshot.bytes": bytes });
          const ahead = hashAhead(file);
          const [etag, sha256] = yield* Effect.all([bucket.putFile(layerKey(ref, digest, path), file, ahead.through), onDisk(() => ahead.sha256)], { concurrency: 2 });
          return { path, bytes, sha256, etag };
        }).pipe(withAlasioSpan(put, { attributes: { ...spanAttributes(ref), "alasio.snapshot.file": path } })), { concurrency: "unbounded" });
      return { digest, files };
    });

  /**
   * Those of `layers` of the workspace's, in their order, a file of which is not in the bucket
   * as it was put, of its size and of the ETag its upload was answered with; none where every
   * file is. Their files are looked at BUCKET_HEADS at a time.
   */
  const notInBucket = (ref: WorkspaceRef, layers: readonly Layer[]): Effect.Effect<readonly Layer[], S3Error> =>
    Effect.forEach(layers.flatMap((layer) => layer.files.map((file) => ({ layer, file }))), ({ layer, file: { path, bytes, etag } }) =>
      bucket.head(layerKey(ref, layer.digest, path)).pipe(
        Effect.map((stored) => (stored !== null && stored.bytes === bytes && stored.etag === etag ? null : layer)),
      ), { concurrency: BUCKET_HEADS }).pipe(Effect.map((missing) => layers.filter((layer) => missing.includes(layer))));

  /** Whether every file of `layer` of the workspace's is in the bucket as it was put (notInBucket). */
  const inBucket = (ref: WorkspaceRef, layer: Layer): Effect.Effect<boolean, S3Error> => Effect.map(notInBucket(ref, [layer]), (missing) => missing.length === 0);

  /**
   * `layer`, an image of the workspace's that a new snapshot refers to, as the bucket holds
   * it: as it was put, checked; or put again from the cache (putAgain), each file's upload as
   * `put` names its span.
   */
  const layerInBucket = (ref: WorkspaceRef, layer: Layer, put: string): Effect.Effect<Layer, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.flatMap(inBucket(ref, layer), (held) => (held ? Effect.succeed(layer) : putAgain(ref, layer, put)));

  /**
   * `layer`, an image of the workspace's that a new snapshot refers to, which the bucket lost,
   * or holds another of its digest: put again from the cache, whose files must be the
   * manifest's, each file's upload as `put` names its span.
   */
  const putAgain = (ref: WorkspaceRef, layer: Layer, put: string): Effect.Effect<Layer, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const reason = `its image ${layer.digest} is not in the bucket as it was put`;
      if (!(yield* cache.holds(ref.environment, ref.workspace, layer.digest))) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, nor in this node's cache` });
      const files = yield* onDisk(() => imageFiles(cache.image(ref.environment, ref.workspace, layer.digest)));
      if (!sameFiles(files, layer)) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, and this node's cache holds other files of it` });
      yield* Effect.logWarning(`${reason}: it is put again from this node's cache`);
      const again = yield* upload(ref, layer.digest, files, put);
      if (!(yield* inBucket(ref, again))) return yield* new SnapshotCorrupt({ ...ref, reason: `${reason}, nor once put again` });
      return again;
    });

  /**
   * Looks at `referred`, the images of the workspace's that `committed`, a snapshot a checkpoint took in one pass, refers to,
   * in the bucket, once it is committed, as its checkpoint, which took them to be there as its parent's manifest says, did not:
   * each the bucket does not hold as it was put is put again from the cache, as layerInBucket puts one. Beside whatever else
   * the agent does, in its scope, the workspace's images spared meanwhile; what fails is said, and counted. A trace of its
   * own, linked to the checkpoint's it began in, which it outlives.
   */
  const checkLater = (committed: Manifest, referred: readonly Layer[]): Effect.Effect<void> =>
    Effect.suspend(() => {
      if (referred.length === 0) return Effect.void;
      const begunIn = currentTraceparent();
      const counted = (outcome: string, count = 1) => Effect.sync(() => layerChecks.add(count, { "alasio.node_agent.checkpoint.layer_check.outcome": outcome }));
      const checked = Effect.gen(function*() {
        const missing = yield* notInBucket(committed, referred);
        yield* counted("held", referred.length - missing.length);
        for (const layer of missing) {
          yield* putAgain(committed, layer, "alasio.node_agent.checkpoint.check.put").pipe(
            Effect.andThen(counted("put again")),
            Effect.catch((error) =>
              counted(error._tag).pipe(Effect.andThen(Effect.logWarning(`the snapshot ${committed.snapshot} of ${prefixOf(committed)} refers to its image ${layer.digest}, which the bucket does not hold as it was put: ${error.message}`)))
            ),
          );
        }
      }).pipe(
        Effect.catch((error) => counted(error._tag).pipe(Effect.andThen(Effect.logWarning(`could not look at the images the snapshot ${committed.snapshot} of ${prefixOf(committed)} refers to in the bucket: ${error.message}`)))),
        (checking) => inFlightWhile([committed], checking),
        withAlasioSpan("alasio.node_agent.checkpoint.check", { attributes: { ...spanAttributes(committed), "alasio.snapshot": committed.snapshot }, parent: null, links: begunIn === null ? [] : [begunIn] }),
        withLogScope("node-agent"),
      );
      return checked.pipe(Effect.forkIn(scope), Effect.asVoid);
    });

  /**
   * Fails where the commit of the workspace's snapshot `snapshot`, whose first upload began at
   * `uploadedAt` by this agent's clock, is overdue (CommitOverdue): checked last before its
   * manifest is written, as a collection keeps what it uploaded only so long.
   */
  const inTime = ({ environment, workspace }: WorkspaceRef, snapshot: string, uploadedAt: number): Effect.Effect<void, CommitOverdue> =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        now - uploadedAt < COLLECT_GRACE_MS - CLOCK_SKEW_MS ? Effect.void : Effect.fail(new CommitOverdue({ environment, workspace, snapshot, uploadedMsAgo: now - uploadedAt }))
      ),
    );

  /**
   * The snapshot of the sandbox `request` names, taken and committed as the module says:
   * its manifest. A delta of its base, its sandbox tracking its writes and the base's image
   * in the cache; whole otherwise. Its save bounded while its sandbox loads, as one asked at
   * `askedAt`, where given (boundWhileLoading).
   */
  const take = (request: CheckpointRequest, askedAt: number | null): Effect.Effect<Manifest, RunscError | ReleaseNotInstalled | ContainerElsewhere | S3Error | CacheError | SnapshotCorrupt | CommitOverdue> =>
    Effect.gen(function*() {
      const { environment, workspace, containerId, podTemplate } = request;
      const running = yield* ofPod(containerId, request.namespace, request.pod);
      const { tracksWrites } = running;
      const takenAt = yield* Clock.currentTimeMillis;
      const snapshot = snapshotId(takenAt);
      const base = bases.get(containerId);
      const [own] = base?.layers ?? [];
      const extended = base !== undefined && own !== undefined && tracksWrites && prefixOf(base) === prefixOf(request) && (yield* cache.holds(environment, workspace, own.digest)) ? base : null;
      // A live snapshot is taken whole, of no parent, where a delta of its parent would make a
      // chain due to be compacted: no live chain is compacted after (compactLater), as its
      // sandbox runs on and its next delta would be of this image whatever a compaction wrote,
      // so the chain the sandbox goes on extending is cut here. A whole save writes what a
      // compaction would, once, as part of a save made anyway, at the sandbox's pace, beside
      // the restore path, and with no second pass over the chain on the node.
      const due = request.live === undefined || extended === null ? null : compactionDue(extended.layers, 1);
      if (due !== null && extended !== null) {
        yield* Effect.logInfo(`taking the snapshot ${snapshot} of ${prefixOf(request)} live whole, not as a delta of ${extended.snapshot}: with it, its chain would be due to be compacted, as ${due}`);
      }
      const parent = due === null ? extended : null;
      yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot, "alasio.snapshot.parent": parent?.snapshot ?? "" });
      yield* cache.room(memoryLimitOf(podTemplate), spared).pipe(Effect.flatMap(logEvicted));
      const staged = yield* cache.stage(environment, workspace, snapshot);
      return yield* commit(request, snapshot, takenAt, staged, parent, running, askedAt);
    });

  const loadWaitSeconds = loadWait.waitSeconds ?? SAVE_LOAD_WAIT_SECONDS;
  const deferredForMs = loadWait.deferredForMs ?? SAVES_DEFERRED_FOR_SECONDS * 1000;

  /**
   * How long runsc's save, under `release`, of the sandbox `request` names, a release's asked at
   * `askedAt` or a live one (null), waits for pages still loading, in seconds, or null where it
   * does not bound that wait: where its release bounds it (savesWithLoadWait), as long as
   * loadWaitSeconds says, but no longer than lets it end, bayma's save hook's own bound
   * (SAVE_HOOK_SECONDS, see loadWaitWatchdogMs) and LOAD_WAIT_WATCHDOG_MARGIN_SECONDS after it,
   * before the latest a release's save may be bounded
   * as its sandbox loads, timings.latestMs from its ask: its time waiting for the workspace's lock
   * counted, so that boundWhileLoading, its watchdog, never revokes its restore's token while runsc
   * bounds its waits itself. Not where that leaves it no second, said so, nor where its pod ends as
   * it is saved (CheckpointRequest's ending), which a deferral cannot help; nor once the
   * workspace's release saves of its sandbox have been deferred so for deferredForMs: those are
   * bounded while their sandbox loads instead, the last the backstop, said so.
   */
  const loadWaitOf = (request: CheckpointRequest, release: string, askedAt: number | null): Effect.Effect<number | null> =>
    Effect.gen(function*() {
      if (!savesWithLoadWait(release, savesWithLoadWaitSince) || request.ending === true) return null;
      const now = yield* Clock.currentTimeMillis;
      const deferred = request.live === undefined ? deferredSince.get(prefixOf(request)) : undefined;
      if (deferred !== undefined && deferred.containerId === request.containerId && now - deferred.since >= deferredForMs) {
        yield* Effect.logWarning(`the release saves of ${prefixOf(request)} have been deferred for ${Math.round((now - deferred.since) / 60_000)} min, its pages still loading from the bucket: this one is bounded as its sandbox loads, not deferred, so the session comes back from bayma's checkpoints if its pages do not load`);
        return null;
      }
      if (askedAt === null) return loadWaitSeconds;
      const room = Math.floor((askedAt + timings.latestMs - now) / 1000) - SAVE_HOOK_SECONDS - LOAD_WAIT_WATCHDOG_MARGIN_SECONDS;
      if (room < 1) {
        yield* Effect.logWarning(`the release save of ${prefixOf(request)}, asked ${Math.round((now - askedAt) / 1000)} s ago, has no time left to wait for its pages still loading, its bayma's hook bounded to ${SAVE_HOOK_SECONDS} s, before the latest it may be bounded as its sandbox loads: it is bounded so, not deferred`);
        return null;
      }
      return Math.min(loadWaitSeconds, room);
    });

  /**
   * Saves the sandbox `request` names into `staged`, a delta of `parent`'s image if given,
   * and commits it as the snapshot `snapshot`: its manifest. What it staged goes if it fails.
   * Its save, as one asked at `askedAt`, where given, bounded while its sandbox loads
   * (boundWhileLoading); where its release bounds its waits for pages still loading
   * (loadWaitOf), runsc's own bound, only past what that bound and bayma's hook's let the save
   * run (loadWaitWatchdogMs), which loadWaitOf ends before the latest a revocation comes: a
   * watchdog, should runsc not defer it.
   */
  const commit = (request: CheckpointRequest, snapshot: string, takenAt: number, staged: string, parent: Manifest | null, container: RunningContainer, askedAt: number | null): Effect.Effect<Manifest, RunscError | ReleaseNotInstalled | S3Error | CacheError | SnapshotCorrupt | CommitOverdue> =>
    Effect.gen(function*() {
      const { environment, workspace, containerId, hook, podTemplate, live } = request;
      const { release } = container;
      const [own] = parent?.layers ?? [];
      const hookSeconds = restoreHookSeconds(backgroundReadMs);
      const waits = yield* loadWaitOf(request, release, askedAt);
      // Of a release's save, where its bayma's hook says what it caught, found while the container's bundle says it, as the save
      // ends the container.
      const caughtAt = live === undefined && request.stateDir !== undefined
        ? yield* stateFileHeld(request.stateDir, "last-save.json", [container]).pipe(
          withinStateRead(posix.join(request.stateDir, "last-save.json")),
          Effect.mapError((error) => new SnapshotCorrupt({ environment, workspace, reason: `what its save caught cannot be told: ${error.message}` })),
        )
        : null;
      // Its summary, of a release that writes one, beside what it staged, which goes with it.
      const summaryFile = savesSummary(release, savesSummarySince) ? `${staged}.summary.json` : null;
      const saved = runsc.checkpoint(release, {
        containerId,
        imagePath: staged,
        hook,
        hookTimeout: `${hookSeconds}s`,
        ...(own === undefined ? {} : { parentImagePath: cache.image(environment, workspace, own.digest) }),
        live: live !== undefined,
        ...(waits === null ? {} : { loadWaitSeconds: waits }),
        ...(summaryFile === null ? {} : { summaryFile }),
      });
      const watchdogMs = waits === null ? 0 : loadWait.watchdogMs ?? loadWaitWatchdogMs(waits);
      const [pauseMs] = yield* timed(askedAt === null ? saved : boundWhileLoading(request, askedAt, saved, watchdogMs)).pipe(
        Effect.tap(([took]) => Effect.annotateCurrentSpan({ "alasio.snapshot.pause_ms": took })),
        step("alasio.node_agent.checkpoint.runsc", request),
      );
      // What the save caught, as bayma's hook wrote it as the sandbox was saved. A release's save has ended its sandbox by now,
      // so one whose last-save.json cannot be read, as workspace storage is slow or fails, is kept all the same, recording no
      // generation, said and counted: the clear of its record before its next bind is what keeps a stale one from restoring.
      const caught = live !== undefined ? yield* lastSave(request, live, [container]) : caughtAt === null || request.stateDir === undefined ? undefined : yield* stateFileAt(caughtAt, posix.join(request.stateDir, "last-save.json"), LastSaveFile).pipe(
        withinStateRead(posix.join(request.stateDir, "last-save.json")),
        Effect.map(({ generation, running }): Caught => ({ stateDir: request.stateDir ?? "", generation, running })),
        Effect.catch((error) =>
          Effect.logWarning(`the snapshot ${snapshot} of ${environment}/${workspace} records no generation of bayma's state, as what its release's save caught could not be read: ${error.message}`).pipe(
            Effect.andThen(Effect.sync(() => uncaught.add(1))),
            Effect.as(undefined),
          )
        ),
      );
      // Its image and the images it holds pages of, as its summary says them; or as runsc's inspect and layers say them, each
      // a process of its own, of a release that writes none.
      const described = summaryFile === null
        ? Effect.all([
          runsc.inspect(release, staged).pipe(step("alasio.node_agent.checkpoint.inspect", request)),
          runsc.layers(release, staged).pipe(step("alasio.node_agent.checkpoint.layers", request)),
        ]).pipe(Effect.map(([inspected, digests]) => ({ image: inspected, layers: digests })))
        : summaryOf(release, summaryFile).pipe(step("alasio.node_agent.checkpoint.summary", request));
      const { image, layers: digests } = yield* described;
      if (image.version !== runscVersion(release)) {
        return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image says runsc ${image.version} saved it, not ${runscVersion(release)}` });
      }
      // Each image it holds pages of is one of its parent's, whose files its manifest says.
      const known = new Map((parent?.layers ?? []).map((layer) => [layer.digest, layer]));
      const referred = yield* Effect.forEach(digests, (digest) => {
        const layer = known.get(digest);
        return layer === undefined ? Effect.fail(new SnapshotCorrupt({ environment, workspace, reason: `its image refers to the image ${digest}, which is none of its parent's` })) : Effect.succeed(layer);
      });
      yield* cache.commit(environment, workspace, snapshot, image.digest).pipe(step("alasio.node_agent.checkpoint.cache", request));
      const directory = cache.image(environment, workspace, image.digest);
      const put = "alasio.node_agent.checkpoint.upload.put";
      // In one pass: each of its files hashed as it is uploaded, all at once, and what it refers to taken to be in the bucket
      // as its parent's manifest says, looked at once it is committed (checkLater). Or its files' SHA-256 first, then each
      // uploaded in turn, then its own image and those it refers to looked at together, as the prepare of a restore looks at
      // its chain's. Its files' SHA-256 are what its manifest records, and the cache's stamp of them as checked so.
      const hashedFirst = onePassSaves ? null : yield* onDisk(() => imageFiles(directory)).pipe(
        Effect.tap((hashed) => cache.stamp(environment, workspace, image.digest, hashed)),
        step("alasio.node_agent.checkpoint.digest", request),
      );
      const uploaded: Effect.Effect<readonly Layer[], S3Error | CacheError | SnapshotCorrupt> = hashedFirst === null
        ? hashedAndPut(request, image.digest, put).pipe(
          Effect.tap(({ files }) => cache.stamp(environment, workspace, image.digest, files.map(({ etag: _, ...file }) => file))),
          Effect.map((own) => [own, ...referred]),
          step("alasio.node_agent.checkpoint.upload", request),
        )
        : Effect.gen(function*() {
          const own = yield* upload(request, image.digest, hashedFirst, put).pipe(step("alasio.node_agent.checkpoint.upload", request));
          return yield* Effect.gen(function*() {
            const missing = yield* notInBucket(request, [own, ...referred]);
            if (missing.includes(own)) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image ${image.digest} is not in the bucket as it was put` });
            return [own, ...yield* Effect.forEach(referred, (layer) => (missing.includes(layer) ? putAgain(request, layer, put) : Effect.succeed(layer)))];
          }).pipe(step("alasio.node_agent.checkpoint.heads", request));
        });
      const uploadedAt = yield* Clock.currentTimeMillis;
      // Its image synced to the node's disk as it uploads, before it is committed.
      const [[uploadMs, layers], verified] = yield* Effect.all([
        timed(uploaded),
        verifiedStaged({ environment, workspace, snapshot, release }, [image.digest, ...referred.map(({ digest }) => digest)]).pipe(step("alasio.node_agent.checkpoint.verify", request)),
        onDisk(() => syncImage(directory)).pipe(step("alasio.node_agent.checkpoint.sync", request)),
      ], { concurrency: 3 });
      yield* inTime(request, snapshot, uploadedAt);
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
        durability: "store",
        ...(caught === undefined ? {} : live !== undefined ? { live: caught } : { caught }),
      };
      // Committed: in the cache, then in the bucket, its manifest and its commit, which make it durable; in one pass, the
      // two written at once, the one waiting on the other no more.
      yield* Effect.gen(function*() {
        yield* onDisk(() => writeFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), JSON.stringify(manifest)));
        if (verified) yield* sayVerified(manifest);
        const named = bucket.putJson(manifestKey(request, snapshot), manifest);
        const committed = bucket.putJson(`${prefixOf(request)}/${CURRENT_FILE}`, manifest);
        yield* onePassSaves ? Effect.all([named, committed], { concurrency: 2, discard: true }) : named.pipe(Effect.andThen(committed));
      }).pipe(step("alasio.node_agent.checkpoint.commit", request));
      if (onePassSaves) yield* checkLater(manifest, referred);
      return manifest;
    }).pipe(
      Effect.onError(() => cache.unstage(request.environment, request.workspace, snapshot).pipe(Effect.ignore)),
      // Its summary, once read, or where it failed, as nothing reads it after.
      Effect.ensuring(onDisk(() => rm(`${staged}.summary.json`, { force: true })).pipe(Effect.ignore)),
    );

  /**
   * Compacts the chain of images of `committed`, the snapshot a release's checkpoint has just
   * committed, where compactionDue says to: its image and its layers into one image, the
   * snapshot `compactedFrom` it, which becomes the workspace's current snapshot, and so the
   * base of the next delta of a sandbox restored from it, if the workspace has not moved on
   * from `committed` meanwhile. Its new snapshot, or null where it did not compact it.
   * Nothing waits on it: it writes and uploads its image before it holds the workspace, then
   * checks it in the bucket and commits it once it holds it, as a checkpoint commits its own.
   * It says how it ended, compacted or superseded, by `ended`.
   */
  const compactNow = (committed: Manifest, ended: (outcome: string) => Effect.Effect<void>): Effect.Effect<Manifest | null, RunscError | ReleaseNotInstalled | S3Error | CacheError | SnapshotCorrupt | CommitOverdue> =>
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
      yield* cache.room(committed.bytes, spared).pipe(Effect.flatMap(logEvicted));
      const staged = yield* cache.stage(environment, workspace, snapshot);
      /** The compacted image's digest, once the cache holds it, until a manifest names it. */
      let unnamed: string | null = null;
      const compacting = join(cache.workspace(environment, workspace), COMPACTING_FILE);
      const say = (digest?: string) => onDisk(() => writeFile(compacting, JSON.stringify({ snapshot, ...(digest === undefined ? {} : { digest }) })));
      yield* say();
      return yield* Effect.gen(function*() {
        // runsc writes the image into a directory it makes. It reads and writes the node's disk, as the hashes and the upload
        // after it read it, beside the restores on this node, which they yield to: runsc stopped while one reads its image, the
        // others waiting meanwhile.
        yield* onDisk(() => rm(staged, { recursive: true, force: true }));
        const [compactMs] = yield* timed(runsc.compact(release, { imagePath: cache.image(environment, workspace, own.digest), layerPath: join(cache.workspace(environment, workspace), LAYERS_DIR), output: staged, pausing: loading.pausing })).pipe(
          step("alasio.node_agent.compaction.compact", committed),
        );
        yield* Effect.sync(() => compactionDuration.record(compactMs / 1000));
        const image = yield* Effect.gen(function*() {
          const inspected = yield* runsc.inspect(release, staged);
          if (inspected.version !== runscVersion(release)) {
            return yield* new SnapshotCorrupt({ environment, workspace, reason: `its compacted image says runsc ${inspected.version} wrote it, not ${runscVersion(release)}` });
          }
          const referred = yield* runsc.layers(release, staged);
          if (referred.length > 0) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its compacted image refers to the images ${referred.join(", ")}, where it is to refer to none` });
          return inspected;
        }).pipe(step("alasio.node_agent.compaction.inspect", committed));
        yield* say(image.digest);
        yield* cache.commit(environment, workspace, snapshot, image.digest);
        unnamed = image.digest;
        const files = yield* onDisk(() => imageFiles(cache.image(environment, workspace, image.digest), quietly));
        yield* cache.stamp(environment, workspace, image.digest, files);
        const uploadedAt = yield* Clock.currentTimeMillis;
        const [[uploadMs, uploaded], verified] = yield* Effect.all([
          timed(upload(committed, image.digest, files, "alasio.node_agent.compaction.upload.put", quietly)).pipe(step("alasio.node_agent.compaction.upload", committed)),
          verifiedStaged({ environment, workspace, snapshot, release }, [image.digest]),
        ], { concurrency: 2 });
        // What it uploaded, should the workspace have moved on meanwhile, no manifest names, and is collected as any such image is.
        const manifest = yield* exclusive(
          [committed],
          Effect.gen(function*() {
            const key = `${prefixOf(committed)}/${CURRENT_FILE}`;
            const read = yield* taggedManifestAt(committed, key);
            if (read?.manifest.snapshot !== committed.snapshot) return null;
            // Checked only now that it holds the workspace, which a collection on this node holds too: one may have taken
            // what it uploaded, which no manifest named, as it waited for it. The cache holds its image, spared meanwhile.
            const layer = yield* layerInBucket(committed, uploaded, "alasio.node_agent.compaction.upload.put");
            yield* inTime(committed, snapshot, uploadedAt);
            const compacted: Manifest = { ...committed, snapshot, layers: [layer], bytes: bytesOf([layer]), uploadMs, compactedFrom: committed.snapshot };
            const cached = join(cache.workspace(environment, workspace), MANIFEST_FILE);
            const sayingVerified = join(cache.workspace(environment, workspace), VERIFIED_FILE);
            const verifiedBefore = yield* onDisk(() => readFile(sayingVerified, "utf8").catch(() => null));
            // The cache saying the commit it compacted again, and what of it was verified, as where its own is not committed.
            const cacheAsBefore = onDisk(() => writeFile(cached, JSON.stringify(committed))).pipe(
              Effect.andThen(onDisk(() => (verifiedBefore === null ? rm(sayingVerified, { force: true }) : writeFile(sayingVerified, verifiedBefore)))),
            );
            yield* onDisk(() => writeFile(cached, JSON.stringify(compacted)));
            if (verified) yield* sayVerified(compacted);
            // Its manifest unwritten, as one that fails or is stopped leaves it, the cache as it was; and its image, which a
            // failure forgets, left to collection where the manifest may be written all the same, as one whose answer was lost
            // may be: forgotten only where the bucket says it holds no such manifest.
            const named = bucket.head(manifestKey(committed, snapshot)).pipe(Effect.map((found) => found !== null), Effect.orElseSucceed(() => true));
            yield* bucket.putJson(manifestKey(committed, snapshot), compacted).pipe(
              Effect.onError(() =>
                cacheAsBefore.pipe(
                  Effect.ignore,
                  Effect.andThen(named),
                  Effect.flatMap((maybe) =>
                    Effect.sync(() => {
                      if (maybe) unnamed = null;
                    })
                  ),
                )
              ),
            );
            // Named by its manifest from here, its image is no more one a failure forgets, as its commit may stand.
            unnamed = null;
            // Uncommitted: the cache as it was, then its manifest goes.
            const uncommit = cacheAsBefore.pipe(Effect.andThen(bucket.remove(manifestKey(committed, snapshot))));
            // Committed only as the bucket holds the commit it read: another node's agent may commit the workspace's next
            // snapshot meanwhile, a release of its sandbox restored there, which no lock of this agent's holds back, and which
            // stands; its image is then left to be collected. A commit whose answer was lost, as the store may have written it
            // all the same, or may yet, is told by the commit read again: committed where that names it. Its image is never
            // forgotten then, as another node's agent may have restored it, committed and checkpointed it meanwhile, a commit
            // that names it among its layers: it is left to collection, which keeps what the commit and the latest manifest
            // name. It is uncommitted only where the commit is another (its ETag another than the one read) that names none
            // of it, as one asked as of the one read can be written no more; and left be where it is the one read still, or
            // cannot be read, or is gone, as another node's agent may have spent it once it was written.
            const put = yield* bucket.putJsonIf(key, compacted, read.etag).pipe(Effect.result);
            if (put._tag === "Success") return put.success ? compacted : yield* Effect.as(uncommit, null);
            const after = yield* taggedManifestAt(committed, key).pipe(Effect.result);
            if (after._tag === "Failure") return yield* put.failure;
            const now = after.success;
            if (now?.manifest.snapshot === snapshot) return compacted;
            if (now !== null && now.etag !== read.etag && !now.manifest.layers.some(({ digest }) => digest === image.digest)) yield* uncommit;
            return yield* put.failure;
          }),
        ).pipe(step("alasio.node_agent.compaction.commit", committed));
        if (manifest === null) {
          yield* ended("superseded");
          yield* Effect.logInfo(`compacted the snapshot ${committed.snapshot} of ${prefixOf(committed)}, as ${why}, but the workspace moved on from it meanwhile: the compacted image is left to be collected`);
          return null;
        }
        yield* ended("compacted");
        // Recorded for the gateway, which restores the workspace from it as it binds it, reading nothing of the bucket.
        yield* recordCompacted(committed, { from: committed.snapshot, manifest }).pipe(
          step("alasio.node_agent.compaction.record", committed),
          Effect.catch((error) => Effect.logWarning(`could not record the compaction of ${committed.snapshot} of ${prefixOf(committed)} into ${snapshot}, so it is restored from ${committed.snapshot}, of its ${committed.layers.length} images: ${error.message}`)),
        );
        yield* Effect.logInfo(
          `compacted the snapshot ${committed.snapshot} of ${prefixOf(committed)}, as ${why}: its ${committed.layers.length} images, ${committed.bytes} bytes, are the image of its snapshot ${snapshot}, ${manifest.bytes} bytes, in ${compactMs} ms, uploaded in ${uploadMs} ms`,
        );
        return manifest;
      }).pipe(
        // What it staged goes; and what it wrote of an image no manifest names, in the cache and the bucket, where it was
        // stopped (prepare) or failed: nothing of it is left to collect, and its upload's parts were aborted with it.
        Effect.onError(() =>
          cache.unstage(environment, workspace, snapshot).pipe(
            Effect.andThen(Effect.suspend(() => (unnamed === null ? Effect.void : forgetImage(committed, unnamed)))),
            Effect.ignore,
          )
        ),
        Effect.ensuring(onDisk(() => rm(compacting, { force: true })).pipe(Effect.ignore)),
      );
    });

  /** Removes the image `digest` of the workspace from the cache and from the bucket, its uploads begun aborted, as one that no manifest names. */
  const forgetImage = (ref: WorkspaceRef, digest: string): Effect.Effect<void, S3Error | CacheError> =>
    cache.drop(ref.environment, ref.workspace, digest).pipe(
      Effect.andThen(bucket.abortUploads(`${prefixOf(ref)}/${LAYERS_DIR}/${digest}/`)),
      Effect.andThen(bucket.list(`${prefixOf(ref)}/${LAYERS_DIR}/${digest}/`)),
      Effect.flatMap((objects) => Effect.forEach(objects, ({ key }) => bucket.remove(key), { discard: true })),
    );

  /**
   * Removes what each compaction an agent before this one left as it stopped wrote, as COMPACTING_FILE says them, as a
   * compaction that fails removes it: its staged image, and its image, where no manifest names it, from the cache and the
   * bucket, its uploads begun aborted. What fails is said, and left to the next. Its runsc ended with that agent, whose pod
   * shares the node's PIDs: its container's processes end with it.
   */
  const recoverCompactions: Effect.Effect<void> = Effect.gen(function*() {
    for (const ref of yield* cache.list) {
      const { environment, workspace } = ref;
      const compacting = join(cache.workspace(environment, workspace), COMPACTING_FILE);
      const text = yield* onDisk(() => readFile(compacting, "utf8").catch(() => null));
      if (text === null) continue;
      const { snapshot, digest } = yield* Schema.decodeUnknownEffect(Compacting)(text);
      yield* cache.unstage(environment, workspace, snapshot);
      const named = digest !== undefined && (yield* bucket.head(manifestKey(ref, snapshot))) !== null;
      if (digest !== undefined && !named) yield* forgetImage(ref, digest);
      yield* onDisk(() => rm(compacting, { force: true }));
      yield* Effect.logInfo(`removed what the compaction into ${snapshot} of ${prefixOf(ref)} an agent before this one left as it stopped wrote: ${digest === undefined || named ? "no image" : `its image ${digest}`}`);
    }
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`could not remove what the compactions an agent before this one left as it stopped wrote: ${error.message}`)),
    withLogScope("node-agent"),
  );
  // Before this one compacts anything.
  yield* recoverCompactions;

  /**
   * Compacts `committed` as compactNow does, once no other compaction runs on the node, in the agent's scope: what fails is
   * said, and changes nothing. One of the workspace's before it is stopped, as the workspace has moved on from it. Each is
   * counted once, by how it ended: one stopped after it has counted its outcome, as it removes what it staged once it has
   * committed or been superseded, is not counted again as stopped. A trace of its own, linked to the checkpoint's it began in,
   * which it outlives.
   */
  const compactLater = (committed: Manifest): Effect.Effect<void> =>
    Effect.suspend(() => {
      const begunIn = currentTraceparent();
      /** How it ended, once counted. */
      let outcome: string | null = null;
      const ended = (counted: string) =>
        Effect.sync(() => {
          if (outcome !== null) return;
          outcome = counted;
          compactions.add(1, { "alasio.node_agent.compaction.outcome": counted });
        });
      return compactNow(committed, ended).pipe(
        compacting.withPermits(1),
        // Its workspace's images, which it reads and writes beside the lock, are spared meanwhile.
        (compacting) => inFlightWhile([committed], compacting),
        Effect.tapError((error) => ended(error._tag)),
        Effect.catch((error) => Effect.logWarning(`compacting the snapshot ${committed.snapshot} of ${prefixOf(committed)} failed, which changes nothing of it: ${error.message}`)),
        Effect.onInterrupt(() =>
          Effect.suspend(() =>
            outcome !== null ? Effect.void : ended("stopped").pipe(
              Effect.andThen(Effect.logInfo(`stopped compacting the snapshot ${committed.snapshot} of ${prefixOf(committed)}, which changes nothing of it`)),
            )
          )
        ),
        withAlasioSpan("alasio.node_agent.compaction", { attributes: { ...spanAttributes(committed), "alasio.snapshot": committed.snapshot }, parent: null, links: begunIn === null ? [] : [begunIn] }),
        withLogScope("node-agent"),
      );
    }).pipe(
      FiberMap.run(compactionsRunning, prefixOf(committed)),
      Effect.asVoid,
    );

  /** Says which workspaces were evicted from the node's cache. */
  const logEvicted = (evicted: readonly CachedWorkspace[]) =>
    Effect.forEach(evicted, ({ environment, workspace, bytes }) =>
      Effect.logInfo(`evicted the images of ${environment}/${workspace} (${bytes} bytes) from the node's cache`), { discard: true });

  const evict: Effect.Effect<void, CacheError> = Effect.suspend(() => cache.evict(spared)).pipe(Effect.flatMap(logEvicted));

  const timings = {
    floorMs: loadingSave.floorMs ?? LOADING_SAVE_FLOOR_SECONDS * 1000,
    stallMs: loadingSave.stallMs ?? LOADING_STALL_SECONDS * 1000,
    latestMs: loadingSave.latestMs ?? LOADING_SAVE_LATEST_SECONDS * 1000,
  };
  const loadingPollMs = loadingSave.pollMs ?? 5000;

  /**
   * `saving`, runsc's save of the sandbox a release's checkpoint, `request`, asked at
   * `askedAt`, names, from as it starts, the workspace's lock held: where the sandbox's restore
   * from the bucket reads it still, its reads are looked at every loadingPollMs as the save runs,
   * their recent window kept (recentReads), and its token revoked, said why, as
   * loadingSaveRevocation says (./names.ts
   * LOADING_SAVE_FLOOR_SECONDS). A save waits for the pages it saves that the restore has not
   * loaded yet, which a restore whose store is away asks for again as long as its sandbox runs,
   * its token lasting as it asks (./store-proxy.ts), the sandbox paused meanwhile, past what
   * alasio waits for the checkpoint. Revoked, its reads are refused, which its gofer does not
   * retry: its loading fails, and so the save does, as one of a sandbox saved no more
   * (PAGES_UNREAD), or as its hook's, where bayma's hook waited on the pages first. Nothing where
   * the save ends first, its sandbox ended with it, or the restore is done reading, or the
   * sandbox was not restored from the bucket. A save under a release that bounds its own waits
   * for the pages (loadWaitOf) is deferred by runsc, its sandbox running on, once they expire:
   * `floorMs`, what runsc's bound lets it run, is then the least it runs before a stall or a rate
   * too slow counts, so that this bounds it only should runsc not have deferred it by then, as a
   * watchdog; and its backstop once the workspace's saves have been deferred for long enough.
   */
  const boundWhileLoading = <A, E>(request: CheckpointRequest, askedAt: number, saving: Effect.Effect<A, E>, floorMs = 0): Effect.Effect<A, E> => {
    const holder = prefixOf(request);
    const bounds = { ...timings, floorMs: Math.max(timings.floorMs, floorMs) };
    const watching = Effect.gen(function*() {
      const startedAt = yield* Clock.currentTimeMillis;
      const first = storeProxy.readingOf(holder);
      if (first === null) return;
      let movedAt = startedAt;
      let last = first.served;
      // Its looks, those within the stall window and the one before them kept, which its recent rate is measured over.
      let seen: ReadsSeen[] = [{ at: startedAt, served: first.served }];
      for (;;) {
        yield* Effect.sleep(loadingPollMs);
        const now = yield* Clock.currentTimeMillis;
        const reading = storeProxy.readingOf(holder);
        if (reading === null) return;
        if (reading.served > last) {
          last = reading.served;
          movedAt = now;
        }
        seen.push({ at: now, served: reading.served });
        const before = seen.findLastIndex((look) => look.at <= now - timings.stallMs);
        if (before > 0) seen = seen.slice(before);
        const recent = recentReads(seen, timings.stallMs);
        const why = loadingSaveRevocation({ askedAt, startedAt, now, movedAt, servedAtStart: first.served, served: reading.served, expected: reading.expected, recentFrom: recent.at, servedAtRecentFrom: recent.served }, bounds);
        if (why === null) continue;
        storeProxy.revoke(holder);
        const of = reading.expected === null || !Number.isFinite(reading.expected) ? "" : ` of ${reading.expected}`;
        return yield* Effect.logWarning(
          `revoked the token of the restore of ${holder} from the bucket, as ${why}, ${reading.served}${of} bytes of its pages served: its reads are refused from now, so its loading fails, and its save with it, rather than hold its sandbox paused past what alasio waits for the save; the session comes back from bayma's checkpoints`,
        );
      }
    });
    return watching.pipe(Effect.forkScoped, Effect.andThen(saving), Effect.scoped);
  };

  const checkpoint = (request: CheckpointRequest): Effect.Effect<Manifest, CheckpointFailed | SandboxUnsaveable | LiveSaveDeferred | SaveDeferred> => {
    /** How the checkpoint fails for `error`, saying `reason`: unsaveable where runsc says its sandbox is, as it will of every checkpoint after. */
    const failed = (error: { readonly message: string }, reason: string) =>
      error.message.includes(PAGES_UNREAD)
        ? new SandboxUnsaveable({ environment: request.environment, workspace: request.workspace, reason })
        : new CheckpointFailed({ environment: request.environment, workspace: request.workspace, reason });
    /** Whether `error` says runsc deferred the save, its wait for pages still loading expired (LOAD_WAIT_EXPIRED): what it wrote is unstaged, and the sandbox runs on. */
    const deferredBy = (error: { readonly message: string }) => error.message.includes(LOAD_WAIT_EXPIRED);
    const deferredSave = (error: { readonly message: string }) => new SaveDeferred({ environment: request.environment, workspace: request.workspace, reason: error.message });
    const moved = "as the workspace has moved on since it was taken";
    const taken = (askedAt: number | null) =>
      take(request, askedAt).pipe(
        Effect.tapError((error) => Effect.sync(() => checkpoints.add(1, { "alasio.node_agent.checkpoint.outcome": deferredBy(error) ? "SaveDeferred" : error._tag }))),
      );
    /** Notes that a release's save of the workspace's sandbox was deferred, from the first of the sandbox's since its last that was not (loadWaitOf). */
    const noteDeferred = Clock.currentTimeMillis.pipe(Effect.map((now) => {
      const before = deferredSince.get(prefixOf(request));
      deferredSince.set(prefixOf(request), { since: before?.containerId === request.containerId ? before.since : now, containerId: request.containerId });
    }));
    const committed = (askedAt: number | null) => request.live === undefined
      // Of a container not there, or another pod's, nothing is saved, nor ended, by this checkpoint, so what was saved of
      // the workspace before is kept: as a release asked again once its sandbox has ended, its last checkpoint the one that
      // ended it, finds.
      ? ofPod(request.containerId, request.namespace, request.pod).pipe(
        // The sandbox its saves were deferred of, if any, is gone, or another's.
        Effect.tapError((error) => Effect.sync(() => {
          checkpoints.add(1, { "alasio.node_agent.checkpoint.outcome": error._tag });
          deferredSince.delete(prefixOf(request));
        })),
        Effect.mapError((error) => failed(error, `${error.message}; its snapshot is kept, as no sandbox of the container was reached`)),
        Effect.andThen(taken(askedAt).pipe(
          Effect.catch((error): Effect.Effect<never, CheckpointFailed | SandboxUnsaveable | SaveDeferred> =>
            // Deferred, what it wrote unstaged, its sandbox running on, its restore reading on: its snapshot is the one it builds on still.
            deferredBy(error) ? noteDeferred.pipe(Effect.andThen(Effect.fail(deferredSave(error)))) : forgetNow(request).pipe(
              Effect.matchEffect({
                onSuccess: () => Effect.fail(failed(error, `${error.message}; its snapshot is forgotten, ${moved}`)),
                onFailure: (forgetting) => Effect.fail(failed(error, `${error.message}; forgetting its snapshot, ${moved}, failed too: ${forgetting.message}`)),
              }),
            )
          ),
          Effect.onInterrupt(() => forgetNow(request).pipe(Effect.ignore)),
          // The sandbox it saved ended, and its restore's reads of the bucket with it; not one deferred, which runs on.
          Effect.onExit((exit) =>
            Exit.isFailure(exit) && Option.exists(Cause.findErrorOption(exit.cause), (error) => error._tag === "SaveDeferred") ? Effect.void : Effect.sync(() => {
              bases.delete(request.containerId);
              storeProxy.revoke(prefixOf(request));
              deferredSince.delete(prefixOf(request));
            })
          ),
        )),
      )
      : taken(null).pipe(
        // The sandbox runs on from the image it was saved to, which its next checkpoint saves what
        // changed since, all of its memory read now: its restore reads the bucket no more.
        Effect.tap((manifest) => Effect.sync(() => {
          bases.set(request.containerId, manifest);
          storeProxy.revoke(prefixOf(request));
        })),
        // What runsc saved of the writes since its base, which the next would build on, is gone with it; not where it was
        // deferred, before it saved them, its writes since its base still tracked.
        Effect.tapError((error) => (deferredBy(error) ? Effect.void : Effect.sync(() => bases.delete(request.containerId)))),
        Effect.mapError((error) => (deferredBy(error) ? deferredSave(error) : failed(error, `${error.message}; its snapshot is kept, as a live checkpoint that fails changes nothing`))),
      );
    // A release's save is bounded while its sandbox loads, as one asked now, its wait for the workspace counted.
    const asked: Effect.Effect<number | null> = request.live === undefined ? Clock.currentTimeMillis : Effect.succeed(null);
    // A live one of a sandbox whose restore reads the bucket still is not taken, said so.
    const deferred: Effect.Effect<void, LiveSaveDeferred> = Effect.suspend(() => {
      const reading = request.live === undefined ? null : storeProxy.readingOf(prefixOf(request));
      if (reading === null) return Effect.void;
      const error = new LiveSaveDeferred({ environment: request.environment, workspace: request.workspace, served: reading.served, expected: reading.expected });
      return Effect.sync(() => checkpoints.add(1, { "alasio.node_agent.checkpoint.outcome": error._tag })).pipe(
        Effect.andThen(Effect.logInfo(error.message)),
        Effect.andThen(Effect.fail(error)),
      );
    });
    return deferred.pipe(
      Effect.andThen(asked),
      Effect.flatMap((at) => exclusive([request], committed(at).pipe(Effect.tap(() => evict.pipe(Effect.catch((error) => Effect.logWarning(error.message))))), "alasio.node_agent.checkpoint.lock")),
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
      Effect.tapError((error) => (error._tag === "LiveSaveDeferred" ? Effect.void : Effect.logWarning(error.message))),
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

  /** The image `layer` of the workspace's snapshot `snapshot`, from the bucket, staged in the cache, each file checked against the manifest's as it is received. */
  const received = (ref: WorkspaceRef, snapshot: string, layer: Layer): Effect.Effect<void, S3Error | CacheError | SnapshotCorrupt> =>
    Effect.gen(function*() {
      const { environment, workspace } = ref;
      const directory = yield* cache.stage(environment, workspace, snapshot);
      for (const file of layer.files) {
        const target = join(directory, file.path);
        yield* onDisk(() => mkdir(dirname(target), { recursive: true }));
        const got = yield* bucket.getFile(layerKey(ref, layer.digest, file.path), target, file.bytes);
        if (got.bytes !== file.bytes || got.sha256 !== file.sha256) {
          return yield* new SnapshotCorrupt({
            environment,
            workspace,
            reason: `its image ${layer.digest}'s file ${file.path} is ${got.bytes} bytes of SHA-256 ${got.sha256}, where its manifest says ${file.bytes} bytes of ${file.sha256}`,
          });
        }
      }
    });

  /** The image `layer`, staged as `received` stages it, among the cache's images, stamped as checked. */
  const committed = ({ environment, workspace }: WorkspaceRef, snapshot: string, layer: Layer): Effect.Effect<void, CacheError> =>
    cache.commit(environment, workspace, snapshot, layer.digest).pipe(Effect.andThen(cache.stamp(environment, workspace, layer.digest, layer.files)));

  /** The image `layer` of the workspace's, from the bucket into the cache, each file checked against the manifest's as it is received, and stamped so. */
  const download = (ref: WorkspaceRef, snapshot: string, layer: Layer): Effect.Effect<void, S3Error | CacheError | SnapshotCorrupt> =>
    received(ref, snapshot, layer).pipe(Effect.andThen(committed(ref, snapshot, layer)));

  /**
   * Whether the cache's copy of the image `layer` of the workspace's is what the manifest
   * says, file by file, as its stamp says it was checked, its files unchanged since, none of
   * them read (the cache's stamped); one that is not is removed, said so, and it alone: the
   * workspace's other images are what they are, as each is looked at in turn.
   *
   * What an image's files hold is checked where they are written: each file's SHA-256 as a
   * checkpoint or a compaction writes it, which its manifest records, and as a download
   * receives it, against its manifest's. Between, the cache holds them to what they were by
   * each one's size, inode and modification time, which whatever writes or replaces a file
   * changes; and runsc verifies the image's structure, its pages metadata's checksums and each
   * layer's identity and size, as the cache stages it (verifiedStaged), or else as a prepare
   * prepares it. What changes a file's bytes in place, its
   * size and modification time kept, as a disk that rots does, is not seen until the restore
   * reads those pages.
   */
  const intact = (ref: WorkspaceRef, layer: Layer): Effect.Effect<boolean, CacheError> =>
    Effect.gen(function*() {
      const { environment, workspace } = ref;
      if (!(yield* cache.holds(environment, workspace, layer.digest))) return false;
      const stamped = yield* cache.stamped(environment, workspace, layer.digest);
      if (stamped !== null && sameFiles(stamped, layer)) return true;
      yield* Effect.logWarning(`the node's cached copy of the image ${layer.digest} of ${prefixOf(ref)} is not what its manifest says, or has changed since it was checked: it is read again from the bucket`);
      yield* cache.drop(environment, workspace, layer.digest);
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

  /**
   * runsc's verdict on the image of `manifest` prepared at `path` (runsc image verify --host):
   * whole and restorable here, `path`; Unrestorable where this node's runsc restores it not
   * (another version, a CPU feature, a platform); otherwise, malformed or missing a layer,
   * corrupt.
   */
  const verifiedAt = ({ environment, workspace, snapshot, release }: Pick<Manifest, "environment" | "workspace" | "snapshot" | "release">, path: string): Effect.Effect<string, Unrestorable | SnapshotCorrupt | RunscError> =>
    runsc.verify(release, path).pipe(
      Effect.catchTag("ReleaseNotInstalled", (error) => Effect.fail(new Unrestorable({ environment, workspace, snapshot, reason: error.message }))),
      Effect.flatMap((found): Effect.Effect<string, Unrestorable | SnapshotCorrupt | RunscError> => {
        if (found.restorable) return Effect.succeed(path);
        if (found.status === VERIFY_STATUSES.notRestorable) {
          return Effect.fail(new Unrestorable({ environment, workspace, snapshot, reason: `this node's runsc does not restore it: ${found.said}` }));
        }
        return Effect.fail(new SnapshotCorrupt({ environment, workspace, reason: `runsc image verify exited ${found.status}: ${found.said}` }));
      }),
    );

  /** The chain of images, by their digests, `manifest` restores from: its image, then its layers. */
  const chainOf = (manifest: Manifest): readonly string[] => manifest.layers.map(({ digest }) => digest);

  /** Whether runsc verified the chain of `manifest`'s images in the cache under its release as the cache staged them, as VERIFIED_FILE says. */
  const verifiedAlready = (manifest: Manifest): Effect.Effect<boolean> =>
    Effect.promise(() => readFile(join(cache.workspace(manifest.environment, manifest.workspace), VERIFIED_FILE), "utf8").catch(() => null)).pipe(
      Effect.map((text) => {
        const said = text === null ? Option.none() : Schema.decodeUnknownOption(VerifiedChain)(text);
        return Option.isSome(said) && said.value.release === manifest.release && JSON.stringify(said.value.digests) === JSON.stringify(chainOf(manifest));
      }),
    );

  /**
   * Whether runsc verifies the chain of images `digests` of `of`'s workspace as the cache has
   * just staged its first, a checkpoint's or a compaction's, as the prepare of a restore of it
   * would (verifiedAt), from a copy of its own (chainCopy), beside the image's upload: one the
   * cache does not hold whole, or that runsc does not verify, is not, said so, and its prepare
   * verifies it. What a restore's prepare fetches into the cache is verified as it prepares
   * it; what is fetched after a restore read it from the bucket (fetchLater) is its own
   * snapshot's, which it spends, and its next snapshot's chain is verified as its checkpoint
   * stages it.
   */
  const verifiedStaged = (of: Pick<Manifest, "environment" | "workspace" | "snapshot" | "release">, digests: readonly string[]): Effect.Effect<boolean> =>
    Effect.gen(function*() {
      for (const digest of digests) if (!(yield* cache.holds(of.environment, of.workspace, digest))) return false;
      yield* Effect.scoped(Effect.flatMap(cache.chainCopy(of.environment, of.workspace, digests), (copy) => verifiedAt(of, copy)));
      return true;
    }).pipe(
      Effect.catch((error) =>
        Effect.logWarning(`runsc did not verify the images of the snapshot ${of.snapshot} of ${prefixOf(of)} as the node's cache staged them, so a restore of it here verifies them: ${error.message}`).pipe(Effect.as(false))
      ),
    );

  /** Says beside the workspace's images that runsc verified `manifest`'s chain as the cache staged it (VERIFIED_FILE). */
  const sayVerified = (manifest: Manifest): Effect.Effect<void, CacheError> =>
    onDisk(() => writeFile(join(cache.workspace(manifest.environment, manifest.workspace), VERIFIED_FILE), JSON.stringify({ release: manifest.release, digests: chainOf(manifest) })));

  /**
   * The checkpoint gofer's options for the image `own` of the workspace's, its layers under
   * its own prefix of them, as the bucket keeps them, read through the agent's proxy of the
   * bucket with `token`, the restore's own, which reads that prefix alone: by the agent's
   * pod's address, or by the proxy's socket on the node, as `transport` says; GOFER_READ_PARALLEL
   * reads at once; reading again, for as long as the sandbox runs, the pages a read of failed,
   * where `pagesRetry`.
   */
  const goferOptions = (ref: WorkspaceRef, own: string, { accessKeyId, secretAccessKey }: StoreToken, transport: Transport, pagesRetry: boolean): string =>
    JSON.stringify({
      ...(transport === "socket" && storeProxy.socket !== null ? { endpoint: STORE_SOCKET_ENDPOINT, socket: storeProxy.socket } : { endpoint: storeProxy.endpoint }),
      region: storeProxy.region,
      bucket: storeProxy.bucket,
      object_prefix: `${prefixOf(ref)}/${LAYERS_DIR}/${own}/`,
      layers_prefix: `${prefixOf(ref)}/${LAYERS_DIR}/`,
      addressing: "path",
      credentials: { access_key_id: accessKeyId, secret_access_key: secretAccessKey },
      read_parallel: GOFER_READ_PARALLEL,
      ...(pagesRetry ? { pages_retry: "while-running" } : {}),
    });

  /**
   * Removes the checkpoint gofer's options file of each snapshot prepared of the workspace,
   * which its restore read as its pod's containers started: the restore's token is on the
   * node no more. One not removed is said, and changes nothing else.
   */
  const goferOptionsGone = (ref: WorkspaceRef): Effect.Effect<void> =>
    cache.unprepareRemote(ref.environment, ref.workspace, GOFER_OPTIONS_FILE).pipe(
      Effect.catch((error) => Effect.logWarning(`could not remove the checkpoint gofer's options of ${prefixOf(ref)}, which hold its restore's token: ${error.message}`)),
    );

  /**
   * Fetches into the cache, one at a time, each image of `manifest` it does not hold, once the
   * restore from the bucket that reads by the token `reader` is done reading it through the
   * proxy (./store-proxy.ts readsDone), in the agent's scope: the next checkpoint of the
   * restored sandbox is a delta of the snapshot's image once the cache holds it. A fetch beside
   * the restore would read the bucket again as the restore reads it, and slow it: the whole
   * image a second time, the chain of a delta whole. Each is downloaded
   * beside the workspace's images, which the cache spares meanwhile, holding the workspace
   * only to look at what the cache holds of it and to commit it, so that a checkpoint of the
   * sandbox, as its pod stops, waits on no download; one fetch of a workspace's runs at a time.
   * What fails is said, and leaves that checkpoint whole. A trace of its own, linked to the
   * prepare's it began in, which it outlives.
   */
  const fetchLater = (ref: WorkspaceRef, manifest: Manifest, reader: string): Effect.Effect<void> =>
    Effect.suspend(() => {
      const begunIn = currentTraceparent();
      return fetchNow(ref, manifest, reader).pipe(
        withAlasioSpan("alasio.node_agent.fetch", { attributes: { ...spanAttributes(ref), "alasio.snapshot": manifest.snapshot }, parent: null, links: begunIn === null ? [] : [begunIn] }),
        withLogScope("node-agent"),
        Effect.forkIn(scope),
        Effect.asVoid,
      );
    });

  /** Fetches into the cache what fetchLater says, as it runs it in the background. */
  const fetchNow = (ref: WorkspaceRef, manifest: Manifest, reader: string): Effect.Effect<void> =>
    Effect.forEach(manifest.layers, (layer) =>
      inFlightWhile([ref], Effect.gen(function*() {
        if (yield* exclusive([ref], intact(ref, layer))) return;
        yield* cache.room(bytesOf([layer]), spared).pipe(Effect.flatMap(logEvicted));
        yield* received(ref, manifest.snapshot, layer);
        yield* exclusive([ref], committed(ref, manifest.snapshot, layer));
        yield* Effect.sync(() => fetches.add(1, { "alasio.node_agent.fetch.outcome": "fetched" }));
      })).pipe(
        Effect.tapError((error) => Effect.sync(() => fetches.add(1, { "alasio.node_agent.fetch.outcome": error._tag }))),
      ), { discard: true }).pipe(
      (fetching) => Effect.scoped(RcMap.get(fetchLocks, prefixOf(ref)).pipe(Effect.flatMap((lock) => fetching.pipe(Semaphore.withPermit(lock))))),
      (fetching) => storeProxy.readsDone(reader).pipe(Effect.andThen(fetching)),
      Effect.tap(() => Effect.logInfo(`fetched the images of the snapshot ${manifest.snapshot} of ${prefixOf(ref)} into the node's cache, for its next delta`)),
      // Within its budget again, the least recently used workspace evicted.
      Effect.tap(() => evict.pipe(Effect.catch((error) => Effect.logWarning(error.message)))),
      Effect.catch((error) => Effect.logWarning(`could not fetch the images of the snapshot ${manifest.snapshot} of ${prefixOf(ref)} into the node's cache, so its next checkpoint is whole: ${error.message}`)),
    );

  /**
   * Tells the proxy how many bytes of pages the restore of the snapshot `snapshot` of the
   * workspace, read from the bucket by the token `accessKeyId` as the gofer's `options` say,
   * reads through it, as runsc says of the image, so that the agent's stop waits for them all
   * however its loader paces its reads: unknown until runsc has said, which it asks in the
   * background, for as long as inspectTimeoutMs at most, its process killed past it; where
   * runsc cannot say, the stop waits on the restore only while its reads go on, as the proxy's
   * quiet tells. runsc reads the image from a copy of the options of its own (remoteCopy), as
   * the restore's are removed once it has resumed, which it may be before runsc has read them.
   */
  const expectPages = (ref: WorkspaceRef, snapshot: string, accessKeyId: string, release: string, options: string): Effect.Effect<void> =>
    Effect.sync(() => storeProxy.expect(accessKeyId, Number.POSITIVE_INFINITY)).pipe(
      Effect.andThen(
        cache.remoteCopy(ref.environment, ref.workspace, snapshot, GOFER_OPTIONS_FILE, options).pipe(
          Effect.flatMap((copy) => runsc.inspect(release, copy)),
          Effect.scoped,
          Effect.timeoutOrElse({ duration: inspectTimeoutMs, orElse: () => Effect.fail(new Error(`it did not answer within ${inspectTimeoutMs / 1000} s`)) }),
          Effect.tap(({ pageBytes }) => Effect.sync(() => storeProxy.expect(accessKeyId, pageBytes))),
          Effect.tapError((error) =>
            Effect.sync(() => storeProxy.expect(accessKeyId, null)).pipe(
              Effect.andThen(Effect.logWarning(`runsc does not say how many bytes of pages the restore of ${prefixOf(ref)} reads through the node agent, so the agent's stop waits on it only while its reads go on: ${error.message}`)),
            )
          ),
          Effect.ignore,
          withLogScope("node-agent"),
          Effect.forkIn(scope),
        ),
      ),
      Effect.asVoid,
    );

  /**
   * The snapshot of the workspace, prepared for a restore on this node: from the node's cache
   * where it holds every image of it intact and this node's runsc restores it there, as
   * `runsc image verify --host` says; otherwise from the bucket, which the restore reads as it
   * goes, through the checkpoint gofer, as the request says it reads it, its images fetched
   * into the cache once it is done reading them (fetchLater).
   */
  const prepare = ({ gofer, manifest, ...ref }: PrepareRequest): Effect.Effect<Prepared, PrepareError> =>
    exclusive(
      [ref],
      Effect.gen(function*() {
        // A restore of the workspace before this one is done with, its reads of the bucket with it.
        storeProxy.revoke(prefixOf(ref));
        // Its compaction is stopped: the restore spends the snapshot, so the workspace moves on from what it would commit,
        // and it would read and write the node's disk as the restore reads its image there.
        yield* FiberMap.remove(compactionsRunning, prefixOf(ref));
        // It reads its image until runsc says it has read every page (resume), for as long as its sandbox's would take at most.
        yield* loading.begin(prefixOf(ref), backgroundReadMs);
        const { environment, workspace, snapshot, release } = manifest;
        yield* Effect.annotateCurrentSpan({ "alasio.snapshot": snapshot });
        const why = yield* unrestorable(manifest);
        if (why !== null) return yield* new Unrestorable({ environment, workspace, snapshot, reason: why });
        const started = yield* Clock.currentTimeMillis;
        const [own] = manifest.layers;
        if (own === undefined) return yield* new SnapshotCorrupt({ environment, workspace, reason: "its manifest names no image" });
        const verified = (path: string) => verifiedAt(manifest, path);
        // The images the cache holds as they were checked (intact): where it holds them all,
        // prepared from it and verified by runsc, as they were as the cache staged them where
        // it says so (verifiedStaged); one that does not verify has the images read from the
        // bucket instead.
        let imagePath: string | null = null;
        const held = yield* Effect.forEach(manifest.layers, (layer) => intact(ref, layer)).pipe(
          Effect.map((intacts) => intacts.every(Boolean)),
          step("alasio.node_agent.prepare.intact", ref),
        );
        if (held) {
          const linked = yield* cache.prepare(environment, workspace, snapshot, manifest.layers.map(({ digest }) => digest)).pipe(step("alasio.node_agent.prepare.cache", ref));
          imagePath = yield* Effect.flatMap(verifiedAlready(manifest), (already) => (already ? Effect.succeed(linked) : verified(linked))).pipe(
            step("alasio.node_agent.prepare.verify", ref),
            Effect.catchTag("SnapshotCorrupt", (error) =>
              Effect.logWarning(`the node's cached copy of the snapshot ${snapshot} of ${prefixOf(ref)} does not restore (${error.reason}): it is read from the bucket`).pipe(
                Effect.andThen(cache.clear(environment, workspace)),
                Effect.as(null),
              )
            ),
          );
        }
        const source: Prepared["source"] = imagePath === null ? "bucket" : "cache";
        // A release that does not restore straight from the bucket restores from the cache: the
        // images it does not hold intact fetched into it first, then verified; where that fails,
        // those it took from the cache are read from the bucket too, and verified again, a
        // failure of which is corrupt.
        const fetchedFirst = imagePath === null && !restoresFromBucket(release, restoresFromBucketSince);
        if (fetchedFirst) {
          const downloaded: Layer[] = [];
          const fetched = Effect.gen(function*() {
            const missing: Layer[] = [];
            for (const layer of manifest.layers) if (!(yield* intact(ref, layer))) missing.push(layer);
            if (missing.length > 0) yield* cache.room(bytesOf(missing), spared).pipe(Effect.flatMap(logEvicted));
            yield* Effect.forEach(missing, (layer) => download(ref, snapshot, layer).pipe(Effect.tap(() => Effect.sync(() => downloaded.push(layer)))), { discard: true }).pipe(
              Effect.onError(() => cache.clear(environment, workspace).pipe(Effect.ignore)),
            );
            return yield* verified(yield* cache.prepare(environment, workspace, snapshot, manifest.layers.map(({ digest }) => digest)));
          });
          imagePath = yield* fetched.pipe(
            step("alasio.node_agent.prepare.download", ref),
            Effect.catchTag("SnapshotCorrupt", (error) => {
              const kept = manifest.layers.filter((layer) => !downloaded.includes(layer));
              if (kept.length === 0) return Effect.fail(error);
              return Effect.logWarning(`${error.message}: the images this node's cache held of it are read again from the bucket`).pipe(
                Effect.andThen(Effect.forEach(kept, ({ digest }) => cache.drop(environment, workspace, digest), { discard: true })),
                Effect.andThen(fetched),
              );
            }),
            Effect.tapErrorTag("SnapshotCorrupt", () => cache.clear(environment, workspace).pipe(Effect.ignore)),
          );
        }
        if (imagePath === null) {
          // Read as the restore goes, each of its images must be in the bucket as it was put: the files of all of them
          // looked at together, as a chain's are many, and each look a round trip to the store.
          const [missing] = yield* notInBucket(ref, manifest.layers).pipe(step("alasio.node_agent.prepare.heads", ref));
          if (missing !== undefined) return yield* new SnapshotCorrupt({ environment, workspace, reason: `its image ${missing.digest} is not in the bucket as it was put` });
        }
        /** The token the restore reads the bucket by, where it does. */
        let reader: string | null = null;
        if (imagePath === null) {
          // Through the socket where asked and the proxy serves one; over TCP otherwise.
          if (gofer?.transport === "socket" && storeProxy.socket === null) yield* Effect.logWarning(`the restore of ${prefixOf(ref)} asks to read the snapshots bucket through the node agent's socket, which it does not serve: it reads it over TCP`);
          const transport: Transport = gofer?.transport === "socket" && storeProxy.socket !== null ? "socket" : "tcp";
          const token = yield* Effect.sync(() => storeProxy.mint(prefixOf(ref), `${prefixOf(ref)}/${LAYERS_DIR}/`, transport)).pipe(step("alasio.node_agent.prepare.token", ref));
          if (token === null) return yield* new AgentStopping({ environment, workspace });
          const options = goferOptions(ref, own.digest, token, transport, gofer?.pagesRetry ?? false);
          imagePath = yield* cache.prepareRemote(environment, workspace, snapshot, GOFER_OPTIONS_FILE, options).pipe(
            Effect.tap(() => expectPages(ref, snapshot, token.accessKeyId, release, options)),
            step("alasio.node_agent.prepare.gofer", ref),
          );
          reader = token.accessKeyId;
        }
        yield* onDisk(() => writeFile(join(cache.workspace(environment, workspace), MANIFEST_FILE), JSON.stringify(manifest)));
        prepared.set(prefixOf(ref), manifest);
        if (reader !== null) yield* fetchLater(ref, manifest, reader);
        const preparedMs = (yield* Clock.currentTimeMillis) - started;
        yield* evict.pipe(Effect.catch((error) => Effect.logWarning(error.message)));
        yield* Effect.sync(() => prepares.add(1, { "alasio.node_agent.prepare.outcome": "prepared", "alasio.node_agent.prepare.source": source }));
        yield* Effect.annotateCurrentSpan({ "alasio.snapshot.source": source, "alasio.snapshot.bytes": manifest.bytes, "alasio.snapshot.prepare_ms": preparedMs });
        yield* Effect.logInfo(`prepared the snapshot ${snapshot} of ${prefixOf(manifest)} from the ${source}: ${manifest.bytes} bytes in ${preparedMs} ms`);
        return { manifest, imagePath, source };
      }),
      "alasio.node_agent.prepare.lock",
    ).pipe(
      // Prepared for no restore, it reads nothing.
      Effect.onError(() => loading.end(prefixOf(ref))),
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
      // A sandbox restored from the one the latest was compacted from, as a compaction committed, extends that one just as well.
      const origin = latest === undefined ? undefined : (yield* manifestAt(ref, latest.key))?.compactedFrom;
      const kept = manifests.filter((each, index) => each === latest || (origin !== undefined && each.key === manifestKey(ref, origin)) || recent(manifests[index + 1] ?? each));
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

  /**
   * The chain `chain` of the filter table of `ip`'s family on the node, as iptables lists it;
   * unlisted where iptables exits 1, as it does for a chain there is not, saying why.
   */
  const listed = (ip: string) => (chain: string): Effect.Effect<Listing, HostCommandError> =>
    onNode(host, [isIPv6(ip) ? "ip6tables" : "iptables", "-t", "filter", "-S", chain], ({ exitCode }) => exitCode === 0 || exitCode === 1).pipe(
      Effect.map(({ exitCode, stdout, stderr }) => (exitCode === 0 ? { rules: stdout } : { unlisted: stderr.trim() || "it exited 1" })),
    );

  /** What of its rules a pod's address lacks in its family's filter table on the node, as ./kube-router.ts says, or null. */
  const policyGap = (address: PodAddress): Effect.Effect<string | null, HostCommandError> => listedEgressGap(listed(address.ip), address);

  /** Waits for every address of the pod's to have its rules, POLICY_WAIT_MS at most: how long it waited, in milliseconds. */
  const policiesInForce = ({ namespace, pod, podIPs, policies }: ResumeRequest & RunningPod): Effect.Effect<number, PoliciesNotInForce | HostCommandError> =>
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
   * The manifest of the snapshot `request` says its sandbox was restored from: as it was
   * prepared on this node, or as the cache keeps it beside its images, or as the bucket keeps
   * it (`manifests/<snapshot>.json`), which a resume after an agent started again, or after
   * another prepare, reads. One that none of them holds, or that is no manifest of it, refuses
   * the resume, the sandbox left paused: whether it may run cannot be told. One the bucket fails
   * to give fails the resume so, the sandbox left paused, for a try after to read it.
   */
  const restoredFrom = (request: ResumeRequest): Effect.Effect<Manifest, SnapshotOutlived | S3Error> =>
    Effect.gen(function*() {
      const named = (manifest: Manifest | null | undefined) => (manifest?.snapshot === request.snapshot ? manifest : null);
      const known = named(prepared.get(prefixOf(request))) ?? named(yield* lastCached(request));
      if (known !== null) return known;
      const read = yield* manifestAt(request, manifestKey(request, request.snapshot)).pipe(Effect.result);
      if (read._tag === "Failure" && read.failure._tag === "S3Error") return yield* Effect.fail(read.failure);
      const found = read._tag === "Success" ? named(read.success) : null;
      if (found !== null) return found;
      return yield* new SnapshotOutlived({
        environment: request.environment,
        workspace: request.workspace,
        snapshot: request.snapshot,
        reason: `its manifest cannot be read, so whether it may run cannot be told: ${read._tag === "Failure" ? read.failure.message : `neither this node nor the bucket holds it`}`,
      });
    });

  /**
   * The file `name` of bayma's state directory `stateDir`, in the container, as `schema`
   * decodes it, read on the volume of `containers` that holds it, as an untrusted file
   * (./volume.ts), of STATE_FILE_BYTES at most, and of no property but the schema's, within
   * STATE_READ_TIMEOUT_MS: one not read by then, as a volume that hangs leaves it, is unread,
   * what it says not told. Fails saying why, as an Error.
   */
  const stateFile = <A>(stateDir: string, name: string, schema: Schema.Decoder<A>, containers: readonly RunningContainer[]): Effect.Effect<A, Error> =>
    Effect.flatMap(stateFileHeld(stateDir, name, containers), (held) => stateFileAt(held, posix.join(stateDir, name), schema)).pipe(withinStateRead(posix.join(stateDir, name)));

  /** `effect`, a read of bayma's state file `path`, failed where it is not done within STATE_READ_TIMEOUT_MS. */
  const withinStateRead = (path: string) => <A>(effect: Effect.Effect<A, Error>): Effect.Effect<A, Error> =>
    effect.pipe(Effect.timeoutOrElse({ duration: STATE_READ_TIMEOUT_MS, orElse: () => Effect.fail(new Error(`${path} was not read within ${STATE_READ_TIMEOUT_MS / 1000} s`)) }));

  /** Where the file `name` of bayma's state directory `stateDir` is on the node, as the volumes of `containers`' pod hold it: what stateFileAt reads, as a container's bundle says it only while it is there. */
  const stateFileHeld = (stateDir: string, name: string, containers: readonly RunningContainer[]): Effect.Effect<UnderRoot, Error> => {
    const path = posix.join(stateDir, name);
    return Effect.tryPromise({
      try: async () => {
        const held = (await Promise.all(containers.map(({ bundle }) => hostPathOf(nodeRoot, bundle, path)))).find((found): found is UnderRoot => found !== null);
        if (held === undefined) throw new Error(`no volume of its pod holds ${path}`);
        return held;
      },
      catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
    });
  };

  /** The file of bayma's state directory `held` names, `path` in its container, as `schema` decodes it, read as stateFile says. */
  const stateFileAt = <A>(held: UnderRoot, path: string, schema: Schema.Decoder<A>): Effect.Effect<A, Error> =>
    Effect.tryPromise({
      try: async () => {
        const bytes = await readUnder(held, STATE_FILE_BYTES);
        try {
          return Schema.decodeUnknownSync(schema, { onExcessProperty: "error" })(UTF8.decode(bytes));
        } catch (error) {
          throw new Error(`${path} is none of bayma's: ${(error instanceof Error ? error.message : String(error)).replace(/\s*\n\s*/gu, " ")}`);
        }
      },
      catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
    });

  /** What the live save of the sandbox `request` names caught, as bayma's last-save.json says. */
  const lastSave = (request: CheckpointRequest, { stateDir }: LiveSave, containers: readonly RunningContainer[]): Effect.Effect<Caught, SnapshotCorrupt> =>
    stateFile(stateDir, "last-save.json", LastSaveFile, containers).pipe(
      Effect.map(({ generation, running }) => ({ stateDir, generation, running })),
      Effect.mapError((error) => new SnapshotCorrupt({ environment: request.environment, workspace: request.workspace, reason: `what its live save caught cannot be told: ${error.message}` })),
    );

  /**
   * Whether `manifest`, taken live or by a release, holds the session still, a restored sandbox let run from
   * it: bayma's state generation is the one the save caught, as its generation.json says on
   * the volume of the pod's containers that holds it. Fails where it has moved on, or where
   * it cannot be told.
   */
  const holdsStill = (manifest: Manifest, caught: Caught, containers: readonly RunningContainer[]): Effect.Effect<void, SnapshotOutlived> =>
    stateFile(caught.stateDir, "generation.json", StateGenerationFile, containers).pipe(
      Effect.flatMap(({ generation }) =>
        generation === caught.generation ? Effect.void : Effect.fail(new Error(`bayma's state generation is ${generation}, and was ${caught.generation} as it was saved`))
      ),
      Effect.mapError((error) => new SnapshotOutlived({ environment: manifest.environment, workspace: manifest.workspace, snapshot: manifest.snapshot, reason: error.message })),
    );

  /**
   * The pod `request` names, restored into, once it runs every container of its, with its
   * addresses, as the API server says it, whose watch is asked again as it ends, and as it fails,
   * as one whose connection drops does, up to WATCH_TRIES times: refused where it is gone, or has
   * not within RESTORE_RUNNING_WAIT_MS, and where what the API server says of it is none the
   * agent takes, as an address or a container's id of another form.
   */
  const runningPod = ({ namespace, pod: name }: ResumeRequest): Effect.Effect<RunningPod, PodNotRunning | KubeApiError> =>
    Effect.gen(function*() {
      const refused = (reason: string) => new PodNotRunning({ namespace, pod: name, reason });
      /** What of `found` a resume needs once it runs, or null while it does not. */
      const runningOf = (found: V1Pod): Effect.Effect<RunningPod | null, PodNotRunning> => {
        const statuses = found.status?.containerStatuses ?? [];
        const podIPs = (found.status?.podIPs ?? []).map(({ ip }) => ip);
        if (statuses.length === 0 || !statuses.every(({ state }) => state?.running) || podIPs.length === 0) return Effect.succeed(null);
        const containerIds = statuses.map(({ containerID }) => containerID?.replace(/^[a-z]+:\/\//u, "") ?? "");
        if (podIPs.some((ip) => isIP(ip) === 0)) return Effect.fail(refused(`its addresses are not all IP addresses: ${podIPs.join(", ")}`));
        if (containerIds.some((id) => !Schema.is(ContainerId)(id))) return Effect.fail(refused(`its containers' ids are not all containerd's: ${containerIds.join(", ")}`));
        return Effect.succeed({ podIPs, containerIds });
      };
      for (;;) {
        const found = yield* pods(namespace, name).pipe(
          Stream.mapEffect((each) => (each === null ? Effect.fail(refused("it is gone")) : runningOf(each))),
          Stream.filter((each): each is RunningPod => each !== null),
          Stream.runHead,
          Effect.retry({ ...WATCH_TRIES, while: (error) => error._tag === "KubeApiError" }),
        );
        if (Option.isSome(found)) return found.value;
      }
    }).pipe(
      Effect.timeoutOrElse({ duration: RESTORE_RUNNING_WAIT_MS, orElse: () => Effect.fail(new PodNotRunning({ namespace, pod: name, reason: `it did not within ${RESTORE_RUNNING_WAIT_MS / 1000} s` })) }),
    );

  /**
   * Waits for the pod `request` names to accept connections on its bayma's port, at the first
   * of its addresses, `running`'s, from the node, as kubelet's readiness probe connects, each
   * try given ACCEPT_TRY_SECONDS, as kubelet gives its own: tried again ACCEPT_POLL_MS after each
   * that fails, for ACCEPT_WAIT_MS at most, by one shell of the agent's own in the node's
   * network; refused where it does not.
   */
  const accepting = ({ namespace, pod, port }: ResumeRequest, { podIPs: [ip = ""] }: RunningPod): Effect.Effect<void, NotAccepting | HostCommandError> => {
    const address = isIPv6(ip) ? `[${ip}]:${port}` : `${ip}:${port}`;
    const tried = `until timeout ${ACCEPT_TRY_SECONDS} bash -c 'exec 3<>/dev/tcp/${ip}/${port}' 2>/dev/null; do sleep ${ACCEPT_POLL_MS / 1000}; done`;
    return host.inNetwork(["timeout", String(ACCEPT_WAIT_MS / 1000), "bash", "-c", tried]).pipe(
      Effect.flatMap(({ exitCode, stderr }) =>
        exitCode === 0 ? Effect.void : Effect.fail(new NotAccepting({ namespace, pod, address, reason: stderr.trim() || `no try connected, the last ended ${exitCode}` }))
      ),
    );
  };

  const resume = (request: ResumeRequest): Effect.Effect<void, ResumeError> =>
    Effect.gen(function*() {
      const running = yield* runningPod(request).pipe(step("alasio.node_agent.resume.pod_running", request));
      const waited = yield* policiesInForce({ ...request, ...running }).pipe(
        Effect.tap((took) => Effect.annotateCurrentSpan({ "alasio.resume.policy_wait_ms": took })),
        step("alasio.node_agent.resume.policies", request),
      );
      yield* Effect.annotateCurrentSpan({ "alasio.resume.policy_wait_ms": waited });
      const containers = yield* Effect.forEach(running.containerIds, (id) => ofPod(id, request.namespace, request.pod)).pipe(step("alasio.node_agent.resume.containers", request));
      const from = yield* restoredFrom(request);
      // Taken live or by a release, of the generation of bayma's state it caught: refused where bayma has moved on since.
      const caughtBy = from.live ?? from.caught;
      if (caughtBy !== undefined) yield* holdsStill(from, caughtBy, containers);
      yield* Effect.forEach(running.podIPs, forgetConnections, { discard: true }).pipe(step("alasio.node_agent.resume.conntrack", request));
      // Its restore reads what its sandbox did not touch first in the background, for a while yet, from its resume on: bayma's
      // hook, which runsc's resume runs, waits on those reads of the pages it touches, however long after the prepare.
      storeProxy.resumed(prefixOf(request), backgroundReadMs);
      yield* Effect.forEach(running.containerIds, (id, index) => runsc.resume(containers[index]?.release ?? "", id), { discard: true }).pipe(step("alasio.node_agent.resume.runsc", request));
      // Its sandbox runs from the snapshot it was restored from, which its next checkpoint saves what changed since, none of
      // its saves deferred yet.
      for (const id of running.containerIds) bases.set(id, from);
      deferredSince.delete(prefixOf(request));
      // It reads its image until runsc says it has read every page: a trace of its own, linked to the resume's, which it
      // outlives, saying how long the agent's background work yielded to restores meanwhile.
      const resumedIn = yield* Effect.sync(currentTraceparent);
      yield* Effect.forEach(running.containerIds, (id, index) => runsc.restored(containers[index]?.release ?? "", id), { discard: true }).pipe(
        Effect.catch((error) => Effect.logWarning(`runsc did not say when the restore of ${prefixOf(request)} has read its image, so the agent's compactions yield to it no more: ${error.message}`)),
        Effect.andThen(loading.end(prefixOf(request))),
        Effect.flatMap((yielded) => (yielded === null ? Effect.void : Effect.annotateCurrentSpan({ "alasio.loading.yield_ms": yielded }))),
        withAlasioSpan("alasio.node_agent.loading", { attributes: spanAttributes(request), parent: null, links: resumedIn === null ? [] : [resumedIn] }),
        withLogScope("node-agent"),
        Effect.forkIn(scope),
      );
      return [waited, running] as const;
    }).pipe(
      // Not resumed, as it failed or its request went, it reads nothing more.
      Effect.onError(() => loading.end(prefixOf(request))),
      // Its gofer read its options as its pod's containers started, the restore's token among them.
      Effect.ensuring(goferOptionsGone(request)),
      // Ready as kubelet's probe would find it, before it is answered.
      Effect.flatMap(([waited, running]) => Effect.as(accepting(request, running).pipe(step("alasio.node_agent.resume.accepting", request)), waited)),
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

  /**
   * Ends the containers of the pod `request` names that runsc holds paused through runsc, then
   * kills what runsc runs for its containers, as the module says: what it killed, of how many it
   * found, and the containers it ended through runsc. Of those held paused, the pod's own, its
   * sandbox's root container, is killed first, which ends every container of the sandbox, the
   * others then listed again: one still held paused, as one of another sandbox would be, is killed
   * next, and one held paused after its kill fails the abandon. One whose sandbox died, which runsc lists stopped, is
   * not killed through runsc; one listed paused whose sandbox is gone as its state is read (SandboxGone), as one that
   * died meanwhile, is killed through the current release's runsc, as the release that ran it is not known any more.
   * A process one of them forks is the container's from its fork on, as its arguments, until it execs, are those of the
   * runsc it forked from, and its own after; so one forked as the scan before ran, or as the process it forked from was
   * killed, is found by the scan after the kills. The scans go on until one kills nothing new, at most ABANDON_SCANS of
   * them.
   */
  const abandonNow = (request: AbandonRequest): Effect.Effect<readonly [readonly Killed[], number, readonly string[]], RunscError | ReleaseNotInstalled> =>
    Effect.gen(function*() {
      // The pod's by its uid too: a pod of its name made since is another, whose processes are its own.
      const listed = Effect.map(runsc.containers, (all) =>
        all.filter(({ annotations }) => podOf(annotations) === `${request.namespace}/${request.pod}` && annotations[POD_UID_ANNOTATION] === request.uid)
      );
      let containers = yield* listed;
      const ended: string[] = [];
      for (;;) {
        const [paused] = containers.filter(({ status }) => status === "paused").sort((a, b) => Number(isRoot(b)) - Number(isRoot(a)));
        if (paused === undefined) break;
        if (ended.includes(paused.id)) {
          return yield* new RunscError({ release: "the node's", command: `kill ${paused.id} KILL`, reason: "runsc holds the container paused still after its kill" });
        }
        // Through the release that runs its sandbox, as its shim would; through the current release where its sandbox is
        // gone, as one that died since it was listed is, which runsc then holds stopped and whose kill it lets succeed. Any
        // other failure fails the abandon: a sandbox that may run on is killed through no runsc but its own release's.
        const release = yield* runsc.container(paused.id).pipe(
          Effect.map(({ release }) => release),
          Effect.catchTag("SandboxGone", (gone) =>
            Effect.as(Effect.logWarning(`killing ${paused.id} through the current release's runsc, ${runsc.current}, as ${gone.message}`), runsc.current)),
        );
        yield* runsc.kill(release, paused.id);
        ended.push(paused.id);
        containers = yield* listed;
      }
      const killed: Killed[] = [];
      // Each process found, by its pid and start time, killed or not.
      const seen = new Set<string>();
      for (let scan = 0; scan < ABANDON_SCANS; scan++) {
        const found = (yield* runsc.processes(containers)).filter(({ pid, startTime }) => !seen.has(`${pid} ${startTime}`));
        let killedNow = 0;
        for (const each of found) {
          seen.add(`${each.pid} ${each.startTime}`);
          if (yield* runsc.killProcess(each)) {
            killed.push({ containerId: each.containerId, pid: each.pid });
            killedNow += 1;
          }
        }
        if (killedNow === 0) break;
      }
      return [killed, seen.size, ended] as const;
    });

  const abandon = (request: AbandonRequest): Effect.Effect<readonly Killed[], RunscError | ReleaseNotInstalled> =>
    exclusive([request], Effect.sync(() => storeProxy.revoke(prefixOf(request))).pipe(Effect.andThen(goferOptionsGone(request)), Effect.andThen(abandonNow(request)))).pipe(
      Effect.ensuring(loading.end(prefixOf(request))),
      Effect.tap(([killed, found, ended]) =>
        Effect.sync(() => abandons.add(1, { "alasio.node_agent.abandon.outcome": "abandoned" })).pipe(
          Effect.andThen(Effect.annotateCurrentSpan({ "alasio.abandon.killed": killed.length, "alasio.abandon.paused": ended.length })),
          Effect.andThen(Effect.logInfo(
            `abandoned the restore of ${prefixOf(request)} in ${request.namespace}/${request.pod}: ${
              ended.length > 0 ? `killed through runsc ${ended.join(", ")}, which runsc held paused; ` : ""
            }${
              killed.length > 0 ? `killed ${killed.map(({ containerId, pid }) => `${pid} of ${containerId}`).join(", ")}` : ended.length > 0 ? "no process of them left to kill" : "runsc ran nothing for it"
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

  /** What the cache holds, as holdings says it, as it was listed last, and as of which of its changes (cacheChanges). */
  let listing: { readonly at: number; readonly cached: Holdings["cached"] } | null = null;

  /**
   * What this node holds as of the cache's change `at`: its releases, read anew, and what its cache holds, listed once a
   * change of it, however many ask, as each watch of what the node holds asks as it changes, and again as its wait ends.
   */
  const holdingsAt = (at: number): Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError> =>
    Effect.gen(function*() {
      const releases = yield* Effect.forEach(yield* runsc.releases, (release) => runsc.cpuFeatures(release).pipe(Effect.map((cpuFeatures) => ({ release, cpuFeatures }))));
      const known = listing;
      if (known?.at === at) return { node, releases, cached: known.cached };
      const cached = (yield* cache.list).map(({ usedAt, ...rest }) => ({ ...rest, usedAt: new Date(usedAt).toISOString() }));
      // Kept where no later listing was.
      if (listing === null || listing.at <= at) listing = { at, cached };
      return { node, releases, cached };
    });

  const holdings: Effect.Effect<Holdings, RunscError | ReleaseNotInstalled | CacheError> = Effect.flatMap(SubscriptionRef.get(cacheChanges), holdingsAt);

  const holdingsAfter = (seen: string, waitMs: number): Effect.Effect<Holdings | null, RunscError | ReleaseNotInstalled | CacheError> =>
    Effect.gen(function*() {
      const deadline = (yield* Clock.currentTimeMillis) + waitMs;
      for (;;) {
        // As of the change it is read at, taken before it is read, so one as it is read is not missed.
        const at = yield* SubscriptionRef.get(cacheChanges);
        const now = yield* holdingsAt(at);
        if (holdingsTag(now) !== seen) return now;
        const left = deadline - (yield* Clock.currentTimeMillis);
        if (left <= 0) return null;
        yield* SubscriptionRef.changes(cacheChanges).pipe(Stream.filter((count) => count !== at), Stream.runHead, Effect.timeoutOption(left));
      }
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
      exclusive([ref], spendNow(ref, snapshot)).pipe(
        Effect.tap((spent) =>
          spent === null
            ? Effect.logInfo(`spent nothing of ${prefixOf(ref)} for the snapshot ${snapshot}: the workspace's current snapshot is neither it nor one compacted from it`)
            : Effect.logInfo(`the snapshot ${snapshot} of ${prefixOf(ref)} is spent${spent.snapshot === snapshot ? "" : `, as ${spent.snapshot}, compacted from it`}`)
        ),
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
    holdingsAfter,
    workspaces: (environment) =>
      bucket.list(`${environment}/`).pipe(
        // What else the bucket holds under it is no workspace's of the agent's.
        Effect.map((listed) => [...new Set(listed.map(({ key }) => key.slice(environment.length + 1).split("/")[0] ?? ""))].filter((name) => NAME.test(name)).sort()),
      ),
    discard: (ref, keep) =>
      // Not while a compaction writes it, beside the lock, nor another operation waits on it.
      exclusive([ref], Effect.suspend(() => ((inFlight.get(prefixOf(ref)) ?? 0) > 1 ? Effect.succeed(false) : cache.discard(ref.environment, ref.workspace, keep)))).pipe(
        Effect.tap((discarded) => (discarded ? Effect.logInfo(`discarded from the node's cache the images of ${prefixOf(ref)} no snapshot needs any more`) : Effect.void)),
        withLogScope("node-agent"),
      ),
  };
});

/** The annotation containerd gives a container's spec of its pod's uid. */
const POD_UID_ANNOTATION = "io.kubernetes.cri.sandbox-uid";
/** The annotation containerd gives a container's spec of what it is of its pod: `sandbox` for the pod's own, its sandbox's root container. */
const CONTAINER_TYPE_ANNOTATION = "io.kubernetes.cri.container-type";

/** Whether `container` is its pod's own, its sandbox's root container, as containerd's annotations of its spec say. */
function isRoot({ annotations }: ListedContainer): boolean {
  return annotations[CONTAINER_TYPE_ANNOTATION] === "sandbox";
}

/** The pod a container is of, `<namespace>/<name>`, as the annotations containerd gives its spec say, `?` for what they do not. */
function podOf(annotations: Readonly<Record<string, string>>): string {
  return `${annotations["io.kubernetes.cri.sandbox-namespace"] ?? "?"}/${annotations["io.kubernetes.cri.sandbox-name"] ?? "?"}`;
}

/** The attributes of a span of an operation on a workspace: of its sandbox's too, which bears its name. */
function spanAttributes({ environment, workspace }: WorkspaceRef): Record<string, string> {
  return { "alasio.environment": environment, "alasio.workspace": workspace, "alasio.sandbox.name": workspace };
}

/** A step, `name`, of an operation on the workspace `ref`, in a span of its own under the operation's. */
function step(name: string, ref: WorkspaceRef) {
  return withAlasioSpan(name, { attributes: spanAttributes(ref) });
}
