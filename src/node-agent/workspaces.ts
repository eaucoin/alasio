/**
 * What a node agent does with workspaces: a workspace's files, on the node's mount of
 * workspace storage (WORKSPACES_ROOT, `<environment>/<workspace>/files`, holding
 * `workspace/` and `home/`), attached into the directory of a pod of a sandbox pool on its
 * node (`ATTACH_ROOT/<namespace>/<pod>`, which the pod mounts at /attach, its own by
 * `subPathExpr`, as pods of different namespaces may share a name), at
 * `ws/`, with its binding's token beside it at `token`, and what its bayma is to run with at
 * `env` (variables, each `NAME='value'`, as a shell reads them); detached once its sandbox can no
 * longer act; and a file of one read, or one received written into it, for what alasio does
 * with a session's files outside its sandbox.
 *
 * A pod's directory is made by kubelet as its pod starts, by its mount of its own: an attach
 * into one that is not there, or is no directory, as no pod of a pool on this node has one, is
 * refused, and none is made. It serves one binding in its life, as its pod does: an attach into
 * one that holds a mount at or under it is refused, so a workspace is never mounted over another,
 * which the pod would go on reaching beneath it. A detach is refused while any container of
 * the pod's sandbox runs: under a sandbox that acts, a lazy unmount would leave it holding
 * descriptors into the workspace it no longer shows. Once its sandbox has ended (its
 * checkpoint ended it, or its containers stopped), each mount under its directory is
 * unmounted lazily, as the sandbox's gofer keeps them busy to the last, then the directory is
 * removed, so its pod can be deleted: kubelet deletes no pod whose directory holds a mount.
 * And a pod's directory whose pod the cluster no longer has, however it went, a deletion with
 * no grace or a node lost among it, which no one may ever ask to detach, is detached by a sweep
 * (sweep, ./attachments.ts) once the directory has not changed for ORPHAN_GRACE_MS, and no
 * process of runsc's runs for any of the pod's containers: a session's workspace mounted into
 * it, or a harness's binding written for it, alike. A pod runsc does not run, as a harness's
 * under runc, has no such process to wait for: its binding is removed as its pod is gone from the
 * cluster, which kubelet ends it after, its harness reading it then for nothing.
 * Nothing is removed where anything is mounted at or under it still, on the node or as the
 * agent sees it; and nothing is removed recursively, which would walk into a workspace a mount
 * left there: what an attach wrote, then each directory by rmdir, empty, and what no attach
 * wrote is left, and said.
 *
 * Each is made, cloned, attached and recorded in workspace storage alone: where the node's
 * mount of it is not there, as for the moment its mount's pod hands it over, the agent
 * refuses (503), rather than write into the directory beneath it.
 *
 * And each is attached, or written into, only once the node's mount holds its quota: JuiceFS's
 * client counts what is written under a directory against the directory's quota only from when
 * it has loaded the quota, as it does every heartbeat, and takes off what is deleted after, so
 * what a workspace's sandbox wrote and deleted before would leave its quota counting less than
 * it holds, short by as much, until the daily quota check repairs it. An attach waits for it,
 * up to QUOTA_HELD_WITHIN_MS; a write, which its file's own bytes may take a while to, does
 * not: where the mount does not hold it, the agent refuses (503), to be asked again.
 *
 * A workspace is made as its session is (create): its directory, with its files' `workspace/`
 * and `home/`, its agent's, and its quota set on the directory (./juicefs.ts), at once; made
 * again, it is as it was, its quota set anew. Its quota is read back as it is asked for. One
 * is made a clone of another (clone), of any environment's, its quota its own; and one is
 * deleted, its quota with it, its files into workspace storage's trash.
 *
 * A file read or written is reached by its path from the workspace's root alone, walked one
 * name at a time as ./volume.ts walks one: one any part of whose path is a symlink, or that
 * is not a regular file, is refused, and so is one not reached in time.
 *
 * A free pod of a harness pool is given its binding (bindFree): the conversation it serves and
 * the token its caller bears, a file of JSON (BINDING_FILE) in its directory, which kubelet
 * made, written whole or not at all, and only where none is there, as a pod serves one
 * binding: a file of its own, its pod's user's alone, synced, then linked to its name, which
 * fails where the name is taken, the directory synced after, and one its pod's user may pass
 * through to its binding alone, as the pod mounts it to read. Detached, what it wrote is
 * removed as an attach's is.
 *
 * A bound pod's bayma that holds its stop for a save is told to go on with it (stop): its
 * hook, `sandbox-hook stop`, run in its container under the gVisor release that runs it, as
 * a checkpoint runs its hook, and in no container but the bayma of a pod of a pool on this
 * node: one whose pod's directory kubelet made, its pod named by containerd's annotations.
 * Such a bayma's sandbox is told to write what it holds of its files through to the node's
 * mount (sync), its `sync` run in it likewise, as a fork of its workspace is cloned from what
 * the mount has.
 *
 * Beside its files, which its sandbox sees, and so out of its sandbox's reach, a workspace
 * keeps the snapshot it was last released into (`released.json`, a Released), which its
 * gateway records as it releases it and reads as it binds it, holding its binding's Lease
 * meanwhile, the one writer at a time: written whole or not at all, a file of its own synced
 * then renamed over it, the directory synced after.
 */
import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { constants, type StatsFs } from "node:fs";
import { chmod, chown, link, lstat, mkdir, open, readdir, readFile, rename, rm, rmdir, statfs, unlink, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { Clock, Effect, Schedule, Schema } from "effect";

import { isValidVolumeId } from "../sandbox/names.ts";
import { type HostCommandError, type HostCommands, onNode } from "./host.ts";
import type { Juicefs, JuicefsError, Quota } from "./juicefs.ts";
import { BAYMA_CONTAINER, SNAPSHOT_ID } from "../kube/sandbox-shape.ts";
import { ATTACH_ROOT, BINDING_FILE, RESTORE_POD, WORKSPACES_ROOT } from "./names.ts";
import type { Runsc } from "./runsc.ts";
import { readUnder, writeUnder } from "./volume.ts";

/** The user and group a sandbox's agent runs as, whose its files are. */
export const AGENT_UID = 1000;

/** An environment's name, as it names its namespaces: main, or a branch's. */
const ENVIRONMENT = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/u;
/** A pod's name, as a label, which `subPathExpr` gives its directory. */
const POD = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/u;

/** What the agent was asked of a workspace and refused: why, and the status it answers with. */
export class WorkspaceRefused extends Schema.TaggedError<WorkspaceRefused>()("WorkspaceRefused", {
  status: Schema.Number,
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

const refused = (status: number, reason: string) => new WorkspaceRefused({ status, reason });

/** A workspace, by its environment and its id. */
export interface WorkspaceRef {
  readonly environment: string;
  readonly workspace: string;
}

/** A variable's name, as a shell takes one. */
const VARIABLE = /^[A-Z_][A-Z0-9_]*$/u;

/** `env` as a shell reads it, each value quoted whole: `NAME='value'`, a quote in it closed, escaped, and opened again. */
export const envFile = (env: Readonly<Record<string, string>>): string =>
  Object.entries(env).map(([name, value]) => `${name}='${value.split("'").join("'\\''")}'\n`).join("");

/** A mount point as mountinfo writes it, its octal escapes (`\040` for a space) undone. */
const unescapeMountPoint = (field: string): string => field.replace(/\\([0-7]{3})/gu, (_escape, octal: string) => String.fromCharCode(Number.parseInt(octal, 8)));

/** The mount points of the node's mount table, from its mountinfo (proc(5)): each line's fifth field. */
export function mountPoints(mountinfo: string): string[] {
  return mountinfo.split("\n").flatMap((line) => {
    const point = line.split(" ")[4];
    return point ? [unescapeMountPoint(point)] : [];
  });
}

/**
 * The snapshot a workspace was last released into: its id, and the gVisor release that took
 * it; where its sandbox was stopped as it ran out of its memory, the limit it ran out of, a
 * Kubernetes quantity, which its restore tells its bayma (../gateway/snapshots.ts); and, where
 * a release's own save took it, the pod it was taken of, by which a release of that pod asked
 * again finds it was saved already.
 */
export const Released = Schema.Struct({
  snapshot: Schema.String.check(Schema.isPattern(new RegExp(`^${SNAPSHOT_ID}$`, "u"))),
  release: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]*$/u)),
  memoryLimit: Schema.optional(Schema.String.check(Schema.isPattern(/^[0-9]+(\.[0-9]+)?[A-Za-z]{0,2}$/u))),
  pod: Schema.optional(Schema.String.check(Schema.isPattern(POD))),
});
export type Released = typeof Released.Type;

/** The name of a workspace's record of the snapshot it was released into, beside its files. */
const RELEASED_FILE = "released.json";

/** What a stop asks: the bound pod, by its namespace and name, its bayma's container by its runtime's id, and bayma's hook, `sandbox-hook stop`. */
export interface StopRequest {
  readonly namespace: string;
  readonly pod: string;
  readonly containerId: string;
  readonly hook: readonly string[];
}

/** How long bayma's hook may take to go on with its stop. */
const STOP_TIMEOUT_MS = 30_000;

/** The user and group a pod runs as, whose what is written for it is. */
export interface PodOwner {
  readonly uid: number;
  readonly gid: number;
}

/** What a sync asks: the bound pod, by its namespace and name, and its bayma's container by its runtime's id. */
export type SyncRequest = Omit<StopRequest, "hook">;

/** How long a bound pod's sandbox may take to write what it holds of its files through. */
const SYNC_TIMEOUT_MS = 60_000;

/** What a workspace's agent does with workspaces; see makeWorkspaces. */
export interface Workspaces {
  /** Makes `ref`, as the module says, its quota `quotaBytes`. */
  readonly create: (ref: WorkspaceRef, quotaBytes: number) => Effect.Effect<void, WorkspaceRefused | JuicefsError>;
  /** Makes `to` a clone of `from`, its quota `quotaBytes`; refused where `from` is not there, or `to` is. */
  readonly clone: (from: WorkspaceRef, to: WorkspaceRef, quotaBytes: number) => Effect.Effect<void, WorkspaceRefused | JuicefsError>;
  /** Deletes `ref`, its quota with it; one that is not there is not an error. */
  readonly remove: (ref: WorkspaceRef) => Effect.Effect<void, WorkspaceRefused | JuicefsError>;
  /** `ref`'s quota, and what it holds; null where it has none. */
  readonly quota: (ref: WorkspaceRef) => Effect.Effect<Quota | null, WorkspaceRefused | JuicefsError>;
  /** The workspaces of `environment` there are, by their names. */
  readonly list: (environment: string) => Effect.Effect<readonly string[], WorkspaceRefused>;
  /** The environments any workspace is of, by their names, as workspace storage holds them. */
  readonly environments: Effect.Effect<readonly string[], WorkspaceRefused>;
  /**
   * Attaches `ref`'s files into `pod`'s directory, its binding's `token` and the variables `env`
   * beside them: the directory kubelet made, or, `made`, one made now, for a restore's pod the
   * gateway makes once its workspace is there.
   */
  readonly attach: (ref: WorkspaceRef, pod: string, token: string, env: Readonly<Record<string, string>>, options?: { readonly made?: boolean }) => Effect.Effect<void, WorkspaceRefused | HostCommandError>;
  /**
   * Gives the free pod `pod` of `namespace`, of a harness pool, its `binding`, as the module
   * says, its user's and group's, `owner`; refused where kubelet made it no directory, or it
   * has a binding already.
   */
  readonly bindFree: (namespace: string, pod: string, owner: PodOwner, binding: string) => Effect.Effect<void, WorkspaceRefused | HostCommandError>;
  /** Detaches what is attached into the directory of `pod` of `namespace`, once none of `containerIds`, its sandbox's containers by their runtime's ids, runs. */
  readonly detach: (namespace: string, pod: string, containerIds: readonly string[]) => Effect.Effect<void, WorkspaceRefused | HostCommandError>;
  /**
   * Detaches what is attached into, or was written for, the directory of each pod of a namespace
   * of `present` that is not among its pods (the pods the cluster has there, by name), as the
   * module says, nothing of a namespace not in it: which it detached, `<namespace>/<pod>` each,
   * and which wait for their sandbox's end, or their mounts'.
   */
  readonly sweep: (present: ReadonlyMap<string, ReadonlySet<string>>) => Effect.Effect<Swept, HostCommandError>;
  /** The namespaces the attach root has pods' directories of. */
  readonly attachedNamespaces: Effect.Effect<readonly string[]>;
  /** The file at `path` of `ref`'s files, `workspace/...` or `home/...`, of `maxBytes` at most. */
  readonly read: (ref: WorkspaceRef, path: string, maxBytes: number) => Effect.Effect<Buffer, WorkspaceRefused>;
  /** Writes `bytes` as the file at `path` of `ref`'s files, its agent's, its directories made. */
  readonly write: (ref: WorkspaceRef, path: string, bytes: Uint8Array) => Effect.Effect<void, WorkspaceRefused>;
  /**
   * The pods of this node a workspace is attached into, each by whether what is attached is
   * reachable still: one whose node's mount of workspace storage ended under it, as a mount's
   * pod killed with no successor leaves it, is not, and its sandbox serves its workspace no more.
   */
  readonly attachments: Effect.Effect<readonly { readonly namespace: string; readonly pod: string; readonly reachable: boolean }[], HostCommandError>;
  /** Has the bayma of a bound pod go on with the stop it holds, as the module says. */
  readonly stop: (request: StopRequest) => Effect.Effect<void, WorkspaceRefused>;
  /** Has the sandbox of a bound pod's bayma write what it holds of its files through, as the module says. */
  readonly sync: (request: SyncRequest) => Effect.Effect<void, WorkspaceRefused>;
  /** The snapshot `ref` was last released into, as the module says; null where none is recorded. */
  readonly released: (ref: WorkspaceRef) => Effect.Effect<Released | null, WorkspaceRefused>;
  /** Records the snapshot `ref` is released into, or none, as the module says; refused where there is no such workspace. */
  readonly setReleased: (ref: WorkspaceRef, released: Released | null) => Effect.Effect<void, WorkspaceRefused>;
}

/** How long a file of a workspace may take to read or write: the workspace's storage may not answer. */
const FILE_TIMEOUT = "30 seconds";

/** `path`, of a workspace's files, as the names under them, or null where it is not one of `workspace/` or `home/`. */
function namesOf(path: string): string[] | null {
  const names = path.split("/");
  return names.length >= 2 && (names[0] === "workspace" || names[0] === "home") ? names : null;
}

/** `io` done, a failure of it refused, as `status` says by what it said, and one that does not end in time too. */
const confined = <A>(path: string, io: () => Promise<A>): Effect.Effect<A, WorkspaceRefused> =>
  Effect.tryPromise({
    try: io,
    catch: (error) => {
      const said = error instanceof Error ? error.message : String(error);
      return refused(/there is none/u.test(said) ? 404 : /more than/u.test(said) ? 413 : /names no file/u.test(said) ? 400 : 403, `${path}: ${said}`);
    },
  }).pipe(Effect.timeoutOrElse({ duration: FILE_TIMEOUT, orElse: () => Effect.fail(refused(504, `${path} was not reached within ${FILE_TIMEOUT}`)) }));

/** What makeWorkspaces is given: the node's commands (mounts are made in its mount namespace), its runsc, JuiceFS's command, none where the agent is given no workspace storage, and, but for tests, where its roots are and whose files are. */
export interface WorkspacesOptions {
  readonly host: HostCommands;
  /** Whether workspace storage is mounted at the workspaces' root: as its file system says, but for tests. */
  readonly mounted?: (root: string) => Promise<boolean>;
  /** Whether what is mounted at a point is reachable: as reaching says of a look in the node's mount namespace (lookOnNode), but for tests. */
  readonly reaches?: (point: string) => Promise<boolean>;
  readonly runsc: Pick<Runsc, "container" | "containers" | "processes" | "exec">;
  readonly juicefs: Juicefs | null;
  readonly workspacesRoot?: string;
  readonly attachRoot?: string;
  readonly agentUid?: number;
  /** How long a fork's quota is left to settle after each repair before it is checked (QUOTA_SETTLE_MS), but for tests. */
  readonly quotaSettleMs?: number;
  /** Whether the node's mount holds the quota of a directory under its root: as holdsQuota says of statfs, but for tests. */
  readonly holds?: (directory: string, root: string) => Promise<boolean>;
  /** How long an attach waits for a workspace's quota to be held by the node's mount (QUOTA_HELD_WITHIN_MS), but for tests. */
  readonly quotaHeldWithinMs?: number;
  /** The agent's own mount namespace's mounts (OwnMounts), but for tests. */
  readonly own?: OwnMounts;
  /** How long a directory whose pod is not there is left after it last changed (ORPHAN_GRACE_MS), but for tests. */
  readonly orphanGraceMs?: number;
}

/** What a sweep did: the pods, `<namespace>/<pod>`, it detached, and those it left for their sandbox's end. */
export interface Swept {
  readonly detached: readonly string[];
  readonly waiting: readonly string[];
}

/**
 * How long a pod's directory whose pod the cluster does not have on the node is left after it
 * last changed, before a sweep detaches it: a restore's is made, and attached into, before its
 * pod is (attach, `made`), which the gateway makes at once after, and detaches itself where it
 * cannot; and the pod a kubelet makes one for may be heard of a moment after.
 */
const ORPHAN_GRACE_MS = 2 * 60_000;

/**
 * How long a fork's quota is left to settle after it is repaired before it is checked, and how
 * many times it is repaired at most: the mount that cloned it writes what it cloned to its
 * directories' stats in the metadata engine a moment later, so the usage it was repaired to may
 * be off what they then sum to. One still off after the last is repaired by the daily quota
 * check (cli/src/manifests/workspace-storage.ts).
 */
const QUOTA_SETTLE_MS = 2_000;
const QUOTA_REPAIRS = 5;

/**
 * How long an attach waits for a workspace's quota to be held by the node's mount once it is
 * set: two of JuiceFS's heartbeats, at which its client loads quotas anew (12 s each, and as
 * much as a twentieth more, pkg/meta base.go refresh, pkg/utils JitterIt), under the 30 s the
 * gateway gives an attach. And how often the mount is looked at meanwhile.
 */
const QUOTA_HELD_WITHIN_MS = 2 * 12_000 * 1.05;
const QUOTA_LOOK_MS = 500;

/** The agent's workspaces, as the module says. */
/** What an attach writes into a pod's directory, beside its workspace's mount point (`ws`), and a free pod's binding; and what its bayma marks there (`served`). */
const ATTACHED_FILES: readonly string[] = ["token", "env", BINDING_FILE, "served"];

/**
 * The agent's own mount namespace: the mount points at or under a directory as it has them, and
 * what unmounts one there, lazily. An agent started after a workspace was attached copied the
 * node's mounts as its container was made, the attach's among them, which an unmount on the
 * node does not take where the node's mounts do not propagate to it.
 */
export interface OwnMounts {
  readonly under: (directory: string) => Promise<string[]>;
  readonly unmount: (point: string) => Promise<void>;
}

const OWN_MOUNTS: OwnMounts = {
  under: (directory) =>
    readFile("/proc/self/mountinfo", "utf8").then(
      (table) => mountPoints(table).filter((point) => point === directory || point.startsWith(`${directory}/`)),
      () => [],
    ),
  unmount: (point) =>
    new Promise((resolve, reject) => execFile("umount", ["-l", point], (error, _stdout, stderr) => (error ? reject(new Error(stderr.trim() || error.message)) : resolve()))),
};

/**
 * Removes a pod's directory, `directory`, nothing mounted under it: what an attach wrote into
 * it, each a file of its own, then its workspace's mount point and itself, each by rmdir, which
 * removes only what is empty, never anything beneath; what is left, as what no attach wrote
 * keeps one not empty, by name.
 */
async function removeAttached(directory: string): Promise<string[]> {
  for (const name of ATTACHED_FILES) {
    const path = join(directory, name);
    if ((await lstat(path).catch(() => null))?.isFile()) await unlink(path);
  }
  const left: string[] = [];
  for (const path of [join(directory, "ws"), directory]) {
    await rmdir(path).catch((error: NodeJS.ErrnoException) => {
      if (error.code !== "ENOENT") left.push(path);
    });
  }
  return left.length === 0 ? [] : (await readdir(directory).catch(() => [] as string[])).map((name) => join(directory, name));
}

/** Linux's magic number of FUSE's file systems (statfs(2)), JuiceFS's mount's. */
const FUSE_SUPER_MAGIC = 0x65735546;

/**
 * Whether the mount at `root` holds the quota of `directory`, under it, as `read` (statfs) says:
 * JuiceFS's client answers a statfs of a directory under a quota it has loaded with the quota,
 * its size what the quota allows (pkg/meta base.go StatFS), and with its file system's
 * otherwise, as of the root. Neither read, it does not.
 */
export const holdsQuota = (read: (path: string) => Promise<StatsFs> = statfs) => (directory: string, root: string): Promise<boolean> =>
  Promise.all([read(directory), read(root)]).then(([under, whole]) => under.blocks * under.bsize !== whole.blocks * whole.bsize, () => false);

/** Whether `root` is FUSE's, as its file system says. */
const fuseAt = (root: string): Promise<boolean> => statfs(root).then(({ type }) => type === FUSE_SUPER_MAGIC, () => false);

/**
 * Whether what is mounted at a point is reachable, as `read` looks at it: not where the
 * mount it was bound from ended under it, as FUSE answers ENOTCONN, or EIO, of one whose
 * process ended; one that does not answer within `timeoutMs` is not taken for lost.
 */
export const reaching = (read: (point: string) => Promise<unknown>, timeoutMs = 5_000) => (point: string): Promise<boolean> =>
  new Promise((resolve) => {
    const timer = setTimeout(() => resolve(true), timeoutMs);
    read(point).then(() => true, (error: NodeJS.ErrnoException) => error.code !== "ENOTCONN" && error.code !== "EIO").then((reached) => {
      clearTimeout(timer);
      resolve(reached);
    });
  });

/**
 * A look at the mount point `point` in the node's mount namespace, by `host`, where the mounts
 * of what is attached into pods are, which the agent's own does not see: as a read of it would
 * fail, its failure's code as `ls` says it (ENOTCONN, EIO, or none it names).
 */
export const lookOnNode = (host: HostCommands) => (point: string): Promise<void> =>
  Effect.runPromise(Effect.orDie(host.run(["ls", "-d", "--", point]))).then(({ exitCode, stderr }) => {
    if (exitCode === 0) return;
    const code = /not connected/iu.test(stderr) ? "ENOTCONN" : /input\/output error/iu.test(stderr) ? "EIO" : "EUNKNOWN";
    throw Object.assign(new Error(stderr.trim()), { code });
  });

export function makeWorkspaces(
  { host, runsc, juicefs, mounted = fuseAt, reaches = reaching(lookOnNode(host)), workspacesRoot = WORKSPACES_ROOT, attachRoot = ATTACH_ROOT, agentUid = AGENT_UID, quotaSettleMs = QUOTA_SETTLE_MS, holds = holdsQuota(), quotaHeldWithinMs = QUOTA_HELD_WITHIN_MS, own = OWN_MOUNTS, orphanGraceMs = ORPHAN_GRACE_MS }: WorkspacesOptions,
): Workspaces {
  /** Where `ref`'s files are on the node, its names checked. */
  const filesOf = ({ environment, workspace }: WorkspaceRef): Effect.Effect<string, WorkspaceRefused> =>
    !ENVIRONMENT.test(environment) || !isValidVolumeId(workspace)
      ? Effect.fail(refused(400, `no workspace is named ${environment}/${workspace}`))
      : Effect.succeed(join(workspacesRoot, environment, workspace, "files"));

  /** Where the directory of `pod` of `namespace` is on the node, their names checked. */
  const podDirectory = (namespace: string, pod: string): Effect.Effect<string, WorkspaceRefused> =>
    !ENVIRONMENT.test(namespace) ? Effect.fail(refused(400, `${namespace} is no namespace's name`))
    : POD.test(pod) ? Effect.succeed(join(attachRoot, namespace, pod))
    : Effect.fail(refused(400, `${pod} is no pod's name`));

  /** The node's mount points at or under `directory`, deepest first. */
  const mountsUnder = (directory: string) =>
    onNode(host, ["cat", "/proc/self/mountinfo"]).pipe(
      Effect.map(({ stdout }) => mountPoints(stdout).filter((point) => point === directory || point.startsWith(`${directory}/`)).toSorted((a, b) => b.length - a.length)),
    );

  /** The container `containerId`, where it is the bayma of the pod `namespace`/`pod` of a pool on this node, as the module says; refused otherwise. */
  const baymaOf = ({ namespace, pod, containerId }: SyncRequest) =>
    Effect.gen(function*() {
      const directory = yield* podDirectory(namespace, pod);
      const container = yield* runsc.container(containerId).pipe(Effect.mapError(() => refused(404, `no container ${containerId} runs on this node`)));
      const { annotations } = container;
      const of = `${annotations["io.kubernetes.cri.sandbox-namespace"] ?? "?"}/${annotations["io.kubernetes.cri.sandbox-name"] ?? "?"}`;
      if (of !== `${namespace}/${pod}` || annotations["io.kubernetes.cri.container-name"] !== BAYMA_CONTAINER) {
        return yield* refused(403, `container ${containerId} is ${of}'s ${annotations["io.kubernetes.cri.container-name"] ?? "?"}, not the bayma of ${namespace}/${pod}`);
      }
      const pooled = yield* Effect.promise(() => lstat(directory).then((stats) => stats.isDirectory(), () => false));
      if (!pooled) return yield* refused(403, `${namespace}/${pod} is no pod of a pool on this node: kubelet has made it no directory`);
      return container;
    });

  /** JuiceFS's command, refused where the agent is given none. */
  const juicefsOr = juicefs ? Effect.succeed(juicefs) : Effect.fail(refused(501, "this node agent makes no workspace, as workspace storage is not given it"));
  /** `ref`'s directory, as its quota names it, from the file system's root. */
  const quotaPath = ({ environment, workspace }: WorkspaceRef): string => `/${environment}/${workspace}`;
  /** Whether `path` is there, on the node's mount. */
  const there = (path: string) => Effect.promise(() => lstat(path).then(() => true, () => false));
  /** Workspace storage mounted at the workspaces' root, as the module says; refused, saying `what` cannot be done, where it is not. */
  const storage = (what: string) =>
    Effect.promise(() => mounted(workspacesRoot)).pipe(
      Effect.filterOrFail((is) => is, () => refused(503, `${what}: workspace storage is not mounted at ${workspacesRoot} on this node now`)),
      Effect.asVoid,
    );
  /**
   * `ref`'s quota held by the node's mount, as the module says, `ref`'s files at `files`,
   * waited for up to `withinMs`; refused, saying `what` cannot be done, where it is not. Of a
   * workspace not there, nothing is looked at: what is done of it says it is not.
   */
  const quotaHeld = (ref: WorkspaceRef, files: string, what: string, withinMs: number) =>
    Effect.gen(function*() {
      const directory = join(files, "..");
      if (!(yield* there(directory))) return;
      const looked = Effect.promise(() => holds(directory, workspacesRoot));
      const held = withinMs === 0 ? yield* looked : yield* looked.pipe(
        Effect.repeat({ schedule: Schedule.spaced(QUOTA_LOOK_MS), until: (is) => is }),
        Effect.timeoutOrElse({ duration: withinMs, orElse: () => Effect.succeed(false) }),
      );
      if (!held) return yield* refused(503, `${what}: the node's mount of workspace storage does not hold ${ref.environment}/${ref.workspace}'s quota yet`);
    });
  /** `work` done on the node's mount, there, a failure of it refused as workspace storage's not answering. */
  const onMount = <A>(what: string, work: () => Promise<A>) =>
    storage(what).pipe(Effect.andThen(Effect.tryPromise({ try: work, catch: (error) => refused(503, `${what}: ${error instanceof Error ? error.message : String(error)}`) })));

/** Detaches what is attached into the directory of `pod` of `namespace`, as the module says: what of the directory it left, as no attach wrote it. */
const detach = (namespace: string, pod: string, containerIds: readonly string[]): Effect.Effect<void, WorkspaceRefused | HostCommandError> =>
    Effect.gen(function*() {
      const directory = yield* podDirectory(namespace, pod);
      const running = yield* Effect.forEach(containerIds, (id) => runsc.container(id).pipe(Effect.as(true), Effect.orElseSucceed(() => false)));
      if (running.some(Boolean)) return yield* refused(409, `${pod}'s sandbox still runs: what is attached into it is detached once it can no longer act`);
      // `-l`, not `--lazy`, which the busybox of k3s's own binaries, first on the node's PATH, does not take.
      for (const point of yield* mountsUnder(directory)) yield* onNode(host, ["umount", "-l", point]);
      // And as the agent's own namespace holds them, as one started after the attach does, deepest first.
      for (const point of (yield* Effect.promise(() => own.under(directory))).toSorted((a, b) => b.length - a.length)) {
        yield* Effect.tryPromise(() => own.unmount(point)).pipe(Effect.catch((error) => Effect.logWarning(`${point} was not unmounted as the agent sees it: ${error.message}`)));
      }
      // Nothing removed where anything is mounted still, on the node or as the agent sees it, which a removal would walk into.
      const still = [...(yield* mountsUnder(directory)), ...(yield* Effect.promise(() => own.under(directory)))];
      if (still.length > 0) return yield* refused(409, `${pod}'s directory holds ${[...new Set(still)].join(", ")} still, unmounted as it was: nothing of it is removed`);
      const left = yield* Effect.promise(() => removeAttached(directory));
      if (left.length > 0) yield* Effect.logWarning(`${pod}'s directory is left on the node, as it holds what no attach wrote: ${left.join(", ")}`);
    });

  /** The directories of pods there are under the attach root, by namespace and pod, as their names are each's. */
  const podDirectories = async (): Promise<{ readonly namespace: string; readonly pod: string; readonly path: string }[]> => {
    const found: { readonly namespace: string; readonly pod: string; readonly path: string }[] = [];
    for (const namespace of (await readdir(attachRoot).catch(() => [] as string[])).filter((name) => ENVIRONMENT.test(name))) {
      for (const entry of await readdir(join(attachRoot, namespace), { withFileTypes: true }).catch(() => [])) {
        if (entry.isDirectory() && POD.test(entry.name)) found.push({ namespace, pod: entry.name, path: join(attachRoot, namespace, entry.name) });
      }
    }
    return found;
  };
  /** When the directory `path`, or what an attach wrote in it, last changed: not its workspace's mount point, whose mount's own root another pod bound to the workspace changes. */
  const lastChanged = async (path: string): Promise<number> =>
    Math.max(...await Promise.all([path, ...ATTACHED_FILES.map((name) => join(path, name))].map((each) => lstat(each).then(({ mtimeMs }) => mtimeMs, () => 0))));
  /** The directories a sweep detached, but for what no attach wrote, left as they are: each said once, not detached again. */
  const leftAlone = new Set<string>();
  const sweep = (present: ReadonlyMap<string, ReadonlySet<string>>): Effect.Effect<Swept, HostCommandError> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const detached: string[] = [];
      const waiting: string[] = [];
      for (const { namespace, pod, path } of yield* Effect.promise(podDirectories)) {
        const name = `${namespace}/${pod}`;
        const there = present.get(namespace);
        if (there === undefined || there.has(pod) || leftAlone.has(path)) continue;
        // One changed lately may be a restore's, whose pod is about to be made.
        if (now - (yield* Effect.promise(() => lastChanged(path))) < orphanGraceMs) continue;
        // Its sandbox ended first, as kubelet ends one of a pod deleted with no grace after it has gone from the
        // cluster: no process of runsc's runs for any container runsc keeps of the pod, as their annotations name it.
        const ending = yield* runsc.containers.pipe(
          Effect.map((listed) => listed.filter(({ annotations }) => annotations["io.kubernetes.cri.sandbox-namespace"] === namespace && annotations["io.kubernetes.cri.sandbox-name"] === pod)),
          Effect.flatMap((kept) => (kept.length === 0 ? Effect.succeed(false) : Effect.map(runsc.processes(kept), (processes) => processes.length > 0))),
          // Where runsc cannot say, it is taken to run, and looked at again.
          Effect.catch((error) => Effect.as(Effect.logWarning(`what runs of ${name}, whose pod is gone from this node, could not be told: ${error.message}`), true)),
        );
        if (ending) {
          waiting.push(name);
          continue;
        }
        const outcome = yield* detach(namespace, pod, []).pipe(
          Effect.as("detached" as const),
          // Its sandbox running still, or a mount under it that did not go: looked at again by the next sweep.
          Effect.catchTag("WorkspaceRefused", (refusal) =>
            refusal.status === 409 ? Effect.succeed("waiting" as const) : Effect.as(Effect.logWarning(`what was attached into ${name}, whose pod is gone from this node, was not detached: ${refusal.message}`), "failed" as const)
          ),
        );
        if (outcome === "detached") {
          detached.push(name);
          if (yield* Effect.promise(() => lstat(path).then(() => true, () => false))) leftAlone.add(path);
        }
        if (outcome === "waiting") waiting.push(name);
      }
      return { detached, waiting };
    });

  return {
    create: (ref, quotaBytes) =>
      Effect.gen(function*() {
        const files = yield* filesOf(ref);
        if (!Number.isSafeInteger(quotaBytes) || quotaBytes <= 0) return yield* refused(400, `${quotaBytes} is no quota`);
        const command = yield* juicefsOr;
        yield* onMount(`${ref.environment}/${ref.workspace} could not be made`, async () => {
          for (const directory of [join(files, "workspace"), join(files, "home")]) await mkdir(directory, { recursive: true });
          for (const directory of [files, join(files, "workspace"), join(files, "home")]) await chown(directory, agentUid, agentUid);
        });
        yield* command.setQuota(quotaPath(ref), quotaBytes);
      }),

    clone: (from, to, quotaBytes) =>
      Effect.gen(function*() {
        const source = yield* filesOf(from);
        const destination = yield* filesOf(to);
        if (!Number.isSafeInteger(quotaBytes) || quotaBytes <= 0) return yield* refused(400, `${quotaBytes} is no quota`);
        const command = yield* juicefsOr;
        if (!(yield* there(source))) return yield* refused(404, `there is no workspace ${from.environment}/${from.workspace}`);
        if (yield* there(join(destination, ".."))) return yield* refused(409, `there is a workspace ${to.environment}/${to.workspace} already`);
        yield* onMount(`${to.environment}/${to.workspace} could not be made`, () => mkdir(join(destination, ".."), { recursive: true }));
        yield* command.clone(source, destination);
        yield* command.setQuota(quotaPath(to), quotaBytes);
        // The usage the quota is set with, counted as the clone's tree was attached, may be off what the tree holds: counted
        // anew, until a check after it has settled finds it holds (QUOTA_SETTLE_MS).
        for (let repairs = 1; ; repairs += 1) {
          yield* command.repairQuota(quotaPath(to));
          yield* Effect.sleep(quotaSettleMs);
          if (yield* command.quotaHolds(quotaPath(to))) break;
          if (repairs === QUOTA_REPAIRS) {
            yield* Effect.logWarning(`${to.environment}/${to.workspace}'s quota is still off what it holds after ${QUOTA_REPAIRS} repairs; the daily quota check repairs it`);
            break;
          }
        }
      }),

    remove: (ref) =>
      Effect.gen(function*() {
        const files = yield* filesOf(ref);
        const command = yield* juicefsOr;
        if (!(yield* there(join(files, "..")))) return;
        yield* command.deleteQuota(quotaPath(ref)).pipe(Effect.catch((error) => Effect.logWarning(`${ref.environment}/${ref.workspace}'s quota was not deleted: ${error.message}`)));
        yield* onMount(`${ref.environment}/${ref.workspace} could not be deleted`, () => rm(join(files, ".."), { recursive: true, force: true }));
      }),

    list: (environment) =>
      Effect.gen(function*() {
        if (!ENVIRONMENT.test(environment)) return yield* refused(400, `${environment} is no environment's name`);
        const names = yield* onMount(`the workspaces of ${environment} could not be listed`, () => readdir(join(workspacesRoot, environment)).catch((error: NodeJS.ErrnoException) => (error.code === "ENOENT" ? [] : Promise.reject(error))));
        return (names as string[]).filter(isValidVolumeId).sort();
      }),

    environments: Effect.suspend(() =>
      onMount("the environments of workspaces could not be listed", () => readdir(workspacesRoot).catch((error: NodeJS.ErrnoException) => (error.code === "ENOENT" ? [] : Promise.reject(error)))).pipe(
        Effect.map((names) => (names as string[]).filter((name) => ENVIRONMENT.test(name)).sort()),
      )
    ),

    quota: (ref) =>
      Effect.gen(function*() {
        const files = yield* filesOf(ref);
        const command = yield* juicefsOr;
        // Of one not there, none.
        if (!(yield* there(join(files, "..")))) return null;
        return yield* command.quota(quotaPath(ref));
      }),

    attach: (ref, pod, token, env, { made = false } = {}) =>
      Effect.gen(function*() {
        const files = yield* filesOf(ref);
        const unnamed = Object.keys(env).find((name) => !VARIABLE.test(name));
        if (unnamed !== undefined) return yield* refused(400, `${unnamed} is no variable's name`);
        // The pod's namespace is its workspace's environment's, as the gateway attaches it.
        const directory = yield* podDirectory(ref.environment, pod);
        if (made) {
          // A restore's pod's alone, made now, never one there already, whatever it is: no pod's that runs is ever attached into so.
          if (!RESTORE_POD.test(pod)) return yield* refused(400, `${pod} is no restore's pod: no directory is made for it`);
          yield* storage(`${ref.environment}/${ref.workspace} could not be attached`);
          const making = yield* Effect.promise(() => mkdir(join(attachRoot, ref.environment), { recursive: true }).then(() => mkdir(directory)).then(() => null, (error: NodeJS.ErrnoException) => error.code ?? "failed"));
          if (making === "EEXIST") return yield* refused(409, `${pod} has a directory on this node already: a restore's pod's is made for it alone`);
          if (making !== null) return yield* refused(500, `${pod}'s directory could not be made: ${making}`);
        }
        const there = yield* Effect.promise(() => lstat(directory).then((stats) => stats.isDirectory(), () => false));
        if (!there) return yield* refused(404, `no pod of a pool on this node is named ${pod}: kubelet has made it no directory`);
        const held = yield* mountsUnder(directory);
        if (held.length > 0) return yield* refused(409, `${pod}'s directory holds ${held.join(", ")}: a pod serves one binding, and nothing is mounted over another`);
        yield* storage(`${ref.environment}/${ref.workspace} could not be attached`);
        const found = yield* host.run(["test", "-d", files]);
        if (found.exitCode !== 0) return yield* refused(404, `there is no workspace ${ref.environment}/${ref.workspace}`);
        yield* quotaHeld(ref, files, `${ref.environment}/${ref.workspace} could not be attached`, quotaHeldWithinMs);
        const ws = join(directory, "ws");
        yield* Effect.promise(async () => {
          await mkdir(ws);
          await chown(directory, agentUid, agentUid);
          await chown(ws, agentUid, agentUid);
        });
        yield* onNode(host, ["mount", "--bind", files, ws]);
        yield* Effect.promise(async () => {
          // Beside the workspace, not in it: its sandbox reads it, and reaches no other's.
          await writeFile(join(directory, "env"), envFile(env), { mode: 0o400 });
          await chown(join(directory, "env"), agentUid, agentUid);
          // Written last, once its workspace is there, as its bayma starts, and a restore's sandbox is restored, once it is.
          await writeFile(join(directory, "token"), token, { mode: 0o400 });
          await chown(join(directory, "token"), agentUid, agentUid);
        });
      }),

    bindFree: (namespace, pod, { uid, gid }, binding) =>
      Effect.gen(function*() {
        if (![uid, gid].every((id) => Number.isSafeInteger(id) && id > 0)) return yield* refused(400, `${uid}:${gid} is no user a harness pool's pod runs as: none runs as root`);
        const directory = yield* podDirectory(namespace, pod);
        const there = yield* Effect.promise(() => lstat(directory).then((stats) => stats.isDirectory(), () => false));
        if (!there) return yield* refused(404, `no pod of a pool on this node is named ${pod}: kubelet has made it no directory`);
        const held = yield* mountsUnder(directory);
        if (held.length > 0) return yield* refused(409, `${pod}'s directory holds ${held.join(", ")}: it is no free pod of a harness pool`);
        const file = join(directory, BINDING_FILE);
        const written = yield* Effect.tryPromise({ catch: (error) => refused(500, `${pod}'s binding could not be written: ${error instanceof Error ? error.message : String(error)}`), try: async () => {
          const staged = `${file}.${randomBytes(8).toString("hex")}`;
          const handle = await open(staged, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o400);
          try {
            await handle.writeFile(binding);
            await handle.chown(uid, gid);
            await handle.sync();
          } finally {
            await handle.close();
          }
          // Linked to its name, which fails where a binding has it: a pod serves one.
          const linked = await link(staged, file).then(() => null, (error: NodeJS.ErrnoException) => error.code ?? "failed");
          await unlink(staged);
          if (linked !== null) return linked;
          await chmod(directory, 0o711);
          const parent = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
          try {
            await parent.sync();
          } finally {
            await parent.close();
          }
          return null;
        } });
        if (written === "EEXIST") return yield* refused(409, `${pod} has its binding already: a pod serves one`);
        if (written !== null) return yield* refused(500, `${pod}'s binding could not be written: ${written}`);
      }),

    detach,

    sweep,

    attachedNamespaces: Effect.promise(() => readdir(attachRoot).then((names) => names.filter((name) => ENVIRONMENT.test(name)), () => [])),

    read: (ref, path, maxBytes) =>
      Effect.gen(function*() {
        const names = namesOf(path);
        if (!names) return yield* refused(400, `${path} is not a path of a workspace's files`);
        const root = yield* filesOf(ref);
        return yield* confined(path, () => readUnder({ root, names }, maxBytes));
      }),

    write: (ref, path, bytes) =>
      Effect.gen(function*() {
        const names = namesOf(path);
        if (!names) return yield* refused(400, `${path} is not a path of a workspace's files`);
        const root = yield* filesOf(ref);
        yield* quotaHeld(ref, root, `${path} could not be written`, 0);
        yield* confined(path, () => writeUnder({ root, names }, bytes, { uid: agentUid, gid: agentUid }));
      }),

    attachments: Effect.gen(function*() {
      const attached = (yield* mountsUnder(attachRoot)).filter((point) => point.endsWith("/ws") && point.split("/").length === attachRoot.split("/").length + 3);
      return yield* Effect.forEach(attached, (point) =>
        Effect.promise(() => reaches(point)).pipe(Effect.map((reachable) => ({ namespace: point.split("/").at(-3) ?? "", pod: point.split("/").at(-2) ?? "", reachable }))), { concurrency: 8 });
    }),

    stop: (request) =>
      Effect.gen(function*() {
        const { namespace, pod, containerId, hook } = request;
        const at = hook.indexOf("sandbox-hook");
        if (at < 0 || hook[at + 1] !== "stop") return yield* refused(400, `${hook.join(" ")} is not bayma's sandbox-hook stop`);
        const container = yield* baymaOf(request);
        yield* runsc.exec(container.release, containerId, hook, STOP_TIMEOUT_MS).pipe(Effect.mapError((error) => refused(502, `bayma of ${namespace}/${pod} did not go on with its stop: ${error.message}`)));
      }),

    sync: (request) =>
      Effect.gen(function*() {
        const { namespace, pod, containerId } = request;
        const container = yield* baymaOf(request);
        yield* runsc.exec(container.release, containerId, ["sync"], SYNC_TIMEOUT_MS).pipe(
          Effect.mapError((error) => refused(502, `the sandbox of ${namespace}/${pod} did not write its files through: ${error.message}`)),
        );
      }),

    released: (ref) =>
      Effect.gen(function*() {
        const directory = join(yield* filesOf(ref), "..");
        const text = yield* onMount(`${ref.environment}/${ref.workspace}'s release could not be read`, () =>
          readFile(join(directory, RELEASED_FILE), { encoding: "utf8", flag: constants.O_RDONLY | constants.O_NOFOLLOW }).catch((error: NodeJS.ErrnoException) =>
            error.code === "ENOENT" ? null : Promise.reject(error)
          ));
        if (text === null) return null;
        return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Released))(text).pipe(
          Effect.mapError(() => refused(500, `${ref.environment}/${ref.workspace}'s record of its release is not one`)),
        );
      }),

    setReleased: (ref, released) =>
      Effect.gen(function*() {
        const directory = join(yield* filesOf(ref), "..");
        if (!(yield* there(directory))) return yield* refused(404, `there is no workspace ${ref.environment}/${ref.workspace}`);
        const file = join(directory, RELEASED_FILE);
        yield* onMount(`${ref.environment}/${ref.workspace}'s release could not be recorded`, async () => {
          if (released === null) {
            await unlink(file).catch((error: NodeJS.ErrnoException) => (error.code === "ENOENT" ? undefined : Promise.reject(error)));
          } else {
            const staged = `${file}.${randomBytes(8).toString("hex")}`;
            const handle = await open(staged, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
            try {
              await handle.writeFile(JSON.stringify(released));
              await handle.sync();
            } finally {
              await handle.close();
            }
            await rename(staged, file);
          }
          const parent = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY);
          try {
            await parent.sync();
          } finally {
            await parent.close();
          }
        });
      }),
  };
}
