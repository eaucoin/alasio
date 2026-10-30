/**
 * Whole-sandbox snapshots of the sandbox gateway's session workspaces, each of a bound pool
 * pod: a workspace, of an environment's sessions' namespace, released is saved whole by the
 * node agent on its pod's node (../sandbox/node-agents.ts), bayma told first (`bayma sandbox-hook`), which ends it; and the
 * snapshot it was released into is recorded with the workspace (its node agents' record of it,
 * ../node-agent/workspaces.ts Released), while the gateway holds its binding's Lease, as every
 * read and write of the record is, for its next bind to restore it.
 *
 * A save comes to an outcome, each counted (alasio.sandbox.releases):
 * snapshotted, into the snapshot it took; kept, as its pod runs no bayma under gVisor, which
 * the gateway then releases unsaved, its pod deleted, as a pool's pods are for it to reuse;
 * lost, its sandbox ended by a save that failed, its REPLs with it; unsaveable, as the agent
 * says no save of it will be taken, whose bayma is told to go on with the stop it would hold
 * for one (its node agent's POST /stop), so its pod ends at once; or failed, its sandbox
 * running on, which keeps the binding. The record names the pod a release's save took its
 * snapshot of, so a release of that pod asked again, as one whose pod's end or detach failed
 * after its save, is released into that snapshot, never saved anew (savedAlready): its sandbox
 * ended with that save, and a save of none would leave the workspace released into none.
 *
 * A workspace released into a snapshot is restored from it as it is next bound, as alasio's
 * sessions were, its sandbox as it was, by a pod the gateway makes, its binding's, of the
 * pool's template the snapshot was taken on: its current one, or the one before it, which
 * `alasio up` carries for one change of it (GatewaySnapshots templates), and nothing older; on
 * a node that has the gVisor release that took it, still kept, and the CPU features it was
 * saved with, those that hold its images first; its workspace attached into its directory there,
 * made for it, before its pod is made, so its sandbox, restored as its bayma starts, finds every
 * file it held open, as a restore reopens each; left paused until its node's agent sees its egress
 * confined there, then resumed, and ready. One whose restore runsc refuses falls back at once;
 * one whose pod cannot start otherwise, as a pull or a node's hiccup fails it, is deferred, as
 * the others that may pass. Only the snapshot the record says it was
 * released into restores, or one its node's agent compacted from it since, which is the same:
 * one it has moved on from since is stale. A restore that cannot be done now, as what it needs
 * fails for a while, is deferred, its snapshot kept, the bind refused to be tried again, for
 * DEFER_FOR_MS from its first; one that never can, or one deferred for that long, falls back,
 * its snapshot and its record forgotten, the workspace bound to a free pod of its pool, from
 * bayma's checkpoints, why logged and counted (alasio.sandbox.restores). A restore refused
 * before its sandbox resumed is abandoned on its node first, as alasio's were. Main's sessions
 * alone are snapshotted, as alasio's were.
 *
 * A bound pod ended by other than the gateway, as a drain, an eviction or a deletion through
 * the API server ends one, is saved while its bayma holds its stop for it (saveStopping), as
 * alasio's sessions were: RELEASES_PER_NODE of a node's at once, the others waiting their turn;
 * none where its bayma holds no stop, where its turn comes once it holds it no more, or where
 * its save fails, the workspace then coming back from bayma's checkpoints. Counted by outcome
 * (alasio.sandbox.stop_releases).
 *
 * A bound pod of main's whose execs run is saved live every `liveEverySeconds` while they run,
 * from that long after they began, its sandbox running on (keepLive), as alasio's sessions
 * were: the snapshot it took holds the workspace while bayma's state generation is the one it
 * caught, and is spent once bayma moves on from it. A binding that ends unsaved, as its pod went
 * while it was bound, or its bayma ended, is released into that snapshot (liveSnapshotOf), whose
 * restore its resume refuses where bayma had moved on from it. One whose live save says it can be
 * saved no more is ended now (unsaveable), to come back from bayma's checkpoints, as its next
 * save would fail as this one did.
 *
 * A workspace deleted has its snapshots forgotten with it, in the bucket and every node's cache
 * (forget); a fork of one released into a snapshot is given a copy of it (copy), which it is
 * recorded released into, so it restores as its source would; and what of main's snapshots no
 * workspace has a use for is collected (collect), as alasio's were: those of workspaces gone,
 * what no snapshot of a workspace needs, and what a node caches that none needs. The gVisor
 * releases the snapshots of every environment's workspaces were taken under are read whole
 * (releasesNamed), as `alasio up` keeps those installed.
 */
import { randomBytes } from "node:crypto";

import type { V1Container, V1Deployment, V1LabelSelector, V1NetworkPolicy, V1Node, V1Pod, V1PodTemplateSpec, V1Toleration } from "@kubernetes/client-node";
import { Clock, Effect, Schedule, Schema, Semaphore } from "effect";

import { BindStepFailed, type MakePod } from "../binder/binder.ts";
import { FREE_LABEL, type Pool } from "../binder/names.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { BAYMA_CONTAINER, BAYMA_STOP_SECONDS, RESTORE_ANNOTATION, RESTORE_PAUSED_ANNOTATION, RESUMED_CONDITION, SANDBOX_STOP_REASON_VARIABLE, SESSION_TEMPLATE_ANNOTATION, STOP_WAIT_ARG } from "../kube/sandbox-shape.ts";
import type { Manifest } from "../node-agent/agent.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import { trace, withLogScope } from "../shared/log.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";
import { type GatewaySnapshots as SnapshotsConfig, ownDirectory, restorePodName } from "./names.ts";

/** Why a session with a snapshot was resumed without it. */
export type Fallback = "release" | "template" | "stale" | "node" | "prepare" | "restore" | "unreadable";

/** Whether bayma's container `bayma` holds a stop it is given for a snapshot: one run with STOP_WAIT_ARG. Pure, for tests. */
export function holdsStop(bayma: V1Container | undefined): boolean {
  return bayma?.args?.includes(STOP_WAIT_ARG) ?? false;
}

/**
 * The command that runs bayma's hook in bayma's container: bayma, as its container runs
 * it after the programs that start it (the arguments after the last of their `--`), told
 * the state directory it serves. Null when the container runs no bayma with one. Pure, for
 * tests.
 */
export function hookOf(bayma: V1Container): string[] | null {
  const command = bayma.command ?? [];
  const program = command.slice(command.lastIndexOf("--") + 1);
  const stateDir = stateDirOf(bayma);
  return program.length > 0 && stateDir ? [...program, "sandbox-hook", "--state-dir", stateDir] : null;
}

/**
 * bayma's hook, as hookOf says it, told to go on with the stop bayma holds (sandbox-hook
 * stop), or null where bayma has none. Pure, for tests.
 */
export function stopHookOf(bayma: V1Container): string[] | null {
  const hook = hookOf(bayma);
  return hook ? [...hook.slice(0, -2), "stop", ...hook.slice(-2)] : null;
}

/** The state directory bayma's container runs bayma with, or null for none. Pure, for tests. */
export function stateDirOf(bayma: V1Container): string | null {
  const args = bayma.args ?? [];
  return args.includes("--state-dir") ? (args[args.indexOf("--state-dir") + 1] ?? null) : null;
}

/**
 * Whether the label selector `selector` selects what has `labels`, as Kubernetes matches
 * one: every label it names, and every expression of it; none selects nothing, an empty one
 * everything.
 */
export function selects(selector: V1LabelSelector | undefined, labels: Readonly<Record<string, string>>): boolean {
  if (!selector) return false;
  const named = Object.entries(selector.matchLabels ?? {}).every(([key, value]) => labels[key] === value);
  return named && (selector.matchExpressions ?? []).every(({ key, operator, values = [] }) => {
    const value = labels[key];
    if (operator === "In") return value !== undefined && values.includes(value);
    if (operator === "NotIn") return value === undefined || !values.includes(value);
    if (operator === "Exists") return value !== undefined;
    if (operator === "DoesNotExist") return value === undefined;
    return false;
  });
}

const releases = meter.createCounter("alasio.sandbox.releases", {
  description:
    "Session sandboxes released, by outcome: snapshotted; kept, as they run under no gVisor, released unsaved; failed, running on; lost, their sandbox ended unsaved, its REPLs with it; unsaveable, as no save of it will be taken; or error, the release failing beside its save",
});

const stopReleases = meter.createCounter("alasio.sandbox.stop_releases", {
  description:
    "Session sandboxes released as their pods were ended by other than the gateway, as a drain, an eviction or a deletion through the API server ends them, by outcome: snapshotted; unsnapshotted, as their snapshot failed; late, as their turn came once their bayma held its stop no more; unheld, as their bayma holds no stop; or error, the release failing beside its save",
});

/** How many bound pods of one node are saved as they end at a time. */
const RELEASES_PER_NODE = 2;

const liveCheckpoints = meter.createCounter("alasio.sandbox.live_checkpoints", {
  description: "Snapshots of running sessions taken live, their sandboxes running on, by outcome: taken, or failed; and spent, as the session moved on from one",
});

const restores = meter.createCounter("alasio.sandbox.restores", {
  description: "Session sandboxes brought up with a snapshot, by outcome: restored, or not, by why",
});
const restoreDuration = meter.createHistogram("alasio.sandbox.restore.duration", {
  description: "How long a session restored from its snapshot took to come back, which the request binding it waits: from its snapshot looked up to its pod ready",
  unit: "s",
});

/** What kubelet says of a container waiting that could not start, which a restore's that cannot be restored is, started again only to fail as it did. */
const START_FAILURES: ReadonlySet<string> = new Set(["CrashLoopBackOff", "RunContainerError", "StartError", "CreateContainerError"]);

/**
 * What a restore's container says of a start that failed as runsc refused the restore itself,
 * the snapshot, as one whose files are not there or do not validate: its OCI runtime's restore
 * failing, its kernel's CompleteRestore, or a container it can no longer restore.
 */
const RESTORE_REFUSED = /OCI runtime restore failed|CompleteRestore|cannot restore container|restore[^;]*validat/iu;

/** How long a restored pod is waited for to run its containers, its images pulled. */
const POD_RUNNING_TIMEOUT_MS = 180_000;
/**
 * How long a workspace's restore is deferred for, from its first that was, while what it needs
 * fails for a while (an agent, the bucket, its pod's start, its network policies), before it
 * is bound without its snapshot.
 */
export const DEFER_FOR_MS = 15 * 60_000;

/**
 * The taints the node lifecycle controller gives a Node that is not Ready, or that it has
 * not heard from: of its readiness, which comes back, not of what the Node is for.
 */
const READINESS_TAINTS: ReadonlySet<string> = new Set(["node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"]);

/** Whether `toleration` tolerates `taint`, as the scheduler has it. */
function tolerates(toleration: V1Toleration, taint: { readonly key: string; readonly value?: string; readonly effect: string }): boolean {
  if (toleration.effect && toleration.effect !== taint.effect) return false;
  if (toleration.operator === "Exists") return !toleration.key || toleration.key === taint.key;
  return toleration.key === taint.key && (toleration.value ?? "") === (taint.value ?? "");
}

/** A restore's pod does not run its containers yet. */
class PodNotRunning extends Schema.TaggedError<PodNotRunning>()("PodNotRunning", {}) {}

/** Why a restore's pod did not come back, whether its sandbox ran meanwhile, and whether its snapshot holds the session no more (as alasio's RestoreFailure). */
class RestoreRefused extends Schema.TaggedError<RestoreRefused>()("RestoreRefused", {
  reason: Schema.String,
  resumed: Schema.Boolean,
  stale: Schema.optional(Schema.Boolean),
  /** Whether no try restores it, as runsc refused to restore its sandbox from its snapshot. */
  failed: Schema.optional(Schema.Boolean),
}) {
  override get message(): string {
    return this.reason;
  }
}

/**
 * What a node agent answers of a checkpoint whose sandbox can be saved no more, its restore
 * from the bucket having failed to read some of its pages (../node-agent/agent.ts
 * SandboxUnsaveable): no checkpoint of it will be taken.
 */
const UNSAVEABLE_STATUS = 410;

/** How many workspaces' snapshots are read at once, as what releases they name is read. */
const READS_AT_ONCE = 8;

/** Whether `error`, a checkpoint's, says its sandbox can be saved no more. */
const unsaveable = (error: NodeAgentError | KubeApiError): boolean => error._tag === "NodeAgentError" && error.status === UNSAVEABLE_STATUS;

/** A snapshot a workspace is released into: its id, and the gVisor release that took it. */
export interface SavedSnapshot {
  readonly snapshot: string;
  readonly release: string;
  /** The pod a release's save took it of, where one did. */
  readonly pod?: string;
}

/** What came of a save of a bound pod's sandbox whole (take). */
export type Taken =
  | { readonly outcome: "snapshotted"; readonly saved: SavedSnapshot }
  | { readonly outcome: "kept" }
  | { readonly outcome: "lost" | "failed" | "unsaveable"; readonly why: string };

/** What a release's save came to: the binding kept, its sandbox running on; or released, into the snapshot it gives, or into none. */
export type Saved = "keep" | { readonly saved: SavedSnapshot | null };

/**
 * How one bind of a workspace restores it, if it does: `make`, the binding's maker, which
 * under the binding's Lease finds whether the workspace restores, and makes its pod where it
 * does, its workspace attached first, or declines; and `ready`, what readies the pod it got, its
 * workspace attached where it is a free pod, and, of a restore, its sandbox let run, the restore
 * done, or fails it, the restore deferred or fallen back, what was attached for it detached.
 */
export interface Restoring {
  readonly make: MakePod;
  /** Readies `pod`: where it is this bind's restore's, its sandbox resumed; where not, its workspace attached, by `attach`. */
  readonly ready: (pod: V1Pod, attach: (pod: V1Pod) => Effect.Effect<void, BindStepFailed>) => Effect.Effect<void, BindStepFailed>;
}

/**
 * How a restore's workspace is attached into the directory of its pod, `pod`, on `node`, made for
 * it, before the pod is (`attach`, of the binding `binding`), its bayma's hook given the variables
 * `env` beside its own; and how what was attached is detached, once the pod has gone, as its
 * restore did not come back (`detach`).
 */
export interface Attaching {
  readonly attach: (node: string, pod: string, binding: string, env: Readonly<Record<string, string>>) => Effect.Effect<void, BindStepFailed>;
  readonly detach: (node: string, pod: string) => Effect.Effect<void>;
}

/** The gateway's snapshots; see makeGatewaySnapshots. */
export interface GatewaySnapshots {
  /**
   * Saves the sandbox of `pod`, bound to `workspace` of `pool`, which is ending though the
   * gateway did not end it, while its bayma holds its stop for it, as the module says, and
   * records what it was released into: the snapshot, or none. Run while the gateway holds the
   * binding's Lease.
   */
  readonly saveStopping: (pool: Pool, workspace: string, pod: V1Pod) => Effect.Effect<void>;
  /**
   * Keeps the snapshot taken live of `pod`, bound to `workspace` of `pool`, true to it, as
   * bayma's /status, `status`, says it at `now`, as the module says: spent once bayma's
   * generation has moved on from it, and one taken every liveEvery while its execs run, in
   * the background, its node saving RELEASES_PER_NODE at a time. Where a live save says it can be
   * saved no more, `unsaveable` is run, which ends its binding.
   */
  readonly keepLive: (pool: Pool, workspace: string, pod: V1Pod, status: BaymaStatus, now: number, unsaveable: Effect.Effect<void>) => Effect.Effect<void>;
  /** Ends `pod`, bound to `workspace` of `pool`, which can be saved no more, as the module says: its bayma told to go on with its stop, released into none, and counted. Run while the gateway holds the binding's Lease. */
  readonly endUnsaveable: (pool: Pool, workspace: string, pod: string) => Effect.Effect<void, KubeApiError>;
  /** The snapshot taken live of `workspace` of `pool`, where its current snapshot is one, as the binding that ends unsaved is released into; none otherwise. */
  readonly liveSnapshotOf: (pool: Pool, workspace: string) => Effect.Effect<SavedSnapshot | null>;
  /** Records the workspace, whose pod ended unsaved, released into the snapshot taken of it live, if any, unless a release's save of that pod took one already. */
  readonly releasedUnsaved: (pool: Pool, workspace: string, pod: string, memoryLimit?: string | null) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** Of the bindings of the sessions' namespace `namespace`, forgets what was kept live of those of workspaces not among `bound`, as they are bound no more. */
  readonly forgetLive: (namespace: string, bound: ReadonlySet<string>) => Effect.Effect<void>;
  /** How a bind of `workspace` of `pool`, of its sessions' internet `mode`, restores it, as the module says, its workspace attached as `attaching` says. */
  readonly restoring: (pool: Pool, workspace: string, mode: "none" | "full", attaching: Attaching) => Restoring;
  /**
   * Saves the sandbox of `pod`, bound to `workspace` of `pool`, for its release, as the module
   * says, and records what it was released into: the binding kept where it runs on, or
   * released. Counted as the release's outcome. Run while the gateway holds the binding's Lease.
   */
  readonly save: (pool: Pool, workspace: string, pod: string) => Effect.Effect<Saved, KubeApiError>;
  /**
   * Forgets the snapshots of `workspace` of `pool`, deleted, in the bucket and in every node's
   * cache that holds them; what cannot be forgotten now is said so, and collected later. Run
   * while the gateway holds the workspace's binding's Lease.
   */
  readonly forget: (pool: Pool, workspace: string) => Effect.Effect<void>;
  /**
   * Makes the snapshot `from` of `pool` was released into, if any, the current snapshot of
   * `to`, its fork, of `into`: the snapshot `to` is then released into, to be recorded so, or
   * none, where `from` was released into none, or holds it no more, or it cannot be copied
   * now, why said. Run while the gateway holds the bindings' Leases of both.
   */
  readonly copy: (pool: Pool, from: string, into: Pool, to: string) => Effect.Effect<SavedSnapshot | null>;
  /**
   * Collects what of main's snapshots no workspace has a use for, whose images hold what its
   * sessions held, secrets among it, as alasio's own were: the snapshots in the bucket of
   * workspaces that are gone, which a deletion that could not forget them left; of each
   * workspace, what its agent says none of its snapshots needs any more; and the images a
   * node caches that none of them needs, but those a restore may be reading.
   */
  readonly collect: Effect.Effect<void>;
  /**
   * The snapshots a workspace may be restored from, by the gVisor release that took each, as
   * a release kept is kept for them (cli/src/kept-releases.ts): of each workspace of every
   * environment workspace storage holds, its current snapshot and the one it was recorded
   * released into, each `<environment>/<workspace>/<snapshot>`; what cannot be read fails it
   * whole, as a release named by what was not read would be unkept.
   */
  readonly releasesNamed: Effect.Effect<ReadonlyMap<string, readonly string[]>, NodeAgentError | KubeApiError>;
}

/** What bayma's /status says: its execs running and queued, since when none has run, if none does, and its state generation, which a snapshot taken of it live holds it at. */
export const BaymaStatus = Schema.Struct({ execsRunning: Schema.Number, execsQueued: Schema.Number, idleSinceMs: Schema.NullOr(Schema.Number), generation: Schema.Number });
export type BaymaStatus = typeof BaymaStatus.Type;

/** What makeGatewaySnapshots is given: the node agents, how main's sessions are snapshotted, none where they are not, and main's sessions' namespace. */
export interface GatewaySnapshotsOptions {
  readonly agents: NodeAgents;
  readonly snapshots: SnapshotsConfig | null;
  readonly mainNamespace: string;
}

/** The gateway's snapshots, as the module says, on its node agents and the KubeClient. */
export const makeGatewaySnapshots = Effect.fnUntraced(function*({ agents, snapshots, mainNamespace }: GatewaySnapshotsOptions): Effect.fn.Return<GatewaySnapshots, never, KubeClient> {
  const kube = yield* KubeClient;
  const ref = (pool: Pool, workspace: string) => ({ environment: pool.namespace, workspace });

  /** The pod `name` of `pool`, as the API server has it now. */
  const podOf = (pool: Pool, name: string) => Effect.map(kube.read("v1", "Pod", pool.namespace, name), (read) => read as V1Pod | null);
  /** The runtime's id of `pod`'s bayma, if it runs one. */
  const baymaId = (pod: V1Pod | null) => pod?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "");

  /**
   * Saves the bound pod's sandbox whole by its node's agent, which ends it: snapshotted, into
   * the snapshot it took; kept, as its pod runs no bayma under gVisor; lost, its sandbox ended
   * by a save that failed; unsaveable, as the agent says no save of it will be taken; or
   * failed, its sandbox running on, why said. Its pool's template is recorded with it, which
   * its restore makes its pod of.
   */
  const take = (pool: Pool, workspace: string, name: string): Effect.Effect<Taken, KubeApiError> =>
    Effect.gen(function*() {
      const pod = yield* podOf(pool, name);
      const containerId = baymaId(pod);
      const bayma = pod?.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma ? hookOf(bayma) : null;
      const node = pod?.spec?.nodeName;
      // Main's sessions alone, as alasio's were.
      if (!snapshots || pool.namespace !== mainNamespace) return { outcome: "kept" } as const;
      if (!node || !containerId || pod?.spec?.runtimeClassName === undefined || !hook) {
        yield* Effect.logWarning(`${pool.namespace}/${workspace} is not snapshotted: its pod runs no bayma under gVisor`);
        return { outcome: "kept" } as const;
      }
      const deployment = (yield* kube.read("apps/v1", "Deployment", pool.namespace, pool.name)) as V1Deployment | null;
      const taken = yield* agents.checkpoint(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, containerId, hook, podTemplate: { ...deployment?.spec?.template } }).pipe(Effect.result);
      if (taken._tag === "Success") {
        const { snapshot, release, bytes, pauseMs, uploadMs } = taken.success;
        yield* Effect.logInfo(`snapshotted ${pool.namespace}/${workspace} as ${snapshot} on ${node}: ${bytes} bytes, paused ${pauseMs} ms, uploaded in ${uploadMs} ms`);
        return { outcome: "snapshotted", saved: { snapshot, release } } as const;
      }
      if (unsaveable(taken.failure)) return { outcome: "unsaveable", why: taken.failure.message } as const;
      const after = yield* podOf(pool, name);
      const ended = after?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.state?.terminated !== undefined;
      return { outcome: ended ? "lost" : "failed", why: taken.failure.message } as const;
    });

  /** Tells the bayma of `name`, which runs on, to go on with the stop it holds, so its pod ends at once rather than wait out a save that will not come. */
  const goOnWithStop = (pool: Pool, workspace: string, name: string): Effect.Effect<void, KubeApiError> =>
    Effect.gen(function*() {
      const pod = yield* podOf(pool, name);
      const bayma = pod?.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma && holdsStop(bayma) ? stopHookOf(bayma) : null;
      const containerId = baymaId(pod);
      if (!pod?.spec?.nodeName || !hook || !containerId) return;
      yield* agents.stop(pod.spec.nodeName, { namespace: pool.namespace, pod: name, containerId, hook }).pipe(
        Effect.catch((error) => Effect.logWarning(`${pool.namespace}/${workspace}'s bayma was not told to go on with its stop, which its pod's end waits out: ${error.message}`)),
      );
    });

  /**
   * The snapshot the pod `name` of `workspace` was saved into by a release of it before, as its
   * record says, or null: a release of the pod asked again, as one whose pod's end or detach
   * failed after its save, is released into that snapshot, never saved anew, as its sandbox
   * ended with that save, and a save of none would leave it released into none.
   */
  const savedAlready = (pool: Pool, workspace: string, name: string): Effect.Effect<SavedSnapshot | null, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      // Read again a few times, as its record is written.
      const recorded = yield* agents.released(ref(pool, workspace)).pipe(Effect.retry({ times: 3, schedule: Schedule.exponential("200 millis") }));
      if (recorded?.pod !== name) return null;
      yield* Effect.logInfo(`${pool.namespace}/${workspace} was saved already as ${recorded.snapshot}, by a release of its pod ${name} before: it is released into it`);
      return { snapshot: recorded.snapshot, release: recorded.release };
    });

  /**
   * Records the workspace, whose pod `pod` ended unsaved, as its pod went while it was bound or its
   * bayma ended, released into the snapshot taken of it live, if any, `memoryLimit` kept with it
   * where it ran out of it: unless a release's save took a snapshot of that pod already, as one whose
   * pod's end or detach failed after its save, which it stays released into (savedAlready). Where the
   * record cannot be read, it is left as it is, said so, rather than written over: the record an
   * older release left, which names no snapshot of this pod, so the next bind falls back to bayma's
   * checkpoints.
   */
  const releasedUnsaved = (pool: Pool, workspace: string, pod: string, memoryLimit: string | null = null): Effect.Effect<void, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      const already = yield* savedAlready(pool, workspace, pod).pipe(Effect.result);
      if (already._tag === "Failure") {
        return yield* Effect.logWarning(`could not read what ${named(pool, workspace)} was released into, so its record is left as it is: ${already.failure.message}`);
      }
      if (already.success) return;
      const into = yield* liveSnapshotOf(pool, workspace);
      yield* agents.setReleased(ref(pool, workspace), into === null || memoryLimit === null ? into : { ...into, memoryLimit });
    }).pipe(withLogScope("snapshots"));

  const save = (pool: Pool, workspace: string, name: string): Effect.Effect<Saved, KubeApiError> =>
    Effect.gen(function*() {
      // One whose record cannot be read keeps its binding, to be asked again.
      const already = yield* savedAlready(pool, workspace, name).pipe(Effect.result);
      if (already._tag === "Failure") {
        yield* Effect.logWarning(`could not read what ${pool.namespace}/${workspace} was released into, so it is not saved now; it runs on, still bound: ${already.failure.message}`);
        return "keep" as const;
      }
      if (already.success) return { saved: already.success };
      const taken = yield* take(pool, workspace, name);
      releases.add(1, { "alasio.sandbox.release.outcome": taken.outcome });
      if (taken.outcome === "failed") {
        yield* Effect.logWarning(`could not snapshot ${pool.namespace}/${workspace}: ${taken.why}; it runs on, still bound`);
        return "keep" as const;
      }
      if (taken.outcome === "lost") yield* Effect.logWarning(`could not snapshot ${pool.namespace}/${workspace}: ${taken.why}; its sandbox ended, so it is released, to come back from bayma's checkpoints`);
      if (taken.outcome === "unsaveable") {
        yield* Effect.logWarning(`${pool.namespace}/${workspace} can be saved no more: ${taken.why}; it is released, to come back from bayma's checkpoints, as bayma tells its next exec`);
      }
      if (taken.outcome === "unsaveable" || taken.outcome === "kept") yield* goOnWithStop(pool, workspace, name);
      const saved = taken.outcome === "snapshotted" ? taken.saved : null;
      // Its record, for its next bind, tried a few times: its sandbox has ended, so it is released all the same where none
      // can be written, its next bind then finding a snapshot it was not released into, which it forgets.
      yield* agents.setReleased(ref(pool, workspace), saved && { ...saved, pod: name }).pipe(
        Effect.retry({ times: 3, schedule: Schedule.exponential("200 millis") }),
        Effect.catch((error) => Effect.logWarning(`${pool.namespace}/${workspace}'s release into ${saved?.snapshot ?? "no snapshot"} could not be recorded, so its next bind comes back from bayma's checkpoints: ${error.message}`)),
      );
      return { saved };
    }).pipe(withLogScope("snapshots"));

  /** Forgets the workspace's snapshot, saying why; one that cannot be forgotten is said so too, and collected later. */
  const forget = (pool: Pool, workspace: string, why: string): Effect.Effect<void> =>
    agents.forget(ref(pool, workspace)).pipe(
      Effect.catch((error) => Effect.logWarning(`could not forget the snapshot of ${pool.namespace}/${workspace}: ${error.message}`)),
      Effect.andThen(Effect.logInfo(`forgot the snapshot of ${pool.namespace}/${workspace}: ${why}`)),
    );

  /** Spike: how many resumed conditions were written, for the variant of the next. */
  let writes = 0;

  /** Since when each workspace's restore has been deferred, from the first that was: in memory, as the gateway's start begins a window anew. */
  const deferredSince = new Map<string, number>();
  const named = (pool: Pool, workspace: string) => `${pool.namespace}/${workspace}`;

  /** Binds the workspace without its snapshot, which is forgotten with its record, saying why, and counted as `fallback`: null, its bind claiming a free pod. */
  const fallBack = (pool: Pool, workspace: string, fallback: Fallback, why: string): Effect.Effect<null> =>
    Effect.logWarning(`${named(pool, workspace)} is bound from bayma's checkpoints, not from its snapshot: ${why}`).pipe(
      Effect.andThen(Effect.sync(() => {
        deferredSince.delete(named(pool, workspace));
        restores.add(1, { "alasio.sandbox.restore.outcome": "fallback", "alasio.sandbox.restore.fallback": fallback });
      })),
      Effect.andThen(forget(pool, workspace, why)),
      Effect.andThen(agents.setReleased(ref(pool, workspace), null).pipe(Effect.catch((error) => Effect.logWarning(`could not forget ${named(pool, workspace)}'s record of its release: ${error.message}`)))),
      Effect.as(null),
    );

  /**
   * Defers the workspace's restore, which failed for `why`, as what it needs may come back: its
   * snapshot kept, the bind refused, to be tried again; once its restores have been deferred
   * for DEFER_FOR_MS, it falls back as `fallback` instead.
   */
  const deferOr = (pool: Pool, workspace: string, fallback: Fallback, why: string): Effect.Effect<null, BindStepFailed> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const since = deferredSince.get(named(pool, workspace)) ?? now;
      if (now - since >= DEFER_FOR_MS) return yield* fallBack(pool, workspace, fallback, `${why}, as its restores have been for ${Math.round((now - since) / 60_000)} minutes`);
      deferredSince.set(named(pool, workspace), since);
      restores.add(1, { "alasio.sandbox.restore.outcome": "deferred", "alasio.sandbox.restore.fallback": fallback });
      yield* Effect.logWarning(`the restore of ${named(pool, workspace)} from its snapshot is deferred, its snapshot kept for the next try: ${why}`);
      return yield* new BindStepFailed({ reason: `its restore from its snapshot is deferred, to be tried again: ${why}` });
    });

  /** Whether an agent's failure `error` may pass: one of reaching it, or of the bucket behind it, not one of what it was asked of. */
  const passing = (error: NodeAgentError | KubeApiError): boolean => error._tag === "KubeApiError" || error.status === undefined || error.status >= 500;

  /**
   * Whether a pod of `tolerations` may be scheduled onto the node `name`, as its Node says, as
   * alasio's restores asked: it takes it where it is not cordoned, is Ready, and has no taint of
   * NoSchedule or NoExecute the pod does not tolerate; it is unready where only its readiness
   * keeps the pod off it, which may come back; it refuses it otherwise. One that cannot be read
   * is taken as one that takes it, which the restore then finds out.
   */
  const schedulable = (name: string, tolerations: readonly V1Toleration[]): Effect.Effect<"takes" | "unready" | "refuses"> =>
    kube.read("v1", "Node", "", name).pipe(
      Effect.map((read) => {
        const node = read as V1Node | null;
        if (node === null) return "takes";
        if (node.spec?.unschedulable === true) return "refuses";
        const keeping = (node.spec?.taints ?? []).filter((taint) => (taint.effect === "NoSchedule" || taint.effect === "NoExecute") && !tolerations.some((toleration) => tolerates(toleration, taint)));
        if (keeping.some(({ key }) => !READINESS_TAINTS.has(key))) return "refuses";
        return node.status?.conditions?.find(({ type }) => type === "Ready")?.status === "True" && keeping.length === 0 ? "takes" : "unready";
      }),
      Effect.catch((error) => Effect.logWarning(`could not read whether node ${name} takes the restore's pod, which is tried as one that does: ${error.message}`).pipe(Effect.as("takes" as const))),
    );

  /**
   * The nodes that can restore `manifest` into a pod of `tolerations`, those holding its images
   * first: they have its release, with every CPU feature it was saved with, and take the pod;
   * whether every agent said what its node holds; and whether a node that has them is kept from
   * the pod only by its readiness.
   */
  const nodesFor = (manifest: Manifest, tolerations: readonly V1Toleration[]) =>
    Effect.gen(function*() {
      const found = yield* agents.nodes;
      const held = yield* Effect.forEach(found, (node) => agents.holdings(node).pipe(Effect.map((holdings) => [holdings]), Effect.catch((error) => Effect.logWarning(error.message).pipe(Effect.as([])))));
      const answered = found.length > 0 && held.every((each) => each.length > 0);
      const capable = held.flat().filter(({ releases: installed }) =>
        installed.some(({ release, cpuFeatures }) => release === manifest.release && manifest.cpuFeatures.every((feature) => cpuFeatures.includes(feature)))
      );
      const takes = yield* Effect.forEach(capable, ({ node }) => schedulable(node, tolerations));
      const able = capable.filter((_, index) => takes[index] === "takes");
      const holds = (cached: (typeof able)[number]["cached"]) =>
        cached.some(({ environment: of, workspace, images }) => of === manifest.environment && workspace === manifest.workspace && manifest.layers.every(({ digest }) => images.includes(digest)));
      return { nodes: [...able.filter(({ cached }) => holds(cached)), ...able.filter(({ cached }) => !holds(cached))].map(({ node }) => node), answered, unready: takes.includes("unready") };
    });

  /**
   * Why the restore's pod `pod` will not come back now, where it will not: it ended, or a
   * container of it ended, or could not start, started again by kubelet only to fail; and
   * whether what failed is the restore itself, runsc refusing the snapshot, as its container's
   * message says (RESTORE_REFUSED), which no try restores, or what may pass, as a pull or a node's
   * hiccup.
   */
  const failedStart = (pod: V1Pod | null): { readonly why: string; readonly refused: boolean } | null => {
    if (pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded") return { why: `its pod ended (${pod.status.reason ?? pod.status.phase})`, refused: false };
    for (const status of [...(pod?.status?.initContainerStatuses ?? []), ...(pod?.status?.containerStatuses ?? [])]) {
      const ended = status.state?.terminated;
      const before = status.lastState?.terminated;
      const waiting = status.state?.waiting;
      const said = [ended?.message, before?.message, waiting?.message].filter((message) => message !== undefined).join("; ");
      const failure = (why: string) => ({ why: `its ${status.name} ${why}`, refused: RESTORE_REFUSED.test(said) });
      if (ended) return failure(`ended (${ended.message ?? ended.reason ?? ended.exitCode})`);
      if (status.restartCount > 0 || before) return failure(`could not start (${before?.message ?? before?.reason ?? "started again"})`);
      if (waiting?.reason !== undefined && START_FAILURES.has(waiting.reason)) return failure(`could not start (${waiting.message ?? waiting.reason})`);
    }
    return null;
  };

  /** The restore's pod `name` of `pool`, once `until` holds of it, `what` it waits for; refused where it will not come back now (failedStart), never to be tried again where runsc refused its restore, and refused where it does not within POD_RUNNING_TIMEOUT_MS. */
  const podOnce = (pool: Pool, name: string, until: (pod: V1Pod) => boolean, what: string): Effect.Effect<V1Pod, RestoreRefused> =>
    podOf(pool, name).pipe(
      Effect.flatMap((pod): Effect.Effect<V1Pod, PodNotRunning | RestoreRefused> => {
        const failed = failedStart(pod);
        if (failed !== null) return Effect.fail(new RestoreRefused({ reason: failed.why, resumed: false, failed: failed.refused }));
        return pod && until(pod) ? Effect.succeed(pod) : Effect.fail(new PodNotRunning());
      }),
      Effect.retry({ while: (error) => error._tag === "PodNotRunning", schedule: Schedule.max([Schedule.spaced("500 millis"), Schedule.during(POD_RUNNING_TIMEOUT_MS)]) }),
      Effect.catchTag("PodNotRunning", () => Effect.fail(new RestoreRefused({ reason: `its pod did not ${what} within ${POD_RUNNING_TIMEOUT_MS / 1000} s`, resumed: false }))),
      Effect.catchTag("KubeApiError", (error) => Effect.fail(new RestoreRefused({ reason: error.message, resumed: false }))),
    );

  /**
   * Lets the restored pod `name`, on `node`, restored from `snapshot`, of images of `bytes`
   * together, its workspace attached before it was made, run: once its containers run, paused,
   * its node's agent resumes its sandbox once the pod's egress is confined there, waited for as
   * long as reading those images may take, and the pod is then ready, by its readiness gate. A
   * pod that ends meanwhile, or cannot start, whose sandbox the agent does not resume, or whose
   * readiness cannot be set, is refused.
   */
  const resumeRestored = (pool: Pool, workspace: string, name: string, node: string, snapshot: string, bytes: number): Effect.Effect<void, RestoreRefused> =>
    Effect.gen(function*() {
      const running = yield* podOnce(
        pool,
        name,
        (pod) => (pod.status?.containerStatuses ?? []).length > 0 && (pod.status?.containerStatuses ?? []).every(({ state }) => state?.running) && (pod.status?.podIPs?.length ?? 0) > 0,
        "run",
      );
      yield* trace(`${workspace} gateway saw-running ${running.status?.containerStatuses?.[0]?.state?.running?.startedAt ?? ""}`);
      const containerIds = (running.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^[a-z]+:\/\//u, "")] : []));
      const podIPs = (running.status?.podIPs ?? []).map(({ ip }) => ip);
      // The policies that select it, which its node's rules for it must name.
      const policies = yield* kube.list("networking.k8s.io/v1", "NetworkPolicy", pool.namespace, "").pipe(
        Effect.map((found) => (found as V1NetworkPolicy[]).filter(({ spec }) => selects(spec?.podSelector, running.metadata?.labels ?? {})).flatMap(({ metadata }) => metadata?.name ?? [])),
        Effect.mapError((error) => new RestoreRefused({ reason: error.message, resumed: false })),
      );
      yield* trace(`${workspace} gateway policies-listed`);
      // Spike: when the resumed condition is written, by turns: after the resume, or before it.
      const variant = (["patch", "early"] as const)[writes++ % 2] ?? "patch";
      yield* trace(`${workspace} gateway variant ${variant}`);
      const resumedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      const resumedCondition = { type: RESUMED_CONDITION, status: "True", lastTransitionTime: resumedAt };
      if (variant === "early") {
        yield* kube.patchPodStatus(pool.namespace, name, { status: { conditions: [resumedCondition] } }).pipe(Effect.mapError((error) => new RestoreRefused({ reason: error.message, resumed: false })));
        yield* trace(`${workspace} gateway condition-set`);
      }
      // Paused where its policies are not in force (504), or where the bucket fails (502), which
      // may pass; of any other failure its sandbox may have run; paused, too, where the snapshot
      // was taken live and holds the session no more (410), which no try restores.
      yield* agents.resume(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, podIPs, containerIds, policies, snapshot }, bytes).pipe(
        Effect.mapError((error) => {
          const status = error._tag === "NodeAgentError" ? error.status : undefined;
          return new RestoreRefused({ reason: error.message, resumed: status !== 502 && status !== 504 && status !== 410, ...(status === 410 ? { stale: true } : {}) });
        }),
      );
      yield* trace(`${workspace} gateway resumed`);
      if (variant === "patch") {
        yield* kube.patchPodStatus(pool.namespace, name, { status: { conditions: [resumedCondition] } }).pipe(
          Effect.mapError((error) => new RestoreRefused({ reason: `its readiness could not be set: ${error.message}`, resumed: true })),
        );
      }
      if (variant !== "early") yield* trace(`${workspace} gateway condition-set`);
    });

  /** Has the agent of `node` abandon the restore into the pod `name`, refused before its sandbox resumed, as alasio's were: what runsc runs for it killed. */
  const abandon = (pool: Pool, workspace: string, name: string, node: string): Effect.Effect<void> =>
    podOf(pool, name).pipe(
      Effect.flatMap((pod) => {
        const uid = pod?.metadata?.uid;
        if (!uid) return Effect.logInfo(`the restore of ${named(pool, workspace)} on ${node} has no pod left, so nothing of it to abandon`);
        return agents.abandon(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, uid }).pipe(
          Effect.flatMap((killed) =>
            killed.length === 0 ? Effect.void : Effect.logInfo(`killed what runsc left running of the restore of ${named(pool, workspace)} on ${node}: ${killed.map(({ containerId, pid }) => `${pid} of ${containerId}`).join(", ")}`)
          ),
        );
      }),
      Effect.catch((error) => Effect.logWarning(`could not abandon the restore of ${named(pool, workspace)} on ${node}: ${error.message}`)),
    );

  /** The pool's template a snapshot taken on `recorded` restores onto, as the configuration keeps them, by its hash: the current one, the one before it, or none. */
  const templateOf = (mode: "none" | "full", recorded: V1PodTemplateSpec): V1PodTemplateSpec | null => {
    const kept = snapshots?.templates[mode];
    const hash = recorded.metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION];
    if (!kept || hash === undefined) return null;
    const hashOf = (template: unknown) => (template as V1PodTemplateSpec | null)?.metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION];
    if (hash === hashOf(kept.current)) return kept.current as V1PodTemplateSpec;
    return hash === hashOf(kept.previous) ? (kept.previous as V1PodTemplateSpec) : null;
  };

  const restoring = (pool: Pool, workspace: string, mode: "none" | "full", attaching: Attaching): Restoring => {
    /** What this bind's maker restored, which its pod's readying resumes. */
    let restored: { readonly pod: string; readonly node: string; readonly snapshot: string; readonly bytes: number; readonly manifest: Manifest; readonly started: number } | null = null;

    const make: MakePod = (labels, annotations, binding) =>
      Effect.gen(function*() {
        if (!snapshots || pool.namespace !== mainNamespace) return null;
        const started = yield* Clock.currentTimeMillis;
        yield* trace(`${workspace} gateway make`);
        const record = yield* agents.released(ref(pool, workspace)).pipe(Effect.result);
        yield* trace(`${workspace} gateway record-read`);
        const read = record._tag === "Success" ? yield* agents.current(ref(pool, workspace)).pipe(Effect.result) : record;
        yield* trace(`${workspace} gateway current-read`);
        if (read._tag === "Failure") return yield* (passing(read.failure) ? deferOr : fallBack)(pool, workspace, "unreadable", read.failure.message);
        const manifest = read.success;
        const into = record._tag === "Success" ? record.success : null;
        if (!manifest) {
          // Released into a snapshot there is no more, as one forgotten: bound anew, the record with it.
          if (into) return yield* fallBack(pool, workspace, "stale", `its record names the snapshot ${into.snapshot}, which it holds no more`);
          return null;
        }
        // Only the snapshot it was released into, or one its node's agent compacted from that one since, which is the same.
        if (into?.snapshot !== manifest.snapshot && (into === null || into.snapshot !== manifest.compactedFrom)) {
          return yield* fallBack(pool, workspace, "stale", `its snapshot ${manifest.snapshot} is not the one it was released into (${into?.snapshot ?? "none"})`);
        }
        const kept = snapshots.releases.find(({ release }) => release === manifest.release);
        if (!kept) return yield* fallBack(pool, workspace, "release", `gVisor ${manifest.release}, which took it, is no longer kept`);
        const base = templateOf(mode, manifest.podTemplate as V1PodTemplateSpec);
        if (!base) return yield* fallBack(pool, workspace, "template", "the pool's template it was taken on is no longer kept");
        const able = yield* nodesFor(manifest, base.spec?.tolerations ?? []).pipe(Effect.result);
        yield* trace(`${workspace} gateway nodes-found`);
        if (able._tag === "Failure") return yield* deferOr(pool, workspace, "unreadable", able.failure.message);
        if (able.success.nodes.length === 0) {
          const none = `no node that takes its pod has gVisor ${manifest.release} and every CPU feature it was saved with`;
          if (able.success.unready) return yield* deferOr(pool, workspace, "node", `${none}: one that has them is not Ready`);
          return yield* (able.success.answered ? fallBack(pool, workspace, "node", none) : deferOr(pool, workspace, "node", `${none} of the nodes whose agents answered`));
        }
        // Each node tried in turn: one that cannot restore it (409) leaves the others; one whose bucket or agent fails may pass.
        const failures: (NodeAgentError | KubeApiError)[] = [];
        for (const node of able.success.nodes) {
          const prepared = yield* agents.prepare(node, ref(pool, workspace), kept.gofer ?? undefined).pipe(Effect.result);
          yield* trace(`${workspace} gateway prepared`);
          if (prepared._tag === "Failure") {
            failures.push(prepared.failure);
            yield* Effect.logWarning(prepared.failure.message);
            continue;
          }
          // What the agent prepared, which a compaction committed since may have made the one compacted from it.
          const from = prepared.success.manifest;
          yield* Effect.logInfo(`restoring ${named(pool, workspace)} on ${node} from its snapshot ${from.snapshot}, taken at ${from.takenAt} under gVisor ${from.release}`);
          const { [FREE_LABEL]: _free, ...own } = base.metadata?.labels ?? {};
          // Named before it is made, as its own directory is by its name (ownDirectory); its workspace
          // attached there first, so its sandbox, restored as its bayma starts, finds every file it held open;
          // and, of one stopped as it ran out of its memory, its bayma's hook told so, for its sessions' next execs.
          const name = restorePodName(pool.name, randomBytes(4).readUInt32BE().toString(36).padStart(7, "0").slice(-7));
          yield* trace(`${workspace} gateway attach`);
          yield* attaching.attach(node, name, binding, into?.memoryLimit === undefined ? {} : { [SANDBOX_STOP_REASON_VARIABLE]: `it ran out of its memory (${into.memoryLimit})` });
          yield* trace(`${workspace} gateway attached`);
          const spec = ownDirectory({ containers: [], ...base.spec }, pool.namespace, name);
          const pod = yield* kube.create<V1Pod>({
            apiVersion: "v1",
            kind: "Pod",
            metadata: {
              name,
              namespace: pool.namespace,
              labels: { ...own, ...labels },
              // Left paused, until its node's agent sees its egress confined (resumeRestored).
              annotations: { ...base.metadata?.annotations, ...annotations, [RESTORE_ANNOTATION]: prepared.success.imagePath, [RESTORE_PAUSED_ANNOTATION]: "true" },
            },
            spec: {
              ...spec,
              // Never started again: its container, ended by its save, would be restored once more from the image it was
              // restored from, which its workspace's files have moved on from, and a sandbox restored paused that fails holds
              // its pod from being deleted.
              restartPolicy: "Never",
              runtimeClassName: kept.runtimeClassName,
              readinessGates: [...(base.spec?.readinessGates ?? []), { conditionType: RESUMED_CONDITION }],
              affinity: { ...base.spec?.affinity, nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [node] }] }] } } },
            },
          }).pipe(
            Effect.tapError(() => attaching.detach(node, name)),
            Effect.mapError((error) => new BindStepFailed({ reason: `its restore's pod could not be made: ${error.message}` })),
          );
          yield* trace(`${workspace} gateway pod-created ${name}`);
          restored = { pod: pod.metadata?.name ?? "", node, snapshot: from.snapshot, bytes: from.bytes, manifest, started };
          return pod;
        }
        // Gone or corrupt (404, 422), or of no node's (409), it never restores; otherwise it may yet.
        const why = failures.map(({ message }) => message).join("; ");
        return yield* (failures.some(passing) && !failures.some((failure) => failure._tag === "NodeAgentError" && (failure.status === 404 || failure.status === 422))
          ? deferOr(pool, workspace, "prepare", why)
          : fallBack(pool, workspace, "prepare", why));
      }).pipe(
        Effect.mapError((error) => (error._tag === "BindStepFailed" ? error : new BindStepFailed({ reason: error.message }))),
        withAlasioSpan("alasio.sandbox.restore", { attributes: { "alasio.sandbox.name": workspace, "k8s.namespace.name": pool.namespace } }),
        withLogScope("snapshots"),
      );

    const ready = (pod: V1Pod, attach: (pod: V1Pod) => Effect.Effect<void, BindStepFailed>): Effect.Effect<void, BindStepFailed> =>
      Effect.gen(function*() {
        const of = restored;
        if (of === null || of.pod !== pod.metadata?.name) return yield* attach(pod);
        const done = yield* resumeRestored(pool, workspace, of.pod, of.node, of.snapshot, of.bytes).pipe(Effect.result);
        if (done._tag === "Success") {
          const now = yield* Clock.currentTimeMillis;
          deferredSince.delete(named(pool, workspace));
          restores.add(1, { "alasio.sandbox.restore.outcome": "restored" });
          restoreDuration.record((now - of.started) / 1000);
          // It moves on from its snapshot, whose images go on as the layers of its next; released into none, until its next release.
          yield* agents.spend(ref(pool, workspace), of.manifest.snapshot).pipe(Effect.catch((error) => Effect.logWarning(`could not spend the snapshot of ${named(pool, workspace)}, which runs on from it: ${error.message}`)));
          yield* agents.setReleased(ref(pool, workspace), null).pipe(Effect.catch((error) => Effect.logWarning(`could not clear ${named(pool, workspace)}'s record of its release: ${error.message}`)));
          yield* trace(`${workspace} gateway spent`);
          return;
        }
        const { reason: why, resumed, stale, failed } = done.failure;
        // What was attached for it detached, once its pod, which its binder deletes, has gone.
        yield* attaching.detach(of.node, of.pod);
        // Its sandbox never having run, its restore is abandoned; one of a snapshot that holds the session no more, or that could not start from it, falls back at once.
        if (resumed) yield* fallBack(pool, workspace, "restore", why);
        else {
          yield* abandon(pool, workspace, of.pod, of.node);
          yield* (stale ? fallBack(pool, workspace, "stale", why) : failed ? fallBack(pool, workspace, "restore", why) : deferOr(pool, workspace, "restore", why)).pipe(Effect.ignore);
        }
        return yield* new BindStepFailed({ reason: `its restore did not come back: ${why}` });
      }).pipe(withLogScope("snapshots"));

    return { make, ready };
  };


  // The releases of ending pods each node runs at once, RELEASES_PER_NODE, the others waiting their turn, as their bayma holds its stop for them.
  const stopSlots = new Map<string, Semaphore.Semaphore>();
  const stopSlot = (node: string): Semaphore.Semaphore => {
    const found = stopSlots.get(node);
    if (found) return found;
    const made = Semaphore.makeUnsafe(RELEASES_PER_NODE);
    stopSlots.set(node, made);
    return made;
  };

  const saveStopping = (pool: Pool, workspace: string, pod: V1Pod): Effect.Effect<void> => {
    const node = pod.spec?.nodeName ?? "";
    const counted = (outcome: string) => Effect.sync(() => stopReleases.add(1, { "alasio.sandbox.stop_release.outcome": outcome }));
    const deletion = pod.metadata?.deletionTimestamp;
    // Its bayma holds its stop until its pod's deletion less its own stop (BAYMA_STOP_SECONDS).
    const heldUntil = deletion === undefined ? Infinity : new Date(deletion).getTime() - BAYMA_STOP_SECONDS * 1000;
    const name = pod.metadata?.name ?? "";
    const record = (saved: SavedSnapshot | null) => agents.setReleased(ref(pool, workspace), saved);
    return Effect.gen(function*() {
      if (!snapshots || pool.namespace !== mainNamespace) return yield* record(null);
      // Saved already by an end of it before, as one whose detach failed after: released into that snapshot. Where that
      // cannot be read, saved as it would be, but recorded only where saved: what the record holds is not written over with
      // the snapshot taken live, or none. A first end that saves nothing so leaves the record an older release left, which
      // names no snapshot of this pod, and the next bind falls back to bayma's checkpoints.
      const already = yield* savedAlready(pool, workspace, name).pipe(Effect.result);
      if (already._tag === "Success" && already.success) return;
      const unread = already._tag === "Failure";
      if (unread) yield* Effect.logWarning(`could not read what ${named(pool, workspace)} was released into, so it is recorded only where saved now: ${already.failure.message}`);
      const recordUnsaved = unread ? Effect.void : Effect.flatMap(liveSnapshotOf(pool, workspace), record);
      if (!holdsStop(pod.spec?.containers.find((container) => container.name === BAYMA_CONTAINER))) {
        yield* Effect.logWarning(`${named(pool, workspace)} is not snapshotted as its pod ends: its bayma holds no stop for it`);
        yield* counted("unheld");
        return yield* recordUnsaved;
      }
      const saved = yield* stopSlot(node).withPermits(1)(Effect.gen(function*() {
        const now = yield* Clock.currentTimeMillis;
        if (now >= heldUntil) {
          yield* Effect.logWarning(`${named(pool, workspace)} is not snapshotted as its pod ends: its turn came ${Math.round((now - heldUntil) / 1000)} s after its bayma held its stop no more`);
          yield* counted("late");
          return null;
        }
        const taken = yield* take(pool, workspace, name);
        if (taken.outcome === "snapshotted") {
          yield* counted("snapshotted");
          return { ...taken.saved, pod: name };
        }
        if (taken.outcome !== "kept") yield* Effect.logWarning(`could not snapshot ${named(pool, workspace)} as its pod ends: ${taken.why}`);
        yield* counted("unsnapshotted");
        return null;
      }));
      // Saved into none, it is released into the snapshot taken of it live, if any, as alasio's sessions were.
      yield* (saved ? record(saved) : recordUnsaved);
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not snapshot ${named(pool, workspace)} as its pod ends: ${error.message}`).pipe(Effect.andThen(counted("error")))),
      withAlasioSpan("alasio.sandbox.stop_release", { attributes: { "alasio.sandbox.name": workspace, "k8s.node.name": node } }),
      withLogScope("snapshots"),
    );
  };

  /**
   * Of each bound workspace, by its pool's namespace and name: the snapshot taken of it live that
   * holds it still, with the generation it holds it at; since when its execs have run; and when
   * it was last saved live. In memory: the gateway's start begins them anew.
   */
  const takenLive = new Map<string, { readonly snapshot: string; readonly generation: number }>();
  const busySince = new Map<string, number>();
  const savedLiveAt = new Map<string, number>();
  /** How many live saves run on each node, which its agent checkpoints one beside the other. */
  const liveOnNode = new Map<string, number>();
  const liveEveryMs = (snapshots?.liveEverySeconds ?? 0) * 1000;

  /** Saves `pod`'s sandbox live by its node's agent, running on: the snapshot it took and the generation it holds the workspace at; null where none was taken; or "unsaveable". */
  const saveLive = (pool: Pool, workspace: string, pod: V1Pod): Effect.Effect<{ readonly snapshot: string; readonly generation: number } | null | "unsaveable", KubeApiError> =>
    Effect.gen(function*() {
      const counted = (outcome: string) => Effect.sync(() => liveCheckpoints.add(1, { "alasio.sandbox.live_checkpoint.outcome": outcome }));
      const containerId = baymaId(pod);
      const bayma = pod.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma ? hookOf(bayma) : null;
      const stateDir = bayma ? stateDirOf(bayma) : null;
      const node = pod.spec?.nodeName;
      if (!node || !containerId || pod.spec?.runtimeClassName === undefined || !hook || !stateDir) return null;
      const deployment = (yield* kube.read("apps/v1", "Deployment", pool.namespace, pool.name)) as V1Deployment | null;
      const name = pod.metadata?.name ?? "";
      const taken = yield* agents.checkpoint(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, containerId, hook, podTemplate: { ...deployment?.spec?.template }, live: { stateDir } }).pipe(Effect.result);
      if (taken._tag === "Failure" && unsaveable(taken.failure)) {
        yield* counted("unsaveable");
        return "unsaveable" as const;
      }
      if (taken._tag === "Failure" || taken.success.live === undefined) {
        yield* counted("failed");
        yield* Effect.logWarning(`could not snapshot ${named(pool, workspace)} live, which runs on: ${taken._tag === "Failure" ? taken.failure.message : "its manifest says nothing of the save"}`);
        return null;
      }
      const { snapshot, bytes, pauseMs, live } = taken.success;
      yield* counted("taken");
      yield* Effect.logInfo(`snapshotted ${named(pool, workspace)} live as ${snapshot} on ${node}, at bayma's generation ${live.generation}: ${bytes} bytes, paused ${pauseMs} ms`);
      return { snapshot, generation: live.generation };
    });

  const keepLive = (pool: Pool, workspace: string, pod: V1Pod, status: BaymaStatus, now: number, ended: Effect.Effect<void>): Effect.Effect<void> =>
    Effect.gen(function*() {
      if (!snapshots || pool.namespace !== mainNamespace || liveEveryMs <= 0) return;
      const it = named(pool, workspace);
      const taken = takenLive.get(it);
      if (taken && taken.generation !== status.generation) {
        takenLive.delete(it);
        yield* agents.spend(ref(pool, workspace), taken.snapshot).pipe(
          Effect.tap(() => Effect.sync(() => liveCheckpoints.add(1, { "alasio.sandbox.live_checkpoint.outcome": "spent" }))),
          Effect.tap(() => Effect.logInfo(`spent the snapshot ${taken.snapshot} of ${it}, taken live, which has moved on from it`)),
          Effect.catch((error) => Effect.logWarning(`could not spend the snapshot ${taken.snapshot} of ${it}, which has moved on from it: ${error.message}`)),
        );
      }
      if (status.execsRunning === 0) {
        busySince.delete(it);
        return;
      }
      const since = busySince.get(it) ?? now;
      busySince.set(it, since);
      if (now - Math.max(since, savedLiveAt.get(it) ?? since) < liveEveryMs) return;
      const node = pod.spec?.nodeName ?? "";
      if ((liveOnNode.get(node) ?? 0) >= RELEASES_PER_NODE) return;
      liveOnNode.set(node, (liveOnNode.get(node) ?? 0) + 1);
      savedLiveAt.set(it, now);
      yield* Effect.forkDetach(saveLive(pool, workspace, pod).pipe(
        Effect.flatMap((saved) => (saved === "unsaveable" ? ended : Effect.sync(() => void (saved ? takenLive.set(it, saved) : undefined)))),
        Effect.catch((error) => Effect.logWarning(`could not snapshot ${it} live: ${error.message}`)),
        Effect.ensuring(Effect.sync(() => void liveOnNode.set(node, (liveOnNode.get(node) ?? 1) - 1))),
        withAlasioSpan("alasio.sandbox.live_checkpoint", { attributes: { "alasio.sandbox.name": workspace } }),
        withLogScope("snapshots"),
      ));
    });

  const liveSnapshotOf = (pool: Pool, workspace: string): Effect.Effect<SavedSnapshot | null> =>
    !snapshots || pool.namespace !== mainNamespace
      ? Effect.succeed(null)
      : agents.current(ref(pool, workspace)).pipe(
        Effect.map((manifest): SavedSnapshot | null => (manifest?.live === undefined ? null : { snapshot: manifest.snapshot, release: manifest.release })),
        // Its restore, or the fallback from it, the one to spend it, not the look as well.
        Effect.tap((live) => Effect.sync(() => (live !== null && takenLive.get(named(pool, workspace))?.snapshot === live.snapshot ? takenLive.delete(named(pool, workspace)) : undefined))),
        Effect.catch((error) => Effect.logWarning(`could not read whether ${named(pool, workspace)} has a snapshot taken live: ${error.message}`).pipe(Effect.as(null))),
      );

  const forgetLive = (namespace: string, bound: ReadonlySet<string>): Effect.Effect<void> =>
    Effect.sync(() => {
      for (const map of [takenLive, busySince, savedLiveAt]) {
        for (const it of [...map.keys()]) if (it.startsWith(`${namespace}/`) && !bound.has(it.slice(namespace.length + 1))) map.delete(it);
      }
    });

  const endUnsaveable = (pool: Pool, workspace: string, name: string): Effect.Effect<void, KubeApiError> =>
    Effect.gen(function*() {
      releases.add(1, { "alasio.sandbox.release.outcome": "unsaveable" });
      yield* Effect.logWarning(`${named(pool, workspace)} can be saved no more, as its live checkpoint failed as every checkpoint of it will: it is released, to come back from bayma's checkpoints`);
      yield* goOnWithStop(pool, workspace, name);
      yield* agents.setReleased(ref(pool, workspace), null).pipe(Effect.catch((error) => Effect.logWarning(`could not record ${named(pool, workspace)}'s release into none: ${error.message}`)));
    }).pipe(withLogScope("snapshots"));

  /** Whether `pool`'s workspaces are snapshotted: main's alone, as alasio's were. */
  const snapshotted = (pool: Pool) => snapshots !== null && pool.namespace === mainNamespace;

  const forgetDeleted = (pool: Pool, workspace: string): Effect.Effect<void> =>
    !snapshotted(pool) ? Effect.void : Effect.gen(function*() {
      yield* agents.forget(ref(pool, workspace));
      for (const node of yield* agents.nodes) yield* agents.discard(node, ref(pool, workspace), []);
      yield* Effect.logInfo(`forgot the snapshots of ${pool.namespace}/${workspace}, deleted`);
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not forget the snapshots of ${pool.namespace}/${workspace}, deleted, which are collected later: ${error.message}`)),
      withLogScope("snapshots"),
    );

  const copy = (pool: Pool, from: string, into: Pool, to: string): Effect.Effect<SavedSnapshot | null> =>
    !snapshotted(pool) || !snapshotted(into) ? Effect.succeed(null) : Effect.gen(function*() {
      // Only the snapshot it was released into, or one compacted from that one since, which is the same: not one it has moved on from.
      const released = yield* agents.released(ref(pool, from));
      if (!released) return null;
      const current = yield* agents.current(ref(pool, from));
      if (current?.snapshot !== released.snapshot && current?.compactedFrom !== released.snapshot) return null;
      const { snapshot, release } = yield* agents.copy(ref(pool, from), ref(into, to));
      yield* Effect.logInfo(`gave the fork ${into.namespace}/${to} of ${pool.namespace}/${from} its snapshot ${snapshot}`);
      return { snapshot, release };
    }).pipe(
      Effect.catch((error) => Effect.as(Effect.logWarning(`the fork ${into.namespace}/${to} of ${pool.namespace}/${from} carries no snapshot: ${error.message}`), null)),
      withLogScope("snapshots"),
    );

  const collect: Effect.Effect<void> = !snapshots ? Effect.void : Effect.gen(function*() {
    const there = new Set(yield* agents.listWorkspaces(mainNamespace));
    const kept = new Map<string, readonly string[]>();
    for (const workspace of yield* agents.workspaces(mainNamespace)) {
      const of = { environment: mainNamespace, workspace };
      if (there.has(workspace)) {
        kept.set(workspace, yield* agents.collect(of));
        continue;
      }
      yield* agents.forget(of);
      yield* Effect.logInfo(`forgot the snapshots of ${mainNamespace}/${workspace}, which is gone`);
    }
    for (const node of yield* agents.nodes) {
      for (const held of (yield* agents.holdings(node)).cached.filter((cached) => cached.environment === mainNamespace)) {
        const keep = kept.get(held.workspace) ?? [];
        if (held.images.some((digest) => !keep.includes(digest))) yield* agents.discard(node, { environment: mainNamespace, workspace: held.workspace }, keep);
      }
    }
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`could not collect main's snapshots: ${error.message}`)),
    withLogScope("snapshots"),
  );

  const releasesNamed: Effect.Effect<ReadonlyMap<string, readonly string[]>, NodeAgentError | KubeApiError> = Effect.gen(function*() {
    const refs = (yield* Effect.forEach(yield* agents.listEnvironments, (environment) =>
      Effect.map(agents.listWorkspaces(environment), (workspaces) => workspaces.map((workspace) => ({ environment, workspace }))), { concurrency: READS_AT_ONCE })).flat();
    const named = yield* Effect.forEach(refs, (of) =>
      Effect.map(Effect.all([agents.current(of), agents.released(of)], { concurrency: 2 }), (saved) =>
        saved.flatMap((each) => (each ? [{ release: each.release, snapshot: `${of.environment}/${of.workspace}/${each.snapshot}` }] : []))), { concurrency: READS_AT_ONCE });
    const releases = new Map<string, string[]>();
    for (const { release, snapshot } of named.flat()) {
      const of = releases.get(release) ?? [];
      if (!of.includes(snapshot)) releases.set(release, [...of, snapshot]);
    }
    return releases;
  });

  return { save, saveStopping, endUnsaveable, keepLive, liveSnapshotOf, releasedUnsaved, forgetLive, restoring, forget: forgetDeleted, copy, collect, releasesNamed };
});
