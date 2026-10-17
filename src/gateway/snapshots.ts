/**
 * Whole-sandbox snapshots of the sandbox gateway's session workspaces, as alasio's own were
 * (../sandbox/snapshots.ts), of a bound pool pod rather than of a Sandbox: a workspace, of an
 * environment's sessions' namespace, released is saved whole by the node agent on its pod's node
 * (../sandbox/node-agents.ts), bayma told first (`bayma sandbox-hook`), which ends it; and the
 * snapshot it was released into is recorded with the workspace (its node agents' record of it,
 * ../node-agent/workspaces.ts Released), while the gateway holds its binding's Lease, as every
 * read and write of the record is, for its next bind to restore it.
 *
 * A save comes to an outcome, each counted (alasio.sandbox.releases), as alasio's did:
 * snapshotted, into the snapshot it took; kept, as its pod runs no bayma under gVisor, which
 * the gateway then releases unsaved, its pod deleted, as a pool's pods are for it to reuse;
 * lost, its sandbox ended by a save that failed, its REPLs with it; unsaveable, as the agent
 * says no save of it will be taken, whose bayma is told to go on with the stop it would hold
 * for one (its node agent's POST /stop), so its pod ends at once; or failed, its sandbox
 * running on, which keeps the binding.
 *
 * A workspace released into a snapshot is restored from it as it is next bound, as alasio's
 * sessions were, its sandbox as it was, by a pod the gateway makes, its binding's, of the
 * pool's template the snapshot was taken on: its current one, or the one before it, which
 * `alasio up` carries for one change of it (GatewaySnapshots templates), and nothing older; on
 * a node that has the gVisor release that took it, still kept, and the CPU features it was
 * saved with, those that hold its images first; left paused until its node's agent sees its
 * egress confined there, then resumed, and ready. Only the snapshot the record says it was
 * released into restores, or one its node's agent compacted from it since, which is the same:
 * one it has moved on from since is stale. A restore that cannot be done now, as what it needs
 * fails for a while, is deferred, its snapshot kept, the bind refused to be tried again, for
 * DEFER_FOR_MS from its first; one that never can, or one deferred for that long, falls back,
 * its snapshot and its record forgotten, the workspace bound to a free pod of its pool, from
 * bayma's checkpoints, why logged and counted (alasio.sandbox.restores). A restore refused
 * before its sandbox resumed is abandoned on its node first, as alasio's were. Main's sessions
 * alone are snapshotted, as alasio's were.
 */
import type { V1Deployment, V1NetworkPolicy, V1Node, V1Pod, V1PodTemplateSpec, V1Toleration } from "@kubernetes/client-node";
import { Clock, Effect, Schedule, Schema } from "effect";

import { BindStepFailed, type MakePod } from "../binder/binder.ts";
import { FREE_LABEL, type Pool } from "../binder/names.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { BAYMA_CONTAINER, RESTORE_ANNOTATION, RESTORE_PAUSED_ANNOTATION, RESUMED_CONDITION, SESSION_TEMPLATE_ANNOTATION } from "../kube/sandbox-shape.ts";
import type { SavedSnapshot } from "../kube/sandboxes.ts";
import type { Manifest } from "../node-agent/agent.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import { type Fallback, holdsStop, hookOf, selects, stopHookOf } from "../sandbox/snapshots.ts";
import { withLogScope } from "../shared/log.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";
import type { GatewaySnapshots as SnapshotsConfig } from "./names.ts";

const releases = meter.createCounter("alasio.sandbox.releases", {
  description:
    "Session sandboxes released, by outcome: snapshotted; kept, as they run under no gVisor, released unsaved; failed, running on; lost, their sandbox ended unsaved, its REPLs with it; unsaveable, as no save of it will be taken; or error, the release failing beside its save",
});

const restores = meter.createCounter("alasio.sandbox.restores", {
  description: "Session sandboxes brought up with a snapshot, by outcome: restored, or not, by why",
});
const restoreDuration = meter.createHistogram("alasio.sandbox.restore.duration", {
  description: "How long a session restored from its snapshot took to come back, which the request binding it waits: from its snapshot looked up to its pod ready",
  unit: "s",
});

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

/** Whether `toleration` tolerates `taint`, as the scheduler has it (../sandbox/snapshots.ts). */
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

/** Whether `error`, a checkpoint's, says its sandbox can be saved no more. */
const unsaveable = (error: NodeAgentError | KubeApiError): boolean => error._tag === "NodeAgentError" && error.status === UNSAVEABLE_STATUS;

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
 * does, or declines; and `ready`, what readies the pod it got, its workspace attached, and, of a
 * restore, its sandbox let run, the restore done, or fails it, the restore deferred or fallen back.
 */
export interface Restoring {
  readonly make: MakePod;
  /** Readies `pod`: its workspace attached, by `attach`, once it runs, then, where it is this bind's restore's, its sandbox resumed. */
  readonly ready: (pod: V1Pod, attach: (pod: V1Pod) => Effect.Effect<void, BindStepFailed>) => Effect.Effect<void, BindStepFailed>;
}

/** The gateway's snapshots; see makeGatewaySnapshots. */
export interface GatewaySnapshots {
  /** How a bind of `workspace` of `pool`, of its sessions' internet `mode`, restores it, as the module says. */
  readonly restoring: (pool: Pool, workspace: string, mode: "none" | "full") => Restoring;
  /**
   * Saves the sandbox of `pod`, bound to `workspace` of `pool`, for its release, as the module
   * says, and records what it was released into: the binding kept where it runs on, or
   * released. Counted as the release's outcome. Run while the gateway holds the binding's Lease.
   */
  readonly save: (pool: Pool, workspace: string, pod: string) => Effect.Effect<Saved, KubeApiError>;
}

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

  const save = (pool: Pool, workspace: string, name: string): Effect.Effect<Saved, KubeApiError> =>
    Effect.gen(function*() {
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
      yield* agents.setReleased(ref(pool, workspace), saved).pipe(
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
   * Lets the restored pod `name`, on `node`, restored from `snapshot`, run, once its containers
   * run, paused, and its workspace is attached by `attach`: its node's agent resumes its sandbox
   * once the pod's egress is confined there,
   * and the pod is then ready, by its readiness gate. A pod that ends meanwhile, whose sandbox
   * the agent does not resume, or whose readiness cannot be set, is refused.
   */
  const resumeRestored = (pool: Pool, workspace: string, name: string, node: string, snapshot: string, attach: (pod: V1Pod) => Effect.Effect<void, BindStepFailed>): Effect.Effect<void, RestoreRefused> =>
    Effect.gen(function*() {
      const running = yield* podOf(pool, name).pipe(
        Effect.flatMap((pod): Effect.Effect<V1Pod, PodNotRunning | RestoreRefused> => {
          const statuses = pod?.status?.containerStatuses ?? [];
          if (pod?.status?.phase === "Failed" || pod?.status?.phase === "Succeeded" || statuses.some(({ state }) => state?.terminated)) {
            return Effect.fail(new RestoreRefused({ reason: `its pod ended before it ran (${pod?.status?.reason ?? pod?.status?.phase ?? "terminated"})`, resumed: false }));
          }
          const ready = pod && statuses.length > 0 && statuses.every(({ state }) => state?.running) && (pod.status?.podIPs?.length ?? 0) > 0;
          return ready ? Effect.succeed(pod) : Effect.fail(new PodNotRunning());
        }),
        Effect.retry({ while: (error) => error._tag === "PodNotRunning", schedule: Schedule.max([Schedule.spaced("500 millis"), Schedule.during(POD_RUNNING_TIMEOUT_MS)]) }),
        Effect.catchTag("PodNotRunning", () => Effect.fail(new RestoreRefused({ reason: `its pod did not run within ${POD_RUNNING_TIMEOUT_MS / 1000} s`, resumed: false }))),
        Effect.catchTag("KubeApiError", (error) => Effect.fail(new RestoreRefused({ reason: error.message, resumed: false }))),
      );
      // Its workspace attached, its sandbox paused, before it runs.
      yield* attach(running).pipe(Effect.mapError((error) => new RestoreRefused({ reason: `its workspace was not attached: ${error.message}`, resumed: false })));
      const containerIds = (running.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^[a-z]+:\/\//u, "")] : []));
      const podIPs = (running.status?.podIPs ?? []).map(({ ip }) => ip);
      // The policies that select it, which its node's rules for it must name.
      const policies = yield* kube.list("networking.k8s.io/v1", "NetworkPolicy", pool.namespace, "").pipe(
        Effect.map((found) => (found as V1NetworkPolicy[]).filter(({ spec }) => selects(spec?.podSelector, running.metadata?.labels ?? {})).flatMap(({ metadata }) => metadata?.name ?? [])),
        Effect.mapError((error) => new RestoreRefused({ reason: error.message, resumed: false })),
      );
      // Paused where its policies are not in force (504), or where the bucket fails (502), which
      // may pass; of any other failure its sandbox may have run; paused, too, where the snapshot
      // was taken live and holds the session no more (410), which no try restores.
      yield* agents.resume(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, podIPs, containerIds, policies, snapshot }).pipe(
        Effect.mapError((error) => {
          const status = error._tag === "NodeAgentError" ? error.status : undefined;
          return new RestoreRefused({ reason: error.message, resumed: status !== 502 && status !== 504 && status !== 410, ...(status === 410 ? { stale: true } : {}) });
        }),
      );
      const now = new Date(yield* Clock.currentTimeMillis).toISOString();
      yield* kube.patchPodStatus(pool.namespace, name, { status: { conditions: [{ type: RESUMED_CONDITION, status: "True", lastTransitionTime: now }] } }).pipe(
        Effect.mapError((error) => new RestoreRefused({ reason: `its readiness could not be set: ${error.message}`, resumed: true })),
      );
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

  const restoring = (pool: Pool, workspace: string, mode: "none" | "full"): Restoring => {
    /** What this bind's maker restored, which its pod's readying resumes. */
    let restored: { readonly pod: string; readonly node: string; readonly snapshot: string; readonly manifest: Manifest; readonly started: number } | null = null;

    const make: MakePod = (labels, annotations) =>
      Effect.gen(function*() {
        if (!snapshots || pool.namespace !== mainNamespace) return null;
        const started = yield* Clock.currentTimeMillis;
        const record = yield* agents.released(ref(pool, workspace)).pipe(Effect.result);
        const read = record._tag === "Success" ? yield* agents.current(ref(pool, workspace)).pipe(Effect.result) : record;
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
        if (able._tag === "Failure") return yield* deferOr(pool, workspace, "unreadable", able.failure.message);
        if (able.success.nodes.length === 0) {
          const none = `no node that takes its pod has gVisor ${manifest.release} and every CPU feature it was saved with`;
          if (able.success.unready) return yield* deferOr(pool, workspace, "node", `${none}: one that has them is not Ready`);
          return yield* (able.success.answered ? fallBack(pool, workspace, "node", none) : deferOr(pool, workspace, "node", `${none} of the nodes whose agents answered`));
        }
        // Each node tried in turn: one that cannot restore it (409) leaves the others; one whose bucket or agent fails may pass.
        const failures: (NodeAgentError | KubeApiError)[] = [];
        for (const node of able.success.nodes) {
          const prepared = yield* agents.prepare(node, ref(pool, workspace)).pipe(Effect.result);
          if (prepared._tag === "Failure") {
            failures.push(prepared.failure);
            yield* Effect.logWarning(prepared.failure.message);
            continue;
          }
          // What the agent prepared, which a compaction committed since may have made the one compacted from it.
          const from = prepared.success.manifest;
          yield* Effect.logInfo(`restoring ${named(pool, workspace)} on ${node} from its snapshot ${from.snapshot}, taken at ${from.takenAt} under gVisor ${from.release}`);
          const { [FREE_LABEL]: _free, ...own } = base.metadata?.labels ?? {};
          const pod = yield* kube.create<V1Pod>({
            apiVersion: "v1",
            kind: "Pod",
            metadata: {
              generateName: `${pool.name}-restore-`,
              namespace: pool.namespace,
              labels: { ...own, ...labels },
              // Left paused, until its node's agent sees its egress confined (resumeRestored).
              annotations: { ...base.metadata?.annotations, ...annotations, [RESTORE_ANNOTATION]: prepared.success.imagePath, [RESTORE_PAUSED_ANNOTATION]: "true" },
            },
            spec: {
              ...base.spec,
              containers: base.spec?.containers ?? [],
              runtimeClassName: kept.runtimeClassName,
              readinessGates: [...(base.spec?.readinessGates ?? []), { conditionType: RESUMED_CONDITION }],
              affinity: { ...base.spec?.affinity, nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [node] }] }] } } },
            },
          }).pipe(Effect.mapError((error) => new BindStepFailed({ reason: `its restore's pod could not be made: ${error.message}` })));
          restored = { pod: pod.metadata?.name ?? "", node, snapshot: from.snapshot, manifest, started };
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
        const done = yield* resumeRestored(pool, workspace, of.pod, of.node, of.snapshot, attach).pipe(Effect.result);
        if (done._tag === "Success") {
          const now = yield* Clock.currentTimeMillis;
          deferredSince.delete(named(pool, workspace));
          restores.add(1, { "alasio.sandbox.restore.outcome": "restored" });
          restoreDuration.record((now - of.started) / 1000);
          // It moves on from its snapshot, whose images go on as the layers of its next; released into none, until its next release.
          yield* agents.spend(ref(pool, workspace), of.manifest.snapshot).pipe(Effect.catch((error) => Effect.logWarning(`could not spend the snapshot of ${named(pool, workspace)}, which runs on from it: ${error.message}`)));
          yield* agents.setReleased(ref(pool, workspace), null).pipe(Effect.catch((error) => Effect.logWarning(`could not clear ${named(pool, workspace)}'s record of its release: ${error.message}`)));
          return;
        }
        const { reason: why, resumed, stale } = done.failure;
        // Its sandbox never having run, its restore is abandoned; one of a snapshot that holds the session no more falls back at once.
        if (resumed) yield* fallBack(pool, workspace, "restore", why);
        else {
          yield* abandon(pool, workspace, of.pod, of.node);
          yield* (stale ? fallBack(pool, workspace, "stale", why) : deferOr(pool, workspace, "restore", why)).pipe(Effect.ignore);
        }
        return yield* new BindStepFailed({ reason: `its restore did not come back: ${why}` });
      }).pipe(withLogScope("snapshots"));

    return { make, ready };
  };

  return { save, restoring };
});
