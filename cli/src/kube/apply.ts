/**
 * Applying an installation of alasio: its objects, every one labelled as the
 * installation's, server-side applied as alasio in the order what runs needs them: CRDs,
 * waited for until established; namespaces, and the cluster's classes of pods and
 * volumes and its CSI drivers; what pods run as and with, and where the API server
 * reaches its extensions (APIServices, webhooks); Jobs, each run to completion afresh
 * (the Neon setup, which makes the Secrets the stack mounts); the workloads; then what
 * scales and sizes them, for KEDA and VPA, whose CRDs are established by then, with what
 * KEDA's scalers authenticate as. What the
 * installation had and no longer has is deleted, but for what holds data and what serves
 * volumes that remain, and its workloads are waited for until they run; a scaler it no
 * longer has goes first, before the workloads it scaled are applied.
 *
 * Removing it deletes what it has, by its label, but for what holds data unless that
 * goes too, and what serves volumes that remain. What a controller of the installation
 * holds with a finalizer (KEDA's ScaledObjects) is deleted first, and waited for, while
 * the controller runs to let go of it, or let go of here where it has not within a
 * minute, as it does not run. A CSI driver deletes its volumes' data,
 * so the driver, what it needs to (VOLUME_DRIVER_LABEL), and the StorageClasses of its
 * volumes go only once those volumes have: removing the data lets go of them first.
 */
import type { KubernetesObject, V1PersistentVolume } from "@kubernetes/client-node";
import { Duration, Effect } from "effect";

import { RELEASE } from "../manifests/common.ts";
import { describeRef, FIELD_MANAGER, hasStatus, type Kind, kind, type KindName, KubeApi, type KubeApiError, type ObjectRef, refOf } from "./api.ts";
import { awaitGone, awaitReady, awaitReclaimed, type NotReadyInTime, type RolloutFailed, selectorOf, type WaitOptions } from "./rollout.ts";

/** The label every object of an installation carries, whose value is the installation: what prune and removal find its objects by. */
const INSTALLATION_LABEL = "alasio.dev/installation";
/** The installation's objects, as a label selector. */
export const INSTALLATION_SELECTOR = selectorOf({ [INSTALLATION_LABEL]: RELEASE });

/**
 * The label of what a CSI driver needs to delete its volumes' data, beside its CSIDriver,
 * whose value is the driver's name: its controller and node service, what they run as,
 * and what they reach the data with (its metadata engine, its credentials).
 */
export const VOLUME_DRIVER_LABEL = "alasio.dev/volume-driver";

/** The kinds alasio makes, in the order it applies them; each phase's objects are applied together. */
const PHASES: readonly (readonly KindName[])[] = [
  ["CustomResourceDefinition"],
  ["Namespace", "PriorityClass", "StorageClass", "CSIDriver"],
  [
    "ServiceAccount",
    "ClusterRole",
    "ClusterRoleBinding",
    "Role",
    "RoleBinding",
    "ConfigMap",
    "PersistentVolumeClaim",
    "Service",
    "NetworkPolicy",
    "PodDisruptionBudget",
    "APIService",
    "MutatingWebhookConfiguration",
    "ValidatingWebhookConfiguration",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
  ],
  ["Job"],
  ["Deployment", "StatefulSet", "DaemonSet", "CronJob"],
  ["TriggerAuthentication", "ScaledObject", "VerticalPodAutoscaler"],
];

/** The kinds alasio makes, in the order it applies them. */
const MADE_KINDS: readonly Kind[] = PHASES.flat().map(kind);

/** The kinds of the workloads alasio waits for until they run. */
export const WORKLOADS: readonly KindName[] = ["Deployment", "StatefulSet", "DaemonSet"];

/**
 * What holds data, which neither a prune nor a removal deletes unless asked to: volumes,
 * the namespaces sessions' volumes are in, and CRDs, whose deletion deletes every object
 * of theirs: every Sandbox, and with them their volumes.
 */
const HOLDS_DATA: ReadonlySet<string> = new Set(["PersistentVolumeClaim", "Namespace", "CustomResourceDefinition"]);

/** Whether `ref` holds data (HOLDS_DATA). */
const holdsData = (ref: ObjectRef): boolean => HOLDS_DATA.has(ref.kind);

/** What a controller of the installation finalizes: KEDA lets go of a ScaledObject once it has stopped scaling it. */
const FINALIZED: ReadonlySet<string> = new Set(["ScaledObject"]);

/** The kinds of FINALIZED. */
const FINALIZED_KINDS: readonly Kind[] = MADE_KINDS.filter(({ kind }) => FINALIZED.has(kind));

/** How long a controller is given to let go of what it finalizes, past which it is taken not to run; the wait's timeout where that is shorter. */
const LET_GO = Duration.minutes(1);

/**
 * Deletes those of `objects` a controller finalizes (FINALIZED), and waits until they are
 * gone. One its controller has not let go of within LET_GO, as where it does not run (KEDA
 * gone, or failing), is let go of here: nothing else would.
 */
const removeFinalized = Effect.fnUntraced(function*(objects: readonly KubernetesObject[], options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  const finalized = objects.filter((object) => FINALIZED.has(object.kind ?? "")).map(refOf);
  if (finalized.length === 0) return;
  yield* Effect.forEach(finalized, (ref) => kube.remove(ref), { concurrency: 8, discard: true });
  const letGo = Duration.min(LET_GO, Duration.fromInputUnsafe(options.timeout));
  yield* awaitGone(finalized, { ...options, timeout: letGo }).pipe(Effect.catchTag("NotReadyInTime", () =>
    Effect.gen(function*() {
      const left = (yield* Effect.forEach(finalized, (ref) => kube.get(ref))).filter((object) => object !== null).map(refOf);
      yield* Effect.forEach(left, (ref) =>
        Effect.andThen(
          Effect.logInfo(`letting go of ${describeRef(ref)}, as its controller has not within ${Duration.toSeconds(letGo)}s`),
          kube.patch(ref, { metadata: { finalizers: null } }, FIELD_MANAGER),
        ).pipe(Effect.catchIf(hasStatus(404), () => Effect.void)), { discard: true });
      yield* awaitGone(left, options);
    })));
});

/** The installation's objects of `kinds` that are not `desired`. */
const staleOf = (desired: readonly KubernetesObject[], kinds: readonly Kind[]): Effect.Effect<KubernetesObject[], KubeApiError, KubeApi> => {
  const wanted = new Set(desired.map((object) => key(refOf(object))));
  return Effect.map(installed(kinds), (objects) => objects.filter((object) => !wanted.has(key(refOf(object)))));
};

const SANDBOX = kind("Sandbox");
const PERSISTENT_VOLUME = kind("PersistentVolume");
const PERSISTENT_VOLUME_CLAIM = kind("PersistentVolumeClaim");

/**
 * Of `volumes`, those `object` serves: a CSI driver's, by its CSIDriver or
 * VOLUME_DRIVER_LABEL, those it provisioned; a StorageClass's, those of its class.
 */
function servedBy(object: KubernetesObject, volumes: readonly V1PersistentVolume[]): V1PersistentVolume[] {
  const name = object.metadata?.name;
  const driver = object.kind === "CSIDriver" ? name : object.metadata?.labels?.[VOLUME_DRIVER_LABEL];
  return volumes.filter(({ spec }) => (driver !== undefined && spec?.csi?.driver === driver) || (object.kind === "StorageClass" && spec?.storageClassName === name));
}

/** `volumes` as people read them: their names. */
const namesOf = (volumes: readonly V1PersistentVolume[]): string => volumes.map(({ metadata }) => metadata?.name ?? "").join(", ");

/** Whether `object` is the installation's, by its label. */
export const ofInstallation = (object: KubernetesObject): boolean => object.metadata?.labels?.[INSTALLATION_LABEL] === RELEASE;

/** `object`, labelled as the installation's. */
export function labelled(object: KubernetesObject): KubernetesObject {
  return { ...object, metadata: { ...object.metadata, labels: { ...object.metadata?.labels, [INSTALLATION_LABEL]: RELEASE } } };
}

const key = (ref: ObjectRef): string => `${ref.apiVersion}/${ref.kind}/${ref.namespace ?? ""}/${ref.name}`;

/** How applying an installation fails. */
export type ApplyError = KubeApiError | NotReadyInTime | RolloutFailed;

/** The installation's objects of `kinds`, as it has them now: none of a kind whose CRD the cluster lacks. */
const installed = (kinds: readonly Kind[]): Effect.Effect<KubernetesObject[], KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    Effect.forEach(kinds, (each) => kube.list(each, { labelSelector: INSTALLATION_SELECTOR }).pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed([]))), { concurrency: 4 }).pipe(
      Effect.map((lists) => lists.flat()),
    ));

/** The cluster's volumes. */
const persistentVolumes: Effect.Effect<readonly V1PersistentVolume[], KubeApiError, KubeApi> = Effect.flatMap(KubeApi, (kube) =>
  kube.list<V1PersistentVolume>(PERSISTENT_VOLUME));

/** Runs `job` to completion afresh: one of its name that ran before is deleted first, as a Job's pod template cannot change, and it is deleted once done. */
const runJob = Effect.fnUntraced(function*(job: KubernetesObject, options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  const ref = refOf(job);
  if (yield* kube.get(ref)) {
    yield* kube.remove(ref);
    yield* awaitGone([ref], options);
  }
  yield* Effect.logInfo(`running ${describeRef(ref)}`);
  yield* kube.apply(job);
  yield* awaitReady([ref], options);
  yield* kube.remove(ref);
});

/**
 * Deletes the installation's objects that are not `desired`, keeping what holds data and
 * what serves volumes that remain; what a controller finalizes first, until it is gone.
 */
const prune = Effect.fnUntraced(function*(desired: readonly KubernetesObject[], options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  // Workloads first, what they need after.
  const stale = yield* staleOf(desired, MADE_KINDS.toReversed());
  yield* removeFinalized(stale, options);
  const volumes = stale.length > 0 ? yield* persistentVolumes : [];
  for (const object of stale.filter(({ kind }) => !FINALIZED.has(kind ?? ""))) {
    const ref = refOf(object);
    const served = servedBy(object, volumes);
    if (holdsData(ref)) {
      yield* Effect.logInfo(`kept ${describeRef(ref)}, no longer part of alasio, as it holds data; alasio uninstall --purge deletes it`);
    } else if (served.length > 0) {
      yield* Effect.logInfo(`kept ${describeRef(ref)}, no longer part of alasio, as volumes it serves remain (${namesOf(served)}); alasio up deletes it once they are gone`);
    } else {
      yield* kube.remove(ref);
      yield* Effect.logInfo(`deleted ${describeRef(ref)}, no longer part of alasio`);
    }
  }
});

/** The manager a workload's replicas are handed over to, from alasio, as its scaler comes to own them (handOverReplicas). */
export const HANDOVER_MANAGER = "alasio-handover";

/** A workload as its replicas' ownership is read: its replicas, and who manages its fields. */
interface Owned extends KubernetesObject {
  readonly spec?: { readonly replicas?: number };
}

/** Whether alasio's own applies own `object`'s replicas, as its managed fields say. */
function ownsReplicas({ metadata }: KubernetesObject): boolean {
  return (metadata?.managedFields ?? []).some(({ manager, operation, fieldsV1 }) => {
    const spec: unknown = (fieldsV1 as Record<string, unknown> | undefined)?.["f:spec"];
    return manager === FIELD_MANAGER && operation === "Apply" && typeof spec === "object" && spec !== null && "f:replicas" in spec;
  });
}

/**
 * Hands the replicas of a workload of `objects` rendered without them, as a scaler moves
 * them now (../manifests/common.ts replicasOf), over from alasio's applies, which set them
 * until now, to a manager of their own, at the count it runs: an apply that stops giving a
 * field its only manager gave deletes it, and a workload would drop to one replica until
 * its scaler raised it again. The scaler takes them over from there.
 */
const handOverReplicas = Effect.fnUntraced(function*(objects: readonly KubernetesObject[]): Effect.fn.Return<void, KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const scaled = objects.filter((object) => (object.kind === "Deployment" || object.kind === "StatefulSet") && (object as Owned).spec?.replicas === undefined);
  yield* Effect.forEach(scaled, Effect.fnUntraced(function*(object) {
    const ref = refOf(object);
    const live = yield* kube.get<Owned>(ref);
    const replicas = live?.spec?.replicas;
    if (!live || replicas === undefined || !ownsReplicas(live)) return;
    yield* Effect.logInfo(`handing the replicas of ${describeRef(ref)} over to its scaler, at ${replicas}`);
    const metadata = { name: ref.name, ...(ref.namespace ? { namespace: ref.namespace } : {}) };
    const handedOver: Owned = { apiVersion: ref.apiVersion, kind: ref.kind, metadata, spec: { replicas } };
    yield* kube.apply(handedOver, HANDOVER_MANAGER);
  }), { concurrency: 8, discard: true });
});

/** A PriorityClass, as its value and preemption policy are read. */
interface PriorityClass extends KubernetesObject {
  readonly value?: number;
  readonly preemptionPolicy?: string;
}

/**
 * Deletes the PriorityClasses of `objects` whose value or preemption policy the cluster's
 * differs in, which cannot change, so they are made anew. A pod keeps the priority it
 * was given as it was made.
 */
const replaceChangedClasses = Effect.fnUntraced(function*(objects: readonly KubernetesObject[], options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  const changed = yield* Effect.filter(objects.filter(({ kind }) => kind === "PriorityClass") as PriorityClass[], (wanted) =>
    Effect.map(kube.get<PriorityClass>(refOf(wanted)), (live) =>
      live !== null && (live.value !== wanted.value || (live.preemptionPolicy ?? "PreemptLowerPriority") !== (wanted.preemptionPolicy ?? "PreemptLowerPriority"))));
  for (const { metadata } of changed) yield* Effect.logInfo(`replacing PriorityClass ${metadata?.name}, whose value and preemption cannot change`);
  yield* Effect.forEach(changed, (object) => kube.remove(refOf(object)), { discard: true });
  yield* awaitGone(changed.map(refOf), options);
});

/** The workloads of `objects`. */
const workloadsOf = (objects: readonly KubernetesObject[]): ObjectRef[] =>
  objects.filter((object) => WORKLOADS.some((name) => name === object.kind)).map(refOf);

/** Applies `objects` in the order of PHASES, as the module says, without waiting for their workloads. */
const applyPhases = Effect.fnUntraced(function*(objects: readonly KubernetesObject[], options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  const unplaced = objects.filter((object) => !MADE_KINDS.some((each) => each.kind === object.kind && each.apiVersion === object.apiVersion));
  if (unplaced.length > 0) return yield* Effect.die(new Error(`alasio does not apply ${unplaced.map((object) => describeRef(refOf(object))).join(", ")}`));
  yield* Effect.logInfo(`applying ${objects.length} objects to ${kube.server}`);
  for (const phase of PHASES) {
    const batch = objects.filter((object) => phase.some((name) => name === object.kind));
    if (phase.includes("Job")) {
      for (const job of batch) yield* runJob(job, options);
      continue;
    }
    if (phase.includes("PriorityClass")) yield* replaceChangedClasses(batch, options);
    if (phase.includes("Deployment")) yield* handOverReplicas(batch);
    yield* Effect.forEach(batch, (object) => kube.apply(object), { concurrency: 8, discard: true });
    if (phase.includes("CustomResourceDefinition")) yield* awaitReady(batch.map(refOf), options);
  }
});

/** Applies the installation `objects` are, deletes what it no longer has, and waits until its workloads run. */
export const applyInstallation = Effect.fnUntraced(function*(
  objects: readonly KubernetesObject[],
  options: WaitOptions,
): Effect.fn.Return<void, ApplyError, KubeApi> {
  const desired = objects.map(labelled);
  // A scaler no longer wanted goes before the workloads are applied, as it would set the
  // replicas they are given back to its own.
  yield* removeFinalized(yield* staleOf(desired, FINALIZED_KINDS), options);
  yield* applyPhases(desired, options);
  yield* prune(desired, options);
  yield* awaitReady(workloadsOf(desired), options);
});

/**
 * Applies `objects`, of no installation (a branch environment's, which a prune of the
 * installation leaves be), and waits until their workloads run.
 */
export const applyObjects = Effect.fnUntraced(function*(objects: readonly KubernetesObject[], options: WaitOptions): Effect.fn.Return<void, ApplyError, KubeApi> {
  yield* applyPhases(objects, options);
  yield* awaitReady(workloadsOf(objects), options);
});

/**
 * Lets go of the volumes the installation's CSI drivers and StorageClasses (of `found`,
 * its objects) serve that are claimed in its namespaces, while the drivers can still
 * delete their data: deletes the Sandboxes in its namespaces, whose pods use them, and
 * waits until they are gone; then deletes the claims, and waits until each volume is
 * deleted, or released and kept as its reclaim policy says.
 */
const releaseVolumes = Effect.fnUntraced(function*(
  found: readonly KubernetesObject[],
  options: WaitOptions,
): Effect.fn.Return<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> {
  const kube = yield* KubeApi;
  const namespaces = found.flatMap((object) => (object.kind === "Namespace" && object.metadata?.name ? [object.metadata.name] : []));
  const volumes = (yield* persistentVolumes).filter((volume) =>
    namespaces.includes(volume.spec?.claimRef?.namespace ?? "") && found.some((object) => servedBy(object, [volume]).length > 0)
  );
  if (volumes.length === 0) return;
  yield* Effect.logInfo(`deleting alasio's Sandboxes, and the claims of the volumes ${namesOf(volumes)}, while what serves them can still delete their data`);
  // None where the Sandbox CRD is gone already.
  const sandboxes = (yield* Effect.forEach(namespaces, (namespace) =>
    kube.list(SANDBOX, { namespace }).pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed([]))))).flat().map(refOf);
  yield* Effect.forEach(sandboxes, (ref) => kube.remove(ref), { concurrency: 8, discard: true });
  yield* awaitGone(sandboxes, options);
  const claims = volumes.map(({ spec }): ObjectRef => ({ ...PERSISTENT_VOLUME_CLAIM, namespace: spec?.claimRef?.namespace, name: spec?.claimRef?.name ?? "" }));
  yield* Effect.forEach(claims, (ref) => kube.remove(ref), { concurrency: 8, discard: true });
  yield* awaitReclaimed(volumes.map(refOf), options);
});

/**
 * Deletes the installation's objects, but for what serves volumes that remain, and what
 * holds data unless `purge`; with `purge` it lets go of the volumes its CSI drivers and
 * StorageClasses serve first, and waits until what holds data is gone: what it deleted.
 */
export const removeInstallation = Effect.fnUntraced(function*(
  { purge }: { readonly purge: boolean },
  options: WaitOptions,
): Effect.fn.Return<readonly ObjectRef[], KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> {
  const kube = yield* KubeApi;
  yield* removeFinalized(yield* installed(MADE_KINDS.filter(({ kind }) => FINALIZED.has(kind))), options);
  const found = yield* installed(MADE_KINDS.toReversed());
  if (purge) yield* releaseVolumes(found, options);
  const volumes = yield* persistentVolumes;
  const removed = found.filter((object) => (purge || !holdsData(refOf(object))) && servedBy(object, volumes).length === 0).map(refOf);
  yield* Effect.forEach(removed, (ref) => kube.remove(ref), { concurrency: 8, discard: true });
  for (const object of found) {
    const ref = refOf(object);
    const served = servedBy(object, volumes);
    if (served.length > 0) yield* Effect.logInfo(`kept ${describeRef(ref)}, as volumes it serves remain (${namesOf(served)})`);
    else if (!purge && holdsData(ref)) yield* Effect.logInfo(`kept ${describeRef(ref)}, as it holds data`);
  }
  if (purge) yield* awaitGone(removed.filter(holdsData), options);
  return removed;
});
