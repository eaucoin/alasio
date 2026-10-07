/**
 * Applying an installation of alasio: its objects, every one labelled as the
 * installation's, server-side applied as alasio in the order what runs needs them: CRDs,
 * waited for until established; namespaces, and the cluster's classes of pods and
 * volumes and its CSI drivers; what pods run as and with; Jobs, each run to completion
 * afresh (the Neon setup, which makes the Secrets the stack mounts); then the workloads.
 * What the installation had and no longer has is deleted, but for what holds data and
 * what serves volumes that remain, and its workloads are waited for until they run.
 *
 * Removing it deletes what it has, by its label, but for what holds data unless that
 * goes too, and what serves volumes that remain. A CSI driver deletes its volumes' data,
 * so the driver, what it needs to (VOLUME_DRIVER_LABEL), and the StorageClasses of its
 * volumes go only once those volumes have: removing the data lets go of them first.
 */
import type { KubernetesObject, V1PersistentVolume } from "@kubernetes/client-node";
import { Effect } from "effect";

import { RELEASE } from "../manifests/common.ts";
import { describeRef, hasStatus, type Kind, kind, type KindName, KubeApi, type KubeApiError, type ObjectRef, refOf } from "./api.ts";
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
  ["ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ConfigMap", "PersistentVolumeClaim", "Service", "NetworkPolicy"],
  ["Job"],
  ["Deployment", "StatefulSet", "DaemonSet", "CronJob"],
];

/** The kinds alasio makes, in the order it applies them. */
const MADE_KINDS: readonly Kind[] = PHASES.flat().map(kind);

/** The kinds of the workloads alasio waits for until they run. */
export const WORKLOADS: readonly KindName[] = ["Deployment", "StatefulSet", "DaemonSet"];

/**
 * What holds data, which neither a prune nor a removal deletes unless asked to: volumes,
 * the namespaces sessions' volumes are in, and the CRD whose deletion would delete every
 * Sandbox, and with them their volumes.
 */
const HOLDS_DATA: ReadonlySet<string> = new Set(["PersistentVolumeClaim", "Namespace", "CustomResourceDefinition"]);

/** Whether `ref` holds data (HOLDS_DATA). */
const holdsData = (ref: ObjectRef): boolean => HOLDS_DATA.has(ref.kind);

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

/** `object`, labelled as the installation's. */
export function labelled(object: KubernetesObject): KubernetesObject {
  return { ...object, metadata: { ...object.metadata, labels: { ...object.metadata?.labels, [INSTALLATION_LABEL]: RELEASE } } };
}

const key = (ref: ObjectRef): string => `${ref.apiVersion}/${ref.kind}/${ref.namespace ?? ""}/${ref.name}`;

/** How applying an installation fails. */
export type ApplyError = KubeApiError | NotReadyInTime | RolloutFailed;

/** The installation's objects of `kinds`, as it has them now. */
const installed = (kinds: readonly Kind[]): Effect.Effect<KubernetesObject[], KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    Effect.forEach(kinds, (each) => kube.list(each, { labelSelector: INSTALLATION_SELECTOR }), { concurrency: 4 }).pipe(
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

/** Deletes the installation's objects that are not `desired`, keeping what holds data and what serves volumes that remain. */
const prune = Effect.fnUntraced(function*(desired: readonly KubernetesObject[]): Effect.fn.Return<void, KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const wanted = new Set(desired.map((object) => key(refOf(object))));
  // Workloads first, what they need after.
  const stale = (yield* installed(MADE_KINDS.toReversed())).filter((object) => !wanted.has(key(refOf(object))));
  const volumes = stale.length > 0 ? yield* persistentVolumes : [];
  for (const object of stale) {
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
  yield* applyPhases(desired, options);
  yield* prune(desired);
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
