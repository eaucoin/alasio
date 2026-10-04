/**
 * Applying an installation of alasio: its objects, every one labelled as the
 * installation's, server-side applied as alasio in the order what runs needs them: CRDs,
 * waited for until established; namespaces; what pods run as and with; Jobs, each run to
 * completion afresh (the Neon setup, which makes the Secrets the stack mounts); then the
 * workloads. What the installation had and no longer has is deleted, but for what holds
 * data, and its workloads are waited for until they run.
 *
 * Removing it deletes what it has, by its label, but for what holds data unless that
 * goes too.
 */
import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect } from "effect";

import { RELEASE } from "../manifests/common.ts";
import { describeRef, type Kind, kind, type KindName, KubeApi, type KubeApiError, type ObjectRef, refOf } from "./api.ts";
import { awaitGone, awaitReady, type NotReadyInTime, type RolloutFailed, selectorOf, type WaitOptions } from "./rollout.ts";

/** The label every object of an installation carries, whose value is the installation: what prune and removal find its objects by. */
const INSTALLATION_LABEL = "alasio.dev/installation";
/** The installation's objects, as a label selector. */
export const INSTALLATION_SELECTOR = selectorOf({ [INSTALLATION_LABEL]: RELEASE });

/** The kinds alasio makes, in the order it applies them; each phase's objects are applied together. */
const PHASES: readonly (readonly KindName[])[] = [
  ["CustomResourceDefinition"],
  ["Namespace"],
  ["ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Role", "RoleBinding", "ConfigMap", "PersistentVolumeClaim", "Service", "NetworkPolicy"],
  ["Job"],
  ["Deployment", "StatefulSet", "CronJob"],
];

/** The kinds alasio makes, in the order it applies them. */
const MADE_KINDS: readonly Kind[] = PHASES.flat().map(kind);

/**
 * What holds data, which neither a prune nor a removal deletes unless asked to: volumes,
 * the namespaces sessions' volumes are in, and the CRD whose deletion would delete every
 * Sandbox, and with them their volumes.
 */
const HOLDS_DATA: ReadonlySet<string> = new Set(["PersistentVolumeClaim", "Namespace", "CustomResourceDefinition"]);

/** Whether `ref` holds data (HOLDS_DATA). */
const holdsData = (ref: ObjectRef): boolean => HOLDS_DATA.has(ref.kind);

/** `object`, labelled as the installation's. */
export function labelled(object: KubernetesObject): KubernetesObject {
  return { ...object, metadata: { ...object.metadata, labels: { ...object.metadata?.labels, [INSTALLATION_LABEL]: RELEASE } } };
}

const key = (ref: ObjectRef): string => `${ref.apiVersion}/${ref.kind}/${ref.namespace ?? ""}/${ref.name}`;

/** How applying an installation fails. */
export type ApplyError = KubeApiError | NotReadyInTime | RolloutFailed;

/** The installation's objects of `kinds`, as it has them now. */
const installed = (kinds: readonly Kind[]): Effect.Effect<ObjectRef[], KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    Effect.forEach(kinds, (each) => kube.list(each, { labelSelector: INSTALLATION_SELECTOR }), { concurrency: 4 }).pipe(
      Effect.map((lists) => lists.flat().map(refOf)),
    ));

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

/** Deletes the installation's objects that are not `desired`, keeping what holds data. */
const prune = Effect.fnUntraced(function*(desired: readonly KubernetesObject[]): Effect.fn.Return<void, KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const wanted = new Set(desired.map((object) => key(refOf(object))));
  // Workloads first, what they need after.
  const stale = (yield* installed(MADE_KINDS.toReversed())).filter((ref) => !wanted.has(key(ref)));
  for (const ref of stale) {
    if (holdsData(ref)) {
      yield* Effect.logInfo(`kept ${describeRef(ref)}, no longer part of alasio, as it holds data; alasio uninstall --purge deletes it`);
    } else {
      yield* kube.remove(ref);
      yield* Effect.logInfo(`deleted ${describeRef(ref)}, no longer part of alasio`);
    }
  }
});

/** Applies the installation `objects` are, deletes what it no longer has, and waits until its workloads run. */
export const applyInstallation = Effect.fnUntraced(function*(
  objects: readonly KubernetesObject[],
  options: WaitOptions,
): Effect.fn.Return<void, ApplyError, KubeApi> {
  const kube = yield* KubeApi;
  const desired = objects.map(labelled);
  const unplaced = desired.filter((object) => !MADE_KINDS.some((each) => each.kind === object.kind && each.apiVersion === object.apiVersion));
  if (unplaced.length > 0) return yield* Effect.die(new Error(`alasio does not apply ${unplaced.map((object) => describeRef(refOf(object))).join(", ")}`));
  const ofPhase = (phase: readonly KindName[]) => desired.filter((object) => phase.some((name) => name === object.kind));
  yield* Effect.logInfo(`applying ${desired.length} objects to ${kube.server}`);
  for (const phase of PHASES) {
    const batch = ofPhase(phase);
    if (phase.includes("Job")) {
      for (const job of batch) yield* runJob(job, options);
      continue;
    }
    yield* Effect.forEach(batch, (object) => kube.apply(object), { concurrency: 8, discard: true });
    if (phase.includes("CustomResourceDefinition")) yield* awaitReady(batch.map(refOf), options);
  }
  yield* prune(desired);
  yield* awaitReady(ofPhase(["Deployment", "StatefulSet"]).map(refOf), options);
});

/** Deletes the installation's objects, but for what holds data unless `purge`, and with `purge` waits until that is gone: what it deleted. */
export const removeInstallation = Effect.fnUntraced(function*(
  { purge }: { readonly purge: boolean },
  options: WaitOptions,
): Effect.fn.Return<readonly ObjectRef[], KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> {
  const kube = yield* KubeApi;
  const found = yield* installed(MADE_KINDS.toReversed());
  const removed = found.filter((ref) => purge || !holdsData(ref));
  yield* Effect.forEach(removed, (ref) => kube.remove(ref), { concurrency: 8, discard: true });
  for (const ref of found.filter((each) => !removed.includes(each))) yield* Effect.logInfo(`kept ${describeRef(ref)}, as it holds data`);
  if (purge) yield* awaitGone(removed.filter(holdsData), options);
  return removed;
});
