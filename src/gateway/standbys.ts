/**
 * Standby pods of main's sessions' pools: one kept on each node a restore ran on, of the pool's
 * template and the gVisor release of the last restore there, booted ahead of the next one as a
 * standby (STANDBY_ANNOTATION): its sandbox and its containers created, none of them started,
 * as the fork's shim keeps them, so that kubelet runs it, with its address, while it runs
 * nothing. A restore of that template and release on that node claims it rather than make a
 * pod (./snapshots.ts), and its node's agent restores the snapshot into it, as runsc restores
 * a pod made for it, without the pod's creation, its sandbox's boot and kubelet's steps before
 * it runs (../node-agent/standby.ts).
 *
 * A standby is a restore's pod in all but its binding, its image and its directory: the pod
 * the gateway would make of the template on that node, under the release's RuntimeClass,
 * named as a restore's pod is, ready once resumed; labelled its pool's and the standby of its
 * node (STANDBY_LABEL), its directory on its node made by kubelet, empty, the workspace attached
 * there only once a restore has claimed it. Its requests are its template's, which the
 * scheduler counts against its node as it would count the session restored into it, so the
 * node's room for sessions is told as it will be once the standby is claimed: a node full keeps
 * its standby pending, and a standby claimed is a session's like any other.
 *
 * A node keeps one, at most, until the memory a node keeps for standbys is set: of the last
 * restore's template and release there, a standby of another taken off as it is made.
 * One is made STANDBY_AFTER_MS after a restore on its node, out of the way of the restore's
 * first execs, which share its node's processors with its boot; one that ended, as a node that
 * stopped ends one, is made anew. A restore claims one by an update conditioned on the version
 * read, as a free pod is claimed, taking its standby label off and giving it the binding's
 * labels and annotations, so two restores never claim one; one claimed is restored into
 * once, as the shim refuses a second, and goes with its binding, as a restore's pod does.
 *
 * Every standby is the gateway's of the moment: those there as it starts, which a gateway
 * before it made, of templates or releases that may be kept no more, are deleted then, and
 * those it made are deleted as it stops.
 */
import { randomBytes } from "node:crypto";

import type { V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, FiberMap, type Scope } from "effect";

import { FREE_LABEL, type Pool } from "../binder/names.ts";
import { hasStatus, KubeClient } from "../kube/client.ts";
import { RESUMED_CONDITION, SESSION_TEMPLATE_ANNOTATION, STANDBY_ANNOTATION, STANDBY_LABEL, STANDBY_SOCKETS } from "../kube/sandbox-shape.ts";
import { withLogScope } from "../shared/log.ts";
import { ATTACH_VOLUME, ownDirectory, restorePodName } from "./names.ts";

/** How long after a restore on a node its next standby is made there. */
export const STANDBY_AFTER_MS = 10_000;

/** The gateway's standbys of main's sessions; see makeStandbys. */
export interface Standbys {
  /**
   * The standby on `node` of `template` under the RuntimeClass `runtimeClassName`, claimed for a
   * binding of `pool`, `labels` and `annotations` its own, as the module says; null where none
   * there runs, with its address, each of its containers started.
   */
  readonly claim: (
    pool: Pool,
    node: string,
    template: V1PodTemplateSpec,
    runtimeClassName: string,
    labels: Readonly<Record<string, string>>,
    annotations: Readonly<Record<string, string>>,
  ) => Effect.Effect<V1Pod | null>;
  /** Keeps a standby on `node`, where a restore of `pool` ran, of `template` under `runtimeClassName`: made in the background, after a while, as the module says. */
  readonly keep: (pool: Pool, node: string, template: V1PodTemplateSpec, runtimeClassName: string) => Effect.Effect<void>;
}

/** Whether `pod` is a standby of `template` under `runtimeClassName`. */
const isOf = (pod: V1Pod, template: V1PodTemplateSpec, runtimeClassName: string): boolean =>
  pod.spec?.runtimeClassName === runtimeClassName && pod.metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION] === template.metadata?.annotations?.[SESSION_TEMPLATE_ANNOTATION];

/** Whether `pod` ended, or a container of it did: as a node that stopped ends it, never to run again. */
const ended = (pod: V1Pod): boolean =>
  pod.status?.phase === "Failed" || pod.status?.phase === "Succeeded" || (pod.status?.containerStatuses ?? []).some(({ state }) => state?.terminated !== undefined);

/** Whether `pod` runs, with its address, each of its containers started, and is not being deleted: a standby a restore may claim. */
const standing = (pod: V1Pod): boolean =>
  !pod.metadata?.deletionTimestamp && pod.status?.phase === "Running" && (pod.status.podIPs?.length ?? 0) > 0 &&
  (pod.status.containerStatuses ?? []).length > 0 && (pod.status.containerStatuses ?? []).every(({ state }) => state?.running !== undefined);

/** The standbys of main's sessions' namespace `namespace`, as the module says, the gateway's while the scope lasts, each made `afterMs` after a restore on its node. */
export const makeStandbys = Effect.fnUntraced(function*(namespace: string, afterMs: number = STANDBY_AFTER_MS): Effect.fn.Return<Standbys, never, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;

  /** The standbys of `namespace`, on `node` where named. */
  const standbysOn = (node?: string) =>
    Effect.map(kube.list("v1", "Pod", namespace, node === undefined ? STANDBY_LABEL : `${STANDBY_LABEL}=${node}`), (found) => found as V1Pod[]);

  /** Deletes the standby `pod`, as of the version it was read at: one changed since, as a claim changes it, is left. */
  const remove = (pod: V1Pod, why: string) =>
    kube.remove("v1", "Pod", namespace, pod.metadata?.name ?? "", pod.metadata?.resourceVersion).pipe(
      Effect.andThen(Effect.logInfo(`deleted the standby ${namespace}/${pod.metadata?.name}: ${why}`)),
      Effect.catchIf(hasStatus(409), () => Effect.void),
    );

  /** Deletes every standby of the namespace, `why`. */
  const clear = (why: string) =>
    standbysOn().pipe(
      Effect.flatMap((found) => Effect.forEach(found, (pod) => remove(pod, why), { discard: true })),
      Effect.catch((error) => Effect.logWarning(`could not delete the standbys of ${namespace}: ${error.message}`)),
      withLogScope("standbys"),
    );

  yield* clear("a gateway before this one made it");
  yield* Effect.addFinalizer(() => clear("the gateway that made it stops"));
  /** The standby being kept on each node, by its node: interrupted as the scope closes, before the standbys are deleted. */
  const keeping = yield* FiberMap.make<string>();

  /** A standby on `node`, of `template` under `runtimeClassName`, of `pool`, as the module says. */
  const standbyOf = (pool: Pool, node: string, template: V1PodTemplateSpec, runtimeClassName: string): V1Pod => {
    const name = restorePodName(pool.name, randomBytes(4).readUInt32BE().toString(36).padStart(7, "0").slice(-7));
    const { [FREE_LABEL]: _free, ...own } = template.metadata?.labels ?? {};
    const spec = ownDirectory({ containers: [], ...template.spec }, namespace, name);
    return {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name,
        namespace,
        labels: { ...own, [STANDBY_LABEL]: node },
        annotations: { ...template.metadata?.annotations, [STANDBY_ANNOTATION]: `${STANDBY_SOCKETS}/${namespace}/${name}.sock` },
      },
      spec: {
        ...spec,
        // Its own directory made by kubelet as it starts, empty until a restore claims it, which attaches its workspace there.
        volumes: (spec.volumes ?? []).map((volume) => (volume.name === ATTACH_VOLUME && volume.hostPath ? { ...volume, hostPath: { ...volume.hostPath, type: "DirectoryOrCreate" } } : volume)),
        // Never started again, as a restore's pod is not: its sandbox, once restored into, would be a fresh one.
        restartPolicy: "Never",
        runtimeClassName,
        readinessGates: [...(template.spec?.readinessGates ?? []), { conditionType: RESUMED_CONDITION }],
        affinity: { ...template.spec?.affinity, nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: [node] }] }] } } },
      },
    };
  };

  const claim = (
    pool: Pool,
    node: string,
    template: V1PodTemplateSpec,
    runtimeClassName: string,
    labels: Readonly<Record<string, string>>,
    annotations: Readonly<Record<string, string>>,
  ): Effect.Effect<V1Pod | null> =>
    Effect.gen(function*() {
      const found = (yield* standbysOn(node)).filter((pod) => isOf(pod, template, runtimeClassName) && standing(pod));
      for (const pod of found) {
        // Relabelled by an update conditioned on the version read, as the binder claims a free pod (../binder/bindings.ts claim).
        const { [STANDBY_LABEL]: _standby, ...own } = pod.metadata?.labels ?? {};
        const claimed = yield* kube.replace<V1Pod>({
          ...pod,
          // A listed object's own, as a list's items come without them.
          apiVersion: "v1",
          kind: "Pod",
          metadata: { ...pod.metadata, labels: { ...own, ...labels }, annotations: { ...pod.metadata?.annotations, ...annotations } },
        }).pipe(Effect.result);
        if (claimed._tag === "Success") return claimed.success;
        yield* Effect.logWarning(`could not claim the standby ${namespace}/${pod.metadata?.name} on ${node}: ${claimed.failure.message}`);
      }
      return null;
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not look for a standby of ${pool.namespace}/${pool.name} on ${node}: ${error.message}`).pipe(Effect.as(null))),
      withLogScope("standbys"),
    );

  const keep = (pool: Pool, node: string, template: V1PodTemplateSpec, runtimeClassName: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      yield* Effect.sleep(afterMs);
      const found = (yield* standbysOn(node)).filter((pod) => !pod.metadata?.deletionTimestamp);
      // One at most on the node: of the last restore's template and release, one that ended made anew.
      const kept = found.find((pod) => isOf(pod, template, runtimeClassName) && !ended(pod));
      for (const pod of found) {
        if (pod !== kept) yield* remove(pod, ended(pod) ? "it ended" : `a restore on ${node} ran of another template or gVisor release`);
      }
      if (kept) return;
      const made = yield* kube.create<V1Pod>(standbyOf(pool, node, template, runtimeClassName));
      yield* Effect.logInfo(`made the standby ${namespace}/${made.metadata?.name} on ${node}, under ${runtimeClassName}`);
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not keep a standby of ${pool.namespace}/${pool.name} on ${node}: ${error.message}`)),
      withLogScope("standbys"),
      // The last restore's on the node, another's being kept there given up.
      FiberMap.run(keeping, node),
      Effect.asVoid,
    );

  return { claim, keep };
});
