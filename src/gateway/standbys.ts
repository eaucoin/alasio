/**
 * Standby pods of main's sessions' pools: one kept on each node a restore ran on, of the pool's
 * template and the gVisor release of the last restore there whose shim restores into a standby
 * (../node-agent/names.ts restoresIntoStandby), booted ahead of the next one as a
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
 * So a standby costs its node a session's room: it requests a whole session's memory, the
 * template's, though an empty sandbox holds far less (some 18 MiB in its pod, and some 14 MiB of
 * shims), as alasio sets no memory budget for standbys yet, nor lowers a standby's requests until
 * it is claimed. A node keeps one, at most, of the last restore's template and release there, a
 * standby of another taken off as it is made.
 * One is made STANDBY_AFTER_MS after a restore on its node, out of the way of the restore's
 * first execs, which share its node's processors with its boot; one that ended, as a node that
 * stopped ends one, is made anew. A restore claims one by an update conditioned on the version
 * read, as a free pod is claimed, taking its standby label off and giving it the binding's
 * labels and annotations, so two restores never claim one; one claimed is restored into
 * once, as the shim refuses a second, and goes with its binding, as a restore's pod does.
 * None is kept on a node cordoned, not Ready, or gone: every STANDBY_SWEEP_MS, those on such a
 * node are deleted, as a drain would find them there, and as one aimed at a node deleted would
 * stay pending for ever, which the cluster collects only once bound; and those that ended.
 *
 * Every standby is the gateway's of the moment. One that starts deletes every standby there,
 * those of a gateway before it, of templates or releases that may be kept no more, and those
 * of a gateway rolled in beside it alike, as it cannot tell them apart: none a restore claimed,
 * as a claim takes its standby label off, and none claimed meanwhile, as a deletion is of the
 * version read (a claim changes it); a gateway beside it makes its own anew after its next
 * restore. One that stops deletes those it made alone, not those of a gateway rolled in beside
 * it.
 */
import { randomBytes } from "node:crypto";

import type { V1Node, V1Pod, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, FiberMap, Schedule, Schema, type Scope } from "effect";

import { FREE_LABEL, type Pool } from "../binder/names.ts";
import { hasStatus, KubeClient } from "../kube/client.ts";
import { RESUMED_CONDITION, SESSION_TEMPLATE_ANNOTATION, STANDBY_ANNOTATION, STANDBY_LABEL, standbySocket } from "../kube/sandbox-shape.ts";
import { withLogScope } from "../shared/log.ts";
import { ATTACH_VOLUME, ownDirectory, restorePodName } from "./names.ts";

/** How long after a restore on a node its next standby is made there. */
export const STANDBY_AFTER_MS = 10_000;

/** How often the standbys are looked at, those that ended and those on a node cordoned, not Ready or gone deleted. */
export const STANDBY_SWEEP_MS = 30_000;

/** A standby whose claim's answer was lost, and which could not be read again to tell whether it was made: the restore cannot go on. */
export class StandbyUnsure extends Schema.TaggedError<StandbyUnsure>()("StandbyUnsure", { pod: Schema.String, reason: Schema.String }) {
  override get message(): string {
    return `whether the standby ${this.pod} was claimed cannot be told, as it could not be read again: ${this.reason}`;
  }
}

/** The gateway's standbys of main's sessions; see makeStandbys. */
export interface Standbys {
  /**
   * The standby on `node` of `template` under the RuntimeClass `runtimeClassName`, claimed for a
   * binding of `pool`, `labels` and `annotations` its own, as the module says; null where none
   * there runs, with its address, each of its containers started. One whose claim's answer was
   * lost is read again, and is the binding's where it carries them; one that cannot be is
   * StandbyUnsure, as it may be the binding's.
   */
  readonly claim: (
    pool: Pool,
    node: string,
    template: V1PodTemplateSpec,
    runtimeClassName: string,
    labels: Readonly<Record<string, string>>,
    annotations: Readonly<Record<string, string>>,
  ) => Effect.Effect<V1Pod | null, StandbyUnsure>;
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

/** How the standbys are kept: each made `afterMs` after a restore on its node (STANDBY_AFTER_MS), and looked at every `sweepMs` (STANDBY_SWEEP_MS). */
export interface StandbysOptions {
  readonly afterMs?: number;
  readonly sweepMs?: number;
}

/** The standbys of main's sessions' namespace `namespace`, as the module says and `options` time them, the gateway's while the scope lasts. */
export const makeStandbys = Effect.fnUntraced(function*(namespace: string, { afterMs = STANDBY_AFTER_MS, sweepMs = STANDBY_SWEEP_MS }: StandbysOptions = {}): Effect.fn.Return<Standbys, never, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;

  /** The standbys of `namespace`, on `node` where named. */
  const standbysOn = (node?: string) =>
    Effect.map(kube.list("v1", "Pod", namespace, node === undefined ? STANDBY_LABEL : `${STANDBY_LABEL}=${node}`), (found) => found as V1Pod[]);

  /**
   * The standbys this gateway made, by name, each with when it was made: those it deletes as it stops, as another gateway,
   * rolled in beside it, keeps its own. One deleted or claimed is forgotten, and one a sweep finds no more.
   */
  const made = new Map<string, number>();

  /** Deletes the standby `pod`, as of the version it was read at: one changed since, as a claim changes it, is left. */
  const remove = (pod: V1Pod, why: string) =>
    kube.remove("v1", "Pod", namespace, pod.metadata?.name ?? "", pod.metadata?.resourceVersion).pipe(
      Effect.andThen(Effect.sync(() => made.delete(pod.metadata?.name ?? ""))),
      Effect.andThen(Effect.logInfo(`deleted the standby ${namespace}/${pod.metadata?.name}: ${why}`)),
      Effect.catchIf(hasStatus(409), () => Effect.void),
    );

  /** Deletes the standbys of the namespace `which` says, `why`. */
  const clear = (which: (pod: V1Pod) => boolean, why: string) =>
    standbysOn().pipe(
      Effect.flatMap((found) => Effect.forEach(found.filter(which), (pod) => remove(pod, why), { discard: true })),
      Effect.catch((error) => Effect.logWarning(`could not delete the standbys of ${namespace}: ${error.message}`)),
      withLogScope("standbys"),
    );

  /**
   * Why `node` keeps no standby: it is gone, as its Node is, so that one aimed at it would stay pending for ever, which the
   * cluster collects only once bound; or it is cordoned or not Ready, as its Node says. Null where it keeps them, and where
   * its Node cannot be read, until it can be.
   */
  const keepsNoneOn = (node: string): Effect.Effect<string | null> =>
    kube.read("v1", "Node", "", node).pipe(
      Effect.map((read) => {
        const found = read as V1Node | null;
        if (found === null) return `its node ${node} is gone`;
        if (found.spec?.unschedulable === true) return `its node ${node} is cordoned`;
        return found.status?.conditions?.find(({ type }) => type === "Ready")?.status === "True" ? null : `its node ${node} is not Ready`;
      }),
      Effect.catch((error) => Effect.logWarning(`could not read whether node ${node} keeps standbys: ${error.message}`).pipe(Effect.as(null))),
    );

  /**
   * Deletes the standbys that ended, and those on a node that keeps none (keepsNoneOn), as one drained, cordoned, not Ready
   * or gone; and forgets those it made that it finds no more, deleted or claimed, made at least a sweep before it looked.
   */
  const sweep = Effect.gen(function*() {
    const looked = Date.now();
    const found = yield* standbysOn();
    const listed = new Set(found.map((pod) => pod.metadata?.name ?? ""));
    for (const [name, at] of made) if (!listed.has(name) && at < looked - sweepMs) made.delete(name);
    yield* Effect.forEach(
      Map.groupBy(found.filter((pod) => !pod.metadata?.deletionTimestamp), (pod) => pod.metadata?.labels?.[STANDBY_LABEL] ?? ""),
      ([node, there]) =>
        Effect.flatMap(keepsNoneOn(node), (why) =>
          Effect.forEach(there, (pod) => (why !== null ? remove(pod, why) : ended(pod) ? remove(pod, "it ended") : Effect.void), { discard: true })),
      { discard: true },
    );
  }).pipe(
    Effect.catch((error) => Effect.logWarning(`could not look at the standbys of ${namespace}: ${error.message}`)),
    withLogScope("standbys"),
  );

  // Every standby there, a gateway's beside this one's among them, as the module says.
  yield* clear(() => true, "a gateway starts, which keeps the standbys of none before it");
  yield* Effect.addFinalizer(() => clear((pod) => made.has(pod.metadata?.name ?? ""), "the gateway that made it stops"));
  yield* Effect.forkScoped(Effect.repeat(sweep, Schedule.spaced(sweepMs)));
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
        annotations: { ...template.metadata?.annotations, [STANDBY_ANNOTATION]: standbySocket(namespace, name) },
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
  ): Effect.Effect<V1Pod | null, StandbyUnsure> =>
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
        if (claimed._tag === "Success") {
          made.delete(pod.metadata?.name ?? "");
          return claimed.success;
        }
        yield* Effect.logWarning(`could not claim the standby ${namespace}/${pod.metadata?.name} on ${node}: ${claimed.failure.message}`);
        // Changed since it was read (409), as another restore claimed it: not this one's. Otherwise the update may have been
        // made and its answer lost: read again, it is this restore's where it carries its binding's labels and annotations.
        if (hasStatus(409)(claimed.failure)) continue;
        const read = (yield* kube.read("v1", "Pod", namespace, pod.metadata?.name ?? "").pipe(
          Effect.retry({ schedule: Schedule.spaced("200 millis"), times: 3 }),
          Effect.mapError((error) => new StandbyUnsure({ pod: pod.metadata?.name ?? "", reason: error.message })),
        )) as V1Pod | null;
        const carried = (read?.metadata?.labels ?? {}) as Record<string, string>;
        const noted = (read?.metadata?.annotations ?? {}) as Record<string, string>;
        if (read !== null && Object.entries(labels).every(([key, value]) => carried[key] === value) && Object.entries(annotations).every(([key, value]) => noted[key] === value)) {
          made.delete(pod.metadata?.name ?? "");
          return read;
        }
      }
      return null;
    }).pipe(
      Effect.catchIf((error) => error._tag === "KubeApiError", (error) => Effect.logWarning(`could not look for a standby of ${pool.namespace}/${pool.name} on ${node}: ${error.message}`).pipe(Effect.as(null))),
      withLogScope("standbys"),
    );

  const keep = (pool: Pool, node: string, template: V1PodTemplateSpec, runtimeClassName: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      yield* Effect.sleep(afterMs);
      const found = (yield* standbysOn(node)).filter((pod) => !pod.metadata?.deletionTimestamp);
      // None on a node cordoned, not Ready or gone since the restore, as a drain would find it there.
      const why = yield* keepsNoneOn(node);
      if (why !== null) {
        yield* Effect.forEach(found, (pod) => remove(pod, why), { discard: true });
        return;
      }
      // One at most on the node: of the last restore's template and release, one that ended made anew.
      const kept = found.find((pod) => isOf(pod, template, runtimeClassName) && !ended(pod));
      for (const pod of found) {
        if (pod !== kept) yield* remove(pod, ended(pod) ? "it ended" : `a restore on ${node} ran of another template or gVisor release`);
      }
      if (kept) return;
      const standby = standbyOf(pool, node, template, runtimeClassName);
      // Its own before it is asked for, so that one made whose answer was lost is deleted as the gateway stops too.
      made.set(standby.metadata?.name ?? "", Date.now());
      yield* kube.create<V1Pod>(standby);
      yield* Effect.logInfo(`made the standby ${namespace}/${standby.metadata?.name} on ${node}, under ${runtimeClassName}`);
    }).pipe(
      Effect.catch((error) => Effect.logWarning(`could not keep a standby of ${pool.namespace}/${pool.name} on ${node}: ${error.message}`)),
      withLogScope("standbys"),
      // The last restore's on the node, another's being kept there given up.
      FiberMap.run(keeping, node),
      Effect.asVoid,
    );

  return { claim, keep };
});
