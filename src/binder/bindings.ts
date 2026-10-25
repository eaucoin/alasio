/**
 * Bindings: a key, what a binding is for, bound to one pod of a pool, on the Kubernetes API
 * alone, so whatever binds holds no state of its own and runs as many replicas as it likes.
 *
 * A pool is a Deployment's pods, each labelled with the pool's name and free; the
 * Deployment's selector selects both labels. A binding is first a Lease, named by the key's
 * hash, which only one creator can make, so a key is never bound twice at once; then a pod
 * carrying the key's hash as a label and the key as an annotation, the Lease's holder. A pod
 * is either claimed, a free pod of the pool relabelled by an update conditioned on the
 * resourceVersion it was read at, so of two claims of one pod one alone succeeds, its free
 * label taken off, which takes it out of its Deployment's selector, so its ReplicaSet
 * replaces it and nothing that rolls out or scales the pool touches it again; or made, by
 * what binds, carrying the binding's labels from the first (as a restore's pod is).
 *
 * A Lease whose binding is under way names no holder; one whose binder ended before it
 * named one is taken over once it has gone unrenewed for a while, BINDING_TIMEOUT_MS by default. A pod
 * serves one binding in its life: claimed, bound, released, deleted, never returned to its
 * pool, nor claimed again whatever its labels come to say, so nothing of one key's can reach
 * the next's. Bound, it says what keeps a node autoscaler from removing its node while it is
 * (BUSY_ANNOTATIONS), as what it holds lives only in it until its release.
 */
import type { KubernetesObject, V1Pod } from "@kubernetes/client-node";
import { Clock, Effect } from "effect";

import { hasStatus, type KubeApiError, KubeClient } from "../kube/client.ts";
import { BINDING_LABEL, boundAnnotations, boundLabels, FREE_LABEL, freeLabels, KEY_ANNOTATION, keyHash, leaseName, type Pool, POOL_LABEL } from "./names.ts";

/** The annotation of a binding's Lease while it is released: the releasing binder. */
export const RELEASING_ANNOTATION = "alasio.dev/releasing";

/** How long a binding under way may go unrenewed before another binder takes it over, by default. */
export const BINDING_TIMEOUT_MS = 60_000;

/**
 * A binding's Lease, as the API takes it: its times in its MicroTime format, microseconds and
 * all, which client-node's model, a Date, sends in milliseconds, and the API refuses.
 */
export interface BindingLease extends KubernetesObject {
  readonly spec?: {
    readonly holderIdentity?: string;
    readonly renewTime?: string;
    readonly leaseDurationSeconds?: number;
  };
}

/** `ms`, as the API's MicroTime. */
const microTime = (ms: number): string => `${new Date(ms).toISOString().slice(0, -1)}000Z`;

const selector = (labels: Readonly<Record<string, string>>): string => Object.entries(labels).map(([label, value]) => `${label}=${value}`).join(",");

/** Whether `pod` runs, its containers started, and is not being deleted: free to be claimed, as it waits for what it is to serve. */
export const running = (pod: V1Pod): boolean => !pod.metadata?.deletionTimestamp && pod.status?.phase === "Running";

/**
 * Whether a container of `pod` ended, or was started again in it, as one whose rootfs failed
 * under it does: a free pod so serves no binding, as what binds takes a bound pod whose container
 * ended or started again for broken (src/gateway/sessions.ts, src/gateway/harnesses.ts hostEnded),
 * and is never claimed, but deleted, for its Deployment to make another (reconcile).
 */
export const restarted = (pod: V1Pod): boolean =>
  (pod.status?.containerStatuses ?? []).some(({ restartCount, state, lastState }) => restartCount > 0 || state?.terminated !== undefined || lastState?.terminated !== undefined);

/** Whether `pod` runs ready, and is not being deleted: serving its binding. */
export const serving = (pod: V1Pod): boolean => running(pod) && (pod.status?.conditions ?? []).some(({ type, status }) => type === "Ready" && status === "True");

/** A Lease as the API holds it, by its key: null where there is none. */
export const readLease = Effect.fnUntraced(function*(pool: Pool, key: string): Effect.fn.Return<BindingLease | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  return (yield* kube.read("coordination.k8s.io/v1", "Lease", pool.namespace, leaseName(key))) as BindingLease | null;
});

/** The Lease of a binding of `key` under way, by `binder`, renewed now. */
const leaseUnderWay = (pool: Pool, key: string, binder: string, now: number, staleMs: number): BindingLease => ({
  apiVersion: "coordination.k8s.io/v1",
  kind: "Lease",
  metadata: { name: leaseName(key), namespace: pool.namespace, labels: { [POOL_LABEL]: pool.name, [BINDING_LABEL]: keyHash(key) }, annotations: { [KEY_ANNOTATION]: key, "alasio.dev/binder": binder } },
  spec: { renewTime: microTime(now), leaseDurationSeconds: Math.ceil(staleMs / 1000) },
});

/** Whether `lease` has gone unrenewed for `staleMs` by `now`. */
export const stale = (lease: BindingLease, staleMs: number, now: number): boolean => now - (Date.parse(lease.spec?.renewTime ?? "") || 0) >= staleMs;

/** What taking a key's Lease came to: taken, by this binder; or held by another binding, bound to a pod or under way. */
export type Taken = { readonly taken: true; readonly lease: BindingLease } | { readonly taken: false; readonly lease: BindingLease };

/**
 * Takes the Lease of `key`'s binding for `binder`: made, where there is none; or taken over,
 * by an update conditioned on the version read, where a binding under way names no holder
 * and has gone unrenewed for `staleMs`. Otherwise it is another binding's.
 */
export const takeLease = Effect.fnUntraced(function*(pool: Pool, key: string, binder: string, staleMs: number = BINDING_TIMEOUT_MS): Effect.fn.Return<Taken, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  for (;;) {
    const now = yield* Clock.currentTimeMillis;
    const made = yield* kube.create(leaseUnderWay(pool, key, binder, now, staleMs)).pipe(
      Effect.map((lease): Taken => ({ taken: true, lease })),
      Effect.catchIf(hasStatus(409), () => Effect.succeed(null)),
    );
    if (made) return made;
    const held = yield* readLease(pool, key);
    // Released between the two: made anew.
    if (!held) continue;
    if (held.spec?.holderIdentity || !stale(held, staleMs, now)) return { taken: false, lease: held };
    const fresh = leaseUnderWay(pool, key, binder, now, staleMs);
    const over = yield* kube.replace({ ...held, metadata: { ...held.metadata, annotations: fresh.metadata?.annotations ?? {} }, spec: fresh.spec ?? {} }).pipe(
      Effect.map((lease): Taken => ({ taken: true, lease })),
      Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
    );
    if (over) return over;
  }
});

/** Names `pod` the holder of the Lease `lease`, by an update conditioned on its version: the Lease, or null where another binder took it over meanwhile. */
export const holdLease = Effect.fnUntraced(function*(lease: BindingLease, pod: string): Effect.fn.Return<BindingLease | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  return yield* kube.replace({ ...lease, spec: { ...lease.spec, holderIdentity: pod } }).pipe(
    Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
  );
});

/** Marks the Lease `lease` of a binding released by `binder`, renewed now, by an update conditioned on its version: the Lease, or null where it changed meanwhile. */
export const markReleasing = Effect.fnUntraced(function*(lease: BindingLease, binder: string): Effect.fn.Return<BindingLease | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const now = yield* Clock.currentTimeMillis;
  return yield* kube.replace({ ...lease, metadata: { ...lease.metadata, annotations: { ...lease.metadata?.annotations, [RELEASING_ANNOTATION]: binder } }, spec: { ...lease.spec, renewTime: microTime(now) } }).pipe(
    Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
  );
});

/** The annotation of a binding's Lease saying when a request of its key last came, in ms: stamped by its binders, now and then. */
export const ACTIVE_ANNOTATION = "alasio.dev/active-at";

/** Stamps the Lease of `key`'s binding with a request of it at `now`; one gone, or changed meanwhile, is left. */
export const touchLease = Effect.fnUntraced(function*(pool: Pool, key: string, now: number): Effect.fn.Return<void, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  yield* kube.patch("coordination.k8s.io/v1", "Lease", pool.namespace, leaseName(key), { metadata: { annotations: { [ACTIVE_ANNOTATION]: String(now) } } }).pipe(
    Effect.catchIf(hasStatus(404), () => Effect.void),
    Effect.asVoid,
  );
});

/** When a request of the binding `lease` last came, as its binders stamped it; null where none ever did. */
export const activeAt = (lease: BindingLease): number | null => {
  const at = Number(lease.metadata?.annotations?.[ACTIVE_ANNOTATION]);
  return Number.isFinite(at) && at > 0 ? at : null;
};

/** The bindings of `pool` whose pods are named, as their Leases hold them. */
export const heldLeases = Effect.fnUntraced(function*(pool: Pool): Effect.fn.Return<readonly BindingLease[], KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const leases = (yield* kube.list("coordination.k8s.io/v1", "Lease", pool.namespace, `${POOL_LABEL}=${pool.name},${BINDING_LABEL}`)) as BindingLease[];
  return leases.filter(({ spec }) => spec?.holderIdentity);
});

/** Takes the releasing mark off the Lease `lease`, by an update conditioned on its version: the Lease, or null where it changed meanwhile. */
export const unmarkReleasing = Effect.fnUntraced(function*(lease: BindingLease): Effect.fn.Return<BindingLease | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const annotations = { ...lease.metadata?.annotations };
  delete annotations[RELEASING_ANNOTATION];
  return yield* kube.replace({ ...lease, metadata: { ...lease.metadata, annotations } }).pipe(
    Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
  );
});

/** Renews the Lease `lease` of a binding under way, or being released, by an update conditioned on its version: the Lease, or null where another binder took it over meanwhile. */
export const renewLease = Effect.fnUntraced(function*(lease: BindingLease): Effect.fn.Return<BindingLease | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const now = yield* Clock.currentTimeMillis;
  return yield* kube.replace({ ...lease, spec: { ...lease.spec, renewTime: microTime(now) } }).pipe(
    Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)),
  );
});

/**
 * Claims a free pod of `pool` that runs for `key`, ready or not, as one is ready only once
 * what it serves is readied for it (BindOptions' prepare): each tried in turn, from a random
 * one on, so binders claiming at once mostly try apart, relabelled by an update conditioned
 * on the version read; one another claimed first is passed over, and so is one a container of
 * which ended or started again (restarted). The pod claimed, or null where none was free.
 */
export const claim = Effect.fnUntraced(function*(pool: Pool, key: string): Effect.fn.Return<V1Pod | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  // A pod that was ever bound is never claimed, whatever its labels came to say since: it
  // serves one binding in its life, as a sandbox released can still reach what it was given.
  const free = ((yield* kube.list("v1", "Pod", pool.namespace, selector(freeLabels(pool)))) as V1Pod[]).filter((pod) =>
    running(pod) && !restarted(pod) && pod.metadata?.labels?.[BINDING_LABEL] === undefined && pod.metadata?.annotations?.[KEY_ANNOTATION] === undefined
  );
  const from = Math.floor(Math.random() * free.length);
  for (let n = 0; n < free.length; n += 1) {
    const pod = free[(from + n) % free.length]!;
    const labels = { ...pod.metadata?.labels };
    delete labels[FREE_LABEL];
    const claimed = yield* kube.replace<V1Pod>({
      ...pod,
      // A listed object's own, as a list's items come without them.
      apiVersion: "v1",
      kind: "Pod",
      metadata: { ...pod.metadata, labels: { ...labels, ...boundLabels(pool, key) }, annotations: { ...pod.metadata?.annotations, ...boundAnnotations(key) } },
    }).pipe(Effect.catchIf((error) => hasStatus(409)(error) || hasStatus(404)(error), () => Effect.succeed(null)));
    if (claimed) return claimed;
  }
  return null;
});

/** The pod bound to `key` in `pool`, serving or not, being deleted or not; null where there is none. */
export const boundPod = Effect.fnUntraced(function*(pool: Pool, key: string): Effect.fn.Return<V1Pod | null, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const pods = (yield* kube.list("v1", "Pod", pool.namespace, selector(boundLabels(pool, key)))) as V1Pod[];
  return pods.find((pod) => pod.metadata?.annotations?.[KEY_ANNOTATION] === key) ?? null;
});

/** Lets the Lease `lease` go, as it is: one changed since, by another binder, is kept (409). */
export const releaseLease = Effect.fnUntraced(function*(lease: BindingLease): Effect.fn.Return<void, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  yield* kube.remove("coordination.k8s.io/v1", "Lease", lease.metadata?.namespace ?? "", lease.metadata?.name ?? "", lease.metadata?.resourceVersion);
});

/** How many keys are bound in `namespace`, of every pool there. */
export const boundIn = Effect.fnUntraced(function*(namespace: string): Effect.fn.Return<number, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const leases = (yield* kube.list("coordination.k8s.io/v1", "Lease", namespace, POOL_LABEL)) as BindingLease[];
  return leases.filter(({ spec }) => spec?.holderIdentity).length;
});

/** How many of `pool`'s pods are free and running, none of whose containers ended or started again, and how many of its keys are bound. */
export const counts = Effect.fnUntraced(function*(pool: Pool): Effect.fn.Return<{ readonly free: number; readonly bound: number }, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const free = ((yield* kube.list("v1", "Pod", pool.namespace, selector(freeLabels(pool)))) as V1Pod[]).filter((pod) => running(pod) && !restarted(pod)).length;
  const leases = (yield* kube.list("coordination.k8s.io/v1", "Lease", pool.namespace, `${POOL_LABEL}=${pool.name}`)) as BindingLease[];
  return { free, bound: leases.filter(({ spec }) => spec?.holderIdentity).length };
});

/**
 * Sets `pool`'s bindings right where a binder ended between their steps: a bound pod no Lease
 * of its key names is deleted, as one claimed by a bind that then gave up; and a Lease naming
 * a pod gone is let go, as it was read, `before` run while it holds still, as what binds may
 * record of it, then told of (`letGo`), as a binding whose pod went while it was bound, which
 * what binds may say lost. A pod being deleted is not gone while it is there, as its binder may
 * be saving it as it ends. A binding under way, its pod not yet named, is left to its binder,
 * or to the next to take it over; and so is one being released, whose releaser deletes its pod
 * before its Lease, and tells of its release itself, unless it has stopped renewing it, so a
 * binding released is never told of as lost too. And a free pod a container of which ended or
 * started again (restarted), which no bind claims, is deleted as it was read, for its Deployment
 * to make another; one claimed since is left to its binder. What was set right, as names.
 */
export const reconcile = Effect.fnUntraced(function*(
  pool: Pool,
  letGo: (lease: BindingLease) => Effect.Effect<void> = () => Effect.void,
  before: (lease: BindingLease) => Effect.Effect<void> = () => Effect.void,
): Effect.fn.Return<{ readonly pods: string[]; readonly leases: string[]; readonly broken: string[] }, KubeApiError, KubeClient> {
  const kube = yield* KubeClient;
  const leases = (yield* kube.list("coordination.k8s.io/v1", "Lease", pool.namespace, `${POOL_LABEL}=${pool.name},${BINDING_LABEL}`)) as BindingLease[];
  const there = (yield* kube.list("v1", "Pod", pool.namespace, `${POOL_LABEL}=${pool.name},${BINDING_LABEL}`)) as V1Pod[];
  const pods = there.filter(({ metadata }) => !metadata?.deletionTimestamp);
  const held = new Map(leases.map((lease) => [lease.metadata?.labels?.[BINDING_LABEL] ?? "", lease]));
  const running = new Set(there.map(({ metadata }) => metadata?.name ?? ""));
  const orphans = pods.filter(({ metadata }) => {
    const lease = held.get(metadata?.labels?.[BINDING_LABEL] ?? "");
    return !lease || (lease.spec?.holderIdentity && lease.spec.holderIdentity !== metadata?.name);
  });
  const now = yield* Clock.currentTimeMillis;
  const released = (lease: BindingLease) => lease.metadata?.annotations?.[RELEASING_ANNOTATION] !== undefined && !stale(lease, BINDING_TIMEOUT_MS, now);
  const gone = leases.filter((lease) => lease.spec?.holderIdentity && !running.has(lease.spec.holderIdentity) && !released(lease));
  yield* Effect.forEach(orphans, ({ metadata }) => kube.remove("v1", "Pod", pool.namespace, metadata?.name ?? ""), { discard: true });
  // One changed since it was read, as by its binder, is no more this one's to let go, nor to tell of.
  const letGone = yield* Effect.forEach(gone, (lease) => before(lease).pipe(Effect.andThen(releaseLease(lease)), Effect.as(true), Effect.catchIf(hasStatus(409), () => Effect.succeed(false))));
  yield* Effect.forEach(gone.filter((_, n) => letGone[n]), letGo, { discard: true });
  const free = (yield* kube.list("v1", "Pod", pool.namespace, selector(freeLabels(pool)))) as V1Pod[];
  const broken = free.filter((pod) => !pod.metadata?.deletionTimestamp && restarted(pod) && pod.metadata?.labels?.[BINDING_LABEL] === undefined && pod.metadata?.annotations?.[KEY_ANNOTATION] === undefined);
  const deleted = yield* Effect.forEach(broken, ({ metadata }) =>
    kube.remove("v1", "Pod", pool.namespace, metadata?.name ?? "", metadata?.resourceVersion).pipe(Effect.as(true), Effect.catchIf(hasStatus(409), () => Effect.succeed(false))));
  return {
    pods: orphans.map(({ metadata }) => metadata?.name ?? ""),
    leases: gone.filter((_, n) => letGone[n]).map(({ metadata }) => metadata?.name ?? ""),
    broken: broken.filter((_, n) => deleted[n]).map(({ metadata }) => metadata?.name ?? ""),
  };
});
