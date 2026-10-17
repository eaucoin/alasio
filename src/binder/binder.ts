/**
 * The binder: a key bound to a pod of its pool, by ./bindings.ts's Leases and labels, for as
 * long as it has work, and released after. It knows nothing of what its pools run.
 *
 * A bind of a key bound already finds its pod; one whose binding another binder has under
 * way waits for it; one whose binding is this call's gets a pod by the pool's way to (a claim
 * of a free pod, unless the caller makes one itself, as a restore does), names it the
 * Lease's holder, and waits for it to serve. Where the pool has no free pod, the bind waits,
 * up to `waitMs`, counted waiting meanwhile, so the pool is scaled for it;
 * one that gives up releases its Lease. A Lease naming a pod gone, as one deleted by hand or
 * evicted, is released, and the key bound anew.
 */
import { randomUUID } from "node:crypto";

import type { V1Pod } from "@kubernetes/client-node";
import { Clock, Context, Effect, Fiber, Layer, Schema, Semaphore } from "effect";

import { hasStatus, type KubeApiError, KubeClient } from "../kube/client.ts";
import { BINDING_TIMEOUT_MS, type BindingLease, boundLabels, boundPod, claim, holdLease, KEY_ANNOTATION, markReleasing, type Pool, readLease, RELEASING_ANNOTATION, releaseLease, renewLease, serving, stale, takeLease, unmarkReleasing } from "./bindings.ts";

/** How often a bind looks again, for a free pod, a binding another has under way, or its pod serving. */
export const BIND_POLL_MS = 250;
/**
 * How often a bind renews its Lease while its binding is under way, by default: a sixth of
 * ./bindings.ts's BINDING_TIMEOUT_MS, so it is taken over only after five renewals in a row
 * have not reached the API server, as when its binder has ended, however long the binding
 * takes.
 */
export const RENEW_MS = 10_000;

/** What Binder.layer is given: how long a bind waits, and how often a binding under way is renewed, and after how long unrenewed it is stale. */
export interface BinderOptions {
  readonly waitMs: number;
  readonly renewMs?: number;
  readonly staleMs?: number;
}

/** A key bound: its binding, its Lease's uid; its pod, and the pod's address. */
export interface Bound {
  readonly binding: string;
  readonly pod: string;
  readonly address: string;
}

/** A bind that gave up: no pod of its pool came free, or its pod did not serve, within its wait. */
export class BindTimeout extends Schema.TaggedError<BindTimeout>()("BindTimeout", {
  key: Schema.String,
  pool: Schema.String,
  waited: Schema.Literals(["free pod", "binding", "pod serving"]),
}) {
  override get message(): string {
    return `${this.key} was not bound in pool ${this.pool} in time: it waited for ${this.waited === "free pod" ? "a free pod" : this.waited === "binding" ? "another binder's binding of it" : "its pod to serve"}`;
  }
}

/**
 * Makes the pod of a binding itself, rather than claim one: given the labels and annotations
 * the pod is to carry from the first, the pod made.
 */
export type MakePod = (labels: Readonly<Record<string, string>>, annotations: Readonly<Record<string, string>>) => Effect.Effect<V1Pod, KubeApiError | BindStepFailed>;

/**
 * Readies the pod got for a binding before it is the binding's, as by attaching what it
 * serves: given the pod, and the binding, its Lease's uid.
 */
export type PreparePod = (pod: V1Pod, binding: string) => Effect.Effect<void, BindStepFailed>;

/** What a bind is given beyond its pool and key: a pod's maker, where it makes its own, and what readies the pod. */
export interface BindOptions {
  readonly make?: MakePod | undefined;
  readonly prepare?: PreparePod | undefined;
}

/** A step of a bind its caller gives, making or readying its pod, failed: why. */
export class BindStepFailed extends Schema.TaggedError<BindStepFailed>()("BindStepFailed", {
  reason: Schema.String,
}) {
  override get message(): string {
    return this.reason;
  }
}

export class Binder extends Context.Service<Binder, {
  /** The key bound in the pool, as the module says. */
  readonly bind: (pool: Pool, key: string, options?: BindOptions) => Effect.Effect<Bound, KubeApiError | BindTimeout | BindStepFailed>;
  /**
   * Releases the key's binding, binds of it waiting meanwhile: `before` run, as a checkpoint
   * of its pod, then its pod deleted, then its Lease. Whether it was released: false where it
   * is not bound, or another binder is binding or releasing it.
   */
  readonly release: <E = never>(pool: Pool, key: string, before?: (bound: { readonly pod: string; readonly binding: string }) => Effect.Effect<void, E>) => Effect.Effect<boolean, KubeApiError | E>;
  /** How many binds of the pool wait for a free pod now, in this binder. */
  readonly waiting: (pool: Pool) => Effect.Effect<number>;
}>()("alasio/binder/Binder") {
  /** On the KubeClient, each bind waiting up to `waitMs`. */
  static readonly layer = ({ waitMs, renewMs = RENEW_MS, staleMs = BINDING_TIMEOUT_MS }: BinderOptions): Layer.Layer<Binder, never, KubeClient> =>
    Layer.effect(
      Binder,
      Effect.gen(function*() {
        const kube = yield* KubeClient;
        const provide = <A, E>(effect: Effect.Effect<A, E, KubeClient>) => Effect.provideService(effect, KubeClient, kube);
        // This replica, as its Leases under way name it.
        const binder = randomUUID();
        const waits = new Map<string, number>();
        const poolKey = (pool: Pool) => `${pool.namespace}/${pool.name}`;

        /** Counts a bind of `pool` waiting for a free pod while `effect` runs. */
        const waitingFor = <A, E>(pool: Pool, effect: Effect.Effect<A, E>): Effect.Effect<A, E> =>
          Effect.acquireUseRelease(
            Effect.sync(() => void waits.set(poolKey(pool), (waits.get(poolKey(pool)) ?? 0) + 1)),
            () => effect,
            () => Effect.sync(() => void waits.set(poolKey(pool), (waits.get(poolKey(pool)) ?? 1) - 1)),
          );

        /** The binding `binding`'s pod `name` once it serves, by `deadline`. */
        const servingBy = (pool: Pool, key: string, binding: string, name: string, deadline: number): Effect.Effect<Bound, KubeApiError | BindTimeout> =>
          Effect.gen(function*() {
            for (;;) {
              const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
              if (pod && serving(pod) && pod.status?.podIP) return { binding, pod: name, address: pod.status.podIP };
              if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindTimeout({ key, pool: pool.name, waited: "pod serving" });
              yield* Effect.sleep(BIND_POLL_MS);
            }
          });

        /**
         * A pod for the binding whose Lease this bind took: one made, or a free one claimed,
         * waited for while `lost` says the Lease is still this bind's. One bound to the key
         * already, as by a binder that ended after it claimed it, readied or not, is deleted
         * first: a pod serves one binding, as it was got for it.
         */
        const podFor = (pool: Pool, key: string, make: MakePod | undefined, deadline: number, lost: () => boolean): Effect.Effect<V1Pod, KubeApiError | BindTimeout | BindStepFailed> =>
          Effect.gen(function*() {
            const already = yield* provide(boundPod(pool, key));
            if (already?.metadata?.name) yield* kube.remove("v1", "Pod", pool.namespace, already.metadata.name);
            if (make) return yield* make(boundLabels(pool, key), { [KEY_ANNOTATION]: key });
            const claimed = yield* provide(claim(pool, key));
            if (claimed) return claimed;
            return yield* waitingFor(pool, Effect.gen(function*() {
              for (;;) {
                yield* Effect.sleep(BIND_POLL_MS);
                if (lost()) return yield* new BindTimeout({ key, pool: pool.name, waited: "binding" });
                const pod = yield* provide(claim(pool, key));
                if (pod) return pod;
                if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindTimeout({ key, pool: pool.name, waited: "free pod" });
              }
            }));
          });

        /**
         * Runs `work` with the Lease `lease` this binder holds renewed every `renewMs` the while,
         * however long it takes, as a restore or a checkpoint may take minutes, so that another
         * binder takes it over only once this one has stopped renewing it; `work` is told
         * whether a renewal found it taken over meanwhile. Then the renewals are stopped, none
         * under way, and what `work` came to is given with the Lease as last renewed, or null
         * where it was taken over; where `work` failed, `failed` is given the Lease as last
         * renewed first.
         */
        const renewing = <A, E>(
          lease: BindingLease,
          work: (lost: () => boolean) => Effect.Effect<A, E>,
          failed: (lease: BindingLease) => Effect.Effect<void>,
        ): Effect.Effect<{ readonly value: A; readonly lease: BindingLease | null }, E> =>
          Effect.gen(function*() {
            let held = lease;
            let lost = false;
            const turn = yield* Semaphore.make(1);
            const renew = Effect.gen(function*() {
              const again = yield* provide(renewLease(held)).pipe(Effect.orElseSucceed(() => held));
              if (again) held = again;
              else lost = true;
            }).pipe(Semaphore.withPermit(turn));
            const renewer = yield* Effect.forkChild(Effect.forever(Effect.andThen(Effect.sleep(renewMs), renew)));
            const value = yield* work(() => lost).pipe(
              Effect.ensuring(Fiber.interrupt(renewer).pipe(Semaphore.withPermit(turn))),
              Effect.tapError(() => failed(held)),
            );
            return { value, lease: lost ? null : held };
          });

        /**
         * Binds `key` with the Lease `taken` this bind took: a pod got for it (podFor) and
         * readied (`prepare`), its Lease renewed the while (renewing), then the pod named its
         * holder. The Lease and the pod's name, or null where another binder took the Lease over
         * meanwhile, which then gets a pod of its own. A bind that gets no pod, or cannot ready
         * the one it got, which is then deleted, lets the Lease go.
         */
        const underWay = (pool: Pool, key: string, taken: BindingLease, { make, prepare }: BindOptions, deadline: number): Effect.Effect<{ readonly lease: BindingLease; readonly name: string } | null, KubeApiError | BindTimeout | BindStepFailed> =>
          Effect.gen(function*() {
            const readied = (lost: () => boolean) =>
              Effect.gen(function*() {
                const pod = yield* podFor(pool, key, make, deadline, lost);
                if (prepare) {
                  yield* prepare(pod, taken.metadata?.uid ?? "").pipe(
                    Effect.tapError(() => kube.remove("v1", "Pod", pool.namespace, pod.metadata?.name ?? "").pipe(Effect.ignore)),
                  );
                }
                return pod;
              });
            // A bind that gets no pod readied lets the Lease go, as last renewed, for the next to take.
            const { value: pod, lease } = yield* renewing(taken, readied, (held) => provide(releaseLease(held)).pipe(Effect.ignore));
            const name = pod.metadata?.name ?? "";
            const holding = lease ? yield* provide(holdLease(lease, name)) : null;
            return holding ? { lease: holding, name } : null;
          });

        /**
         * Releases `key`'s binding: its Lease marked releasing, by an update conditioned on the
         * version read, so of two releases one alone goes on, and binds of the key wait for it;
         * `before` run, its Lease renewed the while; then its pod deleted, then its Lease.
         * Where `before` fails, the binding is kept, no longer marked releasing, and the failure
         * given. Whether it was released; false where it is not bound, or is being bound or
         * released by another binder.
         */
        const releaseBinding = <E>(pool: Pool, key: string, before: (bound: { readonly pod: string; readonly binding: string }) => Effect.Effect<void, E>): Effect.Effect<boolean, KubeApiError | E> =>
          Effect.gen(function*() {
            const lease = yield* provide(readLease(pool, key));
            const pod = lease?.spec?.holderIdentity;
            if (!lease || !pod || lease.metadata?.annotations?.[RELEASING_ANNOTATION]) return false;
            const marked = yield* provide(markReleasing(lease, binder));
            if (!marked) return false;
            const { lease: held } = yield* renewing(marked, () => before({ pod, binding: marked.metadata?.uid ?? "" }), (renewed) => provide(unmarkReleasing(renewed)).pipe(Effect.ignore));
            yield* kube.remove("v1", "Pod", pool.namespace, pod);
            if (held) yield* provide(releaseLease(held)).pipe(Effect.catchIf(hasStatus(409), () => Effect.void));
            return true;
          });

        const bind = (pool: Pool, key: string, options: BindOptions = {}): Effect.Effect<Bound, KubeApiError | BindTimeout | BindStepFailed> =>
          Effect.gen(function*() {
            const deadline = (yield* Clock.currentTimeMillis) + waitMs;
            for (;;) {
              const taken = yield* provide(takeLease(pool, key, binder, staleMs));
              if (taken.taken) {
                const bound = yield* underWay(pool, key, taken.lease, options, deadline);
                if (bound === null) continue;
                return yield* servingBy(pool, key, bound.lease.metadata?.uid ?? "", bound.name, deadline);
              }
              const holder = taken.lease.spec?.holderIdentity;
              if (taken.lease.metadata?.annotations?.[RELEASING_ANNOTATION]) {
                // Being released: waited for, so the key binds after, from what the release left;
                // one whose releaser stopped renewing it, ended, its pod and Lease let go.
                if (stale(taken.lease, staleMs, yield* Clock.currentTimeMillis)) {
                  yield* kube.remove("v1", "Pod", pool.namespace, holder ?? "");
                  yield* provide(releaseLease(taken.lease)).pipe(Effect.catchIf(hasStatus(409), () => Effect.void));
                  continue;
                }
              } else if (holder) {
                const pod = yield* provide(boundPod(pool, key));
                if (pod?.metadata?.name === holder && !pod.metadata.deletionTimestamp) return yield* servingBy(pool, key, taken.lease.metadata?.uid ?? "", holder, deadline);
                // Its pod gone, or going: its Lease let go, as it was read, and the key bound anew.
                yield* provide(releaseLease(taken.lease)).pipe(Effect.catchIf(hasStatus(409), () => Effect.void));
                continue;
              }
              if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindTimeout({ key, pool: pool.name, waited: "binding" });
              yield* Effect.sleep(BIND_POLL_MS);
            }
          });

        return Binder.of({
          bind,
          release: (pool, key, before = () => Effect.void) => releaseBinding(pool, key, before),
          waiting: (pool) => Effect.sync(() => waits.get(poolKey(pool)) ?? 0),
        });
      }),
    );
}
