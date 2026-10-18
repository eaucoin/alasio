/**
 * alasio's own pod, labelled busy for as long as its harnesses run a turn. Its namespace's
 * PodDisruptionBudget of BUSY_LABEL's pods (cli/src/manifests/disruption.ts) lets a drain
 * evict none of them, so it waits until the turn is done; BUSY_ANNOTATIONS keep a node
 * autoscaler from removing its node meanwhile, as a drain reads neither. The pods a turn
 * works in are the sandbox gateway's, which holds them while bound.
 *
 * The pod, held by several turns at once, is busy until the last lets go. A held pod has a
 * labeller of its own, in the background, which a hold or its release only wakes, so no
 * turn waits on the API server: one patch of the pod's at a time, the last after the
 * holds have changed saying what they come to, so a release and a new hold close together
 * leave the pod as the holds are. While held, it is labelled again every `refresh`, so a
 * label given up on is set by the next. A label that cannot be set or taken off within a
 * few seconds is logged, and the turn goes on.
 *
 * What alasio holds no more is unlabelled: as alasio stops, its pod, once its labellers
 * have stopped; and every `sweep`, from its start, each pod of its namespace labelled busy
 * that it does not hold, through a labeller of the pod's, as a hold's release is: what a
 * run of alasio's that ended without taking its label off left labelled, and a label whose
 * patch, given up on, landed after the pod was unlabelled.
 */
import { Config, Context, Deferred, type Duration, Effect, Layer, Option, Scope } from "effect";

import { BUSY_ANNOTATIONS, BUSY_LABEL } from "./busy-label.ts";
import { hasStatus, KubeClient } from "./client.ts";

/** A pod, by its namespace and name. */
export interface PodRef {
  readonly namespace: string;
  readonly name: string;
}

/** The merge patch that labels a pod busy, or, with `busy` false, takes the label and annotations off. */
export function busyPatch(busy: boolean): object {
  return {
    metadata: {
      labels: { [BUSY_LABEL]: busy ? "true" : null },
      annotations: Object.fromEntries(Object.entries(BUSY_ANNOTATIONS).map(([key, value]) => [key, busy ? value : null])),
    },
  };
}

/**
 * What BusyPods.layer is given: how often a held pod is labelled again, and how often the
 * pods alasio does not hold are looked for labelled.
 */
export interface BusyPodsOptions {
  readonly refresh?: Duration.Input | undefined;
  readonly sweep?: Duration.Input | undefined;
}

/** How long a label's patch, or a sweep's list, may take before it is given up on, and logged. */
const PATCH_TIMEOUT: Duration.Input = "5 seconds";

/** A pod's holds, while it has a labeller: the pod, how many there are, and what wakes the labeller as they change. */
interface Held {
  readonly pod: PodRef;
  count: number;
  changed: Deferred.Deferred<void>;
}

export class BusyPods extends Context.Service<BusyPods, {
  /** Holds alasio's own pod busy until the scope closes. */
  readonly hold: Effect.Effect<void, never, Scope.Scope>;
}>()("alasio/kube/BusyPods") {
  /**
   * On the KubeClient, alasio's own pod as its Deployment names it (ALASIO_POD,
   * ALASIO_NAMESPACE), none outside a pod; sweeping alasio's namespace.
   */
  static readonly layer = ({ refresh = "10 seconds", sweep = "1 minute" }: BusyPodsOptions): Layer.Layer<BusyPods, Config.ConfigError, KubeClient> =>
    Layer.effect(
      BusyPods,
      Effect.gen(function*() {
        const kube = yield* KubeClient;
        const scope = yield* Effect.scope;
        const name = yield* Config.option(Config.String("ALASIO_POD"));
        const namespace = yield* Config.String("ALASIO_NAMESPACE").pipe(Config.withDefault("alasio"));
        const own = Option.map(name, (pod): PodRef => ({ namespace, name: pod }));
        const held = new Map<string, Held>();
        const keyOf = (pod: PodRef): string => `${pod.namespace}/${pod.name}`;

        /** Labels the pod busy or not; one not there yet, or gone, is left to its next label. */
        const label = (pod: PodRef, busy: boolean): Effect.Effect<void> =>
          kube.patch("v1", "Pod", pod.namespace, pod.name, busyPatch(busy)).pipe(
            Effect.timeout(PATCH_TIMEOUT),
            Effect.asVoid,
            Effect.catchIf((error) => error._tag === "KubeApiError" && hasStatus(404)(error), () => Effect.void),
            Effect.catch((error) => Effect.logWarning(`could not ${busy ? "label" : "unlabel"} the pod ${pod.namespace}/${pod.name} busy: ${error.message}`)),
          );

        /**
         * The pod's labeller, while it has a hold, or a label to take off: it labels the pod
         * busy, again every `refresh` or as its holds change, while it has any; then takes
         * the label off, and ends once the pod has none still, or labels it again.
         */
        const labeller = (pod: PodRef, holds: Held): Effect.Effect<void> =>
          Effect.gen(function*() {
            while (true) {
              // Made anew before the holds are read, so no change after is missed.
              holds.changed = Deferred.makeUnsafe<void>();
              if (holds.count > 0) {
                yield* label(pod, true);
                yield* Effect.raceFirst(Effect.sleep(refresh), Deferred.await(holds.changed));
                continue;
              }
              yield* label(pod, false);
              if (holds.count === 0) {
                held.delete(keyOf(pod));
                return;
              }
            }
          });

        // As alasio stops, its labellers stop first, then each pod they had is unlabelled.
        yield* Effect.addFinalizer(() => Effect.forEach([...held.values()], ({ pod }) => label(pod, false), { concurrency: "unbounded", discard: true }));
        const labellers = yield* Scope.fork(scope);

        /** Changes the pod's holds by `by`, waking its labeller; one it has not got starts, labelling it, or unlabelling it where `by` is 0. */
        const change = (pod: PodRef, by: 1 | 0 | -1): Effect.Effect<void> =>
          Effect.suspend(() => {
            const holds = held.get(keyOf(pod));
            if (holds) {
              holds.count += by;
              return Effect.asVoid(Deferred.succeed(holds.changed, undefined));
            }
            if (by < 0) return Effect.void;
            const first: Held = { pod, count: by, changed: Deferred.makeUnsafe<void>() };
            held.set(keyOf(pod), first);
            return Effect.asVoid(Effect.forkIn(labeller(pod, first), labellers));
          });

        /** Unlabels the pods of alasio's namespace labelled busy that it does not hold, each through a labeller of its own. */
        const sweepOnce = Effect.suspend(() => kube.list("v1", "Pod", namespace, `${BUSY_LABEL}=true`)).pipe(
          Effect.timeout(PATCH_TIMEOUT),
          Effect.flatMap((pods) =>
            Effect.forEach(pods, ({ metadata }) => {
              const pod = { namespace, name: metadata?.name ?? "" };
              return held.has(keyOf(pod)) ? Effect.void : change(pod, 0);
            }, { discard: true })
          ),
          Effect.catch((error) => Effect.logWarning(`could not find the pods of ${namespace} labelled busy: ${error.message}`)),
        );
        yield* Effect.forkIn(Effect.forever(Effect.andThen(sweepOnce, Effect.sleep(sweep))), labellers);

        return BusyPods.of({
          hold: Option.match(own, { onNone: () => Effect.void, onSome: (pod) => Effect.acquireRelease(change(pod, 1), () => change(pod, -1)) }),
        });
      }),
    );
}
