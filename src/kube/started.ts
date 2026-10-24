/**
 * alasio's starts: each counted (alasio.starts), and, where it runs in Kubernetes, the time of
 * the last kept in a ConfigMap of its namespace's own (./started-map.ts), so that `alasio
 * status` says when it last started while it runs no replica, past the hour KEDA's record of
 * waking it lasts. One that cannot be kept is logged, and alasio starts all the same.
 */
import { Clock, Config, Effect, Layer } from "effect";

import { meter } from "../telemetry/index.ts";
import { KubeClient } from "./client.ts";
import { STARTED_CONFIG_MAP, STARTED_KEY } from "./started-map.ts";

export const STARTS = "alasio.starts";
const starts = meter.createCounter(STARTS, { description: "alasio's starts, each as it comes up", unit: "{start}" });

/** How long keeping the time may take before it is given up on, and logged. */
const KEEP_TIMEOUT = "5 seconds";

/** Counts alasio's start, and, `inKubernetes`, keeps its time in STARTED_CONFIG_MAP of alasio's namespace (ALASIO_NAMESPACE), on `kube`. */
export const recordStart = (inKubernetes: boolean, kube: Layer.Layer<KubeClient> = KubeClient.layer): Layer.Layer<never> =>
  Layer.effectDiscard(Effect.gen(function*() {
    starts.add(1);
    if (!inKubernetes) return;
    const startedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const namespace = yield* Config.String("ALASIO_NAMESPACE").pipe(Config.withDefault("alasio"), Effect.orElseSucceed(() => "alasio"));
    yield* Effect.flatMap(KubeClient, (client) => client.patch("v1", "ConfigMap", namespace, STARTED_CONFIG_MAP, { data: { [STARTED_KEY]: startedAt } })).pipe(
      Effect.timeout(KEEP_TIMEOUT),
      Effect.catch((error) => Effect.logWarning(`could not keep alasio's start, at ${startedAt}, in ${namespace}/${STARTED_CONFIG_MAP}: ${error.message}`)),
      Effect.provide(kube),
    );
  }));
