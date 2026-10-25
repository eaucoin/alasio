/**
 * alasio's own pod, as its Deployment names it to it (ALASIO_POD, ALASIO_NAMESPACE), and alasio
 * told to stop as that pod is deleted. kubelet tells a container its pod is deleted by one
 * SIGTERM, which can be lost: one sent as the container starts, before tini, the image's
 * entrypoint, passes signals on, reaches a PID 1 with no handler of it, which the kernel drops,
 * and the pod runs on, serving and holding its conversations' claims, to the end of its grace.
 * So alasio reads its pod every READ_EVERY from as it starts, and, once the pod is deleted (its
 * deletionTimestamp set, or gone), sends itself that SIGTERM, unless one came already: it stops
 * as kubelet tells it to, through all that hears a SIGTERM (../shared/run.ts,
 * ../telegram/drain.ts, ../codex/worker-claims.ts), at any time, its start among it. Read, not
 * watched, as alasio watches no pod (cli/src/manifests/alasio.ts identity).
 */
import { Config, Effect, Option, type Scope } from "effect";

import type { PodRef } from "./busy.ts";
import { KubeClient } from "./client.ts";

/** alasio's own pod, as its Deployment names it; none outside a pod. */
export const ownPod: Effect.Effect<Option.Option<PodRef>, Config.ConfigError> = Effect.gen(function*() {
  const name = yield* Config.option(Config.String("ALASIO_POD"));
  const namespace = yield* Config.String("ALASIO_NAMESPACE").pipe(Config.withDefault("alasio"));
  return Option.map(name, (pod): PodRef => ({ namespace, name: pod }));
});

/** How often `deleted` reads the pod: a deletion whose SIGTERM was lost stops alasio within it, and the API server is asked once in it. */
export const READ_EVERY = "2 seconds";

/**
 * Waits until `pod` is deleted: its deletionTimestamp set, or gone, as it is read, now and
 * every READ_EVERY; a read that fails is logged, the first of those in a row alone, and the
 * pod read again.
 */
export const deleted = (pod: PodRef): Effect.Effect<void, never, KubeClient> =>
  Effect.gen(function*() {
    const kube = yield* KubeClient;
    let failing = false;
    while (true) {
      const read = yield* kube.read("v1", "Pod", pod.namespace, pod.name).pipe(
        Effect.map(Option.some),
        Effect.catch((error) =>
          Effect.as(
            failing ? Effect.void : Effect.logWarning(`could not read its pod ${pod.namespace}/${pod.name}, to stop as it is deleted: ${error.message}`),
            Option.none(),
          )
        ),
      );
      failing = Option.isNone(read);
      if (Option.isSome(read) && (read.value === null || read.value.metadata?.deletionTimestamp !== undefined)) return;
      yield* Effect.sleep(READ_EVERY);
    }
  });

/**
 * alasio told to stop as its own pod is deleted, as the module says, while the scope is open;
 * nothing outside a pod.
 */
export const stopsAsItsPodIsDeleted: Effect.Effect<void, Config.ConfigError, Scope.Scope> = Effect.gen(function*() {
  const pod = yield* ownPod;
  if (Option.isNone(pod)) return;
  let told = false;
  // Never taken off: kubelet's SIGTERM, come after the one alasio sent itself, ends nothing midway once alasio's stop
  // has taken its own handlers off.
  process.on("SIGTERM", () => void (told = true));
  yield* Effect.forkScoped(
    deleted(pod.value).pipe(
      Effect.andThen(Effect.suspend(() =>
        told ? Effect.void : Effect.logInfo(`its pod ${pod.value.namespace}/${pod.value.name} is deleted, and no SIGTERM came: it stops as told by one`).pipe(
          Effect.andThen(Effect.sync(() => void process.kill(process.pid, "SIGTERM"))),
        )
      )),
      Effect.provide(KubeClient.layer),
    ),
  );
});
