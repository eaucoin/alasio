/**
 * Where the command line reaches an environment's Codex login in its store, its own Codex home
 * among it, whether or not the environment's alasio runs: in a pod of the environment's Codex
 * login relay (../manifests/codex-login.ts), which reaches the store as a role of its own, the
 * login's tables alone. The relay's own pod where one runs and the command may run there;
 * otherwise, as KEDA keeps the relay at none while no Codex harness is bound, or scales it in
 * under a command that takes a while, a pod of its own, of the relay's Deployment's spec, held for
 * what is run in it and deleted after:
 *
 * - one that serves nothing, never ready, so its Service sends it none of the pools' requests;
 *   labelled as held (HELD_LABEL), so no listing of the relay's pods finds it, and by no
 *   ReplicaSet's hash, so no Deployment adopts it;
 * - of none of the key alasio signs the turns' tokens for the relay with, which it checks none of;
 * - of alasio's components' priority, below the turns', so it takes no running pod's place;
 * - ended within HELD_SECONDS, as one whose command line went is; and, ended so, deleted as the
 *   next is made, while one held for another command at once is left to it.
 */
import { randomUUID } from "node:crypto";

import type { V1Deployment, V1Pod } from "@kubernetes/client-node";
import { Duration, Effect, Schedule, Schema } from "effect";

import { type ContainerRef, kind, KubeApi, type KubeApiError } from "../kube/api.ts";
import { HELD_LABEL, selectorOf } from "../kube/rollout.ts";
import { CODEX_LOGIN, CODEX_LOGIN_COMPONENT } from "../manifests/codex-login.ts";
import { priority } from "../manifests/priorities.ts";
import { CODEX_LOGIN_KEY_VARIABLE } from "../../../src/harness/names.ts";
import { runningPodIn } from "./common.ts";

/** How long a held pod lasts at most, as one its command line left, killed say, is ended by then. */
const HELD_SECONDS = 30 * 60;

/** How long a held pod is waited for to run, its image pulled first where its node has none. */
const HELD_START = Duration.minutes(5);

/** The environment of `namespace` has no Codex login relay to reach its login through. */
export class NoLoginRelay extends Schema.TaggedError<NoLoginRelay>()("NoLoginRelay", {
  namespace: Schema.String,
  why: Schema.String,
}) {
  override get message(): string {
    return `the Codex login of ${this.namespace} cannot be reached: ${this.why}`;
  }
}

/** A pod named `name` of the relay `relay`'s spec, held, as the module says. */
export function heldPod(relay: V1Deployment, namespace: string, name: string): V1Pod {
  const template = relay.spec?.template;
  const container = template?.spec?.containers.find(({ name: each }) => each === CODEX_LOGIN_COMPONENT);
  if (!template?.spec || !container) throw new Error(`${relay.metadata?.namespace}/${relay.metadata?.name} has no ${CODEX_LOGIN_COMPONENT} container to hold a pod of`);
  const { ports: _ports, readinessProbe: _readiness, livenessProbe: _liveness, startupProbe: _startup, env = [], ...held } = container;
  return {
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace, labels: { ...template.metadata?.labels, [HELD_LABEL]: "true" } },
    spec: {
      ...template.spec,
      ...priority("component"),
      restartPolicy: "Never",
      activeDeadlineSeconds: HELD_SECONDS,
      containers: [{
        ...held,
        env: env.filter(({ name: variable }) => variable !== CODEX_LOGIN_KEY_VARIABLE),
        command: ["node", "-e", `setTimeout(() => {}, ${HELD_SECONDS * 1000})`],
        // Never ready, so the relay's Service sends it nothing.
        readinessProbe: { exec: { command: ["node", "-e", "process.exit(1)"] }, periodSeconds: 3600 },
      }],
    },
  };
}

/**
 * `use` run on the relay's container of `namespace`'s Codex login relay, as the module says: in
 * a held pod alone where `held` is "always", as for a command a scale-in of the relay must not
 * cut, a device login waiting on its operator.
 */
export const inLoginRelay = <A, E, R>(namespace: string, use: (target: ContainerRef) => Effect.Effect<A, E, R>, { held: holding = "unless-running" }: { readonly held?: "always" | "unless-running" } = {}): Effect.Effect<A, E | KubeApiError | NoLoginRelay, R | KubeApi> =>
  holding === "always" ? held(namespace, use) : runningPodIn(namespace, CODEX_LOGIN_COMPONENT).pipe(
    Effect.map((pod): ContainerRef | null => ({ namespace, pod, container: CODEX_LOGIN_COMPONENT })),
    Effect.catchTag("NoRunningPod", () => Effect.succeed(null)),
    Effect.flatMap((running) => (running ? use(running) : held(namespace, use))),
  );

/** `use` run on a held pod of the relay's in `namespace`, deleted after. */
const held = <A, E, R>(namespace: string, use: (target: ContainerRef) => Effect.Effect<A, E, R>): Effect.Effect<A, E | KubeApiError | NoLoginRelay, R | KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const relay = yield* kube.get<V1Deployment>({ ...kind("Deployment"), namespace, name: CODEX_LOGIN });
    if (!relay) return yield* new NoLoginRelay({ namespace, why: `it has no ${CODEX_LOGIN} Deployment; alasio up makes it` });
    // Those held before that have ended, as one whose command line went ends by its deadline, deleted.
    const before = yield* kube.list<V1Pod>(kind("Pod"), { namespace, labelSelector: selectorOf({ ...relay.spec?.template.metadata?.labels, [HELD_LABEL]: "true" }) });
    yield* Effect.forEach(
      before.filter(({ status }) => status?.phase === "Succeeded" || status?.phase === "Failed"),
      ({ metadata }) => kube.remove({ ...kind("Pod"), namespace, name: metadata?.name ?? "" }).pipe(Effect.ignore),
      { discard: true },
    );
    const name = `${CODEX_LOGIN}-held-${randomUUID().slice(0, 8)}`;
    const ref = { ...kind("Pod"), namespace, name };
    return yield* Effect.acquireUseRelease(
      kube.apply(heldPod(relay, namespace, name)),
      () =>
        kube.get<V1Pod>(ref).pipe(
          Effect.flatMap((pod) => {
            const phase = pod?.status?.phase;
            if (phase === "Running") return Effect.succeed(true);
            if (phase === "Failed" || phase === "Succeeded") return Effect.fail(new NoLoginRelay({ namespace, why: `its held pod ${name} ended before it ran` }));
            return Effect.succeed(false);
          }),
          Effect.repeat({ until: (running) => running, schedule: Schedule.spaced("1 second") }),
          Effect.timeoutOrElse({ duration: HELD_START, orElse: () => Effect.fail(new NoLoginRelay({ namespace, why: `its held pod ${name} did not run within ${Duration.format(HELD_START)}` })) }),
          Effect.andThen(use({ namespace, pod: name, container: CODEX_LOGIN_COMPONENT })),
        ),
      () => kube.remove(ref).pipe(Effect.ignore),
    );
  });
