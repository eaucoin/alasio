/**
 * The gVisor releases alasio keeps: those it pins (cluster/node/pins.json, the current
 * one first, which new sandboxes run), every other one installed that a snapshot still
 * names, as only the release that took a snapshot restores it (src/gateway/snapshots.ts),
 * and every other one installed that a pod not yet ended may run, or that the RuntimeClass
 * `gvisor` runs the pods made next with, as removing a release stops its sandboxes: a
 * session restored from a snapshot and not saved since runs under the release that took it,
 * and no snapshot names that release for it any more (its snapshot spent, the record of
 * its release cleared), so its live state would end unsaved with the release; and a
 * session started while `alasio up` runs, before it makes `gvisor` anew, runs the release
 * before. `alasio up` reads them as it begins, and again just before root removes a
 * release, as its downloads and its wait for the harnesses may take long.
 *
 * The sandbox gateway says what the snapshots name, of every environment's workspaces, the
 * snapshot each was released into and its current one, as main's token reads it (GET
 * /snapshots/releases, src/gateway/sessions.ts), through a port-forward to a gateway pod
 * of its own (./gateway-calls.ts). The cluster's pods and RuntimeClasses say which
 * releases pods may run (releasesInUseOf).
 *
 * Where either cannot be read (the cluster is down, no gateway runs, what it answers is
 * not what it says, or a pod's release cannot be told), every release installed is kept,
 * and it is said so. A release no snapshot names, no pod runs, and that is not pinned, is
 * removed, its RuntimeClass with it. On a cluster in Docker, the releases are its node
 * image's, the pinned ones, which change only with the image, as a node made anew stops
 * all its pods anyway: a snapshot naming another falls back, as one whose release is not
 * kept does. On a cluster of a kubeconfig, the operator keeps the releases. A release
 * before gvisor.restoresFromBucketSince, as the fork's .6, does not end a restore it
 * refuses, which stalls the next pod's teardown for minutes before the fallback
 * (progress/live-lazy.md); its snapshots, released again under a later release, leave it
 * unkept over time.
 */
import type { V1Pod, V1RuntimeClass, V1Secret } from "@kubernetes/client-node";
import { Effect, Schema } from "effect";

import { MAIN_ENVIRONMENT_NAME } from "../../src/branch/names.ts";
import { GVISOR_RELEASE_ANNOTATION, GVISOR_RUNTIME_CLASS } from "./cluster/k3s.ts";
import { gatewayCalls, GatewayUnreached } from "./gateway-calls.ts";
import { kind, KubeApi } from "./kube/api.ts";
import { NAMESPACE, SESSIONS_CALLER_SECRET } from "./manifests/common.ts";

/** The snapshots a workspace may be restored from, by the gVisor release that took each, as the gateway names them. */
export type SnapshotReleases = ReadonlyMap<string, readonly string[]>;

/** What the gateway answers of the releases its snapshots name. */
const ReleasesAnswer = Schema.Struct({ releases: Schema.Record(Schema.String, Schema.Array(Schema.String)) });

/** How long reading what the gateway says may take. */
const READ_TIMEOUT = "30 seconds";

/**
 * What the snapshots name, as `asked`, what the gateway answered, says; null where it
 * failed, or its answer is not one, which is said so, as every release installed is then
 * kept.
 */
export const snapshotReleasesFrom = <R>(asked: Effect.Effect<unknown, { readonly message: string }, R>): Effect.Effect<SnapshotReleases | null, never, R> =>
  asked.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ReleasesAnswer)),
    Effect.map(({ releases }): SnapshotReleases => new Map(Object.entries(releases))),
    Effect.timeout(READ_TIMEOUT),
    Effect.catch((error) =>
      Effect.as(Effect.logWarning(`what the snapshots name could not be read of the sandbox gateway, so every gVisor release installed is kept: ${error.message}`), null)
    ),
  );

/** What the gateway answers of the releases its snapshots name, asked by main's token, signed by main's own key. */
const askGateway = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const secret = yield* kube.get<V1Secret>({ ...kind("Secret"), namespace: NAMESPACE, name: SESSIONS_CALLER_SECRET });
  const callerKey = secret?.data?.["caller-key"];
  if (!callerKey) return yield* new GatewayUnreached({ reason: `${SESSIONS_CALLER_SECRET} holds no caller key of main's` });
  const call = yield* gatewayCalls;
  const { status, text } = yield* call({ callerKey: Buffer.from(callerKey, "base64"), environment: MAIN_ENVIRONMENT_NAME, key: "snapshots", method: "GET", path: "/snapshots/releases" });
  if (status !== 200) return yield* new GatewayUnreached({ reason: `it answered ${status}: ${text}` });
  return yield* Effect.try({ try: (): unknown => JSON.parse(text), catch: (error) => new GatewayUnreached({ reason: error instanceof Error ? error.message : String(error) }) });
}).pipe(Effect.scoped);

/** What the snapshots name, as the sandbox gateway says, by snapshotReleasesFrom; null where it cannot be read. */
export const snapshotReleases: Effect.Effect<SnapshotReleases | null, never, KubeApi> = snapshotReleasesFrom(askGateway);

/** The releases pods not yet ended may run, each with those pods (`namespace/name`). */
export type ReleasesInUse = ReadonlyMap<string, readonly string[]>;

/**
 * What the cluster may run, as its pods and RuntimeClasses say: the releases its pods may run, and the release of the
 * RuntimeClass `gvisor`, which every pod made from now on runs until `alasio up` makes it anew (null where there is none).
 */
export interface ClusterRuns {
  readonly pods: ReleasesInUse;
  readonly current: string | null;
}

/** What the cluster may run, as releasesInUseOf reads it, and the pods whose release cannot be told. */
export interface PodReleases extends ClusterRuns {
  readonly untold: readonly string[];
}

/**
 * Which gVisor releases `pods` may run, as alasio's RuntimeClasses among `classes` say
 * (each annotated with its release): of each pod not ended (neither Succeeded nor Failed)
 * that names a RuntimeClass, the release of that class, as long as the class was made
 * before the pod, in an earlier second, so that the pod's sandbox was made under it; a
 * RuntimeClass alasio does not annotate is not gVisor's. A pod whose class is gone, or was
 * made after it or in the same second (the API server keeps the times to the second, and
 * `gvisor`, which follows the current release, is made anew as that changes), may run any
 * release: it is untold. And the release of `gvisor`, which pods made next run. Pure, for
 * tests.
 */
export function releasesInUseOf(classes: readonly V1RuntimeClass[], pods: readonly V1Pod[]): PodReleases {
  const byName = new Map(classes.map((runtimeClass) => [runtimeClass.metadata?.name ?? "", runtimeClass]));
  const made = (time: Date | string | undefined) => (time === undefined ? Number.NaN : new Date(time).getTime());
  const used = new Map<string, string[]>();
  const untold: string[] = [];
  for (const pod of pods) {
    const name = pod.spec?.runtimeClassName;
    if (!name || pod.status?.phase === "Succeeded" || pod.status?.phase === "Failed") continue;
    const named = `${pod.metadata?.namespace}/${pod.metadata?.name}`;
    const runtimeClass = byName.get(name);
    const release = runtimeClass?.metadata?.annotations?.[GVISOR_RELEASE_ANNOTATION];
    if (runtimeClass && release === undefined) continue;
    if (release === undefined || !(made(runtimeClass?.metadata?.creationTimestamp) < made(pod.metadata?.creationTimestamp))) {
      untold.push(named);
      continue;
    }
    used.set(release, [...(used.get(release) ?? []), named]);
  }
  return { pods: used, untold, current: byName.get(GVISOR_RUNTIME_CLASS)?.metadata?.annotations?.[GVISOR_RELEASE_ANNOTATION] ?? null };
}

/**
 * What the cluster may run, by releasesInUseOf; null where it cannot be read, or a pod's
 * release cannot be told, which is said so, as every release installed is then kept.
 */
export const releasesInUse: Effect.Effect<ClusterRuns | null, never, KubeApi> = Effect.flatMap(KubeApi, (kube) =>
  Effect.all([kube.list<V1RuntimeClass>(kind("RuntimeClass")), kube.list<V1Pod>(kind("Pod"))]).pipe(
    Effect.map(([classes, pods]) => releasesInUseOf(classes, pods)),
    Effect.flatMap(({ pods, current, untold }) =>
      untold.length === 0
        ? Effect.succeed({ pods, current })
        : Effect.as(Effect.logWarning(`the gVisor release of ${untold.join(", ")} cannot be told, so every gVisor release installed is kept`), null)
    ),
    Effect.timeout(READ_TIMEOUT),
    Effect.catch((error) =>
      Effect.as(Effect.logWarning(`which gVisor releases the cluster's pods run could not be read, so every gVisor release installed is kept: ${error.message}`), null)
    ),
  ));

/**
 * The releases kept, of those `installed`, beside the `pinned` ones, as what the
 * snapshots name (`named`) and what the cluster may run (`runs`) say, either null where it
 * is not known: the pinned first, in their order, then those installed that a snapshot
 * names, a pod may run, or `gvisor` runs the pods made next with, in theirs; every one
 * installed where either is not known. Pure, for tests.
 */
export function keptReleases(pinned: readonly string[], installed: readonly string[], named: SnapshotReleases | null, runs: ClusterRuns | null): string[] {
  const holds = (release: string) => named === null || runs === null || named.has(release) || runs.pods.has(release) || runs.current === release;
  return [...new Set([...pinned, ...installed.filter(holds)])];
}

/**
 * The releases the cluster keeps, as its RuntimeClasses of alasio's say (the ones
 * HostCluster and DockerCluster apply), beside the `pinned` ones, in keptReleases' order;
 * the pinned alone where it has none, or they cannot be read, as on a cluster of a
 * kubeconfig.
 */
export const clusterReleases = (pinned: readonly string[]): Effect.Effect<string[], never, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.list<V1RuntimeClass>(kind("RuntimeClass")).pipe(
      Effect.map((classes) => keptReleases(pinned, classes.flatMap(({ metadata }) => metadata?.annotations?.[GVISOR_RELEASE_ANNOTATION] ?? []), null, null)),
      Effect.catch(() => Effect.succeed([...pinned])),
    ));

/** `count` snapshots, as a sentence says it. */
const snapshotsOf = (count: number): string => (count === 1 ? "1 snapshot names it" : `${count} snapshots name it`);

/** `count` pods, as a sentence says it. */
const podsOf = (count: number): string => (count === 1 ? "1 pod runs it" : `${count} pods run it`);

/**
 * What `alasio status` says of the gVisor releases `kept` (the current one first), of
 * those the snapshots name, `named`, and of those pods may run, `used` (either null where
 * it cannot be read), a line each: each release kept, with how many snapshots name it and,
 * where any does, how many pods run it, then each release named but not kept, whose
 * snapshots fall back. Pure, for tests.
 */
export function describeKept(kept: readonly string[], named: SnapshotReleases | null, used: ReleasesInUse | null): string[] {
  const said = (release: string) => {
    const counted = named === null ? "what snapshots name it is not seen" : snapshotsOf(named.get(release)?.length ?? 0);
    const running = used?.get(release)?.length ?? 0;
    return running > 0 ? `${counted}, and ${podsOf(running)}` : counted;
  };
  return [
    ...kept.map((release, index) => `gVisor ${release}, kept${index === 0 ? ", which new sessions run" : ""}: ${said(release)}`),
    ...[...(named?.keys() ?? [])].filter((release) => !kept.includes(release)).map((release) =>
      `gVisor ${release}, not kept: ${said(release)}, restored from bayma's checkpoints instead`
    ),
  ];
}
