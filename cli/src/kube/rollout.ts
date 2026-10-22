/**
 * Waiting for what was applied to run: a CRD until it is established, a Job until it
 * completes, a Deployment, StatefulSet or DaemonSet until its rollout is done (a sandbox
 * pool's Deployment, whose pods are ready only once each is bound, until they run), and a
 * StatefulSet until its pods are scheduled; waiting for what was deleted to be gone; and
 * for volumes let go of to be deleted. A wait says
 * what it still waits for as that changes, and when it gives up, why each thing is not
 * ready, from its pods' state and the cluster's warnings about them. A workload's pod that
 * stops within its grace, as a node agent's does while restores read the bucket through
 * it, holds the wait past its timeout until it is gone, and the timeout after: the wait says
 * so, with what the pod last logged.
 */
import type { CoreV1Event, KubernetesObject, V1ContainerStatus, V1Deployment, V1Pod, V1StatefulSet } from "@kubernetes/client-node";
import { Clock, Duration, Effect, Schedule, Schema, Stream } from "effect";

import { describeRef, kind, KubeApi, type KubeApiError, type ObjectRef } from "./api.ts";

/**
 * Where a thing waited for is: ready, still waiting (and what it is at, and until when a pod
 * of it may still be stopping, in milliseconds since the epoch), or failed, which no wait
 * mends.
 */
export type Readiness =
  | { readonly _tag: "Ready" }
  | { readonly _tag: "Waiting"; readonly status: string; readonly stoppingUntil?: number }
  | { readonly _tag: "Failed"; readonly reason: string };

const READY: Readiness = { _tag: "Ready" };
const waiting = (status: string): Readiness => ({ _tag: "Waiting", status });

/** What the parts of an object a readiness is read from are, as far as it reads them. */
interface Observed {
  readonly metadata?: { readonly generation?: number };
  readonly spec?: { readonly replicas?: number };
  readonly status?: {
    readonly observedGeneration?: number;
    readonly replicas?: number;
    readonly updatedReplicas?: number;
    readonly availableReplicas?: number;
    readonly readyReplicas?: number;
    readonly desiredNumberScheduled?: number;
    readonly updatedNumberScheduled?: number;
    readonly numberReady?: number;
    readonly conditions?: readonly { readonly type: string; readonly status: string; readonly reason?: string; readonly message?: string }[];
  };
}

/** Where `object` is: a CRD, Job, Deployment, StatefulSet or DaemonSet by its status, anything else ready once it exists. */
export function readiness(object: KubernetesObject): Readiness {
  const { metadata, spec, status } = object as KubernetesObject & Observed;
  const condition = (type: string) => status?.conditions?.find((each) => each.type === type && each.status === "True");
  const unseen = (status?.observedGeneration ?? 0) < (metadata?.generation ?? 0);
  const replicas = spec?.replicas ?? 1;
  switch (object.kind) {
    case "CustomResourceDefinition":
      return condition("Established") ? READY : waiting("not established");
    case "Job": {
      const failed = condition("Failed");
      if (failed) return { _tag: "Failed", reason: [failed.reason, failed.message].filter(Boolean).join(": ") || "it failed" };
      return condition("Complete") ? READY : waiting("running");
    }
    case "Deployment": {
      const updated = status?.updatedReplicas ?? 0;
      const available = status?.availableReplicas ?? 0;
      if (unseen) return waiting("not yet rolled out");
      if (updated < replicas) return waiting(`${updated} of ${replicas} updated`);
      if ((status?.replicas ?? 0) > updated) return waiting(`${(status?.replicas ?? 0) - updated} old pods stopping`);
      return available < replicas ? waiting(`${available} of ${replicas} available`) : READY;
    }
    case "StatefulSet": {
      const updated = status?.updatedReplicas ?? 0;
      const ready = status?.readyReplicas ?? 0;
      if (unseen) return waiting("not yet rolled out");
      if (updated < replicas) return waiting(`${updated} of ${replicas} updated`);
      return ready < replicas ? waiting(`${ready} of ${replicas} ready`) : READY;
    }
    case "DaemonSet": {
      // A pod on every node it schedules to, rather than replicas.
      const desired = status?.desiredNumberScheduled ?? 0;
      const updated = status?.updatedNumberScheduled ?? 0;
      const ready = status?.numberReady ?? 0;
      if (unseen) return waiting("not yet rolled out");
      if (updated < desired) return waiting(`${updated} of ${desired} nodes updated`);
      return ready < desired ? waiting(`${ready} of ${desired} nodes ready`) : READY;
    }
    default:
      return READY;
  }
}

/**
 * The annotation of a Deployment whose pods are ready only once each is bound, a sandbox
 * pool's (../manifests/sandbox-gateway.ts), each then out of the Deployment: it is ready once
 * a pod of each of its replicas runs, ready or not (readinessOf).
 */
export const READY_ONCE_BOUND_ANNOTATION = "alasio.dev/ready-once-bound";

/**
 * Where `object` is, as readiness says; but a Deployment annotated READY_ONCE_BOUND_ANNOTATION,
 * once its rollout has made a pod of each of its replicas and none of before, ready once each
 * runs, its containers started, as its pods say.
 */
export const readinessOf = Effect.fnUntraced(function*(object: KubernetesObject): Effect.fn.Return<Readiness, KubeApiError, KubeApi> {
  if (object.kind !== "Deployment" || object.metadata?.annotations?.[READY_ONCE_BOUND_ANNOTATION] !== "true") return readiness(object);
  const { metadata, spec, status } = object as V1Deployment;
  const replicas = spec?.replicas ?? 1;
  const updated = status?.updatedReplicas ?? 0;
  if ((status?.observedGeneration ?? 0) < (metadata?.generation ?? 0)) return waiting("not yet rolled out");
  if (updated < replicas) return waiting(`${updated} of ${replicas} updated`);
  if ((status?.replicas ?? 0) > updated) return waiting(`${(status?.replicas ?? 0) - updated} old pods stopping`);
  const kube = yield* KubeApi;
  const pods = yield* kube.list<V1Pod>(POD, { namespace: metadata?.namespace, labelSelector: selectorOf(spec?.selector?.matchLabels ?? {}) });
  const runs = pods.filter((pod) => !pod.metadata?.deletionTimestamp && pod.status?.phase === "Running" && (pod.status.containerStatuses ?? []).every(({ state }) => state?.running)).length;
  return runs < replicas ? waiting(`${runs} of ${replicas} running`) : READY;
});

/** Something waited for that is not ready, as people read it: the object, and what it is at. */
const NotReadyObject = Schema.Struct({
  object: Schema.String,
  status: Schema.String,
  /** Why: its pods' state and the cluster's warnings about it. */
  why: Schema.Array(Schema.String),
});

const indented = ({ object, status, why }: typeof NotReadyObject.Type): string =>
  [`  ${object}: ${status}`, ...why.map((line) => `    ${line}`)].join("\n");

/** A wait gave up: what it still waited for, and why each was not ready. */
export class NotReadyInTime extends Schema.TaggedError<NotReadyInTime>()("NotReadyInTime", {
  /** How long it was given, in seconds. */
  within: Schema.Number,
  waiting: Schema.Array(NotReadyObject),
}) {
  override get message(): string {
    return `not ready within ${this.within}s, still waiting for:\n${this.waiting.map(indented).join("\n")}`;
  }
}

/** Something waited for failed, as a Job that ran out of retries: what, and why. */
export class RolloutFailed extends Schema.TaggedError<RolloutFailed>()("RolloutFailed", {
  failed: NotReadyObject,
}) {
  override get message(): string {
    return `failed:\n${indented(this.failed)}`;
  }
}

/** A look found something still waiting past the wait's time; the wait's NotReadyInTime says what. */
class TimedOut extends Schema.TaggedError<TimedOut>()("TimedOut", {}) {}

/** How long a wait may take, and how often it looks. */
export interface WaitOptions {
  readonly timeout: Duration.Input;
  readonly poll: Duration.Input;
  /**
   * When the timeout counts from, in milliseconds since the epoch: the wait's start, unless it
   * shares one with waits before it, counted as though it began what they spent before now. A
   * wait past it says the timeout it was given, the budget, not how long it took on the clock.
   */
  readonly since?: number;
}

const POD = kind("Pod");
const EVENT = kind("Event");

/** `labels` as a label selector. */
export function selectorOf(labels: Readonly<Record<string, string>>): string {
  return Object.entries(labels).map(([key, value]) => `${key}=${value}`).join(",");
}

/** What is wrong with `container`, unless nothing is. */
function containerProblem(pod: string, container: V1ContainerStatus): string | null {
  const { state, lastState, restartCount } = container;
  const where = `container ${container.name} of pod ${pod}`;
  const lastExit = lastState?.terminated && restartCount > 0
    ? `, restarted ${restartCount} times, last exiting with ${lastState.terminated.exitCode}${lastState.terminated.reason ? ` (${lastState.terminated.reason})` : ""}`
    : "";
  if (state?.waiting) return `${where} is waiting: ${[state.waiting.reason, state.waiting.message].filter(Boolean).join(": ")}${lastExit}`;
  if (state?.terminated && state.terminated.exitCode !== 0) {
    return `${where} exited with ${state.terminated.exitCode}${state.terminated.reason ? ` (${state.terminated.reason})` : ""}${state.terminated.message ? `: ${state.terminated.message}` : ""}`;
  }
  if (state?.running && !container.ready) return `${where} runs, not ready${lastExit}`;
  return null;
}

/** What is wrong with `pod`: unscheduled, or its containers' problems; nothing when it is ready. */
export function podProblems(pod: V1Pod): string[] {
  const name = pod.metadata?.name ?? "";
  const unscheduled = pod.status?.conditions?.find(({ type, status }) => type === "PodScheduled" && status === "False");
  if (unscheduled) return [`pod ${name} is not scheduled: ${unscheduled.message ?? unscheduled.reason ?? "no node takes it"}`];
  const problems = [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])]
    .map((container) => containerProblem(name, container))
    .filter((problem) => problem !== null);
  const ready = pod.status?.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ?? false;
  return problems.length > 0 || ready ? problems : [`pod ${name} is ${pod.status?.phase ?? "not yet started"}`];
}

/** What a container waits on as its pod starts, which is no problem of it. */
const STARTING = new Set(["ContainerCreating", "PodInitializing"]);

/**
 * Whether `pod` is starting, not stuck: on a node, none of its containers restarted, each
 * running, done or waiting on its start alone, and made within `withinMs` of `now`.
 */
export function starting(pod: V1Pod, withinMs: number, now: number): boolean {
  if (pod.status?.conditions?.some(({ type, status }) => type === "PodScheduled" && status === "False")) return false;
  const started = new Date(pod.metadata?.creationTimestamp ?? 0).getTime();
  return now - started < withinMs && [...(pod.status?.initContainerStatuses ?? []), ...(pod.status?.containerStatuses ?? [])].every(({ restartCount, state }) =>
    restartCount === 0 && (!state?.waiting || STARTING.has(state.waiting.reason ?? "")) && (!state?.terminated || state.terminated.exitCode === 0)
  );
}

/** When `event` last happened. */
const lastSeen = (event: CoreV1Event): number =>
  new Date(event.lastTimestamp ?? event.eventTime ?? event.metadata.creationTimestamp ?? 0).getTime();

/**
 * Why `ref` is not ready: its pods' problems, and the latest of the cluster's warnings
 * about them, it, or what it made (a Deployment's ReplicaSets, named after it). The
 * cluster keeps its warnings about what is in no namespace, as a volume, in `default`.
 */
export const diagnose = Effect.fnUntraced(function*(ref: ObjectRef): Effect.fn.Return<string[], KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const object = yield* kube.get<KubernetesObject & { spec?: { selector?: { matchLabels?: Record<string, string> } } }>(ref);
  const matchLabels = object?.spec?.selector?.matchLabels;
  const pods = matchLabels ? yield* kube.list<V1Pod>(POD, { namespace: ref.namespace, labelSelector: selectorOf(matchLabels) }) : [];
  const warnings = yield* kube.list<CoreV1Event>(EVENT, { namespace: ref.namespace ?? "default", fieldSelector: "type=Warning" });
  const about = (names: readonly string[]) =>
    warnings
      .filter(({ involvedObject }) => names.some((name) => involvedObject.name === name || involvedObject.name?.startsWith(`${name}-`)))
      .sort((a, b) => lastSeen(b) - lastSeen(a))
      .map(({ reason, message }) => `warning: ${[reason, message?.trim()].filter(Boolean).join(": ")}`)
      .filter((line, index, lines) => lines.indexOf(line) === index)
      .slice(0, 3);
  if (pods.length === 0) return [...(matchLabels ? ["no pods"] : []), ...about([ref.name])];
  const notReady = pods.filter((pod) => podProblems(pod).length > 0);
  return [...notReady.flatMap(podProblems), ...about(notReady.map((pod) => pod.metadata?.name ?? ""))];
});

/**
 * Looks at every ref by `look` until each is ready, every `poll` for `timeout`, saying
 * what it still waits for whenever that changes; fails with why when one fails, or when
 * time runs out. A ref one of whose pods is stopping within its grace holds the wait until
 * it is gone, and for `timeout` after the last look that found one.
 */
const awaitAll = Effect.fnUntraced(function*(
  refs: readonly ObjectRef[],
  look: (ref: ObjectRef) => Effect.Effect<Readiness, KubeApiError, KubeApi>,
  why: (ref: ObjectRef) => Effect.Effect<string[], KubeApiError, KubeApi>,
  { timeout, poll, since }: WaitOptions,
): Effect.fn.Return<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> {
  // What the last look found still waiting, and what was last said of it.
  let pending: ReadonlyArray<readonly [ObjectRef, string]> = [];
  let said = "";
  const timeoutMs = Duration.toMillis(Duration.fromInputUnsafe(timeout));
  const startedAt = since ?? (yield* Clock.currentTimeMillis);
  let deadline = startedAt + timeoutMs;
  const once = Effect.gen(function*() {
    const states = yield* Effect.forEach(refs, (ref) => Effect.map(look(ref), (state) => [ref, state] as const), { concurrency: 8 });
    for (const [ref, state] of states) {
      if (state._tag === "Failed") return yield* new RolloutFailed({ failed: { object: describeRef(ref), status: state.reason, why: yield* why(ref) } });
    }
    pending = states.flatMap(([ref, state]) => (state._tag === "Waiting" ? [[ref, state.status] as const] : []));
    if (pending.length === 0) return true;
    const now = yield* Clock.currentTimeMillis;
    if (states.some(([, state]) => state._tag === "Waiting" && (state.stoppingUntil ?? 0) > now)) deadline = Math.max(deadline, now + timeoutMs);
    const saying = pending.map(([ref, status]) => `${ref.name} (${status})`).join(", ");
    if (saying !== said) {
      said = saying;
      yield* Effect.logInfo(`waiting for ${saying}`);
    }
    if (now >= deadline) return yield* new TimedOut();
    return false;
  });
  yield* once.pipe(
    Effect.retry({ while: (error) => error._tag === "KubeApiError" && Date.now() < deadline, schedule: Schedule.spaced(poll) }),
    Effect.repeat({ schedule: Schedule.spaced(poll), until: (done) => done }),
    Effect.catchTag("TimedOut", () =>
      Effect.forEach(pending, ([ref, status]) => Effect.map(why(ref), (lines) => ({ object: describeRef(ref), status, why: lines }))).pipe(
        Effect.flatMap((notReady) => Effect.fail(new NotReadyInTime({ within: (deadline - startedAt) / 1000, waiting: notReady }))),
      )),
  );
});

/** The object `ref`, or null when there is none. */
const current = (ref: ObjectRef): Effect.Effect<KubernetesObject | null, KubeApiError, KubeApi> => Effect.flatMap(KubeApi, (kube) => kube.get(ref));

/** What the kubelet answers, in the log's place, for a container that has ended while its pod is still there. */
const UNRETRIEVABLE_LOG = /^unable to retrieve container logs for /u;

/**
 * What a pod last logged, of its first container: its last line, alone read, or nothing
 * where it cannot be read, as once that container has ended.
 */
const lastLogged = (pod: V1Pod): Effect.Effect<string, never, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.logs(pod.metadata?.namespace ?? "", pod.metadata?.name ?? "", { container: pod.spec?.containers[0]?.name ?? "", tailLines: 1 }).pipe(
      Stream.decodeText,
      Stream.mkString,
      Effect.map((text) => text.trimEnd().split("\n").at(-1)?.trim() ?? ""),
      Effect.map((line) => (UNRETRIEVABLE_LOG.test(line) ? "" : line)),
      Effect.orElseSucceed(() => ""),
    ));

/**
 * Where `object`, a workload, is (readinessOf), and, while it waits, which of its pods stop,
 * each as it may until its grace ends, and what it last logged.
 */
const workloadReadiness = (object: KubernetesObject & { spec?: { selector?: { matchLabels?: Record<string, string> } } }): Effect.Effect<Readiness, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const state = yield* readinessOf(object);
    const matchLabels = object.spec?.selector?.matchLabels;
    if (state._tag !== "Waiting" || !matchLabels) return state;
    const kube = yield* KubeApi;
    const pods = yield* kube.list<V1Pod>(POD, { namespace: object.metadata?.namespace, labelSelector: selectorOf(matchLabels) });
    const stopping = pods.filter(({ metadata }) => metadata?.deletionTimestamp !== undefined);
    if (stopping.length === 0) return state;
    const said = yield* Effect.forEach(stopping, (pod) =>
      Effect.map(lastLogged(pod), (line) => {
        const until = new Date(pod.metadata?.deletionTimestamp ?? 0).toISOString();
        return `pod ${pod.metadata?.name} on ${pod.spec?.nodeName ?? "no node"} stopping, as it may until ${until}${line === "" ? "" : `, last saying: ${line}`}`;
      }));
    const stoppingUntil = Math.max(...stopping.map(({ metadata }) => new Date(metadata?.deletionTimestamp ?? 0).getTime()));
    return { _tag: "Waiting", status: [state.status, ...said].join("; "), stoppingUntil };
  });

/** Waits until every ref is ready (readiness), a workload one of whose pods is stopping within its grace for as long as it may. */
export const awaitReady = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(refs, (ref) => Effect.flatMap(current(ref), (object) => (object ? workloadReadiness(object) : Effect.succeed(waiting("not found")))), diagnose, options);

/**
 * Waits until every StatefulSet ref has made a pod for each of its replicas and each is
 * on a node: scheduled, ready or not. One whose pods start in order makes each only once
 * the one before it is ready.
 */
export const awaitScheduled = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(
    refs,
    (ref) =>
      Effect.gen(function*() {
        const kube = yield* KubeApi;
        const set = yield* kube.get<V1StatefulSet>(ref);
        if (!set) return waiting("not found");
        const replicas = set.spec?.replicas ?? 1;
        const made = set.status?.replicas ?? 0;
        if ((set.status?.observedGeneration ?? 0) < (set.metadata?.generation ?? 0)) return waiting("not yet rolled out");
        if (made < replicas) return waiting(`${made} of ${replicas} pods made`);
        const pods = yield* kube.list<V1Pod>(POD, { namespace: ref.namespace, labelSelector: selectorOf(set.spec?.selector?.matchLabels ?? {}) });
        // Every replica's pod made, the pods listed are all of them.
        const unscheduled = pods.filter((pod) => !pod.spec?.nodeName).length;
        return unscheduled > 0 ? waiting(`${unscheduled} of ${pods.length} pods not yet scheduled`) : READY;
      }),
    diagnose,
    options,
  );

/** Waits until every ref is gone, its deletion done. */
export const awaitGone = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(
    refs,
    (ref) => Effect.map(current(ref), (object) => (object ? waiting("being deleted") : READY)),
    (ref) => Effect.map(current(ref), (object) => (object?.metadata?.finalizers?.length ? [`waits on its finalizers: ${object.metadata.finalizers.join(", ")}`] : [])),
    options,
  );

/** Waits until every Job ref has ended, complete or failed, or is gone. */
export const awaitEnded = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(refs, (ref) => Effect.map(current(ref), (job) => (job === null || readiness(job)._tag !== "Waiting" ? READY : waiting("running"))), diagnose, options);
