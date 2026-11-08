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
 * so, with what the pod last logged. A DaemonSet is ready once it has rolled on the Ready nodes it
 * runs a pod on, those not Ready left be (readyApartFromNotReady); and one a workload is held for is
 * waited for to have rolled so for a while, and gone on from where it has not (awaitRolled).
 */
import type {
  CoreV1Event,
  KubernetesObject,
  V1ContainerStatus,
  V1ControllerRevision,
  V1DaemonSet,
  V1Deployment,
  V1Node,
  V1NodeSelectorRequirement,
  V1Pod,
  V1PodSpec,
  V1StatefulSet,
  V1Taint,
  V1Toleration,
} from "@kubernetes/client-node";
import { Clock, Duration, Effect, Schedule, Schema, Stream } from "effect";

import { daemonPodNode } from "../../../src/kube/daemon-pods.ts";
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
  const pods = yield* kube.list<V1Pod>(POD, { namespace: metadata?.namespace, labelSelector: podSelectorOf(spec?.selector?.matchLabels ?? {}) });
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

/**
 * The label of a pod the command line holds of a workload's spec for a command of its own
 * (../commands/login-relay.ts): its workload's labels, so its NetworkPolicy lets it reach what
 * the workload does, but none of its workload's pods, which a workload's pods are listed without.
 */
export const HELD_LABEL = "alasio.dev/held";

/** The pods of a workload `matchLabels` selects, as a label selector: none of those held (HELD_LABEL). */
export function podSelectorOf(matchLabels: Readonly<Record<string, string>>): string {
  return `${selectorOf(matchLabels)},!${HELD_LABEL}`;
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
  const pods = matchLabels ? yield* kube.list<V1Pod>(POD, { namespace: ref.namespace, labelSelector: podSelectorOf(matchLabels) }) : [];
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
    kube.logs(pod.metadata?.namespace ?? "", pod.metadata?.name ?? "", { container: (pod.spec?.containers ?? [])[0]?.name ?? "", tailLines: 1 }).pipe(
      Stream.decodeText,
      Stream.mkString,
      Effect.map((text) => text.trimEnd().split("\n").at(-1)?.trim() ?? ""),
      Effect.map((line) => (UNRETRIEVABLE_LOG.test(line) ? "" : line)),
      Effect.orElseSucceed(() => ""),
    ));

/**
 * Where `object`, a workload, is (readinessOf), a DaemonSet ready apart from the nodes that are not
 * Ready (readyApartFromNotReady); and, while it waits, which of its pods stop, each as it may until
 * its grace ends, and what it last logged.
 */
const workloadReadiness = (object: KubernetesObject & { spec?: { selector?: { matchLabels?: Record<string, string> } } }): Effect.Effect<Readiness, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const state = yield* readinessOf(object);
    // What a DaemonSet runs on a node not Ready is not ready until that node is, which no wait mends.
    if (state._tag === "Waiting" && object.kind === "DaemonSet" && (yield* readyApartFromNotReady(object as V1DaemonSet))) return READY;
    const matchLabels = object.spec?.selector?.matchLabels;
    if (state._tag !== "Waiting" || !matchLabels) return state;
    const kube = yield* KubeApi;
    const pods = yield* kube.list<V1Pod>(POD, { namespace: object.metadata?.namespace, labelSelector: podSelectorOf(matchLabels) });
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
        const pods = yield* kube.list<V1Pod>(POD, { namespace: ref.namespace, labelSelector: podSelectorOf(set.spec?.selector?.matchLabels ?? {}) });
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

const NODE = kind("Node");
const CONTROLLER_REVISION = kind("ControllerRevision");

/** Whether `node` is Ready, as its condition says. */
const isReady = (node: V1Node): boolean => (node.status?.conditions ?? []).some(({ type, status }) => type === "Ready" && status === "True");

/**
 * The tolerations of the DaemonSet's pods: its template's own, and those its controller gives each beside them, as
 * Kubernetes' AddOrUpdateDaemonPodTolerations does: of a node not ready or unreachable (NoExecute), of its pressures and
 * its cordon (NoSchedule); and of its network not yet set up only for a pod on the host's network.
 */
const daemonTolerations = (template: V1PodSpec | undefined): V1Toleration[] => [
  ...(template?.tolerations ?? []),
  { key: "node.kubernetes.io/not-ready", operator: "Exists", effect: "NoExecute" },
  { key: "node.kubernetes.io/unreachable", operator: "Exists", effect: "NoExecute" },
  ...["disk-pressure", "memory-pressure", "pid-pressure", "unschedulable"].map((taint) => ({ key: `node.kubernetes.io/${taint}`, operator: "Exists", effect: "NoSchedule" })),
  ...(template?.hostNetwork ? [{ key: "node.kubernetes.io/network-unavailable", operator: "Exists", effect: "NoSchedule" }] : []),
];

/** Whether `toleration` tolerates `taint`, as the scheduler matches them. */
const tolerates = ({ key, operator, value, effect }: V1Toleration, taint: V1Taint): boolean =>
  (effect === undefined || effect === "" || effect === taint.effect) &&
  (operator === "Exists" ? key === undefined || key === "" || key === taint.key : key === taint.key && (value ?? "") === (taint.value ?? ""));

/** Whether `node` meets `requirement` of a node selector term, of its labels or, for `metadata.name`, its name. */
const meets = (node: V1Node, of: "labels" | "fields", { key, operator, values = [] }: V1NodeSelectorRequirement): boolean => {
  const found = of === "fields" ? (key === "metadata.name" ? node.metadata?.name : undefined) : node.metadata?.labels?.[key];
  switch (operator) {
    case "In":
      return found !== undefined && values.includes(found);
    case "NotIn":
      return found === undefined || !values.includes(found);
    case "Exists":
      return found !== undefined;
    case "DoesNotExist":
      return found === undefined;
    case "Gt":
    case "Lt": {
      const [bound] = values;
      if (found === undefined || bound === undefined || !/^-?\d+$/u.test(found)) return false;
      return operator === "Gt" ? Number(found) > Number(bound) : Number(found) < Number(bound);
    }
    default:
      return false;
  }
};

/**
 * Whether the DaemonSet `set` runs a pod on `node`, as its controller decides it: the node its template names, where it
 * names one; one of every label its template's node selector names, of a term of its template's required node affinity
 * (as each term is matched, its expressions and fields together, any one term enough), and whose taints that take effect
 * on a pod (NoSchedule, NoExecute) its pods tolerate, those the controller gives them among (daemonTolerations). Its
 * preferred affinity places no pod, and is not read.
 */
const runsOn = (set: V1DaemonSet, node: V1Node): boolean => {
  const template = set.spec?.template.spec;
  const labels = node.metadata?.labels ?? {};
  const named = template?.nodeName === undefined || template.nodeName === "" || template.nodeName === node.metadata?.name;
  const selected = Object.entries(template?.nodeSelector ?? {}).every(([key, value]) => labels[key] === value);
  const terms = template?.affinity?.nodeAffinity?.requiredDuringSchedulingIgnoredDuringExecution?.nodeSelectorTerms;
  const affine = terms === undefined || terms.some(({ matchExpressions = [], matchFields = [] }) =>
    (matchExpressions.length > 0 || matchFields.length > 0) &&
    matchExpressions.every((requirement) => meets(node, "labels", requirement)) && matchFields.every((requirement) => meets(node, "fields", requirement))
  );
  const tolerations = daemonTolerations(template);
  const tolerated = (node.spec?.taints ?? []).filter(({ effect }) => effect === "NoSchedule" || effect === "NoExecute").every((taint) =>
    tolerations.some((toleration) => tolerates(toleration, taint))
  );
  return named && selected && affine && tolerated;
};

/**
 * Where the DaemonSet `set` has rolled: the nodes it runs a pod on (runsOn) that are Ready, cordoned or not, as a cordoned
 * node still runs what is on it; and of those, each on which none of its pods, there or to be there, is of its current
 * revision, running and ready, as one its rollout has not reached is, one crashing, one not yet scheduled, or one whose old
 * pod is gone and whose new one is not yet made, with its pods; and how many it runs a pod on that are not Ready.
 */
const rolloutOf = (set: V1DaemonSet): Effect.Effect<{ readonly ready: number; readonly unrolled: readonly { readonly node: string; readonly pods: readonly V1Pod[] }[]; readonly notReady: number }, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const namespace = set.metadata?.namespace;
    const matchLabels = set.spec?.selector?.matchLabels ?? {};
    const [pods, nodes, revisions] = yield* Effect.all([
      kube.list<V1Pod>(POD, { namespace, labelSelector: podSelectorOf(matchLabels) }),
      kube.list<V1Node>(NODE),
      kube.list<V1ControllerRevision>(CONTROLLER_REVISION, { namespace, labelSelector: selectorOf(matchLabels) }),
    ], { concurrency: 3 });
    // Its current revision, the latest of its own, as its pods' controller-revision-hash label names it.
    const current = revisions.filter(({ metadata }) => metadata?.ownerReferences?.some(({ uid }) => uid === set.metadata?.uid))
      .sort((a, b) => b.revision - a.revision)[0]?.metadata?.labels?.["controller-revision-hash"];
    const eligible = nodes.filter((node) => runsOn(set, node));
    const byNode = new Map<string, V1Pod[]>(eligible.filter(isReady).map(({ metadata }) => [metadata?.name ?? "", []]));
    for (const pod of pods) byNode.get(daemonPodNode(pod) ?? "")?.push(pod);
    const rolled = (pod: V1Pod) =>
      current !== undefined && pod.metadata?.deletionTimestamp === undefined && pod.metadata?.labels?.["controller-revision-hash"] === current &&
      (pod.status?.conditions ?? []).some(({ type, status }) => type === "Ready" && status === "True");
    return {
      ready: byNode.size,
      unrolled: [...byNode].filter(([, on]) => !on.some(rolled)).map(([node, on]) => ({ node, pods: on })),
      notReady: eligible.length - byNode.size,
    };
  });

/**
 * Whether the DaemonSet `set`, whose readiness by its status falls short, is as ready as it can be: its controller has seen
 * its rollout, it runs a pod on a Ready node (rolloutOf) and has rolled on every one, and what its status counts short,
 * of the nodes it runs a pod on (desiredNumberScheduled), updated or ready, is no more than the nodes it runs one on that
 * are not Ready, whose pods no apply readies until they are. One whose every node is not Ready, running nothing ready,
 * is not; one that runs a pod on no node at all, of none desired, is ready by its status already.
 */
const readyApartFromNotReady = (set: V1DaemonSet): Effect.Effect<boolean, KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    if ((set.status?.observedGeneration ?? 0) < (set.metadata?.generation ?? 0)) return false;
    const { ready, unrolled, notReady } = yield* rolloutOf(set);
    const desired = set.status?.desiredNumberScheduled ?? 0;
    const short = desired - Math.min(set.status?.updatedNumberScheduled ?? 0, set.status?.numberReady ?? 0);
    return ready > 0 && unrolled.length === 0 && short <= notReady;
  });

/**
 * Waits, as `options` say, until the DaemonSet `ref` has rolled on the Ready nodes it runs a pod on (rolloutOf), cordoned or
 * not, those not Ready left be: each runs one of its current revision, ready; and one that runs a pod on any node runs one on
 * a Ready node. Whether it has; where not, said as a warning, naming each node it has not rolled on, with what is wrong with
 * its pods and what each last logged, as a crashing one's says why, for its caller to go on regardless.
 */
export const awaitRolled = (ref: ObjectRef, options: WaitOptions): Effect.Effect<boolean, never, KubeApi> =>
  awaitAll(
    [ref],
    (of) =>
      Effect.gen(function*() {
        const set = yield* (yield* KubeApi).get<V1DaemonSet>(of);
        if (!set) return waiting("not found");
        if ((set.status?.observedGeneration ?? 0) < (set.metadata?.generation ?? 0)) return waiting("not yet rolled out");
        const { ready, unrolled } = yield* rolloutOf(set);
        if (unrolled.length > 0) return waiting(`not rolled on ${unrolled.map(({ node }) => node).join(", ")}`);
        return ready === 0 && (set.status?.desiredNumberScheduled ?? 0) > 0 ? waiting("no node it runs a pod on is Ready") : READY;
      }),
    (of) =>
      Effect.gen(function*() {
        const set = yield* (yield* KubeApi).get<V1DaemonSet>(of);
        if (!set) return [];
        const { unrolled } = yield* rolloutOf(set);
        return yield* Effect.forEach(unrolled, ({ node, pods }) =>
          Effect.map(Effect.forEach(pods, (pod) => Effect.map(lastLogged(pod), (line) => [...podProblems(pod), ...(line === "" ? [] : [`pod ${pod.metadata?.name} last saying: ${line}`])])), (said) =>
            `node ${node}: ${pods.length === 0 ? "no pod of it yet" : said.flat().join("; ") || "its pod is not yet of the current revision"}`));
      }),
    options,
  ).pipe(
    Effect.as(true),
    Effect.catch((error) => Effect.logWarning(`going on though ${describeRef(ref)} has not rolled on every Ready node it runs a pod on: ${error.message}`).pipe(Effect.as(false))),
  );
