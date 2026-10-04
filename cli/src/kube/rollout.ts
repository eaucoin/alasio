/**
 * Waiting for what was applied to run: a CRD until it is established, a Job until it
 * completes, a Deployment or StatefulSet until its rollout is done; and waiting for what
 * was deleted to be gone. A wait says what it still waits for as that changes, and when
 * it gives up, why each thing is not ready, from its pods' state and the cluster's
 * warnings about them.
 */
import type { CoreV1Event, KubernetesObject, V1ContainerStatus, V1Pod } from "@kubernetes/client-node";
import { Duration, Effect, Schedule, Schema } from "effect";

import { describeRef, kind, KubeApi, type KubeApiError, type ObjectRef } from "./api.ts";

/** Where a thing waited for is: ready, still waiting (and what it is at), or failed, which no wait mends. */
export type Readiness =
  | { readonly _tag: "Ready" }
  | { readonly _tag: "Waiting"; readonly status: string }
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
    readonly conditions?: readonly { readonly type: string; readonly status: string; readonly reason?: string; readonly message?: string }[];
  };
}

/** Where `object` is: a CRD, Job, Deployment or StatefulSet by its status, anything else ready once it exists. */
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
    default:
      return READY;
  }
}

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

/** A look found something still waiting; the wait's NotReadyInTime says what when it gives up. */
class Pending extends Schema.TaggedError<Pending>()("Pending", {}) {}

/** How long a wait may take, and how often it looks. */
export interface WaitOptions {
  readonly timeout: Duration.Input;
  readonly poll: Duration.Input;
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

/** When `event` last happened. */
const lastSeen = (event: CoreV1Event): number =>
  new Date(event.lastTimestamp ?? event.eventTime ?? event.metadata.creationTimestamp ?? 0).getTime();

/**
 * Why `ref` is not ready: its pods' problems, and the latest of the cluster's warnings
 * about them, it, or what it made (a Deployment's ReplicaSets, named after it).
 */
export const diagnose = Effect.fnUntraced(function*(ref: ObjectRef): Effect.fn.Return<string[], KubeApiError, KubeApi> {
  const kube = yield* KubeApi;
  const object = yield* kube.get<KubernetesObject & { spec?: { selector?: { matchLabels?: Record<string, string> } } }>(ref);
  const matchLabels = object?.spec?.selector?.matchLabels;
  const pods = matchLabels ? yield* kube.list<V1Pod>(POD, { namespace: ref.namespace, labelSelector: selectorOf(matchLabels) }) : [];
  const warnings = ref.namespace ? yield* kube.list<CoreV1Event>(EVENT, { namespace: ref.namespace, fieldSelector: "type=Warning" }) : [];
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
 * time runs out.
 */
const awaitAll = Effect.fnUntraced(function*(
  refs: readonly ObjectRef[],
  look: (ref: ObjectRef) => Effect.Effect<Readiness, KubeApiError, KubeApi>,
  why: (ref: ObjectRef) => Effect.Effect<string[], KubeApiError, KubeApi>,
  { timeout, poll }: WaitOptions,
): Effect.fn.Return<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> {
  // What the last look found still waiting, and what was last said of it.
  let pending: ReadonlyArray<readonly [ObjectRef, string]> = [];
  let said = "";
  const once = Effect.gen(function*() {
    const states = yield* Effect.forEach(refs, (ref) => Effect.map(look(ref), (state) => [ref, state] as const), { concurrency: 8 });
    for (const [ref, state] of states) {
      if (state._tag === "Failed") return yield* new RolloutFailed({ failed: { object: describeRef(ref), status: state.reason, why: yield* why(ref) } });
    }
    pending = states.flatMap(([ref, state]) => (state._tag === "Waiting" ? [[ref, state.status] as const] : []));
    if (pending.length === 0) return;
    const saying = pending.map(([ref, status]) => `${ref.name} (${status})`).join(", ");
    if (saying !== said) {
      said = saying;
      yield* Effect.logInfo(`waiting for ${saying}`);
    }
    return yield* new Pending();
  });
  yield* once.pipe(
    Effect.retry({
      while: (error) => error._tag !== "RolloutFailed",
      schedule: Schedule.max([Schedule.spaced(poll), Schedule.during(timeout)]),
    }),
    Effect.catchTag("Pending", () =>
      Effect.forEach(pending, ([ref, status]) => Effect.map(why(ref), (lines) => ({ object: describeRef(ref), status, why: lines }))).pipe(
        Effect.flatMap((notReady) => Effect.fail(new NotReadyInTime({ within: Duration.toSeconds(timeout), waiting: notReady }))),
      )),
  );
});

/** The object `ref`, or null when there is none. */
const current = (ref: ObjectRef): Effect.Effect<KubernetesObject | null, KubeApiError, KubeApi> => Effect.flatMap(KubeApi, (kube) => kube.get(ref));

/** Waits until every ref is ready (readiness). */
export const awaitReady = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(refs, (ref) => Effect.map(current(ref), (object) => (object ? readiness(object) : waiting("not found"))), diagnose, options);

/** Waits until every ref is gone, its deletion done. */
export const awaitGone = (refs: readonly ObjectRef[], options: WaitOptions): Effect.Effect<void, KubeApiError | NotReadyInTime | RolloutFailed, KubeApi> =>
  awaitAll(
    refs,
    (ref) => Effect.map(current(ref), (object) => (object ? waiting("being deleted") : READY)),
    (ref) => Effect.map(current(ref), (object) => (object?.metadata?.finalizers?.length ? [`waits on its finalizers: ${object.metadata.finalizers.join(", ")}`] : [])),
    options,
  );
