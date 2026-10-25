/**
 * What scales an environment's alasio, as `alasio status` says it: each of its KEDA triggers'
 * value now, as KEDA's external metrics answer it, in the order of the ScaledObject's triggers,
 * which the HorizontalPodAutoscaler KEDA makes of it names its metrics in; and when it was last
 * woken: by KEDA's event of waking it (KEDAScaleTargetActivated), with the trigger it names,
 * while the event lasts, about an hour; otherwise when alasio last started, as it keeps the time
 * (src/kube/started.ts), the cause not kept. And, asleep, since when: by KEDA's event of scaling
 * it to none (KEDAScaleTargetDeactivated), while that lasts.
 */
import type { CoreV1Event, V1ConfigMap } from "@kubernetes/client-node";
import { Effect } from "effect";

import { STARTED_CONFIG_MAP, STARTED_KEY } from "../../src/kube/started-map.ts";
import { hasStatus, kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import type { ScaledObject, Trigger } from "./manifests/scalers.ts";

/** The HorizontalPodAutoscaler KEDA makes of a ScaledObject, as far as it is read: the external metrics it scales on. */
interface HorizontalPodAutoscaler {
  readonly spec?: { readonly metrics?: readonly { readonly external?: { readonly metric?: { readonly name?: string } } }[] };
}

/** A trigger, and its value now: null where KEDA answered none. */
export interface TriggerValue {
  readonly trigger: Trigger;
  readonly value: string | null;
}

/** What scales alasio, as the module says, and why what was not seen was not. */
export interface AlasioWakes {
  readonly triggers: readonly TriggerValue[] | { readonly unseen: string };
  /** KEDA's last event of waking it: when, and the trigger it names, where it names one. */
  readonly woken: { readonly at: string; readonly by: string | null } | null;
  /** When alasio last started, as it keeps it, where it does. */
  readonly started: string | null;
  /** KEDA's last event of scaling it to none: when, while the event lasts. */
  readonly slept: string | null;
}

/** The reason of KEDA's event of waking a ScaledObject's workload from none. */
export const WOKEN_REASON = "KEDAScaleTargetActivated";
/** The reason of KEDA's event of scaling a ScaledObject's workload to none. */
export const SLEPT_REASON = "KEDAScaleTargetDeactivated";

/** What `look` finds, or none where it fails as the kubeconfig may not, or the cluster has not (403, 404): what it says then. */
const orNot = <A>(look: Effect.Effect<A, KubeApiError, KubeApi>): Effect.Effect<A | null, KubeApiError, KubeApi> =>
  look.pipe(Effect.catchIf((error) => hasStatus(403)(error) || hasStatus(404)(error), () => Effect.succeed(null)));

/** Each of `scaler`'s triggers' values now, as KEDA's external metrics answer them, or why they were not seen. */
const triggerValues = (scaler: ScaledObject): Effect.Effect<AlasioWakes["triggers"], never, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const namespace = scaler.metadata?.namespace ?? "";
    const name = scaler.metadata?.name ?? "";
    const hpa = yield* orNot(kube.get<HorizontalPodAutoscaler & { kind?: string }>({ ...kind("HorizontalPodAutoscaler"), namespace, name: `keda-hpa-${name}` }));
    const metrics = hpa?.spec?.metrics?.map(({ external }) => external?.metric?.name);
    if (!metrics || metrics.length !== scaler.spec.triggers.length) return { unseen: "KEDA scales it on no metrics yet" };
    return yield* Effect.forEach(scaler.spec.triggers, (trigger, index) =>
      orNot(kube.externalMetric(namespace, metrics[index] ?? "", `scaledobject.keda.sh/name=${name}`)).pipe(
        Effect.map((values): TriggerValue => ({ trigger, value: values?.[0] ?? null })),
      ));
  }).pipe(Effect.catch((error) => Effect.succeed({ unseen: `KEDA's metrics were not read: ${error.message}` })));

/** KEDA's last event of `reason` of `scaler`'s workload, while the event lasts: when, and what it said. */
const lastEvent = (scaler: ScaledObject, reason: string): Effect.Effect<{ readonly at: string; readonly message: string } | null, never, KubeApi> =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const events = (yield* orNot(kube.list<CoreV1Event>(kind("Event"), {
      namespace: scaler.metadata?.namespace,
      fieldSelector: `involvedObject.kind=ScaledObject,involvedObject.name=${scaler.metadata?.name ?? ""},reason=${reason}`,
    }))) ?? [];
    const at = (event: CoreV1Event) => String(event.lastTimestamp ?? event.eventTime ?? event.firstTimestamp ?? "");
    const [last] = events.toSorted((a, b) => at(b).localeCompare(at(a)));
    return last ? { at: new Date(at(last)).toISOString(), message: last.message ?? "" } : null;
  }).pipe(Effect.orElseSucceed(() => null));

/** KEDA's last event of waking `scaler`'s workload, while the event lasts. */
const lastWoken = (scaler: ScaledObject): Effect.Effect<AlasioWakes["woken"], never, KubeApi> =>
  Effect.map(lastEvent(scaler, WOKEN_REASON), (event) => (event ? { at: event.at, by: /triggered by (.+)$/u.exec(event.message)?.[1] ?? null } : null));

/** When alasio last started, as it keeps it in its namespace (src/kube/started.ts). */
const lastStarted = (namespace: string): Effect.Effect<string | null, never, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => orNot(kube.get<V1ConfigMap>({ ...kind("ConfigMap"), namespace, name: STARTED_CONFIG_MAP }))).pipe(
    Effect.map((found) => found?.data?.[STARTED_KEY] ?? null),
    Effect.orElseSucceed(() => null),
  );

/** What scales the alasio `scaler` scales, as the module says. */
export const wakesOf = (scaler: ScaledObject): Effect.Effect<AlasioWakes, never, KubeApi> =>
  Effect.all({
    triggers: triggerValues(scaler),
    woken: lastWoken(scaler),
    started: lastStarted(scaler.metadata?.namespace ?? ""),
    slept: Effect.map(lastEvent(scaler, SLEPT_REASON), (event) => event?.at ?? null),
  }, { concurrency: "unbounded" });

/** A trigger and its value, as people read it: what waits on a topic, or the harnesses bound. */
function describeTrigger({ trigger: { type, metadata }, value }: TriggerValue): string {
  const of = value ?? "not measured";
  if (type === "kafka") return `${of} waiting on ${metadata["topic"] ?? "its topic"}`;
  if (type === "metrics-api" && metadata["valueLocation"] === "bound") return `${of} of its harnesses bound`;
  return `${type} at ${of}`;
}

/** The lines `alasio status` says of what scales alasio, under its scale's. */
export function describeWakes({ triggers, woken, started }: AlasioWakes): string[] {
  return [
    "unseen" in triggers ? `its triggers: not seen, as ${triggers.unseen}` : `its triggers: ${triggers.map(describeTrigger).join(", ")}`,
    woken ? `last woken at ${woken.at}${woken.by ? `, by ${woken.by}` : ""}` : started ? `last started at ${started}, its cause not kept` : "its last start: not kept",
  ];
}

/** What wakes the workload `scaler` scales, as people read it: what waits on its triggers' topics, its harnesses bound, or its triggers' kinds. */
export function describeWakers(scaler: ScaledObject): string {
  const each = scaler.spec.triggers.map(({ type, metadata }) =>
    type === "kafka" ? `what waits on ${metadata["topic"] ?? "its topic"}` : type === "metrics-api" && metadata["valueLocation"] === "bound" ? "a harness bound" : `its ${type} trigger`
  );
  return each.length > 1 ? `${each.slice(0, -1).join(", ")} or ${each.at(-1)}` : each[0] ?? "nothing";
}

/** A workload `scaler` let sleep at none, as `alasio status` and `alasio logs` say it: since when, where KEDA's event of it lasts, and what wakes it. */
export function describeAsleep(scaler: ScaledObject, { slept }: Pick<AlasioWakes, "slept">): string {
  return `asleep at no replica${slept ? ` since ${slept}` : ""}, as KEDA lets it while nothing waits for it, woken by ${describeWakers(scaler)}`;
}
