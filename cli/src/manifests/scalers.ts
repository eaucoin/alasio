/**
 * A component's scaler, beside its workload in its component's manifest: a KEDA
 * ScaledObject (./keda.ts), which moves the workload's replicas between its scale's min
 * and max on what its triggers measure, and owns its replicas, which the workload is
 * rendered without (./common.ts replicasOf). A component whose scale is a fixed count has
 * none.
 */
import type { KubernetesObject, V1Deployment, V1StatefulSet } from "@kubernetes/client-node";

import { scaled } from "./common.ts";
import type { Scale } from "./config.ts";

/** A trigger of KEDA's: its type, what it is given, as its scaler's documentation says, and the TriggerAuthentication it authenticates with, if any. */
export interface Trigger {
  readonly type: string;
  readonly metricType?: "Utilization";
  readonly metadata: Readonly<Record<string, string>>;
  readonly authenticationRef?: { readonly name: string };
}

/** A TriggerAuthentication, as far as alasio writes one: each parameter of its scalers', from a key of a Secret of its namespace. */
export interface TriggerAuthentication extends KubernetesObject {
  readonly spec: { readonly secretTargetRef: readonly { readonly parameter: string; readonly name: string; readonly key: string }[] };
}

/** A ScaledObject, as far as alasio writes and reads one. */
export interface ScaledObject extends KubernetesObject {
  readonly spec: {
    readonly scaleTargetRef: { readonly apiVersion?: string | undefined; readonly kind?: string | undefined; readonly name: string };
    readonly minReplicaCount: number;
    readonly maxReplicaCount: number;
    /** How often KEDA looks at its triggers, in seconds; its default, 30, where none is said. */
    readonly pollingInterval?: number;
    /** How long its triggers must have asked for none before it scales the workload to its minimum, in seconds; KEDA's default, 300, where none is said. */
    readonly cooldownPeriod?: number;
    readonly triggers: readonly Trigger[];
    /** How the HorizontalPodAutoscaler KEDA makes of it scales: here, how long it holds off scaling up. */
    readonly advanced?: { readonly horizontalPodAutoscalerConfig: { readonly behavior: { readonly scaleUp: { readonly stabilizationWindowSeconds: number } } } };
  };
  /** As KEDA reports it: whether it is ready, active or paused, and when it last became active. */
  readonly status?: {
    readonly conditions?: readonly { readonly type: string; readonly status: string; readonly reason?: string; readonly message?: string }[];
    readonly lastActiveTime?: string;
  };
}

/** How often a ScaledObject looks at its triggers, and how long they must ask for none before it scales its workload to its minimum, in seconds. */
export interface ScalingTiming {
  readonly pollingSeconds: number;
  readonly cooldownSeconds: number;
}

/** The ScaledObject of `workload`, of `scale`, on `triggers`, as often and as long as `timing` says, KEDA's defaults where none: none where the scale is a fixed count. */
export function scaledObject(workload: V1Deployment | V1StatefulSet, scale: Scale, triggers: readonly Trigger[], timing?: ScalingTiming): ScaledObject[] {
  return scaledOn(workload, scale, triggers, undefined, timing);
}

/**
 * The ScaledObject of `workload`, of `scale`, on its CPU's use, averaged over its pods, as
 * a share of what they request, which the cluster's metrics server measures and KEDA keeps
 * at `percent`: none where the scale is a fixed count. A pod starting uses more CPU than
 * it will, which would scale it up as each rolls out, a replica more for each; so KEDA
 * scales it up only on a use that has lasted `startUpSeconds`, longer than its start-up's
 * burst.
 */
export function cpuScaledObject(workload: V1Deployment | V1StatefulSet, scale: Scale, { percent, startUpSeconds }: { readonly percent: number; readonly startUpSeconds: number }): ScaledObject[] {
  const cpu: Trigger = { type: "cpu", metricType: "Utilization", metadata: { value: String(percent) } };
  return scaledOn(workload, scale, [cpu], { horizontalPodAutoscalerConfig: { behavior: { scaleUp: { stabilizationWindowSeconds: startUpSeconds } } } });
}

function scaledOn(workload: V1Deployment | V1StatefulSet, scale: Scale, triggers: readonly Trigger[], advanced: ScaledObject["spec"]["advanced"], timing?: ScalingTiming): ScaledObject[] {
  if (!scaled(scale)) return [];
  const { name = "", namespace = "", labels = {} } = workload.metadata ?? {};
  return [{
    apiVersion: "keda.sh/v1alpha1",
    kind: "ScaledObject",
    metadata: { name, namespace, labels },
    spec: {
      scaleTargetRef: { apiVersion: workload.apiVersion, kind: workload.kind, name },
      minReplicaCount: scale.min,
      maxReplicaCount: scale.max,
      ...(timing ? { pollingInterval: timing.pollingSeconds, cooldownPeriod: timing.cooldownSeconds } : {}),
      triggers,
      ...(advanced ? { advanced } : {}),
    },
  }];
}
