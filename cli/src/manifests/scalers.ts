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

/** A trigger of KEDA's: its type and what it is given, as its scaler's documentation says. */
export interface Trigger {
  readonly type: string;
  readonly metricType?: "Utilization";
  readonly metadata: Readonly<Record<string, string>>;
}

/** A ScaledObject, as far as alasio writes and reads one. */
export interface ScaledObject extends KubernetesObject {
  readonly spec: {
    readonly scaleTargetRef: { readonly apiVersion?: string | undefined; readonly kind?: string | undefined; readonly name: string };
    readonly minReplicaCount: number;
    readonly maxReplicaCount: number;
    readonly triggers: readonly Trigger[];
  };
}

/**
 * The CPU's use, averaged over a workload's pods, as a share of what they request: which
 * the cluster's metrics server measures, and which KEDA keeps at `percent`.
 */
export const cpuUtilization = (percent: number): Trigger => ({ type: "cpu", metricType: "Utilization", metadata: { value: String(percent) } });

/** The ScaledObject of `workload`, of `scale`, on `triggers`: none where the scale is a fixed count. */
export function scaledObject(workload: V1Deployment | V1StatefulSet, scale: Scale, triggers: readonly Trigger[]): ScaledObject[] {
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
      triggers,
    },
  }];
}
