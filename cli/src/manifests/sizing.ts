/**
 * Each workload's VerticalPodAutoscaler, beside it in its component's manifest: VPA
 * (./vpa.ts) recommends every container's requests from what it uses, between a floor as
 * small as alasio's smallest components use and the container's memory limit, and leaves
 * limits as they are. How it applies them is the workload's sizing:
 *
 * - `InPlace`, for what keeps no state and runs no turn: its running pods resized in place
 *   where the cluster resizes pods (Kubernetes' in-place pod resize), and never recreated
 *   to be, as VPA's InPlaceOrRecreate would where it does not, or where a node lacks the
 *   room: a pod not resized keeps its requests until it is next made, which VPA sizes;
 * - `Recommended`, for what keeps state (Neon's storage, the object store, workspace
 *   storage's metadata and its CSI driver), whose recreation would stop the floor; for
 *   what alasio's turns cannot do without a moment (alasio's pod, which runs them in its
 *   harnesses; its database, the compute; and the storage controller, which the compute's
 *   start needs); recommended alone, which `alasio status` shows beside its requests.
 *
 * Sessions' pods have none: agent-sandbox makes each, a pod of its own.
 */
import type { KubernetesObject, V1Container, V1DaemonSet, V1Deployment, V1StatefulSet } from "@kubernetes/client-node";

import { VOLUME_DRIVER_LABEL } from "../kube/apply.ts";

/** How a workload's requests follow its recommendation. */
export type Sizing = "InPlace" | "Recommended";

/** VPA's update mode of each sizing: InPlace is alpha in VPA 1.8.0, behind the feature gate ./vpa.ts gives its updater and admission controller. */
const UPDATE_MODES: Readonly<Record<Sizing, string>> = { InPlace: "InPlace", Recommended: "Off" };

/** The least any container is recommended: a little under what alasio's smallest components use (a storage broker's 13 MiB, a Valkey's 6). */
const MIN_ALLOWED = { cpu: "5m", memory: "16Mi" };

/** The labels of a workload's VerticalPodAutoscaler: the workload's, but that no volume needs it kept (VOLUME_DRIVER_LABEL). */
function sizingLabels(labels: Readonly<Record<string, string>>): Record<string, string> {
  return Object.fromEntries(Object.entries(labels).filter(([key]) => key !== VOLUME_DRIVER_LABEL));
}

/** A VerticalPodAutoscaler, as far as alasio writes and reads one: its target, how it applies its recommendation, and its recommendation. */
export interface VerticalPodAutoscaler extends KubernetesObject {
  readonly spec: {
    readonly targetRef: { readonly apiVersion?: string | undefined; readonly kind?: string | undefined; readonly name?: string | undefined };
    readonly updatePolicy: { readonly updateMode: string; readonly minReplicas?: number };
    readonly resourcePolicy: { readonly containerPolicies: readonly object[] };
  };
  readonly status?: { readonly recommendation?: { readonly containerRecommendations?: readonly { readonly containerName: string; readonly target?: Readonly<Record<string, string>> }[] } };
}

/** The VerticalPodAutoscaler of `workload`, sized as `sizing` says. */
export function verticalPodAutoscaler(workload: V1Deployment | V1StatefulSet | V1DaemonSet, sizing: Sizing): VerticalPodAutoscaler {
  const containers: readonly V1Container[] = workload.spec?.template.spec?.containers ?? [];
  return {
    apiVersion: "autoscaling.k8s.io/v1",
    kind: "VerticalPodAutoscaler",
    metadata: { name: workload.metadata?.name ?? "", namespace: workload.metadata?.namespace ?? "", labels: sizingLabels(workload.metadata?.labels ?? {}) },
    spec: {
      targetRef: { apiVersion: workload.apiVersion, kind: workload.kind, name: workload.metadata?.name },
      // A workload of one replica, as most of alasio's are, is resized too.
      updatePolicy: { updateMode: UPDATE_MODES[sizing], ...(sizing === "InPlace" ? { minReplicas: 1 } : {}) },
      resourcePolicy: {
        containerPolicies: containers.map(({ name, resources }) => {
          const memory = resources?.limits?.["memory"];
          return { containerName: name, minAllowed: MIN_ALLOWED, ...(memory ? { maxAllowed: { memory } } : {}), controlledValues: "RequestsOnly" };
        }),
      },
    },
  };
}
