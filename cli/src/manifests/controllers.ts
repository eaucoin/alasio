/**
 * What the autoscalers' controllers alasio installs (./keda.ts, ./vpa.ts) have alike:
 * one replica each, in alasio's namespace, restricted, on their pinned distroless images'
 * user, under the floor's priority, as they scale and size what sleeps.
 */
import type { KubernetesObject, V1Container, V1Deployment, V1PolicyRule, V1Volume } from "@kubernetes/client-node";

import { componentName, imagePullSecrets, labels, NAMESPACE, ONE, replicasOf, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";
import { verticalPodAutoscaler } from "./sizing.ts";

/** The user of KEDA's and VPA's distroless images. */
const NONROOT = 65532;

/** The Deployment of the controller `component`: `container`, as `serviceAccountName`, with `volumes`. */
function controllerDeployment(config: InstallConfig, component: string, serviceAccountName: string, container: V1Container, volumes: V1Volume[] = []): V1Deployment {
  return {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: labels(component) },
    spec: {
      ...replicasOf(ONE),
      selector: { matchLabels: selectorLabels(component) },
      template: {
        metadata: { labels: labels(component) },
        spec: {
          serviceAccountName,
          ...priority("floor"),
          securityContext: restrictedPod(NONROOT, NONROOT),
          ...imagePullSecrets(config),
          terminationGracePeriodSeconds: 10,
          ...(volumes.length > 0 ? { volumes } : {}),
          containers: [{ ...container, imagePullPolicy: "IfNotPresent", securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true } }],
        },
      },
    },
  };
}

/** The controller `component`'s Deployment (controllerDeployment) and its VerticalPodAutoscaler, which resizes it in place. */
export function sizedController(...args: Parameters<typeof controllerDeployment>): KubernetesObject[] {
  const deployment = controllerDeployment(...args);
  return [deployment, verticalPodAutoscaler(deployment, "InPlace")];
}

/** A rule granting `verbs` on `resources` of `apiGroups`, of those named `resourceNames` alone when given. */
export function rule(apiGroups: string[], resources: string[], verbs: string[], resourceNames?: string[]): V1PolicyRule {
  return { apiGroups, resources, verbs, ...(resourceNames ? { resourceNames } : {}) };
}
