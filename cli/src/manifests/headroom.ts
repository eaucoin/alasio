/**
 * Headroom (autoscaling.headroom): placeholder pods that do nothing but take room, at the
 * lowest priority (./priorities.ts), which any other pod of the cluster's preempts. A node
 * autoscaler sees them as demand and keeps a node's worth of room ready, so a real pod
 * lands at once, in their place, while a new node comes up behind it for them. Of use
 * only where a node autoscaler runs (on a kubeconfig target whose cluster has one); on one
 * machine they only take its room, so there are none unless asked for. Their requests are
 * the room they keep, so no VerticalPodAutoscaler sizes them to the nothing they use.
 */
import type { KubernetesObject, V1Deployment } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, imagePullSecrets, labels, NAMESPACE, replicasOf, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";

/** The user of the pause image. */
const PAUSE_USER = 65535;

/** The headroom's Deployment, when there is any: as many placeholders as its scale fixes, each requesting its resources. */
export function headroomObjects(config: InstallConfig): KubernetesObject[] {
  const { scaling, image, resources } = config.autoscaling.headroom;
  if (scaling.max === 0) return [];
  const deployment: V1Deployment = {
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: componentName("headroom"), namespace: NAMESPACE, labels: labels("headroom") },
    spec: {
      ...replicasOf(scaling),
      selector: { matchLabels: selectorLabels("headroom") },
      template: {
        metadata: { labels: labels("headroom") },
        spec: {
          ...priority("headroom"),
          automountServiceAccountToken: false,
          securityContext: restrictedPod(PAUSE_USER, PAUSE_USER),
          ...imagePullSecrets(config),
          // Preempted, it gives its room at once.
          terminationGracePeriodSeconds: 0,
          containers: [{
            name: "pause",
            image: imageReference(image),
            imagePullPolicy: "IfNotPresent",
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources,
          }],
        },
      },
    },
  };
  return [deployment];
}
