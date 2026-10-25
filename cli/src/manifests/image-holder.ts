/**
 * alasio's image, held on each node alasio may run on: a DaemonSet of one container of the
 * image doing nothing, on alasio's nodes as alasio's pods may be scheduled there (its
 * nodeSelector, tolerations and node affinity), so each of them pulls the image as the
 * installation is made or upgraded, and keeps it, in use, from kubelet's collection of images.
 * alasio's image is pulled whole (tooling/push-whole.sh), its start read from its node's disk:
 * a wake KEDA schedules on a node that never ran alasio would wait for the whole of it, about
 * 80 s, and the press that woke it with it. The sessions' nodes hold it as their node agents
 * run it (./node-agent.ts), so the harnesses' pools and the gateway, which run it there, wait
 * on no pull either.
 */
import type { KubernetesObject, V1DaemonSet } from "@kubernetes/client-node";

import { imageReference } from "../images.ts";
import { componentName, given, imagePullSecrets, labels, NAMESPACE, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";

/** The holder's component. */
export const IMAGE_HOLDER_COMPONENT = "image";

/** The objects that hold alasio's image on its nodes, as the module says. */
export function imageHolderObjects(config: InstallConfig): KubernetesObject[] {
  const { alasio } = config;
  const holder: V1DaemonSet = {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: componentName(IMAGE_HOLDER_COMPONENT), namespace: NAMESPACE, labels: labels(IMAGE_HOLDER_COMPONENT) },
    spec: {
      selector: { matchLabels: selectorLabels(IMAGE_HOLDER_COMPONENT) },
      // Every node's at once, as an upgrade's image is pulled on each before a wake asks for it there, and the holder serves nothing.
      updateStrategy: { type: "RollingUpdate", rollingUpdate: { maxUnavailable: "100%", maxSurge: 0 } },
      template: {
        metadata: { labels: labels(IMAGE_HOLDER_COMPONENT) },
        spec: {
          ...priority("component"),
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          terminationGracePeriodSeconds: 1,
          securityContext: restrictedPod(alasio.runAsUser, alasio.runAsGroup),
          ...imagePullSecrets(config),
          containers: [{
            name: "image",
            image: imageReference(config.images.alasio),
            imagePullPolicy: config.images.pullPolicy,
            // Its arguments, not its command, so it runs under the image's entrypoint, tini, which passes the
            // SIGTERM its pod is stopped with on to it: as PID 1, sleep would ignore it.
            args: ["sleep", "infinity"],
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: { requests: { cpu: "1m", memory: "8Mi" }, limits: { memory: "16Mi" } },
          }],
          ...given("nodeSelector", { ...alasio.nodeSelector }),
          ...given("tolerations", [...alasio.tolerations]),
          // Of alasio's affinity, what says which nodes: its pods' affinities place none of a DaemonSet's.
          ...given("affinity", alasio.affinity.nodeAffinity ? { nodeAffinity: alasio.affinity.nodeAffinity } : {}),
        },
      },
    },
  };
  return [holder];
}
