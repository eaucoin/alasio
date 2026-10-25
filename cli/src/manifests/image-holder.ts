/**
 * alasio's images that wake from none, held on each node their workloads may run on: of
 * each, a DaemonSet of one container of the image doing nothing, on its workload's nodes as
 * that workload's pods may be scheduled there (its nodeSelector, tolerations and node
 * affinity), so each of them pulls the image as the installation is made or upgraded, and
 * keeps it, in use, from kubelet's collection of images. alasio's images are pulled whole
 * (tooling/push-whole.sh), each read from its node's disk: a wake KEDA schedules on a node
 * that never ran its workload would wait for the whole of its image first, and what woke it
 * with it. So alasio's own image, about 80 s whole, which a press wakes; and the lake's, 7 s
 * whole on a runner, of its loader and its intake, which KEDA wakes from none as what they
 * load waits. The sessions' nodes hold alasio's as their node agents run it (./node-agent.ts),
 * so the harnesses' pools and the gateway, which run it there, wait on no pull either; the
 * images that run always, as the log's, grafana's, the collector's and the webhook's, wake on
 * no node, so are held by their own pods.
 */
import type { KubernetesObject, V1Affinity, V1DaemonSet, V1Toleration } from "@kubernetes/client-node";

import { type ImageRef, imageReference } from "../images.ts";
import { componentName, given, imagePullSecrets, labels, lakeRuns, NAMESPACE, restrictedContainer, restrictedPod, selectorLabels } from "./common.ts";
import type { InstallConfig } from "./config.ts";
import { priority } from "./priorities.ts";

/** The holder's component of alasio's image. */
export const IMAGE_HOLDER_COMPONENT = "image";
/** The holder's component of the lake's image. */
export const LAKE_IMAGE_HOLDER_COMPONENT = "lake-image";

/**
 * What a holder runs where its image's entrypoint is no tini, as the lake's, node's image's, is
 * not: a shell, PID 1, which the kernel sends no signal it has no handler for, so one that
 * waits on nothing but a SIGTERM, which it traps and ends on, as its pod is stopped.
 */
export const UNTIL_TERM = ["sh", "-c", "trap 'exit 0' TERM; while :; do sleep 86400 & wait $!; done"] as const;

/** An image to hold, as its workload is placed: its holder's component, the image, what its holder runs, the user it runs as, and its workload's nodes. */
interface Held {
  readonly component: string;
  readonly image: ImageRef;
  readonly args: readonly string[];
  readonly runAsUser: number;
  readonly runAsGroup: number;
  readonly nodeSelector: Readonly<Record<string, string>>;
  readonly tolerations: readonly V1Toleration[];
  readonly affinity?: V1Affinity | undefined;
}

/** The DaemonSet that holds `held`'s image on its workload's nodes, as the module says. */
function holder(config: InstallConfig, { component, image, args, runAsUser, runAsGroup, nodeSelector, tolerations, affinity }: Held): V1DaemonSet {
  return {
    apiVersion: "apps/v1",
    kind: "DaemonSet",
    metadata: { name: componentName(component), namespace: NAMESPACE, labels: labels(component) },
    spec: {
      selector: { matchLabels: selectorLabels(component) },
      // Every node's at once, as an upgrade's image is pulled on each before a wake asks for it there, and the holder serves nothing.
      updateStrategy: { type: "RollingUpdate", rollingUpdate: { maxUnavailable: "100%", maxSurge: 0 } },
      template: {
        metadata: { labels: labels(component) },
        spec: {
          ...priority("component"),
          automountServiceAccountToken: false,
          enableServiceLinks: false,
          terminationGracePeriodSeconds: 1,
          securityContext: restrictedPod(runAsUser, runAsGroup),
          ...imagePullSecrets(config),
          containers: [{
            name: "image",
            image: imageReference(image),
            imagePullPolicy: config.images.pullPolicy,
            // Its arguments, not its command, so it runs under the image's entrypoint, which it ends with as its pod is
            // stopped: alasio's, tini, passing the SIGTERM on to it; another's, its own (UNTIL_TERM).
            args: [...args],
            securityContext: { ...restrictedContainer(), readOnlyRootFilesystem: true },
            resources: { requests: { cpu: "1m", memory: "8Mi" }, limits: { memory: "16Mi" } },
          }],
          ...given("nodeSelector", { ...nodeSelector }),
          ...given("tolerations", [...tolerations]),
          // Of its workload's affinity, what says which nodes: its pods' affinities place none of a DaemonSet's.
          ...given("affinity", affinity?.nodeAffinity ? { nodeAffinity: affinity.nodeAffinity } : {}),
        },
      },
    },
  };
}

/** The objects that hold alasio's images that wake from none on their workloads' nodes, as the module says: alasio's, and the lake's where it runs. */
export function imageHolderObjects(config: InstallConfig): KubernetesObject[] {
  const { alasio, neon } = config;
  return [
    holder(config, { component: IMAGE_HOLDER_COMPONENT, image: config.images.alasio, args: ["sleep", "infinity"], runAsUser: alasio.runAsUser, runAsGroup: alasio.runAsGroup, nodeSelector: alasio.nodeSelector, tolerations: alasio.tolerations, affinity: alasio.affinity }),
    // The lake's on the stack's nodes, as its loader and intake are placed (./common.ts stackPodSpec).
    ...(lakeRuns(config) ? [holder(config, { component: LAKE_IMAGE_HOLDER_COMPONENT, image: config.images.lake, args: UNTIL_TERM, runAsUser: neon.runAsUser, runAsGroup: neon.runAsGroup, nodeSelector: neon.nodeSelector, tolerations: neon.tolerations })] : []),
  ];
}
