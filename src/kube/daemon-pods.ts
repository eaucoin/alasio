/**
 * Where a DaemonSet's pod is, as the gateway reads it of the node agents' (../sandbox/node-agents.ts)
 * and the command line of every DaemonSet it waits on (cli/src/kube/rollout.ts). In a module of its
 * own, with no dependency but the Kubernetes client's types, so the command line takes it as it is.
 */
import type { V1Pod } from "@kubernetes/client-node";

/**
 * The node `pod` is on, or, of a DaemonSet's not yet scheduled, the one it is to be on, as the node affinity its controller
 * gives it names it (`metadata.name`): none where neither says one.
 */
export const daemonPodNode = (pod: V1Pod): string | null =>
  pod.spec?.nodeName ??
    pod.spec?.affinity?.nodeAffinity?.requiredDuringSchedulingIgnoredDuringExecution?.nodeSelectorTerms
      .flatMap(({ matchFields }) => matchFields ?? [])
      .find(({ key, operator }) => key === "metadata.name" && operator === "In")?.values?.[0] ??
    null;
