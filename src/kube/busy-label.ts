/**
 * What marks a pod busy, running a turn: the label alasio sets on it while it does
 * (./busy.ts), which its installation's PodDisruptionBudgets select
 * (cli/src/manifests/disruption.ts), and the annotations that go with it.
 */

/** The label of a pod running a turn, "true" while it does. */
export const BUSY_LABEL = "alasio.dev/busy";

/** What cluster-autoscaler and Karpenter read as "do not remove this pod's node now". */
export const BUSY_ANNOTATIONS: Readonly<Record<string, string>> = {
  "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
  "karpenter.sh/do-not-disrupt": "true",
};
