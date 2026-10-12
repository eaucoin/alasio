/**
 * What keeps a node's drain, or a node autoscaler's scale-down, from stopping more than
 * it should, where a cluster has more than one node:
 *
 * - a component that may run more than one replica has a PodDisruptionBudget of one pod
 *   at a time, and spreads across nodes where it can. One of a single replica has none: a
 *   budget over one pod would block every drain for good, its replacement made only once
 *   the eviction it blocks had happened. A drain on a cluster of several nodes stops it a
 *   moment, as the scheduler starts it elsewhere; two replicas, where it may run them, do
 *   not;
 * - a pod running a turn (alasio's own, while its harnesses run one; a session's; a folder
 *   workspace's bayma) is labelled busy by alasio for as long as it does
 *   (src/kube/busy.ts), and its namespace's budget of busy pods lets none of them be
 *   evicted: a drain waits until the turn is done. Of pods whose owner has no scale (a
 *   Sandbox's), the disruption controller cannot count how many there should be, and so
 *   allows none to be evicted, which is the rule all the same.
 */
import type { V1PodDisruptionBudget, V1TopologySpreadConstraint } from "@kubernetes/client-node";

import { BUSY_LABEL } from "../../../src/kube/busy-label.ts";
import { componentName, labels, selectorLabels } from "./common.ts";
import type { Scale } from "./config.ts";

/** Whether a component of `scale` may run more than one replica. */
export const replicated = ({ max }: Scale): boolean => max > 1;

/** The budget of `component`'s pods in `namespace`, of `scale`: one of them evicted at a time; none for a component of one replica. */
export function replicaBudget(component: string, namespace: string, scale: Scale): V1PodDisruptionBudget[] {
  if (!replicated(scale)) return [];
  return [{
    apiVersion: "policy/v1",
    kind: "PodDisruptionBudget",
    metadata: { name: componentName(component), namespace, labels: labels(component) },
    spec: { maxUnavailable: 1, selector: { matchLabels: selectorLabels(component) } },
  }];
}

/**
 * The budget of `namespace`'s busy pods: none of them evicted, but one not ready, which
 * would otherwise hold a drain for as long as it stayed so, its label held by a turn
 * that may never end.
 */
export function busyBudget(namespace: string): V1PodDisruptionBudget {
  return {
    apiVersion: "policy/v1",
    kind: "PodDisruptionBudget",
    metadata: { name: componentName("busy"), namespace, labels: labels("alasio") },
    spec: { maxUnavailable: 0, selector: { matchLabels: { [BUSY_LABEL]: "true" } }, unhealthyPodEvictionPolicy: "AlwaysAllow" },
  };
}

/** Where `component`'s replicas go, of `scale`: spread across nodes, where the cluster has them, and zones; nothing to say of one replica. */
export function spread(component: string, scale: Scale): { topologySpreadConstraints?: V1TopologySpreadConstraint[] } {
  if (!replicated(scale)) return {};
  const across = (topologyKey: string): V1TopologySpreadConstraint => ({
    maxSkew: 1,
    topologyKey,
    whenUnsatisfiable: "ScheduleAnyway",
    labelSelector: { matchLabels: selectorLabels(component) },
  });
  return { topologySpreadConstraints: [across("kubernetes.io/hostname"), across("topology.kubernetes.io/zone")] };
}
