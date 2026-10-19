/**
 * The PriorityClasses every pod of alasio's runs under. Highest, the floor's pods that
 * their volumes hold to a node: the StatefulSets of what never sleeps that keep their
 * data on claims (Neon's safekeepers, pageserver and storage controller's database, the
 * object store's masters and volume servers, the Valkeys, the filer store's and alasio's,
 * and the log), whose claims a local disk binds to its node. Then, two classes of one value:
 * the rest of the floor, what never sleeps (Neon's proxy and neon-control, and main's
 * compute while it runs, the object store's filers and gateways, workspace storage's
 * metadata and each node's mount of it, main's lake, Grafana, the collector and the controllers,
 * and the Jobs applying alasio waits for, which alasio up fails without); and the pods that
 * run turns, alasio's own, sessions' and folder workspaces' bayma. Of one value, neither
 * takes the other's place: no Grafana scaled up, nor a pod of the floor recreated, ends a
 * turn. Then alasio's other components, which sleep or run once (branch environments'
 * computes and lakes, the Jobs of schedules and of alasio's other commands); and last the
 * headroom's placeholders, which take no pod's place and any pod takes theirs. Kubelet,
 * short of memory, evicts the lowest first.
 *
 * A pod of a class above another's takes its place to be scheduled where there is no
 * room for it (autoscaling.preemption), alasio's and, where alasio shares the cluster,
 * its other pods', of priority 0 unless their class says; or, where it may not, waits
 * for room.
 *
 * A pod held to its node may run there alone, and what may run anywhere would otherwise
 * keep it out for good, having taken the room first, as one is made again (a rollout, a
 * drain, a restore) while the rest scale or start. Above the floor, it takes the place of
 * one that may: that one is made again on another node, or waits where there is none.
 * Above the floor is above the turns too, of one value with it, so where no lower pod's
 * place is enough it may take a turn's, which ends: that is only where the storage cannot
 * run without it, and every turn fails while it does not.
 */
import type { V1PriorityClass } from "@kubernetes/client-node";

import { labels, RELEASE } from "./common.ts";
import type { InstallConfig } from "./config.ts";

/** What a pod is to the stack, which its priority follows. */
export type Tier = "bound" | "floor" | "turn" | "component" | "headroom";

/** Each tier's class's value, highest first, and what its pods are. */
const TIERS: Readonly<Record<Tier, { readonly value: number; readonly description: string }>> = {
  bound: { value: 100_001, description: "alasio's floor that its volumes hold to a node: given up last, taking the place of what may run elsewhere" },
  floor: { value: 100_000, description: "alasio's floor: what never sleeps, given up last" },
  turn: { value: 100_000, description: "alasio's pods that run turns: its own, sessions' and folder workspaces' bayma, given up last" },
  component: { value: 10_000, description: "alasio's components that sleep or run once" },
  headroom: { value: -1, description: "alasio's headroom: placeholders any other pod takes the place of" },
};

/** The PriorityClass of `tier`'s pods, by its name. */
export function priorityClassName(tier: Tier): string {
  return `${RELEASE}-${tier}`;
}

/** What a pod of `tier` has in its spec: its class. */
export function priority(tier: Tier): { priorityClassName: string } {
  return { priorityClassName: priorityClassName(tier) };
}

/** The classes, which are the cluster's. */
export function priorityClassObjects(config: InstallConfig): V1PriorityClass[] {
  return Object.entries(TIERS).map(([tier, { value, description }]) => ({
    apiVersion: "scheduling.k8s.io/v1",
    kind: "PriorityClass",
    metadata: { name: priorityClassName(tier as Tier), labels: labels("priorities") },
    value,
    globalDefault: false,
    // A placeholder of the headroom never takes another pod's place.
    preemptionPolicy: config.autoscaling.preemption && tier !== "headroom" ? "PreemptLowerPriority" : "Never",
    description,
  }));
}
