/**
 * Quantities of CPU and memory, as alasio reads and says them; and what `alasio status`
 * says of how alasio's workloads are scaled and sized, and of the cluster's memory: each
 * workload's replicas, and its scale where KEDA moves it; its pods' requests, and VPA's
 * recommendation beside them; and the memory pods request and use against what the nodes
 * have for pods. Pure, for tests; ./commands/status.ts reads the cluster.
 */
import type { KubernetesObject, V1Container } from "@kubernetes/client-node";

import type { ScaledObject } from "./manifests/scalers.ts";
import type { VerticalPodAutoscaler } from "./manifests/sizing.ts";

/** A workload as far as its scale and sizing are read: its replicas, and its pods' containers. */
export interface ScaledWorkload extends KubernetesObject {
  readonly spec?: { readonly replicas?: number; readonly template?: { readonly spec?: { readonly containers?: readonly V1Container[] } } };
}

/** A memory quantity, in bytes. */
export function memoryBytes(quantity: string): number {
  const match = /^([0-9.]+)([KMGTPE]i?|k|m)?$/u.exec(quantity.trim());
  if (!match) return 0;
  const units: Record<string, number> = { k: 1e3, K: 1e3, M: 1e6, G: 1e9, T: 1e12, P: 1e15, E: 1e18, Ki: 2 ** 10, Mi: 2 ** 20, Gi: 2 ** 30, Ti: 2 ** 40, Pi: 2 ** 50, Ei: 2 ** 60, m: 1e-3 };
  return Number(match[1]) * (match[2] ? units[match[2]] ?? 1 : 1);
}

/** `bytes`, as people read an amount of memory. */
export function memoryText(bytes: number): string {
  return bytes >= 2 ** 30 ? `${(bytes / 2 ** 30).toFixed(1)} GiB` : `${Math.round(bytes / 2 ** 20)} MiB`;
}

/** A CPU quantity, in millicores. */
export function cpuMillis(quantity: string): number {
  const match = /^([0-9.]+)(m?)$/u.exec(quantity.trim());
  if (!match) return 0;
  return match[2] ? Number(match[1]) : Number(match[1]) * 1000;
}

/** `millis` of CPU as people read it. */
export function cpuText(millis: number): string {
  return millis >= 1000 ? `${(millis / 1000).toFixed(1)} CPU` : `${Math.round(millis)}m CPU`;
}

/** What is summed of a pod's containers' quantities: its CPU and its memory. */
interface Amounts {
  readonly cpu: number;
  readonly memory: number;
}

/** `quantities` of each container, summed. */
const summed = (quantities: readonly (Readonly<Record<string, string>> | undefined)[]): Amounts => ({
  cpu: quantities.reduce((sum, each) => sum + cpuMillis(each?.["cpu"] ?? "0"), 0),
  memory: quantities.reduce((sum, each) => sum + memoryBytes(each?.["memory"] ?? "0"), 0),
});

const amountsText = ({ cpu, memory }: Amounts): string => `${cpuText(cpu)} and ${memoryText(memory)}`;

/** A workload's replicas: how many it runs, between its ScaledObject's min and max where KEDA moves it. */
export function describeScale({ spec }: ScaledWorkload, scaledObject: ScaledObject | undefined): string {
  const replicas = spec?.replicas ?? 1;
  const scale = scaledObject ? ` of ${scaledObject.spec.minReplicaCount} to ${scaledObject.spec.maxReplicaCount}, scaled by KEDA` : "";
  return `${replicas} ${replicas === 1 ? "replica" : "replicas"}${scale}`;
}

/** A pod's requests, and what VPA recommends of them, once it has. */
export function describeSizing({ spec }: ScaledWorkload, vpa: VerticalPodAutoscaler | undefined): string {
  const requests = `requests ${amountsText(summed((spec?.template?.spec?.containers ?? []).map(({ resources }) => resources?.requests)))}`;
  const recommended = vpa?.status?.recommendation?.containerRecommendations;
  return recommended ? `${requests}, VPA recommends ${amountsText(summed(recommended.map(({ target }) => target)))}` : requests;
}

/** The cluster's memory: what its pods request, and use where that is measured, of what its nodes have for pods. */
export function describeMemory({ requested, used, allocatable }: { readonly requested: number; readonly used: number | null; readonly allocatable: number }): string {
  return `memory: ${memoryText(requested)} requested${used === null ? "" : `, ${memoryText(used)} used`}, of ${memoryText(allocatable)} the nodes have for pods`;
}
