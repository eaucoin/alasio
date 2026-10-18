/**
 * The CPU baseline: the CPU features sessions' sandboxes are held to, by gVisor's
 * `dev.gvisor.internal.cpufeatures` annotation on their template, so that a snapshot
 * taken on one node restores on any other. gVisor gives a sandbox the features its node
 * has of those the annotation names (runsc/boot/loader.go), so an empty annotation would
 * leave it none: a baseline is never empty, and sessions are held to none, and not
 * snapshotted, until there is one.
 *
 * Each node agent's pod says, as it starts, the features its node has under the current
 * gVisor release (src/node-agent/cpu-features.ts), in its status. `alasio up` holds
 * sessions to those every node has once every node with an agent has said them, and to
 * the baseline it recorded before while any has not.
 *
 * It records the baseline in the sandbox pools' template (manifests/sandbox-gateway.ts),
 * which the gateway's configuration and the sandbox pods' policy take whole, in the
 * cluster rather than in the machine's stamp
 * (cluster/node.ts): a cluster in Docker or of a kubeconfig has no stamp, and a cluster
 * on this machine may have nodes of other machines, which its stamp does not describe.
 *
 * No node is tainted for lacking it, as a taint keeps every pod off the node, not only
 * sessions: on a cluster shared with other workloads, theirs too. A node that lacks it is
 * one that joined since `alasio up` last ran; gVisor gives its sandboxes the features it
 * has of the baseline's, so their snapshots restore anywhere, and a snapshot taken under
 * the whole baseline restores on the nodes that have it, which alasio picks
 * (src/gateway/snapshots.ts nodesFor). `alasio status` says which node lacks it, and the next
 * `alasio up` narrows it to what every node has.
 */
import type { V1ConfigMap, V1Pod } from "@kubernetes/client-node";
import { Effect, Result, Schema } from "effect";

import { CPU_FEATURES_CONTAINER, type CpuFeaturesReport } from "../../src/node-agent/names.ts";
import { currentGvisorRelease } from "./cluster/node.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { selectorOf } from "./kube/rollout.ts";
import type { CpuBaseline } from "./manifests/alasio.ts";
import { NAMESPACE, selectorLabels } from "./manifests/common.ts";
import { GATEWAY, recordedCpuBaseline } from "./manifests/sandbox-gateway.ts";

/** What a node agent's pod says of its node's CPU: the features it has, or why they are not known. */
export type NodeCpu =
  | { readonly node: string; readonly features: readonly string[] }
  | { readonly node: string; readonly unknown: string };

/** What a node agent's CPU_FEATURES_CONTAINER says. */
const Report = Schema.Struct({ release: Schema.String, cpuFeatures: Schema.Array(Schema.String) });

/** What the node agent's pod `pod` says of its node's CPU under gVisor's `release`. Pure, for tests. */
export function nodeCpuOf(pod: V1Pod, release: string): NodeCpu | null {
  const node = pod.spec?.nodeName;
  if (!node) return null;
  const said = pod.status?.initContainerStatuses?.find(({ name }) => name === CPU_FEATURES_CONTAINER);
  const ended = said?.state?.terminated ?? said?.lastState?.terminated;
  if (!ended) return { node, unknown: "its node agent has not said its CPU features yet" };
  if (ended.exitCode !== 0) return { node, unknown: `its node agent could not say its CPU features: ${ended.message?.trim() || `it exited ${ended.exitCode}`}` };
  const report = Schema.decodeUnknownResult(Schema.fromJsonString(Report))(ended.message ?? "");
  if (Result.isFailure(report)) return { node, unknown: "its node agent said its CPU features as nothing alasio reads" };
  const { release: of, cpuFeatures }: CpuFeaturesReport = report.success;
  if (of !== release) return { node, unknown: `its node agent said its CPU features under gVisor ${of}, not ${release}` };
  return { node, features: cpuFeatures };
}

/** What every node agent's pod says of its node's CPU under the current gVisor release, by node; a pod on its way out says nothing. */
export const surveyCpu: Effect.Effect<readonly NodeCpu[], KubeApiError, KubeApi> = Effect.gen(function*() {
  const pods = yield* (yield* KubeApi).list<V1Pod>(kind("Pod"), { namespace: NAMESPACE, labelSelector: selectorOf(selectorLabels("node-agent")) });
  const release = currentGvisorRelease();
  return pods.filter((pod) => !pod.metadata?.deletionTimestamp).flatMap((pod) => nodeCpuOf(pod, release) ?? []).toSorted((a, b) => a.node.localeCompare(b.node));
});

/**
 * The baseline the installation the cluster runs holds sessions to, as its sandbox
 * gateway's configuration records it in its pools' template, and that configuration,
 * which up carries the pools' templates before the current ones from.
 */
export const recordedBaseline: Effect.Effect<{ readonly liveGateway: string | undefined; readonly baseline: CpuBaseline }, KubeApiError, KubeApi> = Effect.gen(function*() {
  const liveGateway = (yield* (yield* KubeApi).get<V1ConfigMap>({ ...kind("ConfigMap"), namespace: NAMESPACE, name: GATEWAY }))?.data?.["config.json"];
  return { liveGateway, baseline: recordedCpuBaseline(liveGateway) };
});

/** The baseline sessions are held to, and what of the nodes it says. */
export interface BaselineDecision {
  readonly baseline: CpuBaseline;
  /** Why sessions are not snapshotted, where there is no baseline. */
  readonly held: string | null;
  /** The nodes that lack features of the baseline: which. */
  readonly lacking: readonly { readonly node: string; readonly missing: readonly string[] }[];
  /** The nodes whose features are not known, and why. */
  readonly unknown: readonly { readonly node: string; readonly unknown: string }[];
}

/**
 * The baseline `survey` comes to, beside the one `recorded` before, as the module says:
 * what every node has, once every node has said; `recorded` while any has not. Its
 * features in order, as the annotation is compared whole. Pure, for tests.
 */
export function decideCpuBaseline(survey: readonly NodeCpu[], recorded: CpuBaseline): BaselineDecision {
  const unknown = survey.flatMap((each) => ("unknown" in each ? [each] : []));
  const [first, ...others] = survey.flatMap((each) => ("features" in each ? [each] : []));
  const shared = first && unknown.length === 0 ? first.features.filter((feature) => others.every(({ features }) => features.includes(feature))).toSorted() : null;
  const baseline = shared === null ? recorded : shared.length > 0 ? shared : null;
  const held = baseline !== null
    ? null
    : shared !== null
    ? "the nodes share no CPU feature under gVisor"
    : survey.length === 0
    ? "no node agent runs yet"
    : `not every node's CPU features are known yet (${unknown.map(({ node }) => node).join(", ")})`;
  return { baseline, held, lacking: lackingOf(survey, baseline), unknown };
}

/** The nodes of `survey` that lack features of `baseline`, and which. */
function lackingOf(survey: readonly NodeCpu[], baseline: CpuBaseline): BaselineDecision["lacking"] {
  if (baseline === null) return [];
  return survey.flatMap((each) => {
    const missing = "features" in each ? baseline.filter((feature) => !each.features.includes(feature)) : [];
    return missing.length > 0 ? [{ node: each.node, missing }] : [];
  });
}

/**
 * Where the nodes of `survey` stand against the baseline `recorded`, which the last
 * `alasio up` held sessions to, as `alasio status` says it: the nodes that lack it, and
 * those not known; and, where there is none, why the next up would hold sessions to none
 * either, if it would. Pure, for tests.
 */
export function standingOf(survey: readonly NodeCpu[], recorded: CpuBaseline): BaselineDecision {
  const next = decideCpuBaseline(survey, null);
  const held = recorded !== null ? null : (next.held ?? "alasio up has not run since every node said its CPU features");
  return { baseline: recorded, held, lacking: lackingOf(survey, recorded), unknown: next.unknown };
}

/** Whether `a` and `b` are one baseline. */
export const sameBaseline = (a: CpuBaseline, b: CpuBaseline): boolean => (a === null || b === null ? a === b : a.join(",") === b.join(","));

/** What of `decision` the operator is to act on, a line each: why sessions are not snapshotted, and which nodes lack the baseline or are not known. */
export function baselineShortfalls({ held, lacking, unknown }: BaselineDecision): string[] {
  return [
    ...(held === null ? [] : [`sessions are not snapshotted: they are held to no CPU baseline, as ${held}`]),
    ...lacking.map(({ node, missing }) =>
      `node ${node} lacks ${missing.join(", ")} of the CPU baseline: snapshots taken elsewhere are not restored there, and alasio up narrows the baseline to it`
    ),
    ...unknown.map(({ node, unknown: why }) => `node ${node}: ${why}`),
  ];
}

/** What `decision` says to the operator, a line each: the baseline sessions are held to, and its shortfalls. */
export function describeBaseline(decision: BaselineDecision): string[] {
  const { baseline } = decision;
  return [
    ...(baseline === null ? [] : [`sessions are held to ${baseline.length} CPU features, those every node had as alasio up last surveyed them`]),
    ...baselineShortfalls(decision),
  ];
}
