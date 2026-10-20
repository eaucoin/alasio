/**
 * Each workload's VerticalPodAutoscaler, beside it in its component's manifest: VPA
 * (./vpa.ts) recommends every container's requests from what it uses, between a floor as
 * small as alasio's smallest components use and a ceiling, half a core of CPU and the
 * container's memory limit, and leaves limits as they are. How it applies them is the
 * workload's sizing:
 *
 * - `InPlace`, for what keeps no state and runs no turn: its running pods resized in place
 *   where the cluster resizes pods (Kubernetes' in-place pod resize), and never recreated
 *   to be, as VPA's InPlaceOrRecreate would where it does not, or where a node lacks the
 *   room: a pod not resized keeps its requests until it is next made, which VPA sizes;
 * - `AtStart`, for alasio's pod, which runs turns, and the log, whose recreation would stop
 *   the floor, the two that come to use more than they are given as installed (on the box,
 *   alasio's pod 1.8 GiB of its 768 MiB, the log 0.8 GiB of its 576 MiB): each pod given its
 *   recommendation as it is made (VPA's Initial), as an upgrade or a restart makes it, and
 *   no running pod resized or recreated for it. Kubelet short of memory evicts first the
 *   pods that use more than they request, whatever their priority;
 * - `Recommended`, for the rest that keeps state, whose recreation would stop the floor
 *   (Neon's safekeepers, pageserver and the storage controller's database, the object
 *   store's masters, volume servers and filer store, workspace storage's metadata), or that
 *   alasio's turns cannot do without a moment (alasio's database, the compute; the storage
 *   controller, which the compute's start needs; neon-control; each node's agent and its
 *   mount of workspace storage), and for a sandbox pool's pods, whose template their
 *   snapshots are restored onto by its hash, so that a pod made of it is as it says:
 *   recommended alone, which `alasio status` shows beside its requests. VPA recommends of
 *   memory the 90th percentile of each day's peak working set, with a margin of 15%: of
 *   what reads and writes files as Neon's storage and the volume servers do, the page cache
 *   they keep active counts, and their peaks come as each starts and catches up, so it is
 *   two to seven times what they hold of their own (on the box, the pageserver 776 MiB
 *   against 140, each safekeeper 237 against 27): given as each pod is made, it would
 *   request more than the box has to spare, and evict, by priority, what wakes to work.
 *
 * Folder instances' pods have none: the sandbox gateway makes each, of the folder template.
 */
import type { KubernetesObject, V1Container, V1DaemonSet, V1Deployment, V1StatefulSet } from "@kubernetes/client-node";

/** How a workload's requests follow its recommendation. */
export type Sizing = "InPlace" | "AtStart" | "Recommended";

/** VPA's update mode of each sizing: InPlace is alpha in VPA 1.8.0, behind the feature gate ./vpa.ts gives its updater and admission controller. */
const UPDATE_MODES: Readonly<Record<Sizing, string>> = { InPlace: "InPlace", AtStart: "Initial", Recommended: "Off" };

/** The least any container is recommended: a little under what alasio's smallest components use (a storage broker's 13 MiB, a Valkey's 6). */
const MIN_ALLOWED = { cpu: "5m", memory: "16Mi" };
/**
 * The most CPU any container is recommended. alasio's containers have no CPU limit, and use
 * what is spare beside their requests; a recommendation follows what one used, bursts and
 * all, and each workload's is made alone. On a node of 3.5 CPU (CI), the lake's reader came
 * to request 977m and the object store's gateway 832m, and the stack 3.8 CPU in all, so
 * that nothing more was scheduled, not even a Job of alasio up's. Half a core at most each,
 * those two, and the rest as they were, request 3.0.
 */
const MAX_ALLOWED_CPU = "500m";

/** A VerticalPodAutoscaler, as far as alasio writes and reads one: its target, how it applies its recommendation, and its recommendation. */
export interface VerticalPodAutoscaler extends KubernetesObject {
  readonly spec: {
    readonly targetRef: { readonly apiVersion?: string | undefined; readonly kind?: string | undefined; readonly name?: string | undefined };
    readonly updatePolicy: { readonly updateMode: string; readonly minReplicas?: number };
    readonly resourcePolicy: { readonly containerPolicies: readonly object[] };
  };
  readonly status?: { readonly recommendation?: { readonly containerRecommendations?: readonly { readonly containerName: string; readonly target?: Readonly<Record<string, string>> }[] } };
}

/**
 * The VerticalPodAutoscaler of `workload`, sized as `sizing` says, of its `controlled`
 * resources: of memory alone where what scales it scales on its CPU, as the two would
 * chase each other; and where it sleeps between bursts of work, as the lake's loader and
 * intake do, woken from none: it uses CPU only while it bursts, which a recommendation of
 * its CPU would request for each pod woken, as much as a core, a share of a small node's
 * that its floor's pods hold already, keeping it from being scheduled. It takes what CPU
 * is spare beside its request, which no limit bounds.
 */
export function verticalPodAutoscaler(workload: V1Deployment | V1StatefulSet | V1DaemonSet, sizing: Sizing, controlled: readonly ("cpu" | "memory")[] = ["cpu", "memory"]): VerticalPodAutoscaler {
  const containers: readonly V1Container[] = workload.spec?.template.spec?.containers ?? [];
  return {
    apiVersion: "autoscaling.k8s.io/v1",
    kind: "VerticalPodAutoscaler",
    metadata: { name: workload.metadata?.name ?? "", namespace: workload.metadata?.namespace ?? "", labels: { ...workload.metadata?.labels } },
    spec: {
      targetRef: { apiVersion: workload.apiVersion, kind: workload.kind, name: workload.metadata?.name },
      // A workload of one replica, as most of alasio's are, is resized too.
      updatePolicy: { updateMode: UPDATE_MODES[sizing], ...(sizing === "InPlace" ? { minReplicas: 1 } : {}) },
      resourcePolicy: {
        containerPolicies: containers.map(({ name, resources }) => {
          const memory = resources?.limits?.["memory"];
          return {
            containerName: name,
            ...(controlled.length < 2 ? { controlledResources: controlled } : {}),
            minAllowed: MIN_ALLOWED,
            maxAllowed: { cpu: MAX_ALLOWED_CPU, ...(memory ? { memory } : {}) },
            controlledValues: "RequestsOnly",
          };
        }),
      },
    },
  };
}
