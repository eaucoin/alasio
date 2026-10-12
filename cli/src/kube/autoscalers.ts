/**
 * The cluster's autoscalers against the installation's: KEDA and VPA are each a cluster's
 * one (the API server takes external metrics from one provider, and two VPAs would size
 * each pod twice), so alasio installs its own only where the cluster has none, and uses
 * the cluster's (autoscaling.keda.enabled, autoscaling.vpa.enabled false) only where it
 * has one. What alasio finds otherwise is refused before anything is applied, saying
 * what to set.
 */
import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect, Schema } from "effect";

import type { InstallConfig } from "../manifests/config.ts";
import { EXTERNAL_METRICS } from "../manifests/keda.ts";
import { kind, KubeApi, type KubeApiError } from "./api.ts";
import { ofInstallation } from "./apply.ts";

/** The cluster's autoscalers are not what the installation's configuration says: what to set, and why. */
export class AutoscalerMismatch extends Schema.TaggedError<AutoscalerMismatch>()("AutoscalerMismatch", {
  message: Schema.String,
}) {}

/** Each autoscaler: its key in autoscaling, its name, the CRD that tells a cluster with it, and the kind of the objects it acts on, and how. */
const AUTOSCALERS = [
  { key: "keda", name: "KEDA", crd: "scaledobjects.keda.sh", kind: "ScaledObject", acts: "scales" },
  { key: "vpa", name: "VPA", crd: "verticalpodautoscalers.autoscaling.k8s.io", kind: "VerticalPodAutoscaler", acts: "sizes" },
] as const;

/** What the APIService of external metrics names as the service that serves them. */
interface APIService extends KubernetesObject {
  readonly spec?: { readonly service?: { readonly namespace?: string; readonly name?: string } };
}

/**
 * Refuses an installation of `objects`, of the configuration `config`, that the cluster's
 * autoscalers do not fit: an autoscaler of alasio's where the cluster has its own, or,
 * for KEDA, another provider of external metrics, already; or objects of an autoscaler
 * the cluster does not have, which alasio is told not to install.
 */
export const checkAutoscalers = Effect.fnUntraced(function*(
  objects: readonly KubernetesObject[],
  { autoscaling }: InstallConfig,
): Effect.fn.Return<void, KubeApiError | AutoscalerMismatch, KubeApi> {
  const kube = yield* KubeApi;
  for (const { key, name, crd: crdName, kind: acted, acts } of AUTOSCALERS) {
    const crd = yield* kube.get({ ...kind("CustomResourceDefinition"), name: crdName });
    const setting = `install.autoscaling.${key}.enabled`;
    if (autoscaling[key].enabled) {
      if (crd && !ofInstallation(crd)) {
        return yield* new AutoscalerMismatch({ message: `this cluster has a ${name} of its own already, and a cluster has one: ${setting} false uses it` });
      }
      continue;
    }
    // alasio's own CRD, kept from when it installed it, is not the cluster's.
    const named = objects.filter((object) => object.kind === acted).map(({ metadata }) => metadata?.name ?? "");
    if (named.length > 0 && (!crd || ofInstallation(crd))) {
      return yield* new AutoscalerMismatch({ message: `this cluster has no ${name}, which ${acts} ${named.join(", ")}: ${setting} true installs alasio's` });
    }
  }
  if (!autoscaling.keda.enabled) return;
  const metrics = yield* kube.get<APIService>({ ...kind("APIService"), name: EXTERNAL_METRICS });
  if (metrics && !ofInstallation(metrics)) {
    const { namespace = "", name = "" } = metrics.spec?.service ?? {};
    return yield* new AutoscalerMismatch({
      message: `this cluster's external metrics are served by ${namespace}/${name} already, and the API server takes them from one provider, which KEDA must be: install.autoscaling.keda.enabled false uses the cluster's KEDA, where it has one`,
    });
  }
});
