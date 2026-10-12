/**
 * The cluster's autoscalers against the installation's: KEDA is each cluster's one, as
 * the API server takes external metrics from one provider, so alasio installs its own
 * only where the cluster has none, and uses the cluster's (autoscaling.keda.enabled
 * false) only where it has one. What alasio found otherwise is refused before anything
 * is applied, saying what to set.
 */
import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect, Schema } from "effect";

import { EXTERNAL_METRICS } from "../manifests/keda.ts";
import { kind, KubeApi, type KubeApiError } from "./api.ts";
import { ofInstallation } from "./apply.ts";

/** The cluster's autoscalers are not what the installation's configuration says: what to set, and why. */
export class AutoscalerMismatch extends Schema.TaggedError<AutoscalerMismatch>()("AutoscalerMismatch", {
  message: Schema.String,
}) {}

/** The CRD of ScaledObjects, which tells a cluster with KEDA. */
const SCALED_OBJECTS = "scaledobjects.keda.sh";

/** What the APIService of external metrics names as the service that serves them. */
interface APIService extends KubernetesObject {
  readonly spec?: { readonly service?: { readonly namespace?: string; readonly name?: string } };
}

/**
 * Refuses an installation of `objects` (`installsKeda` whether they are KEDA's own, as
 * autoscaling.keda.enabled says) that the cluster's autoscalers do not fit: KEDA where the
 * cluster has KEDA, or another provider of external metrics, already; or ScaledObjects
 * where the cluster has no KEDA.
 */
export const checkAutoscalers = Effect.fnUntraced(function*(
  objects: readonly KubernetesObject[],
  installsKeda: boolean,
): Effect.fn.Return<void, KubeApiError | AutoscalerMismatch, KubeApi> {
  const kube = yield* KubeApi;
  const crd = yield* kube.get({ ...kind("CustomResourceDefinition"), name: SCALED_OBJECTS });
  if (installsKeda) {
    if (crd && !ofInstallation(crd)) {
      return yield* new AutoscalerMismatch({ message: "this cluster has a KEDA of its own already, and a cluster has one: install.autoscaling.keda.enabled false uses it" });
    }
    const metrics = yield* kube.get<APIService>({ ...kind("APIService"), name: EXTERNAL_METRICS });
    if (metrics && !ofInstallation(metrics)) {
      const { namespace = "", name = "" } = metrics.spec?.service ?? {};
      return yield* new AutoscalerMismatch({
        message: `this cluster's external metrics are served by ${namespace}/${name} already, and the API server takes them from one provider, which KEDA must be: install.autoscaling.keda.enabled false uses the cluster's KEDA, where it has one`,
      });
    }
    return;
  }
  const scaled = objects.filter((object) => object.kind === "ScaledObject").map(({ metadata }) => metadata?.name ?? "");
  if (scaled.length > 0 && !crd) {
    return yield* new AutoscalerMismatch({
      message: `this cluster has no KEDA, which scales ${scaled.join(", ")}: install.autoscaling.keda.enabled true installs alasio's`,
    });
  }
});
