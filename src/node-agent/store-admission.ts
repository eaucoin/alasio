/**
 * The node agent's wait for its node to be admitted to the bundled store's S3 port, before it
 * prepares a restore whose checkpoint gofer reads the snapshots bucket straight from the store by
 * URLs (./store-urls.ts) from the node's own network, which the installation's policy admits each
 * node's addresses to (cli/src/manifests/network-policies.ts storeFromNodesPolicy). The sandbox
 * gateway is the policy's one writer (../gateway/store-admission.ts), which says what it keeps of
 * each node beside its rule (NODE_ADDRESSES_ANNOTATION): the agent only reads it, so a node's
 * agent cannot change what it admits. It waits until the policy keeps its node's addresses,
 * looking again after a while that doubles from ADMISSION_FIRST_LOOK_MS to ADMISSION_LAST_LOOK_MS,
 * each with a jitter, said once, as a node that joins is admitted within a few seconds.
 */
import type { KubernetesObject } from "@kubernetes/client-node";
import { Duration, Effect, Schedule } from "effect";

import type { KubeApiError, KubeClient } from "../kube/client.ts";
import { NODE_ADDRESSES_ANNOTATION } from "./names.ts";

/** How long the agent waits first, and at the most, between two looks at the policy. */
export const ADMISSION_FIRST_LOOK_MS = 500;
export const ADMISSION_LAST_LOOK_MS = 30_000;

/** Whether `policy` keeps addresses of the node `node`, as its writer says (NODE_ADDRESSES_ANNOTATION). */
export function admits(policy: KubernetesObject | null, node: string): boolean {
  const said = policy?.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION];
  if (said === undefined) return false;
  try {
    const kept = JSON.parse(said) as Record<string, unknown>;
    return Array.isArray(kept[node]) && kept[node].length > 0;
  } catch {
    return false;
  }
}

/** Ends once the policy `name` of `namespace` admits the node `node`, as the module says; its looks failing as the API does said, and looked again. */
export const nodeAdmitted = (kube: Pick<KubeClient["Service"], "read">, namespace: string, name: string, node: string, firstMs = ADMISSION_FIRST_LOOK_MS, lastMs = ADMISSION_LAST_LOOK_MS): Effect.Effect<void> => {
  let said = false;
  const look: Effect.Effect<boolean> = kube.read("networking.k8s.io/v1", "NetworkPolicy", namespace, name).pipe(
    Effect.map((policy) => admits(policy, node)),
    Effect.catch((error: KubeApiError) => Effect.logWarning(`could not read whether ${node} is admitted to the bundled store: ${error.message}`).pipe(Effect.as(false))),
    Effect.tap((admitted) => {
      if (admitted || said) return Effect.void;
      said = true;
      return Effect.logInfo(`${node} prepares no restore by URLs until the bundled store's policy ${namespace}/${name} admits it`);
    }),
  );
  return look.pipe(
    Effect.repeat({
      schedule: Schedule.exponential(`${firstMs} millis`).pipe(
        Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, Duration.millis(lastMs)))),
        Schedule.jittered,
      ),
      until: (admitted) => admitted,
    }),
    Effect.asVoid,
  );
};
