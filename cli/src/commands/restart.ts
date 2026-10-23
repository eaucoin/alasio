/** `alasio restart`: a rollout restart of alasio's Deployment, waited for. */
import type { V1Deployment } from "@kubernetes/client-node";
import { Console, Effect } from "effect";
import { Command } from "effect/cli";

import { kind, KubeApi } from "../kube/api.ts";
import { awaitReady } from "../kube/rollout.ts";
import { NAMESPACE, RELEASE } from "../manifests/common.ts";
import { onCluster, timeoutFlag, waitOptions } from "./common.ts";

/** alasio's Deployment. */
const DEPLOYMENT = { ...kind("Deployment"), namespace: NAMESPACE, name: RELEASE };

/**
 * The manager the restart's annotation is patched as: not alasio, whose applies would
 * otherwise take it back off, restarting alasio again.
 */
const RESTART_MANAGER = "alasio-restart";

export const restart = Command.make("restart", { timeout: timeoutFlag }, ({ timeout }) =>
  onCluster(() =>
    Effect.gen(function*() {
      const kube = yield* KubeApi;
      yield* kube.patch(DEPLOYMENT, { spec: { template: { metadata: { annotations: { "kubectl.kubernetes.io/restartedAt": new Date().toISOString() } } } } }, RESTART_MANAGER);
      yield* Effect.logInfo("restarting alasio");
      yield* awaitReady([DEPLOYMENT], waitOptions(timeout));
      // Of one unless said, as Kubernetes makes a Deployment.
      const replicas = (yield* kube.get<V1Deployment>(DEPLOYMENT))?.spec?.replicas ?? 1;
      yield* Console.log(
        replicas === 0
          ? "alasio runs no pod, asleep as KEDA lets it while nothing waits for it: the one it starts next runs anew."
          : replicas === 1
          ? "alasio restarted; a turn it was running continues."
          : `alasio's ${replicas} pods restarted, each beside the one replacing it; a turn one was running continues.`,
      );
    })
  )).pipe(
    Command.withShortDescription("Restart alasio"),
    Command.withDescription(
      "Restarts alasio's pods, as a rollout restart of its Deployment does, each replaced in turn, and waits until they run again; " +
        "where KEDA has let alasio sleep, at none, there is none to restart, and the one it starts next runs anew. " +
        "Conversations survive it: a turn one was running continues once alasio is back, and workspaces keep running through it.",
    ),
  );
