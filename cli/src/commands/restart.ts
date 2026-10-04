/** `alasio restart`: a rollout restart of alasio's Deployment, waited for. */
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
      yield* Console.log("alasio restarted; a turn it was running continues.");
    })
  )).pipe(
    Command.withShortDescription("Restart alasio"),
    Command.withDescription(
      "Restarts alasio's pod, as a rollout restart of its Deployment does, and waits until it runs again. " +
        "Conversations survive it: a turn it was running continues once alasio is back, and workspaces keep running through it.",
    ),
  );
