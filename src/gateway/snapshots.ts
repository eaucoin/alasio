/**
 * Whole-sandbox snapshots of the sandbox gateway's session workspaces, as alasio's own were
 * (../sandbox/snapshots.ts), of a bound pool pod rather than of a Sandbox: a workspace, of an
 * environment's sessions' namespace, released is saved whole by the node agent on its pod's node
 * (../sandbox/node-agents.ts), bayma told first (`bayma sandbox-hook`), which ends it; and the
 * snapshot it was released into is recorded with the workspace (its node agents' record of it,
 * ../node-agent/workspaces.ts Released), while the gateway holds its binding's Lease, as every
 * read and write of the record is, for its next bind to restore it.
 *
 * A save comes to an outcome, each counted (alasio.sandbox.releases), as alasio's did:
 * snapshotted, into the snapshot it took; kept, as its pod runs no bayma under gVisor, which
 * the gateway then releases unsaved, its pod deleted, as a pool's pods are for it to reuse;
 * lost, its sandbox ended by a save that failed, its REPLs with it; unsaveable, as the agent
 * says no save of it will be taken, whose bayma is told to go on with the stop it would hold
 * for one (its node agent's POST /stop), so its pod ends at once; or failed, its sandbox
 * running on, which keeps the binding.
 */
import type { V1Deployment, V1Pod } from "@kubernetes/client-node";
import { Effect, Schedule } from "effect";

import type { Pool } from "../binder/names.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { BAYMA_CONTAINER } from "../kube/sandbox-shape.ts";
import type { SavedSnapshot } from "../kube/sandboxes.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import { holdsStop, hookOf, stopHookOf } from "../sandbox/snapshots.ts";
import { withLogScope } from "../shared/log.ts";
import { meter } from "../telemetry/index.ts";

const releases = meter.createCounter("alasio.sandbox.releases", {
  description:
    "Session sandboxes released, by outcome: snapshotted; kept, as they run under no gVisor, released unsaved; failed, running on; lost, their sandbox ended unsaved, its REPLs with it; unsaveable, as no save of it will be taken; or error, the release failing beside its save",
});

/**
 * What a node agent answers of a checkpoint whose sandbox can be saved no more, its restore
 * from the bucket having failed to read some of its pages (../node-agent/agent.ts
 * SandboxUnsaveable): no checkpoint of it will be taken.
 */
const UNSAVEABLE_STATUS = 410;

/** Whether `error`, a checkpoint's, says its sandbox can be saved no more. */
const unsaveable = (error: NodeAgentError | KubeApiError): boolean => error._tag === "NodeAgentError" && error.status === UNSAVEABLE_STATUS;

/** What came of a save of a bound pod's sandbox whole (take). */
export type Taken =
  | { readonly outcome: "snapshotted"; readonly saved: SavedSnapshot }
  | { readonly outcome: "kept" }
  | { readonly outcome: "lost" | "failed" | "unsaveable"; readonly why: string };

/** What a release's save came to: the binding kept, its sandbox running on; or released, into the snapshot it gives, or into none. */
export type Saved = "keep" | { readonly saved: SavedSnapshot | null };

/** The gateway's snapshots; see makeGatewaySnapshots. */
export interface GatewaySnapshots {
  /**
   * Saves the sandbox of `pod`, bound to `workspace` of `pool`, for its release, as the module
   * says, and records what it was released into: the binding kept where it runs on, or
   * released. Counted as the release's outcome. Run while the gateway holds the binding's Lease.
   */
  readonly save: (pool: Pool, workspace: string, pod: string) => Effect.Effect<Saved, KubeApiError>;
}

/** The gateway's snapshots, as the module says, on its node agents and the KubeClient. */
export const makeGatewaySnapshots = Effect.fnUntraced(function*(agents: NodeAgents): Effect.fn.Return<GatewaySnapshots, never, KubeClient> {
  const kube = yield* KubeClient;
  const ref = (pool: Pool, workspace: string) => ({ environment: pool.namespace, workspace });

  /** The pod `name` of `pool`, as the API server has it now. */
  const podOf = (pool: Pool, name: string) => Effect.map(kube.read("v1", "Pod", pool.namespace, name), (read) => read as V1Pod | null);
  /** The runtime's id of `pod`'s bayma, if it runs one. */
  const baymaId = (pod: V1Pod | null) => pod?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "");

  /**
   * Saves the bound pod's sandbox whole by its node's agent, which ends it: snapshotted, into
   * the snapshot it took; kept, as its pod runs no bayma under gVisor; lost, its sandbox ended
   * by a save that failed; unsaveable, as the agent says no save of it will be taken; or
   * failed, its sandbox running on, why said. Its pool's template is recorded with it, which
   * its restore makes its pod of.
   */
  const take = (pool: Pool, workspace: string, name: string): Effect.Effect<Taken, KubeApiError> =>
    Effect.gen(function*() {
      const pod = yield* podOf(pool, name);
      const containerId = baymaId(pod);
      const bayma = pod?.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma ? hookOf(bayma) : null;
      const node = pod?.spec?.nodeName;
      if (!node || !containerId || pod?.spec?.runtimeClassName === undefined || !hook) {
        yield* Effect.logWarning(`${pool.namespace}/${workspace} is not snapshotted: its pod runs no bayma under gVisor`);
        return { outcome: "kept" } as const;
      }
      const deployment = (yield* kube.read("apps/v1", "Deployment", pool.namespace, pool.name)) as V1Deployment | null;
      const taken = yield* agents.checkpoint(node, { ...ref(pool, workspace), namespace: pool.namespace, pod: name, containerId, hook, podTemplate: { ...deployment?.spec?.template } }).pipe(Effect.result);
      if (taken._tag === "Success") {
        const { snapshot, release, bytes, pauseMs, uploadMs } = taken.success;
        yield* Effect.logInfo(`snapshotted ${pool.namespace}/${workspace} as ${snapshot} on ${node}: ${bytes} bytes, paused ${pauseMs} ms, uploaded in ${uploadMs} ms`);
        return { outcome: "snapshotted", saved: { snapshot, release } } as const;
      }
      if (unsaveable(taken.failure)) return { outcome: "unsaveable", why: taken.failure.message } as const;
      const after = yield* podOf(pool, name);
      const ended = after?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.state?.terminated !== undefined;
      return { outcome: ended ? "lost" : "failed", why: taken.failure.message } as const;
    });

  /** Tells the bayma of `name`, which runs on, to go on with the stop it holds, so its pod ends at once rather than wait out a save that will not come. */
  const goOnWithStop = (pool: Pool, workspace: string, name: string): Effect.Effect<void, KubeApiError> =>
    Effect.gen(function*() {
      const pod = yield* podOf(pool, name);
      const bayma = pod?.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const hook = bayma && holdsStop(bayma) ? stopHookOf(bayma) : null;
      const containerId = baymaId(pod);
      if (!pod?.spec?.nodeName || !hook || !containerId) return;
      yield* agents.stop(pod.spec.nodeName, { namespace: pool.namespace, pod: name, containerId, hook }).pipe(
        Effect.catch((error) => Effect.logWarning(`${pool.namespace}/${workspace}'s bayma was not told to go on with its stop, which its pod's end waits out: ${error.message}`)),
      );
    });

  const save = (pool: Pool, workspace: string, name: string): Effect.Effect<Saved, KubeApiError> =>
    Effect.gen(function*() {
      const taken = yield* take(pool, workspace, name);
      releases.add(1, { "alasio.sandbox.release.outcome": taken.outcome });
      if (taken.outcome === "failed") {
        yield* Effect.logWarning(`could not snapshot ${pool.namespace}/${workspace}: ${taken.why}; it runs on, still bound`);
        return "keep" as const;
      }
      if (taken.outcome === "lost") yield* Effect.logWarning(`could not snapshot ${pool.namespace}/${workspace}: ${taken.why}; its sandbox ended, so it is released, to come back from bayma's checkpoints`);
      if (taken.outcome === "unsaveable") {
        yield* Effect.logWarning(`${pool.namespace}/${workspace} can be saved no more: ${taken.why}; it is released, to come back from bayma's checkpoints, as bayma tells its next exec`);
      }
      if (taken.outcome === "unsaveable" || taken.outcome === "kept") yield* goOnWithStop(pool, workspace, name);
      const saved = taken.outcome === "snapshotted" ? taken.saved : null;
      // Its record, for its next bind, tried a few times: its sandbox has ended, so it is released all the same where none
      // can be written, its next bind then finding a snapshot it was not released into, which it forgets.
      yield* agents.setReleased(ref(pool, workspace), saved).pipe(
        Effect.retry({ times: 3, schedule: Schedule.exponential("200 millis") }),
        Effect.catch((error) => Effect.logWarning(`${pool.namespace}/${workspace}'s release into ${saved?.snapshot ?? "no snapshot"} could not be recorded, so its next bind comes back from bayma's checkpoints: ${error.message}`)),
      );
      return { saved };
    }).pipe(withLogScope("snapshots"));

  return { save };
});
