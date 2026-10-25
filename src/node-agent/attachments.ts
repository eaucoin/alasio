/**
 * The node's attach state kept to the pods the cluster has: what is attached into, or was
 * written for, the directory of a pod gone (./workspaces.ts sweep), however it went, detached by
 * its node's agent, which owns that state, as no caller may ever ask it to, as a pod deleted
 * with no grace, or that of a node lost, is never seen ending. It complements the gateway's own
 * detaches, as it releases a binding.
 *
 * The pods of each namespace the attach root has directories for are watched, as the agent's
 * ServiceAccount may read those of each such namespace alone (cli/src/manifests/node-agent.ts),
 * all of them listed first: a pod counts as there while it is on this node, or on none yet, as
 * a restore's pod waits to be scheduled after its directory was made for it, and has not ended
 * for good (counts). Once a namespace's
 * have been listed whole, a sweep runs: at once, as each pod there is deleted, and every
 * SWEEP_EVERY_MS, as one missed. A sweep that finds a pod gone whose sandbox runs still, as
 * kubelet ends one deleted with no grace after it has gone from the cluster, is run again every
 * AGAIN_MS for AGAIN_FOR_MS at most, the next after it taking it on from there. Of a namespace
 * whose watch has not listed every pod, as one that failed, asked again after RETRY_MS, twice as
 * long after each failure, up to RETRY_CAP_MS, counted (alasio.node_agent.attachments.watch_failures),
 * or one the agent may not read, as a branch's made before its Role was, nothing
 * is swept: no directory is detached of a pod not known to be gone. A namespace's watch ends once
 * the attach root has no directory of it.
 */
import { Clock, Effect, FiberMap, Queue, Stream } from "effect";
import type { Scope } from "effect";
import type { V1Pod } from "@kubernetes/client-node";

import { INITIAL_EVENTS_END, KubeClient } from "../kube/client.ts";
import { meter } from "../telemetry/index.ts";
import type { Swept, Workspaces } from "./workspaces.ts";

/** How often a sweep runs whatever was heard, the namespaces watched set right first. */
const SWEEP_EVERY_MS = 60_000;
/** How often a sweep that left a pod waiting for its sandbox's end is run again, and for how long at most. */
const AGAIN_MS = 2_000;
const AGAIN_FOR_MS = 2 * 60_000;
/** How long a watch that failed waits before it is asked again, twice as long after each failure since it last listed its namespace, up to RETRY_CAP_MS, as one of a namespace the agent may not read fails each time. */
const RETRY_MS = 5_000;
const RETRY_CAP_MS = 10 * 60_000;

const watchFailures = meter.createCounter("alasio.node_agent.attachments.watch_failures", {
  description: "Watches of a namespace's pods that failed, by which the node agent sweeps nothing of that namespace's until one lists them whole again",
});

/** What convergeAttachments is given: the node, its agent's workspaces, and, but for tests, its timings. */
export interface AttachmentsOptions {
  readonly node: string;
  readonly workspaces: Pick<Workspaces, "sweep" | "attachedNamespaces">;
  readonly everyMs?: number;
  readonly againMs?: number;
  readonly againForMs?: number;
  readonly retryMs?: number;
  readonly retryCapMs?: number;
}

/**
 * Whether `pod` counts as there for this node's attach state: on `node`, or on none yet; not one
 * whose containers have all ended for good (Succeeded, Failed), which kubelet never deletes, and
 * which would hold its directory, and what is mounted in it, for as long as it is kept.
 */
const counts = (pod: V1Pod, node: string): boolean =>
  (!pod.spec?.nodeName || pod.spec.nodeName === node) && pod.status?.phase !== "Succeeded" && pod.status?.phase !== "Failed";

/** Keeps the node's attach state to the pods the cluster has, as the module says, for as long as its scope lives. */
export const convergeAttachments = (
  { node, workspaces, everyMs = SWEEP_EVERY_MS, againMs = AGAIN_MS, againForMs = AGAIN_FOR_MS, retryMs = RETRY_MS, retryCapMs = RETRY_CAP_MS }: AttachmentsOptions,
): Effect.Effect<void, never, KubeClient | Scope.Scope> =>
  Effect.gen(function*() {
    const kube = yield* KubeClient;
    // The pods of each namespace whose watch has listed them all, by name; a namespace not here is not swept.
    const present = new Map<string, ReadonlySet<string>>();
    // What asks for a sweep: one ask stands for any number made meanwhile.
    const asked = yield* Queue.sliding<void>(1);
    const ask = Queue.offer(asked, undefined).pipe(Effect.asVoid);

    /** Watches the pods of `namespace`, anew whenever a watch ends, as the module says. */
    const watchAlways = (namespace: string): Effect.Effect<never> => {
      // The failures since its namespace was last listed whole.
      let failures = 0;
      const once = Effect.suspend(() => {
        const seen = new Set<string>();
        let listed = false;
        return kube.watch("v1", "pods", namespace, "").pipe(
          Stream.runForEach(({ type, object }) =>
            Effect.suspend(() => {
              if (type === "BOOKMARK") {
                // The list's end alone, which a bookmark of the list says: no other bookmark.
                if (listed || object.metadata?.annotations?.[INITIAL_EVENTS_END] !== "true") return Effect.void;
                listed = true;
                failures = 0;
                present.set(namespace, seen);
                return ask;
              }
              const pod = object as V1Pod;
              const name = pod.metadata?.name ?? "";
              if (type !== "DELETED" && counts(pod, node)) return Effect.sync(() => void seen.add(name));
              seen.delete(name);
              return listed ? ask : Effect.void;
            })
          ),
          Effect.catch((error) =>
            Effect.suspend(() => {
              failures += 1;
              watchFailures.add(1, { "k8s.namespace.name": namespace });
              const wait = Math.min(retryMs * 2 ** (failures - 1), retryCapMs);
              return Effect.logWarning(`watching the pods of ${namespace} for ${node} failed, so nothing of it is swept until they are listed again, asked again in ${Math.round(wait / 1000)} s: ${error.message}`).pipe(
                Effect.andThen(Effect.sleep(wait)),
              );
            })
          ),
          // Its watch ended, the namespace is known no more until one lists it again.
          Effect.ensuring(Effect.sync(() => void present.delete(namespace))),
        );
      });
      return Effect.forever(once);
    };
    const watches = yield* FiberMap.make<string>();
    const watched = new Set<string>();
    /** Watches each namespace the attach root has directories for, and ends the watch of each it has none of now. */
    const setRight = Effect.gen(function*() {
      const current = new Set(yield* workspaces.attachedNamespaces);
      for (const namespace of [...watched]) {
        if (current.has(namespace)) continue;
        watched.delete(namespace);
        yield* FiberMap.remove(watches, namespace);
      }
      for (const namespace of current) {
        if (watched.has(namespace)) continue;
        watched.add(namespace);
        yield* FiberMap.run(watches, namespace, watchAlways(namespace), { onlyIfMissing: true });
      }
    });

    const sweepOnce: Effect.Effect<Swept | null> = Effect.suspend(() => (present.size === 0 ? Effect.succeed(null) : workspaces.sweep(new Map(present)))).pipe(
      Effect.tap((swept) => (swept && swept.detached.length > 0 ? Effect.logInfo(`detached what was attached into ${swept.detached.join(", ")}, whose pods are gone from ${node}`) : Effect.void)),
      Effect.catch((error) => Effect.as(Effect.logWarning(`sweeping what is attached on ${node} failed: ${error.message}`), null)),
    );
    // Each ask swept, then again while a pod gone waits for its sandbox's end, for againForMs at most.
    yield* Effect.forkScoped(Effect.forever(Effect.gen(function*() {
      yield* Queue.take(asked);
      const until = (yield* Clock.currentTimeMillis) + againForMs;
      for (;;) {
        const swept = yield* sweepOnce;
        if (!swept || swept.waiting.length === 0 || (yield* Clock.currentTimeMillis) >= until) break;
        yield* Effect.sleep(againMs);
      }
    })));

    yield* setRight;
    yield* Effect.forkScoped(Effect.forever(Effect.andThen(Effect.sleep(everyMs), Effect.andThen(setRight, ask))));
  });
