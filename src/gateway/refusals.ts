/**
 * The sandbox pods' policy's refusals (cli/src/manifests/admission.ts sandboxPodPolicy) in the
 * gateway's pools: each pod the API server refused to make there for it, as it tells the object
 * that asked, a pool's ReplicaSet, by a FailedCreate Event, counted (POD_POLICY_REFUSALS) by
 * its pool's namespace. An Event counts how often it recurred: what it counted since it was last
 * looked at is added, each look; of one that began before the gateway started, what it counted
 * by its first look is not, as the gateway before it counted that. An Event's times are of
 * whole seconds: one of the second the gateway started in is counted whole.
 */
import type { CoreV1Event } from "@kubernetes/client-node";
import { Clock, Effect } from "effect";

import type { Pool } from "../binder/names.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { meter } from "../telemetry/index.ts";
import { SANDBOX_POD_POLICY } from "./names.ts";

/** The counter of the policy's refusals, as the module says. */
export const POD_POLICY_REFUSALS = "alasio.gateway.pod_policy.refusals";
const refusals = meter.createCounter(POD_POLICY_REFUSALS, { description: "Pods the sandbox pods' policy refused to let be made in a pool's namespace" });

/** Whether `event` tells of a pod the policy refused. */
export const refusedByPolicy = (event: CoreV1Event): boolean =>
  event.reason === "FailedCreate" && (event.message ?? "").includes(`ValidatingAdmissionPolicy '${SANDBOX_POD_POLICY}'`);

/** When `event` began, in ms; 0 where it says not. */
const begunAt = (event: CoreV1Event): number => new Date(event.firstTimestamp ?? event.eventTime ?? 0).getTime() || 0;

/** What counts the policy's refusals in `pools`, each time it is run, as the module says. */
export const makeRefusalCount = Effect.fnUntraced(function*(pools: Effect.Effect<readonly Pool[]>): Effect.fn.Return<Effect.Effect<void, KubeApiError>, never, KubeClient> {
  const kube = yield* KubeClient;
  const started = Math.floor((yield* Clock.currentTimeMillis) / 1000) * 1000;
  /** Each refusing Event's count, as last looked at, by its uid. */
  const seen = new Map<string, number>();
  return Effect.gen(function*() {
    const namespaces = [...new Set((yield* pools).map(({ namespace }) => namespace))];
    for (const namespace of namespaces) {
      const events = (yield* kube.list("v1", "Event", namespace, "")) as CoreV1Event[];
      for (const event of events.filter(refusedByPolicy)) {
        const uid = event.metadata?.uid ?? "";
        const count = event.count ?? 1;
        const before = seen.get(uid) ?? (begunAt(event) >= started ? 0 : count);
        seen.set(uid, count);
        if (count > before) refusals.add(count - before, { "k8s.namespace.name": namespace });
      }
    }
  });
});
