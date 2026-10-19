/**
 * The sandbox pods' policy's refusals, as the gateway counts them (src/gateway/refusals.ts), from
 * the Events a real API server holds.
 */
import assert from "node:assert/strict";
import { before, test } from "node:test";

import { added } from "./support/metrics.ts";
import type { CoreV1Event } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";

import { makeRefusalCount, POD_POLICY_REFUSALS } from "../src/gateway/refusals.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));
const NAMESPACE = "refusals-sessions";
const REFUSED = "Error creating: pods \"sandboxes-none-7d9f6c-x\" is forbidden: ValidatingAdmissionPolicy 'alasio-sandbox-pods' with binding 'alasio-sandbox-pods' denied request: a pod of sessions is a pool's, as its template makes it";

/** An Event of the pool's ReplicaSet, begun `ago` ms before now, recurred `count` times. */
const event = (name: string, ago: number, count: number, message = REFUSED, reason = "FailedCreate"): CoreV1Event => ({
  apiVersion: "v1",
  kind: "Event",
  metadata: { name, namespace: NAMESPACE },
  involvedObject: { apiVersion: "apps/v1", kind: "ReplicaSet", name: "sandboxes-none-7d9f6c", namespace: NAMESPACE },
  reason,
  message,
  count,
  firstTimestamp: new Date(Date.now() - ago),
  lastTimestamp: new Date(),
});

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
  await run(Effect.flatMap(KubeClient, (kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: NAMESPACE } })));
});

test("each pod the policy refused in a pool's namespace is counted once, as its Event recurs, and none of before the gateway started, nor another failure", async () => {
  const create = (object: CoreV1Event) => run(Effect.flatMap(KubeClient, (kube) => kube.create(object)));
  const recur = (name: string, count: number) => run(Effect.flatMap(KubeClient, (kube) => kube.patch("v1", "Event", NAMESPACE, name, { count })));
  await create(event("before", 600_000, 3));
  const count = await run(makeRefusalCount(Effect.succeed([{ name: "sandboxes-none", namespace: NAMESPACE }, { name: "sandboxes-full", namespace: NAMESPACE }])));
  const counted = () => run(count);
  await create(event("refused", 0, 1));
  await create(event("quota", 0, 4, "Error creating: pods \"x\" is forbidden: exceeded quota"));
  await create(event("scheduled", 0, 2, "Successfully assigned", "Scheduled"));
  const of = `k8s.namespace.name=${NAMESPACE}`;
  assert.deepEqual(await added(POD_POLICY_REFUSALS, counted), { [of]: 1 });
  // Recurred since: each what it counted since, the one begun before the gateway too.
  await recur("before", 5);
  await recur("refused", 2);
  assert.deepEqual(await added(POD_POLICY_REFUSALS, counted), { [of]: 3 });
  assert.deepEqual(await added(POD_POLICY_REFUSALS, counted), {});
});
