/**
 * The node agent's wait for its node to be admitted to the bundled store
 * (src/node-agent/store-admission.ts), as the policy's writer, the sandbox gateway, says of each
 * node: the agent only reads it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect, Fiber, Logger } from "effect";

import { KubeApiError } from "../src/kube/client.ts";
import { NODE_ADDRESSES_ANNOTATION } from "../src/node-agent/names.ts";
import { admits, nodeAdmitted } from "../src/node-agent/store-admission.ts";

/** The policy, as its writer keeps the addresses `kept` of each node. */
const policyKeeping = (kept: string | Readonly<Record<string, readonly string[]>>): KubernetesObject => ({
  apiVersion: "networking.k8s.io/v1",
  kind: "NetworkPolicy",
  metadata: { name: "alasio-seaweedfs-from-nodes", namespace: "alasio", annotations: { [NODE_ADDRESSES_ANNOTATION]: typeof kept === "string" ? kept : JSON.stringify(kept) } },
});

test("a node is admitted where the policy keeps addresses of it, as its writer says; not where it keeps none, says nothing of it, or is not there", () => {
  assert.ok(admits(policyKeeping({ "node-1": ["172.18.0.2"] }), "node-1"));
  assert.ok(!admits(policyKeeping({ "node-1": ["172.18.0.2"] }), "node-2"));
  assert.ok(!admits(policyKeeping({ "node-1": [] }), "node-1"));
  assert.ok(!admits(policyKeeping("{not json"), "node-1"));
  assert.ok(!admits({ apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", metadata: { name: "alasio-seaweedfs-from-nodes" } }, "node-1"));
  assert.ok(!admits(null, "node-1"));
});

test("the agent waits until its node is admitted, looking again after a while that grows to its last, each with a jitter, said once, its looks that fail said and looked again", async () => {
  let policy: KubernetesObject | null = null;
  const looks: number[] = [];
  let failing = 2;
  const kube = {
    read: () =>
      Effect.suspend(() => {
        looks.push(Date.now());
        if (failing-- > 0) return Effect.fail(new KubeApiError({ status: 503, cause: new Error("the API server is away") }));
        return Effect.succeed(policy);
      }),
  };
  const logged: string[] = [];
  const done = await Effect.runPromise(
    Effect.gen(function*() {
      const waiting = yield* Effect.forkChild(nodeAdmitted(kube, "alasio", "alasio-seaweedfs-from-nodes", "node-1", 20, 120));
      yield* Effect.sleep("900 millis");
      const before = looks.length;
      policy = policyKeeping({ "node-2": ["172.18.0.3"] });
      yield* Effect.sleep("300 millis");
      assert.ok(waiting.pollUnsafe() === undefined, "admitted before its node is");
      policy = policyKeeping({ "node-1": ["172.18.0.2"], "node-2": ["172.18.0.3"] });
      yield* Fiber.join(waiting);
      return before;
    }).pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push([message].flat().join(" ")))]))),
  );
  // Looked at after 20, 40, 80 ms, and 120 at the most after, each within half again or less: some 9 to 14 in 900 ms.
  assert.ok(done >= 7 && done <= 16, `looked ${done} times in 900 ms`);
  const gaps = looks.slice(1).map((at, index) => at - (looks[index] ?? at));
  assert.ok(gaps.slice(4).every((gap) => gap <= 120 * 1.3), `waited ${gaps.join(", ")} ms between looks`);
  assert.equal(logged.filter((line) => line.startsWith("node-1 prepares no restore by URLs until")).length, 1);
  assert.equal(logged.filter((line) => line.startsWith("could not read whether node-1 is admitted")).length, 2);
});
