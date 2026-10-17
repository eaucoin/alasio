import assert from "node:assert/strict";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { freeLabels, KEY_ANNOTATION, type Pool } from "../src/binder/names.ts";
import { makeSweep } from "../src/binder/sweep.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The binder's sweep of idle bindings, whatever says a binding idle and releases it, on a real API server.

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

test("a sweep releases each binding found idle, asks one that is not again only once its time has come, and says which it released", async () => {
  const of: Pool = { name: "sandboxes", namespace: "sweep" };
  const layer = Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer));
  await Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const kube = yield* KubeClient;
    const binder = yield* Binder;
    yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: of.namespace } });
    for (const name of ["free-0", "free-1"]) {
      yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels: { ...freeLabels(of) } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
      yield* kube.patchPodStatus(of.namespace, name, { status: { phase: "Running", podIP: "10.42.0.9", conditions: [{ type: "Ready", status: "True" }] } });
    }
    yield* binder.bind(of, "sessionfs:idle");
    yield* binder.bind(of, "sessionfs:busy");
    const asked: string[] = [];
    const sweep = yield* makeSweep({
      pools: Effect.succeed([of]),
      idleness: (lease, pod) =>
        Effect.sync(() => {
          const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
          asked.push(`${key} ${pod?.metadata?.name === lease.spec?.holderIdentity}`);
          return key === "sessionfs:idle" ? { idle: true } as const : { idle: false, askAt: Date.now() + 500 } as const;
        }),
      release: (pool, key) => binder.release(pool, key),
    });
    assert.deepEqual(yield* sweep, ["sessionfs:idle"]);
    assert.deepEqual(asked.sort(), ["sessionfs:busy true", "sessionfs:idle true"]);
    // The busy one, not asked again before its time, then asked once it has come; the idle one, released, not at all.
    assert.deepEqual(yield* sweep, []);
    assert.equal(asked.length, 2);
    yield* Effect.sleep("700 millis");
    assert.deepEqual(yield* sweep, []);
    assert.deepEqual(asked.slice(2), ["sessionfs:busy true"]);
    assert.equal((yield* kube.list("coordination.k8s.io/v1", "Lease", of.namespace, "alasio.dev/binding")).length, 1);
  }), layer));
});
