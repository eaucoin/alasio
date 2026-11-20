import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { ACTIVE_ANNOTATION, type BindingLease } from "../src/binder/bindings.ts";
import { freeLabels, KEY_ANNOTATION, type Pool } from "../src/binder/names.ts";
import { baymaIdleness, makeSweep } from "../src/binder/sweep.ts";
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

test("a binding whose bayma does not answer is idle, once it has not for releaseAfter, as its Lease's active stamp and its pod's making say: released where no request has come for as long, kept where its caller still asks of it, and asked of its bayma anew once that answers", async () => {
  // Its bayma, answering as `answer` says: refusing (503), hanging past the ask's bound, or saying it has been idle a minute.
  let answer: "refuse" | "hang" | "idle" = "refuse";
  const status = createServer((_incoming, outgoing) => {
    if (answer === "refuse") outgoing.writeHead(503).end();
    else if (answer === "idle") outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 60_000 }));
  });
  status.listen(0, "127.0.0.1");
  await once(status, "listening");
  try {
    const releaseAfterMs = 60_000;
    const idleness = baymaIdleness({ podPort: (status.address() as AddressInfo).port, releaseAfterMs, idlePollMs: 15_000, bindingKey: randomBytes(32) });
    const at = Date.now();
    const leaseActiveAt = (activeAt: number): BindingLease => ({
      apiVersion: "coordination.k8s.io/v1",
      kind: "Lease",
      metadata: { name: "binding-stuck", namespace: "sweep", uid: "stuck", annotations: { [KEY_ANNOTATION]: "sessionfs:stuck", [ACTIVE_ANNOTATION]: String(activeAt) } },
      spec: { holderIdentity: "free-0" },
    });
    const pod: V1Pod = { metadata: { name: "free-0", namespace: "sweep", creationTimestamp: new Date(at - 600_000) }, status: { phase: "Running", podIP: "127.0.0.1" } };
    const asked = (lease: BindingLease, now: number) => Effect.runPromise(idleness(lease, pod, now));
    // A request relayed last ten minutes before: not idle while its bayma has not answered for less than releaseAfter.
    const quiet = leaseActiveAt(at - 600_000);
    assert.deepEqual(await asked(quiet, at), { idle: false, askAt: at + 15_000 });
    assert.equal((await asked(quiet, at + releaseAfterMs - 1)).idle, false);
    // A request relayed meanwhile, as its caller still asks of it: kept, and asked again once its stamp is releaseAfter old.
    assert.deepEqual(await asked(leaseActiveAt(at + 50_000), at + releaseAfterMs), { idle: false, askAt: at + 110_000 });
    // Hanging past the ask's bound counts as not answering too; none relayed for releaseAfter: released.
    answer = "hang";
    assert.deepEqual(await asked(quiet, at + releaseAfterMs), { idle: true });
    // Answering again, it says as before: its count starts anew once it fails again.
    answer = "idle";
    assert.deepEqual(await asked(quiet, at + 2 * releaseAfterMs), { idle: true });
    answer = "refuse";
    assert.equal((await asked(quiet, at + 3 * releaseAfterMs)).idle, false);
    assert.equal((await asked(quiet, at + 4 * releaseAfterMs - 1)).idle, false);
    assert.deepEqual(await asked(quiet, at + 4 * releaseAfterMs), { idle: true });
  } finally {
    status.closeAllConnections();
    status.close();
  }
});
