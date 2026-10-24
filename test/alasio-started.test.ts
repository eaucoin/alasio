/**
 * alasio's start (src/kube/started.ts): counted; and, where it runs in Kubernetes, its time kept
 * in its namespace's alasio-started, as alasio's environment names its namespace; one the API
 * refuses logged, alasio starting all the same; outside Kubernetes, nothing asked of it.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

// Before alasio's modules, which make their instruments as they load.
import { added } from "./support/metrics.ts";

import { ConfigProvider, Effect, Layer, Logger, Stream } from "effect";

import { KubeApiError, KubeClient } from "../src/kube/client.ts";
import { recordStart, STARTS } from "../src/kube/started.ts";

/** A patch alasio sent: what of, and the patch. */
type Patched = readonly [string, string, string, string, object];

/** A KubeClient that takes patches into `patched`, or refuses them with `refusal`. */
function kubeOf(patched: Patched[], refusal?: KubeApiError): Layer.Layer<KubeClient> {
  const unused = () => Effect.die("not called");
  return Layer.succeed(KubeClient, KubeClient.of({
    read: unused,
    list: unused,
    create: unused,
    replace: unused,
    remove: unused,
    exec: unused,
    watch: () => Stream.die("not called"),
    patchPodStatus: unused,
    patch: (apiVersion, kind, namespace, name, patch) => (refusal ? Effect.fail(refusal) : Effect.sync(() => (patched.push([apiVersion, kind, namespace, name, patch]), {}))),
  }));
}

/** Starts alasio's recordStart, `inKubernetes`, on `kube`, in the namespace its environment names: what it logged. */
async function started(inKubernetes: boolean, kube: Layer.Layer<KubeClient>): Promise<string[]> {
  const logged: string[] = [];
  await Effect.runPromise(Effect.scoped(Layer.build(recordStart(inKubernetes, kube))).pipe(
    Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: { ALASIO_NAMESPACE: "alasio-branch-x" } }))),
    Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))])),
  ));
  return logged;
}

test("alasio's start is counted, and its time kept in its namespace's alasio-started where it runs in Kubernetes; a refusal is logged, and outside Kubernetes nothing is asked", async () => {
  const patched: Patched[] = [];
  const counted = await added(STARTS, async () => {
    const before = Date.now();
    assert.deepEqual(await started(true, kubeOf(patched)), []);
    const [[apiVersion, kind, namespace, name, patch] = []] = patched;
    assert.deepEqual([apiVersion, kind, namespace, name], ["v1", "ConfigMap", "alasio-branch-x", "alasio-started"]);
    const at = Date.parse((patch as { data: { startedAt: string } }).data.startedAt);
    assert.ok(at >= before && at <= Date.now(), JSON.stringify(patch));
    // Refused: said, and the start goes on.
    const logged = await started(true, kubeOf([], new KubeApiError({ status: 403, cause: new Error("forbidden") })));
    assert.equal(logged.length, 1);
    assert.match(logged[0] ?? "", /^could not keep alasio's start, at \S+, in alasio-branch-x\/alasio-started: /u);
    // Outside Kubernetes: counted, nothing asked.
    await started(false, kubeOf(patched));
    assert.equal(patched.length, 1);
  });
  assert.deepEqual(Object.values(counted), [3]);
});
