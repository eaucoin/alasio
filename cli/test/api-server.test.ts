/**
 * Where the cluster's API server is (cli/src/api-server.ts), as the `kubernetes` Service's
 * EndpointSlices say, against a fake Kubernetes API.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import type { DiscoveryV1EndpointPort, V1Endpoint } from "@kubernetes/client-node";
import { Effect } from "effect";

import { apiServerOf, readApiServer } from "../src/api-server.ts";
import { KubeApi } from "../src/kube/api.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";

async function rig(t: TestContext): Promise<{ readonly kube: FakeKube; readonly read: () => Promise<unknown> }> {
  const kube = await serveFakeKube();
  const directory = mkdtempSync(join(tmpdir(), "alasio-api-server-"));
  t.after(async () => {
    await kube.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const kubeconfig = join(directory, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  return { kube, read: () => Effect.runPromise(Effect.provide(readApiServer, KubeApi.layer({ path: kubeconfig }))) };
}

/** An EndpointSlice of the `kubernetes` Service, of `addressType`, with `endpoints` and `ports`. */
const slice = (name: string, addressType: string, endpoints: V1Endpoint[], ports: DiscoveryV1EndpointPort[]) => ({
  apiVersion: "discovery.k8s.io/v1",
  kind: "EndpointSlice",
  metadata: { name, namespace: "default", labels: { "kubernetes.io/service-name": "kubernetes" } },
  addressType,
  endpoints,
  ports,
});

test("the API server is where the kubernetes Service's IPv4 slices say, at their endpoints not unready, on their TCP ports", () => {
  assert.deepEqual(
    apiServerOf([
      slice("kubernetes", "IPv4", [{ addresses: ["172.18.0.3"] }, { addresses: ["172.18.0.2"], conditions: { ready: true } }, { addresses: ["172.18.0.9"], conditions: { ready: false } }], [
        { name: "https", port: 6443, protocol: "TCP" },
        { port: 6443 },
        { port: 53, protocol: "UDP" },
      ]),
      slice("kubernetes-v6", "IPv6", [{ addresses: ["fd00::2"] }], [{ port: 6443 }]),
    ]),
    { addresses: ["172.18.0.2", "172.18.0.3"], ports: [6443] },
  );
  assert.equal(apiServerOf([]), null);
  assert.equal(apiServerOf([slice("kubernetes", "IPv4", [], [{ port: 6443 }])]), null);
  assert.equal(apiServerOf([slice("kubernetes", "IPv4", [{ addresses: ["172.18.0.2"] }], [])]), null);
});

test("up reads where the API server is from the cluster, and nowhere where it cannot read its slices", async (t) => {
  const { kube, read } = await rig(t);
  assert.equal(await read(), null);
  kube.put("endpointslices", slice("kubernetes", "IPv4", [{ addresses: ["10.0.0.5"] }], [{ name: "https", port: 6443, protocol: "TCP" }]));
  // Another Service's, which says nothing of the API server.
  kube.put("endpointslices", { ...slice("other", "IPv4", [{ addresses: ["10.0.0.6"] }], [{ port: 80 }]), metadata: { name: "other", namespace: "default", labels: { "kubernetes.io/service-name": "other" } } });
  assert.deepEqual(await read(), { addresses: ["10.0.0.5"], ports: [6443] });
  kube.forbidden.add("/apis/discovery.k8s.io/v1/namespaces/default/endpointslices");
  assert.equal(await read(), null);
});
