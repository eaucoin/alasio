/**
 * The bundled store's admission of the cluster's nodes (src/gateway/store-admission.ts): the
 * sandbox gateway, its one writer, keeps the policy of the store's S3 port, its whole spec,
 * admitting each node's addresses, as its Node object says them, and no other.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject, V1Node } from "@kubernetes/client-node";
import { Effect } from "effect";

import { admitNodes, nodeAddressesOf, rangeStart } from "../src/gateway/store-admission.ts";
import { KubeApiError } from "../src/kube/client.ts";
import { NODE_ADDRESSES_ANNOTATION } from "../src/node-agent/names.ts";

/** A policy as the installation applies it: of the store's pods, of no rule of its own. */
const POLICY: KubernetesObject & { readonly spec: Record<string, unknown> } = {
  apiVersion: "networking.k8s.io/v1",
  kind: "NetworkPolicy",
  metadata: { name: "alasio-seaweedfs-from-nodes", namespace: "alasio", resourceVersion: "1" },
  spec: { podSelector: { matchLabels: { "app.kubernetes.io/component": "seaweedfs" } }, policyTypes: ["Ingress"] },
};

/** A Node of `name`, of the addresses `addresses` (InternalIP unless typed) and the pods' ranges `podCIDRs`. */
const nodeOf = (name: string, addresses: readonly (string | { readonly type: string; readonly address: string })[], podCIDRs: readonly string[] = []): V1Node => ({
  apiVersion: "v1",
  kind: "Node",
  metadata: { name },
  spec: { podCIDRs: [...podCIDRs], ...(podCIDRs[0] === undefined ? {} : { podCIDR: podCIDRs[0] }) },
  status: { addresses: addresses.map((address) => (typeof address === "string" ? { type: "InternalIP", address } : { ...address })) },
});

/**
 * The API server as the gateway calls it: the policy, if any, written only as of the version it
 * is (409 otherwise, as before each write `beforeWrite` may change it), and the nodes it has.
 */
function fakeKube(policy: KubernetesObject | null, nodes: readonly V1Node[]) {
  const state = { policy, nodes: [...nodes], writes: 0, beforeWrite: () => {} };
  return {
    state,
    kube: {
      read: (_apiVersion: string, _kind: string, _namespace: string, name: string) => Effect.sync(() => (state.policy?.metadata?.name === name ? structuredClone(state.policy) : null)),
      list: (_apiVersion: string, kind: string) => Effect.sync(() => (kind === "Node" ? structuredClone(state.nodes) : [])),
      replace: <T extends KubernetesObject>(object: T) =>
        Effect.suspend(() => {
          state.beforeWrite();
          if (object.metadata?.resourceVersion !== state.policy?.metadata?.resourceVersion) return Effect.fail(new KubeApiError({ status: 409, cause: new Error("the object has been modified") }));
          state.writes += 1;
          state.policy = { ...structuredClone(object), metadata: { ...object.metadata, resourceVersion: String(Number(object.metadata?.resourceVersion) + 1) } };
          return Effect.succeed(state.policy as T);
        }),
    },
  };
}

/** What the policy admits: its kept addresses, and the blocks of its one rule, on its port. */
const admitted = (policy: KubernetesObject | null) => {
  const { ingress } = (policy as { readonly spec: { readonly ingress?: readonly { readonly ports: unknown; readonly from: readonly { readonly ipBlock: { readonly cidr: string } }[] }[] } }).spec;
  return {
    kept: JSON.parse(policy?.metadata?.annotations?.[NODE_ADDRESSES_ANNOTATION] ?? "{}") as Record<string, string[]>,
    rules: (ingress ?? []).map(({ ports, from }) => ({ ports, from: from.map(({ ipBlock }) => ipBlock.cidr) })),
  };
};

const PORT = [{ protocol: "TCP", port: 8333 }];

/** The labels of the store's pods, as the gateway is given them. */
const STORE_LABELS = { "app.kubernetes.io/component": "seaweedfs" };

test("a range's start is its network address and the one after it, of IPv4 and IPv6, nothing of what is not a range", () => {
  assert.deepEqual(rangeStart("10.42.1.0/24"), ["10.42.1.0", "10.42.1.1"]);
  assert.deepEqual(rangeStart("10.42.255.255/32"), ["10.42.255.255", "10.43.0.0"]);
  assert.deepEqual(rangeStart("fd00:42:1::/64"), ["fd00:42:1::", "fd00:42:1::1"]);
  // The longest run of zeros compressed, the first of two as long.
  assert.deepEqual(rangeStart("2001:db8:0:0:1::/80"), ["2001:db8:0:0:1::", "2001:db8::1:0:0:1"]);
  assert.deepEqual(rangeStart("10.42.1/24"), []);
  assert.deepEqual(rangeStart("10.42.1.300/24"), []);
});

test("a node's addresses are its InternalIP and ExternalIP ones and the start of each of its pods' ranges, which its own processes reach other nodes' pods from; not its hostname's", () => {
  assert.deepEqual(
    nodeAddressesOf(nodeOf("node-1", ["172.18.0.3", { type: "ExternalIP", address: "203.0.113.7" }, { type: "Hostname", address: "node-1" }], ["10.42.1.0/24", "fd00:42:1::/64"])),
    ["10.42.1.0", "10.42.1.1", "172.18.0.3", "203.0.113.7", "fd00:42:1::", "fd00:42:1::1"],
  );
  // Of a single range, as a cluster of before podCIDRs says it.
  assert.deepEqual(nodeAddressesOf({ metadata: { name: "node-2" }, spec: { podCIDR: "10.42.2.0/24" }, status: { addresses: [{ type: "InternalIP", address: "172.18.0.4" }] } }), ["10.42.2.0", "10.42.2.1", "172.18.0.4"]);
  assert.deepEqual(nodeAddressesOf({ metadata: { name: "node-3" } }), []);
});

test("the gateway has the policy admit every address the cluster's nodes hold to the store's S3 port and no other, as the nodes are, its whole spec its own: a node that joins is admitted, one gone is dropped, none leaves no rule that would admit everyone, a rule, a selector or types changed by anyone else are set back, and a write that finds the policy written since reads it again", async () => {
  const { state, kube } = fakeKube(structuredClone(POLICY), [nodeOf("node-1", ["172.18.0.2"], ["10.42.0.0/24"])]);
  const admit = () => Effect.runPromise(admitNodes(kube, "alasio", "alasio-seaweedfs-from-nodes", STORE_LABELS));

  assert.equal(await admit(), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": ["10.42.0.0", "10.42.0.1", "172.18.0.2"] }, rules: [{ ports: PORT, from: ["10.42.0.0/32", "10.42.0.1/32", "172.18.0.2/32"] }] });
  // As it was: nothing written.
  assert.equal(await admit(), "unchanged");
  assert.equal(state.writes, 1);
  // A node that joins, of an IPv6 range too.
  state.nodes.push(nodeOf("node-2", ["172.18.0.3"], ["fd00:42:1::/64"]));
  assert.equal(await admit(), "written");
  assert.deepEqual(admitted(state.policy).rules, [{ ports: PORT, from: ["10.42.0.0/32", "10.42.0.1/32", "172.18.0.2/32", "172.18.0.3/32", "fd00:42:1::/128", "fd00:42:1::1/128"] }]);
  // Its policy's rule rewritten by anyone else, as to admit everyone: set right on the next round.
  state.policy = { ...state.policy, spec: { ...(state.policy as typeof POLICY).spec, ingress: [{}] } } as KubernetesObject;
  assert.equal(await admit(), "written");
  assert.deepEqual(admitted(state.policy).rules[0]?.from.length, 6);
  // Its selector or its types changed by anyone else, as to admit the namespace's every pod or none: set back too.
  for (const changed of [{ podSelector: {} }, { policyTypes: ["Ingress", "Egress"] }, { egress: [{}] }]) {
    state.policy = { ...state.policy, spec: { ...(state.policy as typeof POLICY).spec, ...changed } } as KubernetesObject;
    assert.equal(await admit(), "written", JSON.stringify(changed));
    assert.deepEqual((state.policy as typeof POLICY).spec, { podSelector: POLICY.spec["podSelector"], policyTypes: ["Ingress"], ingress: (state.policy as typeof POLICY).spec["ingress"] });
  }
  assert.equal(await admit(), "unchanged");
  // A node gone from the cluster is dropped.
  state.nodes = state.nodes.filter(({ metadata }) => metadata?.name !== "node-2");
  assert.equal(await admit(), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": ["10.42.0.0", "10.42.0.1", "172.18.0.2"] }, rules: [{ ports: PORT, from: ["10.42.0.0/32", "10.42.0.1/32", "172.18.0.2/32"] }] });
  // None held: no rule, as a rule of no peer would admit everyone.
  state.nodes = [nodeOf("node-1", [])];
  assert.equal(await admit(), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": [] }, rules: [] });

  // Two replicas at once: the one whose write finds the policy written since reads it again.
  state.nodes = [nodeOf("node-1", ["172.18.0.2"])];
  let raced = false;
  state.beforeWrite = () => {
    if (raced) return;
    raced = true;
    state.beforeWrite = () => {};
    // The other replica writes between this one's read and its write, of a node that joined meanwhile.
    state.nodes.push(nodeOf("node-2", ["172.18.0.3"]));
    Effect.runSync(admitNodes(kube, "alasio", "alasio-seaweedfs-from-nodes", STORE_LABELS).pipe(Effect.orDie));
  };
  assert.equal(await admit(), "unchanged");
  assert.equal(raced, true);
  assert.deepEqual(admitted(state.policy).kept, { "node-1": ["172.18.0.2"], "node-2": ["172.18.0.3"] });
});

test("an installation without the policy, whose restores read through the agent, has nothing admitted", async () => {
  const { state, kube } = fakeKube(null, [nodeOf("node-1", ["172.18.0.2"])]);
  assert.equal(await Effect.runPromise(admitNodes(kube, "alasio", "alasio-seaweedfs-from-nodes", STORE_LABELS)), null);
  assert.equal(state.writes, 0);
});
