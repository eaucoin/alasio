/**
 * The bundled store's admission of its nodes (src/node-agent/store-admission.ts): each node's
 * agent keeps its node's addresses in the policy of the store's S3 port, the cluster's nodes'
 * alone.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect } from "effect";

import { KubeApiError } from "../src/kube/client.ts";
import type { HostCommands } from "../src/node-agent/host.ts";
import { admitNode, NODE_ADDRESSES_ANNOTATION, nodeAddresses } from "../src/node-agent/store-admission.ts";

/** A policy as the installation applies it: of the store's pods, of no rule of its own. */
const POLICY: KubernetesObject & { readonly spec: Record<string, unknown> } = {
  apiVersion: "networking.k8s.io/v1",
  kind: "NetworkPolicy",
  metadata: { name: "alasio-seaweedfs-from-nodes", namespace: "alasio", resourceVersion: "1" },
  spec: { podSelector: { matchLabels: { "app.kubernetes.io/component": "seaweedfs" } }, policyTypes: ["Ingress"] },
};

/**
 * The API server as the agents call it: the policy, if any, written only as of the version it
 * is (409 otherwise, as before each write `beforeWrite` may change it), and the nodes it has.
 */
function fakeKube(policy: KubernetesObject | null, nodes: readonly string[]) {
  const state = { policy, nodes: new Set(nodes), writes: 0, beforeWrite: () => {} };
  return {
    state,
    kube: {
      read: (apiVersion: string, kind: string, _namespace: string, name: string) =>
        Effect.sync(() => {
          if (kind === "Node") return state.nodes.has(name) ? ({ apiVersion, kind, metadata: { name } } as KubernetesObject) : null;
          return state.policy?.metadata?.name === name ? structuredClone(state.policy) : null;
        }),
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

test("a node's addresses are its interfaces' but its loopback's and link-local ones, as its own network says them", async () => {
  let ran: readonly string[] = [];
  const host: HostCommands = {
    run: () => Effect.die("not run"),
    inNetwork: (command) =>
      Effect.sync(() => {
        ran = command;
        return {
          exitCode: 0,
          stderr: "",
          stdout: JSON.stringify({
            lo: [{ address: "127.0.0.1", family: "IPv4", internal: true }, { address: "::1", family: "IPv6", internal: true }],
            eth0: [{ address: "172.18.0.3", family: "IPv4", internal: false }, { address: "fe80::42:acff:fe12:3", family: "IPv6", internal: false }],
            "flannel.1": [{ address: "10.42.1.0", family: "IPv4", internal: false }],
            cni0: [{ address: "10.42.1.1", family: "IPv4", internal: false }, { address: "fd00:42:1::1", family: "IPv6", internal: false }],
            "eth0:1": [{ address: "169.254.10.1", family: "IPv4", internal: false }, { address: "172.18.0.3", family: "IPv4", internal: false }],
          }),
        };
      }),
  };
  assert.deepEqual(await Effect.runPromise(nodeAddresses(host, "/usr/bin/node")), ["10.42.1.0", "10.42.1.1", "172.18.0.3", "fd00:42:1::1"]);
  assert.equal(ran[0], "/usr/bin/node");
});

test("each node's agent keeps its node's addresses in the policy, which admits every address the cluster's nodes hold to the store's S3 port and no other: a node that joins later is admitted, one gone is dropped, one with none leaves no rule that would admit everyone, and a write that finds the policy written since reads it again", async () => {
  const { state, kube } = fakeKube(structuredClone(POLICY), ["node-1", "node-2"]);
  const admit = (node: string, addresses: readonly string[]) => Effect.runPromise(admitNode(kube, "alasio", "alasio-seaweedfs-from-nodes", node, addresses));

  assert.equal(await admit("node-1", ["10.42.0.0", "172.18.0.2"]), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": ["10.42.0.0", "172.18.0.2"] }, rules: [{ ports: PORT, from: ["10.42.0.0/32", "172.18.0.2/32"] }] });
  // As it was: nothing written.
  assert.equal(await admit("node-1", ["10.42.0.0", "172.18.0.2"]), "unchanged");
  assert.equal(state.writes, 1);
  // A node that joins later, of an IPv6 address too.
  assert.equal(await admit("node-2", ["10.42.1.0", "fd00:42:1::1"]), "written");
  assert.deepEqual(admitted(state.policy).rules, [{ ports: PORT, from: ["10.42.0.0/32", "10.42.1.0/32", "172.18.0.2/32", "fd00:42:1::1/128"] }]);
  // Its own addresses changed: the ones before go.
  assert.equal(await admit("node-1", ["10.42.0.0", "172.18.0.9"]), "written");
  assert.deepEqual(admitted(state.policy).kept, { "node-1": ["10.42.0.0", "172.18.0.9"], "node-2": ["10.42.1.0", "fd00:42:1::1"] });
  // A node gone from the cluster is dropped by the next agent that writes.
  state.nodes.delete("node-2");
  assert.equal(await admit("node-1", ["10.42.0.0", "172.18.0.9"]), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": ["10.42.0.0", "172.18.0.9"] }, rules: [{ ports: PORT, from: ["10.42.0.0/32", "172.18.0.9/32"] }] });
  // None held: no rule, as a rule of no peer would admit everyone.
  assert.equal(await admit("node-1", []), "written");
  assert.deepEqual(admitted(state.policy), { kept: { "node-1": [] }, rules: [] });

  // Two agents at once: the one whose write finds the policy written since reads it again, so neither leaves the other out.
  state.nodes.add("node-2");
  let raced = false;
  state.beforeWrite = () => {
    if (raced) return;
    raced = true;
    state.beforeWrite = () => {};
    // node-2's agent writes between node-1's read and its write.
    Effect.runSync(admitNode(kube, "alasio", "alasio-seaweedfs-from-nodes", "node-2", ["10.42.1.0"]).pipe(Effect.orDie));
  };
  assert.equal(await admit("node-1", ["10.42.0.0"]), "written");
  assert.deepEqual(admitted(state.policy).kept, { "node-1": ["10.42.0.0"], "node-2": ["10.42.1.0"] });
});

test("an installation without the policy, whose restores read through the agent, has nothing admitted", async () => {
  const { state, kube } = fakeKube(null, ["node-1"]);
  assert.equal(await Effect.runPromise(admitNode(kube, "alasio", "alasio-seaweedfs-from-nodes", "node-1", ["10.42.0.0"])), null);
  assert.equal(state.writes, 0);
});
