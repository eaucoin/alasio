/**
 * What the clusters alasio makes on this machine share of k3s (cli/src/cluster/k3s.ts):
 * the registries containerd pulls from, CoreDNS's host aliases, nodes ready since they
 * started, and the kubeconfig k3s writes made the operator's.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { KubeConfig } from "@kubernetes/client-node";

import { clusterKubeconfig, nodeHostsWith, readySince, registriesYaml } from "../src/cluster/k3s.ts";

/** The kubeconfig k3s writes. */
const K3S_YAML = `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: Q0E=
    server: https://127.0.0.1:6443
  name: default
contexts:
- context:
    cluster: default
    user: default
  name: default
current-context: default
kind: Config
users:
- name: default
  user:
    client-certificate-data: Q0VSVA==
    client-key-data: S0VZ
`;

test("registriesYaml says the registries in k3s's keys, leaving out what is not given", () => {
  assert.deepEqual(JSON.parse(registriesYaml({})), { mirrors: {}, configs: {} });
  assert.deepEqual(
    JSON.parse(registriesYaml({
      mirrors: { "*": { endpoint: ["https://mirror.example"], rewrite: { "^library/(.*)": "mirrored/$1" } } },
      configs: { "mirror.example": { tls: { caFile: "/etc/ssl/mirror-ca.pem", certFile: "/etc/ssl/node.pem", keyFile: "/etc/ssl/node-key.pem" } } },
    })),
    {
      mirrors: { "*": { endpoint: ["https://mirror.example"], rewrite: { "^library/(.*)": "mirrored/$1" } } },
      configs: { "mirror.example": { tls: { ca_file: "/etc/ssl/mirror-ca.pem", cert_file: "/etc/ssl/node.pem", key_file: "/etc/ssl/node-key.pem" } } },
    },
  );
});

test("nodeHostsWith keeps k3s's lines for the nodes and replaces every other with the aliases", () => {
  const nodeHosts = "172.31.252.250 otelcol.observability\n172.31.252.1 host.docker.internal\n172.31.252.3 dev-server-0";
  assert.equal(
    nodeHostsWith(nodeHosts, ["dev-server-0"], [{ ip: "172.31.252.251", hostnames: ["otelcol.observability", "otelcol"] }]),
    "172.31.252.3 dev-server-0\n172.31.252.251 otelcol.observability otelcol",
  );
  assert.equal(nodeHostsWith(nodeHosts, ["dev-server-0"], []), "172.31.252.3 dev-server-0");
});

test("readySince takes a Ready condition reported since the node started, to the second, and no other", () => {
  const node = (status: string, heartbeat: string) => ({ status: { conditions: [{ type: "Ready", status, lastHeartbeatTime: heartbeat as unknown as Date }] } });
  const started = Date.parse("2026-10-06T10:00:00.700Z");
  assert.equal(readySince(node("True", "2026-10-06T10:00:00Z"), started), true);
  assert.equal(readySince(node("True", "2026-10-06T09:59:59Z"), started), false);
  assert.equal(readySince(node("False", "2026-10-06T10:00:05Z"), started), false);
  assert.equal(readySince({}, started), false);
});

test("clusterKubeconfig names k3s's kubeconfig after the cluster, at the server given or its own, and is null for what is not yet one", () => {
  const read = (text: string | null) => {
    const config = new KubeConfig();
    config.loadFromString(text ?? "");
    return config;
  };
  const moved = read(clusterKubeconfig(K3S_YAML, "dev", "https://127.0.0.1:7443"));
  assert.equal(moved.currentContext, "dev");
  assert.equal(moved.getCurrentCluster()?.server, "https://127.0.0.1:7443");
  assert.equal(moved.getCurrentUser()?.certData, "Q0VSVA==");
  assert.equal(read(clusterKubeconfig(K3S_YAML, "alasio")).getCurrentCluster()?.server, "https://127.0.0.1:6443");
  assert.equal(clusterKubeconfig("", "dev"), null);
  assert.equal(clusterKubeconfig("apiVersion: v1\nclusters: [", "dev"), null);
});
