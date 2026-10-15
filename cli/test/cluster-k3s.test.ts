/**
 * What the clusters alasio makes on this machine share of k3s (cli/src/cluster/k3s.ts):
 * the registries containerd pulls from, CoreDNS's host aliases, gVisor's releases as
 * containerd's runtimes and their RuntimeClasses, nodes ready since they started, and the
 * kubeconfig k3s writes made the operator's.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { KubeConfig } from "@kubernetes/client-node";

import { runscFlags } from "../../src/node-agent/runsc.ts";
import { clusterKubeconfig, gvisorRuntimeClasses, gvisorRuntimeFiles, nodeHostsWith, readySince, registriesYaml, staleRuntimeClasses } from "../src/cluster/k3s.ts";

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

const CONTAINERD = "/var/lib/rancher/k3s/agent/etc/containerd";

test("each gVisor release kept is a runtime of containerd's, after k3s's own configuration, running its own shim, with gVisor's pod annotations and its runsc options", () => {
  const files = gvisorRuntimeFiles(["20260921.0-eaucoin.4", "20260921.0-eaucoin.3"], { systemd: false });
  assert.deepEqual([...files.keys()], [`${CONTAINERD}/config-v3.toml.tmpl`, `${CONTAINERD}/runsc-20260921-0-eaucoin-4.toml`, `${CONTAINERD}/runsc-20260921-0-eaucoin-3.toml`]);
  const template = files.get(`${CONTAINERD}/config-v3.toml.tmpl`) ?? "";
  assert.match(template, /^\{\{ template "base" \. \}\}\n\n# /u);
  const runtime = (label: string, release: string) =>
    [
      "",
      `[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-${label}]`,
      `  runtime_type = "io.containerd.runsc.v1"`,
      `  runtime_path = "/usr/local/lib/alasio/gvisor/${release}/containerd-shim-runsc-v1"`,
      `  pod_annotations = ["dev.gvisor.internal.restore.*", "dev.gvisor.internal.cpufeatures", "dev.gvisor.spec.mount.*"]`,
      "",
      `[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-${label}.options]`,
      `  TypeUrl = "io.containerd.runsc.v1.options"`,
      `  ConfigPath = "${CONTAINERD}/runsc-${label}.toml"`,
    ].join("\n");
  assert.ok(template.endsWith(`${runtime("20260921-0-eaucoin-4", "20260921.0-eaucoin.4")}\n${runtime("20260921-0-eaucoin-3", "20260921.0-eaucoin.3")}\n`), template);
  const options = files.get(`${CONTAINERD}/runsc-20260921-0-eaucoin-3.toml`) ?? "";
  assert.match(options, /^# runsc's options for gVisor 20260921\.0-eaucoin\.3, /u);
  assert.match(options, /\nbinary_name = "\/usr\/local\/lib\/alasio\/gvisor\/20260921\.0-eaucoin\.3\/runsc"\nrestore_direct = true\nrestore_background = true\n\n\[runsc_config\]\n {2}restore-validate-files = "all"\n(?: {2}# [^\n]*\n)+ {2}dirty-tracking = "wp"\n {2}working-set-window = "3s"\n(?: {2}# [^\n]*\n)+ {2}allow-live-tcp-migration = "false"\n$/u);
  // As the node agent reads them, each the flag its shim passes runsc.
  assert.deepEqual(runscFlags(options), ["--restore-validate-files=all", "--dirty-tracking=wp", "--working-set-window=3s", "--allow-live-tcp-migration=false"]);
});

test("a single release kept is one runtime of containerd's, and nothing is rendered for a release before it", () => {
  const files = gvisorRuntimeFiles(["21000101.0"], { systemd: false });
  assert.deepEqual([...files.keys()], [`${CONTAINERD}/config-v3.toml.tmpl`, `${CONTAINERD}/runsc-21000101-0.toml`]);
  assert.equal((files.get(`${CONTAINERD}/config-v3.toml.tmpl`) ?? "").match(/^\[plugins\.'io\.containerd\.cri\.v1\.runtime'\.containerd\.runtimes\.runsc-[^.\]]+\]$/gmu)?.length, 1);
  assert.deepEqual(gvisorRuntimeClasses(["21000101.0"]).map(({ metadata, handler }) => [metadata?.name, handler]), [["gvisor", "runsc-21000101-0"], ["gvisor-21000101-0", "runsc-21000101-0"]]);
  // The classes of the releases kept before it go.
  assert.deepEqual(staleRuntimeClasses(gvisorRuntimeClasses(["20990101.0", "20980101.0"]), gvisorRuntimeClasses(["21000101.0"])), ["gvisor", "gvisor-20990101-0", "gvisor-20980101-0"]);
});

test("where systemd runs the machine, runsc places its sandboxes' cgroups through it, and only there", () => {
  const options = (systemd: boolean) => gvisorRuntimeFiles(["20990101.0"], { systemd }).get(`${CONTAINERD}/runsc-20990101-0.toml`) ?? "";
  assert.match(options(true), /\n {2}allow-live-tcp-migration = "false"\n {2}# [^\n]*\n {2}systemd-cgroup = "true"\n$/u);
  assert.doesNotMatch(options(false), /systemd/u);
});

test("the gvisor RuntimeClass runs the current release, and each release kept has one of its own, labelled as alasio's", () => {
  const labels = { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/component": "gvisor" };
  assert.deepEqual(gvisorRuntimeClasses(["21000101.0", "20990101.0"]).map(({ apiVersion, kind, metadata, handler }) => [apiVersion, kind, metadata?.name, metadata?.labels, handler]), [
    ["node.k8s.io/v1", "RuntimeClass", "gvisor", labels, "runsc-21000101-0"],
    ["node.k8s.io/v1", "RuntimeClass", "gvisor-21000101-0", labels, "runsc-21000101-0"],
    ["node.k8s.io/v1", "RuntimeClass", "gvisor-20990101-0", labels, "runsc-20990101-0"],
  ]);
});

test("the RuntimeClasses deleted are alasio's of releases no longer kept, and any desired with another handler, which cannot change", () => {
  const desired = gvisorRuntimeClasses(["21000101.0", "20990101.0"]);
  const others = { metadata: { name: "kata" }, handler: "kata" };
  assert.deepEqual(staleRuntimeClasses([...gvisorRuntimeClasses(["20990101.0", "20980101.0"]), others], desired), ["gvisor", "gvisor-20980101-0"]);
  // One not labelled as alasio's, of a name alasio's has.
  assert.deepEqual(staleRuntimeClasses([{ metadata: { name: "gvisor" }, handler: "runsc" }, others], desired), ["gvisor"]);
  assert.deepEqual(staleRuntimeClasses(desired, desired), []);
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
