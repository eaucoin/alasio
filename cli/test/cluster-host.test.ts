/**
 * k3s on this machine (cli/src/cluster/host.ts): what it asks root for, by what alasio
 * installed, the files that set it, its service and the operator's kubeconfig; and up,
 * which waits for the node reported ready since k3s started anew, and which, when root's
 * work failed halfway, does the rest of it the next time.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { NodeServices } from "@effect/platform-node";
import { Effect, Layer, Logger } from "effect";

import { HostCluster, kubeconfigState, planNode } from "../src/cluster/host.ts";
import { describeKept, keptReleases, type SnapshotReleases, snapshotReleasesOf } from "../src/kept-releases.ts";
import { Machine } from "../src/cluster/machine.ts";
import { gvisorReleases, NODE_PINS } from "../src/cluster/node.ts";
import { Root } from "../src/cluster/root.ts";
import { CERTIFICATE, serveFakeKube } from "./support/fake-kube.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { fakeReleases } from "./support/fake-releases.ts";

const INSTALLED = { k3s: NODE_PINS.k3s.version, gvisor: gvisorReleases(NODE_PINS), stargz: NODE_PINS.stargz.version, config: "digest" };
const RUNNING = { loaded: true, active: "active", enabled: true };

test("planNode asks for nothing once all is as alasio installed it, and for what is not", () => {
  const plan = (changes: Partial<Parameters<typeof planNode>[1]>) =>
    planNode(NODE_PINS, { stamp: INSTALLED, config: "digest", unit: RUNNING, kubeconfig: "current", firewall: null, kept: gvisorReleases(NODE_PINS), ...changes });
  const none = { firewall: null, configure: false, gvisor: { install: [], remove: [] }, stargz: false, k3s: null, kubeconfig: false, renew: false };
  assert.deepEqual(plan({}), none);
  assert.deepEqual(plan({ stamp: null, unit: { loaded: false, active: "inactive", enabled: false } }), {
    firewall: null,
    configure: true,
    gvisor: { install: gvisorReleases(NODE_PINS), remove: [] },
    stargz: true,
    k3s: "install",
    kubeconfig: true,
    renew: false,
  });
  assert.deepEqual(plan({ config: "another" }), { ...none, configure: true, k3s: "restart", kubeconfig: true });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, gvisor: ["20200101.0"] } }), { ...none, gvisor: { install: gvisorReleases(NODE_PINS), remove: ["20200101.0"] }, k3s: "restart", kubeconfig: true });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, gvisor: [...INSTALLED.gvisor, "20200101.0"] } }), { ...none, gvisor: { install: [], remove: ["20200101.0"] }, k3s: "restart", kubeconfig: true });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, gvisor: [] } }), { ...none, gvisor: { install: gvisorReleases(NODE_PINS), remove: [] }, k3s: "restart", kubeconfig: true });
  // A new stargz-snapshotter restarts k3s, and its daemon with it.
  assert.deepEqual(plan({ stamp: { ...INSTALLED, stargz: "v0.1.0" } }), { ...none, stargz: true, k3s: "restart", kubeconfig: true });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, k3s: "v1.30.0+k3s1" } }), { ...none, k3s: "install", kubeconfig: true });
  assert.deepEqual(plan({ unit: { ...RUNNING, active: "inactive", enabled: false } }), { ...none, k3s: "start", kubeconfig: true });
  assert.deepEqual(plan({ unit: { ...RUNNING, active: "failed" } }), { ...none, k3s: "start", kubeconfig: true });
  assert.deepEqual(plan({ kubeconfig: "missing" }), { ...none, kubeconfig: true });
  assert.deepEqual(plan({ kubeconfig: "expiring" }), { ...none, kubeconfig: true, renew: true });
  // An active firewall lets the cluster's networks in once alasio has opened it, the same one, to them all.
  assert.deepEqual(plan({ firewall: "ufw" }), { ...none, firewall: "ufw" });
  const opened = { tool: "ufw" as const, networks: ["10.42.0.0/16", "10.43.0.0/16"], added: ["10.42.0.0/16"] };
  assert.deepEqual(plan({ firewall: "ufw", stamp: { ...INSTALLED, firewall: opened } }), none);
  assert.deepEqual(plan({ firewall: "firewalld", stamp: { ...INSTALLED, firewall: opened } }), { ...none, firewall: "firewalld" });
  assert.deepEqual(plan({ firewall: "ufw", stamp: { ...INSTALLED, firewall: { ...opened, networks: ["10.42.0.0/16"] } } }), { ...none, firewall: "ufw" });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, firewall: opened } }), none);
  // A restart renews the certificates anyway.
  assert.deepEqual(plan({ kubeconfig: "expiring", config: "another" }), { ...none, configure: true, k3s: "restart", kubeconfig: true });
});

test("planNode installs a newly pinned gVisor release beside the one before it, and removes that once it is neither pinned nor named by a snapshot", () => {
  const plan = (releases: readonly string[], installed: readonly string[], named: SnapshotReleases | null = new Map()) =>
    planNode(fakeReleases({ gvisor: releases }).pins, {
      stamp: { ...INSTALLED, k3s: "v1.99.0+k3s1", gvisor: installed },
      config: "digest",
      unit: RUNNING,
      kubeconfig: "current",
      firewall: null,
      kept: keptReleases(releases, installed, named),
    });
  assert.deepEqual(plan(["21000101.0", "20990101.0"], ["20990101.0"]).gvisor, { install: ["21000101.0"], remove: [] });
  assert.deepEqual(plan(["21000101.0", "20990101.0"], ["20990101.0", "21000101.0"]).gvisor, { install: [], remove: [] });
  assert.deepEqual(plan(["21010101.0", "21000101.0"], ["20990101.0", "21000101.0"]).gvisor, { install: ["21010101.0"], remove: ["20990101.0"] });
  assert.equal(plan(["21000101.0"], ["20990101.0", "21000101.0"]).k3s, "restart");
  // A release a snapshot names is kept, and every one where what they name is not known.
  assert.deepEqual(plan(["21010101.0", "21000101.0"], ["20990101.0", "21000101.0"], new Map([["20990101.0", ["alasio-sessions/fs-abc"]]])).gvisor, { install: ["21010101.0"], remove: [] });
  assert.deepEqual(plan(["21010101.0"], ["20990101.0", "21000101.0"], null).gvisor, { install: ["21010101.0"], remove: [] });
});

test("the releases kept are the pinned ones first, then those installed that a snapshot names, as the cluster's Sandboxes say", () => {
  const bound = (name: string, snapshot?: string, release?: string) => ({
    metadata: { namespace: "alasio-sessions", name, annotations: { ...(snapshot ? { "alasio.dev/snapshot": snapshot } : {}), ...(release ? { "alasio.dev/snapshot-release": release } : {}) } },
  });
  const named = snapshotReleasesOf([bound("fs-a", "s-1", "20990101.0"), bound("fs-b"), bound("fs-c", "s-2", "20990101.0"), bound("fs-d", "s-3", "21000101.0")]);
  assert.deepEqual(named, new Map([["20990101.0", ["alasio-sessions/fs-a", "alasio-sessions/fs-c"]], ["21000101.0", ["alasio-sessions/fs-d"]]]));
  // One bound before its release was recorded makes what they name unknown.
  assert.equal(snapshotReleasesOf([bound("fs-a", "s-1")]), null);
  assert.deepEqual(keptReleases(["21010101.0"], ["20980101.0", "20990101.0", "21010101.0"], named), ["21010101.0", "20990101.0"]);
  assert.deepEqual(keptReleases(["21010101.0"], ["20980101.0", "20990101.0"], null), ["21010101.0", "20980101.0", "20990101.0"]);
  // As status says them: each kept, with the snapshots naming it, then those named and not kept.
  assert.deepEqual(describeKept(["21010101.0", "20990101.0"], named), [
    "gVisor 21010101.0, kept, which new sessions run: 0 snapshots name it",
    "gVisor 20990101.0, kept: 2 snapshots name it",
    "gVisor 21000101.0, not kept: 1 snapshot names it, restored from bayma's checkpoints instead",
  ]);
  assert.deepEqual(describeKept(["21010101.0"], null), ["gVisor 21010101.0, kept, which new sessions run: what snapshots name it is not seen"]);
});

/** A kubeconfig whose client certificate is `certificate`. */
const withCertificate = (certificate: string) => `apiVersion: v1
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: alasio
contexts:
- context:
    cluster: alasio
    user: alasio
  name: alasio
current-context: alasio
kind: Config
users:
- name: alasio
  user:
    client-certificate-data: ${Buffer.from(certificate).toString("base64")}
    client-key-data: S0VZ
`;

/** A certificate that ends on 7 October 2026 at 20:58:36. */
const ENDING = `-----BEGIN CERTIFICATE-----
MIIBgzCCASmgAwIBAgIUdjiZC+gm57VaB6v9xMFNf0wlNIswCgYIKoZIzj0EAwIw
FzEVMBMGA1UEAwwMc3lzdGVtOmFkbWluMB4XDTI2MTAwNjIwNTgzNloXDTI2MTAw
NzIwNTgzNlowFzEVMBMGA1UEAwwMc3lzdGVtOmFkbWluMFkwEwYHKoZIzj0CAQYI
KoZIzj0DAQcDQgAE4WTKEd965lwTbl522Vkx6/DIonQA63p4IL9WSUdRsb6id4FW
D1MBZC+oOv+CtnokSOOCZKt+HsKXEAlmABlBBKNTMFEwHQYDVR0OBBYEFNlQ8U+U
tdA8yP0iLQfwOCupnC9AMB8GA1UdIwQYMBaAFNlQ8U+UtdA8yP0iLQfwOCupnC9A
MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIhAJ0BHoACtqVyPjY0
74Z0x5XosyidAIl3AoUGKODJIsnbAiB/MV+7m6Y5Aa7o7p//9yA0ZZj79DvDX/5z
fhPINBn2Bw==
-----END CERTIFICATE-----
`;

test("the operator's kubeconfig is read anew when it is missing, or its certificate ends within 30 days", () => {
  assert.equal(kubeconfigState(null, new Date()), "missing");
  assert.equal(kubeconfigState("not: [yaml", new Date()), "missing");
  assert.equal(kubeconfigState(withCertificate("not a certificate"), new Date()), "missing");
  assert.equal(kubeconfigState(withCertificate(ENDING), new Date("2026-09-01T00:00:00Z")), "current");
  assert.equal(kubeconfigState(withCertificate(ENDING), new Date("2026-09-20T00:00:00Z")), "expiring");
  assert.equal(kubeconfigState(withCertificate(ENDING), new Date("2027-01-01T00:00:00Z")), "expiring");
  assert.equal(kubeconfigState(withCertificate(CERTIFICATE), new Date()), "current");
});

/** k3s on a machine of the test's, from `releases`, its node in a fake API, which `ready` says whether it reports ready as k3s starts. */
async function rig(t: TestContext, ready: { since: boolean }, releases = fakeReleases()) {
  const kube = await serveFakeKube();
  const directory = mkdtempSync(join(tmpdir(), "alasio-host-"));
  t.after(async () => {
    await kube.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const machine = new FakeMachine(join(directory, "machine"));
  machine.kubeconfig = kube.kubeconfig;
  const node = (heartbeat: string) => kube.put("nodes", { apiVersion: "v1", kind: "Node", metadata: { name: "machine" }, status: { conditions: [{ type: "Ready", status: "True", lastHeartbeatTime: heartbeat }] } });
  // Ready from before, as a node stopped while ready stays until its kubelet reports again.
  node("2026-01-01T00:00:00Z");
  machine.onStart = () => ready.since && node(new Date().toISOString());
  kube.put("configmaps", { apiVersion: "v1", kind: "ConfigMap", metadata: { name: "coredns", namespace: "kube-system" }, data: { NodeHosts: "" } });
  const up = () =>
    Effect.runPromise(
      Effect.flatMap(HostCluster, (cluster) => cluster.up(join(directory, "kubeconfig"))).pipe(
        Effect.match({ onFailure: (error) => error.message, onSuccess: () => null }),
        Effect.provide(
          HostCluster.layer({ storagePath: join(directory, "storage"), readyTimeout: "300 millis", poll: "5 millis" }).pipe(
            Layer.provide(Root.inProcess),
            Layer.provideMerge(Layer.mergeAll(Layer.succeed(Machine, { platform: "linux", arch: "x64", root: machine.root }), machine.layer, machine.systemd, releases.layer)),
            Layer.provideMerge(NodeServices.layer),
          ),
        ),
        Effect.provide(Logger.layer([])),
      ),
    );
  return { kube, machine, up };
}

test("up waits for the node reported ready since k3s started anew, not before", async (t) => {
  const ready = { since: false };
  const { up } = await rig(t, ready);
  assert.match(await up() ?? "", /^k3s on this machine was not ready within 0\.3s, waiting for its node: not reported ready since started: machine$/u);
  ready.since = true;
  assert.equal(await up(), null);
});

test("up applies gVisor's RuntimeClasses, deleting first alasio's of releases no longer kept, and one whose handler is another, which cannot change", async (t) => {
  const { kube, up } = await rig(t, { since: true }, fakeReleases({ gvisor: ["21000101.0", "20990101.0"] }));
  const labels = { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/component": "gvisor" };
  const runtimeClass = (name: string, handler: string, labelled = true) =>
    kube.put("runtimeclasses", { apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name, ...(labelled ? { labels } : {}) }, handler });
  runtimeClass("gvisor", "runsc", false);
  runtimeClass("gvisor-20980101-0", "runsc-20980101-0");
  runtimeClass("kata", "kata", false);
  assert.equal(await up(), null);
  const handlers = [...kube.objects].filter(([path]) => path.startsWith("/apis/node.k8s.io/v1/runtimeclasses/")).map(([, { metadata, handler }]) => [metadata.name, handler]);
  assert.deepEqual(handlers.sort(), [["gvisor", "runsc-21000101-0"], ["gvisor-20990101-0", "runsc-20990101-0"], ["gvisor-21000101-0", "runsc-21000101-0"], ["kata", "kata"]]);
});

test("what root's work did not do as it failed is done the next time, as alasio installed what it did", async (t) => {
  const { machine, up } = await rig(t, { since: true });
  machine.onRun = (ran) => (ran.command === "sh" ? "exited with 1" : machine.act(ran));
  assert.match(await up() ?? "", /^sh \/.*\/install\.sh exited with 1$/u);
  assert.deepEqual(Object.keys(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? "")).sort(), ["config", "gvisor", "stargz"]);
  machine.onRun = (ran) => machine.act(ran);
  assert.equal(await up(), null);
  assert.deepEqual(machine.commands().map((command) => command.replace(/\/\S+\/install\.sh/u, "install.sh")), ["systemctl daemon-reload", "sh install.sh", "sh install.sh"]);
});
