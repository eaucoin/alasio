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
import { Machine } from "../src/cluster/machine.ts";
import { NODE_PINS } from "../src/cluster/node.ts";
import { Root } from "../src/cluster/root.ts";
import { CERTIFICATE, serveFakeKube } from "./support/fake-kube.ts";
import { FakeMachine } from "./support/fake-machine.ts";
import { fakeReleases } from "./support/fake-releases.ts";

const INSTALLED = { k3s: NODE_PINS.k3s.version, gvisor: NODE_PINS.gvisor.release, config: "digest" };
const RUNNING = { loaded: true, active: "active", enabled: true };

test("planNode asks for nothing once all is as alasio installed it, and for what is not", () => {
  const plan = (changes: Partial<Parameters<typeof planNode>[1]>) =>
    planNode(NODE_PINS, { stamp: INSTALLED, config: "digest", unit: RUNNING, kubeconfig: "current", firewall: null, ...changes });
  const none = { firewall: null, configure: false, gvisor: false, k3s: null, kubeconfig: false, renew: false };
  assert.deepEqual(plan({}), none);
  assert.deepEqual(plan({ stamp: null, unit: { loaded: false, active: "inactive", enabled: false } }), { firewall: null, configure: true, gvisor: true, k3s: "install", kubeconfig: true, renew: false });
  assert.deepEqual(plan({ config: "another" }), { ...none, configure: true, k3s: "restart", kubeconfig: true });
  assert.deepEqual(plan({ stamp: { ...INSTALLED, gvisor: "20200101.0" } }), { ...none, gvisor: true, k3s: "restart", kubeconfig: true });
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

/** k3s on a machine of the test's, its node in a fake API, which `ready` says whether it reports ready as k3s starts. */
async function rig(t: TestContext, ready: { since: boolean }) {
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
            Layer.provideMerge(Layer.mergeAll(Layer.succeed(Machine, { platform: "linux", arch: "x64", root: machine.root }), machine.layer, machine.systemd, fakeReleases().layer)),
            Layer.provideMerge(NodeServices.layer),
          ),
        ),
        Effect.provide(Logger.layer([])),
      ),
    );
  return { machine, up };
}

test("up waits for the node reported ready since k3s started anew, not before", async (t) => {
  const ready = { since: false };
  const { up } = await rig(t, ready);
  assert.match(await up() ?? "", /^k3s on this machine was not ready within 0\.3s, waiting for its node: not reported ready since started: machine$/u);
  ready.since = true;
  assert.equal(await up(), null);
});

test("what root's work did not do as it failed is done the next time, as alasio installed what it did", async (t) => {
  const { machine, up } = await rig(t, { since: true });
  machine.onRun = (ran) => (ran.command === "sh" ? "exited with 1" : machine.act(ran));
  assert.match(await up() ?? "", /^sh \/.*\/install\.sh exited with 1$/u);
  assert.deepEqual(Object.keys(JSON.parse(machine.read("/etc/rancher/k3s/alasio.json") ?? "")).sort(), ["config", "gvisor"]);
  machine.onRun = (ran) => machine.act(ran);
  assert.equal(await up(), null);
  assert.deepEqual(machine.commands().map((command) => command.replace(/\/\S+\/install\.sh/u, "install.sh")), ["sh install.sh", "sh install.sh"]);
});
