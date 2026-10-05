/**
 * Applying an installation (cli/src/kube/apply.ts) to a fake Kubernetes API: in the order
 * what runs needs it, as plain JSON, labelled; pruned of what it no longer has but what
 * holds data and what serves volumes that remain; waited for, saying why what does not
 * run does not; and removed, a CSI driver only once its volumes are gone.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Effect, Logger, Result } from "effect";

import { installConfigOf } from "../src/config.ts";
import { installationObjects } from "../src/install.ts";
import { KubeApi } from "../src/kube/api.ts";
import { applyInstallation, removeInstallation, VOLUME_DRIVER_LABEL } from "../src/kube/apply.ts";
import { readiness } from "../src/kube/rollout.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";

const WAIT = { timeout: "2 seconds", poll: "10 millis" } as const;

/** The objects of an installation of `install`. */
function objectsOf(install: Record<string, unknown> = {}) {
  const config = installConfigOf(install, { claude: false });
  if (Result.isFailure(config)) throw new Error(config.failure);
  return installationObjects(config.success);
}

interface Rig {
  readonly kube: FakeKube;
  readonly kubeconfig: string;
  readonly progress: string[];
  readonly run: <A, E>(effect: Effect.Effect<A, E, KubeApi>) => Promise<A>;
}

async function rig(t: TestContext): Promise<Rig> {
  const kube = await serveFakeKube();
  const directory = mkdtempSync(join(tmpdir(), "alasio-apply-"));
  t.after(async () => {
    await kube.close();
    rmSync(directory, { recursive: true, force: true });
  });
  const kubeconfig = join(directory, "kubeconfig");
  writeFileSync(kubeconfig, kube.kubeconfig);
  const progress: string[] = [];
  return {
    kube,
    kubeconfig,
    progress,
    run: (effect) =>
      Effect.runPromise(
        effect.pipe(
          Effect.provide(KubeApi.layer({ path: kubeconfig })),
          Effect.provide(Logger.layer([Logger.make(({ message }) => void progress.push(String(message)))])),
        ),
      ),
  };
}

/** The plural of the resource a change was to. */
const pluralOf = (path: string): string => path.split("/").at(-2) ?? "";

const DRIVER = "csi.example.com";
const CLASS = "workspaces";
const CSI_DRIVER = `/apis/storage.k8s.io/v1/csidrivers/${DRIVER}`;
const STORAGE_CLASS = `/apis/storage.k8s.io/v1/storageclasses/${CLASS}`;
const PRIORITY_CLASS = "/apis/scheduling.k8s.io/v1/priorityclasses/mounts";
const NODE_SERVICE = "/apis/apps/v1/namespaces/kube-system/daemonsets/csi-node";
const CONTROLLER = "/apis/apps/v1/namespaces/kube-system/statefulsets/csi-controller";

/** A CSI driver, as an installation has one: its CSIDriver, its volumes' class, its mount pods' PriorityClass, and its node service and controller, labelled as the driver's. */
function driverObjects() {
  const workload = (kind: string, name: string) => ({
    apiVersion: "apps/v1",
    kind,
    metadata: { name, namespace: "kube-system", labels: { [VOLUME_DRIVER_LABEL]: DRIVER } },
    spec: { selector: { matchLabels: { app: name } }, template: { metadata: { labels: { app: name } }, spec: { containers: [{ name: "driver", image: "example.com/csi:1" }] } } },
  });
  return [
    { apiVersion: "storage.k8s.io/v1", kind: "CSIDriver", metadata: { name: DRIVER }, spec: { attachRequired: false } },
    { apiVersion: "storage.k8s.io/v1", kind: "StorageClass", metadata: { name: CLASS }, provisioner: DRIVER, reclaimPolicy: "Delete" },
    { apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", metadata: { name: "mounts" }, value: 0 },
    workload("DaemonSet", "csi-node"),
    workload("StatefulSet", "csi-controller"),
  ];
}

/** A session's workspace on the driver: its Sandbox, its claim, and the volume the driver provisioned for it. */
function provision(kube: FakeKube, session: string, volume: string): void {
  const namespace = "alasio-sessions";
  const claim = `data-${session}`;
  kube.put("sandboxes", { apiVersion: "agents.x-k8s.io/v1beta1", kind: "Sandbox", metadata: { name: session, namespace } });
  kube.put("persistentvolumeclaims", { apiVersion: "v1", kind: "PersistentVolumeClaim", metadata: { name: claim, namespace }, spec: { storageClassName: CLASS, volumeName: volume } });
  kube.put("persistentvolumes", {
    apiVersion: "v1",
    kind: "PersistentVolume",
    metadata: { name: volume },
    spec: { storageClassName: CLASS, persistentVolumeReclaimPolicy: "Delete", csi: { driver: DRIVER, volumeHandle: volume }, claimRef: { namespace, name: claim } },
    status: { phase: "Bound" },
  });
}

test("an installation is applied as alasio, in the order what runs needs it, every object labelled as the installation's", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));

  const changes = kube.changes.map(({ method, path }) => `${method} ${pluralOf(path)}`);
  const first = (change: string) => changes.indexOf(change);
  const last = (change: string) => changes.lastIndexOf(change);
  assert.equal(changes[0], "PATCH customresourcedefinitions");
  assert.ok(last("PATCH customresourcedefinitions") < first("PATCH namespaces"));
  assert.ok(last("PATCH namespaces") < first("PATCH serviceaccounts"));
  assert.ok(last("PATCH networkpolicies") < first("PATCH jobs"));
  // The setup Job runs to completion, and is deleted, before any workload is applied.
  assert.ok(first("PATCH jobs") < first("DELETE jobs"));
  assert.ok(first("DELETE jobs") < first("PATCH deployments"));
  assert.ok(first("DELETE jobs") < first("PATCH statefulsets"));
  assert.deepEqual(changes.filter((change) => change.startsWith("DELETE")), ["DELETE jobs"]);

  for (const { method, contentType, query } of kube.changes.filter(({ method }) => method === "PATCH")) {
    assert.equal(contentType, "application/apply-patch+yaml", method);
    assert.equal(query.get("fieldManager"), "alasio");
    assert.equal(query.get("force"), "true");
  }
  const stored = [...kube.objects.values()].filter(({ kind }) => kind !== "Pod");
  assert.equal(stored.length, objectsOf().length - 1);
  for (const object of stored) assert.equal(object.metadata.labels?.["alasio.dev/installation"], "alasio", `${object.kind} ${object.metadata.name}`);
});

test("objects reach the API server as the manifests have them, keys client-node's model lacks included", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));
  const policy = kube.get("/apis/networking.k8s.io/v1/namespaces/alasio-sessions/networkpolicies/bayma-from-alasio");
  assert.deepEqual((policy?.spec?.["ingress"] as Array<{ from: unknown[] }>)[0]?.from.length, 1);
  const crd = kube.get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/sandboxes.agents.x-k8s.io");
  assert.match(JSON.stringify(crd), /"x-kubernetes-/u);
});

test("applying again deletes what the installation no longer has, but for what holds data and what is not its own", async (t) => {
  const { kube, progress, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));
  // Something in alasio's namespace that is not the installation's.
  kube.put("deployments", { apiVersion: "apps/v1", kind: "Deployment", metadata: { name: "mine", namespace: "alasio" } });
  const before = kube.changes.length;

  await run(applyInstallation(objectsOf({ sessions: { enabled: false }, neon: { enabled: false, external: { existingSecret: "database" } }, workspaceStorage: { enabled: false } }), WAIT));

  const deleted = kube.changes.slice(before).filter(({ method }) => method === "DELETE").map(({ path }) => path);
  assert.ok(deleted.includes("/apis/apps/v1/namespaces/alasio/deployments/alasio-neon-compute"));
  assert.ok(deleted.includes("/apis/apps/v1/namespaces/alasio/statefulsets/alasio-neon-safekeeper"));
  assert.ok(deleted.includes("/apis/networking.k8s.io/v1/namespaces/alasio-sessions/networkpolicies/default-deny"));
  assert.ok(deleted.includes("/apis/rbac.authorization.k8s.io/v1/namespaces/alasio-sessions/roles/alasio"));
  assert.ok(!deleted.includes("/api/v1/namespaces/alasio-sessions"));
  assert.ok(!deleted.includes("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-neon-control"));
  assert.ok(kube.get("/api/v1/namespaces/alasio-sessions"));
  assert.ok(kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-neon-control"));
  assert.ok(kube.get("/apis/apps/v1/namespaces/alasio/deployments/mine"));
  assert.ok(progress.includes("kept Namespace alasio-sessions, no longer part of alasio, as it holds data; alasio uninstall --purge deletes it"));
  assert.ok(progress.includes("deleted Deployment alasio/alasio-neon-compute, no longer part of alasio"));
});

test("a Job of the installation's left from before, as by an apply interrupted, is deleted and run afresh, its pod template changed", async (t) => {
  const { kube, run } = await rig(t);
  const job = "/apis/batch/v1/namespaces/alasio/jobs/alasio-neon-setup";
  kube.put("jobs", { apiVersion: "batch/v1", kind: "Job", metadata: { name: "alasio-neon-setup", namespace: "alasio" }, spec: { template: { spec: { containers: [] } } } });
  await run(applyInstallation(objectsOf(), WAIT));
  const changes = kube.changes.filter(({ path }) => path === job).map(({ method }) => method);
  assert.deepEqual(changes, ["DELETE", "PATCH", "DELETE"]);
});

test("a workload that does not run is waited for until the timeout, which says why", async (t) => {
  const { kube, progress, run } = await rig(t);
  kube.stuck.add("alasio");
  const error = await run(Effect.flip(applyInstallation(objectsOf(), { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.equal(
    error.message,
    [
      "not ready within 0.3s, still waiting for:",
      "  Deployment alasio/alasio: 0 of 1 available",
      '    container alasio of pod alasio-0 is waiting: ImagePullBackOff: Back-off pulling image "ghcr.io/eaucoin/alasio:0.0.0-development"',
      '    warning: Failed: Failed to pull image "ghcr.io/eaucoin/alasio:0.0.0-development": not found',
    ].join("\n"),
  );
  assert.ok(progress.includes("waiting for alasio (0 of 1 available)"));
});

test("a Job that fails fails the apply, saying why, before any workload is applied", async (t) => {
  const { kube, run } = await rig(t);
  kube.failing.add("alasio-neon-setup");
  const error = await run(Effect.flip(applyInstallation(objectsOf(), WAIT)));
  assert.equal(error._tag, "RolloutFailed");
  assert.match(error.message, /^failed:\n {2}Job alasio\/alasio-neon-setup: BackoffLimitExceeded: Job has reached the specified backoff limit/u);
  assert.ok(!kube.changes.some(({ path }) => pluralOf(path) === "deployments"));
});

test("removing an installation keeps what holds data, and with purge deletes it too", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));
  await run(removeInstallation({ purge: false }, WAIT));
  const left = [...kube.objects.values()].filter(({ kind }) => kind !== "Pod").map(({ kind, metadata }) => `${kind} ${metadata.name}`).sort();
  assert.deepEqual(left, [
    "CustomResourceDefinition sandboxes.agents.x-k8s.io",
    "Namespace alasio",
    "Namespace alasio-sessions",
    "PersistentVolumeClaim alasio",
    "PersistentVolumeClaim alasio-neon-control",
  ]);
  await run(removeInstallation({ purge: true }, WAIT));
  assert.deepEqual([...kube.objects.keys()], []);
});

test("classes and CSI drivers are applied with the namespaces, before what uses them; DaemonSets with the workloads, waited for", async (t) => {
  const { kube, run } = await rig(t);
  const objects = [...objectsOf(), ...driverObjects()];
  await run(applyInstallation(objects, WAIT));
  const changes = kube.changes.map(({ method, path }) => `${method} ${pluralOf(path)}`);
  const first = (change: string) => changes.indexOf(change);
  for (const plural of ["priorityclasses", "storageclasses", "csidrivers"]) {
    assert.ok(changes.lastIndexOf("PATCH customresourcedefinitions") < first(`PATCH ${plural}`), plural);
    assert.ok(first(`PATCH ${plural}`) < first("PATCH serviceaccounts"), plural);
    assert.ok(first(`PATCH ${plural}`) < first("PATCH persistentvolumeclaims"), plural);
  }
  assert.ok(first("DELETE jobs") < first("PATCH daemonsets"));

  kube.stuck.add("csi-node");
  const error = await run(Effect.flip(applyInstallation(objects, { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /^not ready within 0\.3s, still waiting for:\n {2}DaemonSet kube-system\/csi-node: 0 of 1 nodes ready\n {4}container driver of pod csi-node-0 is waiting: ImagePullBackOff/u);
});

test("a DaemonSet is ready once every node it schedules to runs its current pod, ready", () => {
  const daemonSet = (status: Record<string, number>) => ({ apiVersion: "apps/v1", kind: "DaemonSet", metadata: { name: "csi-node", generation: 2 }, status });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 1, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 3 })), { _tag: "Waiting", status: "not yet rolled out" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 1, numberReady: 3 })), { _tag: "Waiting", status: "1 of 3 nodes updated" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 2 })), { _tag: "Waiting", status: "2 of 3 nodes ready" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 3 })), { _tag: "Ready" });
});

test("applying again keeps a CSI driver, what it needs and its volumes' class while those volumes remain, and deletes them once they are gone", async (t) => {
  const { kube, progress, run } = await rig(t);
  await run(applyInstallation([...objectsOf(), ...driverObjects()], WAIT));
  provision(kube, "session-a", "pv-a");
  await run(applyInstallation(objectsOf(), WAIT));
  for (const path of [CSI_DRIVER, STORAGE_CLASS, NODE_SERVICE, CONTROLLER]) assert.ok(kube.get(path), path);
  assert.equal(kube.get(PRIORITY_CLASS), undefined);
  assert.ok(progress.includes(`kept CSIDriver ${DRIVER}, no longer part of alasio, as volumes it serves remain (pv-a); alasio up deletes it once they are gone`));
  assert.ok(progress.includes(`kept StorageClass ${CLASS}, no longer part of alasio, as volumes it serves remain (pv-a); alasio up deletes it once they are gone`));
  assert.ok(progress.includes("kept DaemonSet kube-system/csi-node, no longer part of alasio, as volumes it serves remain (pv-a); alasio up deletes it once they are gone"));

  kube.objects.delete("/api/v1/persistentvolumes/pv-a");
  await run(applyInstallation(objectsOf(), WAIT));
  for (const path of [CSI_DRIVER, STORAGE_CLASS, NODE_SERVICE, CONTROLLER]) assert.equal(kube.get(path), undefined, path);
});

test("removing an installation keeps what serves volumes that remain; with purge, Sandboxes and their claims go first, and the driver once their volumes have", async (t) => {
  const { kube, progress, run } = await rig(t);
  await run(applyInstallation([...objectsOf(), ...driverObjects()], WAIT));
  provision(kube, "session-a", "pv-a");
  provision(kube, "session-b", "pv-b");
  await run(removeInstallation({ purge: false }, WAIT));
  for (const path of [CSI_DRIVER, STORAGE_CLASS, NODE_SERVICE, CONTROLLER, "/api/v1/persistentvolumes/pv-a"]) assert.ok(kube.get(path), path);
  assert.equal(kube.get(PRIORITY_CLASS), undefined);
  assert.ok(progress.includes(`kept CSIDriver ${DRIVER}, as volumes it serves remain (pv-a, pv-b)`));

  const before = kube.changes.length;
  await run(removeInstallation({ purge: true }, WAIT));
  const deleted = kube.changes.slice(before).filter(({ method }) => method === "DELETE").map(({ path }) => path);
  const at = (path: string) => {
    assert.ok(deleted.includes(path), path);
    return deleted.indexOf(path);
  };
  const sandboxes = ["session-a", "session-b"].map((name) => at(`/apis/agents.x-k8s.io/v1beta1/namespaces/alasio-sessions/sandboxes/${name}`));
  const claims = ["data-session-a", "data-session-b"].map((name) => at(`/api/v1/namespaces/alasio-sessions/persistentvolumeclaims/${name}`));
  const driver = [CSI_DRIVER, STORAGE_CLASS, NODE_SERVICE, CONTROLLER, "/api/v1/namespaces/alasio"].map(at);
  assert.ok(Math.max(...sandboxes) < Math.min(...claims));
  assert.ok(Math.max(...claims) < Math.min(...driver));
  assert.deepEqual([...kube.objects.keys()], []);
});

test("a volume whose data cannot be deleted stops a purge before what serves it goes, saying why", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation([...objectsOf(), ...driverObjects()], WAIT));
  provision(kube, "session-a", "pv-a");
  kube.stuck.add("pv-a");
  const error = await run(Effect.flip(removeInstallation({ purge: true }, { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(
    error.message,
    [
      "not ready within 0.3s, still waiting for:",
      "  PersistentVolume pv-a: released, its data being deleted",
      "    warning: VolumeFailedDelete: rpc error: code = Unknown desc = could not delete the data of pv-a",
    ].join("\n"),
  );
  for (const path of [CSI_DRIVER, STORAGE_CLASS, NODE_SERVICE, CONTROLLER, "/api/v1/namespaces/alasio"]) assert.ok(kube.get(path), path);
});

test("the API's refusals and what cannot be reached say which call, and why", async (t) => {
  const { kube, kubeconfig, run } = await rig(t);
  const get = Effect.flatMap(KubeApi, (api) => api.get({ apiVersion: "v1", kind: "Namespace", name: "alasio" }));
  writeFileSync(kubeconfig, kube.kubeconfig.replace("fake-kube-token", "wrong"));
  const refused = await run(Effect.flip(get));
  assert.equal(refused.message, "Kubernetes answered 401 to GET /api/v1/namespaces/alasio: Unauthorized");
  writeFileSync(kubeconfig, kube.kubeconfig);
  await kube.close();
  const unreachable = await run(Effect.flip(get));
  assert.match(unreachable.message, /^Kubernetes could not be reached for GET \/api\/v1\/namespaces\/alasio: connect ECONNREFUSED/u);
  const unusable = await Effect.runPromise(Effect.flip(Effect.provide(get, KubeApi.layer({ path: kubeconfig, context: "elsewhere" }))));
  assert.equal(unusable.message, `the kubeconfig ${kubeconfig} cannot be used: it has no context elsewhere, only fake`);
});
