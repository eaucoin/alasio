/**
 * Applying an installation (cli/src/kube/apply.ts) to a fake Kubernetes API: checked
 * against the cluster's autoscalers (cli/src/kube/autoscalers.ts); in the order what runs
 * needs it, as plain JSON, labelled; pruned of what it no longer has but what holds data
 * and what serves volumes that remain; waited for, saying why what does not run does
 * not; and removed, what a controller finalizes before the controller, and a CSI driver
 * only once its volumes are gone.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import type { KubernetesObject } from "@kubernetes/client-node";
import { Effect, Logger, Result } from "effect";

import { installConfigOf } from "../src/config.ts";
import { installationObjects } from "../src/install.ts";
import { kind, KubeApi } from "../src/kube/api.ts";
import { applyInstallation, removeInstallation } from "../src/kube/apply.ts";
import { checkAutoscalers } from "../src/kube/autoscalers.ts";
import { READY_ONCE_BOUND_ANNOTATION, readiness, readinessOf } from "../src/kube/rollout.ts";
import { type FakeKube, serveFakeKube } from "./support/fake-kube.ts";

const WAIT = { timeout: "2 seconds", poll: "10 millis" } as const;

/** The objects of an installation of `install`. */
function objectsOf(install: Record<string, unknown> = {}) {
  const config = installConfigOf(install, { claude: false, target: "docker" });
  if (Result.isFailure(config)) throw new Error(config.failure);
  return installationObjects(config.success);
}

interface Rig {
  readonly kube: FakeKube;
  readonly kubeconfig: string;
  readonly progress: string[];
  readonly run: <A, E>(effect: Effect.Effect<A, E, KubeApi>) => Promise<A>;
}

async function rig(t: TestContext, { busyFor }: { readonly busyFor?: number } = {}): Promise<Rig> {
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
          // The deadline taken anew as each run starts, as a command takes its own as its install starts.
          Effect.provide(KubeApi.layer({ path: kubeconfig }, busyFor === undefined ? {} : { busyUntil: Date.now() + busyFor })),
          Effect.provide(Logger.layer([Logger.make(({ message }) => void progress.push(String(message)))])),
        ),
      ),
  };
}

/** The plural of the resource a change was to. */
const pluralOf = (path: string): string => path.split("/").at(-2) ?? "";

const PRIORITY_CLASS = "/apis/scheduling.k8s.io/v1/priorityclasses/mounts";

/** A node service, as an installation has one: its pods' PriorityClass, and its DaemonSet. */
function nodeServiceObjects() {
  return [
    { apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", metadata: { name: "mounts" }, value: 0 },
    {
      apiVersion: "apps/v1",
      kind: "DaemonSet",
      metadata: { name: "node-service", namespace: "kube-system" },
      spec: { selector: { matchLabels: { app: "node-service" } }, template: { metadata: { labels: { app: "node-service" } }, spec: { containers: [{ name: "service", image: "example.com/node-service:1" }] } } },
    },
  ];
}

test("an installation is applied as alasio, in the order what runs needs it, every object labelled as the installation's", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));

  const changes = kube.changes.map(({ method, path }) => `${method} ${pluralOf(path)}`);
  const first = (change: string) => changes.indexOf(change);
  const last = (change: string) => changes.lastIndexOf(change);
  assert.equal(changes[0], "PATCH customresourcedefinitions");
  assert.ok(last("PATCH customresourcedefinitions") < first("PATCH namespaces"));
  // The admission policies, which hold the pods of the namespaces that admit privileged pods, before those namespaces.
  assert.ok(first("PATCH validatingadmissionpolicies") > 0 && last("PATCH validatingadmissionpolicybindings") < first("PATCH namespaces"));
  assert.ok(last("PATCH namespaces") < first("PATCH serviceaccounts"));
  assert.ok(last("PATCH networkpolicies") < first("PATCH jobs"));
  // Where the API server reaches KEDA, with what pods run as and with; KEDA itself with the workloads.
  assert.ok(last("PATCH apiservices") < first("PATCH jobs") && last("PATCH validatingwebhookconfigurations") < first("PATCH jobs"));
  // The Jobs (the setup, and VPA's certificate's) run to completion, one after the other, and are deleted, before any workload is applied.
  assert.ok(first("PATCH jobs") < first("DELETE jobs") && first("DELETE jobs") < last("PATCH jobs"));
  assert.ok(last("DELETE jobs") < first("PATCH deployments"));
  assert.ok(last("DELETE jobs") < first("PATCH statefulsets"));
  assert.deepEqual(changes.filter((change) => change.startsWith("DELETE")), ["DELETE jobs", "DELETE jobs"]);

  for (const { method, contentType, query } of kube.changes.filter(({ method }) => method === "PATCH")) {
    assert.equal(contentType, "application/apply-patch+yaml", method);
    assert.equal(query.get("fieldManager"), "alasio");
    assert.equal(query.get("force"), "true");
  }
  const stored = [...kube.objects.values()].filter(({ kind }) => kind !== "Pod");
  // All but the Jobs, deleted once run.
  assert.equal(stored.length, objectsOf().filter(({ kind }) => kind !== "Job").length);
  for (const object of stored) assert.equal(object.metadata.labels?.["alasio.dev/installation"], "alasio", `${object.kind} ${object.metadata.name}`);
});

test("a call the API server is too busy for, answered 504, 503 or 429, is asked again, after the Retry-After it says, within its bound; one failed otherwise, or past the bound, fails as answered", async (t) => {
  const { kube, run } = await rig(t, { busyFor: 5_000 });
  const policy = "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/alasio-harness-pool-pods";
  // Timed out twice as it applies the installation, as an API server under load does, then served.
  kube.busy.set(`PATCH ${policy}`, [{ status: 504 }, { status: 503 }]);
  await run(applyInstallation(objectsOf(), WAIT));
  assert.ok(kube.get(policy), "the policy was not applied");
  assert.deepEqual(kube.busy.get(`PATCH ${policy}`), []);
  // Asked to wait 2 s, past its own first wait of 1 s: asked again no sooner.
  const pool = "/api/v1/namespaces/alasio";
  kube.busy.set(`GET ${pool}`, [{ status: 429, retryAfter: 2 }]);
  const asked = Date.now();
  assert.ok(await run(Effect.flatMap(KubeApi, (api) => api.get({ ...kind("Namespace"), name: "alasio" }))));
  assert.ok(Date.now() - asked >= 2_000, `asked again ${Date.now() - asked} ms on`);
  // A 500 is the call's own failure: not asked again.
  kube.busy.set(`GET ${pool}`, [{ status: 500 }]);
  const failed = await run(Effect.flip(Effect.flatMap(KubeApi, (api) => api.get({ ...kind("Namespace"), name: "alasio" }))));
  assert.equal(failed.status, 500);
  // Done behind the timeout, then answered 504: an apply asked again sets what it set; a delete asked again finds it gone, and is done.
  kube.busy.set(`PATCH ${policy}`, [{ status: 504, committed: true }]);
  const applied = kube.changes.length;
  const before = structuredClone(kube.get(policy));
  await run(Effect.flatMap(KubeApi, (api) => api.apply(kube.get(policy) as KubernetesObject)));
  assert.deepEqual(kube.changes.slice(applied).map(({ method }) => method), ["PATCH", "PATCH"]);
  assert.deepEqual(kube.get(policy)?.spec, before?.spec);
  kube.busy.set(`DELETE ${policy}`, [{ status: 504, committed: true }]);
  await run(Effect.flatMap(KubeApi, (api) => api.remove({ ...kind("ValidatingAdmissionPolicy"), name: "alasio-harness-pool-pods" })));
  assert.equal(kube.get(policy), undefined);
  // Asked to wait past the command's deadline: failing at once, as answered.
  kube.busy.set(`GET ${pool}`, [{ status: 503, retryAfter: 60 }]);
  const told = Date.now();
  assert.equal((await run(Effect.flip(Effect.flatMap(KubeApi, (api) => api.get({ ...kind("Namespace"), name: "alasio" }))))).status, 503);
  assert.ok(Date.now() - told < 1_000, `gave up ${Date.now() - told} ms on`);
  // Too busy past the command's deadline: failing as last answered, at it, not past it; its own
  // waits of 1 s and 2 s, the third, of 4 s, past the deadline 5 s on.
  kube.busy.set(`GET ${pool}`, Array.from({ length: 100 }, () => ({ status: 503 })));
  const since = Date.now();
  const gaveUp = await run(Effect.flip(Effect.flatMap(KubeApi, (api) => api.get({ ...kind("Namespace"), name: "alasio" }))));
  assert.equal(gaveUp.status, 503);
  assert.ok(Date.now() - since >= 2_500 && Date.now() - since <= 5_500, `gave up ${Date.now() - since} ms on`);
});

test("objects reach the API server as the manifests have them, keys client-node's model lacks included", async (t) => {
  const { kube, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));
  const policy = kube.get("/apis/networking.k8s.io/v1/namespaces/alasio-sessions/networkpolicies/alasio-sandboxes");
  assert.deepEqual((policy?.spec?.["ingress"] as Array<{ from: unknown[] }>)[0]?.from.length, 1);
  const crd = kube.get("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/scaledobjects.keda.sh");
  assert.match(JSON.stringify(crd), /"x-kubernetes-/u);
});

test("applying again deletes what the installation no longer has, but for what holds data and what is not its own", async (t) => {
  const { kube, progress, run } = await rig(t);
  await run(applyInstallation(objectsOf(), WAIT));
  // Something in alasio's namespace that is not the installation's.
  kube.put("deployments", { apiVersion: "apps/v1", kind: "Deployment", metadata: { name: "mine", namespace: "alasio" } });
  const before = kube.changes.length;

  await run(applyInstallation(
    objectsOf({
      sessions: { createNamespace: false },
      lake: { enabled: false },
      objectStore: { bundled: { filerStore: { backups: { enabled: false } } } },
    }),
    WAIT,
  ));

  const deleted = kube.changes.slice(before).filter(({ method }) => method === "DELETE").map(({ path }) => path);
  assert.ok(deleted.includes("/apis/apps/v1/namespaces/alasio/deployments/alasio-lake-query"));
  assert.ok(deleted.includes("/apis/apps/v1/namespaces/alasio/deployments/alasio-neon-compute-ro"));
  assert.ok(!deleted.includes("/api/v1/namespaces/alasio-sessions"));
  assert.ok(!deleted.includes("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-seaweedfs-filer-store-backups"));
  assert.ok(kube.get("/api/v1/namespaces/alasio-sessions"));
  assert.ok(kube.get("/api/v1/namespaces/alasio/persistentvolumeclaims/alasio-seaweedfs-filer-store-backups"));
  assert.ok(kube.get("/apis/apps/v1/namespaces/alasio/deployments/mine"));
  assert.ok(progress.includes("kept Namespace alasio-sessions, no longer part of alasio, as it holds data; alasio uninstall --purge deletes it"));
  assert.ok(progress.includes("kept PersistentVolumeClaim alasio/alasio-seaweedfs-filer-store-backups, no longer part of alasio, as it holds data; alasio uninstall --purge deletes it"), progress.join("\n"));
  assert.ok(progress.includes("deleted Deployment alasio/alasio-neon-compute-ro, no longer part of alasio"));
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

test("a workload one of whose pods stops within its grace, as a node agent's does while restores read the bucket through it, is waited for past the timeout until it is gone, the wait saying so", { timeout: 60_000 }, async (t) => {
  const { kube, progress, run } = await rig(t);
  kube.stuck.add("alasio-node-agent");
  const STOPPING = "/api/v1/namespaces/alasio/pods/alasio-node-agent-before";
  /** The agent's pod before, stopping as its grace lets it until `until`, and what it logged last; held there past it, as by a finalizer, until the test removes it. */
  const stopping = (until: Date) => {
    kube.put("pods", {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: "alasio-node-agent-before",
        namespace: "alasio",
        labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" },
        deletionTimestamp: until.toISOString(),
        finalizers: ["example.com/held"],
      },
      spec: { nodeName: "machine", containers: [{ name: "node-agent", image: "alasio" }] },
      status: { phase: "Running" },
    });
    kube.logs.set("alasio-node-agent-before/node-agent", "serving\nthe node agent of machine stops once the restores reading the snapshots bucket through it are done: 1 read it now\n");
  };
  const until = new Date(Date.now() + 60_000);
  stopping(until);
  let ended = false;
  // Its timeout well past a poll, so a look the event loop is late to is still within it.
  const applied = run(applyInstallation(objectsOf(), { timeout: "1 second", poll: "10 millis" })).finally(() => (ended = true));
  /** Waits until alasio says `line`, as it waits. */
  const said = async (line: string) => {
    for (let waited = 0; !progress.includes(line); waited++) {
      assert.ok(waited < 3000, `${line} was not said: ${progress.join("\n")}`);
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  };
  // Once it waits, past its timeout, it waits on, saying for what.
  for (let waited = 0; !progress.some((line) => line.startsWith("waiting for ")); waited++) {
    assert.ok(waited < 3000, progress.join("\n"));
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  await new Promise((resolve) => setTimeout(resolve, 1500));
  assert.equal(ended, false);
  assert.ok(
    progress.includes(`waiting for alasio-node-agent (0 of 1 nodes ready; pod alasio-node-agent-before on machine stopping, as it may until ${until.toISOString()}, last saying: the node agent of machine stops once the restores reading the snapshots bucket through it are done: 1 read it now)`),
    progress.join("\n"),
  );
  // What it last logged, alone read.
  assert.ok(kube.logReads.length > 0);
  assert.ok(kube.logReads.every((query) => query.get("tailLines") === "1"), kube.logReads.map(String).join("; "));
  // Gone, it waits on for the agent after it, for its timeout from the last look that found
  // it; ready: done. Its pod's going and the agent's readiness are each waited on apart, as a
  // look reads the agent, then its pods, and a look that read the agent before one changed and
  // its pods after would not see the two at once.
  kube.objects.delete(STOPPING);
  await said("waiting for alasio-node-agent (0 of 1 nodes ready)");
  assert.equal(ended, false);
  const agents = kube.get("/apis/apps/v1/namespaces/alasio/daemonsets/alasio-node-agent");
  assert.ok(agents);
  agents.status = { ...agents.status, numberReady: 1 };
  await applied;

  // One past its grace, which the kubelet ends, holds no wait.
  kube.stuck.add("alasio-node-agent");
  stopping(new Date(Date.now() - 1000));
  const error = await run(Effect.flip(applyInstallation(objectsOf(), { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /^not ready within 0\.3s, still waiting for:\n {2}DaemonSet alasio\/alasio-node-agent: 0 of 1 nodes ready; pod alasio-node-agent-before on machine stopping/u);
});

test("a stopping pod whose container has ended is said to say nothing last, not what the kubelet answers in its log's place", async (t) => {
  const { kube, run } = await rig(t);
  for (const [logged, saying] of [
    ["serving\nstopping now\n", ", last saying: stopping now"],
    ["unable to retrieve container logs for containerd://5a9aca661f7f4f9ccc21f1425fec61d6c81964a5d156a8a42fdbafb138bdb0a3", ""],
  ] as const) {
    kube.stuck.add("alasio-node-agent");
    kube.put("pods", {
      apiVersion: "v1",
      kind: "Pod",
      metadata: {
        name: "alasio-node-agent-before",
        namespace: "alasio",
        labels: { "app.kubernetes.io/name": "alasio", "app.kubernetes.io/instance": "alasio", "app.kubernetes.io/component": "node-agent" },
        deletionTimestamp: new Date(Date.now() - 1000).toISOString(),
        finalizers: ["example.com/held"],
      },
      spec: { nodeName: "machine", containers: [{ name: "node-agent", image: "alasio" }] },
      status: { phase: "Running" },
    });
    kube.logs.set("alasio-node-agent-before/node-agent", logged);
    const error = await run(Effect.flip(applyInstallation(objectsOf(), { timeout: "300 millis", poll: "10 millis" })));
    assert.equal(error._tag, "NotReadyInTime");
    const pod = /pod alasio-node-agent-before on machine stopping, as it may until [^,;\n)]+(.*)$/mu.exec(error.message);
    assert.ok(pod, error.message);
    assert.equal(pod[1], saying, error.message);
  }
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
    "CustomResourceDefinition cloudeventsources.eventing.keda.sh",
    "CustomResourceDefinition clustercloudeventsources.eventing.keda.sh",
    "CustomResourceDefinition clustertriggerauthentications.keda.sh",
    "CustomResourceDefinition scaledjobs.keda.sh",
    "CustomResourceDefinition scaledobjects.keda.sh",
    "CustomResourceDefinition triggerauthentications.keda.sh",
    "CustomResourceDefinition verticalpodautoscalercheckpoints.autoscaling.k8s.io",
    "CustomResourceDefinition verticalpodautoscalers.autoscaling.k8s.io",
    "Namespace alasio",
    "Namespace alasio-sessions",
    "PersistentVolumeClaim alasio-seaweedfs-filer-store-backups",
  ]);
  await run(removeInstallation({ purge: true }, WAIT));
  assert.deepEqual([...kube.objects.keys()], []);
});

test("what scales a workload is applied after the workloads, and a ScaledObject, which KEDA finalizes, is deleted and waited for before KEDA is, pruned or removed", async (t) => {
  const { kube, run } = await rig(t);
  const operator = "/apis/apps/v1/namespaces/alasio/deployments/alasio-keda-operator";
  const path = "/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/scaled";
  kube.finalizers.set("finalizer.keda.sh", operator);
  const scaledObject = { apiVersion: "keda.sh/v1alpha1", kind: "ScaledObject", metadata: { name: "scaled", namespace: "alasio" }, spec: { scaleTargetRef: { name: "alasio" } } };
  /** Applies the installation of `install` with the ScaledObject, which KEDA then finalizes. */
  const applyScaled = async (install: Record<string, unknown> = {}) => {
    await run(applyInstallation([...objectsOf(install), scaledObject], WAIT));
    const applied = kube.get(path);
    assert.ok(applied);
    applied.metadata["finalizers"] = ["finalizer.keda.sh"];
  };

  await applyScaled();
  const changes = kube.changes.map(({ method, path }) => `${method} ${pluralOf(path)}`);
  assert.ok(changes.lastIndexOf("PATCH deployments") < changes.indexOf("PATCH scaledobjects"));

  // The ScaledObject and KEDA both go, KEDA once it has let go of the ScaledObject.
  await run(applyInstallation(objectsOf({ autoscaling: { keda: { enabled: false } } }), WAIT));
  assert.equal(kube.get(path), undefined);
  assert.equal(kube.get(operator), undefined);

  await applyScaled();
  await run(removeInstallation({ purge: false }, WAIT));
  assert.equal(kube.get(path), undefined);
  const deleted = kube.changes.filter(({ method }) => method === "DELETE").map(({ path }) => path);
  assert.ok(deleted.lastIndexOf(path) < deleted.lastIndexOf(operator));
});

test("a workload whose scale becomes a range keeps the replicas it runs as its scaler takes them over, rather than dropping to one", async (t) => {
  const { kube, run } = await rig(t);
  const grafana = "/apis/apps/v1/namespaces/alasio/deployments/alasio-grafana";
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 2, max: 2 } } }), WAIT));
  const before = kube.changes.length;
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 2, max: 4 } } }), WAIT));
  assert.equal(kube.get(grafana)?.spec?.["replicas"], 2);
  const handedOver = kube.changes.slice(before).filter(({ path, query }) => path === grafana && query.get("fieldManager") === "alasio-handover");
  assert.equal(handedOver.length, 1);
  // Once handed over, applying again hands nothing over.
  const again = kube.changes.length;
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 2, max: 4 } } }), WAIT));
  assert.deepEqual(kube.changes.slice(again).filter(({ query }) => query.get("fieldManager") === "alasio-handover"), []);
});

test("a scaler no longer wanted is gone before the workloads it scaled are applied, so it sets none of their replicas back", async (t) => {
  const { kube, run } = await rig(t);
  const scaledObject = "/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/alasio-grafana";
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 1, max: 3 } } }), WAIT));
  assert.ok(kube.get(scaledObject));
  const before = kube.changes.length;
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 2, max: 2 } } }), WAIT));
  const changes = kube.changes.slice(before).map(({ method, path }) => `${method} ${path}`);
  assert.ok(changes.indexOf(`DELETE ${scaledObject}`) >= 0 && changes.indexOf(`DELETE ${scaledObject}`) < changes.findIndex((change) => change.startsWith("PATCH /apis/apps/v1/")));
  assert.equal(kube.get(scaledObject), undefined);
  assert.equal(kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio-grafana")?.spec?.["replicas"], 2);
});

test("a ScaledObject whose controller does not run to let go of it is let go of, saying so, rather than waited for until the timeout", async (t) => {
  const { kube, progress, run } = await rig(t);
  // KEDA's finalizer, of an operator that is not there.
  kube.finalizers.set("finalizer.keda.sh", "/apis/apps/v1/namespaces/elsewhere/deployments/keda-operator");
  const path = "/apis/keda.sh/v1alpha1/namespaces/alasio/scaledobjects/alasio-grafana";
  await run(applyInstallation(objectsOf({ grafana: { scaling: { min: 1, max: 3 } } }), WAIT));
  const applied = kube.get(path);
  assert.ok(applied);
  applied.metadata["finalizers"] = ["finalizer.keda.sh"];
  await run(applyInstallation(objectsOf(), WAIT));
  assert.equal(kube.get(path), undefined);
  assert.ok(progress.includes("letting go of ScaledObject alasio/alasio-grafana, as its controller has not within 2s"));
});

test("a PriorityClass whose preemption changes, which cannot change, is made anew; one unchanged is left be", async (t) => {
  const { kube, run } = await rig(t);
  const floor = "/apis/scheduling.k8s.io/v1/priorityclasses/alasio-floor";
  await run(applyInstallation(objectsOf(), WAIT));
  assert.equal(kube.get(floor)?.["preemptionPolicy"], "PreemptLowerPriority");
  const before = kube.changes.length;
  await run(applyInstallation(objectsOf({ autoscaling: { preemption: false } }), WAIT));
  assert.equal(kube.get(floor)?.["preemptionPolicy"], "Never");
  const deleted = kube.changes.slice(before).filter(({ method }) => method === "DELETE").map(({ path }) => path);
  assert.deepEqual(deleted.filter((path) => path.includes("/priorityclasses/")).sort(), [
    "/apis/scheduling.k8s.io/v1/priorityclasses/alasio-bound",
    "/apis/scheduling.k8s.io/v1/priorityclasses/alasio-component",
    floor,
    "/apis/scheduling.k8s.io/v1/priorityclasses/alasio-turn",
  ]);
  const again = kube.changes.length;
  await run(applyInstallation(objectsOf({ autoscaling: { preemption: false } }), WAIT));
  assert.deepEqual(kube.changes.slice(again).filter(({ method, path }) => method === "DELETE" && path.includes("/priorityclasses/")), []);
});

test("PriorityClasses are applied with the namespaces, before what uses them; DaemonSets with the workloads, waited for", async (t) => {
  const { kube, run } = await rig(t);
  const objects = [...objectsOf(), ...nodeServiceObjects()];
  await run(applyInstallation(objects, WAIT));
  const changes = kube.changes.map(({ method, path }) => `${method} ${pluralOf(path)}`);
  const first = (change: string) => changes.indexOf(change);
  assert.ok(changes.lastIndexOf("PATCH customresourcedefinitions") < first("PATCH priorityclasses"));
  assert.ok(first("PATCH priorityclasses") < first("PATCH serviceaccounts"));
  assert.ok(first("PATCH priorityclasses") < first("PATCH persistentvolumeclaims"));
  assert.ok(first("DELETE jobs") < first("PATCH daemonsets"));
  assert.ok(kube.get(PRIORITY_CLASS));

  kube.stuck.add("node-service");
  const error = await run(Effect.flip(applyInstallation(objects, { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /^not ready within 0\.3s, still waiting for:\n {2}DaemonSet kube-system\/node-service: 0 of 1 nodes ready\n {4}container service of pod node-service-0 is waiting: ImagePullBackOff/u);
});

test("the nodes' mount of workspace storage is applied only once the Valkeys it reaches are ready, so its pods never take a FUSE session over as its metadata engine restarts", async (t) => {
  const { kube, run, progress } = await rig(t);
  const objects = objectsOf();
  const changed = (from = 0) => kube.changes.slice(from).filter(({ method }) => method === "PATCH").map(({ path }) => `${pluralOf(path)}/${path.split("/").at(-1)}`);
  // Its metadata engine never ready: the mount is not applied at all.
  kube.stuck.add("alasio-valkey");
  const error = await run(Effect.flip(applyInstallation(objects, { timeout: "300 millis", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /StatefulSet alasio\/alasio-valkey: /u);
  assert.ok(changed().includes("statefulsets/alasio-valkey"));
  assert.ok(!changed().includes("daemonsets/alasio-workspace-mount"), "the mount was applied as its metadata engine was not ready");
  // Ready, the mount is applied after both Valkeys, said.
  kube.stuck.delete("alasio-valkey");
  const since = kube.changes.length;
  await run(applyInstallation(objects, WAIT));
  const order = changed(since);
  const mount = order.indexOf("daemonsets/alasio-workspace-mount");
  assert.ok(mount > order.indexOf("statefulsets/alasio-valkey") && mount > order.indexOf("statefulsets/alasio-seaweedfs-filer-store"), order.join(", "));
  assert.ok(progress.includes("applying DaemonSet alasio/alasio-workspace-mount once StatefulSet alasio/alasio-valkey, StatefulSet alasio/alasio-seaweedfs-filer-store are ready"), progress.join("\n"));
});

test("the wait for the Valkeys the mount waits on and the wait for every workload share one timeout: a Valkey ready only late leaves the rest what is left of it", async (t) => {
  const { kube, run, progress } = await rig(t);
  kube.stuck.add("alasio-valkey");
  kube.stuck.add("alasio");
  /** When the mount began to wait for the Valkeys, as it says. */
  let waited: number | undefined;
  // Ready two thirds of the way through the timeout from that, as one slow to load its file is.
  const late = setInterval(() => {
    if (waited === undefined && progress.some((line) => line.startsWith("applying DaemonSet alasio/alasio-workspace-mount once"))) waited = Date.now();
    if (waited === undefined || Date.now() - waited < 2_700) return;
    clearInterval(late);
    const valkey = kube.get("/apis/apps/v1/namespaces/alasio/statefulsets/alasio-valkey");
    assert.ok(valkey);
    valkey.status = { ...valkey.status, readyReplicas: 1, availableReplicas: 1 };
  }, 10);
  t.after(() => clearInterval(late));
  const error = await run(Effect.flip(applyInstallation(objectsOf(), { timeout: "4 seconds", poll: "10 millis" })));
  assert.ok(waited !== undefined, "the mount waited for no Valkey");
  const took = Date.now() - waited;
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /Deployment alasio\/alasio: /u);
  assert.doesNotMatch(error.message, /alasio-valkey/u);
  // Said of the one timeout it was given.
  assert.match(error.message, /^not ready within 4s, /u);
  // Within its one timeout of the Valkey's wait, and what applying and saying why take: not a
  // second timeout after it, which would end it 6.7 s on at the soonest.
  assert.ok(took < 5_200, `failed ${took} ms after the Valkeys' wait began, given 4000 ms`);
});

/** The path of the Job `name` in alasio's namespace, as the fake keeps it. */
const jobPath = (name: string) => `/apis/batch/v1/namespaces/alasio/jobs/${name}`;

/**
 * Has the Job `name`, stuck running, complete `ms` after it is applied, as one that takes its
 * time does: when it completed, once it has, as the apply's pace under load is the runner's.
 */
function completesAfter(t: TestContext, kube: FakeKube, name: string, ms: number): { completedAt: number | null } {
  kube.stuck.add(name);
  const completion: { completedAt: number | null } = { completedAt: null };
  let appliedAt: number | null = null;
  const watching = setInterval(() => {
    const job = kube.get(jobPath(name));
    if (!job) return;
    appliedAt ??= Date.now();
    if (Date.now() - appliedAt < ms) return;
    clearInterval(watching);
    kube.stuck.delete(name);
    job.status = { conditions: [{ type: "Complete", status: "True", reason: "Complete", message: "" }] };
    completion.completedAt = Date.now();
  }, 10);
  t.after(() => clearInterval(watching));
  return completion;
}

test("what an apply does before its workloads' waits, a slow Job among it, takes nothing of the timeout those waits share", async (t) => {
  const { kube, run } = await rig(t);
  // A setup taking three quarters of the timeout, and alasio ready only a while after it: past the timeout from the start, within it from the waits.
  const setup = completesAfter(t, kube, "alasio-neon-setup", 1_500);
  kube.stuck.add("alasio");
  const ready = setInterval(() => {
    if (setup.completedAt === null || Date.now() - setup.completedAt < 1_500) return;
    clearInterval(ready);
    const alasio = kube.get("/apis/apps/v1/namespaces/alasio/deployments/alasio");
    assert.ok(alasio);
    alasio.status = { ...alasio.status, readyReplicas: 1, availableReplicas: 1 };
  }, 10);
  t.after(() => clearInterval(ready));
  await run(applyInstallation(objectsOf(), { timeout: "2 seconds", poll: "10 millis" }));
  assert.ok(setup.completedAt !== null && Date.now() - setup.completedAt >= 1_500, "alasio was ready before the test made it so");
});

test("a Valkey the mount waits for that is never ready, its wait begun well into the apply, fails it one timeout after that wait began, naming it", async (t) => {
  const { kube, run } = await rig(t);
  const setup = completesAfter(t, kube, "alasio-neon-setup", 1_500);
  kube.stuck.add("alasio-valkey");
  const error = await run(Effect.flip(applyInstallation(objectsOf(), { timeout: "2 seconds", poll: "10 millis" })));
  assert.equal(error._tag, "NotReadyInTime");
  assert.match(error.message, /^not ready within 2s, still waiting for:\n {2}StatefulSet alasio\/alasio-valkey: /u);
  // The Valkey's whole 2 s, from the wait that began as the Job completed, and what saying why takes.
  assert.ok(setup.completedAt !== null);
  const waited = Date.now() - setup.completedAt;
  assert.ok(waited >= 1_900 && waited < 3_500, `failed ${waited} ms after the Job completed`);
});

test("a workload's applied-after naming what the installation applies in another phase fails the apply, naming it; one naming what it has not waits for the rest alone", async (t) => {
  const { kube, run, progress } = await rig(t);
  const naming = (extra: string) => objectsOf().map((object) =>
    object.kind === "DaemonSet" && object.metadata?.name === "alasio-workspace-mount"
      ? { ...object, metadata: { ...object.metadata, annotations: { ...object.metadata.annotations, "alasio.dev/applied-after": `${object.metadata.annotations?.["alasio.dev/applied-after"]},${extra}` } } }
      : object
  );
  const configMap = objectsOf().find((object) => object.kind === "ConfigMap" && object.metadata?.namespace === "alasio");
  assert.ok(configMap?.metadata?.name);
  await assert.rejects(run(applyInstallation(naming(`ConfigMap/alasio/${configMap.metadata.name}`), WAIT)), new RegExp(`DaemonSet alasio/alasio-workspace-mount waits for ConfigMap/alasio/${configMap.metadata.name}, which is applied in another phase than its own`, "u"));
  // Refused before anything is applied.
  assert.deepEqual(kube.changes.filter(({ method }) => method === "PATCH"), []);
  await run(applyInstallation(naming("StatefulSet/alasio/alasio-nothing"), WAIT));
  assert.ok(progress.includes("applying DaemonSet alasio/alasio-workspace-mount once StatefulSet alasio/alasio-valkey, StatefulSet alasio/alasio-seaweedfs-filer-store are ready"), progress.join("\n"));
});

test("a DaemonSet is ready once every node it schedules to runs its current pod, ready", () => {
  const daemonSet = (status: Record<string, number>) => ({ apiVersion: "apps/v1", kind: "DaemonSet", metadata: { name: "csi-node", generation: 2 }, status });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 1, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 3 })), { _tag: "Waiting", status: "not yet rolled out" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 1, numberReady: 3 })), { _tag: "Waiting", status: "1 of 3 nodes updated" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 2 })), { _tag: "Waiting", status: "2 of 3 nodes ready" });
  assert.deepEqual(readiness(daemonSet({ observedGeneration: 2, desiredNumberScheduled: 3, updatedNumberScheduled: 3, numberReady: 3 })), { _tag: "Ready" });
});

test("a sandbox pool's Deployment, whose pods are ready only once bound, is ready once a pod of each replica runs, ready or not; another, once each is available", async (t) => {
  const { kube, run } = await rig(t);
  const deployment = (annotated: boolean) => ({
    apiVersion: "apps/v1",
    kind: "Deployment",
    metadata: { name: "sandboxes-none", namespace: "alasio-sessions", generation: 1, ...(annotated ? { annotations: { [READY_ONCE_BOUND_ANNOTATION]: "true" } } : {}) },
    spec: { replicas: 2, selector: { matchLabels: { "alasio.dev/pool": "sandboxes-none", "alasio.dev/free": "true" } } },
    status: { observedGeneration: 1, replicas: 2, updatedReplicas: 2, availableReplicas: 0 },
  });
  const pod = (name: string, running: boolean, labels: Record<string, string> = { "alasio.dev/pool": "sandboxes-none", "alasio.dev/free": "true" }) => ({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: "alasio-sessions", labels },
    status: { phase: running ? "Running" : "Pending", conditions: [{ type: "Ready", status: "False" }], containerStatuses: [{ name: "bayma", ready: false, state: running ? { running: {} } : { waiting: { reason: "ContainerCreating" } } }] },
  });
  kube.put("pods", pod("free-0", true));
  kube.put("pods", pod("free-1", false));
  // One bound, out of the Deployment's selector, is none of its.
  kube.put("pods", pod("bound-0", true, { "alasio.dev/pool": "sandboxes-none", "alasio.dev/binding": "0".repeat(40) }));
  const of = (object: KubernetesObject & { readonly status?: object }) => run(readinessOf(object));
  assert.deepEqual(await of(deployment(true)), { _tag: "Waiting", status: "1 of 2 running" });
  kube.put("pods", pod("free-1", true));
  assert.deepEqual(await of(deployment(true)), { _tag: "Ready" });
  assert.deepEqual(await of(deployment(false)), { _tag: "Waiting", status: "0 of 2 available" });
  // Its rollout not done: as a Deployment's.
  assert.deepEqual(await of({ ...deployment(true), status: { observedGeneration: 1, replicas: 3, updatedReplicas: 2 } }), { _tag: "Waiting", status: "1 old pods stopping" });
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

test("KEDA and VPA are installed where the cluster has none, refused where it has its own (or for KEDA another provider of external metrics), and the cluster's used where it has one", async (t) => {
  const { kube, run } = await rig(t);
  /** What checking the installation of `install`, with `objects` added, comes to. */
  const check = (install: Record<string, unknown>, objects: readonly KubernetesObject[] = []) => {
    const config = installConfigOf(install, { claude: false, target: "docker" });
    if (Result.isFailure(config)) throw new Error(config.failure);
    return run(Effect.result(checkAutoscalers([...installationObjects(config.success), ...objects], config.success))).then((result) => (Result.isFailure(result) ? result.failure.message : "fits"));
  };
  const without = (key: string) => ({ autoscaling: { [key]: { enabled: false } } });
  const scaledObject = { apiVersion: "keda.sh/v1alpha1", kind: "ScaledObject", metadata: { name: "alasio-grafana", namespace: "alasio" } };
  const verticalPodAutoscaler = { apiVersion: "autoscaling.k8s.io/v1", kind: "VerticalPodAutoscaler", metadata: { name: "alasio-lake", namespace: "alasio" } };
  const crd = (name: string) => kube.put("customresourcedefinitions", { apiVersion: "apiextensions.k8s.io/v1", kind: "CustomResourceDefinition", metadata: { name } });
  assert.equal(await check({}), "fits");
  assert.equal(await check(without("keda"), [scaledObject]), "this cluster has no KEDA, which scales alasio, alasio-codex-login, alasio-lake-loader, alasio-lake-intake, sandboxes-none, sandboxes-full, harness-codex, harness-claude, alasio-grafana: install.autoscaling.keda.enabled true installs alasio's");
  assert.match(await check(without("vpa")), /^this cluster has no VPA, which sizes alasio-keda-operator, .*alasio-lake.*: install\.autoscaling\.vpa\.enabled true installs alasio's$/u);
  // alasio, the relay of Codex's login, the lake's intake and loader, which KEDA wakes, the sandbox service's pools and the harness pools, each at a count of its own: nothing scaled.
  const fixed = { min: 1, max: 1 };
  assert.equal(await check({ ...without("keda"), alasio: { scaling: fixed, codexLogin: { scaling: fixed } }, lake: { intake: { scaling: fixed }, loader: { scaling: fixed } }, sessions: { pools: fixed }, harness: { pools: { scaling: fixed } } }), "fits");

  // alasio's own, installed, are no other's, and not the cluster's once alasio is told to use the cluster's.
  await run(applyInstallation(objectsOf(), WAIT));
  assert.equal(await check({}), "fits");
  assert.match(await check(without("vpa"), [verticalPodAutoscaler]), /^this cluster has no VPA, which sizes .*alasio-lake/u);

  kube.objects.clear();
  crd("scaledobjects.keda.sh");
  assert.equal(await check({}), "this cluster has a KEDA of its own already, and a cluster has one: install.autoscaling.keda.enabled false uses it");
  assert.equal(await check(without("keda"), [scaledObject]), "fits");
  crd("verticalpodautoscalers.autoscaling.k8s.io");
  assert.equal(await check(without("keda")), "this cluster has a VPA of its own already, and a cluster has one: install.autoscaling.vpa.enabled false uses it");
  assert.equal(await check({ autoscaling: { keda: { enabled: false }, vpa: { enabled: false } } }, [verticalPodAutoscaler]), "fits");

  kube.objects.clear();
  kube.put("apiservices", { apiVersion: "apiregistration.k8s.io/v1", kind: "APIService", metadata: { name: "v1beta1.external.metrics.k8s.io" }, spec: { service: { namespace: "monitoring", name: "prometheus-adapter" } } });
  assert.match(await check({}), /^this cluster's external metrics are served by monitoring\/prometheus-adapter already, /u);
});
