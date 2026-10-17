/**
 * The sandbox pods' policy (cli/src/manifests/admission.ts sandboxPodPolicy), as a real API
 * server holds pods to it: a pool's pod as its ReplicaSet makes it from its template is
 * admitted, and the same pod with any field it pins changed is refused; a pod bound changes its
 * binding's labels and annotations alone; no pod gets an ephemeral container; and no pod but a
 * pool's is made in a namespace of sessions.
 */
import assert from "node:assert/strict";
import { before, test } from "node:test";

import { CoreV1Api, KubeConfig, type KubernetesObject, type V1Deployment, type V1Pod, type V1PodSpec } from "@kubernetes/client-node";
import { Effect, Layer, Result } from "effect";

import { decodeInstallConfig, manifests } from "../cli/src/manifests/index.ts";
import { refusedByPolicy } from "../src/gateway/refusals.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>) => run(Effect.flatMap(KubeClient, use));

const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
const objects = manifests(decoded.success);
const of = <T extends KubernetesObject>(kind: string, name: string) => objects.find((object) => object.kind === kind && object.metadata?.name === name) as T;
const pool = of<V1Deployment>("Deployment", "sandboxes-none");
const template = pool.spec?.template;
assert.ok(template?.spec);
const SESSIONS = pool.metadata?.namespace ?? "";

/** A pod of the pool's, as its ReplicaSet makes it from its template, changed as `change` says. */
function poolPod(change: (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void = () => {}): V1Pod {
  const pod = structuredClone({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { generateName: "sandboxes-none-7d9f6c-", namespace: SESSIONS, labels: { ...template?.metadata?.labels, "pod-template-hash": "7d9f6c" }, annotations: { ...template?.metadata?.annotations } },
    spec: template?.spec as V1PodSpec,
  });
  change(pod);
  return pod as V1Pod;
}

/** Whether the API server makes `pod`: its name, or why not. */
const made = (pod: V1Pod): Promise<{ readonly name: string } | { readonly refused: string }> =>
  withKube((kube) => kube.create(pod).pipe(Effect.map((created) => ({ name: created.metadata?.name ?? "" })), Effect.catch((error) => Effect.succeed({ refused: error.message }))));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
  await withKube((kube) =>
    Effect.gen(function*() {
      for (const object of objects.filter(({ kind }) => kind === "PriorityClass")) yield* kube.create(object);
      // gVisor's, which the pools run under, and another, which none may.
      yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" });
      yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "runc" }, handler: "runc" });
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: SESSIONS } });
      for (const object of objects.filter(({ metadata }) => metadata?.name === "alasio-sandbox-pods")) yield* kube.create(object);
    })
  );
  // The policy in force, as the API server takes a while to: a pod it refuses refused.
  for (const deadline = Date.now() + 30_000; ; await new Promise((resolve) => setTimeout(resolve, 200))) {
    const tried = await made(poolPod((pod) => void (pod.spec.hostNetwork = true)));
    if ("refused" in tried) break;
    await withKube((kube) => kube.remove("v1", "Pod", SESSIONS, tried.name));
    assert.ok(Date.now() < deadline, "the policy never came into force");
  }
});

test("a pool's pod, as its ReplicaSet makes it from its template, is made", async () => {
  const pod = await made(poolPod());
  assert.ok("name" in pod, JSON.stringify(pod));
});

test("a pool's pod with any field its template pins changed is refused", async () => {
  const container = (pod: { spec: V1PodSpec }) => pod.spec.containers[0]!;
  const changes: Record<string, (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void> = {
    "no runtime class": (pod) => void delete pod.spec.runtimeClassName,
    "another runtime class": (pod) => void (pod.spec.runtimeClassName = "runc"),
    "its pod run as root": (pod) => void (pod.spec.securityContext = { ...pod.spec.securityContext, runAsUser: 0, runAsNonRoot: false }),
    "its container privileged": (pod) => void (container(pod).securityContext = { ...container(pod).securityContext, privileged: true, allowPrivilegeEscalation: true }),
    "its container escalating": (pod) => void (container(pod).securityContext = { ...container(pod).securityContext, allowPrivilegeEscalation: true }),
    "a capability added": (pod) => void (container(pod).securityContext = { ...container(pod).securityContext, capabilities: { drop: ["ALL"], add: ["NET_ADMIN"] } }),
    "its host path elsewhere": (pod) => void (pod.spec.volumes![0]!.hostPath = { path: "/", type: "DirectoryOrCreate" }),
    "its host path of another type": (pod) => void (pod.spec.volumes![0]!.hostPath = { ...pod.spec.volumes![0]!.hostPath!, type: "Directory" }),
    "another host path": (pod) => void pod.spec.volumes!.push({ name: "root", hostPath: { path: "/" } }),
    "its directory by a fixed subPath": (pod) => void (container(pod).volumeMounts![0] = { name: "attach", mountPath: "/attach", subPath: "other-pod", mountPropagation: "HostToContainer" }),
    "its directory by another subPathExpr": (pod) => void (container(pod).volumeMounts![0] = { ...container(pod).volumeMounts![0]!, subPathExpr: "$(POD_NAMESPACE)" }),
    "a ServiceAccount": (pod) => void (pod.spec.serviceAccountName = "alasio"),
    "its token mounted": (pod) => void (pod.spec.automountServiceAccountToken = true),
    "the host's network": (pod) => void (pod.spec.hostNetwork = true),
    "the host's PIDs": (pod) => void (pod.spec.hostPID = true),
    "the host's IPC": (pod) => void (pod.spec.hostIPC = true),
    "the cluster's DNS": (pod) => void (pod.spec.dnsPolicy = "ClusterFirst"),
    "another image": (pod) => void (container(pod).image = "busybox"),
    "another command": (pod) => void (container(pod).command = ["sh"]),
    "another container": (pod) => void pod.spec.containers.push({ name: "other", image: "busybox" }),
    "an init container": (pod) => void (pod.spec.initContainers = [{ name: "first", image: "busybox" }]),
    "another node selector": (pod) => void (pod.spec.nodeSelector = { "kubernetes.io/hostname": "elsewhere" }),
    "another internet's label": (pod) => void ((pod.metadata["labels"] as Record<string, string>)["alasio.dev/net-mode"] = "full"),
    "an annotation of its own": (pod) => void ((pod.metadata["annotations"] as Record<string, string>)["dev.gvisor.spec.mount.attach.share"] = "shared"),
  };
  for (const [what, change] of Object.entries(changes)) {
    const pod = await made(poolPod(change));
    assert.ok("refused" in pod && /sandbox-pods/u.test(pod.refused), `${what}: ${JSON.stringify(pod)}`);
  }
});

test("a pool's pod bound changes its binding's labels and annotations alone, keeps its spec, and gets no ephemeral container", async () => {
  const made_ = await made(poolPod());
  assert.ok("name" in made_);
  const read = () => withKube((kube) => kube.read("v1", "Pod", SESSIONS, made_.name)) as Promise<V1Pod>;
  const replaced = (change: (pod: V1Pod) => void) =>
    read().then((pod) => {
      change(pod);
      return withKube((kube) => kube.replace(pod).pipe(Effect.as("replaced"), Effect.catch((error) => Effect.succeed(error.message))));
    });
  // Claimed, as the binder claims it.
  assert.equal(await replaced((pod) => {
    const labels = { ...pod.metadata?.labels };
    delete labels["alasio.dev/free"];
    pod.metadata = { ...pod.metadata, labels: { ...labels, "alasio.dev/binding": "0".repeat(40) }, annotations: { ...pod.metadata?.annotations, "alasio.dev/binding-key": "sessionfs:fs-abc123", "karpenter.sh/do-not-disrupt": "true", "cluster-autoscaler.kubernetes.io/safe-to-evict": "false" } };
  }), "replaced");
  for (const [what, change] of Object.entries<(pod: V1Pod) => void>({
    "another label": (pod) => void (pod.metadata!.labels!["alasio.dev/net-mode"] = "full"),
    "another annotation": (pod) => void (pod.metadata!.annotations!["other"] = "x"),
    "its spec": (pod) => void (pod.spec!.activeDeadlineSeconds = 60),
    "an owner": (pod) => void (pod.metadata!.ownerReferences = [{ apiVersion: "v1", kind: "Pod", name: "other", uid: "00000000-0000-0000-0000-000000000000" }]),
  })) {
    const outcome = await replaced(change);
    assert.match(outcome, /sandbox-pods/u, what);
  }
  // An ephemeral container, by the subresource that adds one.
  const config = new KubeConfig();
  config.loadFromFile(process.env["KUBECONFIG"] ?? "");
  const pod = await read();
  const added = await config.makeApiClient(CoreV1Api).replaceNamespacedPodEphemeralcontainers({
    name: made_.name,
    namespace: SESSIONS,
    body: { ...pod, spec: { ...pod.spec!, ephemeralContainers: [{ name: "debug", image: "busybox" }] } },
  }).then(() => "added", (error: unknown) => String(error instanceof Error ? error.message : error));
  assert.match(added, /sandbox-pods|ephemeral container/u);
});

test("no pod but a pool's is made in a namespace of sessions", async () => {
  const other = await made({ apiVersion: "v1", kind: "Pod", metadata: { name: "other", namespace: SESSIONS }, spec: { containers: [{ name: "other", image: "busybox" }] } });
  assert.ok("refused" in other && /sandbox-pods/u.test(other.refused), JSON.stringify(other));
  // As a ReplicaSet tells of it, which the gateway counts (src/gateway/refusals.ts).
  assert.ok(refusedByPolicy({ reason: "FailedCreate", message: `Error creating: ${other.refused}`, involvedObject: {}, metadata: {} }), other.refused);
});
