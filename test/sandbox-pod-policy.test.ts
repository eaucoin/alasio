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

import { request } from "node:https";

import { gvisorReleases, NODE_PINS } from "../cli/src/cluster/node.ts";
import { decodeInstallConfig, manifests, NOTHING_CARRIED } from "../cli/src/manifests/index.ts";
import { GATEWAY_USER } from "../cli/src/manifests/sandbox-gateway.ts";
import { gvisorRuntimeClass, SNAPSHOT_CACHE_DIR } from "../src/node-agent/names.ts";
import { refusedByPolicy } from "../src/gateway/refusals.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>) => run(Effect.flatMap(KubeClient, use));

const decoded = decodeInstallConfig({ alasio: { telegram: { existingSecret: "telegram" } } });
if (Result.isFailure(decoded)) throw new Error(decoded.failure.message);
// Main's sessions snapshotted, as the nodes' CPU baseline is recorded: the gateway restores them.
const objects = manifests(decoded.success, { ...NOTHING_CARRIED, cpuBaseline: ["sse"] });
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

let api: { readonly server: string; readonly token: string; readonly ca: Buffer };

/** Whether the API server makes `pod` asked by `user`, as the administrator impersonates it: its name, or why not. */
const madeAs = (user: string, pod: V1Pod): Promise<{ readonly name: string } | { readonly refused: string }> =>
  new Promise((resolve, reject) => {
    const url = new URL(`${api.server}/api/v1/namespaces/${pod.metadata?.namespace}/pods`);
    const asked = request(url, { method: "POST", ca: api.ca, headers: { authorization: `Bearer ${api.token}`, "content-type": "application/json", "impersonate-user": user, "impersonate-group": "system:authenticated" } }, (response) => {
      let body = "";
      response.on("data", (chunk: Buffer) => (body += chunk.toString()));
      response.on("end", () => {
        const answered = JSON.parse(body) as { metadata?: { name?: string }; message?: string };
        resolve(response.statusCode === 201 ? { name: answered.metadata?.name ?? "" } : { refused: answered.message ?? body });
      });
    });
    asked.on("error", reject);
    asked.end(JSON.stringify(pod));
  });

before(async () => {
  const started = await startKubeApiServer();
  process.env["KUBECONFIG"] = started.kubeconfig;
  const config = JSON.parse((await import("node:fs")).readFileSync(started.kubeconfig, "utf8")) as { clusters: { cluster: { "certificate-authority-data": string } }[] };
  api = { server: started.server, token: started.token, ca: Buffer.from(config.clusters[0]?.cluster["certificate-authority-data"] ?? "", "base64") };
  await withKube((kube) =>
    Effect.gen(function*() {
      for (const object of objects.filter(({ kind }) => kind === "PriorityClass")) yield* kube.create(object);
      // gVisor's, which the pools run under, and another, which none may.
      yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" });
      yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "runc" }, handler: "runc" });
      // The kept releases', which restores run under.
      for (const release of gvisorReleases(NODE_PINS)) yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: gvisorRuntimeClass(release) }, handler: gvisorRuntimeClass(release) });
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: SESSIONS } });
      for (const object of objects.filter(({ metadata }) => metadata?.name === "alasio-sandbox-pods")) yield* kube.create(object);
      // What the gateway may do in main's sessions, as alasio up gives it; and another, which may make pods there too.
      for (const object of objects.filter(({ kind, metadata }) => (kind === "Role" || kind === "RoleBinding") && metadata?.name === "alasio-sandbox-gateway" && metadata.namespace === SESSIONS)) yield* kube.create(object);
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata: { name: "pod-maker", namespace: SESSIONS }, rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["create"] }] } as never);
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", metadata: { name: "pod-maker", namespace: SESSIONS }, roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "pod-maker" }, subjects: [{ kind: "User", name: "system:serviceaccount:alasio:alasio" }] } as never);
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

/** A restore's pod of the pool's, as the gateway makes it of its template, of the workspace `workspace`, changed as `change` says. */
function restorePod(workspace = "fs-abc123", change: (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void = () => {}): V1Pod {
  const { ["alasio.dev/free"]: _free, ...labels } = template?.metadata?.labels ?? {};
  const [release] = gvisorReleases(NODE_PINS);
  const pod = structuredClone({
    apiVersion: "v1",
    kind: "Pod",
    metadata: {
      generateName: "sandboxes-none-restore-",
      namespace: SESSIONS,
      labels: { ...labels, "alasio.dev/binding": "0".repeat(40) },
      annotations: {
        ...template?.metadata?.annotations,
        "alasio.dev/binding-key": `sessionfs:${workspace}`,
        "karpenter.sh/do-not-disrupt": "true",
        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
        "dev.gvisor.internal.restore.host-image-path": `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/${workspace}/20261017T120000000Z-0000000a`,
        "dev.gvisor.internal.restore.paused": "true",
      },
    },
    spec: {
      ...(template?.spec as V1PodSpec),
      runtimeClassName: gvisorRuntimeClass(release ?? ""),
      readinessGates: [{ conditionType: "alasio.dev/resumed" }],
      affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: { nodeSelectorTerms: [{ matchFields: [{ key: "metadata.name", operator: "In", values: ["node-a"] }] }] } } },
    },
  });
  change(pod as never);
  return pod as V1Pod;
}

test("a restore's pod of a pool's template is made by the sandbox gateway alone, its image its workspace's, under a release kept, on one node, ready once resumed, and bound from the first", async () => {
  const made = await madeAs(GATEWAY_USER, restorePod());
  assert.ok("name" in made, JSON.stringify(made));
  const changes: Record<string, (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void> = {
    "another release's": (pod) => void (pod.spec.runtimeClassName = "runc"),
    "free": (pod) => void ((pod.metadata["labels"] as Record<string, string>)["alasio.dev/free"] = "true"),
    "another workspace's image": (pod) => void ((pod.metadata["annotations"] as Record<string, string>)["dev.gvisor.internal.restore.host-image-path"] = `${SNAPSHOT_CACHE_DIR}/${SESSIONS}/fs-other/20261017T120000000Z-0000000a`),
    "an image elsewhere": (pod) => void ((pod.metadata["annotations"] as Record<string, string>)["dev.gvisor.internal.restore.host-image-path"] = "/etc/passwd"),
    "never resumed": (pod) => void (pod.spec.readinessGates = []),
    "on any node": (pod) => void delete pod.spec.affinity,
    "another image": (pod) => void (pod.spec.containers[0]!.image = "busybox"),
  };
  for (const [what, change] of Object.entries(changes)) {
    const refused = await madeAs(GATEWAY_USER, restorePod("fs-abc123", change));
    assert.ok("refused" in refused && /sandbox-pods/u.test(refused.refused), `${what}: ${JSON.stringify(refused)}`);
  }
  // Nor by anyone but the gateway.
  const other = await madeAs("system:serviceaccount:alasio:alasio", restorePod());
  assert.ok("refused" in other && /sandbox-pods/u.test(other.refused), JSON.stringify(other));
});
