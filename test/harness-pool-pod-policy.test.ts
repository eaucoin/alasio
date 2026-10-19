/**
 * The harness pools' pods' policies (cli/src/manifests/admission.ts harnessPoolPodPolicy), each
 * environment's its own, as a real API server holds pods to them: a pod of a harness pool as
 * its Deployment's ReplicaSet makes it of its template is admitted, main's and a branch
 * environment's of the branch's own configuration, its overrides among it, the folder
 * conversations' as the operator with the machine's mounts; the same pod with any field the
 * template pins changed, of another branch's, of main's in a branch's namespace, or of a
 * branch's in main's, is refused; a pod of no harness pool is left alone.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { request } from "node:https";
import { before, test } from "node:test";

import type { KubernetesObject, V1Deployment, V1Pod, V1PodSpec } from "@kubernetes/client-node";
import { Effect, Layer, Result } from "effect";

import { BINDING_LABEL, FREE_LABEL, KEY_ANNOTATION, POOL_LABEL } from "../src/binder/names.ts";
import { KubeApi } from "../cli/src/kube/api.ts";
import { applyObjects } from "../cli/src/kube/apply.ts";
import { branchEnvironment, branchObjects } from "../cli/src/manifests/branch.ts";
import { MAIN_ENVIRONMENT } from "../cli/src/manifests/common.ts";
import { harnessPoolObjects } from "../cli/src/manifests/harness-pools.ts";
import { decodeInstallConfig, type InstallConfig, manifests, NOTHING_CARRIED } from "../cli/src/manifests/index.ts";
import { HARNESS_CONTAINER } from "../src/gateway/names.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>) => run(Effect.flatMap(KubeClient, use));

const decoded = decodeInstallConfig({
  alasio: { telegram: { existingSecret: "telegram" } },
  host: { enabled: true, uid: 1001, gid: 1002, home: "/home/operator", mounts: [{ name: "home", hostPath: "/home/operator", mountPath: "/home/operator" }] },
  harness: { pools: { enabled: true } },
});
if (Result.isFailure(decoded)) throw new Error("the configuration does not decode");
const config: InstallConfig = decoded.success;
const objects = manifests(config, { ...NOTHING_CARRIED, cpuBaseline: ["avx"] });
// The branch's configuration main's, overridden, as alasio branch create makes it.
const branchConfig: InstallConfig = { ...config, alasio: { ...config.alasio, env: { ...config.alasio.env, TELEGRAM_API_ROOT: "http://telegram-branch" } } };
const branch = branchObjects(branchConfig, "try", ["avx"]);

/** The Deployment `name` of `of`. */
const deployment = (of: readonly KubernetesObject[], name: string): V1Deployment => {
  const found = of.find((object) => object.kind === "Deployment" && object.metadata?.name === name) as V1Deployment | undefined;
  assert.ok(found, `no Deployment ${name}`);
  return found;
};

let made = 0;
/** A pod of the Deployment `pool`, as its ReplicaSet makes it in `namespace`, changed as `change` says. */
function poolPod(pool: V1Deployment, namespace: string, change: (pod: { metadata: { labels: Record<string, string> }; spec: V1PodSpec }) => void = () => {}): V1Pod {
  const template = structuredClone(pool.spec?.template);
  const pod = { apiVersion: "v1", kind: "Pod", metadata: { name: `${pool.metadata?.name}-5d4f8b7c9-${String((made += 1)).padStart(5, "0")}`, namespace, labels: { ...template?.metadata?.labels, "pod-template-hash": "5d4f8b7c9" } }, spec: template?.spec as V1PodSpec };
  change(pod);
  return pod as V1Pod;
}

/** Whether the API server makes `pod`: its name, or why not. */
const made_ = (pod: V1Pod) => run(Effect.flatMap(KubeClient, (kube) => kube.create<V1Pod>(pod)).pipe(Effect.map((created) => created.metadata?.name ?? ""), Effect.catch((error) => Effect.succeed({ refused: error.message }))));
const refused = (answer: string | { readonly refused: string }) => typeof answer !== "string" && answer.refused.includes("alasio-harness-pool-pods");

let api: { readonly server: string; readonly token: string; readonly ca: Buffer };

/** Whether the API server takes the merge patch `body` of the pod `name` of `namespace`, asked by the sandbox gateway, as the administrator impersonates it: its name, or why not. */
const patchedByGateway = (namespace: string, name: string, body: unknown): Promise<string | { readonly refused: string }> => patched(namespace, name, body, { gateway: true });

/** Whether the API server takes the merge patch `body` (or, `strategic`, the strategic one) of the pod `name` of `namespace`, or of its `subresource`, asked by the administrator, or by the sandbox gateway as it impersonates it: its name, or why not. */
const patched = (namespace: string, name: string, body: unknown, { gateway = false, subresource = "", strategic = false }: { readonly gateway?: boolean; readonly subresource?: string; readonly strategic?: boolean } = {}): Promise<string | { readonly refused: string }> =>
  asked("PATCH", `/api/v1/namespaces/${namespace}/pods/${name}${subresource ? `/${subresource}` : ""}`, body, { gateway, contentType: strategic ? "application/strategic-merge-patch+json" : "application/merge-patch+json" });

/** Whether the API server takes the merge patch, or the object, `body` the sandbox gateway asks `method` of `path` with, as the administrator impersonates it: what it is named, or why not. */
const askedByGateway = (method: "POST" | "PATCH" | "DELETE", path: string, body: unknown): Promise<string | { readonly refused: string }> =>
  asked(method, path, body, { gateway: true, contentType: method === "PATCH" ? "application/merge-patch+json" : "application/json" });

/** Whether the API server takes `method` of `path` with `body`, asked by the administrator, or by the sandbox gateway as it impersonates it: what it is named, or why not. */
const asked = (method: string, path: string, body: unknown, { gateway, contentType }: { readonly gateway: boolean; readonly contentType: string }): Promise<string | { readonly refused: string }> =>
  new Promise((resolve, reject) => {
    const headers = {
      authorization: `Bearer ${api.token}`,
      "content-type": contentType,
      ...(gateway ? { "impersonate-user": "system:serviceaccount:alasio:alasio-sandbox-gateway", "impersonate-group": "system:authenticated" } : {}),
    };
    const asking = request(new URL(`${api.server}${path}`), { method, ca: api.ca, headers }, (response) => {
      let text = "";
      response.on("data", (chunk: Buffer) => (text += chunk.toString()));
      response.on("end", () => {
        const answered = JSON.parse(text) as { metadata?: { name?: string }; details?: { name?: string }; message?: string };
        resolve(response.statusCode !== undefined && response.statusCode < 300 ? answered.metadata?.name ?? answered.details?.name ?? "" : { refused: answered.message ?? text });
      });
    });
    asking.on("error", reject);
    asking.end(body === undefined ? undefined : JSON.stringify(body));
  });

before(async () => {
  const started = await startKubeApiServer();
  process.env["KUBECONFIG"] = started.kubeconfig;
  const kubeconfig = JSON.parse(readFileSync(started.kubeconfig, "utf8")) as { clusters: { cluster: { "certificate-authority-data": string } }[] };
  api = { server: started.server, token: started.token, ca: Buffer.from(kubeconfig.clusters[0]?.cluster["certificate-authority-data"] ?? "", "base64") };
  await withKube((kube) =>
    Effect.gen(function*() {
      for (const object of objects.filter(({ kind }) => kind === "PriorityClass")) yield* kube.create(object);
      for (const name of ["alasio", "alasio-branch-try"]) {
        yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name } });
        yield* kube.create({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "default", namespace: name } });
      }
      for (const object of [...objects, ...branch].filter(({ metadata }) => metadata?.name?.startsWith("alasio-harness-pool-pods") || metadata?.name === "alasio-harness-pods")) yield* kube.create(object);
    })
  );
  // Each environment's policy in force, as the API server takes a while to: a pod it refuses refused.
  for (const [of, namespace] of [[objects, "alasio"], [branch, "alasio-branch-try"]] as const) {
    for (const deadline = Date.now() + 30_000; ; await new Promise((resolve) => setTimeout(resolve, 200))) {
      const tried = await made_(poolPod(deployment(of, "harness-codex"), namespace, (pod) => void (pod.spec.hostNetwork = true)));
      if (typeof tried !== "string") break;
      await withKube((kube) => kube.remove("v1", "Pod", namespace, tried));
      assert.ok(Date.now() < deadline, `${namespace}'s policy never came into force`);
    }
  }
});

test("a harness pool's pod as its ReplicaSet makes it of its template is made, main's, its folder conversations' and a branch environment's of its own configuration", async () => {
  for (const name of ["harness-codex", "harness-claude", "harness-folder-codex", "harness-folder-claude"]) {
    const answer = await made_(poolPod(deployment(objects, name), "alasio"));
    assert.equal(typeof answer, "string", `${name}: ${JSON.stringify(answer)}`);
  }
  const answer = await made_(poolPod(deployment(branch, "harness-codex"), "alasio-branch-try"));
  assert.equal(typeof answer, "string", JSON.stringify(answer));
});

test("a harness pool's pod with any field its template pins changed is refused: a folder conversation's of another user, other mounts, or more capabilities", async () => {
  const host = (pod: { spec: V1PodSpec }) => pod.spec.containers[0]!;
  const changes: Record<string, (pod: { metadata: { labels: Record<string, string> }; spec: V1PodSpec }) => void> = {
    "another user": (pod) => void (pod.spec.securityContext!.runAsUser = 0),
    "another group": (pod) => void (pod.spec.securityContext!.supplementalGroups = [0]),
    "another of the machine's paths": (pod) => void pod.spec.volumes!.push({ name: "root", hostPath: { path: "/" } }),
    "a capability": (pod) => void (host(pod).securityContext = { ...host(pod).securityContext, capabilities: { add: ["SYS_ADMIN"] } }),
    "privilege escalation": (pod) => void (host(pod).securityContext = { ...host(pod).securityContext, allowPrivilegeEscalation: true }),
    "another image": (pod) => void (host(pod).image = "busybox"),
    "another command": (pod) => void (host(pod).command = ["sh", "-c", "sleep 1d"]),
    "a variable more": (pod) => void host(pod).env!.push({ name: "NODE_OPTIONS", value: "--inspect" }),
    "the machine's network": (pod) => void (pod.spec.hostNetwork = true),
    "the account's token": (pod) => void (pod.spec.automountServiceAccountToken = true),
    "an init container": (pod) => void (pod.spec.initContainers = [{ name: "first", image: "busybox" }]),
    "a session pool's label": (pod) => void (pod.metadata.labels["alasio.dev/pool"] = "harness-codex"),
  };
  for (const [what, change] of Object.entries(changes)) {
    assert.ok(refused(await made_(poolPod(deployment(objects, "harness-folder-claude"), "alasio", change))), what);
  }
});

test("a harness pool's pod changed by anyone changes but its binding's labels and annotations: no spec, other label or annotation, owner, or ephemeral container", async () => {
  const name = await made_(poolPod(deployment(objects, "harness-codex"), "alasio"));
  assert.equal(typeof name, "string", JSON.stringify(name));
  const pod = name as string;
  // Claimed, as the gateway binds it.
  const claim = { metadata: { labels: { [FREE_LABEL]: "false", [BINDING_LABEL]: "a".repeat(40) }, annotations: { [KEY_ANNOTATION]: "harness:telegram:41" } } };
  assert.equal(await patched("alasio", pod, claim), pod);
  const changes: Record<string, { readonly body: unknown; readonly subresource?: string; readonly strategic?: boolean }> = {
    "its image": { body: { spec: { containers: [{ name: HARNESS_CONTAINER, image: "busybox" }] } }, strategic: true },
    "a label of its own": { body: { metadata: { labels: { other: "x" } } } },
    "its pool": { body: { metadata: { labels: { [POOL_LABEL]: "harness-claude" } } } },
    "an annotation of its own": { body: { metadata: { annotations: { other: "x" } } } },
    "an owner": { body: { metadata: { ownerReferences: [{ apiVersion: "v1", kind: "Pod", name: "other", uid: "00000000-0000-0000-0000-000000000000" }] } } },
    "an ephemeral container": { body: { spec: { ephemeralContainers: [{ name: "debug", image: "busybox" }] } }, subresource: "ephemeralcontainers", strategic: true },
  };
  for (const [what, { body, ...how }] of Object.entries(changes)) {
    const answer = await patched("alasio", pod, body, how);
    assert.ok(typeof answer !== "string" && answer.refused.includes("alasio-harness-pool-pods"), `${what}: ${JSON.stringify(answer)}`);
  }
});

test("a branch environment's harness pool's pod is its own branch's, in its own namespace; main's is made in no branch's", async () => {
  const otherBranch = (pod: { spec: V1PodSpec }) => {
    const variable = pod.spec.containers[0]!.env!.find(({ name }) => name === "ALASIO_BRANCH");
    if (variable) variable.value = "other";
  };
  assert.ok(refused(await made_(poolPod(deployment(branch, "harness-codex"), "alasio-branch-try", otherBranch))));
  assert.ok(refused(await made_(poolPod(deployment(objects, "harness-codex"), "alasio-branch-try"))));
  assert.ok(refused(await made_(poolPod(deployment(branch, "harness-codex"), "alasio"))));
  // Of the branch's own configuration, not main's: main's alasio up renders no branch's.
  const asMain = poolPod(deployment(branchObjects(config, "try", ["avx"]), "harness-codex"), "alasio-branch-try");
  assert.ok(refused(await made_(asMain)));
});

/** The kinds of what scales and sizes the pools, whose controllers, and their resources, this API server has none of. */
const AUTOSCALERS = ["ScaledObject", "VerticalPodAutoscaler"];

/**
 * What a pod outside the templates of the pool harness-codex of `namespace` finds as the pool is
 * first there, `pools` applied as alasio up and alasio branch create apply what they make.
 */
async function foundAsPoolsAreThere(namespace: string, pools: readonly KubernetesObject[]): Promise<string | { readonly refused: string } | undefined> {
  let found: string | { readonly refused: string } | undefined;
  let applied = false;
  const watching = (async () => {
    for (; !applied; await new Promise((resolve) => setTimeout(resolve, 10))) {
      const pool = await run(Effect.flatMap(KubeClient, (kube) => kube.read("apps/v1", "Deployment", namespace, "harness-codex")));
      if (pool) return void (found = await made_(poolPod(pool as V1Deployment, namespace, (pod) => void (pod.spec.hostNetwork = true))));
    }
  })();
  // Its workloads never ready, as no controller runs here: what it waits on is not this test's.
  await Effect.runPromise(applyObjects(pools, { timeout: "1 second", poll: "100 millis" }).pipe(Effect.ignore, Effect.provide(KubeApi.layer({ path: process.env["KUBECONFIG"] ?? "" }))));
  applied = true;
  await watching;
  return found;
}

test("alasio branch create makes nothing of a branch's pools until its pools' pods' policy is in force, which a pod outside their templates finds", async () => {
  const namespace = "alasio-branch-late";
  await withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } });
      yield* kube.create({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "default", namespace } });
    })
  );
  // Its pools, as alasio branch create makes them, of its own configuration.
  const found = await foundAsPoolsAreThere(namespace, harnessPoolObjects(branchConfig, branchEnvironment("late")).filter(({ kind }) => !AUTOSCALERS.includes(kind ?? "")));
  assert.ok(found !== undefined && refused(found), JSON.stringify(found));
});

test("alasio up makes nothing of main's pools until their pods' policy is in force, which a pod outside their templates finds", async () => {
  // main's policy gone, as before alasio up first makes it, its pools with it.
  await withKube((kube) =>
    Effect.gen(function*() {
      for (const kind of ["ValidatingAdmissionPolicyBinding", "ValidatingAdmissionPolicy"]) yield* kube.remove("admissionregistration.k8s.io/v1", kind, "", "alasio-harness-pool-pods");
      for (const name of ["harness-codex", "harness-claude", "harness-folder-codex", "harness-folder-claude"]) yield* kube.remove("apps/v1", "Deployment", "alasio", name);
    })
  );
  const found = await foundAsPoolsAreThere("alasio", harnessPoolObjects(config, MAIN_ENVIRONMENT).filter(({ kind }) => !AUTOSCALERS.includes(kind ?? "")));
  assert.ok(found !== undefined && refused(found), JSON.stringify(found));
});

test("the gateway changes no pod of a branch's own namespace but its harness pools', and binds the sandbox pods of its sessions' namespace, of a name of the same form", async () => {
  // A branch's namespace and its sessions', labelled as alasio branch create makes them, each with a pod of no harness pool, which RBAC lets the gateway change.
  const namespaces = [["alasio-branch-gw", "alasio"], ["alasio-branch-gw-sessions", "session"]] as const;
  await withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", metadata: { name: "gateway-pods" }, rules: [{ apiGroups: [""], resources: ["pods"], verbs: ["get", "patch", "update", "delete"] }] });
      for (const [name, component] of namespaces) {
        yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name, labels: { "app.kubernetes.io/component": component } } });
        yield* kube.create({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "default", namespace: name } });
        yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", metadata: { name: "gateway", namespace: name }, roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "ClusterRole", name: "gateway-pods" }, subjects: [{ kind: "ServiceAccount", name: "alasio-sandbox-gateway", namespace: "alasio" }] });
        yield* kube.create({ apiVersion: "v1", kind: "Pod", metadata: { name: "other", namespace: name }, spec: { containers: [{ name: "other", image: "busybox" }] } });
      }
    })
  );
  const claim = { metadata: { labels: { "alasio.dev/free": "false" } } };
  // In force once it refuses the gateway in the branch's own namespace.
  for (const deadline = Date.now() + 30_000; typeof (await patchedByGateway("alasio-branch-gw", "other", claim)) === "string"; await new Promise((resolve) => setTimeout(resolve, 200))) {
    assert.ok(Date.now() < deadline, "the gateway's pods' policy never came into force");
  }
  const own = await patchedByGateway("alasio-branch-gw", "other", claim);
  assert.ok(typeof own !== "string" && own.refused.includes("alasio-harness-pods"), JSON.stringify(own));
  assert.equal(await patchedByGateway("alasio-branch-gw-sessions", "other", claim), "other");
});

test("the gateway writes no Lease of an environment's namespace but its harness pools' bindings', none of KEDA's or VPA's", async () => {
  await withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata: { name: "gateway-leases", namespace: "alasio" }, rules: [{ apiGroups: ["coordination.k8s.io"], resources: ["leases"], verbs: ["get", "create", "update", "patch", "delete"] }] });
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", metadata: { name: "gateway-leases", namespace: "alasio" }, roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "gateway-leases" }, subjects: [{ kind: "ServiceAccount", name: "alasio-sandbox-gateway", namespace: "alasio" }] });
      // A leader's Lease of KEDA's, as its operator keeps it there.
      yield* kube.create({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name: "operator.keda.sh", namespace: "alasio" }, spec: { holderIdentity: "keda" } });
    })
  );
  const lease = (name: string, labels: Record<string, string>) => ({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name, namespace: "alasio", labels } });
  const binding = { [POOL_LABEL]: "harness-codex", [BINDING_LABEL]: "b".repeat(40) };
  const leases = "/apis/coordination.k8s.io/v1/namespaces/alasio/leases";
  // In force once it refuses the gateway a Lease of no binding.
  for (const deadline = Date.now() + 30_000; typeof (await askedByGateway("POST", leases, lease("probe", {}))) === "string"; await new Promise((resolve) => setTimeout(resolve, 200))) {
    await withKube((kube) => kube.remove("coordination.k8s.io/v1", "Lease", "alasio", "probe"));
    assert.ok(Date.now() < deadline, "the gateway's policy never came into force");
  }
  assert.equal(await askedByGateway("POST", leases, lease("binding-of-harness", binding)), "binding-of-harness");
  assert.equal(await askedByGateway("PATCH", `${leases}/binding-of-harness`, { spec: { holderIdentity: "harness-codex-1" } }), "binding-of-harness");
  for (const [what, method, path, body] of [
    ["a Lease of its own", "POST", leases, lease("of-its-own", {})],
    ["a Lease of another pool's", "POST", leases, lease("of-another", { [POOL_LABEL]: "other" })],
    ["KEDA's leader's, changed", "PATCH", `${leases}/operator.keda.sh`, { spec: { holderIdentity: "gateway" } }],
    ["KEDA's leader's, deleted", "DELETE", `${leases}/operator.keda.sh`, undefined],
    ["a binding's, its pool taken off", "PATCH", `${leases}/binding-of-harness`, { metadata: { labels: { [POOL_LABEL]: null } } }],
  ] as const) {
    const answer = await askedByGateway(method, path, body);
    assert.ok(typeof answer !== "string" && answer.refused.includes("alasio-harness-pods"), `${what}: ${JSON.stringify(answer)}`);
  }
  assert.equal(await askedByGateway("DELETE", `${leases}/binding-of-harness`, undefined), "binding-of-harness");
});

test("a pod of no harness pool is left to what else holds the namespace", async () => {
  const answer = await made_({ apiVersion: "v1", kind: "Pod", metadata: { name: "other", namespace: "alasio" }, spec: { containers: [{ name: "other", image: "busybox" }] } });
  assert.equal(answer, "other");
});
