/**
 * The folder pods' policy (cli/src/manifests/admission.ts folderPodPolicy), as a real API
 * server holds pods to it: a folder instance's pod, as the gateway makes it of the folder
 * template (src/gateway/folders.ts), is admitted, made by the gateway; the same pod with any
 * field it pins changed, or made by anyone else, is refused; no other pod is made in the folder
 * namespace; a pod's spec and labels never change, nor its annotations but what keeps its
 * node; no pod gets an ephemeral container. And the folder Secrets' policy: the gateway makes
 * no Secret there but a folder instance's token.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { request } from "node:https";
import { before, test } from "node:test";

import type { V1Pod, V1PodSpec, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, Layer, Result, Schema } from "effect";

import { carriedFrom, decodeInstallConfig, manifests, NOTHING_KEPT } from "../cli/src/manifests/index.ts";
import { GATEWAY_USER } from "../cli/src/manifests/sandbox-gateway.ts";
import { boundAnnotations, boundLabels } from "../src/binder/names.ts";
import { FOLDER_POOL, folderKey, folderPodName, folderPodSpec, folderStateDir, folderTokenSecret, GatewayConfig } from "../src/gateway/names.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>) => run(Effect.flatMap(KubeClient, use));

const STATE_ROOT = "/home/operator/.alasio/bayma";
const decoded = decodeInstallConfig({
  alasio: { telegram: { existingSecret: "telegram" } },
  host: { enabled: true, stateRoot: STATE_ROOT, mounts: [{ name: "home", hostPath: "/home/operator", mountPath: "/home/operator" }] },
});
if (Result.isFailure(decoded)) throw new Error("the configuration does not decode");
const objects = manifests(decoded.success, carriedFrom(decoded.success, NOTHING_KEPT));
const gatewayConfig = Schema.decodeUnknownSync(Schema.fromJsonString(GatewayConfig))(
  (objects.find(({ kind, metadata }) => kind === "ConfigMap" && metadata?.name === "alasio-sandbox-gateway") as { data: Record<string, string> }).data["config.json"],
);
assert.ok(gatewayConfig.folders, "the gateway's configuration carries no folder instances");
const FOLDERS = gatewayConfig.folders.namespace;
const template = gatewayConfig.folders.template as V1PodTemplateSpec;

/** A folder instance's pod of `conversation`, as the gateway makes it of the folder template, changed as `change` says. */
let made = 0;
function folderPod(conversation = "telegram:42", change: (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void = () => {}): V1Pod {
  const key = folderKey("claude", conversation);
  const name = folderPodName(key, String((made += 1)).padStart(5, "0"));
  const pool = { name: FOLDER_POOL, namespace: FOLDERS };
  const pod = structuredClone({
    apiVersion: "v1",
    kind: "Pod",
    metadata: { name, namespace: FOLDERS, labels: { ...template.metadata?.labels, ...boundLabels(pool, key) }, annotations: { ...template.metadata?.annotations, ...boundAnnotations(key) } },
    spec: folderPodSpec(template.spec as V1PodSpec, name, folderStateDir(STATE_ROOT, "claude", conversation)),
  });
  change(pod);
  return pod as V1Pod;
}

let api: { readonly server: string; readonly token: string; readonly ca: Buffer };

/** Whether the API server takes `body` at `path` by `method`, asked by `user`, as the administrator impersonates it: its name, or why not. */
const askedAs = (user: string, method: string, path: string, type: string, body: unknown): Promise<{ readonly name: string } | { readonly refused: string }> =>
  new Promise((resolve, reject) => {
    const asked = request(new URL(`${api.server}${path}`), { method, ca: api.ca, headers: { authorization: `Bearer ${api.token}`, "content-type": type, "impersonate-user": user, "impersonate-group": "system:authenticated" } }, (response) => {
      let text = "";
      response.on("data", (chunk: Buffer) => (text += chunk.toString()));
      response.on("end", () => {
        const answered = JSON.parse(text) as { metadata?: { name?: string }; message?: string };
        resolve(response.statusCode !== undefined && response.statusCode < 300 ? { name: answered.metadata?.name ?? "" } : { refused: answered.message ?? text });
      });
    });
    asked.on("error", reject);
    asked.end(JSON.stringify(body));
  });

const madeAs = (user: string, pod: V1Pod) => askedAs(user, "POST", `/api/v1/namespaces/${FOLDERS}/pods`, "application/json", pod);
const refusedByPolicy = (answer: { readonly name: string } | { readonly refused: string }, policy = "alasio-folder-pods") => "refused" in answer && answer.refused.includes(policy);

before(async () => {
  const started = await startKubeApiServer();
  process.env["KUBECONFIG"] = started.kubeconfig;
  const config = JSON.parse(readFileSync(started.kubeconfig, "utf8")) as { clusters: { cluster: { "certificate-authority-data": string } }[] };
  api = { server: started.server, token: started.token, ca: Buffer.from(config.clusters[0]?.cluster["certificate-authority-data"] ?? "", "base64") };
  await withKube((kube) =>
    Effect.gen(function*() {
      for (const object of objects.filter(({ kind }) => kind === "PriorityClass")) yield* kube.create(object);
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: FOLDERS } });
      // A runtime there is, which a folder instance's pod may not run under.
      yield* kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" });
      yield* kube.create({ apiVersion: "v1", kind: "ServiceAccount", metadata: { name: "alasio-host-agent", namespace: FOLDERS } });
      for (const object of objects.filter(({ metadata }) => metadata?.name === "alasio-folder-pods" || metadata?.name === "alasio-folder-secrets")) yield* kube.create(object);
      // What the gateway may do in the folder namespace, as alasio up gives it; and alasio, which may make pods there too, for its policy to refuse.
      for (const object of objects.filter(({ kind, metadata }) => (kind === "Role" || kind === "RoleBinding") && metadata?.namespace === FOLDERS && metadata.name === "alasio-sandbox-gateway")) yield* kube.create(object);
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", metadata: { name: "pod-maker", namespace: FOLDERS }, rules: [{ apiGroups: [""], resources: ["pods", "pods/ephemeralcontainers", "secrets"], verbs: ["create", "patch"] }] } as never);
      yield* kube.create({ apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", metadata: { name: "pod-maker", namespace: FOLDERS }, roleRef: { apiGroup: "rbac.authorization.k8s.io", kind: "Role", name: "pod-maker" }, subjects: [{ kind: "User", name: "system:serviceaccount:alasio:alasio" }] } as never);
    })
  );
  // The policy in force, as the API server takes a while to: a pod it refuses refused.
  for (const deadline = Date.now() + 30_000; ; await new Promise((resolve) => setTimeout(resolve, 200))) {
    const tried = await madeAs(GATEWAY_USER, folderPod("telegram:warm-up", (pod) => void (pod.spec.hostNetwork = true)));
    if ("refused" in tried) break;
    await withKube((kube) => kube.remove("v1", "Pod", FOLDERS, tried.name));
    assert.ok(Date.now() < deadline, "the policy never came into force");
  }
  // And the Secrets' policy: a Secret it refuses refused.
  for (let tries = 0, deadline = Date.now() + 30_000; ; tries += 1, await new Promise((resolve) => setTimeout(resolve, 200))) {
    const tried = await askedAs(GATEWAY_USER, "POST", `/api/v1/namespaces/${FOLDERS}/secrets`, "application/json", { apiVersion: "v1", kind: "Secret", metadata: { name: `warm-up-${tries}`, namespace: FOLDERS }, type: "Opaque", stringData: { token: "t" } });
    if ("refused" in tried) break;
    await withKube((kube) => kube.remove("v1", "Secret", FOLDERS, tried.name));
    assert.ok(Date.now() < deadline, "the Secrets' policy never came into force");
  }
});

test("a folder instance's pod, as the gateway makes it of the folder template, is made by the gateway, and by no one else", async () => {
  const pod = await madeAs(GATEWAY_USER, folderPod());
  assert.ok("name" in pod, JSON.stringify(pod));
  assert.ok(refusedByPolicy(await madeAs("system:serviceaccount:alasio:alasio", folderPod())));
});

test("a folder instance's pod with any field the template pins changed, or widened, is refused", async () => {
  const bayma = (pod: { spec: V1PodSpec }) => pod.spec.containers[0]!;
  const changes: Record<string, (pod: { metadata: Record<string, unknown>; spec: V1PodSpec }) => void> = {
    "another image": (pod) => void (bayma(pod).image = "busybox"),
    "a capability more": (pod) => void (bayma(pod).securityContext!.capabilities!.add = [...(bayma(pod).securityContext?.capabilities?.add ?? []), "SYS_ADMIN"]),
    "privileged": (pod) => void (bayma(pod).securityContext!.privileged = true),
    "another of the machine's paths": (pod) => void pod.spec.volumes!.push({ name: "root", hostPath: { path: "/" } }),
    "the machine's network": (pod) => void (pod.spec.hostNetwork = true),
    "another user": (pod) => void (pod.spec.securityContext!.runAsUser = 0),
    "an argument more": (pod) => void bayma(pod).args!.push("--unsafe"),
    "a state directory outside the root": (pod) => void (bayma(pod).args![bayma(pod).args!.length - 1] = "/etc/elsewhere/claude/x"),
    "another pod's token": (pod) => void (pod.spec.volumes!.find(({ name }) => name === "alasio-bayma-token")!.secret!.secretName = "bayma-0000000000000000-00000-bayma-token"),
    "a short stop": (pod) => void (pod.spec.terminationGracePeriodSeconds = 30),
    "started again": (pod) => void (pod.spec.restartPolicy = "Always"),
    "a runtime class": (pod) => void (pod.spec.runtimeClassName = "gvisor"),
    "an init container": (pod) => void (pod.spec.initContainers = [{ name: "first", image: "busybox" }]),
    "bound to nothing": (pod) => void delete (pod.metadata["labels"] as Record<string, string>)["alasio.dev/binding"],
    "of no folder's key": (pod) => void ((pod.metadata["annotations"] as Record<string, string>)["alasio.dev/binding-key"] = "sessionfs:fs-abc123"),
    "named as no folder instance": (pod) => void (pod.metadata["name"] = "bayma-elsewhere"),
  };
  for (const [what, change] of Object.entries(changes)) {
    assert.ok(refusedByPolicy(await madeAs(GATEWAY_USER, folderPod("telegram:widened", change))), what);
  }
  // No other pod there, by the gateway either.
  const other = await madeAs(GATEWAY_USER, { apiVersion: "v1", kind: "Pod", metadata: { name: "other", namespace: FOLDERS }, spec: { containers: [{ name: "other", image: "busybox" }] } });
  assert.ok(refusedByPolicy(other), JSON.stringify(other));
});

test("a folder instance's pod's spec and labels never change, nor its annotations but what keeps its node, and it gets no ephemeral container", async () => {
  const pod = folderPod("telegram:changed");
  const made = await madeAs(GATEWAY_USER, pod);
  assert.ok("name" in made, JSON.stringify(made));
  // By anyone who may patch a pod there, as the policy holds every one.
  const patched = (patch: unknown) => askedAs("system:serviceaccount:alasio:alasio", "PATCH", `/api/v1/namespaces/${FOLDERS}/pods/${made.name}`, "application/merge-patch+json", patch);
  assert.ok("name" in await patched({ metadata: { annotations: { "karpenter.sh/do-not-disrupt": "false" } } }));
  assert.ok(refusedByPolicy(await patched({ metadata: { annotations: { "alasio.dev/binding-key": "folder:claude:telegram:other" } } })));
  assert.ok(refusedByPolicy(await patched({ metadata: { labels: { "alasio.dev/binding": "1".repeat(40) } } })));
  assert.ok(refusedByPolicy(await patched({ spec: { activeDeadlineSeconds: 10 } })));
  const ephemeral = await askedAs("system:serviceaccount:alasio:alasio", "PATCH", `/api/v1/namespaces/${FOLDERS}/pods/${made.name}/ephemeralcontainers`, "application/strategic-merge-patch+json", {
    spec: { ephemeralContainers: [{ name: "debug", image: "busybox" }] },
  });
  assert.ok(refusedByPolicy(ephemeral), JSON.stringify(ephemeral));
});

test("the gateway makes no Secret in the folder namespace but a folder instance's token, of its one key, named for its pod", async () => {
  const secret = (name: string, data: Record<string, string>) => ({ apiVersion: "v1", kind: "Secret", metadata: { name, namespace: FOLDERS }, type: "Opaque", stringData: data });
  const asked = (body: unknown) => askedAs(GATEWAY_USER, "POST", `/api/v1/namespaces/${FOLDERS}/secrets`, "application/json", body);
  const name = folderTokenSecret(folderPodName(folderKey("claude", "telegram:42"), "secrt"));
  assert.ok("name" in await asked(secret(name, { token: "t" })));
  assert.ok(refusedByPolicy(await asked(secret("anything", { token: "t" })), "alasio-folder-secrets"));
  assert.ok(refusedByPolicy(await asked(secret(folderTokenSecret(folderPodName(folderKey("claude", "telegram:43"), "secrt")), { token: "t", more: "x" })), "alasio-folder-secrets"));
});
