import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Pod, V1Secret } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { readLease } from "../src/binder/bindings.ts";
import { BINDING_LABEL, keyHash } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import { makeFolders } from "../src/gateway/folders.ts";
import { FOLDER_POOL, FOLDER_STOP_SECONDS, folderKey, folderTokenSecret } from "../src/gateway/names.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The gateway's folder instances on a real API server: each bound to a pod the gateway makes
// of the folder template, at the loopback's address, where a stand-in for bayma answers
// /status as the test says.

const bindingKey = randomBytes(32);
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);
const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

/** A folder instance's bayma, as the folder template runs it. */
const TEMPLATE = {
  metadata: { labels: { "app.kubernetes.io/component": "folder-bayma" } },
  spec: {
    containers: [{ name: "bayma", image: "bayma", args: ["mcp-http", "--host", "0.0.0.0", "--port", "7290", "--default-durability", "checkpointed"], ports: [{ name: "mcp", containerPort: 7290 }] }],
    volumes: [{ name: "home", hostPath: { path: "/home/operator" } }],
  },
};

/** A stand-in for bayma's /status, answering as the test says. */
async function bayma(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null }; close: () => void }> {
  const stand = { status: { execsRunning: 1, execsQueued: 0, idleSinceMs: null as number | null } };
  const server = createServer((_incoming, outgoing) => void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status)));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, close: () => server.close() };
}

let namespaces = 0;

/** The gateway's folder instances in a namespace of the test's own, their bayma the stand-in's, idle ones kept `releaseAfterMs`. */
async function foldersOf(port: number, releaseAfterMs = 60_000) {
  const namespace = `folders-${(namespaces += 1)}`;
  await run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } })));
  return Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const binder = yield* Binder;
    const folders = yield* makeFolders({ namespace, template: TEMPLATE, stateRoot: "/home/operator/.alasio/bayma", releaseAfterMs, main: "main", podPort: port, bindingKey, idlePollMs: 50 }, binder);
    return { namespace, binder, folders };
  }), Layer.provideMerge(Binder.layer({ waitMs: 10_000 }), Layer.fresh(KubeClient.layer))));
}

/** Binds `key` as the gateway's front does, its pod run, as kubelet would run it, and ready. */
async function bound(of: Awaited<ReturnType<typeof foldersOf>>, key: string): Promise<V1Pod> {
  const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
  const binding = Effect.runPromise(of.binder.bind(placement.pool, key, placement));
  for (;;) {
    const [pod] = (await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)))) as V1Pod[];
    if (pod?.metadata?.name) {
      await run(withKube((kube) =>
        kube.patchPodStatus(of.namespace, pod.metadata?.name ?? "", {
          status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "bayma", imageID: "", ready: true, restartCount: 0, containerID: "containerd://c-0", state: { running: {} } }] },
        })
      ));
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  const done = await binding;
  return (await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, done.pod)))) as V1Pod;
}

test("a folder instance is main's alone, placed in the folder pool, of a folder's key alone", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const placed = await Effect.runPromise(of.folders.place(folderKey("claude", "telegram:42"), { environment: "main" }));
    assert.deepEqual(placed.pool, { name: FOLDER_POOL, namespace: of.namespace });
    const refusal = (key: string, environment: string) => Effect.runPromise(Effect.flip(of.folders.place(key, { environment }))).then(({ message }) => message);
    assert.equal(await refusal(folderKey("claude", "telegram:42"), "try"), "a folder is the machine's own files, of main's alone, not try's");
    assert.equal(await refusal("sessionfs:fs-abc123", "main"), "sessionfs:fs-abc123 is no folder instance's");
  } finally {
    stand.close();
  }
});

test("a folder instance is a pod the gateway makes at its bind, of the template, on the conversation's state directory, its binding's token in a Secret it owns, stopped within its time and never started again", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const key = folderKey("codex", "telegram:42");
    const pod = await bound(of, key);
    const name = pod.metadata?.name ?? "";
    assert.match(name, /^bayma-[0-9a-f]{16}-[0-9a-z]{5}$/u);
    assert.equal(pod.metadata?.labels?.["app.kubernetes.io/component"], "folder-bayma");
    assert.deepEqual(pod.spec?.containers[0]?.args?.slice(-4), ["--token-file", "/run/alasio/bayma/token", "--state-dir", "/home/operator/.alasio/bayma/codex/telegram-42"]);
    assert.equal(pod.spec?.terminationGracePeriodSeconds, FOLDER_STOP_SECONDS);
    assert.equal(pod.spec?.restartPolicy, "Never");
    assert.deepEqual(JSON.parse(JSON.stringify(pod.spec?.volumes?.at(-1))), { name: "alasio-bayma-token", secret: { secretName: folderTokenSecret(name), defaultMode: 0o440 } });
    const secret = (await run(withKube((kube) => kube.read("v1", "Secret", of.namespace, folderTokenSecret(name))))) as V1Secret;
    const token = Buffer.from(secret.data?.["token"] ?? "", "base64").toString("utf8");
    const lease = await run(readLease({ name: FOLDER_POOL, namespace: of.namespace }, key));
    assert.deepEqual(verifyToken(bindingKey, token, Date.now()), { key, binding: lease?.metadata?.uid, namespace: of.namespace });
    assert.deepEqual(secret.metadata?.ownerReferences?.map(({ kind, name: owner, uid }) => ({ kind, owner, uid })), [{ kind: "Pod", owner: name, uid: pod.metadata?.uid }]);
    // Bound again: the same pod.
    assert.equal((await Effect.runPromise(of.binder.bound({ name: FOLDER_POOL, namespace: of.namespace }, key)))?.pod, name);
  } finally {
    stand.close();
  }
});

test("a folder instance idle for its time is released, its pod gone before its Lease, and one whose bayma ended released as broken", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 200);
    const pool = { name: FOLDER_POOL, namespace: of.namespace };
    const key = folderKey("claude", "telegram:idle");
    const pod = await bound(of, key);
    // Busy: kept.
    assert.deepEqual(await Effect.runPromise(Effect.provide(of.folders.sweep, Layer.fresh(KubeClient.layer))), []);
    // Idle past its time: released, its pod gone, and its Lease.
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 };
    let released: readonly string[] = [];
    for (const deadline = Date.now() + 10_000; released.length === 0 && Date.now() < deadline; await new Promise((resolve) => setTimeout(resolve, 100))) {
      released = await Effect.runPromise(Effect.provide(of.folders.sweep, Layer.fresh(KubeClient.layer)));
    }
    assert.deepEqual(released, [key]);
    assert.equal(await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, pod.metadata?.name ?? ""))), null);
    assert.equal(await run(readLease(pool, key)), null);
    // One whose bayma ended: released, the next bind making another.
    stand.status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null };
    const broken = await bound(of, folderKey("claude", "telegram:broken"));
    await run(withKube((kube) => kube.patchPodStatus(of.namespace, broken.metadata?.name ?? "", {
      status: { containerStatuses: [{ name: "bayma", image: "bayma", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 1 } } }] },
    })));
    await Effect.runPromise(Effect.provide(of.folders.live, Layer.fresh(KubeClient.layer)));
    for (const deadline = Date.now() + 10_000; ; await new Promise((resolve) => setTimeout(resolve, 100))) {
      if ((await run(readLease(pool, folderKey("claude", "telegram:broken")))) === null) break;
      assert.ok(Date.now() < deadline, "the broken folder instance was not released");
    }
    assert.equal(await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, broken.metadata?.name ?? ""))), null);
  } finally {
    stand.close();
  }
});
