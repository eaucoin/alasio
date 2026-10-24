import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Lease, V1Pod, V1Secret } from "@kubernetes/client-node";
import { Effect, Layer, Scope } from "effect";

import { added } from "./support/metrics.ts";
import { Binder } from "../src/binder/binder.ts";
import { readLease } from "../src/binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, keyHash } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import { makeFolders, type MemoryPeaks, memoryRequestOf, SIZING, sizingLease } from "../src/gateway/folders.ts";
import { RELEASES } from "../src/gateway/releases.ts";
import { FOLDER_POOL, FOLDER_STOP_SECONDS, folderKey, folderTokenSecret } from "../src/gateway/names.ts";
import { KubeApiError, KubeClient } from "../src/kube/client.ts";
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

/** The folder template with a memory limit, as alasio up renders one. */
const LIMITED = { ...TEMPLATE, spec: { ...TEMPLATE.spec, containers: TEMPLATE.spec.containers.map((container) => ({ ...container, resources: { limits: { memory: "4Gi" } } })) } };

/** A stand-in for bayma's /status, answering as the test says. */
async function bayma(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null }; close: () => void }> {
  const stand = { status: { execsRunning: 1, execsQueued: 0, idleSinceMs: null as number | null } };
  const server = createServer((_incoming, outgoing) => void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status)));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, close: () => server.close() };
}

let namespaces = 0;

/**
 * The gateway's folder instances in a namespace of the test's own, their bayma the stand-in's, idle ones kept `releaseAfterMs`;
 * of `template`, where given, and what the metrics server says each pod's bayma uses, `used`, where given: it answers nothing otherwise, as an API server alone does.
 */
async function foldersOf(
  port: number,
  releaseAfterMs = 60_000,
  startMs = 10_000,
  pullMs = 3_000,
  { refusingSecretPatches = false, binderWaitMs = 10_000, template = TEMPLATE, used, sizingIntervalMs, metricsHang = false }: { refusingSecretPatches?: boolean; binderWaitMs?: number; template?: typeof TEMPLATE; used?: Map<string, string>; sizingIntervalMs?: number; metricsHang?: boolean } = {},
) {
  const namespace = `folders-${(namespaces += 1)}`;
  const kubeLayer = Layer.effect(KubeClient, Effect.map(KubeClient, (kube) =>
    KubeClient.of({
      ...kube,
      patch: (apiVersion, kind, ...rest) => (refusingSecretPatches && kind === "Secret" ? Effect.fail(new KubeApiError({ status: 500, cause: "refused" })) : kube.patch(apiVersion, kind, ...rest)),
      list: (apiVersion, kind, ...rest) =>
        kind !== "PodMetrics" ? kube.list(apiVersion, kind, ...rest)
        : metricsHang ? Effect.never
        : used ? Effect.sync(() => [...used].map(([name, memory]) => ({ metadata: { name }, containers: [{ name: "bayma", usage: { memory } }] })))
        : Effect.fail(new KubeApiError({ status: 404, cause: "the metrics server does not answer" })),
    })
  )).pipe(Layer.provide(Layer.fresh(KubeClient.layer)));
  await run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } })));
  return Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const binder = yield* Binder;
    const folders = yield* makeFolders({ namespace, template, stateRoot: "/home/operator/.alasio/bayma", releaseAfterMs, main: "main", podPort: port, bindingKey, idlePollMs: 50, startMs, pullMs, metricsTimeoutMs: 500, ...(sizingIntervalMs ? { sizingIntervalMs } : {}) }, binder);
    return { namespace, binder, folders };
  }).pipe(Scope.provide(Scope.makeUnsafe())), Layer.provideMerge(Binder.layer({ waitMs: binderWaitMs }), kubeLayer)));
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

test("a folder instance's bind makes its pod only once the pod of it before is gone, as that one's bayma still dumps its REPL sessions", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const key = folderKey("claude", "telegram:43");
    const labels = { "alasio.dev/pool": FOLDER_POOL, [BINDING_LABEL]: keyHash(key) };
    const pods = async () => ((await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)))) as V1Pod[]).map((pod) => pod.metadata?.name);
    // The pod before, being deleted, held as its kubelet holds it while its bayma stops.
    await run(withKube((kube) =>
      kube.create({ apiVersion: "v1", kind: "Pod", metadata: { name: "bayma-before", namespace: of.namespace, labels, annotations: { [KEY_ANNOTATION]: key }, finalizers: ["alasio.dev/test"] }, spec: { containers: [{ name: "bayma", image: "bayma" }] } })
    ));
    await run(withKube((kube) => kube.remove("v1", "Pod", of.namespace, "bayma-before")));
    const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    const binding = Effect.runPromise(Effect.result(of.binder.bind(placement.pool, key, placement)));
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    assert.deepEqual(await pods(), ["bayma-before"]);
    await run(withKube((kube) => kube.patch("v1", "Pod", of.namespace, "bayma-before", { metadata: { finalizers: null } })));
    for (let tries = 0; (await pods()).length !== 1 || (await pods())[0] === "bayma-before"; tries += 1) {
      assert.ok(tries < 100, `no pod made after the one before was gone: ${(await pods()).join(", ")}`);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const [made = ""] = await pods();
    await run(withKube((kube) =>
      kube.patchPodStatus(of.namespace, made, {
        status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "bayma", imageID: "", ready: true, restartCount: 0, containerID: "containerd://c-0", state: { running: {} } }] },
      })
    ));
    const done = await binding;
    assert.equal(done._tag === "Success" ? done.success.pod : String(done.failure), made);
  } finally {
    stand.close();
  }
});

test("a folder instance's pod whose token's Secret it cannot own is deleted with it, as nothing would delete the Secret after", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 60_000, 10_000, 3_000, { refusingSecretPatches: true });
    const key = folderKey("claude", "telegram:44");
    const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    const failed = await Effect.runPromise(Effect.flip(of.binder.bind(placement.pool, key, placement)));
    assert.equal(failed._tag, "KubeApiError");
    const left = async (kind: string) => ((await run(withKube((kube) => kube.list("v1", kind, of.namespace, "")))) as V1Pod[]).map(({ metadata }) => metadata?.name);
    assert.deepEqual([await left("Pod"), await left("Secret")], [[], []]);
  } finally {
    stand.close();
  }
});

test("a folder instance idle for its time is released, its pod gone before its Lease, one whose bayma ended released as broken, or as oom where it was killed for its memory, and one its caller lets go, each release counted", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 200, 10_000, 3_000, { template: LIMITED });
    const pool = { name: FOLDER_POOL, namespace: of.namespace };
    const key = folderKey("claude", "telegram:idle");
    const pod = await bound(of, key);
    // Busy: kept.
    assert.deepEqual(await Effect.runPromise(Effect.provide(of.folders.sweep, Layer.fresh(KubeClient.layer))), []);
    const counted = await added(RELEASES, async () => {
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
      const killed = await bound(of, folderKey("claude", "telegram:killed"));
      for (const [pod, terminated] of [[broken, { exitCode: 1 }], [killed, { exitCode: 137, reason: "OOMKilled" }]] as const) {
        await run(withKube((kube) => kube.patchPodStatus(of.namespace, pod.metadata?.name ?? "", {
          status: { containerStatuses: [{ name: "bayma", image: "bayma", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated } }] },
        })));
      }
      await Effect.runPromise(Effect.provide(of.folders.live, Layer.fresh(KubeClient.layer)));
      for (const conversation of ["telegram:broken", "telegram:killed"]) {
        for (const deadline = Date.now() + 10_000; ; await new Promise((resolve) => setTimeout(resolve, 100))) {
          if ((await run(readLease(pool, folderKey("claude", conversation)))) === null) break;
          assert.ok(Date.now() < deadline, `the folder instance of ${conversation} was not released`);
        }
      }
      for (const pod of [broken, killed]) assert.equal(await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, pod.metadata?.name ?? ""))), null);
      // One its caller lets go, as a restart does.
      const asked = await bound(of, folderKey("claude", "telegram:asked"));
      const placement = await Effect.runPromise(of.folders.place(folderKey("claude", "telegram:asked"), { environment: "main" }));
      assert.equal(await Effect.runPromise(of.binder.release(pool, folderKey("claude", "telegram:asked"), placement.release)), "released");
      assert.equal(await run(withKube((kube) => kube.read("v1", "Pod", of.namespace, asked.metadata?.name ?? ""))), null);
    });
    // Each counted, by why, as a session workspace's release is.
    const reason = (why: string) => `alasio.binder.pool=${FOLDER_POOL},k8s.namespace.name=${of.namespace},alasio.gateway.release.reason=${why}`;
    assert.deepEqual(counted, { [reason("idle")]: 1, [reason("broken")]: 1, [reason("oom")]: 1, [reason("asked")]: 1 });
  } finally {
    stand.close();
  }
});

/** Binds `key` as the front does, its pod given `status` as kubelet would, or none: how the bind ended. */
async function bindAs(of: Awaited<ReturnType<typeof foldersOf>>, key: string, status: object | null, options: Readonly<Record<string, string>> = {}) {
  const placement = await Effect.runPromise(of.folders.place(key, { environment: "main", ...options }));
  const binding = Effect.runPromise(Effect.result(of.binder.bind(placement.pool, key, placement)));
  if (status) {
    for (;;) {
      const [pod] = (await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)))) as V1Pod[];
      if (pod?.metadata?.name) {
        await run(withKube((kube) => kube.patchPodStatus(of.namespace, pod.metadata?.name ?? "", { status })));
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }
  return { placement, result: await binding };
}

test("binds of a folder instance failing at once, as on two replicas, are each counted", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const key = folderKey("codex", "telegram:47");
    const placed = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    await Promise.all([1, 2, 3].map((n) => Effect.runPromise(placed.settled?.(`its pod is pending: no node (${n})`) ?? Effect.void)));
    const held = await Effect.runPromise((await Effect.runPromise(of.folders.place(key, { environment: "main" }))).held ?? Effect.succeed(null));
    assert.match(held ?? "", /^its last 3 binds failed/u);
  } finally {
    stand.close();
  }
});

test("what a folder instance's bind under way waits on is answered of the key a token of main's names, as its pods say", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const asked = async (key: string, environment = "main") => {
      const answered = await Effect.runPromise(Effect.provide(of.folders.operate({ key, expires: Date.now() + 60_000, options: { environment } }, { method: "GET", path: "/folders/starting", query: new URLSearchParams(), body: new Uint8Array() }), Layer.fresh(KubeClient.layer)));
      return answered.status === 200 ? (answered.json as { starting: string | null }).starting : answered.status;
    };
    const podOf = async (key: string, name: string, status: object | null, deleting = false) => {
      const labels = { "alasio.dev/pool": FOLDER_POOL, [BINDING_LABEL]: keyHash(key) };
      await run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels, annotations: { [KEY_ANNOTATION]: key }, ...(deleting ? { finalizers: ["alasio.dev/test"] } : {}) }, spec: { containers: [{ name: "bayma", image: "bayma" }] } })));
      if (status) await run(withKube((kube) => kube.patchPodStatus(of.namespace, name, { status })));
      if (deleting) await run(withKube((kube) => kube.remove("v1", "Pod", of.namespace, name)));
    };
    const container = (ready: boolean, state: object) => ({ name: "bayma", image: "bayma", imageID: "", ready, restartCount: 0, containerID: "containerd://c-0", state });
    const key = (name: string) => folderKey("claude", `telegram:${name}`);
    await podOf(key("saving"), "bayma-saving", null, true);
    await podOf(key("pulling"), "bayma-pulling", { phase: "Pending", conditions: [{ type: "PodScheduled", status: "True" }], containerStatuses: [container(false, { waiting: { reason: "ContainerCreating" } })] });
    await podOf(key("pending"), "bayma-pending", { phase: "Pending", conditions: [{ type: "PodScheduled", status: "False", reason: "Unschedulable", message: "0/1 nodes are available" }] });
    await podOf(key("starting"), "bayma-starting", { phase: "Running", conditions: [{ type: "PodScheduled", status: "True" }], containerStatuses: [container(false, { running: {} })] });
    await podOf(key("serving"), "bayma-serving", { phase: "Running", conditions: [{ type: "Ready", status: "True" }], containerStatuses: [container(true, { running: {} })] });
    assert.deepEqual(
      [await asked(key("saving")), await asked(key("pulling")), await asked(key("pending")), await asked(key("starting")), await asked(key("serving")), await asked(key("none"))],
      ["whose instance before is saving its REPL sessions", "whose image is being pulled", "whose pod is pending: 0/1 nodes are available", "whose bayma is starting", null, null],
    );
    // Main's alone, of a folder's key alone.
    assert.deepEqual([await asked(key("pulling"), "try"), await asked("sessionfs:fs-1")], [403, 404]);
    await run(withKube((kube) => kube.patch("v1", "Pod", of.namespace, "bayma-saving", { metadata: { finalizers: null } })));
  } finally {
    stand.close();
  }
});

test("a held folder instance's restart, its caller's release, forgets its binds' failures, bound or not, so its next bind goes on", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const key = folderKey("claude", "telegram:8");
    const placed = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    for (let n = 0; n < 3; n += 1) await Effect.runPromise(placed.settled?.("its pod is pending: no node") ?? Effect.void);
    const heldNow = async () => Effect.runPromise((await Effect.runPromise(of.folders.place(key, { environment: "main" }))).held ?? Effect.succeed(null));
    assert.notEqual(await heldNow(), null);
    await Effect.runPromise(Effect.provide(placed.reset ?? Effect.void, Layer.fresh(KubeClient.layer)));
    assert.equal(await heldNow(), null);
    // Of none to forget, as it is reset again, none is refused.
    await Effect.runPromise(Effect.provide(placed.reset ?? Effect.void, Layer.fresh(KubeClient.layer)));
  } finally {
    stand.close();
  }
});

test("a folder instance's bind gives its pod its time to run and its bayma to answer, or deletes the pod and its Lease, saying why", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 60_000, 1_500, 3_000, { template: LIMITED });
    const pool = { name: FOLDER_POOL, namespace: of.namespace };
    const container = (state: object) => ({ name: "bayma", image: "bayma", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state });
    const failures: [string, object | null, RegExp][] = [
      ["unscheduled", { phase: "Pending", conditions: [{ type: "PodScheduled", status: "False", reason: "Unschedulable", message: "0/1 nodes are available: 1 Insufficient memory." }] }, /did not serve within 1\.5 s: its pod is pending: 0\/1 nodes are available: 1 Insufficient memory\.$/u],
      ["unpulled", { phase: "Pending", containerStatuses: [container({ waiting: { reason: "ImagePullBackOff", message: "Back-off pulling image \"bayma\"" } })] }, /did not serve within 1\.5 s: its bayma did not start: ImagePullBackOff \(Back-off pulling image "bayma"\)$/u],
      ["ended", { phase: "Running", podIP: "127.0.0.1", containerStatuses: [container({ terminated: { exitCode: 2, reason: "Error" } })] }, /^its bayma ended \(Error\)$/u],
      ["killed", { phase: "Running", podIP: "127.0.0.1", containerStatuses: [container({ terminated: { exitCode: 137, reason: "OOMKilled" } })] }, /^its bayma ran out of its memory \(4Gi\)$/u],
      ["unanswered", { phase: "Running", podIP: "127.0.0.2", podIPs: [{ ip: "127.0.0.2" }], containerStatuses: [container({ running: {} })] }, /^its bayma did not answer within 1\.5 s of its start: /u],
      // Scheduled, its image pulled the longer while, then given up on.
      ["pulling", { phase: "Pending", conditions: [{ type: "PodScheduled", status: "True" }], containerStatuses: [container({ waiting: { reason: "ContainerCreating" } })] }, /did not serve within 3 s: its image is still being pulled$/u],
    ];
    for (const [conversation, status, reason] of failures) {
      const key = folderKey("claude", `telegram:${conversation}`);
      const { result } = await bindAs(of, key, status);
      assert.equal(result._tag, "Failure", conversation);
      assert.match(result._tag === "Failure" ? result.failure.message : "", reason, conversation);
      assert.equal(await run(readLease(pool, key)), null, conversation);
      for (const deadline = Date.now() + 10_000; ; await new Promise((resolve) => setTimeout(resolve, 100))) {
        const left = await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)));
        if (left.every((pod) => (pod as V1Pod).metadata?.deletionTimestamp)) break;
        assert.ok(Date.now() < deadline, `the pod of ${conversation} was not deleted`);
      }
    }
  } finally {
    stand.close();
  }
});

test("a folder instance's bind whose image is pulled for longer than the gateway's binds wait goes through, and a bind of it meanwhile waits for it", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 60_000, 2_000, 6_000, { binderWaitMs: 1_000 });
    const key = folderKey("claude", "telegram:45");
    const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    const bind = () => Effect.runPromise(Effect.result(of.binder.bind(placement.pool, key, placement)));
    const first = bind();
    let made = "";
    while (!made) {
      [made = ""] = ((await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)))) as V1Pod[]).map((pod) => pod.metadata?.name ?? "");
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const container = (ready: boolean, state: object) => ({ name: "bayma", image: "bayma", imageID: "", ready, restartCount: 0, containerID: "containerd://c-0", state });
    await run(withKube((kube) => kube.patchPodStatus(of.namespace, made, { status: { phase: "Pending", conditions: [{ type: "PodScheduled", status: "True" }], containerStatuses: [container(false, { waiting: { reason: "ContainerCreating" } })] } })));
    const second = bind();
    // Pulled for three times as long as the gateway's binds wait.
    await new Promise((resolve) => setTimeout(resolve, 3_000));
    await run(withKube((kube) =>
      kube.patchPodStatus(of.namespace, made, { status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], conditions: [{ type: "Ready", status: "True" }], containerStatuses: [container(true, { running: {} })] } })
    ));
    const pods = (result: Awaited<ReturnType<typeof bind>>) => (result._tag === "Success" ? result.success.pod : String(result.failure));
    assert.deepEqual([pods(await first), pods(await second)], [made, made]);
  } finally {
    stand.close();
  }
});

test("a folder instance's bind while it is being released waits for its release, for longer than the gateway's binds wait, then makes its pod anew", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port, 60_000, 10_000, 3_000, { binderWaitMs: 1_000 });
    const key = folderKey("codex", "telegram:46");
    const before = (await bound(of, key)).metadata?.name ?? "";
    // Held as its kubelet holds it while its bayma stops.
    await run(withKube((kube) => kube.patch("v1", "Pod", of.namespace, before, { metadata: { finalizers: ["alasio.dev/test"] } })));
    const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    const releasing = Effect.runPromise(of.binder.release(placement.pool, key, placement.release));
    await new Promise((resolve) => setTimeout(resolve, 500));
    const again = Effect.runPromise(Effect.result(of.binder.bind(placement.pool, key, placement)));
    await new Promise((resolve) => setTimeout(resolve, 2_500));
    await run(withKube((kube) => kube.patch("v1", "Pod", of.namespace, before, { metadata: { finalizers: null } })));
    assert.equal(await releasing, "released");
    let made = "";
    for (let tries = 0; !made; tries += 1) {
      assert.ok(tries < 100, `no pod made after the release: ${String(await Promise.race([again, Promise.resolve("binding")]))}`);
      [made = ""] = ((await run(withKube((kube) => kube.list("v1", "Pod", of.namespace, `${BINDING_LABEL}=${keyHash(key)}`)))) as V1Pod[]).map((pod) => pod.metadata?.name ?? "").filter((name) => name !== before);
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    await run(withKube((kube) =>
      kube.patchPodStatus(of.namespace, made, {
        status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "bayma", imageID: "", ready: true, restartCount: 0, containerID: "containerd://c-1", state: { running: {} } }] },
      })
    ));
    const result = await again;
    assert.equal(result._tag === "Success" ? result.success.pod : String(result.failure), made);
  } finally {
    stand.close();
  }
});

test("three binds of a folder instance failing in a row hold it, bound again only as its caller asks, until one goes through; the instances are listed to main's callers, with their failures, and the held counted", async () => {
  const stand = await bayma();
  try {
    const of = await foldersOf(stand.port);
    const key = folderKey("codex", "telegram:7");
    const placed = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    const settle = (failure: string | null) => Effect.runPromise(placed.settled?.(failure) ?? Effect.void);
    const heldNow = async (options: Readonly<Record<string, string>> = {}) => Effect.runPromise((await Effect.runPromise(of.folders.place(key, { environment: "main", ...options }))).held ?? Effect.succeed(null));
    await settle("its pod is pending: no node");
    await settle("its pod is pending: no node");
    assert.equal(await heldNow(), null);
    await settle("its bayma did not answer: refused");
    assert.equal(await heldNow(), `its last 3 binds failed, the last as its bayma did not answer: refused`);
    // As its caller asks, it is bound.
    assert.equal(await heldNow({ retry: "1" }), null);
    const listed = await Effect.runPromise(Effect.provide(of.folders.list, Layer.fresh(KubeClient.layer)));
    assert.equal(listed.length, 1);
    assert.deepEqual({ ...listed[0], failing: { ...listed[0]?.failing, at: "" } }, {
      key, harness: "codex", conversation: "telegram:7", pod: null, boundAt: null, lastExecAt: null,
      failing: { reason: "its bayma did not answer: refused", at: "", times: 3, held: true },
    });
    await Effect.runPromise(Effect.provide(of.folders.live, Layer.fresh(KubeClient.layer)));
    assert.equal(of.folders.failing(), 1);
    // Main's callers alone, of GET /folders alone.
    const main = { key: "folder:*", expires: Date.now() + 60_000, options: { environment: "main" } };
    const asked = (claims: typeof main, method: string, path: string) => Effect.runPromise(Effect.provide(of.folders.operate(claims, { method, path, query: new URLSearchParams(), body: new Uint8Array() }), Layer.fresh(KubeClient.layer)));
    const answered = await asked(main, "GET", "/folders");
    assert.equal(answered.status, 200);
    assert.equal((answered.json as { folders: { key: string }[] }).folders[0]?.key, key);
    assert.equal((await asked({ ...main, options: { environment: "try" } }, "GET", "/folders")).status, 403);
    assert.equal((await asked(main, "POST", "/folders")).status, 404);
    // A bind that goes through forgets them.
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.parse("2026-10-18T12:00:00Z") };
    const pod = await bound(of, key);
    await settle(null);
    assert.equal(await heldNow(), null);
    const after = await Effect.runPromise(Effect.provide(of.folders.list, Layer.fresh(KubeClient.layer)));
    assert.deepEqual(after.map(({ pod: name, failing, lastExecAt }) => ({ name, failing, lastExecAt })), [{ name: pod.metadata?.name, failing: null, lastExecAt: "2026-10-18T12:00:00.000Z" }]);
    await Effect.runPromise(Effect.provide(of.folders.live, Layer.fresh(KubeClient.layer)));
    assert.equal(of.folders.failing(), 0);
  } finally {
    stand.close();
  }
});

test("a folder instance requests the 90th percentile of its folder's hours' memory peaks with a margin, older ones weighing less, within its template's request and limit", () => {
  const hour = SIZING.intervalMs;
  const MiB = 2 ** 20;
  const now = 1000 * hour;
  const request = (peaks: MemoryPeaks) => memoryRequestOf(peaks, now, hour, 512 * MiB, 8192 * MiB) / MiB;
  // None: its template's.
  assert.equal(request([]), 512);
  // Ten hours of 1 GiB and one of 4: the 90th percentile is 1 GiB, with its margin, in whole MiB.
  assert.equal(request([...Array.from({ length: 10 }, (_, n) => [now - n * hour, 1024 * MiB] as const), [now - hour, 4096 * MiB]]), Math.ceil(1024 * 1.15));
  // One big session a week ago, beside a day of small ones: it weighs too little to set it.
  assert.equal(request([[now - 7 * 24 * hour, 6144 * MiB], ...Array.from({ length: 3 }, (_, n) => [now - n * hour, 600 * MiB] as const)]), Math.ceil(600 * 1.15));
  // The same a day ago: it does.
  assert.equal(request([[now - 24 * hour, 6144 * MiB], ...Array.from({ length: 3 }, (_, n) => [now - n * hour, 600 * MiB] as const)]), Math.ceil(6144 * 1.15));
  // Past its eight days: none.
  assert.equal(request([[now - SIZING.intervals * hour, 6144 * MiB]]), 512);
  // Never under its template's request, nor over its limit, which it is in bytes where it is no whole MiB.
  assert.equal(request([[now, 100 * MiB]]), 512);
  assert.equal(request([[now, 16384 * MiB]]), 8192);
  assert.equal(memoryRequestOf([[now, 16384 * MiB]], now, hour, 512 * MiB, 2e9), 2e9);
  // And in whole bytes, where the limit is of none, as 0.1Gi is.
  assert.equal(memoryRequestOf([[now, 16384 * MiB]], now, hour, 16 * MiB, 0.1 * 2 ** 30), Math.floor(0.1 * 2 ** 30));
});

test("a folder instance's next pod requests what its folder's instances came to use, kept in a Lease of the folder's own, which no other folder's pod is sized by", async () => {
  const stand = await bayma();
  try {
    const MiB = 2 ** 20;
    const used = new Map<string, string>();
    const template = { ...TEMPLATE, spec: { ...TEMPLATE.spec, containers: TEMPLATE.spec.containers.map((container) => ({ ...container, resources: { requests: { memory: "512Mi" }, limits: { memory: "8Gi" } } })) } };
    const of = await foldersOf(stand.port, 60_000, 10_000, 3_000, { template, used, sizingIntervalMs: 100 });
    const measure = () => Effect.runPromise(of.folders.measure);
    const key = folderKey("claude", "telegram:sized");
    const requested = (pod: V1Pod) => pod.spec?.containers.find(({ name }) => name === "bayma")?.resources?.requests?.["memory"];
    const first = await bound(of, key);
    assert.equal(requested(first), "512Mi");
    // Its bayma holding 1.5 GiB over some thirty intervals, then more for a moment, as the metrics server says.
    used.set(first.metadata?.name ?? "", `${1536 * 1024}Ki`);
    for (let n = 0; n < 30; n += 1) {
      if (n === 29) used.set(first.metadata?.name ?? "", "3Gi");
      await measure();
      await new Promise((resolve) => setTimeout(resolve, 60));
    }
    const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
    assert.equal(await Effect.runPromise(of.binder.release(placement.pool, key, placement.release)), "released");
    const lease = (await run(withKube((kube) => kube.read("coordination.k8s.io/v1", "Lease", of.namespace, sizingLease(key))))) as V1Lease | null;
    const peaks = JSON.parse(lease?.metadata?.annotations?.["alasio.dev/memory-peaks"] ?? "[]") as [number, number][];
    assert.ok(peaks.length >= 4 && peaks.every(([, bytes]) => bytes === 1536 * MiB || bytes === 3 * 1024 * MiB), JSON.stringify(peaks));
    // Its next instance: the 90th percentile, 1.5 GiB, with its margin; the moment's 3 GiB one interval of some thirty.
    assert.equal(requested(await bound(of, key)), String(Math.ceil(1536 * 1.15) * MiB));
    // Another conversation's instance: its template's.
    assert.equal(requested(await bound(of, folderKey("claude", "telegram:other"))), "512Mi");
  } finally {
    stand.close();
  }
});

test("a folder instance is made of its template's request where the metrics server does not answer", async () => {
  const stand = await bayma();
  try {
    await unmeasured(stand.port, {});
  } finally {
    stand.close();
  }
});

test("a folder instance is made of its template's request where the metrics server never answers, its look cut short", { timeout: 60_000 }, async () => {
  const stand = await bayma();
  try {
    await unmeasured(stand.port, { metricsHang: true });
  } finally {
    stand.close();
  }
});

/** A folder template's, its bayma requesting 512Mi, up to 2 GB, a limit of no whole MiB. */
const SIZED = { ...TEMPLATE, spec: { ...TEMPLATE.spec, containers: TEMPLATE.spec.containers.map((container) => ({ ...container, resources: { requests: { memory: "512Mi" }, limits: { memory: "2G" } } })) } };
const requestedOf = (pod: V1Pod) => pod.spec?.containers.find(({ name }) => name === "bayma")?.resources?.requests?.["memory"];
/** Releases `key`, as its caller asks. */
async function release(of: Awaited<ReturnType<typeof foldersOf>>, key: string) {
  const placement = await Effect.runPromise(of.folders.place(key, { environment: "main" }));
  assert.equal(await Effect.runPromise(of.binder.release(placement.pool, key, placement.release)), "released");
}

/** That an instance measured, as `options` have the metrics server answer, keeps nothing: its next pod of its template's request. */
async function unmeasured(port: number, options: { readonly metricsHang?: boolean }) {
  const of = await foldersOf(port, 60_000, 10_000, 3_000, { template: SIZED, sizingIntervalMs: 100, ...options });
  const key = folderKey("claude", "telegram:unmeasured");
  await bound(of, key);
  await Effect.runPromise(of.folders.measure);
  await release(of, key);
  assert.equal(requestedOf(await bound(of, key)), "512Mi");
}

test("a folder instance measured over its limit of no whole MiB requests the limit, in bytes, as the API server takes it; and its folder's peaks are forgotten once all are past their days", async () => {
  const stand = await bayma();
  try {
    const used = new Map<string, string>();
    const of = await foldersOf(stand.port, 60_000, 10_000, 3_000, { template: SIZED, used, sizingIntervalMs: 100 });
    const key = folderKey("claude", "telegram:capped");
    used.set((await bound(of, key)).metadata?.name ?? "", "4Gi");
    await Effect.runPromise(of.folders.measure);
    await release(of, key);
    assert.equal(requestedOf(await bound(of, key)), "2G");
    // A folder's Lease whose peaks are all past their days: forgotten; one with a peak of these days: kept.
    const sizingOf = (name: string) => run(withKube((kube) => kube.read("coordination.k8s.io/v1", "Lease", of.namespace, name)));
    const old = folderKey("claude", "telegram:old");
    await run(withKube((kube) =>
      kube.create({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { name: sizingLease(old), namespace: of.namespace, labels: { "alasio.dev/folder-sizing": "true" }, annotations: { [KEY_ANNOTATION]: old, "alasio.dev/memory-peaks": JSON.stringify([[Date.now() - 200 * 100, 2 ** 30]]) } } })
    ));
    await new Promise((resolve) => setTimeout(resolve, 150));
    await Effect.runPromise(of.folders.measure);
    assert.equal(await sizingOf(sizingLease(old)), null);
    assert.notEqual(await sizingOf(sizingLease(key)), null);
  } finally {
    stand.close();
  }
});

test("a folder instance being released is not measured, as its release keeps its peak, and none after would be", async () => {
  const stand = await bayma();
  try {
    const used = new Map<string, string>();
    const of = await foldersOf(stand.port, 60_000, 10_000, 3_000, { template: SIZED, used, sizingIntervalMs: 100 });
    const key = folderKey("claude", "telegram:releasing");
    used.set((await bound(of, key)).metadata?.name ?? "", "1Gi");
    const pool = { name: FOLDER_POOL, namespace: of.namespace };
    // Marked releasing, as a binder marks it as its release begins.
    const lease = await run(readLease(pool, key));
    await run(withKube((kube) => kube.patch("coordination.k8s.io/v1", "Lease", of.namespace, lease?.metadata?.name ?? "", { metadata: { annotations: { "alasio.dev/releasing": "another-binder" } } })));
    await Effect.runPromise(of.folders.measure);
    await new Promise((resolve) => setTimeout(resolve, 150));
    await Effect.runPromise(of.folders.measure);
    assert.equal(await run(withKube((kube) => kube.read("coordination.k8s.io/v1", "Lease", of.namespace, sizingLease(key)))), null);
  } finally {
    stand.close();
  }
});
