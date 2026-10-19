import assert from "node:assert/strict";
import { isIP } from "node:net";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Effect, Fiber, Layer } from "effect";

import { added } from "./support/metrics.ts";
import { BIND_DURATION, BindStepFailed, BindTimeout, Binder, type BinderOptions } from "../src/binder/binder.ts";
import { type BindingLease, claim, markReleasing, readLease, reconcile, releaseLease, renewLease, takeLease } from "../src/binder/bindings.ts";
import { BINDING_LABEL, boundLabels, FREE_LABEL, freeLabels, KEY_ANNOTATION, keyHash, leaseName, type Pool, POOL_LABEL } from "../src/binder/names.ts";
import { KubeClient } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// Bindings on a real API server, as the races between binders are its to decide: each
// binder a client of its own, with connections of its own.

/** A binder's client of the API server: its own, as each replica's is. */
const binder = (): Layer.Layer<KubeClient> => Layer.fresh(KubeClient.layer);

/** `use` of the client. */
const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);

const run = <A, E>(layer: Layer.Layer<KubeClient>, effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, layer));

let namespaces = 0;

/** A namespace of the test's own, holding a pool. */
async function pool(): Promise<Pool> {
  const name = `pool-${(namespaces += 1)}`;
  await run(binder(), withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name } })));
  return { name: "sandboxes", namespace: name };
}

/** Reports the pod `name` of `of` serving ready, at an address of its own, as kubelet would. */
let addresses = 0;
const serve = (client: KubeClient["Service"], of: Pool, name: string) =>
  client.patchPodStatus(of.namespace, name, { status: { phase: "Running", podIP: `10.42.0.${(addresses += 1) % 250}`, conditions: [{ type: "Ready", status: "True" }] } });

/** `count` free pods of `of`, serving ready, named from `first` on. */
async function freePods(of: Pool, count: number, first = 0): Promise<string[]> {
  const kube = binder();
  return Promise.all(Array.from({ length: count }, (_, n) =>
    run(kube, Effect.gen(function*() {
      const client = yield* KubeClient;
      const name = `free-${first + n}`;
      yield* client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels: { ...freeLabels(of) } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
      yield* serve(client, of, name);
      return name;
    }))
  ));
}

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

test("of binders taking one key's Lease at once, one alone takes it, every time", async () => {
  const of = await pool();
  const binders = [binder(), binder(), binder(), binder()];
  const keys = Array.from({ length: 50 }, (_, n) => `sessionfs:workspace-${n}`);
  const taken = await Promise.all(keys.map((key) => Promise.all(binders.map((layer, n) => run(layer, takeLease(of, key, `binder-${n}`))))));
  assert.deepEqual(taken.map((each) => each.filter(({ taken }) => taken).length), keys.map(() => 1));
  // Those it was not taken for see it as the one that took it made it.
  for (const each of taken) {
    const [made] = each.filter(({ taken }) => taken);
    assert.ok(each.every(({ lease }) => lease.metadata?.annotations?.["alasio.dev/binder"] === made?.lease.metadata?.annotations?.["alasio.dev/binder"]));
  }
  const lease = await run(binder(), readLease(of, keys[0]!));
  assert.deepEqual([lease?.metadata?.name, lease?.metadata?.annotations?.[KEY_ANNOTATION], lease?.metadata?.labels?.[BINDING_LABEL]], [leaseName(keys[0]!), keys[0], keyHash(keys[0]!)]);
});

test("a binding under way is taken over once it goes unrenewed for its timeout, and one holding a pod never is", async () => {
  const of = await pool();
  const kube = binder();
  const lease = (key: string, renewed: string, holder?: string): BindingLease => ({
    apiVersion: "coordination.k8s.io/v1",
    kind: "Lease",
    metadata: { name: leaseName(key), namespace: of.namespace, annotations: { [KEY_ANNOTATION]: key, "alasio.dev/binder": "gone" } },
    spec: { renewTime: renewed, leaseDurationSeconds: 60, ...(holder ? { holderIdentity: holder } : {}) },
  });
  const old = `${new Date(Date.now() - 61_000).toISOString().slice(0, -1)}000Z`;
  await run(kube, withKube((client) => Effect.all([
    client.create(lease("stale", old)),
    client.create(lease("recent", `${new Date().toISOString().slice(0, -1)}000Z`)),
    client.create(lease("bound", old, "pool-pod")),
  ])));
  const [stale, recent, bound] = await Promise.all(["stale", "recent", "bound"].map((key) => run(kube, takeLease(of, key, "binder-new"))));
  assert.deepEqual([stale?.taken, recent?.taken, bound?.taken], [true, false, false]);
  assert.equal(stale?.lease.metadata?.annotations?.["alasio.dev/binder"], "binder-new");
});

test("a Lease let go as it was read is kept where another binder changed it since", async () => {
  const of = await pool();
  const kube = binder();
  const { lease } = await run(kube, takeLease(of, "sessionfs:workspace", "binder"));
  const renewed = await run(kube, renewLease(lease));
  assert.ok(renewed);
  await assert.rejects(run(kube, releaseLease(lease)), (error: unknown) => error instanceof Error && "status" in error && error.status === 409);
  assert.ok(await run(kube, readLease(of, "sessionfs:workspace")));
  await run(kube, releaseLease(renewed));
  assert.equal(await run(kube, readLease(of, "sessionfs:workspace")), null);
});

test("of binders claiming a pool's free pods at once, each pod is claimed once, out of its Deployment's selector, and those left over claim none", async () => {
  const of = await pool();
  const pods = await freePods(of, 10);
  const binders = [binder(), binder(), binder()];
  const keys = Array.from({ length: 30 }, (_, n) => `sessionfs:workspace-${n}`);
  const claimed = await Promise.all(keys.map((key, n) => run(binders[n % binders.length]!, claim(of, key))));
  const won = claimed.flatMap((pod, n) => (pod ? [[pod.metadata?.name, keys[n]!] as const] : []));
  assert.equal(won.length, pods.length);
  assert.equal(new Set(won.map(([pod]) => pod)).size, pods.length);
  const held = await run(binder(), withKube((client) => client.list("v1", "Pod", of.namespace, ""))) as V1Pod[];
  for (const [name, key] of won) {
    const pod = held.find(({ metadata }) => metadata?.name === name);
    assert.deepEqual(
      [pod?.metadata?.labels?.[FREE_LABEL], pod?.metadata?.labels?.[POOL_LABEL], pod?.metadata?.labels?.[BINDING_LABEL], pod?.metadata?.annotations?.[KEY_ANNOTATION]],
      [undefined, of.name, keyHash(key), key],
    );
    // Its node kept while it is bound, as what it holds lives in it alone.
    assert.deepEqual(
      [pod?.metadata?.annotations?.["cluster-autoscaler.kubernetes.io/safe-to-evict"], pod?.metadata?.annotations?.["karpenter.sh/do-not-disrupt"]],
      ["false", "true"],
    );
  }
});

test("a claim takes a free pod that runs, ready or not, and passes over one not yet running, and one being deleted", async () => {
  const of = await pool();
  const kube = binder();
  await run(kube, withKube((client) =>
    client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name: "starting", namespace: of.namespace, labels: { ...freeLabels(of) } }, spec: { containers: [{ name: "bayma", image: "agent" }] } })
  ));
  // Held by a finalizer as it is deleted, as kubelet holds a pod until its containers have stopped.
  const [ending] = await freePods(of, 1);
  await run(kube, withKube((client) => Effect.andThen(client.patch("v1", "Pod", of.namespace, ending!, { metadata: { finalizers: ["alasio.dev/test"] } }), client.remove("v1", "Pod", of.namespace, ending!))));
  assert.equal(await run(kube, claim(of, "sessionfs:workspace")), null);
  await run(kube, withKube((client) => client.patch("v1", "Pod", of.namespace, ending!, { metadata: { finalizers: null } })));
  // Running, its containers started, not ready, as a pool's pod waits for its workspace: claimed.
  await run(kube, withKube((client) => client.patchPodStatus(of.namespace, "starting", { status: { phase: "Running", conditions: [{ type: "Ready", status: "False" }] } })));
  assert.equal((await run(kube, claim(of, "sessionfs:workspace")))?.metadata?.name, "starting");
});

/** A binder's Binder, binds waiting up to `waitMs`, on a client of its own. */
const binderOf = (options: Partial<BinderOptions> = {}): Layer.Layer<Binder | KubeClient> => Layer.provideMerge(Binder.layer({ waitMs: 10_000, ...options }), binder());

const bindIn = <A, E>(layer: Layer.Layer<Binder | KubeClient>, use: (binder: Binder["Service"]) => Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(Effect.provide(Effect.flatMap(Binder, use), layer));

/** The pods of `of` bound to any key. */
const boundPods = async (of: Pool) =>
  ((await run(binder(), withKube((client) => client.list("v1", "Pod", of.namespace, BINDING_LABEL)))) as V1Pod[]).filter(({ metadata }) => !metadata?.deletionTimestamp);


test("binders binding one key at once bind it to one pod, which each is routed to", async () => {
  const of = await pool();
  await freePods(of, 5);
  const binders = [binderOf(), binderOf(), binderOf()];
  const bound = await Promise.all(binders.map((layer) => bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"))));
  assert.equal(new Set(bound.map(({ pod }) => pod)).size, 1);
  assert.equal(new Set(bound.map(({ address }) => address)).size, 1);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), [bound[0]?.pod]);
  // Routed to the pod's own address, there as it serves, not a name of DNS's, whose record may come after.
  assert.equal(isIP(bound[0]?.address ?? ""), 4);
  assert.equal(bound[0]?.address, (await boundPods(of))[0]?.status?.podIP);
  const lease = await run(binder(), readLease(of, "sessionfs:workspace"));
  assert.equal(lease?.spec?.holderIdentity, bound[0]?.pod);
  // Bound already, a bind finds its pod.
  assert.deepEqual(await bindIn(binderOf(), (each) => each.bind(of, "sessionfs:workspace")), bound[0]);
});

test("a bind with no free pod waits for one, counted waiting meanwhile, and one that waits too long gives its Lease up", async () => {
  const of = await pool();
  const layer = binderOf({ waitMs: 5_000 });
  await Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const each = yield* Binder;
    const waiting = yield* Effect.forkChild(each.bind(of, "sessionfs:waits"));
    for (let tries = 0; (yield* each.waiting(of)) === 0; tries += 1) {
      assert.ok(tries < 100, "the bind never waited");
      yield* Effect.sleep(50);
    }
    yield* Effect.promise(() => freePods(of, 1));
    const bound = yield* Fiber.join(waiting);
    assert.equal(bound.pod, "free-0");
    assert.equal(yield* each.waiting(of), 0);
    const timedOut = yield* Effect.flip(each.bind(of, "sessionfs:never"));
    assert.ok(timedOut instanceof BindTimeout && timedOut.waited === "free pod", String(timedOut));
    assert.equal(yield* each.waiting(of), 0);
  }), layer));
  assert.equal(await run(binder(), readLease(of, "sessionfs:never")), null);
});

test("a key whose Lease names a pod gone is bound anew, to another pod", async () => {
  const of = await pool();
  await freePods(of, 2);
  const layer = binderOf();
  const first = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  await run(binder(), withKube((client) => client.remove("v1", "Pod", of.namespace, first.pod)));
  const again = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  assert.notEqual(again.pod, first.pod);
  assert.equal((await run(binder(), readLease(of, "sessionfs:workspace")))?.spec?.holderIdentity, again.pod);
});

test("a bind that makes its own pod makes it bound to the key from the first, given the binding it makes it for, and claims none", async () => {
  const of = await pool();
  await freePods(of, 1);
  const made: Record<string, string>[][] = [];
  const bindings: string[] = [];
  const layer = binderOf();
  const bound = await bindIn(layer, (each) =>
    each.bind(of, "sessionfs:restored", {
      make: (labels, annotations, binding) =>
        withKube((client) =>
          Effect.gen(function*() {
            made.push([{ ...labels }, { ...annotations }]);
            bindings.push(binding);
            const pod = yield* client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name: "restored", namespace: of.namespace, labels: { ...labels }, annotations: { ...annotations } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
            yield* serve(client, of, "restored");
            return pod;
          })
        ).pipe(Effect.provide(binder())),
    })
  );
  assert.equal(bound.pod, "restored");
  assert.deepEqual(made, [[boundLabels(of, "sessionfs:restored"), {
    [KEY_ANNOTATION]: "sessionfs:restored",
    "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
    "karpenter.sh/do-not-disrupt": "true",
  }]]);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), ["restored"]);
  // The binding's, its Lease's uid, as its pod's token names it.
  assert.deepEqual(bindings, [bound.binding]);
});

test("a binding under way is renewed however long its pod takes to make, so no other binder takes it over meanwhile", async () => {
  const of = await pool();
  const timing = { renewMs: 300, staleMs: 1_500 };
  let made = 0;
  const slow = (labels: Readonly<Record<string, string>>, annotations: Readonly<Record<string, string>>) =>
    withKube((client) =>
      Effect.gen(function*() {
        made += 1;
        // Longer than a binding goes unrenewed before it is stale, as a restore may be.
        yield* Effect.sleep(4_000);
        const name = `restored-${made}`;
        const pod = yield* client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels: { ...labels }, annotations: { ...annotations } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
        yield* serve(client, of, name);
        return pod;
      })
    ).pipe(Effect.provide(binder()));
  const first = bindIn(binderOf(timing), (each) => each.bind(of, "sessionfs:slow", { make: slow }));
  await new Promise((resolve) => setTimeout(resolve, 2_500));
  const second = await bindIn(binderOf(timing), (each) => each.bind(of, "sessionfs:slow", { make: slow }));
  assert.equal(made, 1);
  assert.deepEqual(second, await first);
});

test("reconciling a pool deletes a bound pod no Lease names and lets go a Lease whose pod is gone, what comes before run while it holds, telling of it, and leaves a binding whole, under way, or whose pod is ending", async () => {
  const of = await pool();
  await freePods(of, 4);
  const layer = binderOf();
  const whole = await bindIn(layer, (each) => each.bind(of, "sessionfs:whole"));
  // A pod claimed by a bind that ended before it named it, then gave its Lease up.
  const orphan = await run(binder(), claim(of, "sessionfs:orphan"));
  // A Lease whose pod was deleted.
  const gone = await bindIn(layer, (each) => each.bind(of, "sessionfs:gone"));
  await run(binder(), withKube((client) => client.remove("v1", "Pod", of.namespace, gone.pod)));
  // A binding under way.
  await run(binder(), takeLease(of, "sessionfs:under-way", "binder"));
  // A binding whose pod is being deleted, held there as its binder saves it as it ends.
  const ending = await bindIn(layer, (each) => each.bind(of, "sessionfs:ending"));
  await run(binder(), withKube((client) => client.patch("v1", "Pod", of.namespace, ending.pod, { metadata: { finalizers: ["alasio.dev/test"] } })));
  await run(binder(), withKube((client) => client.remove("v1", "Pod", of.namespace, ending.pod)));
  const told: string[] = [];
  const held: boolean[] = [];
  const set = await run(binder(), reconcile(
    of,
    (lease) => Effect.sync(() => void told.push(`${lease.metadata?.annotations?.[KEY_ANNOTATION]} ${lease.metadata?.uid === gone.binding} ${lease.spec?.holderIdentity === gone.pod}`)),
    (lease) => Effect.map(readLease(of, lease.metadata?.annotations?.[KEY_ANNOTATION] ?? ""), (still) => void held.push(still !== null)).pipe(Effect.provide(binder()), Effect.orDie),
  ));
  assert.deepEqual(set, { pods: [orphan?.metadata?.name], leases: [leaseName("sessionfs:gone")] });
  // The binding whose pod went, as what binds may say lost: its key, binding and pod; what comes before run while its Lease held.
  assert.deepEqual(told, ["sessionfs:gone true true"]);
  assert.deepEqual(held, [true]);
  assert.equal((await run(binder(), readLease(of, "sessionfs:ending")))?.spec?.holderIdentity, ending.pod);
  await run(binder(), withKube((client) => client.patch("v1", "Pod", of.namespace, ending.pod, { metadata: { finalizers: null } })));
  assert.ok(await run(binder(), readLease(of, "sessionfs:under-way")));
  assert.equal((await run(binder(), readLease(of, "sessionfs:whole")))?.spec?.holderIdentity, whole.pod);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), [whole.pod]);
});

test("a release runs what comes before it, its Lease renewed, then deletes the pod and its Lease; a bind meanwhile waits for it, and binds anew after", async () => {
  const of = await pool();
  await freePods(of, 2);
  const layer = binderOf({ renewMs: 300, staleMs: 1_500 });
  const first = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  const happened: string[] = [];
  const releasing = bindIn(layer, (each) =>
    each.release(of, "sessionfs:workspace", ({ pod, binding }) =>
      Effect.gen(function*() {
        happened.push(`before ${pod} ${binding === first.binding}`);
        // Longer than a release goes unrenewed before it is stale, as a checkpoint may be.
        yield* Effect.sleep(3_000);
        happened.push("checkpointed");
      })
    )
  );
  await new Promise((resolve) => setTimeout(resolve, 500));
  const again = bindIn(binderOf({ renewMs: 300, staleMs: 1_500 }), (each) => Effect.tap(each.bind(of, "sessionfs:workspace"), () => Effect.sync(() => void happened.push("bound again"))));
  assert.equal(await releasing, "released");
  const bound = await again;
  assert.deepEqual(happened, [`before ${first.pod} true`, "checkpointed", "bound again"]);
  assert.notEqual(bound.pod, first.pod);
  assert.notEqual(bound.binding, first.binding);
});

test("of two releases of one binding at once one alone goes on, and one whose step before fails keeps the binding", async () => {
  const of = await pool();
  await freePods(of, 1);
  const layer = binderOf();
  const bound = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  const failing = await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => Effect.flip(each.release(of, "sessionfs:workspace", () => Effect.fail("the checkpoint failed")))), layer));
  assert.equal(failing, "the checkpoint failed");
  const lease = await run(binder(), readLease(of, "sessionfs:workspace"));
  assert.deepEqual([lease?.spec?.holderIdentity, lease?.metadata?.annotations?.["alasio.dev/releasing"]], [bound.pod, undefined]);
  let before = 0;
  const released = await Promise.all([binderOf(), binderOf()].map((each) => bindIn(each, (binderService) => binderService.release(of, "sessionfs:workspace", () => Effect.sleep(500).pipe(Effect.andThen(Effect.sync(() => void (before += 1))))))));
  assert.deepEqual([released.toSorted(), before], [["busy", "released"], 1]);
  assert.equal(await run(binder(), readLease(of, "sessionfs:workspace")), null);
  assert.equal(await bindIn(layer, (each) => each.release(of, "sessionfs:workspace")), "unbound");
});

test("a release of a key a bind is under way of releases none and says so, and one of a bind under way whose binder ended finds none to release", async () => {
  const of = await pool();
  await run(binder(), takeLease(of, "sessionfs:under-way", "binder"));
  await run(binder(), takeLease(of, "sessionfs:ended", "ended", 1_000));
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  assert.equal(await bindIn(binderOf(), (each) => each.release(of, "sessionfs:under-way")), "busy");
  assert.equal(await bindIn(binderOf({ staleMs: 1_000 }), (each) => each.release(of, "sessionfs:ended")), "unbound");
});

test("a bind of a key whose release was left unrenewed, its releaser ended, finishes the release and binds the key anew", async () => {
  const of = await pool();
  await freePods(of, 2);
  const layer = binderOf({ staleMs: 1_500 });
  const first = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  const lease = await run(binder(), readLease(of, "sessionfs:workspace"));
  assert.ok(lease);
  await run(binder(), markReleasing(lease, "ended"));
  await new Promise((resolve) => setTimeout(resolve, 1_600));
  const again = await bindIn(layer, (each) => each.bind(of, "sessionfs:workspace"));
  assert.notEqual(again.pod, first.pod);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), [again.pod]);
});

test("a pod once bound is never claimed again, even labelled free again", async () => {
  const of = await pool();
  await freePods(of, 1);
  const layer = binderOf({ waitMs: 1_000 });
  const first = await bindIn(layer, (each) => each.bind(of, "sessionfs:first"));
  // Its release left half done, its pod labelled free again, as by hand.
  await run(binder(), withKube((client) => client.patch("v1", "Pod", of.namespace, first.pod, { metadata: { labels: { [FREE_LABEL]: "true" } } })));
  assert.equal(await run(binder(), claim(of, "sessionfs:second")), null);
  const timedOut = await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => Effect.flip(each.bind(of, "sessionfs:second"))), layer));
  assert.ok(timedOut instanceof BindTimeout && timedOut.waited === "free pod", String(timedOut));
});

test("a bind readies the pod it got before it is the binding's, and one it cannot ready is deleted, its Lease let go", async () => {
  const of = await pool();
  await freePods(of, 2);
  const layer = binderOf();
  const readied: string[] = [];
  const bound = await bindIn(layer, (each) =>
    each.bind(of, "sessionfs:workspace", {
      prepare: (pod, binding) =>
        Effect.gen(function*() {
          const lease = yield* withKube((client) => client.read("coordination.k8s.io/v1", "Lease", of.namespace, leaseName("sessionfs:workspace"))).pipe(Effect.provide(binder()), Effect.orDie);
          // Not yet the binding's: its Lease names no holder while it is readied.
          readied.push(`${pod.metadata?.name} ${binding === lease?.metadata?.uid} ${(lease as BindingLease | null)?.spec?.holderIdentity ?? "none"}`);
        }),
    })
  );
  assert.deepEqual(readied, [`${bound.pod} true none`]);
  const failed = await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => Effect.flip(each.bind(of, "sessionfs:failing", { prepare: () => Effect.fail(new BindStepFailed({ reason: "the attach was refused" })) }))), layer));
  assert.ok(failed instanceof BindStepFailed, String(failed));
  assert.equal(await run(binder(), readLease(of, "sessionfs:failing")), null);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), [bound.pod]);
});

test("a bind that finds a pod bound to the key and not yet held, as a binder that ended left it, deletes it and gets another", async () => {
  const of = await pool();
  await freePods(of, 2);
  // A binder that took the Lease and claimed a pod, then ended: its Lease stale, its pod unheld.
  const kube = binder();
  await run(kube, takeLease(of, "sessionfs:workspace", "ended", 1_000));
  const left = await run(kube, claim(of, "sessionfs:workspace"));
  await new Promise((resolve) => setTimeout(resolve, 1_100));
  const bound = await bindIn(binderOf({ staleMs: 1_000 }), (each) => each.bind(of, "sessionfs:workspace"));
  assert.notEqual(bound.pod, left?.metadata?.name);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), [bound.pod]);
});

test("each binding a binder makes is timed, by its pool and how its pod came: claimed at once, waited for, or made; a bind of a key bound already is not", async () => {
  const of = await pool();
  await freePods(of, 1);
  const layer = binderOf({ waitMs: 10_000 });
  const timed = await added(BIND_DURATION, () =>
    Effect.runPromise(Effect.provide(Effect.gen(function*() {
      const each = yield* Binder;
      yield* each.bind(of, "sessionfs:warm");
      yield* each.bind(of, "sessionfs:warm");
      const waiting = yield* Effect.forkChild(each.bind(of, "sessionfs:cold"));
      for (let tries = 0; (yield* each.waiting(of)) === 0; tries += 1) {
        assert.ok(tries < 100, "the bind never waited");
        yield* Effect.sleep(50);
      }
      yield* Effect.promise(() => freePods(of, 1, 1));
      yield* Fiber.join(waiting);
      yield* each.bind(of, "sessionfs:made", {
        make: (labels, annotations) =>
          withKube((client) =>
            Effect.gen(function*() {
              const pod = yield* client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name: "made", namespace: of.namespace, labels: { ...labels }, annotations: { ...annotations } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
              yield* serve(client, of, "made");
              return pod;
            })
          ).pipe(Effect.provide(binder())),
      });
    }), layer)));
  const of_ = (start: string) => `alasio.binder.pool=sandboxes,k8s.namespace.name=${of.namespace},alasio.binder.start=${start}`;
  assert.deepEqual(timed, { [of_("warm")]: 1, [of_("cold")]: 1, [of_("made")]: 1 });
});

test("a bind whose maker declines, as it finds under the binding's Lease, claims a free pod instead, its maker asked while it holds the Lease", async () => {
  const of = await pool();
  await freePods(of, 1);
  const layer = binderOf();
  const held: (string | undefined)[] = [];
  const bound = await bindIn(layer, (each) =>
    each.bind(of, "sessionfs:declined", {
      make: () =>
        readLease(of, "sessionfs:declined").pipe(
          Effect.map((lease) => (held.push(lease?.metadata?.annotations?.["alasio.dev/binder"]), null)),
          Effect.provide(binder()),
        ),
    })
  );
  assert.equal(bound.pod, "free-0");
  assert.equal(held.length, 1);
  assert.ok(held[0]);
});

test("a hold of a key runs what it holds for under the key's Lease, a bind meanwhile waiting for it, then lets it go; one of a key bound runs nothing, and one whose effect fails lets the Lease go", async () => {
  const of = await pool();
  await freePods(of, 1);
  const layer = binderOf();
  await Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const each = yield* Binder;
    const order: string[] = [];
    const held = yield* each.hold(of, "sessionfs:held", Effect.gen(function*() {
      const lease = yield* Effect.promise(() => run(binder(), readLease(of, "sessionfs:held")));
      assert.ok(lease && !lease.spec?.holderIdentity, "its Lease is not held under way");
      // A bind meanwhile, by another binder, waits for it.
      const binding = yield* Effect.forkChild(Effect.promise(() => bindIn(binderOf(), (other) => other.bind(of, "sessionfs:held"))).pipe(Effect.tap(() => Effect.sync(() => order.push("bound")))));
      yield* Effect.sleep("1500 millis");
      order.push("held");
      return binding;
    }));
    assert.ok(held);
    const bound = yield* Fiber.join(held.value);
    assert.deepEqual(order, ["held", "bound"]);
    // Bound: nothing run.
    let ran = false;
    assert.equal(yield* each.hold(of, "sessionfs:held", Effect.sync(() => (ran = true))), null);
    assert.equal(ran, false);
    assert.equal((yield* Effect.promise(() => run(binder(), readLease(of, "sessionfs:held"))))?.spec?.holderIdentity, bound.pod);
    // Failed: its failure given, the Lease let go.
    assert.equal(yield* Effect.flip(each.hold(of, "sessionfs:failing", Effect.fail("it failed"))), "it failed");
    assert.equal(yield* Effect.promise(() => run(binder(), readLease(of, "sessionfs:failing"))), null);
  }), layer));
});
