import assert from "node:assert/strict";
import { before, test } from "node:test";

import type { V1Pod } from "@kubernetes/client-node";
import { Effect, Fiber, Layer } from "effect";

import { BindTimeout, Binder, type BinderOptions } from "../src/binder/binder.ts";
import { BINDING_LABEL, type BindingLease, boundLabels, claim, FREE_LABEL, freeLabels, KEY_ANNOTATION, keyHash, leaseName, markReleasing, type Pool, POOL_LABEL, readLease, reconcile, releaseLease, renewLease, takeLease } from "../src/binder/bindings.ts";
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
  }
});

test("a claim passes over a free pod not serving, and one being deleted", async () => {
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

test("a bind that makes its own pod makes it bound to the key from the first, and claims none", async () => {
  const of = await pool();
  await freePods(of, 1);
  const made: Record<string, string>[] = [];
  const layer = binderOf();
  const bound = await bindIn(layer, (each) =>
    each.bind(of, "sessionfs:restored", {
      make: (labels, annotations) =>
        withKube((client) =>
          Effect.gen(function*() {
            made.push({ ...labels });
            const pod = yield* client.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name: "restored", namespace: of.namespace, labels: { ...labels }, annotations: { ...annotations } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
            yield* serve(client, of, "restored");
            return pod;
          })
        ).pipe(Effect.provide(binder())),
    })
  );
  assert.equal(bound.pod, "restored");
  assert.deepEqual(made, [boundLabels(of, "sessionfs:restored")]);
  assert.deepEqual((await boundPods(of)).map(({ metadata }) => metadata?.name), ["restored"]);
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

test("reconciling a pool deletes a bound pod no Lease names and lets go a Lease whose pod is gone, and leaves a binding whole, or under way", async () => {
  const of = await pool();
  await freePods(of, 3);
  const layer = binderOf();
  const whole = await bindIn(layer, (each) => each.bind(of, "sessionfs:whole"));
  // A pod claimed by a bind that ended before it named it, then gave its Lease up.
  const orphan = await run(binder(), claim(of, "sessionfs:orphan"));
  // A Lease whose pod was deleted.
  const gone = await bindIn(layer, (each) => each.bind(of, "sessionfs:gone"));
  await run(binder(), withKube((client) => client.remove("v1", "Pod", of.namespace, gone.pod)));
  // A binding under way.
  await run(binder(), takeLease(of, "sessionfs:under-way", "binder"));
  const set = await run(binder(), reconcile(of));
  assert.deepEqual(set, { pods: [orphan?.metadata?.name], leases: [leaseName("sessionfs:gone")] });
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
  assert.equal(await releasing, true);
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
  assert.deepEqual([released.toSorted(), before], [[false, true], 1]);
  assert.equal(await run(binder(), readLease(of, "sessionfs:workspace")), null);
  assert.equal(await bindIn(layer, (each) => each.release(of, "sessionfs:workspace")), false);
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
