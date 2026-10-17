import assert from "node:assert/strict";
import { before, test } from "node:test";

import { type Cause, Context, Effect, Exit, Layer, Queue, Scope, Stream } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { readLease } from "../src/binder/bindings.ts";
import { freeLabels, KEY_ANNOTATION, type Pool } from "../src/binder/names.ts";
import { type BindingNamespaces, LiveBindings } from "../src/binder/live.ts";
import { INITIAL_EVENTS_END, KubeClient, type WatchEvent } from "../src/kube/client.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The bindings there are, as their Leases are watched on a real API server.

const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

/** The namespaces `names`, as there are now and as known: those alone. */
const only = (names: Iterable<string>): BindingNamespaces => ({
  current: Effect.sync(() => [...names]),
  known: (namespace) => Effect.sync(() => [...names].includes(namespace)),
});

/** Waits until `holds` answers `expected`, for five seconds at most. */
async function until(holds: () => Promise<boolean>, expected: boolean, what: string): Promise<void> {
  for (const deadline = Date.now() + 5_000; (await holds()) !== expected;) {
    assert.ok(Date.now() < deadline, what);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

test("a binding is confirmed while its Lease names its pod, under its own key alone, and no more once it is released", async () => {
  const of: Pool = { name: "sandboxes", namespace: "live" };
  await run(Effect.flatMap(KubeClient, (kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: of.namespace } })));
  await run(Effect.flatMap(KubeClient, (kube) =>
    Effect.gen(function*() {
      for (const name of ["free-0", "free-1"]) {
        yield* kube.create({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: of.namespace, labels: { ...freeLabels(of) } }, spec: { containers: [{ name: "bayma", image: "agent" }] } });
        yield* kube.patchPodStatus(of.namespace, name, { status: { phase: "Running", podIP: "10.42.0.9", conditions: [{ type: "Ready", status: "True" }] } });
      }
    })
  ));
  const binder = Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer));
  // One bound before the watch began, one after.
  const before = await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => each.bind(of, "sessionfs:before")), binder));
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only([of.namespace]) }), Layer.fresh(KubeClient.layer)), scope)), LiveBindings);
  try {
    const holds = (key: string, binding: string) => Effect.runPromise(live.holds(key, binding, of.namespace));
    await until(async () => (await holds("sessionfs:before", before.binding)) === "held", true, "the binding made before the watch was never confirmed");
    const after = await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => each.bind(of, "sessionfs:after")), binder));
    await until(async () => (await holds("sessionfs:after", after.binding)) === "held", true, "the binding made after the watch began was never confirmed");
    // Of another key, or no binding at all: not.
    assert.equal(await holds("sessionfs:after", before.binding), "gone");
    assert.equal(await holds("sessionfs:before", "no-such-uid"), "gone");
    const released = Date.now();
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => each.release(of, "sessionfs:before")), binder));
    assert.equal(await run(readLease(of, "sessionfs:before")), null);
    await until(async () => (await holds("sessionfs:before", before.binding)) === "gone", true, "the released binding was still confirmed");
    assert.ok(Date.now() - released < 1_000, `confirmed ${Date.now() - released} ms after its release`);
    assert.equal(await holds("sessionfs:after", after.binding), "held");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a namespace whose watch has not sent every Lease there was, or has ended, can say of no binding that it is gone until it is watched anew", async () => {
  const watches: Queue.Queue<WatchEvent, Cause.Done>[] = [];
  const unreachable = () => Effect.die("not called");
  const kube = KubeClient.of({
    read: unreachable, list: unreachable, create: unreachable, replace: unreachable, patch: unreachable, patchPodStatus: unreachable, remove: unreachable, exec: unreachable,
    watch: () => Stream.callback<WatchEvent>((queue) => Effect.sync(() => void watches.push(queue))),
  });
  const lease = (uid: string) => ({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid, annotations: { [KEY_ANNOTATION]: "sessionfs:workspace" } }, spec: { holderIdentity: "pod" } });
  const caughtUp = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { annotations: { [INITIAL_EVENTS_END]: "true" } } };
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(["sessions"]) }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = () => Effect.runPromise(live.holds("sessionfs:workspace", "uid-1", "sessions"));
  try {
    await until(async () => watches.length === 1, true, "nothing was watched");
    Queue.offerUnsafe(watches[0]!, { type: "ADDED", object: lease("uid-1") });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(await holds(), "unknown");
    Queue.offerUnsafe(watches[0]!, { type: "BOOKMARK", object: caughtUp });
    await until(async () => (await holds()) === "held", true, "the binding was never confirmed once caught up");
    assert.equal(await Effect.runPromise(live.holds("sessionfs:workspace", "uid-2", "sessions")), "gone");
    // Ended by the API server: watched anew, and nothing confirmed until it has caught up again.
    Queue.endUnsafe(watches[0]!);
    await until(async () => watches.length === 2, true, "it was not watched anew");
    assert.equal(await holds(), "unknown");
    Queue.offerUnsafe(watches[1]!, { type: "ADDED", object: lease("uid-1") });
    Queue.offerUnsafe(watches[1]!, { type: "BOOKMARK", object: caughtUp });
    await until(async () => (await holds()) === "held", true, "the binding was never confirmed once caught up again");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a namespace of no environment's has no binding; one that comes to be is watched as soon as a token names it, and one that goes is watched no more", async () => {
  const watches = new Map<string, Queue.Queue<WatchEvent, Cause.Done>[]>();
  const unreachable = () => Effect.die("not called");
  const kube = KubeClient.of({
    read: unreachable, list: unreachable, create: unreachable, replace: unreachable, patch: unreachable, patchPodStatus: unreachable, remove: unreachable, exec: unreachable,
    watch: (_apiVersion, _plural, namespace) => Stream.callback<WatchEvent>((queue) => Effect.sync(() => void watches.set(namespace, [...(watches.get(namespace) ?? []), queue]))),
  });
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-1", annotations: { [KEY_ANNOTATION]: "sessionfs:workspace" } }, spec: { holderIdentity: "pod" } };
  const caughtUp = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { annotations: { [INITIAL_EVENTS_END]: "true" } } };
  // The environments there are: main's alone at first, then a branch made, then gone.
  const there = new Set(["main-sessions"]);
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(there), refreshMs: 50 }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = () => Effect.runPromise(live.holds("sessionfs:workspace", "uid-1", "branch-sessions"));
  try {
    await until(async () => watches.has("main-sessions"), true, "main's sessions were not watched");
    assert.equal(await holds(), "gone");
    assert.equal(watches.has("branch-sessions"), false);
    // Made: watched as a token names it, before the namespaces are looked at anew.
    there.add("branch-sessions");
    assert.equal(await holds(), "unknown");
    await until(async () => watches.get("branch-sessions")?.length === 1, true, "the branch's sessions were not watched");
    Queue.offerUnsafe(watches.get("branch-sessions")![0]!, { type: "ADDED", object: lease });
    Queue.offerUnsafe(watches.get("branch-sessions")![0]!, { type: "BOOKMARK", object: caughtUp });
    await until(async () => (await holds()) === "held", true, "the branch's binding was never confirmed");
    // Gone: its watch ended, its bindings none.
    there.delete("branch-sessions");
    await until(async () => (await holds()) === "gone", true, "the branch gone still had its binding");
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(watches.get("branch-sessions")?.length, 1);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
