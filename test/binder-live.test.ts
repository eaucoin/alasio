import assert from "node:assert/strict";
import { before, test } from "node:test";

import { Cause, Context, Effect, Exit, Layer, Queue, Scope, Stream } from "effect";

import { Binder } from "../src/binder/binder.ts";
import { readLease } from "../src/binder/bindings.ts";
import { freeLabels, KEY_ANNOTATION, type Pool } from "../src/binder/names.ts";
import { type BindingNamespaces, LiveBindings } from "../src/binder/live.ts";
import { INITIAL_EVENTS_END, KubeApiError, KubeClient, type WatchEvent } from "../src/kube/client.ts";
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

test("a watch cut, on a real API server, is watched again from the last version it saw, and hears what changed while it was cut", async () => {
  const of: Pool = { name: "sandboxes", namespace: "live-cut" };
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
  const bind = (key: string) => Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => each.bind(of, key)), binder));
  const before = await bind("sessionfs:before");
  // The real client's watches, the first cut once its list is through, the next held until the test lets it go on.
  const real = await run(KubeClient);
  const asked: (string | undefined)[] = [];
  let goOn!: () => void;
  const gone = new Promise<void>((resolve) => { goOn = resolve; });
  const kube = KubeClient.of({
    ...real,
    watch: (apiVersion, plural, namespace, labelSelector, from) => {
      asked.push(from);
      const watched = real.watch(apiVersion, plural, namespace, labelSelector, from);
      return asked.length === 1
        ? Stream.takeUntil(watched, ({ object }) => object.metadata?.annotations?.[INITIAL_EVENTS_END] === "true")
        : Stream.unwrap(Effect.as(Effect.promise(() => gone), watched));
    },
  });
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only([of.namespace]) }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  try {
    const holds = (key: string, binding: string) => Effect.runPromise(live.holds(key, binding, of.namespace));
    await until(async () => asked.length === 2, true, "the cut watch was not watched again");
    assert.equal(asked[0], undefined);
    assert.ok(asked[1], "the cut watch was listed anew, not watched again from a version");
    assert.equal(await holds("sessionfs:before", before.binding), "held");
    // While it is cut: one released, one bound.
    await Effect.runPromise(Effect.provide(Effect.flatMap(Binder, (each) => each.release(of, "sessionfs:before")), binder));
    const after = await bind("sessionfs:after");
    goOn();
    await until(async () => (await holds("sessionfs:after", after.binding)) === "held", true, "the binding made while the watch was cut was never confirmed");
    assert.equal(await holds("sessionfs:before", before.binding), "gone");
    assert.equal(asked.length, 2);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

/** A watch the fake KubeClient below was asked for: the version it was asked to watch from, if any, and what it sends. */
interface FakeWatch {
  readonly from: string | undefined;
  readonly queue: Queue.Queue<WatchEvent, KubeApiError | Cause.Done>;
}

/** A KubeClient that only watches, each watch asked for kept, by its namespace, for the test to send it what it will. */
function watchingOnly(): { readonly kube: KubeClient["Service"]; readonly watches: (namespace: string) => readonly FakeWatch[] } {
  const watches = new Map<string, FakeWatch[]>();
  const unreachable = () => Effect.die("not called");
  const kube = KubeClient.of({
    read: unreachable, list: unreachable, create: unreachable, replace: unreachable, patch: unreachable, patchPodStatus: unreachable, remove: unreachable, exec: unreachable,
    watch: (_apiVersion, _plural, namespace, _labelSelector, from) =>
      Stream.callback<WatchEvent, KubeApiError>((queue) => Effect.sync(() => void watches.set(namespace, [...(watches.get(namespace) ?? []), { from, queue }]))),
  });
  return { kube, watches: (namespace) => watches.get(namespace) ?? [] };
}

/** A binding's Lease of `key`, its uid `uid`, at `version`. */
const bindingLease = (uid: string, version: string, key = "sessionfs:workspace") =>
  ({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid, resourceVersion: version, annotations: { [KEY_ANNOTATION]: key } }, spec: { holderIdentity: "pod" } });

/** A bookmark at `version`; the one that ends a list's Leases, where `ending`. */
const bookmark = (version: string, ending = false) =>
  ({ apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { resourceVersion: version, ...(ending ? { annotations: { [INITIAL_EVENTS_END]: "true" } } : {}) } });

test("a namespace whose first list has not sent every Lease there was can say of no binding that it is gone", async () => {
  const { kube, watches } = watchingOnly();
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(["sessions"]) }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = (binding: string) => Effect.runPromise(live.holds("sessionfs:workspace", binding, "sessions"));
  try {
    await until(async () => watches("sessions").length === 1, true, "nothing was watched");
    assert.equal(watches("sessions")[0]!.from, undefined);
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "ADDED", object: bindingLease("uid-1", "5") });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(await holds("uid-1"), "unknown");
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "BOOKMARK", object: bookmark("6", true) });
    await until(async () => (await holds("uid-1")) === "held", true, "the binding was never confirmed once caught up");
    assert.equal(await holds("uid-2"), "gone");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a watch that ends, or fails, is watched again from the last version it saw, its bindings known meanwhile", async () => {
  const { kube, watches } = watchingOnly();
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(["sessions"]) }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = (binding: string) => Effect.runPromise(live.holds("sessionfs:workspace", binding, "sessions"));
  try {
    await until(async () => watches("sessions").length === 1, true, "nothing was watched");
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "ADDED", object: bindingLease("uid-1", "5") });
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "BOOKMARK", object: bookmark("6", true) });
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "ADDED", object: bindingLease("uid-2", "7") });
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "BOOKMARK", object: bookmark("9") });
    await until(async () => (await holds("uid-2")) === "held", true, "the binding made once caught up was never confirmed");
    // Ended by the API server: watched again from the bookmark, every binding known all along.
    Queue.endUnsafe(watches("sessions")[0]!.queue);
    assert.equal(await holds("uid-1"), "held");
    await until(async () => watches("sessions").length === 2, true, "it was not watched again");
    assert.equal(watches("sessions")[1]!.from, "9");
    assert.equal(await holds("uid-1"), "held");
    // What changed while it was not watched, sent by the API server from that version on.
    Queue.offerUnsafe(watches("sessions")[1]!.queue, { type: "DELETED", object: bindingLease("uid-1", "10") });
    Queue.offerUnsafe(watches("sessions")[1]!.queue, { type: "ADDED", object: bindingLease("uid-3", "11") });
    await until(async () => (await holds("uid-1")) === "gone", true, "the binding released while it was not watched was still confirmed");
    assert.equal(await holds("uid-3"), "held");
    assert.equal(await holds("uid-2"), "held");
    // Failed: watched again, a second on, from the last change it saw.
    Queue.failCauseUnsafe(watches("sessions")[1]!.queue, Cause.fail(new KubeApiError({ cause: new Error("connection reset") })));
    assert.equal(await holds("uid-3"), "held");
    await until(async () => watches("sessions").length === 3, true, "it was not watched again once it failed");
    assert.equal(watches("sessions")[2]!.from, "11");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a watch from a version the API server keeps no change from is listed anew, its bindings known until the new list has sent them all", async () => {
  const { kube, watches } = watchingOnly();
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(["sessions"]) }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = (binding: string) => Effect.runPromise(live.holds("sessionfs:workspace", binding, "sessions"));
  try {
    await until(async () => watches("sessions").length === 1, true, "nothing was watched");
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "ADDED", object: bindingLease("uid-1", "5") });
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "BOOKMARK", object: bookmark("6", true) });
    await until(async () => (await holds("uid-1")) === "held", true, "the binding was never confirmed");
    Queue.endUnsafe(watches("sessions")[0]!.queue);
    await until(async () => watches("sessions").length === 2, true, "it was not watched again");
    Queue.failCauseUnsafe(watches("sessions")[1]!.queue, Cause.fail(new KubeApiError({ status: 410, cause: new Error("too old resource version: 6") })));
    await until(async () => watches("sessions").length === 3, true, "it was not listed anew");
    assert.equal(watches("sessions")[2]!.from, undefined);
    // The new list under way: the bindings known before it, none of its own yet.
    Queue.offerUnsafe(watches("sessions")[2]!.queue, { type: "ADDED", object: bindingLease("uid-2", "40") });
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(await holds("uid-1"), "held");
    assert.equal(await holds("uid-2"), "gone");
    // Every Lease sent: the list's alone.
    Queue.offerUnsafe(watches("sessions")[2]!.queue, { type: "BOOKMARK", object: bookmark("41", true) });
    await until(async () => (await holds("uid-2")) === "held", true, "the new list's binding was never confirmed");
    assert.equal(await holds("uid-1"), "gone");
    Queue.endUnsafe(watches("sessions")[2]!.queue);
    await until(async () => watches("sessions").length === 4, true, "it was not watched again");
    assert.equal(watches("sessions")[3]!.from, "41");
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a namespace nothing has been heard of for its bindings' time can say of no binding that it is gone, until it is heard of again", async () => {
  const { kube, watches } = watchingOnly();
  const scope = await Effect.runPromise(Scope.make());
  const live = Context.get(await Effect.runPromise(Layer.buildWithScope(Layer.provide(LiveBindings.layer({ namespaces: only(["sessions"]), knownForMs: 300 }), Layer.succeed(KubeClient, kube)), scope)), LiveBindings);
  const holds = () => Effect.runPromise(live.holds("sessionfs:workspace", "uid-1", "sessions"));
  try {
    await until(async () => watches("sessions").length === 1, true, "nothing was watched");
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "ADDED", object: bindingLease("uid-1", "5") });
    Queue.offerUnsafe(watches("sessions")[0]!.queue, { type: "BOOKMARK", object: bookmark("6", true) });
    await until(async () => (await holds()) === "held", true, "the binding was never confirmed");
    // The API server unreachable: watched again, and again, and nothing heard.
    Queue.endUnsafe(watches("sessions")[0]!.queue);
    await until(async () => (await holds()) === "unknown", true, "the binding was confirmed though nothing had been heard of it");
    Queue.offerUnsafe(watches("sessions").at(-1)!.queue, { type: "BOOKMARK", object: bookmark("7") });
    await until(async () => (await holds()) === "held", true, "the binding was not confirmed once heard of again");
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
