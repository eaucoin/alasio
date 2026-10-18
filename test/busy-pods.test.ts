/**
 * Pods held busy while turns run in them (src/kube/busy.ts): labelled as the first hold
 * takes them, again while held, and unlabelled as the last lets go, or as alasio stops;
 * alasio's own with every one, where it runs in a pod; what is labelled and not held,
 * from a run before or by a patch that landed late, unlabelled; and held by ActiveTurns,
 * for as long as a turn is registered.
 */
import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";

import { ConfigProvider, Context, Effect, Exit, Layer, Logger, Scope, Stream } from "effect";

import type { RunningTurn } from "../src/harness/active-turns.ts";
import { workspacePodOf } from "../src/harness/index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../src/harness/names.ts";
import { busyPatch, BusyPods } from "../src/kube/busy.ts";
import { KubeApiError, KubeClient } from "../src/kube/client.ts";
import { activeTurnsHolding, type Hold } from "./support/busy-pods.ts";

/** A patch alasio sent: the pod's namespace and name, and the patch. */
type Patched = readonly [string, string, object];

/**
 * The pods of a fake API: the patches it took, the pods not there, those labelled busy
 * (from before, and as its patches label them), and how it answers a patch otherwise:
 * refusing, never, or after `delay`; and the most patches of one pod it has had in
 * flight at once.
 */
interface FakePods {
  readonly patched: Patched[];
  readonly missing?: Set<string>;
  readonly labelled?: Set<string>;
  readonly refusal?: KubeApiError;
  readonly hangs?: boolean;
  readonly delay?: number;
  readonly inFlight?: { most: number };
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Waits until `check` holds, every 10ms for `timeoutMs`. */
async function until(what: string, check: () => boolean, timeoutMs = 2000): Promise<void> {
  for (const deadline = Date.now() + timeoutMs; !check(); await sleep(10)) if (Date.now() > deadline) assert.fail(`${what}, not within ${timeoutMs}ms`);
}

/**
 * BusyPods on a KubeClient of `pods`, in a pod `env` names, of the namespace
 * alasio-sessions besides its own, labelling a held pod again every 50ms and sweeping
 * every 100ms, for the test; and what stops it, as alasio stopping does.
 */
async function busyPods(
  t: TestContext,
  env: Record<string, string>,
  { patched, missing = new Set(), labelled = new Set(), refusal, hangs = false, delay = 0, inFlight = { most: 0 } }: FakePods,
): Promise<BusyPods["Service"] & { readonly stop: () => Promise<void> }> {
  const unused = () => Effect.die("not called");
  const flying = new Map<string, number>();
  const kube = KubeClient.of({
    read: unused,
    create: unused,
    replace: unused,
    remove: unused,
    exec: unused,
    watch: () => Stream.die("not called"),
    patchPodStatus: unused,
    list: (apiVersion, kind, namespace, labelSelector) => {
      assert.deepEqual([apiVersion, kind, labelSelector], ["v1", "Pod", "alasio.dev/busy=true"]);
      return Effect.succeed([...labelled].filter((pod) => pod.startsWith(`${namespace}/`)).map((pod) => ({ metadata: { name: pod.slice(namespace.length + 1) } })));
    },
    patch: (apiVersion, kind, namespace, name, patch) => {
      assert.deepEqual([apiVersion, kind], ["v1", "Pod"]);
      if (hangs) return Effect.never;
      if (refusal) return Effect.fail(refusal);
      if (missing.has(`${namespace}/${name}`)) return Effect.fail(new KubeApiError({ status: 404, cause: new Error(`pods "${name}" not found`) }));
      const pod = `${namespace}/${name}`;
      return Effect.acquireUseRelease(
        Effect.sync(() => {
          flying.set(pod, (flying.get(pod) ?? 0) + 1);
          inFlight.most = Math.max(inFlight.most, flying.get(pod) ?? 0);
        }),
        () =>
          Effect.andThen(
            Effect.sleep(delay),
            Effect.sync(() => {
              patched.push([namespace, name, patch]);
              if (JSON.stringify(patch) === JSON.stringify(busyPatch(true))) labelled.add(pod);
              else labelled.delete(pod);
            }),
          ),
        () => Effect.sync(() => void flying.set(pod, (flying.get(pod) ?? 1) - 1)),
      ).pipe(Effect.as({}));
    },
  });
  const scope = Scope.makeUnsafe();
  t.after(() => Effect.runPromise(Scope.close(scope, Exit.void)));
  const layer = BusyPods.layer({ namespaces: ["alasio-sessions"], refresh: "50 millis", sweep: "100 millis" }).pipe(
    Layer.provide(Layer.succeed(KubeClient, kube)),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))),
    Layer.orDie,
  );
  const busy = Context.get(await Effect.runPromise(Layer.build(layer).pipe(Scope.provide(scope))), BusyPods);
  return { ...busy, stop: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

const WORKSPACE = { namespace: "alasio-sessions", name: "s1" };
const OWN = { ALASIO_POD: "alasio-7c99", ALASIO_NAMESPACE: "alasio" };
const busyOf = (patched: readonly Patched[], busy: boolean) => patched.filter(([, , patch]) => JSON.stringify(patch) === JSON.stringify(busyPatch(busy))).map(([namespace, name]) => `${namespace}/${name}`);
/** Whether the pod's last patch labelled it busy. */
const lastBusy = (patched: readonly Patched[], pod: string) =>
  JSON.stringify(patched.findLast(([namespace, name]) => `${namespace}/${name}` === pod)?.[2]) === JSON.stringify(busyPatch(true));

test("alasio's pod and a turn's workspace are labelled busy as the first hold takes them, again while held, and unlabelled as the last lets go", async (t) => {
  const patched: Patched[] = [];
  const busy = await busyPods(t, OWN, { patched });
  const first = Scope.makeUnsafe();
  const second = Scope.makeUnsafe();
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(first)));
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(second)));
  await until("both labelled", () => busyOf(patched, true).includes("alasio/alasio-7c99") && busyOf(patched, true).includes("alasio-sessions/s1"));
  await sleep(200);
  assert.ok(busyOf(patched, true).filter((pod) => pod === "alasio-sessions/s1").length >= 3, "labelled again while held");
  await Effect.runPromise(Scope.close(first, Exit.void));
  await sleep(100);
  assert.deepEqual(busyOf(patched, false), [], "the other hold keeps them busy");
  await Effect.runPromise(Scope.close(second, Exit.void));
  await until("both unlabelled", () => busyOf(patched, false).length === 2);
  assert.deepEqual(busyOf(patched, false).toSorted(), ["alasio-sessions/s1", "alasio/alasio-7c99"]);
  const after = patched.length;
  await sleep(200);
  assert.equal(patched.length, after, "labelled no more");
  assert.deepEqual(busyPatch(true), {
    metadata: {
      labels: { "alasio.dev/busy": "true" },
      annotations: { "cluster-autoscaler.kubernetes.io/safe-to-evict": "false", "karpenter.sh/do-not-disrupt": "true" },
    },
  });
  // A merge patch's null takes a key off.
  assert.deepEqual(busyPatch(false), {
    metadata: { labels: { "alasio.dev/busy": null }, annotations: { "cluster-autoscaler.kubernetes.io/safe-to-evict": null, "karpenter.sh/do-not-disrupt": null } },
  });
});

test("a pod's patches go one at a time, the last as its holds come to, however close a release and the next hold are", async (t) => {
  const patched: Patched[] = [];
  const inFlight = { most: 0 };
  // Each patch takes as long as several holds and releases do.
  const busy = await busyPods(t, {}, { patched, delay: 40, inFlight });
  for (let round = 0; round < 5; round += 1) {
    const scope = Scope.makeUnsafe();
    await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(scope)));
    await sleep(round * 15);
    // Released, and held again by the next turn at once.
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
  const last = Scope.makeUnsafe();
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(last)));
  await sleep(300);
  assert.ok(lastBusy(patched, "alasio-sessions/s1"), "busy, as the last hold holds it");
  await Effect.runPromise(Scope.close(last, Exit.void));
  await until("unlabelled once let go", () => !lastBusy(patched, "alasio-sessions/s1"));
  const after = patched.length;
  await sleep(200);
  assert.equal(patched.length, after, "labelled no more");
  assert.equal(inFlight.most, 1);
});

test("a workspace's pod not there yet as its turn takes the hold is labelled once it is", async (t) => {
  const patched: Patched[] = [];
  const missing = new Set(["alasio-sessions/s1"]);
  const busy = await busyPods(t, {}, { patched, missing });
  const scope = Scope.makeUnsafe();
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(scope)));
  await sleep(100);
  assert.deepEqual(patched, []);
  missing.clear();
  await until("labelled", () => busyOf(patched, true).includes("alasio-sessions/s1"));
  await Effect.runPromise(Scope.close(scope, Exit.void));
});

test("what a run of alasio's left labelled busy is unlabelled as the next starts, and a label that lands late as the next sweep comes", async (t) => {
  const patched: Patched[] = [];
  const labelled = new Set(["alasio/alasio-0ld", "alasio-sessions/s0", "elsewhere/not-alasio's"]);
  const busy = await busyPods(t, OWN, { patched, labelled });
  await until("the run before's unlabelled", () => busyOf(patched, false).length === 2);
  assert.deepEqual(busyOf(patched, false).toSorted(), ["alasio-sessions/s0", "alasio/alasio-0ld"]);
  // A hold's label, given up on, lands after the hold's release unlabelled the pod.
  await Effect.runPromise(Effect.scoped(busy.hold(WORKSPACE)));
  await until("released", () => busyOf(patched, false).includes("alasio-sessions/s1"));
  labelled.add("alasio-sessions/s1");
  await until("the late label taken off", () => !labelled.has("alasio-sessions/s1"));
  assert.ok(!lastBusy(patched, "alasio-sessions/s1"));
  // One held is the hold's to keep, whatever a sweep finds.
  const scope = Scope.makeUnsafe();
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(scope)));
  await sleep(300);
  assert.ok(lastBusy(patched, "alasio-sessions/s1"));
  await Effect.runPromise(Scope.close(scope, Exit.void));
});

test("as alasio stops, the pods it holds are unlabelled", async (t) => {
  const patched: Patched[] = [];
  const busy = await busyPods(t, OWN, { patched });
  await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(Scope.makeUnsafe())));
  await until("labelled", () => lastBusy(patched, "alasio-sessions/s1") && lastBusy(patched, "alasio/alasio-7c99"));
  await busy.stop();
  assert.ok(!lastBusy(patched, "alasio-sessions/s1") && !lastBusy(patched, "alasio/alasio-7c99"));
  const after = patched.length;
  await sleep(200);
  assert.equal(patched.length, after, "labelled no more");
});

test("a label the API refuses, or does not answer in time, is logged, and the turn goes on", async (t) => {
  const logged: string[] = [];
  const logging = Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))]));
  /** Holds the workspace with `busy`, until the label's patch has been logged, then lets go: how long the hold and its release took. */
  const heldUntilLogged = async (busy: BusyPods["Service"], timeoutMs: number) => {
    const scope = Scope.makeUnsafe();
    let took = Date.now();
    await Effect.runPromise(busy.hold(WORKSPACE).pipe(Scope.provide(scope), logging));
    took = Date.now() - took;
    await until("the label logged", () => logged.length === 1, timeoutMs);
    const releasing = Date.now();
    await Effect.runPromise(Scope.close(scope, Exit.void));
    return took + Date.now() - releasing;
  };
  const refusing = await busyPods(t, {}, { patched: [], refusal: new KubeApiError({ status: 403, cause: new Error("pods is forbidden") }) });
  await heldUntilLogged(refusing, 2000);
  await until("its removal logged", () => logged.length === 2);
  assert.deepEqual(logged, ["could not label the pod alasio-sessions/s1 busy: pods is forbidden", "could not unlabel the pod alasio-sessions/s1 busy: pods is forbidden"]);

  logged.length = 0;
  const hanging = await busyPods(t, {}, { patched: [], hangs: true });
  assert.ok(await heldUntilLogged(hanging, 10_000) < 1000, "the turn waited on neither patch");
  await until("its removal logged", () => logged.length === 2, 10_000);
  assert.deepEqual(logged.map((line) => line.split(":")[0]), ["could not label the pod alasio-sessions/s1 busy", "could not unlabel the pod alasio-sessions/s1 busy"]);
});

test("a conversation's turns hold its folder's bayma's pod under the harness busy, where the service that runs it is there, and none of a session's, which the gateway holds busy itself", () => {
  const hostBayma = { pod: ({ harness, threadKey }: { harness: string; threadKey: string }) => ({ namespace: "alasio-host", name: `${harness}-${threadKey}` }) };
  const session = { harness: CODEX_HARNESS, workingDirectory: "sessionfs:session-1", threadKey: "telegram:1" } as const;
  const folder = { harness: CLAUDE_HARNESS, workingDirectory: "/home/operator/repo", threadKey: "telegram:1" } as const;
  assert.equal(workspacePodOf(session, hostBayma), null);
  assert.deepEqual(workspacePodOf(folder, hostBayma), { namespace: "alasio-host", name: "claude-telegram:1" });
  assert.equal(workspacePodOf(folder, null), null);
});

test("a turn registered holds its pods until it ends, and one not registered, its conversation busy already, holds none", async () => {
  const holds: Hold[] = [];
  const activeTurns = activeTurnsHolding(holds);
  const turn: RunningTurn = { stop: () => Effect.void, steer: () => Effect.succeed(false), cliInitiated: false };
  const registration = Scope.makeUnsafe();
  await Effect.runPromise(activeTurns.register("telegram:1", turn, { workspace: WORKSPACE }).pipe(Scope.provide(registration)));
  await Effect.runPromise(Effect.scoped(activeTurns.register("telegram:1", { ...turn, cliInitiated: true }, { onlyIfIdle: true, workspace: WORKSPACE })));
  assert.deepEqual(holds, [{ workspace: WORKSPACE, released: false }]);
  await Effect.runPromise(Scope.close(registration, Exit.void));
  assert.deepEqual(holds, [{ workspace: WORKSPACE, released: true }]);
});
