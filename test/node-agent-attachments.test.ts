import assert from "node:assert/strict";
import { test } from "node:test";

import { added } from "./support/metrics.ts";

import type { KubernetesObject } from "@kubernetes/client-node";
import { type Cause, Effect, Exit, Layer, Queue, Scope, Stream } from "effect";

import { INITIAL_EVENTS_END, KubeApiError, KubeClient, type WatchEvent } from "../src/kube/client.ts";
import { convergeAttachments } from "../src/node-agent/attachments.ts";
import type { Swept } from "../src/node-agent/workspaces.ts";

// What is attached on a node kept to the pods a cluster has, as its agent watches them: each watch
// one the test sends the events of, as an API server would.

/** A pod of `namespace`, on `node`, or on none yet, in its `phase`. */
const pod = (namespace: string, name: string, node?: string, phase = "Running"): KubernetesObject =>
  ({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace }, spec: { ...(node === undefined ? {} : { nodeName: node }), containers: [] }, status: { phase } } as KubernetesObject);
/** The bookmark that ends a watch's list of what there is; and one that does not. */
const listed: WatchEvent = { type: "BOOKMARK", object: { apiVersion: "v1", kind: "Pod", metadata: { annotations: { [INITIAL_EVENTS_END]: "true" } } } };
const bookmark: WatchEvent = { type: "BOOKMARK", object: { apiVersion: "v1", kind: "Pod", metadata: { resourceVersion: "9" } } };

/**
 * The agent's convergence on node-x, its attach root holding directories of `namespaces`, each
 * namespace's watch the last of `watches` made of it, which the test sends to, ends or fails, and
 * each sweep's present pods, by namespace, as it was given them, and what it answers with
 * (`answer`); for as long as `use` runs.
 */
async function converging(
  namespaces: string[],
  use: (at: {
    readonly sweeps: Record<string, string[]>[];
    readonly watch: (namespace: string) => Promise<Queue.Queue<WatchEvent, KubeApiError | Cause.Done>>;
    readonly answer: { waiting: number };
    /** Each namespace's watches, when each was made, and the namespaces each of whose watches was closed. */
    readonly made: Map<string, number[]>;
    readonly closed: Set<string>;
  }) => Promise<void>,
  { everyMs = 60_000, retryMs = 20, retryCapMs = 10_000 }: { readonly everyMs?: number; readonly retryMs?: number; readonly retryCapMs?: number } = {},
): Promise<void> {
  const watches = new Map<string, Queue.Queue<WatchEvent, KubeApiError | Cause.Done>[]>();
  const made = new Map<string, number[]>();
  const closed = new Set<string>();
  const sweeps: Record<string, string[]>[] = [];
  const answer = { waiting: 0 };
  const kube = {
    watch: (_apiVersion: string, plural: string, namespace: string) => {
      assert.equal(plural, "pods");
      return Stream.unwrap(Effect.map(Queue.unbounded<WatchEvent, KubeApiError | Cause.Done>(), (queue) => {
        watches.set(namespace, [...watches.get(namespace) ?? [], queue]);
        made.set(namespace, [...made.get(namespace) ?? [], Date.now()]);
        return Stream.fromQueue(queue).pipe(Stream.ensuring(Effect.sync(() => void closed.add(namespace))));
      }));
    },
  } as unknown as KubeClient["Service"];
  const workspaces = {
    sweep: (present: ReadonlyMap<string, ReadonlySet<string>>) =>
      Effect.sync((): Swept => {
        sweeps.push(Object.fromEntries([...present].map(([namespace, pods]) => [namespace, [...pods].sort()])));
        if (answer.waiting === 0) return { detached: [], waiting: [] };
        answer.waiting -= 1;
        return { detached: [], waiting: ["ns-a/gone-0"] };
      }),
    attachedNamespaces: Effect.sync(() => [...namespaces]),
  };
  const scope = await Effect.runPromise(Scope.make());
  try {
    await Effect.runPromise(convergeAttachments({ node: "node-x", workspaces, everyMs, againMs: 20, againForMs: 5_000, retryMs, retryCapMs }).pipe(
      Effect.provide(Layer.succeed(KubeClient, kube)),
      Scope.provide(scope),
    ));
    const watch = async (namespace: string) => {
      for (const deadline = Date.now() + 5_000; !watches.get(namespace)?.length; await settle()) assert.ok(Date.now() < deadline, `${namespace} was not watched`);
      return watches.get(namespace)?.at(-1) as Queue.Queue<WatchEvent, KubeApiError | Cause.Done>;
    };
    await use({ sweeps, watch, answer, made, closed });
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

const settle = (ms = 30) => new Promise((resolve) => setTimeout(resolve, ms));
const send = (queue: Queue.Queue<WatchEvent, KubeApiError | Cause.Done>, ...events: WatchEvent[]) => Effect.runPromise(Queue.offerAll(queue, events));

/** Waits until `holds`, for five seconds at most. */
async function until(holds: () => boolean, what: string): Promise<void> {
  for (const deadline = Date.now() + 5_000; !holds(); await settle(10)) assert.ok(Date.now() < deadline, what);
}

test("each namespace's pods are swept by once its watch has listed them all: those on the node, and those on none yet, as a restore's waits to be scheduled; again as one is deleted, and while a pod gone waits for its sandbox's end", async () => {
  await converging(["ns-a"], async ({ sweeps, watch, answer }) => {
    const a = await watch("ns-a");
    // One ended for good, which kubelet keeps: gone as any is.
    await send(a, { type: "ADDED", object: pod("ns-a", "pool-0", "node-x") }, { type: "ADDED", object: pod("ns-a", "restore-0", undefined, "Pending") }, { type: "ADDED", object: pod("ns-a", "pool-1", "node-y") }, { type: "ADDED", object: pod("ns-a", "done-0", "node-x", "Succeeded") }, { type: "ADDED", object: pod("ns-a", "failed-0", "node-x", "Failed") }, listed);
    await until(() => sweeps.length === 1, "no sweep ran once the namespace was listed");
    assert.deepEqual(sweeps[0], { "ns-a": ["pool-0", "restore-0"] });
    // Scheduled elsewhere: not this node's.
    await send(a, { type: "MODIFIED", object: pod("ns-a", "restore-0", "node-y") });
    await until(() => sweeps.length === 2, "a pod scheduled elsewhere was not swept as gone from this node");
    answer.waiting = 2;
    await send(a, { type: "DELETED", object: pod("ns-a", "pool-0", "node-x") });
    await until(() => sweeps.length === 5, `the deletion was not swept, then swept again while its pod waited: ${JSON.stringify(sweeps)}`);
    assert.deepEqual(sweeps.slice(2), [{ "ns-a": [] }, { "ns-a": [] }, { "ns-a": [] }]);
    await settle(200);
    assert.equal(sweeps.length, 5);
  });
});

test("nothing is swept before a namespace's watch has listed it whole: not as its list goes on, not as a pod is deleted meanwhile, not at a bookmark that ends no list, not at the sweeps' own time", async () => {
  await converging(["ns-a"], async ({ sweeps, watch }) => {
    const a = await watch("ns-a");
    await send(a, { type: "ADDED", object: pod("ns-a", "pool-0", "node-x") }, { type: "DELETED", object: pod("ns-a", "pool-0", "node-x") }, bookmark);
    await settle(300);
    assert.deepEqual(sweeps, []);
    await send(a, listed);
    await until(() => sweeps.length >= 1, "no sweep ran once the namespace was listed");
    assert.deepEqual(sweeps[0], { "ns-a": [] });
  }, { everyMs: 50 });
});

test("a namespace whose watch ended, or failed, is swept no more until a watch lists it whole again; another listed is swept meanwhile", async () => {
  await converging(["ns-a", "ns-b"], async ({ sweeps, watch }) => {
    const [a, b] = [await watch("ns-a"), await watch("ns-b")];
    await send(a, { type: "ADDED", object: pod("ns-a", "pool-0", "node-x") }, listed);
    await send(b, listed);
    await until(() => sweeps.some((swept) => "ns-a" in swept && "ns-b" in swept), "no sweep ran of both namespaces");
    // Its watch ended, as the API server ends one now and then: known no more, the other swept alone at the sweeps' time.
    await Effect.runPromise(Queue.end(a));
    // Once its end is heard, which a sweep at the sweeps' time may come before: the next sweeps are of the other alone.
    await until(() => sweeps.at(-1) !== undefined && !("ns-a" in (sweeps.at(-1) ?? {})), "the namespace whose watch ended was swept still");
    const ended = sweeps.length;
    await until(() => sweeps.length > ended + 1, "no sweep ran at the sweeps' time");
    assert.ok(sweeps.slice(ended).every((swept) => !("ns-a" in swept) && "ns-b" in swept), JSON.stringify(sweeps.slice(ended)));
    // Watched anew, listed whole again: swept.
    const again = await watch("ns-a");
    assert.notEqual(again, a);
    await send(again, { type: "ADDED", object: pod("ns-a", "pool-0", "node-x") }, listed);
    await until(() => sweeps.at(-1)?.["ns-a"] !== undefined, "the namespace listed again was not swept");
    // Its watch failed: likewise.
    await Effect.runPromise(Queue.fail(again, KubeApiError.of(new Error("watching the pods was refused"))));
    await until(() => sweeps.at(-1) !== undefined && !("ns-a" in (sweeps.at(-1) ?? {})), "the namespace whose watch failed was swept still");
    const failed = sweeps.length;
    await until(() => sweeps.length > failed + 1, "no sweep ran at the sweeps' time");
    assert.ok(sweeps.slice(failed).every((swept) => !("ns-a" in swept)), JSON.stringify(sweeps.slice(failed)));
  }, { everyMs: 50 });
});

test("a namespace's watch ends as the attach root has no directory of it any more, and it is swept no more", async () => {
  const namespaces = ["ns-a", "ns-b"];
  await converging(namespaces, async ({ sweeps, watch, closed }) => {
    const [a, b] = [await watch("ns-a"), await watch("ns-b")];
    await send(a, listed);
    await send(b, listed);
    await until(() => sweeps.some((swept) => "ns-a" in swept && "ns-b" in swept), "no sweep ran of both namespaces");
    namespaces.splice(namespaces.indexOf("ns-b"), 1);
    await until(() => closed.has("ns-b"), "the watch of the namespace with no directory left was not ended");
    await until(() => sweeps.at(-1) !== undefined && !("ns-b" in (sweeps.at(-1) ?? {})), "the namespace with no directory left was swept still");
    const after = sweeps.length;
    await until(() => sweeps.length > after + 1, "no sweep ran at the sweeps' time");
    assert.ok(sweeps.slice(after).every((swept) => !("ns-b" in swept) && "ns-a" in swept), JSON.stringify(sweeps.slice(after)));
    assert.equal(closed.has("ns-a"), false);
  }, { everyMs: 50 });
});

test("each watch that fails is counted, and asked again after twice as long as the one before, up to its cap, until one lists its namespace", async () => {
  await converging(["ns-a"], async ({ watch, made }) => {
    const failed = await added("alasio.node_agent.attachments.watch_failures", async () => {
      for (let failures = 1; failures <= 4; failures += 1) {
        const queue = await watch("ns-a");
        await Effect.runPromise(Queue.fail(queue, KubeApiError.of(Object.assign(new Error("pods is forbidden"), { code: 403 }))));
        await until(() => (made.get("ns-a")?.length ?? 0) > failures, `the watch was not asked again after its failure ${failures}`);
      }
    });
    assert.deepEqual(failed, { "k8s.namespace.name=ns-a": 4 });
    // 40, 80, 160, then capped at 200 ms, each at least as long as it was to be.
    const [, ...times] = made.get("ns-a") ?? [];
    const waits = times.slice(1).map((time, n) => time - (times[n] ?? 0));
    assert.ok(waits.length === 3 && waits.every((wait, n) => wait >= [80, 160, 200][n]! - 15), JSON.stringify(waits));
  }, { retryMs: 40, retryCapMs: 200 });
});
