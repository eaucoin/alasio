import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Deployment, V1Pod, V1PodSpec } from "@kubernetes/client-node";
import { Effect, Layer, Logger } from "effect";

import { added } from "./support/metrics.ts";
import { Binder } from "../src/binder/binder.ts";
import { ACTIVE_ANNOTATION, type BindingLease, readLease, touchLease } from "../src/binder/bindings.ts";
import { freeLabels, type Pool } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import type { EventInput } from "../src/events/events.ts";
import type { Environments, SessionsEnvironment } from "../src/gateway/environments.ts";
import { harnessIdleness, type HarnessesConfig, makeHarnesses } from "../src/gateway/harnesses.ts";
import { harnessKey } from "../src/gateway/names.ts";
import { branchNamespace } from "../src/branch/names.ts";
import { RELEASES } from "../src/gateway/releases.ts";
import type { HarnessStatus } from "../src/harness/host.ts";
import { HOST_PATHS } from "../src/harness/wire.ts";
import { KubeClient } from "../src/kube/client.ts";
import { type BindingRequest, NodeAgentError, type NodeAgents } from "../src/sandbox/node-agents.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";
import { eventually } from "./support/wait.ts";

// The gateway's conversations' harnesses on a real API server, each pool's pods at the
// loopback's address, where a stand-in for a harness host answers as the test says, and the
// node agents a stand-in recording what they are asked.

const bindingKey = randomBytes(32);

const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);
const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
});

const IDLE: HarnessStatus = { busy: false, running: false, cliInitiated: false, held: false, idleSince: 0 };

/** A stand-in for a bound harness host: what it says of its conversation now, what it was asked, by whose token, how long its drain takes, and whether something goes on in it again as it is drained. */
async function host(): Promise<{ port: number; status: HarnessStatus; working: boolean; drainMs: number; busyAgain: boolean; asked: string[]; bearers: string[]; close: () => void }> {
  const stand = { status: IDLE, working: false, drainMs: 0, busyAgain: false, asked: [] as string[], bearers: [] as string[] };
  const server = createServer((incoming, outgoing) => {
    stand.asked.push(incoming.url ?? "");
    stand.bearers.push(String(incoming.headers.authorization));
    const answer = (body: object) => outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(body));
    incoming.resume();
    if (incoming.url === HOST_PATHS.status) answer(stand.status);
    else if (incoming.url === HOST_PATHS.backgroundWork) answer({ working: stand.working });
    else if (incoming.url === HOST_PATHS.drain) setTimeout(() => answer({ drained: !stand.busyAgain }), stand.drainMs);
    else outgoing.writeHead(404).end();
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return Object.assign(stand, { port: (server.address() as AddressInfo).port, close: () => {
    server.closeAllConnections();
    server.close();
  } });
}

/** Node agents that record the bindings given, and the detaches asked; the first binding failing where `fails` says. */
function agents({ fails = false } = {}): { agents: NodeAgents; bound: (BindingRequest & { node: string })[]; detached: string[] } {
  const bound: (BindingRequest & { node: string })[] = [];
  const detached: string[] = [];
  let failing = fails;
  const unused = () => Effect.die("not asked");
  return {
    bound,
    detached,
    agents: {
      nodes: Effect.succeed(["node-a"]),
      current: unused, released: unused, attachments: unused, attach: unused, checkpoint: unused, stop: unused, setReleased: unused,
      prepare: unused, copy: unused, forget: unused, resume: unused, holdings: unused, workspaces: unused, discard: unused, spend: unused, collect: unused, abandon: unused,
      readFile: unused, writeFile: unused, createWorkspace: unused, cloneWorkspace: unused, removeWorkspace: unused, listWorkspaces: unused, listEnvironments: unused(), quota: unused, sync: unused,
      bindFree: (node, request) =>
        failing ? ((failing = false), Effect.fail(new NodeAgentError({ node, call: "bind", status: 409, reason: "it has its binding already" }))) : Effect.sync(() => void bound.push({ node, ...request })),
      detach: (node, namespace, pod) => Effect.sync(() => void detached.push(`${node} ${namespace}/${pod}`)),
    },
  };
}

let made = 0;
/** main's environment of the test's own: its own namespace, its sessions', and `count` free pods of its Codex pool on node-a, at the loopback. */
async function environment(count: number): Promise<SessionsEnvironment & { home: string; codex: Pool }> {
  made += 1;
  const home = `alasio-${made}`;
  const codex: Pool = { name: "harness-codex", namespace: home };
  await run(withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: home } });
      const spec = { containers: [{ name: "harness", image: "alasio" }] } as V1PodSpec;
      yield* kube.create<V1Deployment>({ apiVersion: "apps/v1", kind: "Deployment", metadata: { name: codex.name, namespace: home }, spec: { replicas: 0, selector: { matchLabels: { ...freeLabels(codex) } }, template: { metadata: { labels: { ...freeLabels(codex) } }, spec } } });
      for (let n = 0; n < count; n += 1) yield* freePod(kube, codex, `codex-${n}`, spec);
    })
  ));
  return { name: "main", namespace: `alasio-${made}-sessions`, parent: null, home, codex };
}

/** A free pod `name` of `pool`, running on node-a at the loopback, ready. */
const freePod = (kube: KubeClient["Service"], pool: Pool, name: string, spec: V1PodSpec) =>
  Effect.gen(function*() {
    yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace: pool.namespace, labels: { ...freeLabels(pool) } }, spec: { nodeName: "node-a", ...spec } });
    yield* kube.patchPodStatus(pool.namespace, name, {
      status: { phase: "Running", podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "harness", image: "alasio", imageID: "", ready: true, restartCount: 0, state: { running: {} } }] },
    });
  });

/** `pod`'s host ended, as kubelet says it once its container stops. */
const stopped = (namespace: string, pod: string, restartCount = 0) =>
  run(withKube((kube) => kube.patchPodStatus(namespace, pod, { status: { containerStatuses: [{ name: "harness", image: "alasio", imageID: "", ready: false, restartCount, state: { terminated: { exitCode: 1 } } }] } })));

/** Ends `pod` once its deletion is asked, as kubelet would, its host's container stopped. */
const endsOnceDeleted = (namespace: string, pod: string) =>
  (async () => {
    for (;;) {
      const read = await run(withKube((kube) => kube.read("v1", "Pod", namespace, pod))) as V1Pod | null;
      if (!read) return;
      if (read.metadata?.deletionTimestamp) return stopped(namespace, pod);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();

/** The environments `list`, as found: those alone. */
const found = (list: readonly SessionsEnvironment[]): Environments => ({
  current: Effect.succeed(list),
  named: (name) => Effect.succeed(list.find((each) => each.name === name) ?? null),
  ofNamespace: (namespace) => Effect.succeed(list.find((each) => each.namespace === namespace) ?? null),
});

/** What the harnesses told of, as each environment's alasio reads it. */
const told: EventInput[] = [];

/** The harnesses' configuration, main's pools in `home`, its hosts at `podPort`. */
const configOf = (home: string, podPort: number, overrides: Partial<HarnessesConfig> = {}): HarnessesConfig => ({
  namespace: home,
  pools: { codex: "harness-codex", claude: "harness-claude" },
  podPort,
  owner: { uid: 1000, gid: 1000 },
  releaseAfterSeconds: 60,
  drainSeconds: 120,
  folders: { pools: { codex: "harness-folder-codex", claude: "harness-folder-claude" }, owner: { uid: 1001, gid: 1002 } },
  ...overrides,
});

/** The gateway's harnesses and its Binder, on clients of their own, of main's environment `env` and the branch `try`. */
const harnessesOf = (env: SessionsEnvironment & { home: string }, config: HarnessesConfig, nodeAgents: NodeAgents, idlePollMs?: number, unreachedMs?: number) =>
  Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const binder = yield* Binder;
    const environments = found([env, { name: "try", namespace: "alasio-branch-try-sessions", parent: "main" }]);
    const tell = (event: EventInput) => Effect.sync(() => void told.push(event));
    const events = { tell, send: tell };
    return { binder, harnesses: yield* makeHarnesses({ config, environments, main: "main", bindingKey, events, ...(idlePollMs === undefined ? {} : { idlePollMs }), ...(unreachedMs === undefined ? {} : { unreachedMs }) }, nodeAgents, binder) };
  }), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));

const sweep = (harnesses: { readonly sweep: Effect.Effect<readonly string[], unknown, never> }) => Effect.runPromise(harnesses.sweep);

test("a harness is idle once nothing goes on in it and nothing has for its wait, as its last turn, its last work seen in the background, the last call of it and its binding say; asked again no sooner than it could be", () => {
  const lease: BindingLease = { metadata: { annotations: { [ACTIVE_ANNOTATION]: "5000" } } };
  const pod = { metadata: { creationTimestamp: new Date(1_000) } } as V1Pod;
  const timing = { releaseAfterMs: 10_000, idlePollMs: 1_000 };
  const idle = (status: Partial<HarnessStatus>, backgroundWork = false, now = 100_000, workSeenAt?: number) => harnessIdleness({ status: { ...IDLE, ...status }, backgroundWork }, lease, pod, now, timing, workSeenAt);
  for (const busy of [{ busy: true }, { running: true }, { cliInitiated: true }, { held: true }, { idleSince: null }]) assert.deepEqual(idle(busy), { idle: false, askAt: 101_000 }, JSON.stringify(busy));
  assert.deepEqual(idle({}, true), { idle: false, askAt: 101_000 });
  assert.deepEqual(idle({ idleSince: 2_000 }), { idle: true });
  // Its last call, the Lease's stamp, later than its last turn: kept until its wait from it.
  assert.deepEqual(idle({ idleSince: 2_000 }, false, 8_000), { idle: false, askAt: 15_000 });
  assert.deepEqual(idle({ idleSince: 2_000 }, false, 14_500), { idle: false, askAt: 15_500 });
  // Its work in the background last seen later than all of those: kept until its wait from it.
  assert.deepEqual(idle({ idleSince: 2_000 }, false, 95_000, 90_000), { idle: false, askAt: 100_000 });
  assert.deepEqual(idle({ idleSince: 2_000 }, false, 100_000, 90_000), { idle: true });
});

test("a conversation's harness is placed in its environment's own namespace's pool of its harness, a folder's in main's, main's alone, and nothing else is", async () => {
  const env = await environment(0);
  const { harnesses } = await harnessesOf(env, configOf(env.home, 8080), agents().agents);
  const key = harnessKey("telegram:41");
  const placed = (options: Record<string, string>, of = key) =>
    Effect.runPromise(Effect.result(harnesses.place(of, options))).then((result) => (result._tag === "Failure" ? result.failure.message : result.success.pool));
  assert.deepEqual(await placed({ environment: "main", harness: "codex", workspace: "session" }), { name: "harness-codex", namespace: env.home });
  assert.deepEqual(await placed({ environment: "try", harness: "claude", workspace: "session" }), { name: "harness-claude", namespace: "alasio-branch-try" });
  assert.deepEqual(await placed({ environment: "main", harness: "claude", workspace: "folder" }), { name: "harness-folder-claude", namespace: env.home });
  assert.equal(await placed({ environment: "try", harness: "claude", workspace: "folder" }), "a folder is the machine's own files, of main's alone where it has folder workspaces, not try's");
  assert.equal(await placed({ environment: "elsewhere", harness: "codex", workspace: "session" }), "there is no environment elsewhere");
  assert.equal(await placed({ environment: "main", harness: "gemini", workspace: "session" }), "there is no harness gemini");
  assert.equal(await placed({ environment: "main", harness: "codex", workspace: "volume" }), "a conversation's workspace is a session or a folder, not volume");
  assert.equal(await placed({ environment: "main", harness: "codex", workspace: "session" }, "sessionfs:fs-abc123"), "sessionfs:fs-abc123 is no conversation's harness's");
  // Without folder workspaces, no folder conversation's.
  const { harnesses: unfoldered } = await harnessesOf(env, configOf(env.home, 8080, { folders: null }), agents().agents);
  const refused = await Effect.runPromise(Effect.flip(unfoldered.place(key, { environment: "main", harness: "claude", workspace: "folder" })));
  assert.equal(refused.message, "a folder is the machine's own files, of main's alone where it has folder workspaces, not main's");
  // Its pools, of every environment that has them: main's; a branch made before they were turned on has none, nor the gateway's Role there, and is passed over, said so.
  const logged: string[] = [];
  const withLog = Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))]);
  assert.deepEqual(await Effect.runPromise(harnesses.namespaces.pipe(Effect.provide(withLog))), [env.home]);
  assert.deepEqual(logged.filter((line) => line.includes("has no harnesses' pools")), [
    "the branch environment try has no harnesses' pools, as it was made before they were turned on: its conversations' harnesses are bound nowhere until it is made again (alasio branch delete try, then alasio branch create try)",
  ]);
  // Made again, with them: its pools too.
  await run(withKube((kube) => kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: "alasio-branch-try" } })));
  const spec = { containers: [{ name: "harness", image: "alasio" }] } as V1PodSpec;
  const tried: Pool = { name: "harness-codex", namespace: "alasio-branch-try" };
  await run(withKube((kube) => kube.create<V1Deployment>({ apiVersion: "apps/v1", kind: "Deployment", metadata: { name: tried.name, namespace: tried.namespace }, spec: { replicas: 0, selector: { matchLabels: { ...freeLabels(tried) } }, template: { metadata: { labels: { ...freeLabels(tried) } }, spec } } })));
  const { harnesses: again } = await harnessesOf(env, configOf(env.home, 8080), agents().agents);
  assert.deepEqual(await Effect.runPromise(again.namespaces), [env.home, "alasio-branch-try"]);
  assert.deepEqual(await Promise.all([env.home, "alasio-branch-try", "alasio-branch-gone", env.namespace].map((namespace) => Effect.runPromise(harnesses.ownsNamespace(namespace)))), [true, true, false, false]);
});

test("a harness's pod is readied by its node's agent giving it its binding, the conversation and its binding's token, as its pool's user; one whose readying failed is removed once its containers stopped", async () => {
  const env = await environment(2);
  const recorded = agents({ fails: true });
  const { binder, harnesses } = await harnessesOf(env, configOf(env.home, 8080), recorded.agents);
  const key = harnessKey("telegram:41");
  const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
  // The first refused by its agent: the bind fails, its pod deleted, and what was written for it removed once it stopped.
  const failed = await Effect.runPromise(Effect.result(binder.bind(placement.pool, key, placement)));
  assert.equal(failed._tag, "Failure");
  const [deleted] = ((await run(withKube((kube) => kube.list("v1", "Pod", env.home, "")))) as V1Pod[]).filter(({ metadata }) => metadata?.deletionTimestamp).map(({ metadata }) => metadata?.name ?? "");
  assert.ok(deleted, "no pod was deleted");
  await stopped(env.home, deleted);
  for (const deadline = Date.now() + 10_000; !recorded.detached.includes(`node-a ${env.home}/${deleted}`); await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, recorded.detached.join("\n"));
  // The next, given its binding.
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  const [given] = recorded.bound;
  assert.deepEqual([given?.node, given?.namespace, given?.pod, given?.uid, given?.gid], ["node-a", env.home, bound.pod, 1000, 1000]);
  const binding = JSON.parse(given?.binding ?? "{}") as { conversationId: string; token: string };
  assert.equal(binding.conversationId, "telegram:41");
  assert.deepEqual(verifyToken(bindingKey, binding.token, Date.now()), { key, binding: bound.binding, namespace: env.home });
});

test("a bound harness is kept while anything goes on in it, its host asked with its binding's token no oftener than its poll; once idle for its wait, drained, its pod ended, what was written for it removed, and told of", async () => {
  const env = await environment(1);
  const stand = await host();
  const recorded = agents();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 4 }), recorded.agents, 2_000);
    const key = harnessKey("telegram:41");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    // A turn runs: kept, its background work not asked.
    stand.status = { ...IDLE, busy: true, running: true, idleSince: null };
    assert.deepEqual(await sweep(harnesses), []);
    assert.deepEqual(await sweep(harnesses), []);
    assert.deepEqual(stand.asked, [HOST_PATHS.status]);
    assert.deepEqual(verifyToken(bindingKey, stand.bearers[0]?.slice("Bearer ".length) ?? "", Date.now()), { key, binding: bound.binding, namespace: env.home });
    // Its harness works on its own: kept.
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    stand.status = { ...IDLE, idleSince: Date.now() - 600_000 };
    stand.working = true;
    assert.deepEqual(await sweep(harnesses), []);
    assert.deepEqual(stand.asked.slice(1), [HOST_PATHS.status, HOST_PATHS.backgroundWork]);
    // Its last call within its wait: kept.
    stand.working = false;
    await run(touchLease(env.codex, key, Date.now()));
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    assert.deepEqual(await sweep(harnesses), []);
    await new Promise((resolve) => setTimeout(resolve, 4_000));
    const ending = endsOnceDeleted(env.home, bound.pod);
    let released: readonly string[] = [];
    const counted = await added(RELEASES, async () => (released = await sweep(harnesses)));
    await ending;
    assert.deepEqual(released, [key]);
    assert.ok(stand.asked.some((path: string) => path === HOST_PATHS.drain));
    assert.deepEqual(counted, { [`k8s.namespace.name=${env.home},alasio.gateway.release.reason=idle`]: 1 });
    assert.deepEqual(recorded.detached, [`node-a ${env.home}/${bound.pod}`]);
    assert.equal(await run(readLease(env.codex, key)), null);
    assert.deepEqual(told.filter((event) => event.subject.key === "telegram:41" && event.kind === "harness.released" && event.binding === bound.binding), [
      { kind: "harness.released", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:41" }, binding: bound.binding, reason: "idle" },
    ]);
  } finally {
    stand.close();
  }
});

test("a bound harness whose work in the background ends is kept its whole wait from the last sweep that found it working, however long ago its last turn ended", async () => {
  const env = await environment(1);
  const stand = await host();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 2 }), agents().agents, 100);
    const key = harnessKey("telegram:46");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    const ending = endsOnceDeleted(env.home, bound.pod);
    /** Kept by a sweep now: none released, its pod not ended. */
    const kept = async (why: string) => {
      assert.deepEqual(await sweep(harnesses), [], why);
      assert.equal(((await run(withKube((kube) => kube.read("v1", "Pod", env.home, bound.pod)))) as V1Pod | null)?.metadata?.deletionTimestamp, undefined, why);
    };
    // Its last turn long over, past its binding's making and its last call, its command in the background runs on: kept.
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    stand.status = { ...IDLE, idleSince: Date.now() - 600_000 };
    stand.working = true;
    await kept("its command runs in the background");
    const seen = Date.now();
    // Its command done: kept, as idle since then, not since its last turn.
    stand.working = false;
    await new Promise((resolve) => setTimeout(resolve, 150));
    await kept("its command has just ended");
    await new Promise((resolve) => setTimeout(resolve, 1_000));
    await kept("its command ended within its wait");
    // Its wait from then passed: released.
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, seen + 2_100 - Date.now())));
    assert.deepEqual(await sweep(harnesses), [key]);
    await ending;
  } finally {
    stand.close();
  }
});

test("a pod the gateway ends as it releases its binding is not told of as ending though it did not end it", async () => {
  const env = await environment(1);
  const stand = await host();
  const recorded = agents();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 0 }), recorded.agents, 100);
    const key = harnessKey("telegram:44");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    // Idle: released, its pod ending as the gateway ended it, while what looks for pods ending looks.
    const releasing = sweep(harnesses);
    for (const deadline = Date.now() + 10_000; !((await run(withKube((kube) => kube.read("v1", "Pod", env.home, bound.pod)))) as V1Pod | null)?.metadata?.deletionTimestamp; await new Promise((resolve) => setTimeout(resolve, 50))) {
      assert.ok(Date.now() < deadline, "the idle pod was not ended");
    }
    const logged: string[] = [];
    await Effect.runPromise(harnesses.live.pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))]))));
    await stopped(env.home, bound.pod);
    assert.deepEqual(await releasing, [key]);
    assert.deepEqual(logged.filter((line) => line.includes("though the gateway did not end it")), []);
    assert.deepEqual(told.filter((event) => event.subject.key === "telegram:44").map((event) => (event.kind === "harness.released" ? event.reason : event.kind)), ["idle"]);
  } finally {
    stand.close();
  }
});

test("a bound harness whose host something goes on in again as it is drained is kept, its release given up, and told of by none", async () => {
  const env = await environment(1);
  const stand = await host();
  const recorded = agents();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 0 }), recorded.agents, 100);
    const key = harnessKey("telegram:45");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.busyAgain = true;
    assert.deepEqual(await sweep(harnesses), []);
    assert.ok(stand.asked.includes(HOST_PATHS.drain));
    const lease = await run(readLease(env.codex, key));
    assert.deepEqual([lease?.spec?.holderIdentity, lease?.metadata?.annotations?.["alasio.dev/releasing"]], [bound.pod, undefined]);
    assert.equal(((await run(withKube((kube) => kube.read("v1", "Pod", env.home, bound.pod)))) as V1Pod | null)?.metadata?.deletionTimestamp, undefined);
    assert.deepEqual(told.filter((event) => event.subject.key === "telegram:45"), []);
    // Idle again: released.
    stand.busyAgain = false;
    const ending = endsOnceDeleted(env.home, bound.pod);
    assert.deepEqual(await sweep(harnesses), [key]);
    await ending;
  } finally {
    stand.close();
  }
});

test("a bound harness whose host is not reached is kept; one whose drain does not end in time is let go of all the same", async () => {
  const env = await environment(1);
  const stand = await host();
  const recorded = agents();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 0, drainSeconds: 1 }), recorded.agents, 100);
    const key = harnessKey("telegram:42");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.close();
    assert.deepEqual(await sweep(harnesses), []);
    const again = await host();
    try {
      const { harnesses: reaching } = await harnessesOf(env, configOf(env.home, again.port, { releaseAfterSeconds: 0, drainSeconds: 1 }), recorded.agents, 100);
      again.drainMs = 5_000;
      const ending = endsOnceDeleted(env.home, bound.pod);
      const started = Date.now();
      assert.deepEqual(await sweep(reaching), [key]);
      await ending;
      assert.ok(Date.now() - started < 4_000, "the release waited out the drain");
      assert.equal(await run(readLease(env.codex, key)), null);
    } finally {
      again.close();
    }
  } finally {
    stand.close();
  }
});

test("a bound harness whose host cannot be asked is kept, said so as it first fails and as why changes; once it has answered no sweep for its bound, its pod is ended, undrained, and its binding let go of as lost, told why", async () => {
  const env = await environment(1);
  const stand = await host();
  const recorded = agents();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port, { releaseAfterSeconds: 0 }), recorded.agents, 100, 1_500);
    const key = harnessKey("telegram:43");
    const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.close();
    const logged: string[] = [];
    const sweepLogged = () => Effect.runPromise(harnesses.sweep.pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => void logged.push(String(message)))]))));
    // Missed once, and again for the same reason: kept, said so once.
    assert.deepEqual(await sweepLogged(), []);
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.deepEqual(await sweepLogged(), []);
    assert.ok(await run(readLease(env.codex, key)), "the binding was let go of");
    assert.deepEqual(logged.filter((line) => line.includes("could not be asked whether it is idle")).length, 1, logged.join("\n"));
    // Past its bound: its pod ended, undrained, what was written for it removed, and told of as lost.
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    const ending = endsOnceDeleted(env.home, bound.pod);
    assert.deepEqual(await sweepLogged(), []);
    await ending;
    for (const deadline = Date.now() + 10_000; await run(readLease(env.codex, key)); await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, `${key} was not released`);
    assert.equal(stand.asked.includes(HOST_PATHS.drain), false);
    for (const deadline = Date.now() + 10_000; !recorded.detached.includes(`node-a ${env.home}/${bound.pod}`); await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, recorded.detached.join("\n"));
    // Told once its release is done, which its detach is a step of.
    const lost = await eventually("the binding told of as lost", () => {
      const found = told.filter((event) => event.kind === "harness.lost" && event.subject.key === "telegram:43");
      return found.length > 0 ? found : undefined;
    });
    assert.equal(lost.length, 1);
    assert.deepEqual({ ...lost[0], reason: undefined }, { kind: "harness.lost", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:43" }, binding: bound.binding, pod: bound.pod, reason: undefined });
    assert.match(lost[0]?.kind === "harness.lost" ? lost[0].reason : "", /^its host answered no sweep for 1\.5 s: /u);
  } finally {
    stand.close();
  }
});

test("a bound pod whose host ended, or was started again in it, is released as broken, undrained; one ended by other than the gateway, as deleted; and a binding lost, told of", async () => {
  const env = await environment(3);
  const recorded = agents();
  const stand = await host();
  try {
    const { binder, harnesses } = await harnessesOf(env, configOf(env.home, stand.port), recorded.agents);
    const placed = async (conversation: string) => {
      const key = harnessKey(conversation);
      const placement = await Effect.runPromise(harnesses.place(key, { environment: "main", harness: "codex", workspace: "session" }));
      return { key, bound: await Effect.runPromise(binder.bind(placement.pool, key, placement)) };
    };
    const ended = await placed("telegram:51");
    const restarted = await placed("telegram:52");
    const deleted = await placed("telegram:53");
    await stopped(env.home, ended.bound.pod);
    await run(withKube((kube) => kube.patchPodStatus(env.home, restarted.bound.pod, { status: { containerStatuses: [{ name: "harness", image: "alasio", imageID: "", ready: true, restartCount: 1, state: { running: {} } }] } })));
    const ending = [endsOnceDeleted(env.home, restarted.bound.pod), endsOnceDeleted(env.home, deleted.bound.pod)];
    await run(withKube((kube) => kube.remove("v1", "Pod", env.home, deleted.bound.pod)));
    await Effect.runPromise(harnesses.live);
    await Promise.all(ending);
    for (const { key } of [ended, restarted, deleted]) {
      for (const deadline = Date.now() + 10_000; await run(readLease(env.codex, key)); await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, `${key} was not released`);
    }
    assert.equal(stand.asked.includes(HOST_PATHS.drain), false);
    const reasons = (conversation: string) => told.filter((event) => event.subject.key === conversation && event.kind === "harness.released").map((event) => (event.kind === "harness.released" ? event.reason : ""));
    // Each told once its release in the background is done, which its Lease's deletion is a step of.
    await eventually("each release told of", () => ([reasons("telegram:51"), reasons("telegram:52"), reasons("telegram:53")].every((told) => told.length > 0) ? true : undefined));
    assert.deepEqual([reasons("telegram:51"), reasons("telegram:52"), reasons("telegram:53")], [["broken"], ["broken"], ["deleted"]]);
    // A binding whose pod went while it was bound, told of as lost.
    const lease = await run(readLease(env.codex, ended.key));
    assert.equal(lease, null);
    await Effect.runPromise(harnesses.lost(env.codex, { metadata: { uid: "uid-lost", annotations: { "alasio.dev/binding-key": harnessKey("telegram:54") } }, spec: { holderIdentity: "codex-gone" } }));
    assert.deepEqual(told.filter((event) => event.kind === "harness.lost" && event.subject.key === "telegram:54"), [
      { kind: "harness.lost", environment: "main", source: "sandbox-gateway", subject: { kind: "conversation", key: "telegram:54" }, binding: "uid-lost", pod: "codex-gone", reason: "its pod went while it was bound" },
    ]);
  } finally {
    stand.close();
  }
});

test("what a bound host tells is told its environment's alasio as the host's, of its own conversation and an event's kind alone, the record the gateway's; anything else is refused", async () => {
  const env = await environment(0);
  const { harnesses } = await harnessesOf(env, configOf(env.home, 8080), agents().agents);
  const key = harnessKey("telegram:41");
  const relay = (namespace: string, said: unknown, raw?: string) =>
    Effect.runPromise(harnesses.relay({ key, binding: "binding-1", namespace }, Buffer.from(raw ?? JSON.stringify(said))));
  const subject = { kind: "conversation", key: "telegram:41" } as const;
  const before = told.length;
  // main's and the branch's, each its own environment's.
  assert.deepEqual(await relay(env.home, { kind: "harness.idle", subject }), { status: 202 });
  assert.deepEqual(await relay(branchNamespace("try"), { kind: "harness.background-response", subject }), { status: 202 });
  assert.deepEqual(told.slice(before), [
    { kind: "harness.idle", environment: "main", source: "harness", subject },
    { kind: "harness.background-response", environment: "try", source: "harness", subject },
  ]);
  // Of a kind no host tells, another conversation, more than an event's kind and its conversation (its id or source among it), no JSON, or of a namespace no environment's harnesses are in: refused, told no one.
  const refused = [
    [env.home, { kind: "harness.released", subject, binding: "binding-1", reason: "idle" }, 403],
    [env.home, { kind: "workspace.fork-answered", subject }, 403],
    [env.home, { kind: "harness.idle", subject: { kind: "conversation", key: "telegram:42" } }, 403],
    [env.home, { kind: "harness.idle", subject, id: "00000000-0000-4000-8000-000000000001" }, 400],
    [env.home, { kind: "harness.idle", subject, source: "sandbox-gateway" }, 400],
    // Nor any WAL position, which the lake's loader would wait for: alasio alone names one, of its own read.
    [env.home, { kind: "harness.idle", subject, lsn: "FFFFFFFF/FFFFFFFF" }, 400],
    [env.home, { kind: "harness.idle", subject: { ...subject, kind: "workspace" } }, 400],
    ["elsewhere", { kind: "harness.idle", subject }, 403],
  ] as const;
  for (const [namespace, said, status] of refused) assert.equal((await relay(namespace, said)).status, status, JSON.stringify(said));
  assert.equal((await relay(env.home, null, "not json")).status, 400);
  assert.equal(told.length, before + 2);
});
