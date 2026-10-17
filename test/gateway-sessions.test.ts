import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { lookup } from "node:dns/promises";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { before, test } from "node:test";

import type { V1Deployment, V1Pod, V1PodSpec, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Effect, Layer } from "effect";
import { TestClock } from "effect/testing";

import { added } from "./support/metrics.ts";
import { Binder } from "../src/binder/binder.ts";
import { readLease, touchLease } from "../src/binder/bindings.ts";
import { freeLabels, type Pool } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import type { Environments, SessionsEnvironment } from "../src/gateway/environments.ts";
import type { EventInput } from "../src/events/events.ts";
import { ATTACH_DURATION, DETACH_DURATION, makeSessions, RELEASES, type SessionsOptions } from "../src/gateway/sessions.ts";
import { KubeClient } from "../src/kube/client.ts";
import type { CheckpointRequest, Manifest } from "../src/node-agent/agent.ts";
import { DEFER_FOR_MS } from "../src/gateway/snapshots.ts";
import { type AttachRequest, NodeAgentError, type NodeAgents } from "../src/sandbox/node-agents.ts";
import { sessionKey } from "../src/sandbox/names.ts";
import { startKubeApiServer } from "./support/kube-apiserver.ts";

// The gateway's session workspaces on a real API server, each pool's pods at the loopback's
// address, where a stand-in for bayma answers /status as the test says, and the node
// agents a stand-in recording what they are asked.

const bindingKey = randomBytes(32);

const withKube = <A, E>(use: (kube: KubeClient["Service"]) => Effect.Effect<A, E>): Effect.Effect<A, E, KubeClient> => Effect.flatMap(KubeClient, use);
const run = <A, E>(effect: Effect.Effect<A, E, KubeClient>): Promise<A> => Effect.runPromise(Effect.provide(effect, Layer.fresh(KubeClient.layer)));

before(async () => {
  process.env["KUBECONFIG"] = (await startKubeApiServer()).kubeconfig;
  // The RuntimeClass gVisor pods name, as the API server admits no pod of one there is not.
  await run(withKube((kube) => kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor" }, handler: "runsc" } as never)));
});

let namespaces = 0;

/** A pool pod's bayma, as its template runs it, with the state directory its hook is told. */
const BAYMA_SPEC = {
  containers: [{ name: "bayma", image: "agent", command: ["/usr/bin/tini", "-s", "--", "node", "/opt/bayma/bayma.js"], args: ["mcp-http", "--state-dir", "/home/agent/.bayma"] }],
};

/** A stand-in for bayma's /status, answering what `status` says now, and the bearers it was asked with. */
async function bayma(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null }; asked: string[]; close: () => void }> {
  const asked: string[] = [];
  const stand = { status: { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 as number | null } };
  const server = createServer((incoming, outgoing) => {
    asked.push(String(incoming.headers.authorization));
    outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, asked, close: () => server.close() };
}

/**
 * Node agents that record what they are asked, in order; a checkpoint does what `checkpoint`
 * says to the pod it saves: ends it saved, fails ending it, or fails leaving it running.
 */
function agents(checkpoint: "saves" | "fails, ending it" | "fails" | "unsaveable" = "saves"): { agents: NodeAgents; attached: (AttachRequest & { node: string })[]; detached: string[]; asked: string[]; saved: CheckpointRequest[] } {
  const attached: (AttachRequest & { node: string })[] = [];
  const detached: string[] = [];
  const asked: string[] = [];
  const saved: CheckpointRequest[] = [];
  const unused = () => Effect.die("not asked");
  const end = (request: CheckpointRequest) =>
    run(withKube((kube) => kube.patchPodStatus(request.namespace, request.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: `containerd://${request.containerId}`, state: { terminated: { exitCode: 0 } } }] } })));
  return {
    attached,
    detached,
    asked,
    saved,
    agents: {
      nodes: Effect.succeed(["node-a"]),
      // No workspace of theirs has a snapshot to restore, nor a record of a release, unless a test gives it one.
      current: () => Effect.succeed(null),
      released: () => Effect.succeed(null),
      prepare: unused, copy: unused, forget: unused, resume: unused, holdings: unused, workspaces: unused, discard: unused, spend: unused, collect: unused, abandon: unused,
      readFile: unused, writeFile: unused, createWorkspace: unused, cloneWorkspace: unused, removeWorkspace: unused, listWorkspaces: unused, quota: unused,
      checkpoint: (node, request) =>
        Effect.gen(function*() {
          asked.push(`checkpoint ${node} ${request.pod}${request.live ? " live" : ""}`);
          saved.push(request);
          // Taken live, its sandbox runs on, at bayma's generation 1.
          if (request.live) return { snapshot: "s-live", release: "r1", live: { stateDir: request.live.stateDir, generation: 1, running: [] } } as unknown as Manifest;
          if (checkpoint === "unsaveable") return yield* new NodeAgentError({ node, call: "snapshot", status: 410, reason: "its restore from the bucket failed to read some of its pages" });
          if (checkpoint !== "fails") yield* Effect.promise(() => end(request));
          if (checkpoint !== "saves") return yield* new NodeAgentError({ node, call: "snapshot", status: 500, reason: "runsc checkpoint failed" });
          return { snapshot: "s-1" } as unknown as Manifest;
        }),
      attach: (node, request) => Effect.sync(() => void attached.push({ node, ...request })),
      setReleased: (ref, into) => Effect.sync(() => void asked.push(`record ${ref.environment}/${ref.workspace} released into ${into?.snapshot ?? "none"}`)),
      // Its bayma, told to go on with its stop, ends.
      stop: (node, request) =>
        Effect.sync(() => void asked.push(`stop ${node} ${request.pod} ${request.hook.join(" ")}`)).pipe(
          Effect.andThen(Effect.promise(() => end({ namespace: request.namespace, pod: request.pod, containerId: request.containerId } as CheckpointRequest))),
        ),
      detach: (node, pod, containerIds) =>
        Effect.sync(() => {
          asked.push(`detach ${node} ${pod}`);
          detached.push(`${node} ${pod} ${containerIds.join(",")}`);
        }),
    },
  };
}

/** An environment of the test's own, its pools' namespace, with `count` free pods of the `none` pool on node-a, serving at the loopback. */
async function environment(count: number, { gvisor = false, holdsStop = false } = {}): Promise<SessionsEnvironment & { none: Pool; template: V1PodTemplateSpec }> {
  const namespace = `sessions-${(namespaces += 1)}`;
  const none: Pool = { name: "sandboxes-none", namespace };
  let template: V1PodTemplateSpec = {};
  await run(withKube((kube) =>
    Effect.gen(function*() {
      yield* kube.create({ apiVersion: "v1", kind: "Namespace", metadata: { name: namespace } });
      // The pool's template, as its snapshots record it.
      // Its bayma holding its stop for a save, where it is told to, as a pool's of snapshotted sessions does.
      const spec = holdsStop
        ? { terminationGracePeriodSeconds: 90, containers: BAYMA_SPEC.containers.map((container) => ({ ...container, args: [...container.args, "--stop-wait-ms", "60000"] })) }
        : BAYMA_SPEC;
      template = { metadata: { labels: { ...freeLabels(none) }, annotations: { "alasio.dev/session-template": "t-current" } }, spec: { ...spec, ...(gvisor ? { runtimeClassName: "gvisor" } : {}) } };
      yield* kube.create<V1Deployment>({ apiVersion: "apps/v1", kind: "Deployment", metadata: { name: none.name, namespace }, spec: { replicas: 0, selector: { matchLabels: { ...freeLabels(none) } }, template } });
      for (let n = 0; n < count; n += 1) {
        const name = `none-${n}`;
        yield* kube.create<V1Pod>({ apiVersion: "v1", kind: "Pod", metadata: { name, namespace, labels: { ...freeLabels(none) } }, spec: { nodeName: "node-a", ...template.spec } as V1PodSpec });
        yield* kube.patchPodStatus(namespace, name, {
          status: { phase: "Running", podIP: "127.0.0.1", conditions: [{ type: "Ready", status: "True" }], containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: true, restartCount: 0, containerID: `containerd://c-${n}`, state: { running: {} } }] },
        });
      }
    })
  ));
  return { name: `env-${namespaces}`, namespace, parent: null, none, template };
}

/** The environments `list`, as found: those alone. */
const found = (list: readonly SessionsEnvironment[]): Environments => ({
  current: Effect.succeed(list),
  named: (name) => Effect.succeed(list.find((each) => each.name === name) ?? null),
  ofNamespace: (namespace) => Effect.succeed(list.find((each) => each.namespace === namespace) ?? null),
});

/** What the sessions told of, as each environment's alasio reads it. */
const told: EventInput[] = [];

/** The gateway's sessions' options, of the environment `env`, main, whose sessions are snapshotted under the release `r1`, onto its pool's template. */
const options = (env: SessionsEnvironment & { template?: V1PodTemplateSpec }, podPort: number): SessionsOptions => ({
  environments: found([env]),
  mainNamespace: env.namespace,
  snapshots: {
    liveEverySeconds: 600,
    releases: [{ release: "r1", runtimeClassName: "gvisor-r1" }],
    templates: { none: { current: { ...env.template }, previous: null }, full: { current: { ...env.template }, previous: null } },
  },
  pools: { none: "sandboxes-none", full: "sandboxes-full" },
  releaseAfterMs: 60_000,
  podPort,
  quotaBytes: 2 * 1024 ** 3,
  bindingKey,
  events: { tell: (event) => Effect.sync(() => void told.push(event)) },
});

/** The gateway's sessions and its Binder, on clients of their own. */
const sessionsOf = (opts: SessionsOptions, nodeAgents: NodeAgents) =>
  Effect.runPromise(Effect.provide(Effect.gen(function*() {
    const binder = yield* Binder;
    return { binder, sessions: yield* makeSessions(opts, nodeAgents, binder) };
  }), Layer.provideMerge(Binder.layer({ waitMs: 5_000 }), Layer.fresh(KubeClient.layer))));

test("a session's workspace is placed in its environment's pool of its internet, and readied by its node's agent attaching it with its binding's token", async () => {
  const env = await environment(1);
  const recorded = agents();
  const telemetry = { host: "localhost", port: 4318, env: { OTEL_TRACES_EXPORTER: "otlp" } };
  const { binder, sessions } = await sessionsOf({ ...options(env, 7290), telemetry }, recorded.agents);
  const key = sessionKey("fs-abc123");
  for (const [said, why] of [
    [{ environment: env.name, mode: "none" }, null],
    [{ environment: "elsewhere", mode: "none" }, "there is no environment elsewhere of sessions"],
    [{ environment: env.name, mode: "some" }, "a session's internet is none or full, not some"],
  ] as const) {
    const placed = await Effect.runPromise(Effect.result(sessions.place(key, said)));
    assert.equal(placed._tag === "Failure" ? placed.failure.message : null, why);
  }
  assert.equal((await Effect.runPromise(Effect.flip(sessions.place("folder:claude:conversation", { environment: env.name, mode: "none" })))).message, "folder:claude:conversation is no session workspace's");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  assert.deepEqual(placement.pool, env.none);
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  const [attach] = recorded.attached;
  assert.deepEqual([attach?.node, attach?.environment, attach?.workspace, attach?.pod], ["node-a", env.namespace, "fs-abc123", bound.pod]);
  assert.deepEqual(verifyToken(bindingKey, attach?.token ?? "", Date.now()), { key, binding: bound.binding, namespace: env.namespace });
  // Its bayma exports to the receiver by its address, as a session with no internet resolves no name.
  assert.deepEqual(attach?.env, { OTEL_TRACES_EXPORTER: "otlp", OTEL_EXPORTER_OTLP_ENDPOINT: `http://${(await lookup("localhost")).address}:4318` });
});

test("a bound pod whose bayma runs an exec is kept, and its bayma asked again only once its poll comes round, with its binding's token", async () => {
  const env = await environment(2);
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf(options(env, stand.port), recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    // Its bayma busy: not released, and asked no more until its poll comes round.
    stand.status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null };
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    assert.equal(stand.asked.length, 1);
    assert.deepEqual(verifyToken(bindingKey, stand.asked[0]?.slice("Bearer ".length) ?? "", Date.now()), { key, binding: bound.binding, namespace: env.namespace });
    assert.ok(await run(readLease(env.none, key)));
  } finally {
    stand.close();
  }
});

test("a bound pod whose bayma is idle is kept while a request of it came within the wait, and once none did, released: under a runtime that saves nothing, its pod's deletion ends it, its workspace detached once its containers stopped, then its binding gone", async () => {
  const env = await environment(1);
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 2_000, idlePollMs: 500 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    await run(touchLease(env.none, key, Date.now()));
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer))), []);
    // Its wait past its last request, as kubelet stops its containers once its deletion is asked.
    await new Promise((resolve) => setTimeout(resolve, 2_100));
    const stopping = (async () => {
      for (;;) {
        const pod = await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound.pod))) as V1Pod | null;
        if (pod?.metadata?.deletionTimestamp) {
          await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 143 } } }] } })));
          return;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    let released: readonly string[] = [];
    const counted = await added(RELEASES, async () => (released = await Effect.runPromise(Effect.provide(sessions.sweep, Layer.fresh(KubeClient.layer)))));
    await stopping;
    assert.deepEqual(released, [key]);
    assert.deepEqual(counted, { [`k8s.namespace.name=${env.namespace},alasio.gateway.release.reason=idle`]: 1 });
    assert.deepEqual(recorded.detached, [`node-a ${bound.pod} c-0`]);
    assert.equal(await run(readLease(env.none, key)), null);
    // Told of, to its environment's alasio.
    assert.deepEqual(told.filter((event) => event.subject.key === key && event.environment === env.name), [
      { kind: "workspace.released", environment: env.name, source: "sandbox-gateway", subject: { kind: "workspace", key }, binding: bound.binding, reason: "idle" },
    ]);
  } finally {
    stand.close();
  }
});

test("a bound gVisor pod idle for its wait is saved whole by its node's agent first, which ends it, then detached, then deleted with its binding", async () => {
  const env = await environment(1, { gvisor: true });
  const stand = await bayma();
  const recorded = agents();
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    assert.deepEqual(await Effect.runPromise(sessions.sweep), [key]);
    // Its release recorded with the workspace, for its next bind, before its workspace is detached.
    assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-abc123 released into s-1`, `detach node-a ${bound.pod}`]);
    const [saved] = recorded.saved;
    assert.deepEqual([saved?.environment, saved?.workspace, saved?.namespace, saved?.containerId], [env.namespace, "fs-abc123", env.namespace, "c-0"]);
    assert.deepEqual(saved?.hook, ["node", "/opt/bayma/bayma.js", "sandbox-hook", "--state-dir", "/home/agent/.bayma"]);
    assert.equal((saved?.podTemplate as { spec?: { runtimeClassName?: string } }).spec?.runtimeClassName, "gvisor");
    assert.equal(await run(readLease(env.none, key)), null);
  } finally {
    stand.close();
  }
});

test("a release whose save fails leaving the sandbox running keeps the binding, and one whose save fails ending it goes on, as bayma's checkpoints keep what ran", async () => {
  for (const [outcome, kept] of [["fails", true], ["fails, ending it", false]] as const) {
    const env = await environment(1, { gvisor: true });
    const stand = await bayma();
    const recorded = agents(outcome);
    try {
      const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
      const key = sessionKey("fs-abc123");
      const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
      const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
      stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
      assert.deepEqual(await Effect.runPromise(sessions.sweep), kept ? [] : [key], outcome);
      const lease = await run(readLease(env.none, key));
      if (kept) {
        assert.deepEqual([lease?.spec?.holderIdentity, lease?.metadata?.annotations?.["alasio.dev/releasing"]], [bound.pod, undefined], outcome);
        assert.deepEqual(recorded.detached, [], outcome);
      } else {
        assert.equal(lease, null, outcome);
        assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-abc123 released into none`, `detach node-a ${bound.pod}`], outcome);
      }
    } finally {
      stand.close();
    }
  }
});

test("a binding let go as its pod went while it was bound is told of, to its environment's alasio, as lost", async () => {
  const env = { name: "try", namespace: "alasio-branch-try-sessions", parent: "main" };
  const { sessions } = await sessionsOf(options(env, 7290), agents().agents);
  const key = sessionKey("fs-lost");
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-lost", namespace: env.namespace, annotations: { "alasio.dev/binding-key": key } }, spec: { holderIdentity: "sandboxes-none-0" } };
  await Effect.runPromise(sessions.lost({ name: "sandboxes-none", namespace: env.namespace }, lease));
  assert.deepEqual(told.filter((event) => event.subject.key === key), [
    { kind: "workspace.lost", environment: "try", source: "sandbox-gateway", subject: { kind: "workspace", key }, binding: "uid-lost", pod: "sandboxes-none-0", reason: "its pod went while it was bound" },
  ]);
});

/** Node agents that keep workspaces as a map of their files, by environment and name, recording each call. */
function workspaceAgents(): { agents: NodeAgents; calls: string[]; files: Map<string, Map<string, Uint8Array>> } {
  const calls: string[] = [];
  const files = new Map<string, Map<string, Uint8Array>>();
  const name = ({ environment, workspace }: { environment: string; workspace: string }) => `${environment}/${workspace}`;
  const missing = (call: string) => new NodeAgentError({ node: "node-a", call, status: 404, reason: "there is no such workspace" });
  const base = agents().agents;
  return {
    calls,
    files,
    agents: {
      ...base,
      createWorkspace: (ref, quotaBytes) => Effect.sync(() => (calls.push(`create ${name(ref)} ${quotaBytes}`), void files.set(name(ref), files.get(name(ref)) ?? new Map()))),
      removeWorkspace: (ref) => Effect.sync(() => (calls.push(`remove ${name(ref)}`), void files.delete(name(ref)))),
      cloneWorkspace: (from, to, quotaBytes) =>
        Effect.suspend(() => {
          calls.push(`clone ${name(from)} ${name(to)} ${quotaBytes}`);
          const source = files.get(name(from));
          return source ? Effect.sync(() => void files.set(name(to), new Map(source))) : Effect.fail(missing("clone"));
        }),
      listWorkspaces: (environment) => Effect.sync(() => [...files.keys()].filter((each) => each.startsWith(`${environment}/`)).map((each) => each.slice(environment.length + 1))),
      quota: (ref) => Effect.sync(() => (files.has(name(ref)) ? { capacityBytes: 2 * 1024 ** 3, usedBytes: 5 } : null)),
      readFile: (ref, path) => {
        const file = files.get(name(ref))?.get(path);
        return file ? Effect.succeed(file) : Effect.fail(missing("read"));
      },
      writeFile: (ref, path, bytes) => {
        const workspace = files.get(name(ref));
        return workspace ? Effect.sync(() => void workspace.set(path, Uint8Array.from(bytes))) : Effect.fail(missing("write"));
      },
    },
  };
}

test("a caller makes, writes, reads, measures, lists and deletes its own environment's workspace through the gateway alone, one bound not deleted", async () => {
  const env = await environment(1);
  const recorded = workspaceAgents();
  const { binder, sessions } = await sessionsOf({ ...options(env, 7290), environments: found([env, { name: "other", namespace: "other-sessions", parent: null }]) }, recorded.agents);
  const key = sessionKey("fs-own123");
  const claims = { key, expires: Date.now() + 60_000, options: { environment: env.name, mode: "none" } };
  const ask = (method: string, path: string, body: unknown = "", of = claims) => {
    const [route = "", search = ""] = path.split("?", 2);
    const bytes = typeof body === "string" ? Buffer.from(body) : Buffer.from(JSON.stringify(body));
    return Effect.runPromise(sessions.operate(of, { method, path: route, query: new URLSearchParams(search), body: bytes }));
  };
  assert.deepEqual(await ask("POST", "/workspace"), { status: 204 });
  assert.deepEqual(await ask("PUT", "/workspace/files?path=workspace/note.txt", "hello"), { status: 204 });
  const read = await ask("GET", "/workspace/files?path=workspace/note.txt");
  assert.deepEqual([read.status, Buffer.from(read.bytes ?? []).toString()], [200, "hello"]);
  assert.equal((await ask("GET", "/workspace/files?path=workspace/none.txt")).status, 404);
  assert.equal((await ask("GET", "/workspace/files?path=workspace/note.txt&max=many")).status, 400);
  assert.deepEqual(await ask("GET", "/workspace/quota"), { status: 200, json: { capacityBytes: 2 * 1024 ** 3, usedBytes: 5 } });
  assert.deepEqual(await ask("GET", "/workspace/all"), { status: 200, json: { workspaces: ["fs-own123"] } });
  // Made in its own environment's namespace, of the gateway's quota; another's lists none of it.
  assert.deepEqual(recorded.calls, [`create ${env.namespace}/fs-own123 ${2 * 1024 ** 3}`]);
  assert.deepEqual(await ask("GET", "/workspace/all", "", { ...claims, options: { environment: "other", mode: "none" } }), { status: 200, json: { workspaces: [] } });
  // Bound, it is not deleted; released, it is.
  const placement = await Effect.runPromise(sessions.place(key, claims.options));
  await Effect.runPromise(binder.bind(placement.pool, key, placement));
  assert.equal((await ask("DELETE", "/workspace")).status, 409);
  await Effect.runPromise(binder.release(placement.pool, key));
  assert.deepEqual(await ask("DELETE", "/workspace"), { status: 204 });
  assert.equal(recorded.files.has(`${env.namespace}/fs-own123`), false);
  // An environment there is not, a key of no workspace, and a route there is not.
  assert.equal((await ask("POST", "/workspace", "", { ...claims, options: { environment: "nowhere", mode: "none" } })).status, 403);
  assert.equal((await ask("POST", "/workspace", "", { ...claims, key: "folder:claude:conversation" })).status, 403);
  assert.equal((await ask("PATCH", "/workspace")).status, 404);
});

test("main forks its workspace into a branch of its own, as the branch records main its parent, and a branch into itself, but never into main nor into a sibling", async () => {
  const main: SessionsEnvironment = { name: "main", namespace: "fork-main-sessions", parent: null };
  const one = { name: "one", namespace: "alasio-branch-one-sessions", parent: "main" };
  const two = { name: "two", namespace: "alasio-branch-two-sessions", parent: "main" };
  const recorded = workspaceAgents();
  const { sessions } = await sessionsOf({ ...options(main, 7290), environments: found([main, one, two]) }, recorded.agents);
  for (const env of [main, one]) recorded.files.set(`${env.namespace}/fs-shared1`, new Map([["workspace/a", Buffer.from(env.name)]]));
  const fork = (from: SessionsEnvironment, to: string, workspace = "fs-shared1") =>
    Effect.runPromise(sessions.operate(
      { key: sessionKey("fs-shared1"), expires: Date.now() + 60_000, options: { environment: from.name, mode: "none" } },
      { method: "POST", path: "/workspace/forks", query: new URLSearchParams(), body: Buffer.from(JSON.stringify({ environment: to, workspace })) },
    ));
  assert.deepEqual(await fork(main, "one"), { status: 204 });
  assert.equal(Buffer.from(recorded.files.get(`${one.namespace}/fs-shared1`)?.get("workspace/a") ?? []).toString(), "main");
  assert.deepEqual(await fork(main, "main", "fs-copy01"), { status: 204 });
  assert.deepEqual(await fork(one, "one", "fs-copy02"), { status: 204 });
  for (const [from, to] of [[one, "main"], [one, "two"], [main, "nowhere"]] as const) {
    const refused = await fork(from, to, "fs-stolen1");
    assert.equal(refused.status, 403, `${from.name} into ${to}`);
  }
  assert.equal((await fork(main, "one", "Not A Name")).status, 400);
  assert.deepEqual(recorded.calls, [
    `clone ${main.namespace}/fs-shared1 ${one.namespace}/fs-shared1 ${2 * 1024 ** 3}`,
    `clone ${main.namespace}/fs-shared1 ${main.namespace}/fs-copy01 ${2 * 1024 ** 3}`,
    `clone ${one.namespace}/fs-shared1 ${one.namespace}/fs-copy02 ${2 * 1024 ** 3}`,
  ]);
});

test("each attach and detach is timed, and each binding released counted by why: asked by its caller, or lost as its pod went", async () => {
  const env = await environment(1);
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-timed1");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const of = `k8s.namespace.name=${env.namespace}`;
  assert.deepEqual(await added(ATTACH_DURATION, () => Effect.runPromise(binder.bind(placement.pool, key, placement))), { [of]: 1 });
  // Released as its caller asks, as the binder's front runs its placement's release; its pod's deletion, under no gVisor, ends it.
  const bound = await Effect.runPromise(binder.bound(placement.pool, key));
  const stopping = (async () => {
    for (;;) {
      const pod = await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound?.pod ?? ""))) as V1Pod | null;
      if (!pod || pod.metadata?.deletionTimestamp) {
        if (pod) await run(withKube((kube) => kube.patchPodStatus(env.namespace, pod.metadata?.name ?? "", { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-0", state: { terminated: { exitCode: 143 } } }] } })));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();
  const detached = await added(DETACH_DURATION, async () => {
    const asked = await added(RELEASES, async () => {
      assert.equal(await Effect.runPromise(binder.release(placement.pool, key, placement.release)), true);
    });
    assert.deepEqual(asked, { [`${of},alasio.gateway.release.reason=asked`]: 1 });
  });
  await stopping;
  assert.deepEqual(detached, { [of]: 1 });
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-gone", namespace: env.namespace, annotations: { "alasio.dev/binding-key": key } }, spec: { holderIdentity: "none-0" } };
  assert.deepEqual(await added(RELEASES, () => Effect.runPromise(sessions.lost(env.none, lease))), { [`${of},alasio.gateway.release.reason=lost`]: 1 });
});

test("a release whose sandbox can be saved no more has its bayma go on with the stop it holds, its node's agent running its hook, and is released into no snapshot", async () => {
  const env = await environment(1, { gvisor: true, holdsStop: true });
  const stand = await bayma();
  const recorded = agents("unsaveable");
  try {
    const { binder, sessions } = await sessionsOf({ ...options(env, stand.port), releaseAfterMs: 0 }, recorded.agents);
    const key = sessionKey("fs-abc123");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now() - 600_000 };
    const counted = await added("alasio.sandbox.releases", async () => assert.deepEqual(await Effect.runPromise(sessions.sweep), [key]));
    assert.deepEqual(counted, { "alasio.sandbox.release.outcome=unsaveable": 1 });
    assert.deepEqual(recorded.asked, [
      `checkpoint node-a ${bound.pod}`,
      `stop node-a ${bound.pod} node /opt/bayma/bayma.js sandbox-hook stop --state-dir /home/agent/.bayma`,
      `record ${env.namespace}/fs-abc123 released into none`,
      `detach node-a ${bound.pod}`,
    ]);
    assert.equal(await run(readLease(env.none, key)), null);
  } finally {
    stand.close();
  }
});

/** Its snapshot `snapshot` of the workspace `workspace` of `env`, taken under `r1` on its pool's template. */
const manifestOf = (env: SessionsEnvironment & { template: V1PodTemplateSpec }, workspace: string, snapshot: string): Manifest => ({
  environment: env.namespace,
  workspace,
  snapshot,
  release: "r1",
  cpuFeatures: ["sse"],
  podTemplate: { ...env.template },
  layers: [{ digest: `sha256:${"a".repeat(64)}`, files: [] }],
  bytes: 0,
  takenAt: "2026-10-17T12:00:00Z",
  pauseMs: 0,
  uploadMs: 0,
});

/** Node agents as `agents` makes them, which hold `current` of the workspace, and its record of `released`; their prepare does as `prepare` says. */
function restoringAgents(current: Manifest | null, released: { snapshot: string; release: string } | null, { prepare = "prepares" as "prepares" | "fails" } = {}) {
  const base = agents();
  const state = { current, released };
  const restoring: NodeAgents = {
    ...base.agents,
    released: () => Effect.sync(() => state.released),
    setReleased: (ref, into) => Effect.sync(() => (base.asked.push(`record ${ref.environment}/${ref.workspace} released into ${into?.snapshot ?? "none"}`), void (state.released = into))),
    current: () => Effect.sync(() => state.current),
    holdings: (node) => Effect.succeed({ node, releases: [{ release: "r1", cpuFeatures: ["sse", "avx"] }], cached: [] }),
    prepare: (node) =>
      prepare === "fails"
        ? Effect.fail(new NodeAgentError({ node, call: "prepare", status: 502, reason: "the snapshots bucket failed" }))
        : Effect.sync(() => (base.asked.push(`prepare ${node}`), { manifest: state.current as Manifest, imagePath: "/var/cache/alasio/image", source: "cache" as const })),
    resume: (node, request) => Effect.sync(() => void base.asked.push(`resume ${node} ${request.pod} ${request.snapshot}`)),
    spend: (_ref, snapshot) => Effect.sync(() => (base.asked.push(`spend ${snapshot}`), void (state.current = null))),
    forget: (ref) => Effect.sync(() => (base.asked.push(`forget ${ref.workspace}`), void (state.current = null))),
  };
  return { ...base, state, agents: restoring };
}

test("a workspace released into its snapshot is restored from it as it is next bound, by a pod of its own of the pool's template, paused until its node's agent resumes it, then spent", async () => {
  const env = await environment(1, { gvisor: true });
  await run(withKube((kube) => kube.create({ apiVersion: "node.k8s.io/v1", kind: "RuntimeClass", metadata: { name: "gvisor-r1" }, handler: "runsc-r1" } as never)).pipe(Effect.ignore));
  const snapshot = "20261017T120000000Z-0000000a";
  const recorded = restoringAgents(manifestOf(env, "fs-restored", snapshot), { snapshot, release: "r1" });
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-restored");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const counted = await added("alasio.sandbox.restores", async () => {
    const binding = Effect.runPromise(binder.bind(placement.pool, key, placement));
    // Its pod runs, as kubelet would run it, then, once resumed, is ready.
    const restored = await (async () => {
      for (;;) {
        const [pod] = (await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "alasio.dev/binding")))) as V1Pod[];
        if (pod?.metadata?.name?.startsWith("sandboxes-none-restore-")) return pod;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    })();
    const name = restored.metadata?.name ?? "";
    await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { phase: "Running", podIP: "127.0.0.1", podIPs: [{ ip: "127.0.0.1" }], containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 0, containerID: "containerd://c-restored", state: { running: {} } }] } })));
    for (;;) {
      const pod = (await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, name)))) as V1Pod;
      if (pod.status?.conditions?.some(({ type, status }) => type === "alasio.dev/resumed" && status === "True")) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    await run(withKube((kube) => kube.patchPodStatus(env.namespace, name, { status: { conditions: [{ type: "Ready", status: "True" }] } })));
    const bound = await binding;
    assert.equal(bound.pod, name);
    // Of the pool's template, its sandbox restored paused from the image its node's agent prepared, under the release that took it, on that node.
    assert.deepEqual([restored.metadata?.annotations?.["dev.gvisor.internal.restore.host-image-path"], restored.metadata?.annotations?.["dev.gvisor.internal.restore.paused"]], ["/var/cache/alasio/image", "true"]);
    assert.equal(restored.spec?.runtimeClassName, "gvisor-r1");
    const plain = (value: unknown) => JSON.parse(JSON.stringify(value)) as unknown;
    assert.deepEqual(plain(restored.spec?.readinessGates), [{ conditionType: "alasio.dev/resumed" }]);
    assert.deepEqual(plain(restored.spec?.affinity?.nodeAffinity?.requiredDuringSchedulingIgnoredDuringExecution?.nodeSelectorTerms), [{ matchFields: [{ key: "metadata.name", operator: "In", values: ["node-a"] }] }]);
    // Bound from the first, and no pool's free pod, which its ReplicaSet would take for its own.
    assert.equal(restored.metadata?.labels?.["alasio.dev/free"], undefined);
    assert.equal(restored.metadata?.annotations?.["alasio.dev/binding-key"], key);
    assert.deepEqual(recorded.asked, [`prepare node-a`, `resume node-a ${name} ${snapshot}`, `spend ${snapshot}`, `record ${env.namespace}/fs-restored released into none`]);
    assert.equal(recorded.attached[0]?.pod, name);
  });
  assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=restored": 1 });
  // A free pod of the pool's claimed by none.
  assert.equal(((await run(withKube((kube) => kube.list("v1", "Pod", env.namespace, "alasio.dev/free=true")))) as V1Pod[]).length, 1);
});

test("a workspace whose record names a snapshot it holds no more, or another than its current one, is bound to a free pod, its snapshot and its record forgotten", async () => {
  for (const [current, into, why] of [
    [null, "20261017T120000000Z-0000000a", "a record of a snapshot gone"],
    ["20261017T130000000Z-0000000b", "20261017T120000000Z-0000000a", "a record of a snapshot moved on from"],
    ["20261017T130000000Z-0000000b", null, "a snapshot no release recorded"],
  ] as const) {
    const env = await environment(1, { gvisor: true });
    const recorded = restoringAgents(current ? manifestOf(env, "fs-stale", current) : null, into ? { snapshot: into, release: "r1" } : null);
    const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
    const key = sessionKey("fs-stale");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const counted = await added("alasio.sandbox.restores", async () => assert.equal((await Effect.runPromise(binder.bind(placement.pool, key, placement))).pod, "none-0", why));
    assert.deepEqual(counted, { "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=stale": 1 }, why);
    assert.deepEqual([recorded.state.current, recorded.state.released], [null, null], why);
    assert.deepEqual(recorded.asked, [`forget fs-stale`, `record ${env.namespace}/fs-stale released into none`], why);
  }
});

test("a restore whose snapshot cannot be prepared now is deferred, the bind refused to be tried again, its snapshot kept; and one deferred for long enough falls back", async () => {
  const env = await environment(1, { gvisor: true });
  const snapshot = "20261017T120000000Z-0000000a";
  const recorded = restoringAgents(manifestOf(env, "fs-deferred", snapshot), { snapshot, release: "r1" }, { prepare: "fails" });
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-deferred");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  // Its deferrals timed by a clock of the test's, from its first.
  const later = await added("alasio.sandbox.restores", () =>
    Effect.runPromise(Effect.gen(function*() {
      const refused = yield* Effect.flip(binder.bind(placement.pool, key, placement));
      assert.match(refused.message, /deferred, to be tried again: .*the snapshots bucket failed/u);
      assert.equal(recorded.state.current?.snapshot, snapshot);
      assert.equal(yield* Effect.promise(() => run(readLease(env.none, key))), null);
      // Deferred for long enough: bound to a free pod, its snapshot forgotten.
      yield* TestClock.adjust(DEFER_FOR_MS);
      return yield* binder.bind(placement.pool, key, placement);
    }).pipe(Effect.provide(TestClock.layer()))));
  assert.deepEqual(later, {
    "alasio.sandbox.restore.outcome=deferred,alasio.sandbox.restore.fallback=prepare": 1,
    "alasio.sandbox.restore.outcome=fallback,alasio.sandbox.restore.fallback=prepare": 1,
  });
  assert.equal(recorded.state.current, null);
  assert.equal((await run(readLease(env.none, key)))?.spec?.holderIdentity, "none-0");
});

test("a bound pod ended by other than the gateway, as a drain ends one, is saved as its bayma holds its stop, its release recorded, its workspace detached once its containers stopped, and its binding let go", async () => {
  const env = await environment(1, { gvisor: true, holdsStop: true });
  const recorded = agents();
  const { binder, sessions } = await sessionsOf(options(env, 7290), recorded.agents);
  const key = sessionKey("fs-drained");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  // Deleted as a drain deletes it: with its grace, kubelet to end it.
  await run(withKube((kube) => kube.remove("v1", "Pod", env.namespace, bound.pod)));
  const counted = await added("alasio.sandbox.stop_releases", async () => {
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.drain, Layer.fresh(KubeClient.layer))), [key]);
    // Begun once, however often it is looked at meanwhile.
    assert.deepEqual(await Effect.runPromise(Effect.provide(sessions.drain, Layer.fresh(KubeClient.layer))), []);
    for (const deadline = Date.now() + 10_000; (await run(readLease(env.none, key))) !== null; await new Promise((resolve) => setTimeout(resolve, 100))) {
      assert.ok(Date.now() < deadline, "its binding was not let go");
    }
  });
  assert.deepEqual(counted, { "alasio.sandbox.stop_release.outcome=snapshotted": 1 });
  assert.deepEqual(recorded.asked, [`checkpoint node-a ${bound.pod}`, `record ${env.namespace}/fs-drained released into s-1`, `detach node-a ${bound.pod}`]);
  assert.ok(told.some((event) => event.kind === "workspace.released" && event.subject.key === key && event.reason === "deleted" && event.binding === bound.binding));
});

/** A stand-in for bayma's /status that says its state generation too, as bayma 0.14 does. */
async function baymaOfGeneration(): Promise<{ port: number; status: { execsRunning: number; execsQueued: number; idleSinceMs: number | null; generation: number }; close: () => void }> {
  const stand = { status: { execsRunning: 0, execsQueued: 0, idleSinceMs: 0 as number | null, generation: 1 } };
  const server = createServer((_incoming, outgoing) => void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify(stand.status)));
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { port: (server.address() as AddressInfo).port, get status() { return stand.status; }, set status(value) { stand.status = value; }, close: () => server.close() };
}

test("a bound pod whose execs run is saved live every liveEvery, its sandbox running on, and its snapshot spent once bayma's generation moves on from it", async () => {
  const env = await environment(1, { gvisor: true });
  const stand = await baymaOfGeneration();
  const recorded = agents();
  const spent: string[] = [];
  try {
    const opts = options(env, stand.port);
    const { binder, sessions } = await sessionsOf({ ...opts, snapshots: { ...opts.snapshots!, liveEverySeconds: 1 } }, { ...recorded.agents, spend: (_ref, snapshot) => Effect.sync(() => void spent.push(snapshot)) });
    const key = sessionKey("fs-busy");
    const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
    const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
    stand.status = { execsRunning: 1, execsQueued: 0, idleSinceMs: null, generation: 1 };
    const look = () => Effect.runPromise(Effect.provide(sessions.live, Layer.fresh(KubeClient.layer)));
    const counted = await added("alasio.sandbox.live_checkpoints", async () => {
      // Its execs begun: not saved live before liveEvery has passed since.
      await look();
      assert.deepEqual(recorded.asked, [] as string[]);
      await new Promise((resolve) => setTimeout(resolve, 1_100));
      await look();
      for (const deadline = Date.now() + 5_000; !recorded.asked.includes(`checkpoint node-a ${bound.pod} live`); await new Promise((resolve) => setTimeout(resolve, 50))) assert.ok(Date.now() < deadline);
      assert.deepEqual(recorded.saved.at(-1)?.live, { stateDir: "/home/agent/.bayma" });
      // bayma moved on from the generation it caught: spent.
      stand.status = { execsRunning: 0, execsQueued: 0, idleSinceMs: Date.now(), generation: 2 };
      await look();
    });
    assert.deepEqual(spent, ["s-live"]);
    assert.deepEqual(counted, { "alasio.sandbox.live_checkpoint.outcome=taken": 1, "alasio.sandbox.live_checkpoint.outcome=spent": 1 });
  } finally {
    stand.close();
  }
});

test("a bound pod whose bayma ended in it, or was started again, is released as broken, into the snapshot taken of it live; and one whose pod went, as lost, into it too, recorded while its Lease holds", async () => {
  const env = await environment(2, { gvisor: true });
  const recorded = agents();
  // Saved live once it is bound.
  let saved = false;
  const live = { ...recorded.agents, current: () => Effect.succeed(saved ? ({ snapshot: "s-live", release: "r1", live: { stateDir: "/home/agent/.bayma", generation: 1, running: [] } } as unknown as Manifest) : null) };
  const { binder, sessions } = await sessionsOf(options(env, 7290), live);
  const key = sessionKey("fs-broken");
  const placement = await Effect.runPromise(sessions.place(key, { environment: env.name, mode: "none" }));
  const bound = await Effect.runPromise(binder.bind(placement.pool, key, placement));
  saved = true;
  // Started again in its pod, as kubelet starts a container that ended.
  await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 1, containerID: "containerd://c-0", state: { waiting: { reason: "CrashLoopBackOff" } } }] } })));
  // Its deletion ends it, as kubelet would.
  const ending = (async () => {
    for (;;) {
      const pod = (await run(withKube((kube) => kube.read("v1", "Pod", env.namespace, bound.pod)))) as V1Pod | null;
      if (pod?.metadata?.deletionTimestamp) {
        await run(withKube((kube) => kube.patchPodStatus(env.namespace, bound.pod, { status: { containerStatuses: [{ name: "bayma", image: "agent", imageID: "", ready: false, restartCount: 1, containerID: "containerd://c-0", state: { terminated: { exitCode: 1 } } }] } })));
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  })();
  await Effect.runPromise(Effect.provide(sessions.live, Layer.fresh(KubeClient.layer)));
  await ending;
  for (const deadline = Date.now() + 10_000; (await run(readLease(env.none, key))) !== null; await new Promise((resolve) => setTimeout(resolve, 100))) assert.ok(Date.now() < deadline, "its binding was not let go");
  assert.deepEqual(recorded.asked, [`record ${env.namespace}/fs-broken released into s-live`, `detach node-a ${bound.pod}`]);
  assert.ok(told.some((event) => event.kind === "workspace.released" && event.subject.key === key && event.reason === "broken"));
  // One whose pod went: released into it as its Lease is let go.
  const lease = { apiVersion: "coordination.k8s.io/v1", kind: "Lease", metadata: { uid: "uid-gone", namespace: env.namespace, annotations: { "alasio.dev/binding-key": sessionKey("fs-gone") } }, spec: { holderIdentity: "none-9" } };
  await Effect.runPromise(sessions.recordLost(env.none, lease));
  assert.equal(recorded.asked.at(-1), `record ${env.namespace}/fs-gone released into s-live`);
});
