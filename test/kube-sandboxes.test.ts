import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import type { KubernetesObject, V1Condition, V1Container, V1ObjectMeta } from "@kubernetes/client-node";
import { ConfigProvider, Effect, Exit, Fiber, Layer, Option, Result, Schema, type Scope, Stream } from "effect";
import { FetchHttpClient } from "effect/http";
import { TestClock } from "effect/testing";

import { type ExecResult, exitCodeOf, KubeApiError, KubeClient, KubeExecError, mergePatch, patchBody } from "../src/kube/client.ts";
import { decodeKubeTemplates, type HostProfile, type KubeTemplates, loadKubeTemplates } from "../src/kube/config.ts";
import {
  makeSandboxes,
  newToken,
  podEnded,
  POD_TEMPLATE_ANNOTATION,
  podTemplateHash,
  type SandboxSpec,
  type SandboxStatus,
  sandboxManifest,
  sandboxReady,
  tokenSecretManifest,
} from "../src/kube/sandboxes.ts";
import { SessionToken } from "../src/sandbox/names.ts";
import { sameToken } from "../src/shared/tokens.ts";

const profile = (extra: Partial<HostProfile> = {}): HostProfile => ({
  namespace: "alasio-host",
  port: 7290,
  stateRoot: "/var/lib/alasio/bayma",
  podTemplate: {
    metadata: { labels: { team: "a" } },
    spec: {
      containers: [{ name: "bayma", image: "agent@sha256:1", args: ["mcp-http", "--port", "7290"], securityContext: { runAsUser: 1000 } }],
      initContainers: [{ name: "prepare", image: "agent@sha256:1" }],
      volumes: [{ name: "tmp", emptyDir: {} }],
    },
  },
  ...extra,
});

/** A Sandbox, Secret or Pod as the fake API server keeps it, with the uid and generation it gave it. */
interface KeptObject extends KubernetesObject {
  metadata: V1ObjectMeta & { uid: string; generation: number };
  spec?: Partial<SandboxSpec> & { nodeName?: string; containers?: V1Container[] };
  status?: SandboxStatus;
  data?: Record<string, string>;
  stringData?: Record<string, string>;
}

/** A merge patch of a Sandbox's annotations and spec, the only kind alasio makes. */
interface SandboxPatch {
  readonly metadata?: { readonly annotations?: Readonly<Record<string, string | undefined>> };
  readonly spec?: Partial<SandboxSpec>;
}

/** `target` with the JSON merge patch `patch` applied (RFC 7386), as the API server applies one. */
function applyMergePatch(target: unknown, patch: unknown): unknown {
  if (typeof patch !== "object" || patch === null || Array.isArray(patch)) return patch;
  const result: Record<string, unknown> = typeof target === "object" && target !== null && !Array.isArray(target) ? { ...target } : {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete result[key];
    else result[key] = applyMergePatch(result[key], value);
  }
  return result;
}

type ExecArgs = Parameters<KubeClient["Service"]["exec"]>;

/** A call the fake API server was made, with what identifies it. */
type KubeCall =
  | readonly [verb: "read" | "create" | "remove", kind: string | undefined, name: string | undefined]
  | readonly [verb: "list", kind: string, labelSelector: string]
  | readonly [verb: "patch", kind: string, name: string, patch: SandboxPatch]
  | readonly [verb: "exec", ...ExecArgs];

interface FakeKubeOptions {
  /** What a command run in a container comes to: its result, or how the exec failed. */
  readonly onExec?: (...args: ExecArgs) => ExecResult | KubeExecError;
  /** How deleting the object of `kind` and `name` fails, if it does. */
  readonly onRemove?: (kind: string, name: string) => KubeApiError | undefined;
}

/** The Ready condition agent-sandbox's controller sets once it has seen `generation`. */
const readyCondition = (observedGeneration: number): V1Condition => ({
  type: "Ready",
  status: "True",
  observedGeneration,
  reason: "Ready",
  message: "",
  lastTransitionTime: new Date(0),
});

/** The API's refusal `status`, as the client fails with it. */
const refusal = (status: number, message: string) => new KubeApiError({ status, cause: new Error(message) });

/**
 * An API server of Sandboxes, Secrets and Pods in memory, as a KubeClient. `ready(name)`
 * marks a Sandbox's pod ready, as agent-sandbox's controller would; `exec` answers with
 * `onExec`, and `remove` fails as `onRemove` says.
 */
function fakeKube({ onExec = () => ({ exitCode: 0, stdout: Buffer.alloc(0), stderr: "" }), onRemove = () => undefined }: FakeKubeOptions = {}) {
  const objects = new Map<string, KeptObject>();
  const key = (kind: string | undefined, namespace: string | undefined, name: string | undefined) => `${kind}/${namespace}/${name}`;
  let uid = 0;
  const calls: KubeCall[] = [];
  const read = (kind: string, namespace: string, name: string): KeptObject | null => {
    const object = objects.get(key(kind, namespace, name));
    if (!object) return null;
    const copy = structuredClone(object);
    if (kind === "Secret" && copy.stringData) {
      copy.data = Object.fromEntries(Object.entries(copy.stringData).map(([k, v]) => [k, Buffer.from(v).toString("base64")]));
      delete copy.stringData;
    }
    return copy;
  };
  const create = <T extends KubernetesObject>(object: T): Effect.Effect<T, KubeApiError> =>
    Effect.suspend(() => {
      // As the API server names an object by its generateName.
      const name = object.metadata?.name ?? `${object.metadata?.generateName}${uid + 1}`;
      calls.push(["create", object.kind, name]);
      const k = key(object.kind, object.metadata?.namespace, name);
      if (objects.has(k)) return Effect.fail(refusal(409, "exists"));
      const stored = { ...structuredClone(object), metadata: { ...structuredClone(object.metadata), name, uid: `uid-${++uid}`, generation: 1 } };
      objects.set(k, stored);
      return Effect.succeed(structuredClone(stored));
    });
  const patch = (kind: string, namespace: string, name: string, change: SandboxPatch): Effect.Effect<KeptObject, KubeApiError> =>
    Effect.suspend(() => {
      calls.push(["patch", kind, name, change]);
      const object = objects.get(key(kind, namespace, name));
      if (!object) return Effect.fail(refusal(404, "absent"));
      const { metadata, spec } = applyMergePatch(object, structuredClone(change)) as KeptObject;
      object.metadata = metadata;
      object.spec = spec ?? {};
      // As the API server does: a change of spec is a new generation.
      if (change.spec) object.metadata.generation += 1;
      return Effect.succeed(structuredClone(object));
    });
  const service = KubeClient.of({
    read: (_apiVersion, kind, namespace, name) => Effect.sync(() => {
      calls.push(["read", kind, name]);
      return read(kind, namespace, name);
    }),
    create,
    list: (_apiVersion, kind, namespace, labelSelector) => Effect.sync(() => {
      calls.push(["list", kind, labelSelector]);
      const wanted = labelSelector.split(",").map((term) => term.split("="));
      return [...objects.values()]
        .filter(({ kind: kept, metadata }) => kept === kind && metadata.namespace === namespace && wanted.every(([label, value]) => metadata.labels?.[label ?? ""] === value))
        .map((object) => structuredClone(object));
    }),
    replace: () => Effect.die("alasio replaces no object"),
    patch: (_apiVersion, kind, namespace, name, change) => patch(kind, namespace, name, change),
    patchPodStatus: () => Effect.die("no pod's status is patched"),
    remove: (_apiVersion, kind, namespace, name) => Effect.suspend(() => {
      calls.push(["remove", kind, name]);
      const refused = onRemove(kind, name);
      if (refused) return Effect.fail(refused);
      objects.delete(key(kind, namespace, name));
      for (const [k, object] of objects) {
        if (object.metadata.ownerReferences?.some((owner) => owner.name === name && owner.kind === kind)) objects.delete(k);
      }
      return Effect.void;
    }),
    watch: () => Stream.die("nothing is watched"),
    exec: (...args) => Effect.suspend(() => {
      calls.push(["exec", ...args]);
      const result = onExec(...args);
      return result instanceof KubeExecError ? Effect.fail(result) : Effect.succeed(result);
    }),
  });
  return {
    objects,
    calls,
    layer: Layer.succeed(KubeClient, service),
    /** What the API server holds, read as alasio would, outside the calls it records. */
    peek: read,
    create: (object: KubernetesObject) => Effect.runSync(create(object)),
    patch: (kind: string, namespace: string, name: string, change: SandboxPatch) => Effect.runSync(patch(kind, namespace, name, change)),
    ready(namespace: string, name: string) {
      const sandbox = objects.get(key("Sandbox", namespace, name));
      assert.ok(sandbox, `Sandbox ${namespace}/${name} exists`);
      sandbox.status = {
        serviceFQDN: `${name}.${namespace}.svc.cluster.local`,
        conditions: [readyCondition(sandbox.metadata.generation)],
      };
    },
    /**
     * Acts on the Sandbox every few milliseconds as agent-sandbox's controller does: ends
     * the pod of a suspended one, saying so, and starts a running one's from its current
     * pod template, on the node `node-a`, recorded in `pods`, unless `fails` gives a
     * reason that pod ends at once with; a change of a running one's spec leaves its pod
     * as it is. Stops when `stop()` is called.
     */
    control(namespace: string, name: string, fails: (sandbox: KeptObject) => string | null = () => null) {
      const pods: string[] = [];
      const timer = setInterval(() => {
        const sandbox = objects.get(key("Sandbox", namespace, name));
        if (!sandbox) return;
        const generation = sandbox.metadata.generation;
        if (sandbox.spec?.operatingMode === "Suspended") {
          objects.delete(key("Pod", namespace, name));
          sandbox.status = { conditions: [{ ...readyCondition(generation), type: "Suspended", reason: "SandboxSuspendedPodTerminated" }] };
        } else if (!sandboxReady(sandbox) && !podEnded(sandbox) && objects.has(key("Pod", namespace, name))) {
          this.ready(namespace, name);
        } else if (!sandboxReady(sandbox) && !podEnded(sandbox)) {
          // As agent-sandbox, which replaces no pod that has ended.
          pods.push(String(sandbox.spec?.podTemplate?.spec?.containers[0]?.image));
          objects.set(key("Pod", namespace, name), {
            apiVersion: "v1",
            kind: "Pod",
            metadata: { name, namespace, uid: `uid-${++uid}`, generation: 1 },
            spec: { containers: structuredClone(sandbox.spec?.podTemplate?.spec?.containers ?? []), nodeName: "node-a" },
          });
          const ended = fails(sandbox);
          if (ended === null) this.ready(namespace, name);
          else sandbox.status = { conditions: [{ ...readyCondition(generation), status: "False", reason: ended, message: "" }] };
        }
      }, 5);
      return { pods, stop: () => clearInterval(timer) };
    },
  };
}

type FakeKube = ReturnType<typeof fakeKube>;

/** bayma as seen over HTTP: 401 without the token, 400 with it (no MCP session). */
function fakeBayma(kube: FakeKube, namespace: string): typeof fetch {
  return async (url, init) => {
    const name = new URL(url instanceof Request ? url.url : url).hostname.split(".")[0];
    const encoded = kube.peek("Secret", namespace, `${name}-bayma-token`)?.data?.["token"];
    assert.ok(encoded, `${name} has a token Secret`);
    const token = Buffer.from(encoded, "base64").toString();
    return new Response(null, { status: new Headers(init?.headers).get("Authorization") === `Bearer ${token}` ? 400 : 401 });
  };
}

/** `effect` run against `kube`, reaching bayma with `fetch`, in a scope of its own. */
function onKube<A, E>(kube: FakeKube, fetch: typeof globalThis.fetch, effect: Effect.Effect<A, E, KubeClient | Scope.Scope>): Promise<A> {
  return Effect.runPromise(Effect.scoped(effect).pipe(Effect.provide(Layer.merge(kube.layer, Layer.succeed(FetchHttpClient.Fetch, fetch)))));
}

/** Waits `ms` of real time, as the fake controller takes to mark a Sandbox ready. */
const after = (ms: number, then: () => void) => setTimeout(then, ms);

test("the deployment's templates are checked as alasio starts", async () => {
  const load = (value: unknown): Result.Result<KubeTemplates, { readonly message: string }> => decodeKubeTemplates("/t.json", JSON.stringify(value));
  const failure = (value: unknown) => Result.match(load(value), { onSuccess: () => assert.fail("decoded"), onFailure: (error) => error.message });
  const host = Result.getOrThrow(load({ host: profile() }));
  assert.equal(host.host?.podTemplate.spec?.containers[0]?.image, "agent@sha256:1");
  assert.equal(Result.getOrThrow(load({})).host, null);
  // A namespace YAML reads as a number is that name.
  assert.equal(Result.getOrThrow(load({ host: { ...profile(), namespace: 123 } })).host?.namespace, "123");
  assert.equal(
    Result.match(decodeKubeTemplates("/t.json", "{"), { onSuccess: () => "", onFailure: (error) => error.message }),
    "ALASIO_KUBE_TEMPLATES /t.json is not readable JSON: Expected property name or '}' in JSON at position 1 (line 1 column 2)",
  );
  assert.equal(failure({ host: { ...profile(), namespace: "Bad_NS" } }), "ALASIO_KUBE_TEMPLATES host.namespace must be a namespace name");
  assert.equal(failure({ host: { ...profile(), port: 0 } }), "ALASIO_KUBE_TEMPLATES host.port must be a port number");
  assert.equal(
    failure({ host: { ...profile(), podTemplate: { spec: { containers: [{ name: "x" }] } } } }),
    'ALASIO_KUBE_TEMPLATES host.podTemplate.spec.containers must include one named "bayma"',
  );
  assert.equal(failure({ host: { ...profile(), stateRoot: undefined } }), "ALASIO_KUBE_TEMPLATES host.stateRoot must be a path");
  assert.equal(failure({ host: 5 }), "ALASIO_KUBE_TEMPLATES host must be an object");

  const loading = (env: Record<string, string>) =>
    Effect.runPromiseExit(loadKubeTemplates.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })))));
  const unset = await loading({});
  assert.ok(Exit.isFailure(unset));
  assert.equal(Option.getOrThrow(Exit.findErrorOption(unset)).message, "ALASIO_KUBE_TEMPLATES is not set: alasio runs where alasio up installs it");
  const directory = mkdtempSync(join(tmpdir(), "alasio-templates-"));
  try {
    const path = join(directory, "templates.json");
    writeFileSync(path, JSON.stringify({ host: profile() }));
    const loaded = await loading({ ALASIO_KUBE_TEMPLATES: path });
    assert.ok(Exit.isSuccess(loaded));
    assert.equal(loaded.value.host?.namespace, "alasio-host");
    const missing = await loading({ ALASIO_KUBE_TEMPLATES: join(directory, "missing.json") });
    assert.ok(Exit.isFailure(missing));
    assert.match(Option.getOrThrow(Exit.findErrorOption(missing)).message, /^ALASIO_KUBE_TEMPLATES .*missing\.json is not readable JSON: ENOENT/u);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("a token names its Sandbox and is compared whole", () => {
  const token = newToken("fs-abc123", () => "secret");
  assert.equal(token, "fs-abc123.secret");
  const session = (presented: unknown) => Option.map(Schema.decodeUnknownOption(SessionToken)(presented), ([volumeId]) => volumeId);
  assert.deepEqual(session(token), Option.some("fs-abc123"));
  assert.deepEqual(session("nodot"), Option.none());
  assert.deepEqual(session("Bad.secret"), Option.none());
  assert.deepEqual(session(undefined), Option.none());
  assert.equal(sameToken(token, "fs-abc123.secret"), true);
  assert.equal(sameToken(token, "fs-abc123.secreT"), false);
  assert.equal(sameToken(token, "fs-abc123.secret2"), false);
});

test("every Sandbox serves bayma with its token, from a Secret it owns", () => {
  const sandbox = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), labels: { extra: "1" } });
  assert.equal(sandbox.spec.operatingMode, "Running");
  assert.equal(sandbox.spec.service, true);
  assert.deepEqual(sandbox.metadata.labels, { "app.kubernetes.io/managed-by": "alasio", "alasio.dev/sandbox": "fs-abc123", extra: "1" });
  assert.deepEqual(sandbox.spec.podTemplate.metadata?.labels, { team: "a", ...sandbox.metadata.labels });
  const podSpec = sandbox.spec.podTemplate.spec;
  assert.ok(podSpec);
  const [bayma] = podSpec.containers;
  assert.ok(bayma);
  assert.deepEqual(bayma.args, ["mcp-http", "--port", "7290", "--token-file", "/run/alasio/bayma/token"]);
  assert.deepEqual(bayma.volumeMounts, [{ name: "alasio-bayma-token", mountPath: "/run/alasio/bayma", readOnly: true }]);
  assert.deepEqual(podSpec.volumes?.at(-1), {
    name: "alasio-bayma-token",
    secret: { secretName: "fs-abc123-bayma-token", defaultMode: 0o440 },
  });
  assert.equal(sandbox.spec.volumeClaimTemplates, undefined);
  assert.equal(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), operatingMode: "Suspended" }).spec.operatingMode, "Suspended");
  assert.throws(
    () => sandboxManifest({ name: "x", namespace: "n", template: { podTemplate: { spec: { containers: [{ name: "other" }] } } } }),
    /no container named "bayma"/,
  );

  const secret = tokenSecretManifest({ metadata: { name: "fs-abc123", namespace: "alasio-sessions", uid: "u1" } }, "t");
  assert.deepEqual(secret.metadata?.ownerReferences, [{
    apiVersion: "agents.x-k8s.io/v1beta1", kind: "Sandbox", name: "fs-abc123", uid: "u1", controller: true, blockOwnerDeletion: true,
  }]);
  assert.deepEqual(secret.stringData, { token: "t" });
});

test("a Sandbox is ready only once its controller has seen its current spec", () => {
  const sandbox = (generation: number, observed: number) => ({
    metadata: { generation },
    status: { conditions: [readyCondition(observed)] },
  });
  assert.equal(sandboxReady(sandbox(2, 2)), true);
  assert.equal(sandboxReady(sandbox(3, 2)), false);
  assert.equal(sandboxReady({ metadata: { generation: 1 } }), false);
});

test("ensure makes the Sandbox and its token, waits for bayma to answer, and is shared by callers asking at once", async () => {
  const kube = fakeKube();
  const fetches: Parameters<typeof fetch>[0][] = [];
  const bayma = fakeBayma(kube, "alasio-sessions");
  let answered = 0;
  // bayma's first answer is the connection refused while its policy applies.
  const fetchBayma: typeof fetch = async (url, options) => {
    fetches.push(url);
    if (answered++ === 0) throw new Error("ECONNREFUSED");
    return await bayma(url, options);
  };
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const { a, b, token } = await onKube(kube, fetchBayma, Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    const [a, b] = yield* Effect.all([sandboxes.ensure("fs-abc123", manifest), sandboxes.ensure("fs-abc123", manifest)], { concurrency: "unbounded" });
    return { a, b, token: yield* sandboxes.token("fs-abc123") };
  }));
  assert.deepEqual(a, b);
  assert.equal(a.url, "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp");
  assert.match(token ?? "", /^fs-abc123\.[\w-]{43}$/u);
  assert.deepEqual(a.headers, { Authorization: `Bearer ${token}` });
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Sandbox").length, 1);
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Secret").length, 1);
  assert.equal(fetches.length, 2);
});

test("ensure resumes a suspended Sandbox and gives one left without a token its Secret", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(manifest());
  kube.patch("Sandbox", "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
  kube.ready("alasio-sessions", "fs-abc123");
  kube.calls.length = 0;
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    // The resume raises the generation, so the earlier readiness no longer counts.
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    yield* sandboxes.ensure("fs-abc123", manifest);
  }));
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.spec?.operatingMode, "Running");
  assert.equal(kube.peek("Secret", "alasio-sessions", "fs-abc123-bayma-token")?.metadata.ownerReferences?.[0]?.uid, created.metadata?.uid);
  // Made from its current template, it is resumed and nothing else.
  assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch" || verb === "create").map(([verb, kind, , change]) => [verb, kind, change]), [
    ["create", "Secret", undefined],
    ["patch", "Sandbox", { spec: { operatingMode: "Running" } }],
  ]);
});

test("suspend returns once the Sandbox's pod has ended, and leaves one that is gone as it is", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const controller = kube.control("alasio-sessions", "fs-abc123");
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", manifest);
    assert.ok(kube.peek("Pod", "alasio-sessions", "fs-abc123"));
    yield* sandboxes.suspend("fs-abc123");
    assert.equal(kube.peek("Pod", "alasio-sessions", "fs-abc123"), null);
    yield* sandboxes.suspend("fs-gone");
  })).finally(controller.stop);
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.spec?.operatingMode, "Suspended");
});

test("a Sandbox records the pod template it runs, the same for the same template however it is written", () => {
  const hashOf = (template: HostProfile) =>
    sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template }).metadata.annotations?.[POD_TEMPLATE_ANNOTATION];
  const recorded = hashOf(profile());
  assert.match(recorded ?? "", /^[0-9a-f]{64}$/u);
  assert.equal(recorded, podTemplateHash(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() }).spec.podTemplate));
  // The same template with its keys in another order.
  const podTemplate = profile().podTemplate;
  assert.equal(hashOf(profile({ podTemplate: Object.fromEntries(Object.entries(podTemplate).reverse()) })), recorded);
  assert.notEqual(hashOf(profile({ podTemplate: { ...profile().podTemplate, spec: { containers: [{ name: "bayma", image: "agent@sha256:2" }] } } })), recorded);
  // What the caller adds to the Sandbox's own metadata is beside it.
  assert.deepEqual(
    sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile(), annotations: { "alasio.dev/conversation": "telegram:1" } }).metadata.annotations,
    { "alasio.dev/conversation": "telegram:1", [POD_TEMPLATE_ANNOTATION]: recorded },
  );
});

test("a merge patch makes one object into another, removing what the other lacks and giving arrays whole", () => {
  const from = { a: 1, b: { c: 2, d: 3 }, e: [1, 2], f: "same", g: { h: 1 } };
  const to = { a: 1, b: { c: 2, x: 4 }, e: [1], f: "same", g: { h: 1 }, i: { j: true }, k: undefined };
  const patch = mergePatch(from, to);
  assert.deepEqual(patch, { b: { d: null, x: 4 }, e: [1], i: { j: true } });
  assert.deepEqual(mergePatch(to, to), {});
  assert.deepEqual(mergePatch({ a: 1 }, { a: null }), { a: null });
  assert.equal(mergePatch(1, 2), 2);
});

test("a patch is sent addressed to its object, beside what it changes in its metadata", () => {
  const sandbox = ["agents.x-k8s.io/v1beta1", "Sandbox", "alasio-sessions", "fs-abc123"] as const;
  assert.deepEqual(patchBody(...sandbox, { spec: { operatingMode: "Running" } }), {
    apiVersion: "agents.x-k8s.io/v1beta1",
    kind: "Sandbox",
    metadata: { namespace: "alasio-sessions", name: "fs-abc123" },
    spec: { operatingMode: "Running" },
  });
  assert.deepEqual(patchBody(...sandbox, { metadata: { annotations: { a: "1" }, name: "other" }, spec: {} }), {
    apiVersion: "agents.x-k8s.io/v1beta1",
    kind: "Sandbox",
    metadata: { annotations: { a: "1" }, namespace: "alasio-sessions", name: "fs-abc123" },
    spec: {},
  });
});

test("ensure moves a Sandbox made from another pod template onto alasio's, ending its pod and keeping its volumes", async () => {
  const kube = fakeKube();
  const older = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(older);
  // Its pod of the older template runs.
  kube.ready("alasio-sessions", "fs-abc123");
  // A newer bayma, and a label alasio no longer sets.
  const template = profile({
    podTemplate: {
      metadata: {},
      spec: { ...profile().podTemplate.spec, containers: [{ name: "bayma", image: "agent@sha256:2", args: ["mcp-http", "--port", "7290"] }] },
    },
  });
  const current = sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template, annotations: { "alasio.dev/conversation": "telegram:1" } });
  const controller = kube.control("alasio-sessions", "fs-abc123");
  const patches = await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", () => current);
    const patches = kube.calls.filter(([verb]) => verb === "patch").map(([, , , change]) => change);
    // Brought up again, it is on alasio's template, and left as it is.
    kube.calls.length = 0;
    yield* sandboxes.ensure("fs-abc123", () => current);
    assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch"), []);
    return patches;
  })).finally(controller.stop);
  // Suspended with the current template, which ends the older pod, then resumed: one pod, of the current template.
  assert.deepEqual(patches, [
    {
      metadata: { annotations: { [POD_TEMPLATE_ANNOTATION]: current.metadata.annotations?.[POD_TEMPLATE_ANNOTATION] } },
      spec: { operatingMode: "Suspended", podTemplate: mergePatch(older.spec.podTemplate, current.spec.podTemplate) },
    },
    { spec: { operatingMode: "Running" } },
  ]);
  assert.deepEqual(controller.pods, ["agent@sha256:2"]);
  const moved = kube.peek("Sandbox", "alasio-sessions", "fs-abc123");
  assert.ok(moved);
  assert.deepEqual(moved.spec?.podTemplate, current.spec.podTemplate);
  assert.equal(moved.metadata.annotations?.[POD_TEMPLATE_ANNOTATION], current.metadata.annotations?.[POD_TEMPLATE_ANNOTATION]);
  assert.equal(moved.spec?.operatingMode, "Running");
  assert.equal(moved.metadata.uid, created.metadata?.uid, "the Sandbox, and so its volumes, are the same");
  assert.equal(kube.peek("Secret", "alasio-sessions", "fs-abc123-bayma-token")?.metadata.ownerReferences?.[0]?.uid, created.metadata?.uid);
});

test("ensure restarts a Sandbox whose pod has ended, which agent-sandbox never replaces, keeping its volumes", async () => {
  const kube = fakeKube();
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const created = kube.create(manifest());
  // Evicted: the controller says so, for the Sandbox's current spec, and leaves it.
  const sandbox = kube.objects.get("Sandbox/alasio-sessions/fs-abc123");
  assert.ok(sandbox);
  sandbox.status = { conditions: [{ ...readyCondition(sandbox.metadata.generation), status: "False", reason: "PodFailed", message: "Pod failed" }] };
  assert.equal(podEnded(sandbox), true);
  kube.calls.length = 0;
  const controller = kube.control("alasio-sessions", "fs-abc123");
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    yield* sandboxes.ensure("fs-abc123", manifest);
  })).finally(controller.stop);
  assert.deepEqual(kube.calls.filter(([verb]) => verb === "patch").map(([, , , change]) => change), [
    { spec: { operatingMode: "Suspended" } },
    { spec: { operatingMode: "Running" } },
  ]);
  assert.equal(controller.pods.length, 1);
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.metadata.uid, created.metadata?.uid);
});

test("ensure gives up on a Sandbox that does not become ready in five minutes, saying why", async () => {
  const kube = fakeKube();
  const error = await onKube(kube, fetch, Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "ns", port: 7290 });
    const ensuring = yield* Effect.forkChild(Effect.flip(sandboxes.ensure("fs-abc123", () => sandboxManifest({ name: "fs-abc123", namespace: "ns", template: profile() }))));
    let waited = 0;
    while (ensuring.pollUnsafe() === undefined) {
      yield* TestClock.adjust("500 millis");
      waited += 500;
    }
    assert.ok(waited >= 300_000 && waited <= 301_000, `gave up after ${waited} ms`);
    return yield* Fiber.join(ensuring);
  }).pipe(Effect.provide(TestClock.layer())));
  assert.equal(error.message, "Sandbox ns/fs-abc123 did not become ready within 300s");
});

test("an exec's exit code is read from its status", () => {
  assert.deepEqual(exitCodeOf({ status: "Success" }), Result.succeed(0));
  assert.deepEqual(exitCodeOf({ status: "Failure", reason: "NonZeroExitCode", details: { causes: [{ reason: "ExitCode", message: "3" }] } }), Result.succeed(3));
  const failed = exitCodeOf({ status: "Failure", message: "container not found" });
  assert.ok(Result.isFailure(failed));
  assert.equal(failed.failure.message, "exec failed: container not found");
});
