import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import type { KubernetesObject, V1Condition, V1ObjectMeta } from "@kubernetes/client-node";
import type { Attributes } from "@opentelemetry/api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { ConfigProvider, Context, Effect, Exit, Fiber, Layer, Option, Result, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";
import { TestClock } from "effect/testing";

import { type ExecResult, exitCodeOf, KubeApiError, KubeClient } from "../src/kube/client.ts";
import { decodeKubeTemplates, type KubeTemplates, loadKubeTemplates, type SessionsProfile } from "../src/kube/config.ts";
import {
  makeSandboxes,
  newToken,
  type SandboxSpec,
  type SandboxStatus,
  sameToken,
  sandboxManifest,
  sandboxReady,
  tokenSecretManifest,
} from "../src/kube/sandboxes.ts";
import { EGRESS_GATE_SCRIPT, SessionSandboxes, sessionFilesystemsFacade, sessionSandboxManifest } from "../src/sandbox/index.ts";
import { SessionToken } from "../src/sandbox/names.ts";
import { makeRateLimiter, serveTelemetryReceiver } from "../src/sandbox/telemetry-receiver.ts";
import { effectRunner } from "../src/shared/effects.ts";
import type { Signal } from "../src/telemetry/config.ts";
import type { ForwardResult, OtlpEncoding, OtlpForwarder } from "../src/telemetry/forward.ts";

const profile = (extra: Partial<SessionsProfile> = {}): SessionsProfile => ({
  namespace: "alasio-sessions",
  port: 7290,
  workspaceDir: "/workspace",
  podTemplate: {
    metadata: { labels: { team: "a" } },
    spec: {
      containers: [{ name: "bayma", image: "agent@sha256:1", args: ["mcp-http", "--port", "7290"], securityContext: { runAsUser: 1000 } }],
      initContainers: [{ name: "prepare", image: "agent@sha256:1" }],
      volumes: [{ name: "tmp", emptyDir: {} }],
    },
  },
  volumeClaimTemplates: [{ metadata: { name: "data" }, spec: { resources: { requests: { storage: "1Gi" } } } }],
  ...extra,
});

/** A Sandbox or Secret as the fake API server keeps it, with the uid and generation it gave it. */
interface KeptObject extends KubernetesObject {
  metadata: V1ObjectMeta & { uid: string; generation: number };
  spec?: Partial<SandboxSpec>;
  status?: SandboxStatus;
  data?: Record<string, string>;
  stringData?: Record<string, string>;
}

/** A merge patch of a Sandbox's spec, the only kind alasio makes. */
interface SandboxPatch {
  readonly spec?: Partial<SandboxSpec>;
}

type ExecArgs = Parameters<KubeClient["Service"]["exec"]>;

/** A call the fake API server was made, with what identifies it. */
type KubeCall =
  | readonly [verb: "read" | "create" | "remove", kind: string | undefined, name: string | undefined]
  | readonly [verb: "patch", kind: string, name: string, patch: SandboxPatch]
  | readonly [verb: "exec", ...ExecArgs];

interface FakeKubeOptions {
  readonly onExec?: (...args: ExecArgs) => ExecResult;
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
 * An API server of Sandboxes and Secrets in memory, as a KubeClient. `ready(name)` marks
 * a Sandbox's pod ready, as agent-sandbox's controller would; `exec` answers with `onExec`.
 */
function fakeKube({ onExec = () => ({ exitCode: 0, stdout: Buffer.alloc(0), stderr: "" }) }: FakeKubeOptions = {}) {
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
      calls.push(["create", object.kind, object.metadata?.name]);
      const k = key(object.kind, object.metadata?.namespace, object.metadata?.name);
      if (objects.has(k)) return Effect.fail(refusal(409, "exists"));
      const stored = { ...structuredClone(object), metadata: { ...structuredClone(object.metadata), uid: `uid-${++uid}`, generation: 1 } };
      objects.set(k, stored);
      return Effect.succeed(structuredClone(stored));
    });
  const patch = (kind: string, namespace: string, name: string, change: SandboxPatch): Effect.Effect<KeptObject, KubeApiError> =>
    Effect.suspend(() => {
      calls.push(["patch", kind, name, change]);
      const object = objects.get(key(kind, namespace, name));
      if (!object) return Effect.fail(refusal(404, "absent"));
      object.spec = { ...object.spec, ...change.spec };
      object.metadata.generation += 1;
      return Effect.succeed(structuredClone(object));
    });
  const service = KubeClient.of({
    read: (_apiVersion, kind, namespace, name) => Effect.sync(() => {
      calls.push(["read", kind, name]);
      return read(kind, namespace, name);
    }),
    create,
    replace: () => Effect.die("alasio replaces no object"),
    patch: (_apiVersion, kind, namespace, name, change) => patch(kind, namespace, name, change),
    remove: (_apiVersion, kind, namespace, name) => Effect.sync(() => {
      calls.push(["remove", kind, name]);
      objects.delete(key(kind, namespace, name));
      for (const [k, object] of objects) {
        if (object.metadata.ownerReferences?.some((owner) => owner.name === name && owner.kind === kind)) objects.delete(k);
      }
    }),
    exec: (...args) => Effect.sync(() => {
      calls.push(["exec", ...args]);
      return onExec(...args);
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
  const sessions = Result.getOrThrow(load({ sessions: profile() }));
  assert.equal(sessions.host, null);
  assert.equal(sessions.sessions?.podTemplate.spec?.containers[0]?.image, "agent@sha256:1");
  // A namespace YAML reads as a number is that name.
  assert.equal(Result.getOrThrow(load({ sessions: { ...profile(), namespace: 123 } })).sessions?.namespace, "123");
  assert.equal(
    Result.match(decodeKubeTemplates("/t.json", "{"), { onSuccess: () => "", onFailure: (error) => error.message }),
    "ALASIO_KUBE_TEMPLATES /t.json is not readable JSON: Expected property name or '}' in JSON at position 1 (line 1 column 2)",
  );
  assert.equal(failure({ sessions: { ...profile(), namespace: "Bad_NS" } }), "ALASIO_KUBE_TEMPLATES sessions.namespace must be a namespace name");
  assert.equal(failure({ sessions: { ...profile(), port: 0 } }), "ALASIO_KUBE_TEMPLATES sessions.port must be a port number");
  assert.equal(
    failure({ host: { ...profile(), stateRoot: "/s", podTemplate: { spec: { containers: [{ name: "x" }] } } } }),
    'ALASIO_KUBE_TEMPLATES host.podTemplate.spec.containers must include one named "bayma"',
  );
  assert.equal(
    failure({ sessions: { ...profile(), podTemplate: { spec: { containers: { name: "bayma" } } } } }),
    'ALASIO_KUBE_TEMPLATES sessions.podTemplate.spec.containers must include one named "bayma"',
  );
  assert.equal(failure({ sessions: { ...profile(), workspaceDir: undefined } }), "ALASIO_KUBE_TEMPLATES sessions.workspaceDir must be a path");
  assert.equal(failure({ host: profile() }), "ALASIO_KUBE_TEMPLATES host.stateRoot must be a path");
  assert.equal(failure({ sessions: 5 }), "ALASIO_KUBE_TEMPLATES sessions must be an object");

  const loading = (env: Record<string, string>) =>
    Effect.runPromiseExit(loadKubeTemplates.pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env })))));
  const unset = await loading({});
  assert.ok(Exit.isFailure(unset));
  assert.equal(Option.getOrThrow(Exit.findErrorOption(unset)).message, "ALASIO_KUBE_TEMPLATES is not set: alasio runs where its Helm chart deploys it");
  const directory = mkdtempSync(join(tmpdir(), "alasio-templates-"));
  try {
    const path = join(directory, "templates.json");
    writeFileSync(path, JSON.stringify({ sessions: profile() }));
    const loaded = await loading({ ALASIO_KUBE_TEMPLATES: path });
    assert.ok(Exit.isSuccess(loaded));
    assert.equal(loaded.value.sessions?.namespace, "alasio-sessions");
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
  assert.equal(sandbox.spec.volumeClaimTemplates?.[0]?.metadata?.name, "data");
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
  const created = kube.create(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() }));
  kube.patch("Sandbox", "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
  kube.ready("alasio-sessions", "fs-abc123");
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sandboxes = yield* makeSandboxes({ namespace: "alasio-sessions", port: 7290, poll: "5 millis" });
    // The resume raises the generation, so the earlier readiness no longer counts.
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    yield* sandboxes.ensure("fs-abc123", () => assert.fail("an existing Sandbox is not made again"));
  }));
  assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.spec?.operatingMode, "Running");
  assert.equal(kube.peek("Secret", "alasio-sessions", "fs-abc123-bayma-token")?.metadata.ownerReferences?.[0]?.uid, created.metadata?.uid);
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

test("a session's Sandbox is confined by its labels, DNS and egress gate, and exports telemetry with its token", () => {
  const telemetry = { endpoint: "http://10.43.0.9:4318", env: { OTEL_TRACES_EXPORTER: "otlp" } };
  const none = sessionSandboxManifest({ volumeId: "fs-abc123", netMode: "none", profile: profile(), telemetry });
  assert.equal(none.metadata.labels?.["alasio.dev/net-mode"], "none");
  assert.equal(none.metadata.labels?.["alasio.dev/workload"], "session");
  const spec = none.spec.podTemplate.spec;
  assert.ok(spec);
  assert.equal(spec.automountServiceAccountToken, false);
  assert.equal(spec.enableServiceLinks, false);
  assert.equal(spec.dnsPolicy, "None");
  assert.deepEqual(spec.dnsConfig, { nameservers: ["127.0.0.1"] });
  assert.deepEqual(spec.initContainers?.map((container) => container.name), ["egress-gate", "prepare"]);
  const [gate] = spec.initContainers ?? [];
  assert.ok(gate);
  assert.deepEqual(gate.command, ["node", "-e", EGRESS_GATE_SCRIPT]);
  assert.equal(gate.image, "agent@sha256:1");
  assert.deepEqual(gate.securityContext, { runAsUser: 1000 });
  const [bayma] = spec.containers;
  assert.ok(bayma?.env);
  const env = Object.fromEntries(bayma.env.map(({ name, value, valueFrom }) => [name, value ?? valueFrom]));
  assert.deepEqual(env["ALASIO_SANDBOX_TOKEN"], { secretKeyRef: { name: "fs-abc123-bayma-token", key: "token" } });
  assert.equal(env["OTEL_EXPORTER_OTLP_ENDPOINT"], "http://10.43.0.9:4318");
  assert.equal(env["OTEL_EXPORTER_OTLP_HEADERS"], "authorization=Bearer%20$(ALASIO_SANDBOX_TOKEN)");
  assert.equal(env["OTEL_TRACES_EXPORTER"], "otlp");

  const full = sessionSandboxManifest({ volumeId: "fs-abc123", netMode: "full", profile: profile({ egressGate: false }), telemetry: null });
  assert.equal(full.metadata.labels?.["alasio.dev/net-mode"], "full");
  const fullSpec = full.spec.podTemplate.spec;
  assert.ok(fullSpec);
  assert.deepEqual(fullSpec.dnsConfig, { nameservers: ["1.1.1.1", "8.8.8.8"] });
  assert.deepEqual(fullSpec.initContainers?.map((container) => container.name), ["prepare"]);
  const [fullBayma] = fullSpec.containers;
  assert.ok(fullBayma);
  assert.equal(fullBayma.env, undefined);
  assert.throws(() => sessionSandboxManifest({ volumeId: "Bad", netMode: "none", profile: profile(), telemetry: null }), /invalid session volume id/);
});

/** The egress gate against an API server on `port` of this machine, given 1.5s: its exit code and stderr. */
function runGate(port: number) {
  return new Promise<{ code: number | null; stderr: string }>((resolve) => {
    const child = spawn(process.execPath, ["-e", EGRESS_GATE_SCRIPT.replace("120000", "1500")], {
      env: { KUBERNETES_SERVICE_HOST: "127.0.0.1", KUBERNETES_SERVICE_PORT: String(port) },
    });
    let stderr = "";
    child.stderr.on("data", (chunk) => { stderr += chunk; });
    child.on("exit", (code) => resolve({ code, stderr }));
  });
}

test("the egress gate passes once the API server is unreachable, and fails a pod whose egress stays open", async () => {
  // Nothing listens on port 9 here: connections are refused, as egress policy refuses them.
  assert.equal((await runGate(9)).code, 0);
  const server = createServer().listen(0, "127.0.0.1");
  await once(server, "listening");
  try {
    // Listening on a TCP port, the server's address is an AddressInfo.
    const open = await runGate((server.address() as AddressInfo).port);
    assert.equal(open.code, 1);
    assert.match(open.stderr, /egress is not confined/);
  } finally {
    server.close();
  }
});

test("a session on Kubernetes is made when created, and its files are read as its agent through exec", async () => {
  const kube = fakeKube({
    onExec: (_namespace, _pod, _container, command) => {
      const path = command[4];
      if (path === "big.bin") return { exitCode: 4, stdout: Buffer.alloc(0), stderr: "9999999" };
      if (path === "missing.txt") return { exitCode: 3, stdout: Buffer.alloc(0), stderr: "" };
      return { exitCode: 0, stdout: Buffer.from("hello"), stderr: "" };
    },
  });
  const layer = SessionSandboxes.layer({
    profile: profile(),
    stateDir: "/tmp/alasio-kube-test",
    env: {},
    createForwarder: () => assert.fail("no telemetry is exported"),
  });
  await onKube(kube, fakeBayma(kube, "alasio-sessions"), Effect.gen(function*() {
    const sessions = yield* SessionSandboxes;
    after(20, () => kube.ready("alasio-sessions", "fs-abc123"));
    assert.deepEqual(yield* sessions.volumes.create("fs-abc123", "full"), { volumeId: "fs-abc123", netMode: "full" });
    assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123")?.metadata.labels?.["alasio.dev/net-mode"], "full");
    const { bayma } = yield* sessions.ensureSession("fs-abc123");
    assert.equal(bayma.url, "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp");

    assert.deepEqual(yield* sessions.readFile("fs-abc123", "out/a.txt", 100), { bytes: Buffer.from("hello") });
    const exec = kube.calls.find((call): call is Extract<KubeCall, readonly ["exec", ...ExecArgs]> => call[0] === "exec");
    assert.ok(exec);
    const [, namespace, pod, container, command, options] = exec;
    assert.deepEqual([namespace, pod, container], ["alasio-sessions", "fs-abc123", "bayma"]);
    assert.deepEqual(command.slice(3), ["sh", "out/a.txt", "100", "/workspace"]);
    assert.deepEqual(options, { maxBytes: 101 });
    assert.deepEqual(yield* sessions.readFile("fs-abc123", "missing.txt", 100), { note: "file not found" });
    assert.match((yield* sessions.readFile("fs-abc123", "big.bin", 100)).note ?? "", /over/);

    kube.patch("Sandbox", "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
    assert.deepEqual(yield* sessions.readFile("fs-abc123", "out/a.txt", 100), { note: "the session is not running" });
    yield* sessions.volumes.destroy("fs-abc123");
    assert.equal(kube.peek("Sandbox", "alasio-sessions", "fs-abc123"), null);
  }).pipe(Effect.provide(layer), Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} })))));
});

test("the app finds session filesystems only where alasio has them", async () => {
  assert.equal(sessionFilesystemsFacade(effectRunner(Context.empty())), null);
  const kube = fakeKube();
  const layer = SessionSandboxes.layer({ profile: profile(), stateDir: "/tmp/alasio-kube-test", env: {} }).pipe(
    Layer.provide(kube.layer),
    Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} }))),
  );
  const facade = await Effect.runPromise(Effect.scoped(Layer.build(layer).pipe(Effect.map((services) => sessionFilesystemsFacade(effectRunner(services))))));
  assert.equal(facade?.enabled, true);
  assert.deepEqual(kube.calls, []);
});

test("an exec's exit code is read from its status", () => {
  assert.deepEqual(exitCodeOf({ status: "Success" }), Result.succeed(0));
  assert.deepEqual(exitCodeOf({ status: "Failure", reason: "NonZeroExitCode", details: { causes: [{ reason: "ExitCode", message: "3" }] } }), Result.succeed(3));
  const failed = exitCodeOf({ status: "Failure", message: "container not found" });
  assert.ok(Result.isFailure(failed));
  assert.equal(failed.failure.message, "exec failed: container not found");
});

test("a session's byte budget refills over time and asks an exporter to wait when spent", async () => {
  await Effect.runPromise(Effect.gen(function*() {
    const limiter = yield* makeRateLimiter({ rate: 100, burst: 300 });
    assert.equal(yield* limiter.take("a", 250), 0);
    assert.equal(yield* limiter.take("a", 100), 1);
    assert.equal(yield* limiter.take("b", 300), 0);
    yield* TestClock.adjust("1 second");
    assert.equal(yield* limiter.take("a", 100), 0);
  }).pipe(Effect.provide(TestClock.layer())));
});

function traceRequest(resource: Attributes) {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ resource: resourceFromAttributes(resource), spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.getTracer("t").startSpan("work").end();
  const request = ProtobufTraceSerializer.serializeRequest(exporter.getFinishedSpans());
  assert.ok(request);
  return Buffer.from(request);
}

/** What the receiver's `post` sends: a bearer token (or none), a body, its content type, and whether it is gzipped. */
interface ReceiverPost {
  readonly token?: string | null;
  readonly body?: Buffer;
  readonly type?: string;
  readonly gzip?: boolean;
}

test("the receiver takes a session's OTLP with its token only, stamped with what alasio knows", async () => {
  const exported: { signal: Signal; encoding: OtlpEncoding; body: Buffer }[] = [];
  const forwarder = {
    protocols: { traces: "http/protobuf" },
    async export(signal: Signal, encoding: OtlpEncoding, body: Buffer): Promise<ForwardResult> {
      exported.push({ signal, encoding, body });
      return { ok: true };
    },
  } satisfies Pick<OtlpForwarder, "protocols" | "export">;
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const receiver = yield* serveTelemetryReceiver({
      port: 0,
      host: "127.0.0.1",
      forwarder,
      stampFor: (volumeId) => ({ "service.name": "bayma", "alasio.volume.id": volumeId }),
      authenticate: (token) => Effect.succeed(token === "fs-abc123.good" ? "fs-abc123" : null),
      limiter: yield* makeRateLimiter({ rate: 1, burst: 100_000 }),
    });
    const url = (path: string) => `http://127.0.0.1:${receiver.port}${path}`;
    const post = (path: string, { token = "fs-abc123.good", body = traceRequest({ "service.name": "liar", "alasio.volume.id": "fs-other" }), type = "application/x-protobuf", gzip = false }: ReceiverPost = {}) => fetch(url(path), {
      method: "POST",
      headers: { "content-type": type, ...(token ? { authorization: `Bearer ${token}` } : {}), ...(gzip ? { "content-encoding": "gzip" } : {}) },
      body: gzip ? gzipSync(body) : body,
    });
    yield* Effect.promise(async () => {
      assert.equal((await post("/v1/traces", { token: null })).status, 401);
      assert.equal((await post("/v1/traces", { token: "fs-abc123.bad" })).status, 401);
      assert.equal((await post("/v1/spans")).status, 404);
      assert.equal((await post("/v1/traces", { type: "text/plain" })).status, 415);
      assert.equal((await post("/v1/traces", { type: "constructor" })).status, 415);
      assert.equal((await post("/v1/traces", { body: Buffer.from([0x0a, 0xff]) })).status, 400);
      assert.equal((await post("/v1/logs")).status, 200);
      assert.equal(exported.length, 0);

      const response = await post("/v1/traces", { gzip: true });
      assert.equal(response.status, 200);
      // A full success, as the exporter decodes it: an empty protobuf message.
      assert.equal(response.headers.get("content-type"), "application/x-protobuf");
      assert.equal((await response.arrayBuffer()).byteLength, 0);
      const json = await post("/v1/logs", { type: "application/json; charset=utf-8", body: Buffer.from("{}") });
      assert.equal(await json.text(), "{}");
      assert.equal(exported.length, 1);
      const [stamped] = exported;
      assert.ok(stamped);
      const text = stamped.body.toString("latin1");
      assert.match(text, /fs-abc123/u);
      assert.doesNotMatch(text, /liar|fs-other/u);

      const flood = await post("/v1/traces", { body: Buffer.alloc(200_000) });
      assert.equal(flood.status, 429);
      assert.ok(Number(flood.headers.get("retry-after")) > 0);
      const huge = await post("/v1/traces", { body: Buffer.alloc(5 * 1024 * 1024) });
      assert.equal(huge.status, 413);
    });
  })));
});
