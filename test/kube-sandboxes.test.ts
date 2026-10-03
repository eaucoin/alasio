import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { gzipSync } from "node:zlib";

import type { KubernetesObject, V1Condition, V1ObjectMeta } from "@kubernetes/client-node";
import type { Attributes } from "@opentelemetry/api";
import { ProtobufTraceSerializer } from "@opentelemetry/otlp-transformer";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";

import { type ExecResult, exitCodeOf, type KubeClient } from "../src/kube/client.ts";
import { loadKubeTemplates, type SessionsProfile } from "../src/kube/config.ts";
import {
  createSandboxes,
  newToken,
  type SandboxSpec,
  type SandboxStatus,
  sameToken,
  sandboxManifest,
  sandboxReady,
  tokenSandboxName,
  tokenSecretManifest,
} from "../src/kube/sandboxes.ts";
import { createSandbox, EGRESS_GATE_SCRIPT, sessionSandboxManifest } from "../src/sandbox/index.ts";
import { createRateLimiter, startTelemetryReceiver } from "../src/sandbox/telemetry-receiver.ts";
import type { Signal } from "../src/telemetry/config.ts";
import type { ForwardResult, OtlpEncoding, OtlpForwarder } from "../src/telemetry/forward.ts";

const SANDBOX = ["agents.x-k8s.io/v1beta1", "Sandbox"] as const;

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

type ExecArgs = Parameters<KubeClient["exec"]>;

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

/**
 * An API server of Sandboxes and Secrets in memory. `ready(name)` marks a Sandbox's pod
 * ready, as agent-sandbox's controller would; `exec` answers with `onExec`.
 */
function fakeKube({ onExec = () => ({ exitCode: 0, stdout: Buffer.alloc(0), stderr: "" }) }: FakeKubeOptions = {}) {
  const objects = new Map<string, KeptObject>();
  const key = (kind: string | undefined, namespace: string | undefined, name: string | undefined) => `${kind}/${namespace}/${name}`;
  let uid = 0;
  const calls: KubeCall[] = [];
  return {
    objects,
    calls,
    ready(namespace: string, name: string) {
      const sandbox = objects.get(key("Sandbox", namespace, name));
      assert.ok(sandbox, `Sandbox ${namespace}/${name} exists`);
      sandbox.status = {
        serviceFQDN: `${name}.${namespace}.svc.cluster.local`,
        conditions: [readyCondition(sandbox.metadata.generation)],
      };
    },
    async read(_apiVersion: string, kind: string, namespace: string, name: string): Promise<KeptObject | null> {
      calls.push(["read", kind, name]);
      const object = objects.get(key(kind, namespace, name));
      if (!object) return null;
      const copy = structuredClone(object);
      if (kind === "Secret" && copy.stringData) {
        copy.data = Object.fromEntries(Object.entries(copy.stringData).map(([k, v]) => [k, Buffer.from(v).toString("base64")]));
        delete copy.stringData;
      }
      return copy;
    },
    async create<T extends KubernetesObject>(object: T): Promise<T> {
      calls.push(["create", object.kind, object.metadata?.name]);
      const k = key(object.kind, object.metadata?.namespace, object.metadata?.name);
      if (objects.has(k)) throw Object.assign(new Error("exists"), { code: 409 });
      const stored = { ...structuredClone(object), metadata: { ...structuredClone(object.metadata), uid: `uid-${++uid}`, generation: 1 } };
      objects.set(k, stored);
      return structuredClone(stored);
    },
    async patch(_apiVersion: string, kind: string, namespace: string, name: string, patch: SandboxPatch): Promise<KeptObject> {
      calls.push(["patch", kind, name, patch]);
      const object = objects.get(key(kind, namespace, name));
      if (!object) throw Object.assign(new Error("absent"), { code: 404 });
      object.spec = { ...object.spec, ...patch.spec };
      object.metadata.generation += 1;
      return structuredClone(object);
    },
    async remove(_apiVersion: string, kind: string, namespace: string, name: string): Promise<void> {
      calls.push(["remove", kind, name]);
      objects.delete(key(kind, namespace, name));
      for (const [k, object] of objects) {
        if (object.metadata.ownerReferences?.some((owner) => owner.name === name && owner.kind === kind)) objects.delete(k);
      }
    },
    async exec(...args: ExecArgs): Promise<ExecResult> {
      calls.push(["exec", ...args]);
      return onExec(...args);
    },
  };
}

type FakeKube = ReturnType<typeof fakeKube>;

/** bayma as seen over HTTP: 401 without the token, 400 with it (no MCP session). */
function fakeBayma(kube: FakeKube, namespace: string): typeof fetch {
  return async (url, init) => {
    const name = new URL(url instanceof Request ? url.url : url).hostname.split(".")[0];
    const secret = await kube.read("v1", "Secret", namespace, `${name}-bayma-token`);
    const encoded = secret?.data?.["token"];
    assert.ok(encoded, `${name} has a token Secret`);
    const token = Buffer.from(encoded, "base64").toString();
    return new Response(null, { status: new Headers(init?.headers).get("Authorization") === `Bearer ${token}` ? 400 : 401 });
  };
}

test("the deployment's templates are checked as alasio starts", () => {
  const env = { ALASIO_KUBE_TEMPLATES: "/t.json" };
  const load = (value: unknown) => loadKubeTemplates(env, () => JSON.stringify(value));
  assert.deepEqual(load({ sessions: profile() }).host, null);
  assert.throws(() => loadKubeTemplates({}), /ALASIO_KUBE_TEMPLATES is not set/);
  assert.throws(() => loadKubeTemplates(env, () => "{"), /not readable JSON/);
  assert.throws(() => load({ sessions: { ...profile(), namespace: "Bad_NS" } }), /sessions.namespace/);
  assert.throws(() => load({ sessions: { ...profile(), port: 0 } }), /sessions.port/);
  assert.throws(() => load({ host: { ...profile(), podTemplate: { spec: { containers: [{ name: "x" }] } } } }), /named "bayma"/);
  assert.throws(() => load({ sessions: { ...profile(), workspaceDir: undefined } }), /workspaceDir/);
  assert.throws(() => load({ host: profile() }), /stateRoot/);
});

test("a token names its Sandbox and is compared whole", () => {
  const token = newToken("fs-abc123", () => "secret");
  assert.equal(token, "fs-abc123.secret");
  assert.equal(tokenSandboxName(token), "fs-abc123");
  assert.equal(tokenSandboxName("nodot"), null);
  // What presents no token at all is outside the types, and still names no Sandbox.
  assert.equal(tokenSandboxName(undefined as unknown as string), null);
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
  const sandboxes = createSandboxes({
    kube,
    namespace: "alasio-sessions",
    port: 7290,
    pollMs: 5,
    // bayma's first answer is the connection refused while its policy applies.
    async fetchImpl(url, options) {
      fetches.push(url);
      if (answered++ === 0) throw new Error("ECONNREFUSED");
      return await bayma(url, options);
    },
  });
  const manifest = () => sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() });
  const first = sandboxes.ensure("fs-abc123", manifest);
  const second = sandboxes.ensure("fs-abc123", manifest);
  setTimeout(() => kube.ready("alasio-sessions", "fs-abc123"), 20);
  const [a, b] = await Promise.all([first, second]);
  assert.deepEqual(a, b);
  assert.equal(a.url, "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp");
  const token = await sandboxes.token("fs-abc123");
  assert.match(token ?? "", /^fs-abc123\.[\w-]{43}$/u);
  assert.deepEqual(a.headers, { Authorization: `Bearer ${token}` });
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Sandbox").length, 1);
  assert.equal(kube.calls.filter(([verb, kind]) => verb === "create" && kind === "Secret").length, 1);
  assert.equal(fetches.length, 2);
});

test("ensure resumes a suspended Sandbox and gives one left without a token its Secret", async () => {
  const kube = fakeKube();
  const created = await kube.create(sandboxManifest({ name: "fs-abc123", namespace: "alasio-sessions", template: profile() }));
  await kube.patch(...SANDBOX, "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
  kube.ready("alasio-sessions", "fs-abc123");
  const sandboxes = createSandboxes({ kube, namespace: "alasio-sessions", port: 7290, pollMs: 5, fetchImpl: fakeBayma(kube, "alasio-sessions") });
  const ensured = sandboxes.ensure("fs-abc123", () => assert.fail("an existing Sandbox is not made again"));
  // The resume raises the generation, so the earlier readiness no longer counts.
  setTimeout(() => kube.ready("alasio-sessions", "fs-abc123"), 20);
  await ensured;
  const sandbox = await kube.read(...SANDBOX, "alasio-sessions", "fs-abc123");
  assert.equal(sandbox?.spec?.operatingMode, "Running");
  const secret = await kube.read("v1", "Secret", "alasio-sessions", "fs-abc123-bayma-token");
  assert.equal(secret?.metadata.ownerReferences?.[0]?.uid, created.metadata.uid);
});

test("ensure gives up on a Sandbox that does not become ready, saying why", async () => {
  const kube = fakeKube();
  const sandboxes = createSandboxes({ kube, namespace: "ns", port: 7290, pollMs: 5, readyTimeoutMs: 30 });
  await assert.rejects(
    sandboxes.ensure("fs-abc123", () => sandboxManifest({ name: "fs-abc123", namespace: "ns", template: profile() })),
    /Sandbox ns\/fs-abc123 did not become ready within 0.03s/,
  );
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
  const sandbox = createSandbox({
    templates: { sessions: profile(), host: null },
    stateDir: "/tmp/alasio-kube-test",
    env: {},
    kube,
    createForwarder: () => assert.fail("no telemetry is exported"),
    fetchImpl: fakeBayma(kube, "alasio-sessions"),
  });
  assert.ok(sandbox);
  const creating = sandbox.volumes.create("fs-abc123", "full");
  setTimeout(() => kube.ready("alasio-sessions", "fs-abc123"), 20);
  assert.deepEqual(await creating, { volumeId: "fs-abc123", netMode: "full" });
  const made = await kube.read(...SANDBOX, "alasio-sessions", "fs-abc123");
  assert.equal(made?.metadata.labels?.["alasio.dev/net-mode"], "full");
  const { bayma } = await sandbox.ensureSession("fs-abc123");
  assert.equal(bayma.url, "http://fs-abc123.alasio-sessions.svc.cluster.local:7290/mcp");

  assert.deepEqual(await sandbox.readFile("fs-abc123", "out/a.txt", 100), { bytes: Buffer.from("hello") });
  const exec = kube.calls.find((call): call is Extract<KubeCall, readonly ["exec", ...ExecArgs]> => call[0] === "exec");
  assert.ok(exec);
  const [, namespace, pod, container, command, options] = exec;
  assert.deepEqual([namespace, pod, container], ["alasio-sessions", "fs-abc123", "bayma"]);
  assert.deepEqual(command.slice(3), ["sh", "out/a.txt", "100", "/workspace"]);
  assert.deepEqual(options, { maxBytes: 101 });
  assert.deepEqual(await sandbox.readFile("fs-abc123", "missing.txt", 100), { note: "file not found" });
  assert.match((await sandbox.readFile("fs-abc123", "big.bin", 100)).note ?? "", /over/);

  await kube.patch(...SANDBOX, "alasio-sessions", "fs-abc123", { spec: { operatingMode: "Suspended" } });
  assert.deepEqual(await sandbox.readFile("fs-abc123", "out/a.txt", 100), { note: "the session is not running" });
  await sandbox.volumes.destroy("fs-abc123");
  assert.equal(await kube.read(...SANDBOX, "alasio-sessions", "fs-abc123"), null);
  await sandbox.close();
});

test("without a sessions template there are no session filesystems", () => {
  assert.equal(createSandbox({ templates: { sessions: null, host: { ...profile(), stateRoot: "/state" } }, stateDir: "/tmp", kube: fakeKube() }), null);
});

test("an exec's exit code is read from its status", () => {
  assert.equal(exitCodeOf({ status: "Success" }), 0);
  assert.equal(exitCodeOf({ status: "Failure", reason: "NonZeroExitCode", details: { causes: [{ reason: "ExitCode", message: "3" }] } }), 3);
  assert.throws(() => exitCodeOf({ status: "Failure", message: "container not found" }), /container not found/);
});

test("a session's byte budget refills over time and asks an exporter to wait when spent", () => {
  let now = 0;
  const limiter = createRateLimiter({ rate: 100, burst: 300, now: () => now });
  assert.equal(limiter.take("a", 250), 0);
  assert.equal(limiter.take("a", 100), 1);
  assert.equal(limiter.take("b", 300), 0);
  now = 1000;
  assert.equal(limiter.take("a", 100), 0);
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
  const receiver = await startTelemetryReceiver({
    port: 0,
    host: "127.0.0.1",
    forwarder,
    stampFor: (volumeId) => ({ "service.name": "bayma", "alasio.volume.id": volumeId }),
    authenticate: async (token) => (token === "fs-abc123.good" ? "fs-abc123" : null),
    limiter: createRateLimiter({ rate: 1, burst: 100_000 }),
  });
  const url = (path: string) => `http://127.0.0.1:${receiver.port}${path}`;
  const post = (path: string, { token = "fs-abc123.good", body = traceRequest({ "service.name": "liar", "alasio.volume.id": "fs-other" }), type = "application/x-protobuf", gzip = false }: ReceiverPost = {}) => fetch(url(path), {
    method: "POST",
    headers: { "content-type": type, ...(token ? { authorization: `Bearer ${token}` } : {}), ...(gzip ? { "content-encoding": "gzip" } : {}) },
    body: gzip ? gzipSync(body) : body,
  });
  try {
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
    const json = await post("/v1/logs", { type: "application/json", body: Buffer.from("{}") });
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
  } finally {
    await receiver.close();
  }
});
