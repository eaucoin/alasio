/**
 * Sandboxes: agent-sandbox's `Sandbox` objects (agents.x-k8s.io/v1beta1), each one pod
 * running bayma's MCP over HTTP, with its own Service, volumes and bearer token, which
 * alasio creates, resumes, suspends and deletes. Session filesystems (../sandbox/) and
 * folder workspaces' bayma (../mcp/bayma.ts) are both Sandboxes; they differ in the
 * template they are made from and what each adds.
 *
 * A Sandbox's token is in a Secret of its own, owned by the Sandbox, so it is deleted
 * with it. bayma reads the token from a file (`--token-file`) and refuses any request
 * without it, so a Sandbox is closed to everything but alasio even before the
 * NetworkPolicy that admits only alasio applies to its new pod.
 *
 * agent-sandbox never changes a pod it has made, so a Sandbox records the pod template
 * it runs, and one made from another than alasio's current template (as after an upgrade
 * that brings a newer bayma) is moved onto it the next time alasio brings it up. Nor does
 * it replace a pod that has ended, or one whose fault only a new pod mends, so alasio
 * restarts the Sandbox of either as it brings it up.
 */
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

import type {
  KubernetesObject,
  V1Condition,
  V1Container,
  V1ObjectMeta,
  V1PersistentVolumeClaim,
  V1PodSpec,
  V1PodTemplateSpec,
  V1Secret,
} from "@kubernetes/client-node";
import { Clock, Duration, Effect, Fiber, FiberMap, Option, Schedule, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import { withAlasioSpan } from "../telemetry/index.ts";
import { hasStatus, type KubeApiError, KubeClient, mergePatch } from "./client.ts";

export const SANDBOX_API_VERSION = "agents.x-k8s.io/v1beta1";
export const SANDBOX_KIND = "Sandbox";

/** Whether a Sandbox's pod runs; suspending one keeps its volumes. */
export type SandboxOperatingMode = "Running" | "Suspended";

/** A Sandbox's metadata, which always names it and its namespace. */
export interface SandboxMetadata extends V1ObjectMeta {
  name: string;
  namespace: string;
}

/** What alasio sets of a Sandbox's spec (cli/src/manifests/sandboxes.agents.x-k8s.io.json). */
export interface SandboxSpec {
  operatingMode?: SandboxOperatingMode;
  /** Whether agent-sandbox gives the Sandbox a Service of its own. */
  service?: boolean;
  podTemplate: V1PodTemplateSpec;
  readonly volumeClaimTemplates?: readonly V1PersistentVolumeClaim[];
}

/** What alasio reads of a Sandbox's status. */
export interface SandboxStatus {
  conditions?: V1Condition[];
  serviceFQDN?: string;
}

/** An agent-sandbox Sandbox, as far as alasio makes and reads one. */
export interface Sandbox extends KubernetesObject {
  apiVersion: typeof SANDBOX_API_VERSION;
  kind: typeof SANDBOX_KIND;
  metadata: SandboxMetadata;
  spec: SandboxSpec;
  status?: SandboxStatus;
}

/** What sandboxReady reads of a Sandbox: its generation, and its status. */
export interface SandboxReadiness {
  readonly metadata: Pick<V1ObjectMeta, "generation">;
  readonly status?: SandboxStatus;
}

/** A Sandbox the API server holds, which has given it its uid. */
export interface StoredSandbox extends Sandbox {
  metadata: SandboxMetadata & { uid: string };
}

/** What a Sandbox is made from: a profile's templates (./config.ts). */
export interface SandboxTemplate {
  readonly podTemplate?: V1PodTemplateSpec;
  readonly volumeClaimTemplates?: readonly V1PersistentVolumeClaim[] | undefined;
}

/** What sandboxManifest is given. */
export interface SandboxManifestOptions {
  readonly name: string;
  readonly namespace: string;
  readonly template: SandboxTemplate;
  readonly labels?: Readonly<Record<string, string>>;
  readonly annotations?: Readonly<Record<string, string>>;
  /** Whether the Sandbox is made running, as it is unless told, or suspended, its volumes made and no pod started. */
  readonly operatingMode?: SandboxOperatingMode;
  /** Adds what is the caller's own to the pod spec, whose bayma container is `bayma`, and returns the spec. */
  readonly configure?: (spec: V1PodSpec, bayma: V1Container) => V1PodSpec;
}

/** Where a Sandbox's bayma serves MCP, and the bearer to reach it with. */
export interface BaymaEndpoint {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

/** A Sandbox did not become ready in time: what its Ready condition last said, if anything. */
export class SandboxNotReady extends Schema.TaggedError<SandboxNotReady>()("SandboxNotReady", {
  namespace: Schema.String,
  name: Schema.String,
  /** How long it was given, in seconds. */
  within: Schema.Number,
  reason: Schema.optional(Schema.String),
}) {
  override get message(): string {
    return `Sandbox ${this.namespace}/${this.name} did not become ready within ${this.within}s${this.reason ? `: ${this.reason}` : ""}`;
  }
}

/** A Sandbox was deleted while alasio made it, or while it started. */
export class SandboxGone extends Schema.TaggedError<SandboxGone>()("SandboxGone", {
  namespace: Schema.String,
  name: Schema.String,
  during: Schema.Literals(["was made", "started"]),
}) {
  override get message(): string {
    return `Sandbox ${this.namespace}/${this.name} was deleted while it ${this.during}`;
  }
}

/** A ready Sandbox had no token Secret to reach its bayma with. */
export class SandboxTokenMissing extends Schema.TaggedError<SandboxTokenMissing>()("SandboxTokenMissing", {
  namespace: Schema.String,
  name: Schema.String,
}) {
  override get message(): string {
    return `Sandbox ${this.namespace}/${this.name} has no token Secret`;
  }
}

/** bayma in a ready Sandbox did not answer over its Service in time: why it last did not. */
export class BaymaNotAnswering extends Schema.TaggedError<BaymaNotAnswering>()("BaymaNotAnswering", {
  url: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `bayma in ${this.url} did not answer: ${this.reason}`;
  }
}

/**
 * What is wrong with a running Sandbox's pod that only a new pod mends: `why`, said after
 * "whose", and what to do once the pod is suspended and before the Sandbox resumes.
 */
export interface SandboxFault {
  readonly why: string;
  readonly beforeResume: Effect.Effect<void>;
}

/** How bringing a Sandbox's bayma up fails. */
export type SandboxError = KubeApiError | SandboxNotReady | SandboxGone | SandboxTokenMissing | BaymaNotAnswering;

/** The Sandboxes of one namespace; see makeSandboxes. */
export interface Sandboxes {
  readonly namespace: string;
  /**
   * Makes sure the Sandbox exists (made from `manifest()` when it does not), runs a pod
   * of `manifest()`'s template, replacing one of another, one that has ended, or one
   * with a fault (SandboxesOptions), its volumes kept, and its bayma answers: the MCP
   * endpoint and the bearer to reach it with. Callers asking at once share one bring-up.
   */
  readonly ensure: (name: string, manifest: () => Sandbox) => Effect.Effect<BaymaEndpoint, SandboxError>;
  /** The Sandbox's token, or null when it has none. */
  readonly token: (name: string) => Effect.Effect<string | null, KubeApiError>;
  readonly exists: (name: string) => Effect.Effect<boolean, KubeApiError>;
  /** Suspends the Sandbox, once its pod has ended: nothing in it runs, and its volumes are kept. One that is gone is left so. */
  readonly suspend: (name: string) => Effect.Effect<void, KubeApiError | SandboxNotReady | SandboxGone>;
  readonly remove: (name: string) => Effect.Effect<void, KubeApiError>;
}

/**
 * What makeSandboxes is given: where, bayma's port in every one, how long and how often
 * to wait for one, and what may be wrong with a running one's pod.
 */
export interface SandboxesOptions {
  readonly namespace: string;
  readonly port: number;
  readonly readyTimeout?: Duration.Input;
  readonly poll?: Duration.Input;
  /**
   * What is wrong with the pod of the running Sandbox `name` that only a new pod mends,
   * looked at as it is brought up, or null when nothing is.
   */
  readonly fault?: (name: string) => Effect.Effect<SandboxFault | null>;
}

/** The container in every Sandbox's pod that runs bayma. */
export const BAYMA_CONTAINER = "bayma";
/** Where a Sandbox's token Secret is mounted in its bayma container. */
const TOKEN_DIR = "/run/alasio/bayma";
const TOKEN_KEY = "token";
const TOKEN_VOLUME = "alasio-bayma-token";

/** The label every object alasio makes carries, and the one that names its Sandbox. */
const MANAGED_BY = { "app.kubernetes.io/managed-by": "alasio" };
const SANDBOX_LABEL = "alasio.dev/sandbox";
/** The annotation of a Sandbox that says which pod template it runs: podTemplateHash's. */
export const POD_TEMPLATE_ANNOTATION = "alasio.dev/pod-template";

// A first start pulls the image and provisions the volume, so it is given minutes.
const READY_TIMEOUT: Duration.Input = "5 minutes";
const POLL: Duration.Input = "500 millis";
/** How long one look at whether bayma answers may take. */
const ANSWER_TIMEOUT_MS = 5000;

/** The claim agent-sandbox makes for the Sandbox `sandbox` of its volume claim template `template`. */
export function claimName(template: string, sandbox: string): string {
  return `${template}-${sandbox}`;
}

/** The name of a Sandbox's token Secret. */
export function tokenSecretName(name: string): string {
  return `${name}-bayma-token`;
}

/**
 * A token that names its Sandbox, `<name>.<random>`, so what presents one can be looked
 * up by it (../sandbox/names.ts SessionToken) without a table of tokens.
 */
export function newToken(name: string, random: () => string = () => randomBytes(32).toString("base64url")): string {
  return `${name}.${random()}`;
}

/** Whether `presented` is `expected`, compared in constant time. */
export function sameToken(presented: string, expected: string): boolean {
  const a = Buffer.from(presented);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * A digest of `podTemplate` that two templates share only when they are the same,
 * whatever the order of their keys.
 */
export function podTemplateHash(podTemplate: V1PodTemplateSpec): string {
  const sorted = (_key: string, value: unknown): unknown =>
    typeof value === "object" && value !== null && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : value;
  return createHash("sha256").update(JSON.stringify(podTemplate, sorted)).digest("hex");
}

/**
 * The Sandbox for `name` from a profile's `template` (`{ podTemplate,
 * volumeClaimTemplates }`, as the installation gives it), with what every Sandbox
 * needs: its labels, its operating mode (Running unless told), its Service, bayma given
 * its token, and the hash of the pod template it runs. `configure(podSpec, bayma)` adds
 * what is the caller's own and returns the spec. Pure, for tests.
 */
export function sandboxManifest({
  name,
  namespace,
  template,
  labels = {},
  annotations = {},
  operatingMode = "Running",
  configure = (spec) => spec,
}: SandboxManifestOptions): Sandbox {
  const podTemplate: V1PodTemplateSpec = structuredClone(template.podTemplate ?? {});
  const spec: V1PodSpec = podTemplate.spec ?? { containers: [] };
  const containers = spec.containers ?? [];
  const bayma = containers.find((container) => container.name === BAYMA_CONTAINER);
  if (!bayma) throw new Error(`the Sandbox template for ${namespace} has no container named ${JSON.stringify(BAYMA_CONTAINER)}`);
  bayma.args = [...(bayma.args ?? []), "--token-file", `${TOKEN_DIR}/${TOKEN_KEY}`];
  bayma.volumeMounts = [...(bayma.volumeMounts ?? []), { name: TOKEN_VOLUME, mountPath: TOKEN_DIR, readOnly: true }];
  spec.volumes = [
    ...(spec.volumes ?? []),
    // Readable by the pod's group, which is how a non-root bayma reads it.
    { name: TOKEN_VOLUME, secret: { secretName: tokenSecretName(name), defaultMode: 0o440 } },
  ];
  const allLabels = { ...MANAGED_BY, [SANDBOX_LABEL]: name, ...labels };
  podTemplate.metadata = {
    ...podTemplate.metadata,
    labels: { ...podTemplate.metadata?.labels, ...allLabels },
  };
  podTemplate.spec = configure(spec, bayma);
  return {
    apiVersion: SANDBOX_API_VERSION,
    kind: SANDBOX_KIND,
    metadata: { name, namespace, labels: allLabels, annotations: { ...annotations, [POD_TEMPLATE_ANNOTATION]: podTemplateHash(podTemplate) } },
    spec: {
      operatingMode,
      service: true,
      podTemplate,
      ...(template.volumeClaimTemplates?.length ? { volumeClaimTemplates: structuredClone(template.volumeClaimTemplates) } : {}),
    },
  };
}

/** The token Secret of `sandbox`, owned by it so it goes when the Sandbox does. Pure, for tests. */
export function tokenSecretManifest(sandbox: Pick<StoredSandbox, "metadata">, token: string): V1Secret {
  return {
    apiVersion: "v1",
    kind: "Secret",
    metadata: {
      name: tokenSecretName(sandbox.metadata.name),
      namespace: sandbox.metadata.namespace,
      labels: { ...MANAGED_BY, [SANDBOX_LABEL]: sandbox.metadata.name },
      ownerReferences: [{
        apiVersion: SANDBOX_API_VERSION,
        kind: SANDBOX_KIND,
        name: sandbox.metadata.name,
        uid: sandbox.metadata.uid,
        controller: true,
        blockOwnerDeletion: true,
      }],
    },
    type: "Opaque",
    stringData: { [TOKEN_KEY]: token },
  };
}

function condition(sandbox: SandboxReadiness, type: string): V1Condition | null {
  return sandbox.status?.conditions?.find((entry) => entry.type === type) ?? null;
}

/**
 * `object` as the Sandbox it is: what the API server returns for a Sandbox is one it
 * stores, in its CRD's schema and with the server's own metadata.
 */
function stored(object: KubernetesObject): StoredSandbox {
  return object as StoredSandbox;
}

/** The conditions of a Sandbox alasio waits for. */
type SandboxConditionType = "Ready" | "Suspended";

/** Whether the Sandbox's `type` condition holds, for its current spec. */
function holds(sandbox: SandboxReadiness, type: SandboxConditionType): boolean {
  const found = condition(sandbox, type);
  return found?.status === "True" && (found.observedGeneration ?? 0) >= (sandbox.metadata.generation ?? 0);
}

/** Whether the Sandbox's pod is ready and its Service exists, for its current spec. */
export function sandboxReady(sandbox: SandboxReadiness): boolean {
  return holds(sandbox, "Ready");
}

/** The reasons agent-sandbox gives a Sandbox that is not ready because its pod has ended: failed (evicted, say) or exited. */
const POD_ENDED = new Set(["PodFailed", "PodSucceeded"]);

/** Whether the Sandbox's pod has ended, for its current spec; agent-sandbox makes no other in its place. */
export function podEnded(sandbox: SandboxReadiness): boolean {
  const ready = condition(sandbox, "Ready");
  return ready !== null && POD_ENDED.has(ready.reason) && (ready.observedGeneration ?? 0) >= (sandbox.metadata.generation ?? 0);
}

/**
 * The Sandboxes of `namespace`, on the KubeClient, whose bayma serves on `port`; bayma
 * is reached with FetchHttpClient.Fetch to see that it answers. Bring-ups run in the
 * scope this is made in, which interrupts any still running as it closes.
 */
export const makeSandboxes = Effect.fnUntraced(function*({
  namespace,
  port,
  readyTimeout = READY_TIMEOUT,
  poll = POLL,
  fault,
}: SandboxesOptions): Effect.fn.Return<Sandboxes, never, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;
  const fetch = yield* FetchHttpClient.Fetch;
  const ensuring = yield* FiberMap.make<string, BaymaEndpoint, SandboxError>();
  // A token never changes once its Secret exists, so each is read once.
  const tokens = new Map<string, string>();

  const read = (name: string): Effect.Effect<StoredSandbox | null, KubeApiError> =>
    kube.read(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name).pipe(Effect.map((object) => object && stored(object)));

  const token = Effect.fnUntraced(function*(name: string): Effect.fn.Return<string | null, KubeApiError> {
    const known = tokens.get(name);
    if (known !== undefined) return known;
    // A core Secret, as the API server returns one.
    const secret = (yield* kube.read("v1", "Secret", namespace, tokenSecretName(name))) as V1Secret | null;
    const encoded = secret?.data?.[TOKEN_KEY];
    const value = encoded ? Buffer.from(encoded, "base64").toString("utf8") : null;
    if (value) tokens.set(name, value);
    return value;
  });

  /** The Sandbox, made from `manifest` when it does not exist. */
  const readOrCreate = Effect.fnUntraced(function*(name: string, manifest: Sandbox): Effect.fn.Return<StoredSandbox, KubeApiError | SandboxGone> {
    const existing = yield* read(name);
    if (existing) return existing;
    const created = yield* kube.create(manifest).pipe(
      Effect.map(stored),
      Effect.tap(() => Effect.logInfo(`created Sandbox ${namespace}/${name}`)),
      // Made meanwhile by another alasio: that one is it.
      Effect.catchIf(hasStatus(409), () => read(name)),
    );
    return created ?? (yield* new SandboxGone({ namespace, name, during: "was made" }));
  });

  /**
   * Makes the Sandbox's token Secret unless it has one. Its pod waits for the Secret,
   * so one made just after the Sandbox, or by a later alasio after a failure between
   * the two, starts it.
   */
  const ensureToken = Effect.fnUntraced(function*(sandbox: StoredSandbox): Effect.fn.Return<void, KubeApiError> {
    const name = sandbox.metadata.name;
    if (yield* token(name)) return;
    const value = newToken(name);
    yield* kube.create(tokenSecretManifest(sandbox, value)).pipe(
      Effect.andThen(Effect.sync(() => tokens.set(name, value))),
      Effect.catchIf(hasStatus(409), () => Effect.void),
    );
  });

  const notYet = (sandbox: StoredSandbox, type: SandboxConditionType): SandboxNotReady =>
    new SandboxNotReady({
      namespace,
      name: sandbox.metadata.name,
      within: Duration.toSeconds(readyTimeout),
      reason: condition(sandbox, type)?.message || undefined,
    });

  /** The Sandbox once its controller says its `type` condition holds, looked at every `poll` for `readyTimeout`. */
  const awaitCondition = (initial: StoredSandbox, type: SandboxConditionType): Effect.Effect<StoredSandbox, KubeApiError | SandboxNotReady | SandboxGone> => {
    const name = initial.metadata.name;
    const check = (sandbox: StoredSandbox): Effect.Effect<StoredSandbox, SandboxNotReady> =>
      holds(sandbox, type) ? Effect.succeed(sandbox) : Effect.fail(notYet(sandbox, type));
    const again: Effect.Effect<StoredSandbox, KubeApiError | SandboxNotReady | SandboxGone> = Effect.sleep(poll).pipe(
      Effect.andThen(read(name)),
      Effect.flatMap((sandbox): Effect.Effect<StoredSandbox, SandboxNotReady | SandboxGone> =>
        sandbox ? check(sandbox) : Effect.fail(new SandboxGone({ namespace, name, during: "started" }))
      ),
    );
    return check(initial).pipe(
      Effect.catchTag("SandboxNotReady", () =>
        again.pipe(Effect.retry({ schedule: Schedule.during(readyTimeout), while: (error) => error._tag === "SandboxNotReady" }))),
    );
  };

  /** Waits, for up to `remaining`, until bayma in the Sandbox answers over its Service. */
  const awaitAnswer = (url: string, headers: Readonly<Record<string, string>>, remaining: Duration.Duration): Effect.Effect<void, BaymaNotAnswering> => {
    if (!Duration.isGreaterThan(remaining, Duration.zero)) return Effect.fail(new BaymaNotAnswering({ url, reason: "timed out" }));
    const answered = Effect.tryPromise({
      try: async (signal) => {
        const response = await fetch(url, { method: "GET", headers, signal: AbortSignal.any([signal, AbortSignal.timeout(ANSWER_TIMEOUT_MS)]) });
        await response.body?.cancel();
        return response.status;
      },
      catch: (cause) => new BaymaNotAnswering({ url, reason: cause instanceof Error ? cause.message : String(cause) }),
    }).pipe(
      // Any response is bayma answering: a GET without a session is refused.
      Effect.filterOrFail((status) => status !== 401, () => new BaymaNotAnswering({ url, reason: "bayma refused the Sandbox's token" })),
    );
    return answered.pipe(Effect.retry(Schedule.max([Schedule.spaced(poll), Schedule.during(remaining)])), Effect.asVoid);
  };

  /**
   * `sandbox`, made from another pod template than `desired`'s, given it and suspended:
   * suspending ends its pod and keeps its volumes, so the pod it resumes with is made
   * from that template.
   */
  const moveOnto = Effect.fnUntraced(function*(sandbox: StoredSandbox, desired: Sandbox): Effect.fn.Return<StoredSandbox, KubeApiError | SandboxNotReady | SandboxGone> {
    const name = sandbox.metadata.name;
    yield* Effect.logInfo(`moving Sandbox ${namespace}/${name} onto its current pod template`);
    const moved = stored(yield* kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, {
      metadata: { annotations: { [POD_TEMPLATE_ANNOTATION]: desired.metadata.annotations?.[POD_TEMPLATE_ANNOTATION] } },
      spec: { operatingMode: "Suspended", podTemplate: mergePatch(sandbox.spec.podTemplate, desired.spec.podTemplate) },
    }));
    return yield* awaitCondition(moved, "Suspended");
  });

  /**
   * `sandbox`, whose pod has ended or has a fault only a new pod mends, suspended:
   * agent-sandbox replaces no pod that has ended, so the Sandbox would never be ready
   * again, and the pod it resumes with is new. `why` says which, after "whose", and
   * `beforeResume` is done once it is suspended.
   */
  const restart = Effect.fnUntraced(function*(sandbox: StoredSandbox, why: string, beforeResume: Effect.Effect<void> = Effect.void): Effect.fn.Return<StoredSandbox, KubeApiError | SandboxNotReady | SandboxGone> {
    const name = sandbox.metadata.name;
    yield* Effect.logInfo(`restarting Sandbox ${namespace}/${name}, whose ${why}`);
    const suspended = stored(yield* kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, { spec: { operatingMode: "Suspended" } }));
    const restarted = yield* awaitCondition(suspended, "Suspended");
    yield* beforeResume;
    return restarted;
  });

  const bringUp = Effect.fnUntraced(function*(name: string, manifest: () => Sandbox): Effect.fn.Return<BaymaEndpoint, SandboxError> {
    const desired = manifest();
    let sandbox = yield* readOrCreate(name, desired);
    yield* ensureToken(sandbox);
    if (sandbox.metadata.annotations?.[POD_TEMPLATE_ANNOTATION] !== desired.metadata.annotations?.[POD_TEMPLATE_ANNOTATION]) {
      sandbox = yield* moveOnto(sandbox, desired);
    } else if (podEnded(sandbox)) {
      sandbox = yield* restart(sandbox, `pod ended (${condition(sandbox, "Ready")?.reason})`);
    } else if (fault && sandbox.spec?.operatingMode !== "Suspended" && sandboxReady(sandbox)) {
      // Only a pod that ran before: one just made, or about to be resumed, is new.
      const found = yield* fault(name);
      if (found !== null) sandbox = yield* restart(sandbox, found.why, found.beforeResume);
    }
    if (sandbox.spec?.operatingMode === "Suspended") {
      sandbox = stored(yield* kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, { spec: { operatingMode: "Running" } }));
      yield* Effect.logInfo(`resumed Sandbox ${namespace}/${name}`);
    }
    // Readiness and bayma's answer share one deadline.
    const deadline = (yield* Clock.currentTimeMillis) + Duration.toMillis(readyTimeout);
    sandbox = yield* awaitCondition(sandbox, "Ready");
    const value = yield* token(name);
    if (!value) return yield* new SandboxTokenMissing({ namespace, name });
    const host = sandbox.status?.serviceFQDN ?? `${name}.${namespace}.svc`;
    const url = `http://${host}:${port}/mcp`;
    const headers = { Authorization: `Bearer ${value}` };
    yield* awaitAnswer(url, headers, Duration.millis(deadline - (yield* Clock.currentTimeMillis)));
    return { url, headers };
  });

  return {
    namespace,

    ensure: (name, manifest) =>
      Effect.suspend(() =>
        Option.match(FiberMap.getUnsafe(ensuring, name), {
          onSome: Effect.succeed,
          onNone: () =>
            FiberMap.run(
              ensuring,
              name,
              bringUp(name, manifest).pipe(
                withAlasioSpan("alasio.sandbox.ensure", { attributes: { "alasio.sandbox.name": name, "k8s.namespace.name": namespace } }),
                withLogScope("sandboxes"),
              ),
            ),
        })
      ).pipe(Effect.flatMap(Fiber.join)),

    token,

    exists: (name) => read(name).pipe(Effect.map((sandbox) => sandbox !== null)),

    suspend: (name) =>
      Effect.gen(function*() {
        const suspending = yield* kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, { spec: { operatingMode: "Suspended" } }).pipe(
          Effect.map(stored),
          Effect.catchIf(hasStatus(404), () => Effect.succeed(null)),
        );
        if (!suspending) return;
        yield* awaitCondition(suspending, "Suspended");
        yield* Effect.logInfo(`suspended Sandbox ${namespace}/${name}`);
      }).pipe(withLogScope("sandboxes")),

    remove: (name) =>
      Effect.sync(() => tokens.delete(name)).pipe(
        Effect.andThen(kube.remove(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name)),
        Effect.andThen(Effect.logInfo(`deleted Sandbox ${namespace}/${name}`)),
        withLogScope("sandboxes"),
      ),
  };
});
