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
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

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

import { createLogger } from "../shared/log.ts";
import { inSpan } from "../telemetry/index.ts";
import { isStatus, type KubeClient } from "./client.ts";

const log = createLogger("sandboxes");

export const SANDBOX_API_VERSION = "agents.x-k8s.io/v1beta1";
export const SANDBOX_KIND = "Sandbox";

/** Whether a Sandbox's pod runs; suspending one keeps its volumes. */
export type SandboxOperatingMode = "Running" | "Suspended";

/** A Sandbox's metadata, which always names it and its namespace. */
export interface SandboxMetadata extends V1ObjectMeta {
  name: string;
  namespace: string;
}

/** What alasio sets of a Sandbox's spec (charts/agent-sandbox/crds). */
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
  /** Adds what is the caller's own to the pod spec, whose bayma container is `bayma`, and returns the spec. */
  readonly configure?: (spec: V1PodSpec, bayma: V1Container) => V1PodSpec;
}

/** Where a Sandbox's bayma serves MCP, and the bearer to reach it with. */
export interface BaymaEndpoint {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

/** The Sandboxes of one namespace; see createSandboxes. */
export interface Sandboxes {
  readonly namespace: string;
  ensure(name: string, manifest: () => Sandbox): Promise<BaymaEndpoint>;
  token(name: string): Promise<string | null>;
  exists(name: string): Promise<boolean>;
  suspend(name: string): Promise<void>;
  remove(name: string): Promise<void>;
}

/** What createSandboxes is given. */
export interface SandboxesOptions {
  readonly kube: Pick<KubeClient, "read" | "create" | "patch" | "remove">;
  readonly namespace: string;
  readonly port: number;
  readonly fetchImpl?: typeof fetch;
  readonly readyTimeoutMs?: number;
  readonly pollMs?: number;
}

/** The container in every Sandbox's pod that runs bayma. */
export const BAYMA_CONTAINER = "bayma";
/** Where a Sandbox's token Secret is mounted in its bayma container. */
export const TOKEN_DIR = "/run/alasio/bayma";
const TOKEN_KEY = "token";
const TOKEN_VOLUME = "alasio-bayma-token";

/** The label every object alasio makes carries, and the one that names its Sandbox. */
export const MANAGED_BY = { "app.kubernetes.io/managed-by": "alasio" };
export const SANDBOX_LABEL = "alasio.dev/sandbox";

// A first start pulls the image and provisions the volume, so it is given minutes.
const READY_TIMEOUT_MS = 300_000;
const POLL_MS = 500;

/** The name of a Sandbox's token Secret. */
export function tokenSecretName(name: string): string {
  return `${name}-bayma-token`;
}

/**
 * A token that names its Sandbox, `<name>.<random>`, so what presents one can be looked
 * up by it (../sandbox/telemetry-receiver.ts) without a table of tokens.
 */
export function newToken(name: string, random: () => string = () => randomBytes(32).toString("base64url")): string {
  return `${name}.${random()}`;
}

/** The Sandbox's name in `token`, or null for one that names none. */
export function tokenSandboxName(token: string): string | null {
  const dot = typeof token === "string" ? token.indexOf(".") : -1;
  return dot > 0 ? token.slice(0, dot) : null;
}

/** Whether `presented` is `expected`, compared in constant time. */
export function sameToken(presented: string, expected: string): boolean {
  const a = Buffer.from(String(presented));
  const b = Buffer.from(String(expected));
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * The Sandbox for `name` from a profile's `template` (`{ podTemplate,
 * volumeClaimTemplates }`, rendered by the chart), with what every Sandbox needs: its
 * labels, Running, its Service, and bayma given its token. `configure(podSpec, bayma)`
 * adds what is the caller's own and returns the spec. Pure, for tests.
 */
export function sandboxManifest({
  name,
  namespace,
  template,
  labels = {},
  annotations = {},
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
    metadata: { name, namespace, labels: allLabels, ...(Object.keys(annotations).length ? { annotations } : {}) },
    spec: {
      operatingMode: "Running",
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
  return sandbox?.status?.conditions?.find((entry) => entry.type === type) ?? null;
}

/**
 * `object` as the Sandbox it is: what the API server returns for a Sandbox is one it
 * stores, in its CRD's schema and with the server's own metadata.
 */
function stored(object: KubernetesObject): StoredSandbox {
  return object as StoredSandbox;
}

/** Whether the Sandbox's pod is ready and its Service exists, for its current spec. */
export function sandboxReady(sandbox: SandboxReadiness): boolean {
  const ready = condition(sandbox, "Ready");
  return ready?.status === "True" && (ready.observedGeneration ?? 0) >= (sandbox.metadata.generation ?? 0);
}

/**
 * The Sandboxes of one namespace. `kube` is ./client.ts's client; `port` is
 * bayma's in every one; `fetchImpl` reaches bayma to see that it answers.
 *
 * - `ensure(name, manifest)`: makes sure the Sandbox exists (made from `manifest()`
 *   when it does not), runs, and its bayma answers; resolves `{ url, headers }`, the
 *   MCP endpoint and the bearer to reach it with. Callers asking at once share one.
 * - `token(name)`: the Sandbox's token, or null when it has none.
 * - `suspend(name)`, `remove(name)`, `exists(name)`.
 */
export function createSandboxes({
  kube,
  namespace,
  port,
  fetchImpl = fetch,
  readyTimeoutMs = READY_TIMEOUT_MS,
  pollMs = POLL_MS,
}: SandboxesOptions): Sandboxes {
  const ensuring = new Map<string, Promise<BaymaEndpoint>>();
  const tokens = new Map<string, string>();

  const read = async (name: string): Promise<StoredSandbox | null> => {
    const object = await kube.read(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name);
    return object && stored(object);
  };

  async function token(name: string): Promise<string | null> {
    const known = tokens.get(name);
    if (known !== undefined) return known;
    // A core Secret, as the API server returns one.
    const secret = (await kube.read("v1", "Secret", namespace, tokenSecretName(name))) as V1Secret | null;
    const encoded = secret?.data?.[TOKEN_KEY];
    const value = encoded ? Buffer.from(encoded, "base64").toString("utf8") : null;
    if (value) tokens.set(name, value);
    return value;
  }

  /** The Sandbox, made from `manifest()` when it does not exist. */
  async function readOrCreate(name: string, manifest: () => Sandbox): Promise<StoredSandbox | null> {
    const existing = await read(name);
    if (existing) return existing;
    try {
      const created = stored(await kube.create(manifest()));
      log.info(`created Sandbox ${namespace}/${name}`);
      return created;
    } catch (error) {
      if (!isStatus(error, 409)) throw error;
      return await read(name);
    }
  }

  /**
   * Makes the Sandbox's token Secret unless it has one. Its pod waits for the Secret,
   * so one made just after the Sandbox, or by a later alasio after a failure between
   * the two, starts it.
   */
  async function ensureToken(sandbox: StoredSandbox): Promise<void> {
    const name = sandbox.metadata.name;
    if (await token(name)) return;
    const value = newToken(name);
    try {
      await kube.create(tokenSecretManifest(sandbox, value));
      tokens.set(name, value);
    } catch (error) {
      if (!isStatus(error, 409)) throw error;
    }
  }

  /** Waits until bayma in the Sandbox answers over its Service. */
  async function answering(url: string, headers: Readonly<Record<string, string>>, deadline: number): Promise<void> {
    let last: unknown = null;
    while (Date.now() < deadline) {
      try {
        // Any response is bayma answering: a GET without a session is refused.
        const response = await fetchImpl(url, { method: "GET", headers, signal: AbortSignal.timeout(5000) });
        await response.body?.cancel();
        if (response.status !== 401) return;
        last = new Error("bayma refused the Sandbox's token");
      } catch (error) {
        last = error;
      }
      await sleep(pollMs);
    }
    throw new Error(`bayma in ${url} did not answer: ${last instanceof Error ? last.message : "timed out"}`);
  }

  async function bringUp(name: string, manifest: () => Sandbox): Promise<BaymaEndpoint> {
    return await inSpan("alasio.sandbox.ensure", { attributes: { "alasio.sandbox.name": name, "k8s.namespace.name": namespace } }, async () => {
      let sandbox = await readOrCreate(name, manifest);
      if (!sandbox) throw new Error(`Sandbox ${namespace}/${name} was deleted while it was made`);
      await ensureToken(sandbox);
      if (sandbox.spec?.operatingMode === "Suspended") {
        sandbox = stored(await kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, { spec: { operatingMode: "Running" } }));
        log.info(`resumed Sandbox ${namespace}/${name}`);
      }
      const deadline = Date.now() + readyTimeoutMs;
      while (!sandboxReady(sandbox)) {
        if (Date.now() > deadline) {
          const ready = condition(sandbox, "Ready");
          throw new Error(`Sandbox ${namespace}/${name} did not become ready within ${readyTimeoutMs / 1000}s${ready?.message ? `: ${ready.message}` : ""}`);
        }
        await sleep(pollMs);
        sandbox = await read(name);
        if (!sandbox) throw new Error(`Sandbox ${namespace}/${name} was deleted while it started`);
      }
      const value = await token(name);
      if (!value) throw new Error(`Sandbox ${namespace}/${name} has no token Secret`);
      const host = sandbox.status?.serviceFQDN ?? `${name}.${namespace}.svc`;
      const url = `http://${host}:${port}/mcp`;
      const headers = { Authorization: `Bearer ${value}` };
      await answering(url, headers, deadline);
      return { url, headers };
    });
  }

  return {
    namespace,

    ensure(name, manifest) {
      let pending = ensuring.get(name);
      if (!pending) {
        pending = bringUp(name, manifest).finally(() => ensuring.delete(name));
        ensuring.set(name, pending);
      }
      return pending;
    },

    token,

    async exists(name) {
      return (await read(name)) !== null;
    },

    async suspend(name) {
      try {
        await kube.patch(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name, { spec: { operatingMode: "Suspended" } });
        log.info(`suspended Sandbox ${namespace}/${name}`);
      } catch (error) {
        if (!isStatus(error, 404)) throw error;
      }
    },

    async remove(name) {
      tokens.delete(name);
      await kube.remove(SANDBOX_API_VERSION, SANDBOX_KIND, namespace, name);
      log.info(`deleted Sandbox ${namespace}/${name}`);
    },
  };
}
