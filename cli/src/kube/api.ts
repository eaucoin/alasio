/**
 * The Kubernetes API, as alasio's command line calls it: objects applied, read, listed,
 * patched and deleted as the plain JSON the API server takes and gives, over HTTP(S) with
 * a kubeconfig's credentials. Nothing of client-node's object model is between the
 * manifests and the cluster, as its serializer drops what its model lacks (a
 * NetworkPolicy's `from`, a CRD schema's `x-kubernetes-*` keys); client-node only reads
 * the kubeconfig, and runs commands in containers, and forwards connections to pods,
 * over its websockets.
 *
 * Objects are addressed by their kind, as KINDS lists those alasio reads or makes, so
 * no discovery is needed.
 */
import { type ClientRequest, type IncomingMessage, request as httpRequest, STATUS_CODES } from "node:http";
import { request as httpsRequest, type RequestOptions } from "node:https";
import type { Duplex, Readable, Writable } from "node:stream";
import { buffer } from "node:stream/consumers";
import { inspect } from "node:util";

import { NodeStream } from "@effect/platform-node";
import { Exec, KubeConfig, type KubernetesObject, PortForward, type V1Status } from "@kubernetes/client-node";
import { Context, Effect, Layer, Predicate, Schema, type Scope, Stream } from "effect";

/** A kind of object: its API version and kind, as an object of it says them. */
export interface Kind {
  readonly apiVersion: string;
  readonly kind: string;
}

/** An object, by its kind, its namespace (none for a cluster-scoped one) and its name. */
export interface ObjectRef extends Kind {
  readonly namespace?: string | undefined;
  readonly name: string;
}

/** The kinds alasio's command line reads or makes, by name: their API version, their resource's plural, and whether their objects are in a namespace. */
const KINDS = {
  CustomResourceDefinition: { apiVersion: "apiextensions.k8s.io/v1", plural: "customresourcedefinitions", namespaced: false },
  Namespace: { apiVersion: "v1", plural: "namespaces", namespaced: false },
  Node: { apiVersion: "v1", plural: "nodes", namespaced: false },
  PersistentVolume: { apiVersion: "v1", plural: "persistentvolumes", namespaced: false },
  PriorityClass: { apiVersion: "scheduling.k8s.io/v1", plural: "priorityclasses", namespaced: false },
  RuntimeClass: { apiVersion: "node.k8s.io/v1", plural: "runtimeclasses", namespaced: false },
  StorageClass: { apiVersion: "storage.k8s.io/v1", plural: "storageclasses", namespaced: false },
  CSIDriver: { apiVersion: "storage.k8s.io/v1", plural: "csidrivers", namespaced: false },
  ClusterRole: { apiVersion: "rbac.authorization.k8s.io/v1", plural: "clusterroles", namespaced: false },
  ClusterRoleBinding: { apiVersion: "rbac.authorization.k8s.io/v1", plural: "clusterrolebindings", namespaced: false },
  Role: { apiVersion: "rbac.authorization.k8s.io/v1", plural: "roles", namespaced: true },
  RoleBinding: { apiVersion: "rbac.authorization.k8s.io/v1", plural: "rolebindings", namespaced: true },
  ServiceAccount: { apiVersion: "v1", plural: "serviceaccounts", namespaced: true },
  ConfigMap: { apiVersion: "v1", plural: "configmaps", namespaced: true },
  Secret: { apiVersion: "v1", plural: "secrets", namespaced: true },
  PersistentVolumeClaim: { apiVersion: "v1", plural: "persistentvolumeclaims", namespaced: true },
  Service: { apiVersion: "v1", plural: "services", namespaced: true },
  Pod: { apiVersion: "v1", plural: "pods", namespaced: true },
  Event: { apiVersion: "v1", plural: "events", namespaced: true },
  NetworkPolicy: { apiVersion: "networking.k8s.io/v1", plural: "networkpolicies", namespaced: true },
  Deployment: { apiVersion: "apps/v1", plural: "deployments", namespaced: true },
  StatefulSet: { apiVersion: "apps/v1", plural: "statefulsets", namespaced: true },
  DaemonSet: { apiVersion: "apps/v1", plural: "daemonsets", namespaced: true },
  Job: { apiVersion: "batch/v1", plural: "jobs", namespaced: true },
  CronJob: { apiVersion: "batch/v1", plural: "cronjobs", namespaced: true },
  Sandbox: { apiVersion: "agents.x-k8s.io/v1beta1", plural: "sandboxes", namespaced: true },
  // What the cluster's metrics server measures its pods and nodes using now.
  PodMetrics: { apiVersion: "metrics.k8s.io/v1beta1", plural: "pods", namespaced: true },
  NodeMetrics: { apiVersion: "metrics.k8s.io/v1beta1", plural: "nodes", namespaced: false },
} as const;

export type KindName = keyof typeof KINDS;

/** The kind of that name. */
export function kind(name: KindName): Kind {
  return { apiVersion: KINDS[name].apiVersion, kind: name };
}

/** The resource of `kind`; a kind not in KINDS is a defect of the caller's. */
function resource({ apiVersion, kind: name }: Kind): { readonly plural: string; readonly namespaced: boolean } {
  const found = Object.hasOwn(KINDS, name) ? KINDS[name as KindName] : undefined;
  if (found?.apiVersion !== apiVersion) throw new Error(`alasio does not know the kind ${name} of ${apiVersion}`);
  return found;
}

/** The path of the objects of `kind`: in `namespace`, or, for a namespaced kind without one, in every namespace. */
function collectionPath(kind: Kind, namespace?: string): string {
  const { plural, namespaced: inNamespace } = resource(kind);
  const group = kind.apiVersion === "v1" ? "/api/v1" : `/apis/${kind.apiVersion}`;
  return inNamespace && namespace ? `${group}/namespaces/${encodeURIComponent(namespace)}/${plural}` : `${group}/${plural}`;
}

/** The path of the object `ref`. */
function objectPath(ref: ObjectRef): string {
  return `${collectionPath(ref, ref.namespace)}/${encodeURIComponent(ref.name)}`;
}

/** The reference of `object`, by its own kind, namespace and name. */
export function refOf(object: KubernetesObject): ObjectRef {
  return { apiVersion: object.apiVersion ?? "", kind: object.kind ?? "", namespace: object.metadata?.namespace, name: object.metadata?.name ?? "" };
}

/** `ref` as people read it: `Kind namespace/name`, or `Kind name` when it is in no namespace. */
export function describeRef(ref: ObjectRef): string {
  return `${ref.kind} ${ref.namespace ? `${ref.namespace}/` : ""}${ref.name}`;
}

/** The API refused a call, or could not be reached: the status it answered with, when it answered. */
export class KubeApiError extends Schema.TaggedError<KubeApiError>()("KubeApiError", {
  /** The call, as its method and path. */
  call: Schema.String,
  /** The API's HTTP status (404 for absent, 409 for a conflict), when it answered. */
  status: Schema.optional(Schema.Number),
  /** What the API answered, or why it could not be reached. */
  reason: Schema.String,
}) {
  override get message(): string {
    return `Kubernetes ${this.status === undefined ? "could not be reached for" : `answered ${this.status} to`} ${this.call}: ${this.reason}`;
  }
}

/** The kubeconfig cannot reach a cluster: it cannot be read, or lacks the context or cluster asked for. */
export class KubeconfigUnusable extends Schema.TaggedError<KubeconfigUnusable>()("KubeconfigUnusable", {
  kubeconfig: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the kubeconfig ${this.kubeconfig} cannot be used: ${this.reason}`;
  }
}

/** A command run in a container came to no exit code. */
export class KubeExecError extends Schema.TaggedError<KubeExecError>()("KubeExecError", {
  message: Schema.String,
}) {}

/** Whether `error` is the API's answer `status`. */
export const hasStatus = (status: number) => (error: KubeApiError): boolean => error.status === status;

/** A kubeconfig, and the context of it to use: the default kubeconfig (KUBECONFIG, or ~/.kube/config) when no path is given, its current context when no context is. */
export interface KubeconfigRef {
  readonly path?: string | undefined;
  readonly context?: string | undefined;
}

/** What `list` is given beyond the kind. */
export interface ListOptions {
  /** The namespace listed; every namespace when none is given. */
  readonly namespace?: string | undefined;
  readonly labelSelector?: string | undefined;
  readonly fieldSelector?: string | undefined;
}

/** What `logs` is given beyond the pod. */
export interface LogOptions {
  readonly container: string;
  readonly follow?: boolean | undefined;
  readonly sinceSeconds?: number | undefined;
}

/** A container a command is run in. */
export interface ContainerRef {
  readonly namespace: string;
  readonly pod: string;
  readonly container: string;
}

/** A pod a connection is forwarded to. */
export interface PodRef {
  readonly namespace: string;
  readonly pod: string;
}

/** Where a command run by `exec` reads and writes; `tty` gives it a terminal, whose size follows `stdout`'s. */
export interface ExecStreams {
  readonly stdin: Readable | null;
  readonly stdout: Writable;
  readonly stderr: Writable;
  readonly tty: boolean;
}

/** The manager alasio applies its objects as, which owns every field it sets. */
const FIELD_MANAGER = "alasio";

/** The Kubernetes API a kubeconfig reaches, as alasio's command line calls it. */
export class KubeApi extends Context.Service<KubeApi, {
  /** The API server's URL. */
  readonly server: string;
  /** Server-side applies `object` as FIELD_MANAGER, taking over fields other managers set: the object as applied. */
  readonly apply: (object: KubernetesObject) => Effect.Effect<KubernetesObject, KubeApiError>;
  /** The object, or null when there is none. */
  readonly get: <T extends KubernetesObject = KubernetesObject>(ref: ObjectRef) => Effect.Effect<T | null, KubeApiError>;
  readonly list: <T extends KubernetesObject = KubernetesObject>(kind: Kind, options?: ListOptions) => Effect.Effect<readonly T[], KubeApiError>;
  /** A JSON merge patch, as `fieldManager`. */
  readonly patch: (ref: ObjectRef, patch: object, fieldManager: string) => Effect.Effect<KubernetesObject, KubeApiError>;
  /** Deletes it, with its dependents in the background; one already gone is not an error. */
  readonly remove: (ref: ObjectRef) => Effect.Effect<void, KubeApiError>;
  /** What the container writes to its log, as it writes it while `follow`. */
  readonly logs: (namespace: string, pod: string, options: LogOptions) => Stream.Stream<Uint8Array, KubeApiError>;
  /** Runs `command` in the container with `streams`: its exit code. Interrupting it closes the exec. */
  readonly exec: (target: ContainerRef, command: readonly string[], streams: ExecStreams) => Effect.Effect<number, KubeApiError | KubeExecError>;
  /** Forwards `connection` to `port` of the pod, as the API server's port-forward does, until either end closes it; interrupting it closes both. */
  readonly portForward: (target: PodRef, port: number, connection: Duplex) => Effect.Effect<void, KubeApiError>;
}>()("alasio/kube/KubeApi") {
  /** The API `kubeconfig` reaches. */
  static readonly layer = (kubeconfig: KubeconfigRef): Layer.Layer<KubeApi, KubeconfigUnusable> =>
    Layer.effect(KubeApi, Effect.map(loadKubeConfig(kubeconfig), makeKubeApi));
}

/** The kubeconfig `ref` names, at its context. */
const loadKubeConfig = (ref: KubeconfigRef): Effect.Effect<KubeConfig, KubeconfigUnusable> =>
  Effect.try({
    try: () => {
      const kubeConfig = new KubeConfig();
      if (ref.path) kubeConfig.loadFromFile(ref.path);
      else kubeConfig.loadFromDefault();
      if (ref.context) {
        const contexts = kubeConfig.getContexts().map(({ name }) => name);
        if (!contexts.includes(ref.context)) throw new Error(`it has no context ${ref.context}, only ${contexts.join(", ") || "none"}`);
        kubeConfig.setCurrentContext(ref.context);
      }
      if (!kubeConfig.getCurrentCluster()) throw new Error(`its context ${kubeConfig.getCurrentContext() || "(none)"} names no cluster`);
      return kubeConfig;
    },
    catch: (cause) => new KubeconfigUnusable({ kubeconfig: ref.path ?? "of KUBECONFIG or ~/.kube/config", reason: cause instanceof Error ? cause.message : String(cause) }),
  });

/** A call of the API: its query, and its body with the content type it is sent as. */
interface KubeCall {
  readonly method: "GET" | "PATCH" | "DELETE";
  readonly path: string;
  readonly query?: Readonly<Record<string, string | undefined>>;
  readonly body?: { readonly contentType: string; readonly content: unknown };
}

const describeCall = (call: KubeCall): string => `${call.method} ${call.path}`;

/** `call` failed, without the API's answer: why, as `cause` (an Error, or an event of a websocket's that carries a message) says. */
const unreachable = (call: KubeCall, cause: unknown): KubeApiError =>
  new KubeApiError({ call: describeCall(call), reason: Predicate.hasProperty(cause, "message") && Predicate.isString(cause.message) ? cause.message : inspect(cause) });

/** The exit code a `pods/exec` status reports: 0 on success, the process's own otherwise; null when it reports none. */
function exitCodeOf(status: V1Status): number | null {
  if (status.status === "Success") return 0;
  const cause = status.details?.causes?.find((entry) => entry.reason === "ExitCode");
  return cause ? Number(cause.message) : null;
}

/** The API `kubeConfig` reaches, at its current context. */
function makeKubeApi(kubeConfig: KubeConfig): KubeApi["Service"] {
  const server = new URL(kubeConfig.getCurrentCluster()?.server ?? "");
  const request = server.protocol === "http:" ? httpRequest : httpsRequest;
  const executor = new Exec(kubeConfig);
  const forwarder = new PortForward(kubeConfig);

  /** Sends `call` with the kubeconfig's credentials. */
  const send = (call: KubeCall): Effect.Effect<ClientRequest, KubeApiError> =>
    Effect.tryPromise({
      try: async () => {
        const query = new URLSearchParams(Object.entries(call.query ?? {}).filter((entry): entry is [string, string] => entry[1] !== undefined));
        const body = call.body === undefined ? undefined : JSON.stringify(call.body.content);
        const options: RequestOptions = {
          method: call.method,
          headers: { Accept: "application/json", ...(call.body && body !== undefined ? { "Content-Type": call.body.contentType, "Content-Length": Buffer.byteLength(body) } : {}) },
        };
        await kubeConfig.applyToHTTPSOptions(options);
        const url = new URL(`${server.pathname.replace(/\/$/u, "")}${call.path}${query.size ? `?${query}` : ""}`, server);
        const sent = request(url, options);
        sent.end(body);
        return sent;
      },
      catch: (cause) => unreachable(call, cause),
    });

  const readAll = (call: KubeCall, stream: Readable): Effect.Effect<Buffer, KubeApiError> =>
    Effect.tryPromise({ try: () => buffer(stream), catch: (cause) => unreachable(call, cause) });

  /** The API's refusal of `call`: the status of `response`, and the message of the Status its body holds. */
  const refusal = (call: KubeCall, response: IncomingMessage): Effect.Effect<never, KubeApiError> =>
    readAll(call, response).pipe(
      Effect.flatMap((content) => {
        const status = response.statusCode ?? 0;
        const text = content.toString("utf8").trim();
        let reason = text || STATUS_CODES[status] || "no message";
        try {
          const parsed: unknown = JSON.parse(text);
          if (Predicate.hasProperty(parsed, "message") && Predicate.isString(parsed.message)) reason = parsed.message;
        } catch {
          // Not JSON: the text is the message.
        }
        return Effect.fail(new KubeApiError({ call: describeCall(call), status, reason }));
      }),
    );

  /** The response to `call`, open while the scope is; a status of 400 or over is its refusal. */
  const open = (call: KubeCall): Effect.Effect<IncomingMessage, KubeApiError, Scope.Scope> =>
    Effect.acquireRelease(
      Effect.flatMap(send(call), (sent) =>
        Effect.callback<IncomingMessage, KubeApiError>((resume) => {
          sent.on("response", (response) => resume(Effect.succeed(response)));
          sent.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
          return Effect.sync(() => sent.destroy());
        })),
      (response) => Effect.sync(() => response.destroy()),
    ).pipe(Effect.tap((response) => ((response.statusCode ?? 0) >= 400 ? refusal(call, response) : Effect.void)));

  /** The JSON the API answers `call` with. */
  const json = <A>(call: KubeCall): Effect.Effect<A, KubeApiError> =>
    Effect.scoped(Effect.flatMap(open(call), (response) => readAll(call, response))).pipe(
      Effect.map((content) => JSON.parse(content.toString("utf8")) as A),
    );

  const exec = Effect.fnUntraced(function*(
    { namespace, pod, container }: ContainerRef,
    command: readonly string[],
    { stdin, stdout, stderr, tty }: ExecStreams,
  ): Effect.fn.Return<number, KubeApiError | KubeExecError> {
    const call: KubeCall = { method: "GET", path: `/api/v1/namespaces/${namespace}/pods/${pod}/exec` };
    // Held in an object: the status callback sets it where control flow cannot see.
    const reported: { status: V1Status | null } = { status: null };
    yield* Effect.callback<void, KubeApiError>((resume) => {
      let socket: Awaited<ReturnType<Exec["exec"]>> | null = null;
      executor
        .exec(namespace, pod, container, [...command], stdout, stderr, stdin, tty, (status) => {
          reported.status = status;
        })
        .then((opened) => {
          socket = opened;
          opened.on("close", () => resume(Effect.void));
          opened.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
        }, (cause: unknown) => resume(Effect.fail(unreachable(call, cause))));
      return Effect.sync(() => socket?.close());
    });
    const exitCode = reported.status ? exitCodeOf(reported.status) : null;
    if (exitCode === null) {
      return yield* new KubeExecError({
        message: `${command[0] ?? "the command"} in ${namespace}/${pod} came to no exit code${reported.status?.message ? `: ${reported.status.message}` : ""}`,
      });
    }
    return exitCode;
  });

  const portForward = ({ namespace, pod }: PodRef, port: number, connection: Duplex): Effect.Effect<void, KubeApiError> => {
    const call: KubeCall = { method: "GET", path: `/api/v1/namespaces/${namespace}/pods/${pod}/portforward` };
    return Effect.callback<void, KubeApiError>((resume) => {
      let socket: { close(): void } | null = null;
      const close = () => socket?.close();
      connection.on("close", close);
      forwarder.portForward(namespace, pod, [port], connection, null, connection).then((opened) => {
        // Given no retries, the forward is its websocket.
        const forwarded = typeof opened === "function" ? opened() : opened;
        if (!forwarded) return resume(Effect.fail(unreachable(call, new Error("the port-forward opened no websocket"))));
        socket = forwarded;
        forwarded.on("close", () => {
          connection.destroy();
          resume(Effect.void);
        });
        forwarded.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
      }, (cause: unknown) => {
        connection.destroy();
        resume(Effect.fail(unreachable(call, cause)));
      });
      return Effect.sync(() => {
        close();
        connection.destroy();
      });
    });
  };

  return KubeApi.of({
    server: server.href.replace(/\/$/u, ""),
    apply: (object) =>
      json({
        method: "PATCH",
        path: objectPath(refOf(object)),
        query: { fieldManager: FIELD_MANAGER, force: "true" },
        // JSON is YAML, which an apply patch is sent as.
        body: { contentType: "application/apply-patch+yaml", content: object },
      }),
    get: <T extends KubernetesObject>(ref: ObjectRef) =>
      json<T>({ method: "GET", path: objectPath(ref) }).pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed(null))),
    list: <T extends KubernetesObject>(kind: Kind, { namespace, labelSelector, fieldSelector }: ListOptions = {}) =>
      json<{ readonly items: readonly T[] }>({ method: "GET", path: collectionPath(kind, namespace), query: { labelSelector, fieldSelector } }).pipe(
        Effect.map(({ items }) => items.map((item) => ({ ...item, apiVersion: kind.apiVersion, kind: kind.kind }))),
      ),
    patch: (ref, patch, fieldManager) =>
      json({ method: "PATCH", path: objectPath(ref), query: { fieldManager }, body: { contentType: "application/merge-patch+json", content: patch } }),
    remove: (ref) =>
      json({ method: "DELETE", path: objectPath(ref), query: { propagationPolicy: "Background" } }).pipe(
        Effect.asVoid,
        Effect.catchIf(hasStatus(404), () => Effect.void),
      ),
    logs: (namespace, pod, { container, follow = false, sinceSeconds }) => {
      const call: KubeCall = {
        method: "GET",
        path: `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods/${encodeURIComponent(pod)}/log`,
        query: { container, follow: String(follow), sinceSeconds: sinceSeconds === undefined ? undefined : String(sinceSeconds) },
      };
      return Stream.unwrap(
        Effect.map(open(call), (response) => NodeStream.fromReadable({ evaluate: () => response, onError: (cause) => unreachable(call, cause) })),
      );
    },
    exec,
    portForward,
  });
}
