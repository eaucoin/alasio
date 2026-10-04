/**
 * A Kubernetes API server in memory, over HTTPS, for the tests of alasio's command line:
 * the endpoints its KubeApi (../../src/kube/api.ts) calls. It keeps objects by their path,
 * takes server-side applies and merge patches, lists by label and field selectors,
 * deletes (a namespace with what is in it), serves pods' logs, and runs commands in
 * containers over a websocket, as the API server's exec does (channel protocol v4), with
 * what the test answers. It records every change, in order, and every command.
 *
 * Its controllers are make-believe: what is applied is at once as the cluster would make
 * it once it runs (a CRD established, a Job complete, a workload rolled out with a ready
 * pod), unless the test says an object is stuck (its pods wait on an image pull the
 * cluster warns about) or, for a Job, fails.
 */
import { createHash } from "node:crypto";
import { once } from "node:events";
import type { IncomingMessage, ServerResponse } from "node:http";
import { createServer } from "node:https";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { buffer } from "node:stream/consumers";

import { KubeConfig } from "@kubernetes/client-node";

/** The fake's certificate, for 127.0.0.1, and its key: made once for these tests, valid for a century. */
export const CERTIFICATE = `-----BEGIN CERTIFICATE-----
MIIBkDCCATagAwIBAgIUXXWRzs0lXBHSGKq+4GuCxbqU8HowCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJZmFrZS1rdWJlMCAXDTI2MTAwNDIyNDI0OFoYDzIxMjYwOTEw
MjI0MjQ4WjAUMRIwEAYDVQQDDAlmYWtlLWt1YmUwWTATBgcqhkjOPQIBBggqhkjO
PQMBBwNCAARQZY7mJ3e6xwwwhvDIezBAp1HX1MJ3TkSTLQIn6KoYEux2yKdaN5mf
HmoLRMPnci6D99PDS1kWI7FWqIMLSwAfo2QwYjAdBgNVHQ4EFgQURLvsbWTusq5r
M2Mu2Y6y/PN+htswHwYDVR0jBBgwFoAURLvsbWTusq5rM2Mu2Y6y/PN+htswDwYD
VR0TAQH/BAUwAwEB/zAPBgNVHREECDAGhwR/AAABMAoGCCqGSM49BAMCA0gAMEUC
IQCc9dNZK0gCNL0F2e89EdhrN1QJsWuzhl4juoHu3PFupQIgJayufCuSgCVP3iWe
uDYOYuBAM67egFYesQBlI/kATew=
-----END CERTIFICATE-----
`;

const KEY = `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgk1jTSvrNN+BAmhWZ
MI84d0cDA0ZvFIbPgxBZpnpSM3mhRANCAARQZY7mJ3e6xwwwhvDIezBAp1HX1MJ3
TkSTLQIn6KoYEux2yKdaN5mfHmoLRMPnci6D99PDS1kWI7FWqIMLSwAf
-----END PRIVATE KEY-----
`;

/** The bearer token the fake takes, and refuses requests without. */
export const TOKEN = "fake-kube-token";

export type KubeObject = Record<string, unknown> & {
  apiVersion: string;
  kind: string;
  metadata: Record<string, unknown> & { name: string; namespace?: string; labels?: Record<string, string>; generation?: number };
  spec?: Record<string, unknown>;
  status?: Record<string, unknown>;
};

/** A change the fake was asked for: its method, its path, and the content type and query of a patch. */
export interface KubeChange {
  readonly method: string;
  readonly path: string;
  readonly contentType?: string;
  readonly query: URLSearchParams;
}

/** A command run in a container, as it was asked for. */
export interface KubeExec {
  readonly namespace: string;
  readonly pod: string;
  readonly container: string;
  readonly command: readonly string[];
  readonly tty: boolean;
}

/** What a command run in a container came to. */
export interface KubeExecResult {
  readonly exitCode: number;
  readonly stdout?: string;
  readonly stderr?: string;
}

export interface FakeKube {
  /** Its URL, https://127.0.0.1:PORT. */
  readonly server: string;
  readonly port: number;
  /** A kubeconfig that reaches it, context `fake`. */
  readonly kubeconfig: string;
  /** Its objects, by path. */
  readonly objects: Map<string, KubeObject>;
  readonly changes: KubeChange[];
  /** Names of workloads whose pods never run, and of Jobs that fail. */
  readonly stuck: Set<string>;
  readonly failing: Set<string>;
  /** What a pod logs, by its name. */
  readonly logs: Map<string, string>;
  readonly execs: KubeExec[];
  /** How the test answers a command run in a container; exit 0, saying nothing, unless it says. */
  onExec: (exec: KubeExec) => KubeExecResult;
  /** The object at `path`. */
  readonly get: (path: string) => KubeObject | undefined;
  /** Puts `object` at the path its kind (`plural`) and names make. */
  readonly put: (plural: string, object: KubeObject) => void;
  readonly close: () => Promise<void>;
}

/** The path of an object of `plural` in `apiPath`, in `namespace` unless it is cluster-scoped. */
function pathOf(apiPath: string, plural: string, namespace: string | undefined, name: string): string {
  return namespace ? `${apiPath}/namespaces/${namespace}/${plural}/${name}` : `${apiPath}/${plural}/${name}`;
}

/** A request's path, read: its API's path, the resource's plural, namespace and name, and a subresource. */
function parse(path: string): { api: string; plural: string; namespace?: string; name?: string; sub?: string } | null {
  const segments = path.split("/").filter(Boolean);
  const apiLength = segments[0] === "api" ? 2 : segments[0] === "apis" ? 3 : 0;
  if (apiLength === 0) return null;
  const api = `/${segments.slice(0, apiLength).join("/")}`;
  const rest = segments.slice(apiLength);
  if (rest[0] === "namespaces" && rest.length >= 3) {
    return { api, namespace: rest[1] ?? "", plural: rest[2] ?? "", ...(rest[3] ? { name: rest[3] } : {}), ...(rest[4] ? { sub: rest[4] } : {}) };
  }
  return { api, plural: rest[0] ?? "", ...(rest[1] ? { name: rest[1] } : {}) };
}

/** Whether `labels` carry every `key=value` of `selector`. */
function selects(selector: string | null, labels: Readonly<Record<string, string>> | undefined): boolean {
  if (!selector) return true;
  return selector.split(",").every((pair) => {
    const [key = "", value = ""] = pair.split("=");
    return labels?.[key] === value;
  });
}

/** Whether `object` has every `path=value` of the field `selector` (dotted paths). */
function fields(selector: string | null, object: KubeObject): boolean {
  if (!selector) return true;
  return selector.split(",").every((pair) => {
    const [path = "", value = ""] = pair.split("=");
    const found = path.split(".").reduce<unknown>((at, key) => (typeof at === "object" && at !== null ? (at as Record<string, unknown>)[key] : undefined), object);
    return found === value;
  });
}

/** `patch` merged into `target`, as a JSON merge patch (RFC 7386) is. */
function merged(target: unknown, patch: unknown): unknown {
  if (typeof patch !== "object" || patch === null || Array.isArray(patch)) return patch;
  const result: Record<string, unknown> = typeof target === "object" && target !== null && !Array.isArray(target) ? { ...(target as Record<string, unknown>) } : {};
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) delete result[key];
    else result[key] = merged(result[key], value);
  }
  return result;
}

const json = (response: ServerResponse, status: number, body: unknown): void => {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
};

const refuse = (response: ServerResponse, status: number, message: string): void =>
  json(response, status, { kind: "Status", apiVersion: "v1", status: "Failure", message, code: status });

/** A websocket's binary frame of `payload`, unmasked, as a server sends one. */
function frame(payload: Buffer): Buffer {
  const length = payload.length;
  const header = length < 126
    ? Buffer.from([0x82, length])
    : length < 65536
    ? Buffer.from([0x82, 126, length >> 8, length & 0xff])
    : Buffer.concat([Buffer.from([0x82, 127]), Buffer.from(BigInt(length).toString(16).padStart(16, "0"), "hex")]);
  return Buffer.concat([header, payload]);
}

/** A frame of `channel` (1 stdout, 2 stderr, 3 the exec's status), as the exec protocol has it. */
const channel = (number: number, content: string): Buffer => frame(Buffer.concat([Buffer.from([number]), Buffer.from(content)]));

/** The exec's status, as the API server reports a command's exit. */
const execStatus = (exitCode: number): string =>
  JSON.stringify(
    exitCode === 0
      ? { status: "Success" }
      : { status: "Failure", reason: "NonZeroExitCode", message: `command terminated with non-zero exit code: ${exitCode}`, details: { causes: [{ reason: "ExitCode", message: String(exitCode) }] } },
  );

/** Serves a fake Kubernetes API on the loopback, until `close`. */
export async function serveFakeKube(): Promise<FakeKube> {
  const objects = new Map<string, KubeObject>();
  const changes: KubeChange[] = [];
  const stuck = new Set<string>();
  const failing = new Set<string>();
  const logs = new Map<string, string>();
  const execs: KubeExec[] = [];
  let version = 0;

  const put = (plural: string, object: KubeObject): void => {
    const api = object.apiVersion === "v1" ? "/api/v1" : `/apis/${object.apiVersion}`;
    objects.set(pathOf(api, plural, object.metadata.namespace, object.metadata.name), object);
  };

  /** What the cluster would make of `object` once it runs, or fails to. */
  const settle = (object: KubeObject): void => {
    const { name, namespace, generation = 1 } = object.metadata;
    const condition = (type: string, reason = type) => ({ type, status: "True", reason, message: "" });
    if (object.kind === "CustomResourceDefinition") object.status = { conditions: [condition("Established")] };
    if (object.kind === "Job") object.status = { conditions: [failing.has(name) ? { ...condition("Failed", "BackoffLimitExceeded"), message: "Job has reached the specified backoff limit" } : condition("Complete")] };
    if (object.kind !== "Deployment" && object.kind !== "StatefulSet") return;
    const replicas = (object.spec?.["replicas"] as number | undefined) ?? 1;
    const blocked = stuck.has(name);
    object.status = {
      observedGeneration: generation,
      replicas,
      updatedReplicas: replicas,
      availableReplicas: blocked ? 0 : replicas,
      readyReplicas: blocked ? 0 : replicas,
    };
    const template = object.spec?.["template"] as { metadata?: { labels?: Record<string, string> }; spec?: { containers?: { name: string; image: string }[] } } | undefined;
    const container = template?.spec?.containers?.[0];
    const pod = `${name}-0`;
    put("pods", {
      apiVersion: "v1",
      kind: "Pod",
      metadata: { name: pod, ...(namespace ? { namespace } : {}), labels: template?.metadata?.labels ?? {}, creationTimestamp: new Date().toISOString() },
      status: blocked
        ? {
          phase: "Pending",
          conditions: [{ type: "Ready", status: "False" }],
          containerStatuses: [{ name: container?.name ?? "main", ready: false, restartCount: 0, image: container?.image ?? "", imageID: "", state: { waiting: { reason: "ImagePullBackOff", message: `Back-off pulling image "${container?.image}"` } } }],
        }
        : {
          phase: "Running",
          conditions: [{ type: "Ready", status: "True" }],
          containerStatuses: [{ name: container?.name ?? "main", ready: true, restartCount: 0, image: container?.image ?? "", imageID: "", state: { running: {} } }],
        },
    });
    if (blocked) {
      put("events", {
        apiVersion: "v1",
        kind: "Event",
        metadata: { name: `${pod}.warning`, ...(namespace ? { namespace } : {}) },
        type: "Warning",
        reason: "Failed",
        message: `Failed to pull image "${container?.image}": not found`,
        involvedObject: { kind: "Pod", name: pod, namespace },
        lastTimestamp: new Date().toISOString(),
      });
    }
  };

  /** Deletes the object at `path`, and what is in it or of it: a namespace's objects, a workload's pods. */
  const remove = (path: string, object: KubeObject): void => {
    objects.delete(path);
    const { name, namespace } = object.metadata;
    for (const [other, each] of [...objects]) {
      const inside = object.kind === "Namespace" && each.metadata.namespace === name;
      const owned = each.kind === "Pod" && each.metadata.namespace === namespace && each.metadata.name.startsWith(`${name}-`);
      if (inside || owned) objects.delete(other);
    }
  };

  const handle = (request: IncomingMessage, body: Buffer, response: ServerResponse): void => {
    if (request.headers.authorization !== `Bearer ${TOKEN}`) return refuse(response, 401, "Unauthorized");
    const url = new URL(request.url ?? "/", "https://fake");
    const method = request.method ?? "GET";
    const target = parse(url.pathname);
    if (!target) return refuse(response, 404, `the server could not find the requested resource (${url.pathname})`);
    const path = url.pathname;
    if (method === "GET" && target.sub === "log") {
      const text = logs.get(target.name ?? "");
      if (text === undefined) return refuse(response, 404, `pods "${target.name}" not found`);
      response.writeHead(200, { "Content-Type": "text/plain" });
      return void response.end(text);
    }
    if (method === "GET" && !target.name) {
      const prefix = target.namespace ? `${target.api}/namespaces/${target.namespace}/${target.plural}/` : undefined;
      const items = [...objects]
        .filter(([key, object]) =>
          (prefix ? key.startsWith(prefix) : key.startsWith(`${target.api}/`) && key.split("/").at(-2) === target.plural)
          && selects(url.searchParams.get("labelSelector"), object.metadata.labels)
          && fields(url.searchParams.get("fieldSelector"), object)
        )
        .map(([, { apiVersion: _apiVersion, kind: _kind, ...object }]) => object);
      return json(response, 200, { kind: "List", apiVersion: "v1", metadata: {}, items });
    }
    const existing = objects.get(path);
    if (method === "GET") return existing ? json(response, 200, existing) : refuse(response, 404, `${target.plural} "${target.name}" not found`);
    if (method === "DELETE") {
      changes.push({ method, path, query: url.searchParams });
      if (!existing) return refuse(response, 404, `${target.plural} "${target.name}" not found`);
      remove(path, existing);
      return json(response, 200, { kind: "Status", apiVersion: "v1", status: "Success" });
    }
    if (method === "PATCH") {
      const contentType = request.headers["content-type"] ?? "";
      changes.push({ method, path, contentType, query: url.searchParams });
      const patch = JSON.parse(body.toString("utf8")) as KubeObject;
      if (contentType === "application/merge-patch+json" && !existing) return refuse(response, 404, `${target.plural} "${target.name}" not found`);
      version += 1;
      const base: KubeObject = existing ?? { ...patch, metadata: { ...patch.metadata, uid: `uid-${version}`, generation: 0, creationTimestamp: new Date().toISOString() } };
      const next = (contentType === "application/apply-patch+yaml" ? { ...base, ...patch, metadata: { ...base.metadata, ...patch.metadata } } : merged(base, patch)) as KubeObject;
      const specChanged = JSON.stringify(next.spec) !== JSON.stringify(existing?.spec);
      next.metadata = { ...next.metadata, resourceVersion: String(version), generation: (base.metadata.generation ?? 0) + (specChanged || !existing ? 1 : 0) };
      settle(next);
      objects.set(path, next);
      return json(response, existing ? 200 : 201, next);
    }
    refuse(response, 405, `${method} is not allowed`);
  };

  const server = createServer({ cert: CERTIFICATE, key: KEY }, (request, response) => {
    buffer(request).then((body) => handle(request, body, response), (error: unknown) => response.destroy(error as Error));
  });
  /** An exec, upgraded to a websocket: the command run as the test answers, its output and status sent, and the socket closed. */
  const exec = (request: IncomingMessage, socket: Duplex): void => {
    const url = new URL(request.url ?? "/", "https://fake");
    const target = parse(url.pathname);
    if (request.headers.authorization !== `Bearer ${TOKEN}` || target?.sub !== "exec") {
      return void socket.end("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n");
    }
    const accept = createHash("sha1").update(`${request.headers["sec-websocket-key"]}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
    socket.write(
      ["HTTP/1.1 101 Switching Protocols", "Upgrade: websocket", "Connection: Upgrade", `Sec-WebSocket-Accept: ${accept}`, "Sec-WebSocket-Protocol: v4.channel.k8s.io", "", ""].join("\r\n"),
    );
    socket.on("data", () => {});
    socket.on("error", () => socket.destroy());
    const asked: KubeExec = {
      namespace: target.namespace ?? "",
      pod: target.name ?? "",
      container: url.searchParams.get("container") ?? "",
      command: url.searchParams.getAll("command"),
      tty: url.searchParams.get("tty") === "true",
    };
    execs.push(asked);
    const { exitCode, stdout = "", stderr = "" } = fake.onExec(asked);
    socket.end(Buffer.concat([...(stdout ? [channel(1, stdout)] : []), ...(stderr ? [channel(2, stderr)] : []), channel(3, execStatus(exitCode)), Buffer.from([0x88, 0])]));
  };
  server.on("upgrade", exec);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  const url = `https://127.0.0.1:${port}`;
  const kubeConfig = new KubeConfig();
  kubeConfig.loadFromOptions({
    clusters: [{ name: "fake", server: url, caData: Buffer.from(CERTIFICATE).toString("base64") }],
    users: [{ name: "fake", token: TOKEN }],
    contexts: [{ name: "fake", cluster: "fake", user: "fake" }],
    currentContext: "fake",
  });

  const fake: FakeKube = {
    server: url,
    port,
    kubeconfig: kubeConfig.exportConfig(),
    objects,
    changes,
    stuck,
    failing,
    logs,
    execs,
    onExec: () => ({ exitCode: 0 }),
    get: (path) => objects.get(path),
    put,
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
    },
  };
  return fake;
}
