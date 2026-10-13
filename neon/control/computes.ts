/**
 * The computes, as neon-control wakes them: each the Deployment of
 * one replica, or none, that a branch environment's objects (or main's) run it as, which
 * neon-control scales through its scale subresource with its ServiceAccount's token, and
 * reaches at its Service, which takes connections before its pod is ready.
 *
 * It logs in to each compute as a role of its own, which every compute's spec gives a
 * password of that compute's own (./branches.ts): so it logs in once compute_ctl has
 * applied that spec's roles, alasio's among them, and a wake is answered then, not once
 * kubelet next probes the pod, nor before a new branch's compute has its own roles in
 * place of the ones it forked from its parent, whose own password for it the copy holds.
 * compute_ctl makes every role of a spec a member of neon_superuser
 * (compute_tools/src/spec_apply.rs at Neon's release-compute-9073), and so this one,
 * which may then read and write any table: it does nothing but log in, and its
 * passwords are neon-control's alone, which holds the stack's signing key, and main's
 * passwords, besides.
 */
import { readFileSync } from "node:fs";
import { request } from "node:https";

import pg from "pg";

const SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount";
/** How long a call of the API server may take. */
const KUBE_PATIENCE_MS = 10_000;

/** A compute: its Deployment, in its namespace, which is also its Service's name. */
export interface ComputeRef {
  namespace: string;
  name: string;
}

/** A compute's Deployment, as far as neon-control reads it. */
export interface ComputeDeployment {
  replicas: number;
}

/** Calls the API server with neon-control's ServiceAccount: what it answered, its body parsed. */
function kube(method: string, path: string, body?: object): Promise<{ status: number; json: Record<string, unknown> }> {
  return new Promise((resolve, reject) => {
    const req = request({
      host: process.env["KUBERNETES_SERVICE_HOST"] ?? "kubernetes.default.svc",
      port: process.env["KUBERNETES_SERVICE_PORT"] ?? 443,
      path,
      method,
      ca: readFileSync(`${SERVICE_ACCOUNT}/ca.crt`),
      headers: {
        // Read on every call, as kubelet renews it.
        authorization: `Bearer ${readFileSync(`${SERVICE_ACCOUNT}/token`, "utf8")}`,
        accept: "application/json",
        ...(body ? { "content-type": "application/merge-patch+json" } : {}),
      },
    }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk: Buffer) => chunks.push(chunk));
      response.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        try {
          resolve({ status: response.statusCode ?? 0, json: JSON.parse(text || "{}") });
        } catch {
          reject(new Error(`${method} ${path}: the API server answered ${response.statusCode ?? 0}, not JSON: ${text.slice(0, 200)}`));
        }
      });
      response.on("error", reject);
    });
    req.on("error", reject);
    req.setTimeout(KUBE_PATIENCE_MS, () => req.destroy(new Error(`${method} ${path}: the API server did not answer in ${KUBE_PATIENCE_MS} ms`)));
    req.end(body ? JSON.stringify(body) : undefined);
  });
}

const deploymentPath = ({ namespace, name }: ComputeRef) => `/apis/apps/v1/namespaces/${encodeURIComponent(namespace)}/deployments/${encodeURIComponent(name)}`;

/** The compute's Deployment, or null when there is none: a branch environment not made yet, or being deleted. */
export async function computeDeployment(ref: ComputeRef): Promise<ComputeDeployment | null> {
  const { status, json } = await kube("GET", deploymentPath(ref));
  if (status === 404) return null;
  if (status !== 200) throw new Error(`reading the Deployment ${ref.namespace}/${ref.name}: ${status} ${JSON.stringify(json)}`);
  const { spec } = json as { spec?: { replicas?: number } };
  return { replicas: spec?.replicas ?? 1 };
}

/** Scales the compute to `replicas`: 1 wakes it. */
export async function scaleCompute(ref: ComputeRef, replicas: 1): Promise<void> {
  const { status, json } = await kube("PATCH", `${deploymentPath(ref)}/scale`, { spec: { replicas } });
  if (status !== 200) throw new Error(`scaling ${ref.namespace}/${ref.name} to ${replicas}: ${status} ${JSON.stringify(json)}`);
}

/**
 * Logs in to the compute at `host`:`port` as neon-control's own role, on the `postgres`
 * database, which no client uses, and out again. Fails while it cannot: the compute not
 * started, or its spec's roles not yet applied.
 */
export async function computeAccepts(host: string, port: number, role: string, password: string): Promise<void> {
  const client = new pg.Client({ host, port, user: role, password, database: "postgres", connectionTimeoutMillis: 5000 });
  client.on("error", () => {});
  await client.connect();
  await client.end().catch(() => {});
}
