/**
 * The computes, as neon-control wakes them and puts them to sleep: each the Deployment of
 * one replica, or none, that a branch environment's objects (or main's) run it as, which
 * neon-control scales through its scale subresource, and restarts when it must start on
 * another spec, with its ServiceAccount's token, and reaches at its Service, which takes
 * connections before its pod is ready.
 *
 * It logs in to each compute as a role of its own, which every compute's spec gives a
 * password of that compute's own (./branches.ts): so it logs in once compute_ctl has
 * applied that spec's roles, alasio's among them, and a wake is answered then, not once
 * kubelet next probes the pod, nor before a new branch's compute has its own roles in
 * place of the ones it forked from its parent, whose own password for it the copy holds.
 * compute_ctl makes every role of a spec a member of neon_superuser
 * (compute_tools/src/spec_apply.rs at Neon's release-compute-9073), and so this one,
 * which may then read and write any table: it does nothing but count, in read-only
 * transactions, and its passwords are neon-control's alone, which holds the stack's
 * signing key, and main's passwords, besides.
 *
 * Its Deployment says how long it may be idle before it sleeps (SUSPEND_AFTER), which its
 * environment's configuration gives it; one that does not say never sleeps.
 */
import { readFileSync } from "node:fs";
import { request } from "node:https";

import pg from "pg";

import { COMPUTE_CTL_PORT, type ComputeConfig } from "./branches.ts";
import { type Activity, SUSPEND_AFTER } from "./endpoints.ts";

const SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount";
/** The container of a compute's pod that runs compute_ctl, and Postgres under it. */
const COMPUTE_CONTAINER = "compute";
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
  ready: number;
  /** Its pods' label selector, as the API takes one. */
  selector: string;
  /** How long it may be idle before it sleeps, or null for never. */
  suspendAfterMs: number | null;
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
  const { metadata, spec, status: observed } = json as {
    metadata?: { annotations?: Record<string, string> };
    spec?: { replicas?: number; selector?: { matchLabels?: Record<string, string> } };
    status?: { readyReplicas?: number };
  };
  const seconds = Number(metadata?.annotations?.[SUSPEND_AFTER]);
  return {
    replicas: spec?.replicas ?? 1,
    ready: observed?.readyReplicas ?? 0,
    selector: Object.entries(spec?.selector?.matchLabels ?? {}).map(([key, value]) => `${key}=${value}`).join(","),
    suspendAfterMs: Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : null,
  };
}

/** How many pods the compute's Deployment has that run, or may (livePods), as the API lists them. */
export async function computePods({ namespace }: ComputeRef, { selector }: ComputeDeployment): Promise<number> {
  const { status, json } = await kube("GET", `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods?labelSelector=${encodeURIComponent(selector)}`);
  if (status !== 200) throw new Error(`listing the pods of ${namespace}'s compute: ${status} ${JSON.stringify(json)}`);
  return livePods((json["items"] as { status?: { phase?: string } }[] | undefined) ?? []);
}

/**
 * How many of a compute's pods `items` run, or may: one stopping among them, but none that
 * has ended, Failed or Succeeded, as an evicted pod is, which lingers until deleted.
 */
export const livePods = (items: readonly { status?: { phase?: string } }[]): number =>
  items.filter(({ status }) => status?.phase !== "Failed" && status?.phase !== "Succeeded").length;

/** A pod as the API lists it, as far as whether its container runs is read. */
interface ListedPod {
  metadata?: { deletionTimestamp?: string };
  status?: { containerStatuses?: { name: string; state?: { running?: object } }[] };
}

/** Whether any of `items`, not being deleted, runs its container `container`. */
export const containerRuns = (items: readonly ListedPod[], container: string): boolean =>
  items.some(({ metadata, status }) => !metadata?.deletionTimestamp && (status?.containerStatuses ?? []).some(({ name, state }) => name === container && state?.running !== undefined));

/** Whether a pod of the compute's Deployment runs its compute's container, compute_ctl in it (containerRuns). */
export async function computeRuns({ namespace }: ComputeRef, { selector }: ComputeDeployment): Promise<boolean> {
  const { status, json } = await kube("GET", `/api/v1/namespaces/${encodeURIComponent(namespace)}/pods?labelSelector=${encodeURIComponent(selector)}`);
  if (status !== 200) throw new Error(`listing the pods of ${namespace}'s compute: ${status} ${JSON.stringify(json)}`);
  return containerRuns((json["items"] as ListedPod[] | undefined) ?? [], COMPUTE_CONTAINER);
}

/** Restarts the compute, as `kubectl rollout restart` does: its pod made anew, which fetches its spec as it is now. */
export async function restartCompute(ref: ComputeRef): Promise<void> {
  const restartedAt = new Date().toISOString();
  const { status, json } = await kube("PATCH", deploymentPath(ref), { spec: { template: { metadata: { annotations: { "kubectl.kubernetes.io/restartedAt": restartedAt } } } } });
  if (status !== 200) throw new Error(`restarting ${ref.namespace}/${ref.name}: ${status} ${JSON.stringify(json)}`);
}

/** Scales the compute to `replicas`, 1 to wake it or 0 to put it to sleep. */
export async function scaleCompute(ref: ComputeRef, replicas: 0 | 1): Promise<void> {
  const { status, json } = await kube("PATCH", `${deploymentPath(ref)}/scale`, { spec: { replicas } });
  if (status !== 200) throw new Error(`scaling ${ref.namespace}/${ref.name} to ${replicas}: ${status} ${JSON.stringify(json)}`);
}

/**
 * What the clients of the compute at `host`:`port` are doing (./endpoints.ts's Activity),
 * counted as neon-control's own role, on the `postgres` database, which no client uses:
 * their sessions open now, the backends on a database of any role but its own and
 * compute_ctl's (cloud_admin), whatever their state, so an idle one holding an advisory
 * lock counts (by what pg_stat_activity shows of every backend to any role: no Postgres
 * process of its own has both a database and a role);
 * and the sessions established to every other database. Fails while it cannot log in: the
 * compute not started, or its spec's roles not yet applied.
 */
export async function computeActivity(host: string, port: number, role: string, password: string): Promise<Activity> {
  // Read-only, as all it does is count.
  const client = new pg.Client({ host, port, user: role, password, database: "postgres", connectionTimeoutMillis: 5000, options: "-c default_transaction_read_only=on" });
  client.on("error", () => {});
  await client.connect();
  try {
    const { rows } = await client.query<Activity>(
      `select
         (select count(*)::int from pg_stat_activity
          where pid <> pg_backend_pid() and datname is not null and usename not in ($1, 'cloud_admin')) as sessions,
         (select coalesce(sum(sessions), 0)::int from pg_stat_database
          where datname not in ('postgres', 'template0', 'template1')) as established`,
      [role],
    );
    // One row, of two counts.
    return rows[0]!;
  } finally {
    await client.end().catch(() => {});
  }
}

/**
 * How long compute_ctl may take to apply a spec: a reload of its settings, and its roles
 * and databases checked, which took about 120 ms in CI; one that takes longer is asked
 * again with the storage controller's next notification, which waits 10 s for an answer
 * (./service.ts's NOTIFY_PATIENCE_MS).
 */
const CONFIGURE_PATIENCE_MS = 4_000;
/** How long compute_ctl may take to say how its compute is. */
const STATUS_PATIENCE_MS = 2_000;

/**
 * Gives the compute at `host` the spec of `config` through compute_ctl's /configure, with
 * `token` (computeCtlToken), which applies it live, a changed pageserver connstring by a
 * reload: the status it answered, 200 once it has, 412 while the compute is not running
 * (computeCtlStatus says how it is), or 0 where compute_ctl was not reached.
 */
export async function configureCompute(host: string, token: string, config: ComputeConfig): Promise<number> {
  const response = await fetch(`http://${host}:${COMPUTE_CTL_PORT}/configure`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: JSON.stringify(config),
    signal: AbortSignal.timeout(CONFIGURE_PATIENCE_MS),
  }).catch(() => null);
  await response?.body?.cancel();
  return response?.status ?? 0;
}

/**
 * How compute_ctl says the compute at `host` is (its /status, with `token`): `init` while
 * it starts, `running`, `configuration` while it applies a spec, `failed`, and others
 * (libs/compute_api/src/responses.rs at Neon's release-compute-9073); null where it was not
 * reached, or did not say.
 */
export async function computeCtlStatus(host: string, token: string): Promise<string | null> {
  const response = await fetch(`http://${host}:${COMPUTE_CTL_PORT}/status`, {
    headers: { authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(STATUS_PATIENCE_MS),
  }).catch(() => null);
  const body: unknown = response?.ok ? await response.json().catch(() => null) : null;
  return typeof body === "object" && body !== null && "status" in body && typeof body.status === "string" ? body.status : null;
}

/**
 * The LSN the compute at `host`:`port` has: a primary's flushed WAL's end, or a replica's
 * replayed (`replica`), as neon-control's own role reads it, read-only.
 */
export async function computeLsn(host: string, port: number, role: string, password: string, replica: boolean): Promise<string> {
  const client = new pg.Client({ host, port, user: role, password, database: "postgres", connectionTimeoutMillis: 5000, options: "-c default_transaction_read_only=on" });
  client.on("error", () => {});
  await client.connect();
  try {
    const { rows } = await client.query<{ lsn: string | null }>(`select ${replica ? "pg_last_wal_replay_lsn()" : "pg_current_wal_flush_lsn()"}::text as lsn`);
    const lsn = rows[0]?.lsn;
    if (!lsn) throw new Error(`${host} has no LSN to read`);
    return lsn;
  } finally {
    await client.end().catch(() => {});
  }
}
