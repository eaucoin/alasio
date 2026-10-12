/**
 * The control plane Neon's proxy asks, as 06's gate needs it (its `cplane-v1` backend:
 * proxy/src/control_plane/client/cplane_proxy_v1.rs and messages.rs at release-9129), in
 * a pod of alasio's image in alasio's namespace, standing in for what 06 adds to
 * neon-control:
 *
 * - `GET /get_endpoint_access_control?endpointish&role`: the role's verifier, from the
 *   root Secret (./verifiers.ts), on main's endpoint, `alasio`; 404 otherwise, which the
 *   proxy takes as "no secret".
 * - `GET /wake_compute?endpointish`: main's compute scaled to one if it is at none, and,
 *   once its pod is ready, its address.
 *
 * Both take the proxy's bearer token, TOKEN. It logs each wake's timeline as a JSON line.
 *
 *   TOKEN=… node gate/06/cplane.ts
 */
import { readFileSync } from "node:fs";
import { createServer, type ServerResponse } from "node:http";
import { request } from "node:https";

import type { StackSecrets } from "../../neon/control/secrets.ts";
import { isRole, verifier } from "./verifiers.ts";

const ENDPOINT = "alasio";
const NAMESPACE = "alasio";
const COMPUTE = "alasio-neon-compute";
const ADDRESS = `${COMPUTE}.${NAMESPACE}.svc.cluster.local:55433`;
const SERVICE_ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount";

const token = process.env["TOKEN"];
if (!token) throw new Error("TOKEN is not set");
const secrets = JSON.parse(readFileSync("/secrets/secrets.json", "utf8")) as StackSecrets;

/** The API server, with the pod's ServiceAccount. */
function kube(method: string, path: string, body?: object): Promise<{ status: number; json: Record<string, unknown> }> {
  return new Promise((resolve, reject) => {
    const req = request({
      host: "kubernetes.default.svc",
      path,
      method,
      ca: readFileSync(`${SERVICE_ACCOUNT}/ca.crt`),
      headers: {
        authorization: `Bearer ${readFileSync(`${SERVICE_ACCOUNT}/token`, "utf8")}`,
        ...(body ? { "content-type": "application/merge-patch+json" } : {}),
      },
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, json: JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}") }));
    });
    req.on("error", reject);
    req.end(body ? JSON.stringify(body) : undefined);
  });
}

const deployment = `/apis/apps/v1/namespaces/${NAMESPACE}/deployments/${COMPUTE}`;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** One wake at a time: concurrent wakes share it. */
let waking: Promise<"warm" | "pool_miss"> | null = null;

async function wake(): Promise<"warm" | "pool_miss"> {
  const started = Date.now();
  const status = async () => ((await kube("GET", deployment)).json["status"] ?? {}) as { readyReplicas?: number };
  if ((await status()).readyReplicas) return "warm";
  await kube("PATCH", `${deployment}/scale`, { spec: { replicas: 1 } });
  const scaled = Date.now();
  while (!(await status()).readyReplicas) await sleep(250);
  console.log(JSON.stringify({ event: "woken", scaleMs: scaled - started, readyMs: Date.now() - started }));
  return "pool_miss";
}

/** Neon's error JSON, as `parse_body` reads it. */
function failure(res: ServerResponse, status: number, reason: string, message: string) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify({ error: message, status: { code: reason, message, details: { error_info: { reason }, retry_info: null, user_facing_message: { message } } } }));
}

createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://cplane");
  if (url.pathname === "/healthz") return void res.end("ok");
  if (req.headers.authorization !== `Bearer ${token}`) return failure(res, 401, "UNKNOWN", "not the proxy's token");
  const endpoint = url.searchParams.get("endpointish");
  console.log(JSON.stringify({ event: "request", path: url.pathname, endpoint, role: url.searchParams.get("role"), session: url.searchParams.get("session_id") }));
  if (endpoint !== ENDPOINT) return failure(res, 404, "ENDPOINT_NOT_FOUND", `no endpoint ${endpoint}`);
  if (url.pathname === "/get_endpoint_access_control") {
    const role = url.searchParams.get("role") ?? "";
    if (!isRole(role)) return failure(res, 404, "ROLE_NOT_FOUND", `no role ${role}`);
    res.writeHead(200, { "content-type": "application/json" });
    return void res.end(JSON.stringify({ role_secret: verifier(secrets, role), project_id: "alasio", allowed_ips: null }));
  }
  if (url.pathname === "/wake_compute") {
    waking ??= wake().finally(() => (waking = null));
    return void waking.then(
      (cold) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ address: ADDRESS, aux: { endpoint_id: ENDPOINT, project_id: "alasio", branch_id: "main", compute_id: ENDPOINT, cold_start_info: cold } }));
      },
      (error: unknown) => failure(res, 500, "UNKNOWN", String(error)),
    );
  }
  failure(res, 404, "RESOURCE_NOT_FOUND", `no ${url.pathname}`);
}).listen(8080, () => console.log(JSON.stringify({ event: "listening" })));
