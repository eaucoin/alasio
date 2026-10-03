/**
 * neon-control: alasio's small stand-in for Neon's control plane, the one part
 * of Neon that is not open source. Runs as a service of the stack.
 *
 * On start it bootstraps what the storage services need and cannot do
 * themselves, idempotently: registers the safekeepers with the storage
 * controller, waits for the pageserver to register, creates alasio's tenant and
 * timeline once, and makes the compute's spec. It then answers the storage
 * controller's compute hooks and serves the compute its spec, and reports
 * healthy once the compute can start.
 *
 * The compute fetches its spec as Neon's compute_ctl does from a control plane
 * (`--control-plane-uri`): `GET /compute/api/v2/computes/<id>/spec`, with the
 * compute's token as its bearer.
 *
 * Environment: CONTROLLER_URL; NEON_SAFEKEEPER_HOSTS, the safekeepers' hosts in
 * the order of their ids, comma-separated; and NEON_PAGESERVER_HOST. Mounts:
 * /secrets (secrets.json, the signing key), /keys (the public key), /state (its
 * record of what it bootstrapped).
 */
import { readFileSync, renameSync, writeFileSync, existsSync } from "node:fs";
import { createServer, type IncomingMessage } from "node:http";
import { timingSafeEqual } from "node:crypto";

import { type JsonWebKeySet, publicJwks, signToken, verifyToken } from "./jwt.ts";
import { scramVerifier } from "./scram.ts";
import type { StackSecrets } from "./secrets.ts";

/** A safekeeper of the stack, by its id. */
interface Safekeeper {
  id: number;
  host: string;
  pgPort: number;
  httpPort: number;
}

/** Where the storage controller placed the timeline: its safekeepers, by id, at a generation. */
interface Placement {
  generation: number;
  ids: number[];
}

/** What it bootstrapped: the timeline, and where it was placed. */
interface TimelineRecord {
  tenantId: string;
  timelineId: string;
  safekeepers: Placement;
}

/** RECORD, which holds nothing until the timeline is created. */
type BootstrapRecord = TimelineRecord | { safekeepers?: undefined };

/** A call to the storage controller. */
interface ControllerRequest {
  body?: unknown;
  token?: string;
  /** Statuses that are answers rather than failures. */
  allow?: readonly number[];
}

/** An HTTP answer, its body parsed as JSON, or null when it had none. */
interface JsonResponse<Body> {
  status: number;
  body: Body | null;
}

/** The storage controller's answer to creating a timeline. */
interface TimelineCreated {
  safekeepers?: { generation: number; safekeepers: { id: number }[] } | null;
}

/** A pageserver as the storage controller lists it. */
interface StorageNode {
  id: number;
  availability: unknown;
}

/** A safekeeper's status of a timeline, with its membership configuration. */
interface TimelineStatus {
  mconf: unknown;
}

type SettingType = "string" | "integer" | "enum" | "bool";

/** A Postgres setting in a compute spec. */
interface ComputeSetting {
  name: string;
  value: string;
  vartype: SettingType;
}

/** What compute_ctl is given: its spec and its own configuration. */
interface ComputeConfig {
  spec: ComputeSpec;
  compute_ctl_config: { jwks: JsonWebKeySet };
}

/** The compute's spec, as compute_ctl reads it (compute_api's ComputeSpec). */
interface ComputeSpec {
  format_version: number;
  suspend_timeout_seconds: number;
  cluster: {
    cluster_id: string;
    name: string;
    roles: { name: string; encrypted_password: string; options: null }[];
    databases: { name: string; owner: string; options: null }[];
    settings: ComputeSetting[];
  };
  delta_operations: unknown[];
  tenant_id: string;
  timeline_id: string;
  mode: "Primary";
  pageserver_connstring: string;
  safekeepers_generation: number;
  safekeeper_connstrings: string[];
  storage_auth_token: string;
}

const CONTROLLER_URL = process.env["CONTROLLER_URL"];
const PG_VERSION = 17;
const COMPUTE_PORT = 55433;
const SAFEKEEPER_HOSTS = (process.env["NEON_SAFEKEEPER_HOSTS"] ?? "").split(",").map((host) => host.trim()).filter(Boolean);
const SAFEKEEPERS: Safekeeper[] = SAFEKEEPER_HOSTS.map((host, index) => ({ id: index + 1, host, pgPort: 5454, httpPort: 7676 }));
const PAGESERVER_HOST = process.env["NEON_PAGESERVER_HOST"];
const RECORD = "/state/bootstrap.json";
if (!CONTROLLER_URL || SAFEKEEPER_HOSTS.length === 0 || !PAGESERVER_HOST) {
  throw new Error("CONTROLLER_URL, NEON_SAFEKEEPER_HOSTS and NEON_PAGESERVER_HOST must be set");
}
const COMPUTE_ID = "alasio";

// Written by the setup job (./kube-setup.ts).
const secrets: StackSecrets = JSON.parse(readFileSync("/secrets/secrets.json", "utf8"));
const privateKeyPem = readFileSync("/secrets/auth_private_key.pem", "utf8");
const publicKeyPem = readFileSync("/keys/auth_public_key.pem", "utf8");
// The storage controller's /control and /debug APIs take the admin scope.
const adminToken = signToken(privateKeyPem, "admin");
const safekeeperToken = signToken(privateKeyPem, "safekeeperdata");
const REPAIR_INTERVAL_MS = 30_000;

let ready = false;

function log(message: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Calls the storage controller; `Body` is what its API answers this call with. */
async function controller<Body = unknown>(
  method: string,
  path: string,
  { body, token = adminToken, allow = [] }: ControllerRequest = {},
): Promise<JsonResponse<Body>> {
  const response = await fetch(`${CONTROLLER_URL}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const text = await response.text();
  if (!response.ok && !allow.includes(response.status)) {
    throw new Error(`${method} ${path}: ${response.status} ${text}`);
  }
  const parsed: Body | null = text ? JSON.parse(text) : null;
  return { status: response.status, body: parsed };
}

async function until<T>(what: string, check: () => Promise<T>, { attempts = 300, delayMs = 1000 } = {}): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      const result = await check();
      if (result) return result;
    } catch (error) {
      if (attempt === attempts) throw error;
    }
    if (attempt === attempts) throw new Error(`gave up waiting for ${what}`);
    await sleep(delayMs);
  }
}

function readRecord(): BootstrapRecord {
  // This service's own record (writeAtomically), or nothing yet.
  const record: BootstrapRecord = existsSync(RECORD) ? JSON.parse(readFileSync(RECORD, "utf8")) : {};
  return record;
}

function writeAtomically(path: string, value: unknown): void {
  writeFileSync(`${path}.tmp`, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  renameSync(`${path}.tmp`, path);
}

async function registerSafekeepers(): Promise<void> {
  for (const sk of SAFEKEEPERS) {
    await controller("POST", `/control/v1/safekeeper/${sk.id}`, {
      body: {
        id: sk.id,
        region_id: "local",
        version: 1,
        host: sk.host,
        port: sk.pgPort,
        http_port: sk.httpPort,
        // One zone each: a timeline's safekeepers are placed in distinct zones.
        availability_zone_id: `az-safekeeper-${sk.id}`,
        created_at: "2026-01-01T00:00:00Z",
        updated_at: "2026-01-01T00:00:00Z",
      },
    });
    await controller("POST", `/control/v1/safekeeper/${sk.id}/scheduling_policy`, {
      body: { scheduling_policy: "Active" },
    });
  }
  log("safekeepers registered");
}

async function ensureTenant(): Promise<void> {
  const found = await controller("GET", `/control/v1/tenant/${secrets.tenantId}`, { allow: [404] });
  if (found.status === 404) {
    await controller("POST", "/v1/tenant", {
      body: { new_tenant_id: secrets.tenantId, placement_policy: { Attached: 0 } },
    });
    log("tenant created", { tenantId: secrets.tenantId });
  }
}

/** Creates the timeline once, recording the safekeepers it was placed on. */
async function ensureTimeline(record: BootstrapRecord): Promise<TimelineRecord> {
  if (record.safekeepers) return record;
  const created = await controller<TimelineCreated>("POST", `/v1/tenant/${secrets.tenantId}/timeline`, {
    body: { new_timeline_id: secrets.timelineId, pg_version: PG_VERSION },
  });
  // A timeline created is answered with its description.
  const placed = created.body!.safekeepers;
  if (!placed) throw new Error("the storage controller placed the timeline on no safekeepers");
  const next: TimelineRecord = {
    ...record,
    tenantId: secrets.tenantId,
    timelineId: secrets.timelineId,
    safekeepers: { generation: placed.generation, ids: placed.safekeepers.map((sk) => sk.id) },
  };
  writeAtomically(RECORD, next);
  log("timeline created", { timelineId: secrets.timelineId, safekeepers: next.safekeepers });
  return next;
}

/** Calls a safekeeper's HTTP API; `Body` is what it answers this call with. */
async function safekeeper<Body = unknown>(sk: Safekeeper, method: string, path: string, body?: unknown): Promise<JsonResponse<Body>> {
  const response = await fetch(`http://${sk.host}:${sk.httpPort}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${safekeeperToken}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  const text = await response.text();
  const parsed: Body | null = text ? JSON.parse(text) : null;
  return { status: response.status, body: parsed };
}

/** A safekeeper and its status of the timeline, or null where it could not be asked. */
interface SafekeeperState {
  sk: Safekeeper;
  state: JsonResponse<TimelineStatus> | null;
}

/** A safekeeper that has the timeline: it answered 200, with the timeline's status. */
interface HealthySafekeeper {
  sk: Safekeeper;
  state: { status: 200; body: TimelineStatus };
}

/**
 * A safekeeper that lost its disk comes back without the timeline, and in
 * membership mode does not recreate it: it pulls it from the peers that
 * have it, joining the timeline's current membership.
 */
async function repairSafekeepers(record: TimelineRecord): Promise<void> {
  const placed = SAFEKEEPERS.filter((sk) => record.safekeepers.ids.includes(sk.id));
  const path = `/v1/tenant/${record.tenantId}/timeline/${record.timelineId}`;
  const states = await Promise.all(
    placed.map(async (sk): Promise<SafekeeperState> => ({ sk, state: await safekeeper<TimelineStatus>(sk, "GET", path).catch(() => null) })),
  );
  const healthy = states.filter((entry): entry is HealthySafekeeper => entry.state?.status === 200);
  for (const { sk, state } of states) {
    if (state?.status !== 404 || healthy.length === 0) continue;
    const pulled = await safekeeper(sk, "POST", "/v1/pull_timeline", {
      tenant_id: record.tenantId,
      timeline_id: record.timelineId,
      http_hosts: healthy.map(({ sk: peer }) => `http://${peer.host}:${peer.httpPort}`),
      // Not empty: checked above.
      mconf: healthy[0]!.state.body.mconf,
    });
    log(pulled.status === 200 ? "safekeeper repaired from its peers" : "safekeeper repair failed", {
      safekeeper: sk.id,
      status: pulled.status,
      body: pulled.body,
    });
  }
}

function setting(name: string, value: string | number, vartype: SettingType): ComputeSetting {
  return { name, value: String(value), vartype };
}

let computeConfig: ComputeConfig | null = null;

/** The compute's spec: alasio's role and database on alasio's timeline. */
function makeSpec(record: TimelineRecord): void {
  // The timeline was placed on safekeepers of the stack, so each id is one of theirs.
  const hostOf = (id: number) => SAFEKEEPERS.find((sk) => sk.id === id)!.host;
  computeConfig = {
    spec: {
      format_version: 1.0,
      suspend_timeout_seconds: -1,
      cluster: {
        cluster_id: "alasio",
        name: "alasio",
        roles: [{ name: "alasio", encrypted_password: scramVerifier(secrets.alasioPassword), options: null }],
        databases: [{ name: "alasio", owner: "alasio", options: null }],
        settings: [
          setting("listen_addresses", "0.0.0.0", "string"),
          setting("port", COMPUTE_PORT, "integer"),
          setting("max_connections", 100, "integer"),
          setting("shared_buffers", "128MB", "string"),
          setting("password_encryption", "scram-sha-256", "enum"),
          // Durability is the safekeepers': a commit waits for their quorum,
          // and the compute's own disk is rebuilt on every start.
          setting("fsync", "off", "bool"),
          setting("synchronous_standby_names", "walproposer", "string"),
          setting("shared_preload_libraries", "neon", "string"),
          setting("wal_level", "replica", "enum"),
          setting("wal_log_hints", "off", "bool"),
          setting("wal_keep_size", 0, "integer"),
          setting("wal_sender_timeout", "5s", "string"),
          setting("max_wal_senders", 10, "integer"),
          setting("max_replication_slots", 10, "integer"),
          setting("max_replication_write_lag", "15MB", "string"),
          setting("max_replication_flush_lag", "10GB", "string"),
          setting("restart_after_crash", "off", "bool"),
        ],
      },
      delta_operations: [],
      tenant_id: record.tenantId,
      timeline_id: record.timelineId,
      mode: "Primary",
      pageserver_connstring: `postgresql://no_user@${PAGESERVER_HOST}:6400`,
      safekeepers_generation: record.safekeepers.generation,
      safekeeper_connstrings: record.safekeepers.ids.map((id) => `${hostOf(id)}:5454`),
      storage_auth_token: signToken(privateKeyPem, "tenant", record.tenantId),
    },
    compute_ctl_config: { jwks: publicJwks(publicKeyPem) },
  };
  log("compute spec made");
}

async function bootstrap(): Promise<void> {
  await until("the storage controller", async () => (await controller("GET", "/status")).status === 200);
  await registerSafekeepers();
  await until("the pageserver to register", async () => {
    // The controller answers with its nodes; a failure here is retried.
    const nodes = (await controller<StorageNode[]>("GET", "/control/v1/node")).body!;
    return nodes.some((node) => node.id === 1 && node.availability === "Active");
  });
  await ensureTenant();
  const record = await ensureTimeline(readRecord());
  await repairSafekeepers(record);
  makeSpec(record);
  ready = true;
  log("ready");
  setInterval(() => {
    repairSafekeepers(record).catch((error: unknown) => log("safekeeper check failed", { error: errorText(error) }));
  }, REPAIR_INTERVAL_MS).unref();
}

function authorized(request: IncomingMessage): boolean {
  const token = request.headers.authorization?.replace(/^Bearer /, "");
  return Boolean(token && verifyToken(publicKeyPem, token));
}

/** Whether the request carries the compute's token, which only the compute is given. */
function fromCompute(request: IncomingMessage): boolean {
  const presented = Buffer.from(request.headers.authorization ?? "");
  const expected = Buffer.from(`Bearer ${secrets.computeControlToken}`);
  return Boolean(secrets.computeControlToken) && presented.length === expected.length && timingSafeEqual(presented, expected);
}

const server = createServer((request, response) => {
  if (request.method === "GET" && request.url === "/healthz") {
    response.writeHead(ready ? 200 : 503).end(ready ? "ready\n" : "bootstrapping\n");
    return;
  }
  if (request.method === "GET" && request.url === `/compute/api/v2/computes/${COMPUTE_ID}/spec`) {
    if (!fromCompute(request)) {
      response.writeHead(401).end();
      return;
    }
    // Until bootstrapped there is no spec yet; compute_ctl retries, and then
    // its container is restarted.
    if (!computeConfig) {
      response.writeHead(503).end();
      return;
    }
    response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ ...computeConfig, status: "attached" }));
    return;
  }
  if (request.method === "PUT" && (request.url === "/notify-attach" || request.url === "/notify-safekeepers")) {
    if (!authorized(request)) {
      response.writeHead(401).end();
      return;
    }
    let body = "";
    request.on("data", (chunk: Buffer) => (body += chunk));
    request.on("end", () => {
      // One compute, whose spec names the one pageserver and the timeline's
      // safekeepers: an attach changes nothing it needs. A safekeeper
      // migration would, and alasio never migrates; it is recorded loudly.
      log(`compute hook ${request.url}`, { body: body.slice(0, 2000) });
      response.writeHead(200).end();
    });
    return;
  }
  response.writeHead(404).end();
});
server.listen(8080, "0.0.0.0", () => log("listening", { port: 8080 }));

// As PID 1 in its container, Node would otherwise ignore the SIGTERM that stops it.
process.on("SIGTERM", () => server.close(() => process.exit(0)));

bootstrap().catch((error: unknown) => {
  log("bootstrap failed", { error: errorText(error) });
  process.exit(1);
});
