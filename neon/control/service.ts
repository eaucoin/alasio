// @ts-nocheck
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
import { createServer } from "node:http";
import { timingSafeEqual } from "node:crypto";

import { publicJwks, signToken, verifyToken } from "./jwt.ts";
import { scramVerifier } from "./scram.ts";

const CONTROLLER_URL = process.env.CONTROLLER_URL;
const PG_VERSION = 17;
const COMPUTE_PORT = 55433;
const SAFEKEEPER_HOSTS = (process.env.NEON_SAFEKEEPER_HOSTS ?? "").split(",").map((host) => host.trim()).filter(Boolean);
const SAFEKEEPERS = SAFEKEEPER_HOSTS.map((host, index) => ({ id: index + 1, host, pgPort: 5454, httpPort: 7676 }));
const PAGESERVER_HOST = process.env.NEON_PAGESERVER_HOST;
const RECORD = "/state/bootstrap.json";
if (!CONTROLLER_URL || SAFEKEEPER_HOSTS.length === 0 || !PAGESERVER_HOST) {
  throw new Error("CONTROLLER_URL, NEON_SAFEKEEPER_HOSTS and NEON_PAGESERVER_HOST must be set");
}
const COMPUTE_ID = "alasio";

const secrets = JSON.parse(readFileSync("/secrets/secrets.json", "utf8"));
const privateKeyPem = readFileSync("/secrets/auth_private_key.pem", "utf8");
const publicKeyPem = readFileSync("/keys/auth_public_key.pem", "utf8");
// The storage controller's /control and /debug APIs take the admin scope.
const adminToken = signToken(privateKeyPem, "admin");
const safekeeperToken = signToken(privateKeyPem, "safekeeperdata");
const REPAIR_INTERVAL_MS = 30_000;

let ready = false;

function log(message, fields = {}) {
  console.log(JSON.stringify({ time: new Date().toISOString(), message, ...fields }));
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function controller(method, path, { body, token = adminToken, allow = [] } = {}) {
  const response = await fetch(`${CONTROLLER_URL}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok && !allow.includes(response.status)) {
    throw new Error(`${method} ${path}: ${response.status} ${text}`);
  }
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

async function until(what, check, { attempts = 300, delayMs = 1000 } = {}) {
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

function readRecord() {
  return existsSync(RECORD) ? JSON.parse(readFileSync(RECORD, "utf8")) : {};
}

function writeAtomically(path, value) {
  writeFileSync(`${path}.tmp`, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  renameSync(`${path}.tmp`, path);
}

async function registerSafekeepers() {
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

async function ensureTenant() {
  const found = await controller("GET", `/control/v1/tenant/${secrets.tenantId}`, { allow: [404] });
  if (found.status === 404) {
    await controller("POST", "/v1/tenant", {
      body: { new_tenant_id: secrets.tenantId, placement_policy: { Attached: 0 } },
    });
    log("tenant created", { tenantId: secrets.tenantId });
  }
}

/** Creates the timeline once, recording the safekeepers it was placed on. */
async function ensureTimeline(record) {
  if (record.safekeepers) return record;
  const created = await controller("POST", `/v1/tenant/${secrets.tenantId}/timeline`, {
    body: { new_timeline_id: secrets.timelineId, pg_version: PG_VERSION },
  });
  const placed = created.body.safekeepers;
  if (!placed) throw new Error("the storage controller placed the timeline on no safekeepers");
  const next = {
    ...record,
    tenantId: secrets.tenantId,
    timelineId: secrets.timelineId,
    safekeepers: { generation: placed.generation, ids: placed.safekeepers.map((sk) => sk.id) },
  };
  writeAtomically(RECORD, next);
  log("timeline created", { timelineId: secrets.timelineId, safekeepers: next.safekeepers });
  return next;
}

async function safekeeper(sk, method, path, body) {
  const response = await fetch(`http://${sk.host}:${sk.httpPort}${path}`, {
    method,
    headers: {
      authorization: `Bearer ${safekeeperToken}`,
      ...(body === undefined ? {} : { "content-type": "application/json" }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : null };
}

/**
 * A safekeeper that lost its disk comes back without the timeline, and in
 * membership mode does not recreate it: it pulls it from the peers that
 * have it, joining the timeline's current membership.
 */
async function repairSafekeepers(record) {
  const placed = SAFEKEEPERS.filter((sk) => record.safekeepers.ids.includes(sk.id));
  const path = `/v1/tenant/${record.tenantId}/timeline/${record.timelineId}`;
  const states = await Promise.all(
    placed.map(async (sk) => ({ sk, state: await safekeeper(sk, "GET", path).catch(() => null) })),
  );
  const healthy = states.filter(({ state }) => state?.status === 200);
  for (const { sk, state } of states) {
    if (state?.status !== 404 || healthy.length === 0) continue;
    const pulled = await safekeeper(sk, "POST", "/v1/pull_timeline", {
      tenant_id: record.tenantId,
      timeline_id: record.timelineId,
      http_hosts: healthy.map(({ sk: peer }) => `http://${peer.host}:${peer.httpPort}`),
      mconf: healthy[0].state.body.mconf,
    });
    log(pulled.status === 200 ? "safekeeper repaired from its peers" : "safekeeper repair failed", {
      safekeeper: sk.id,
      status: pulled.status,
      body: pulled.body,
    });
  }
}

function setting(name, value, vartype) {
  return { name, value: String(value), vartype };
}

let computeConfig = null;

/** The compute's spec: alasio's role and database on alasio's timeline. */
function makeSpec(record) {
  const hostOf = (id) => SAFEKEEPERS.find((sk) => sk.id === id).host;
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

async function bootstrap() {
  await until("the storage controller", async () => (await controller("GET", "/status")).status === 200);
  await registerSafekeepers();
  await until("the pageserver to register", async () => {
    const nodes = (await controller("GET", "/control/v1/node")).body;
    return nodes.some((node) => node.id === 1 && node.availability === "Active");
  });
  await ensureTenant();
  const record = await ensureTimeline(readRecord());
  await repairSafekeepers(record);
  makeSpec(record);
  ready = true;
  log("ready");
  setInterval(() => {
    repairSafekeepers(record).catch((error) => log("safekeeper check failed", { error: error.message }));
  }, REPAIR_INTERVAL_MS).unref();
}

function authorized(request) {
  const token = request.headers.authorization?.replace(/^Bearer /, "");
  return Boolean(token && verifyToken(publicKeyPem, token));
}

/** Whether the request carries the compute's token, which only the compute is given. */
function fromCompute(request) {
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
    request.on("data", (chunk) => (body += chunk));
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

bootstrap().catch((error) => {
  log("bootstrap failed", { error: error.message });
  process.exit(1);
});
