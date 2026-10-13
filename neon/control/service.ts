/**
 * neon-control: alasio's small stand-in for Neon's control plane, the one part
 * of Neon that is not open source. Runs as a service of the stack, as many replicas as
 * its scale says, which share its record of the branches in the storage controller's
 * Postgres (./record.ts).
 *
 * On start it bootstraps what the storage services need and cannot do
 * themselves, idempotently: registers the safekeepers with the storage
 * controller, waits for the pageserver to register, creates alasio's tenant
 * and its timeline, the branch `main`, once, and configures the tenant to keep
 * a day of history (HISTORY_DAYS). It then manages branches, child
 * timelines of main or of each other (./branches.ts), serves each branch's
 * compute its spec, from the record, or as last read while the record cannot be, answers
 * the storage controller's compute hooks, and reports healthy once main's compute can
 * start. The replica that leads (./record.ts) repairs every branch's safekeepers.
 *
 * A compute fetches its spec as Neon's compute_ctl does from a control plane
 * (`--control-plane-uri`): `GET /compute/api/v2/computes/<id>/spec`, with its
 * compute's own token as its bearer (401 for no compute's, 403 for another's);
 * its id is `alasio` for main's, `branch-<name>` for a branch's. neon-control
 * serves a branch's spec; it does not run its compute.
 *
 * Its API takes a token of the admin scope, as the storage controller's hooks
 * do, or for `GET /branches` alone one of the `branches` scope, the lake's, and
 * answers 401 without one, and 503 until bootstrapped. It speaks JSON; a
 * refusal is `{ "error": "<why>" }` with its status:
 *
 * - `POST /branches`, `{ name, parent?, lsn?, compute }`: creates the branch
 *   `name` of `parent` (main if not given) at `lsn`, its compute's credentials
 *   `compute` (`{ passwordVerifier, tokenSha256 }`, ./branches.ts), and answers
 *   it (201), or the same
 *   branch asked for again (200). Without `lsn`, the branch point is where the
 *   pageserver has the parent's WAL up to, which trails its last commits by as
 *   long as the WAL takes to reach it; a branch that must hold a commit is
 *   asked for at an LSN after it, which the pageserver waits for. 400 for a name
 *   that is not a DNS-1123 label of at most 30 characters, or is `main`; 404 for
 *   a parent there is not; 409 for a name taken by another branch or being
 *   deleted, or a parent not ready; 406 for an `lsn` its parent no longer has,
 *   as it keeps a day of history, or does not reach; 503 when the storage
 *   controller did not create it, and 502 when it created something else: asked
 *   again, the same timeline is asked for.
 * - `GET /branches`: every branch, main first (200, `{ branches }`).
 * - `DELETE /branches/<name>`: deletes the branch's timeline (204). Its compute
 *   must have stopped first, which is the caller's to see to: neon-control
 *   does not run computes. 400 for main; 404 for a branch there is not; 412 for
 *   one with branches of its own; 409 while the storage controller is still
 *   deleting it, and 503 when it did not, when it is asked again.
 *
 * 503, `{ error }`, too, for a lock of the record another replica held for longer than its
 * patience (./record.ts), when it may be asked again.
 *
 * A branch answered is `{ name, parent, timelineId, lsn, createdAt, state,
 * safekeepers?, computeId }`, its state `creating`, `ready` or `deleting`.
 *
 * Environment: CONTROLLER_URL; NEON_SAFEKEEPER_HOSTS, the safekeepers' hosts in
 * the order of their ids, comma-separated; NEON_PAGESERVER_HOST; and NEON_CONTROLLER_DB_HOST,
 * the storage controller's Postgres, which its record is in. Mounts: /secrets
 * (secrets.json, the signing key), /keys (the public key).
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import {
  type Branch,
  BranchError,
  branchNamed,
  branchOfCompute,
  branchPoint,
  branchRequest,
  type BranchRequest,
  branchView,
  callScopes,
  type ComputeStack,
  computeConfig,
  computeOfToken,
  creationRefused,
  deleting,
  existingBranch,
  HISTORY_DAYS,
  MAIN,
  placementOf,
  type ReadyBranch,
  type Safekeeper,
  safekeepersNotified,
  type TimelineCreated,
  withBranch,
  withNotifiedPlacement,
  withoutBranch,
} from "./branches.ts";
import { bearsScope, publicJwks, signToken } from "./jwt.ts";
import { BranchRecord, type Changing, RecordBusy } from "./record.ts";
import { ROLE, type StackSecrets } from "./secrets.ts";

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

/** A pageserver as the storage controller lists it. */
interface StorageNode {
  id: number;
  availability: unknown;
}

/** A timeline as the pageserver describes it, as far as neon-control reads it. */
interface TimelineInfo {
  last_record_lsn: string;
}

/** A safekeeper's status of a timeline, with its membership configuration. */
interface TimelineStatus {
  mconf: unknown;
}

/** The environment variable `name`, which must be set. */
function setting(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} must be set`);
  return value;
}

const CONTROLLER_URL = setting("CONTROLLER_URL");
const PG_VERSION = 17;
const SAFEKEEPER_HOSTS = setting("NEON_SAFEKEEPER_HOSTS").split(",").map((host) => host.trim()).filter(Boolean);
const SAFEKEEPERS: Safekeeper[] = SAFEKEEPER_HOSTS.map((host, index) => ({ id: index + 1, host, pgPort: 5454, httpPort: 7676 }));
const PAGESERVER_HOST = setting("NEON_PAGESERVER_HOST");
const CONTROLLER_DB_HOST = setting("NEON_CONTROLLER_DB_HOST");

// Written by the setup job (./kube-setup.ts).
const secrets: StackSecrets = JSON.parse(readFileSync("/secrets/secrets.json", "utf8"));
const privateKeyPem = readFileSync("/secrets/auth_private_key.pem", "utf8");
const publicKeyPem = readFileSync("/keys/auth_public_key.pem", "utf8");
// The storage controller's /control and /debug APIs take the admin scope.
const adminToken = signToken(privateKeyPem, "admin");
const safekeeperToken = signToken(privateKeyPem, "safekeeperdata");
/** How often the replica that leads repairs safekeepers. */
const LEAD_INTERVAL_MS = 30_000;
/** How long a call of the storage controller may take: longer than its own waits for the pageserver. */
const CONTROLLER_PATIENCE_MS = 120_000;
/** How long a call of a safekeeper may take. */
const SAFEKEEPER_PATIENCE_MS = 30_000;

/** What every branch's compute spec shares. */
const stack: ComputeStack = {
  tenantId: secrets.tenantId,
  pageserverHost: PAGESERVER_HOST,
  safekeepers: SAFEKEEPERS,
  passwordVerifier: secrets.verifiers[ROLE],
  storageAuthToken: signToken(privateKeyPem, "tenant", secrets.tenantId),
  jwks: publicJwks(publicKeyPem),
};

const record = new BranchRecord(
  `postgresql://storage_controller:${encodeURIComponent(secrets.controllerDbPassword)}@${CONTROLLER_DB_HOST}:5432/storage_controller`,
);

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
    signal: AbortSignal.timeout(CONTROLLER_PATIENCE_MS),
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

/**
 * Creates alasio's tenant once, and gives it its configuration, whole, each time: the
 * storage controller keeps it, and gives it the pageserver with the tenant.
 */
async function ensureTenant(): Promise<void> {
  const found = await controller("GET", `/control/v1/tenant/${secrets.tenantId}`, { allow: [404] });
  if (found.status === 404) {
    await controller("POST", "/v1/tenant", {
      body: { new_tenant_id: secrets.tenantId, placement_policy: { Attached: 0 } },
    });
    log("tenant created", { tenantId: secrets.tenantId });
  }
  await controller("PUT", "/v1/tenant/config", { body: { tenant_id: secrets.tenantId, pitr_interval: `${HISTORY_DAYS}d` } });
}

/** Creates main's timeline once, bootstrapped, recording the safekeepers it was placed on. */
async function ensureMain({ branches, save }: Changing): Promise<void> {
  if (branchNamed(branches, MAIN)?.state === "ready") return;
  const created = await controller<TimelineCreated>("POST", `/v1/tenant/${secrets.tenantId}/timeline`, {
    body: { new_timeline_id: secrets.timelineId, pg_version: PG_VERSION },
  });
  const main = { name: MAIN, parent: null, timelineId: secrets.timelineId, lsn: null, createdAt: new Date().toISOString(), compute: null };
  // A timeline created is answered with its description.
  const safekeepers = placementOf(main, null, created.body!);
  await save(withBranch(branches, { ...main, state: "ready", safekeepers }));
  log("timeline created", { branch: MAIN, timelineId: secrets.timelineId, safekeepers });
}

/** Where `parent`'s timeline ends now, as the pageserver has it: where a branch of it with no LSN asked for starts. */
async function endOf(parent: Branch): Promise<string> {
  const info = await controller<TimelineInfo>("GET", `/v1/tenant/${secrets.tenantId}/timeline/${parent.timelineId}`);
  // The pageserver writes LSNs as branchPoint reads them.
  return branchPoint(info.body!.last_record_lsn)!;
}

/**
 * Creates the branch `request` asks for, or answers the one asked for already. A branch
 * not yet created is recorded first, with its timeline id and branch point, so that
 * asking again asks the storage controller for the same timeline, which it creates once.
 */
async function createBranch(changing: Changing, request: BranchRequest): Promise<{ status: number; branch: Branch }> {
  const existing = existingBranch(changing.branches, request);
  if (existing?.state === "ready") return { status: 200, branch: existing };
  // Checked by existingBranch: a branch's parent is there, and a new one's ready.
  const parent = branchNamed(changing.branches, request.parent)!;
  let branch = existing;
  if (!branch) {
    const lsn = request.lsn ?? (await endOf(parent).catch((error: unknown) => {
      throw new BranchError(503, `the storage controller did not say where ${parent.name} ends: ${errorText(error)}`);
    }));
    branch = {
      name: request.name,
      parent: parent.name,
      timelineId: randomUUID().replaceAll("-", ""),
      lsn,
      createdAt: new Date().toISOString(),
      compute: request.compute,
      state: "creating",
    };
    await changing.save(withBranch(changing.branches, branch));
  }
  const created = await controller<TimelineCreated>("POST", `/v1/tenant/${secrets.tenantId}/timeline`, {
    body: { new_timeline_id: branch.timelineId, ancestor_timeline_id: parent.timelineId, ancestor_start_lsn: branch.lsn },
    allow: [406, 409],
  }).catch((error: unknown) => {
    throw creationRefused(branch, null, errorText(error));
  });
  if (created.status === 406 || created.status === 409) {
    const refusal = creationRefused(branch, created.status, JSON.stringify(created.body));
    // Refused for its branch point, nothing was created, and asking again would be refused again.
    if (refusal.status === 406) await changing.save(withoutBranch(changing.branches, branch.name));
    throw refusal;
  }
  const placed: ReadyBranch = { ...branch, state: "ready", safekeepers: placementOf(branch, parent.timelineId, created.body!) };
  await changing.save(withBranch(changing.branches, placed));
  log("branch created", { branch: placed.name, parent: placed.parent, timelineId: placed.timelineId, lsn: placed.lsn, safekeepers: placed.safekeepers });
  return { status: 201, branch: placed };
}

/** Deletes the branch `name`'s timeline, and then its record. */
async function deleteBranch(changing: Changing, name: string): Promise<void> {
  await changing.save(deleting(changing.branches, name));
  // Checked by deleting: the branch is there.
  const { timelineId } = branchNamed(changing.branches, name)!;
  const deleted = await controller("DELETE", `/v1/tenant/${secrets.tenantId}/timeline/${timelineId}`, { allow: [409] }).catch((error: unknown) => {
    throw new BranchError(503, `the storage controller did not delete ${name}, which may be deleted again: ${errorText(error)}`);
  });
  // The storage controller waits a while for the pageserver, and answers 409 if it is not done by then.
  if (deleted.status === 409) throw new BranchError(409, `the storage controller is still deleting ${name}, which may be deleted again`);
  await changing.save(withoutBranch(changing.branches, name));
  log("branch deleted", { branch: name, timelineId });
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
    signal: AbortSignal.timeout(SAFEKEEPER_PATIENCE_MS),
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
 * A safekeeper that lost its disk comes back without the branch's timeline, and
 * in membership mode does not recreate it: it pulls it from the peers that have
 * it, joining the timeline's current membership. Each pull is made holding the lock of
 * changes, the branch found ready under it, so none pulls back a timeline whose deletion
 * has begun; while another holds the lock, the pull waits for the next round. What only
 * asks the safekeepers takes no lock.
 */
async function repairSafekeepers(branch: ReadyBranch): Promise<void> {
  const placed = SAFEKEEPERS.filter((sk) => branch.safekeepers.ids.includes(sk.id));
  const path = `/v1/tenant/${secrets.tenantId}/timeline/${branch.timelineId}`;
  const states = await Promise.all(
    placed.map(async (sk): Promise<SafekeeperState> => ({ sk, state: await safekeeper<TimelineStatus>(sk, "GET", path).catch(() => null) })),
  );
  const healthy = states.filter((entry): entry is HealthySafekeeper => entry.state?.status === 200);
  for (const { sk, state } of states) {
    if (state?.status !== 404 || healthy.length === 0) continue;
    const held = await record.tryChanging(({ branches }) => {
      if (branchNamed(branches, branch.name)?.state !== "ready") return Promise.resolve(null);
      return safekeeper(sk, "POST", "/v1/pull_timeline", {
        tenant_id: secrets.tenantId,
        timeline_id: branch.timelineId,
        http_hosts: healthy.map(({ sk: peer }) => `http://${peer.host}:${peer.httpPort}`),
        // Not empty: checked above.
        mconf: healthy[0]!.state.body.mconf,
      });
    });
    const pulled = held?.value;
    if (!pulled) {
      log("safekeeper repair put off", { branch: branch.name, safekeeper: sk.id, why: held ? "the branch is no longer ready" : "the lock of changes is held" });
      continue;
    }
    log(pulled.status === 200 ? "safekeeper repaired from its peers" : "safekeeper repair failed", {
      branch: branch.name,
      safekeeper: sk.id,
      status: pulled.status,
      body: pulled.body,
    });
  }
}

/** Repairs the safekeepers of every ready branch of `branches`, each where its timeline is placed. */
async function repairEverySafekeeper(branches: readonly Branch[]): Promise<void> {
  for (const branch of branches) {
    if (branch.state !== "ready") continue;
    await repairSafekeepers(branch).catch((error: unknown) => log("safekeeper check failed", { branch: branch.name, error: errorText(error) }));
  }
}

/** Whether this replica led at its last round, which it says when that changes. */
let led = false;
/** Whether a round of leading is under way: one never starts beside another. */
let leading = false;

/** What the replica that leads does, now and then: each branch's safekeepers repaired. */
async function lead(): Promise<void> {
  if (leading) return;
  leading = true;
  try {
    const leads = await record.leads();
    if (leads !== led) log(leads ? "leading" : "no longer leading");
    led = leads;
    if (!leads) return;
    await repairEverySafekeeper(await record.branches());
  } finally {
    leading = false;
  }
}

/** The branches as last read: what serves the computes their specs while the record cannot be read. */
let lastRead: Branch[] | null = null;
/** Until when the record is not asked again, having failed to be read: each lookup meanwhile answers at once. */
let unreadUntil = 0;
/** How long the record is let be once it could not be read. */
const UNREAD_PAUSE_MS = 30_000;

/**
 * The branches, as recorded now, or, while the record cannot be read, as last read: once a
 * read fails, the record is not asked again for UNREAD_PAUSE_MS, so no lookup meanwhile
 * waits for a connection that fails.
 */
async function knownBranches(): Promise<Branch[]> {
  if (lastRead !== null && Date.now() < unreadUntil) return lastRead;
  try {
    lastRead = await record.branches();
  } catch (error) {
    if (lastRead === null) throw error;
    unreadUntil = Date.now() + UNREAD_PAUSE_MS;
    log("the record was not read; answering from the last read", { error: errorText(error) });
  }
  return lastRead;
}

async function bootstrap(): Promise<void> {
  await until("the storage controller", async () => (await controller("GET", "/status")).status === 200);
  await record.ensureSchema();
  await registerSafekeepers();
  await until("the pageserver to register", async () => {
    // The controller answers with its nodes; a failure here is retried.
    const nodes = (await controller<StorageNode[]>("GET", "/control/v1/node")).body!;
    return nodes.some((node) => node.id === 1 && node.availability === "Active");
  });
  await ensureTenant();
  await record.changing(ensureMain);
  lastRead = await record.branches();
  ready = true;
  log("ready");
  const round = () => lead().catch((error: unknown) => log("leading failed", { error: errorText(error) }));
  void round();
  setInterval(() => void round(), LEAD_INTERVAL_MS).unref();
}

/** Whether the request bears a token of a scope its call is taken with (callScopes). */
const authorized = (request: IncomingMessage): boolean =>
  bearsScope(publicKeyPem, request.headers.authorization, ...callScopes(request.method ?? "", request.url ?? ""));

/** The request's body, parsed as JSON. */
async function jsonBody(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new BranchError(400, "the body is not JSON");
  }
}

function answer(response: ServerResponse, status: number, body?: unknown): void {
  if (body === undefined) {
    response.writeHead(status).end();
    return;
  }
  response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
}

const SPEC = /^\/compute\/api\/v2\/computes\/([^/]+)\/spec$/u;
const BRANCH = /^\/branches\/([^/]+)$/u;

async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const { method, url = "" } = request;
  if (method === "GET" && url === "/healthz") {
    response.writeHead(ready ? 200 : 503).end(ready ? "ready\n" : "bootstrapping\n");
    return;
  }
  const spec = method === "GET" ? SPEC.exec(url) : null;
  if (spec) {
    // Until bootstrapped there is no spec yet, nor perhaps a record; compute_ctl retries,
    // and then its container is restarted.
    if (!ready) return answer(response, 503);
    // A compute's token is its own: main's the stack's, a branch's the one it was created with.
    const id = decodeURIComponent(spec[1] ?? "");
    const branches = await knownBranches();
    const holder = computeOfToken(branches, request.headers.authorization, secrets.computeControlToken);
    if (holder === null) return answer(response, 401);
    if (holder !== id) return answer(response, 403, { error: `the token is not the compute ${id}'s` });
    const branch = branchOfCompute(branches, id);
    if (!branch) return answer(response, 404, { error: `no ready branch has the compute ${id}` });
    return answer(response, 200, { ...computeConfig(branch, stack), status: "attached" });
  }
  const branch = BRANCH.exec(url);
  const hook = method === "PUT" && (url === "/notify-attach" || url === "/notify-safekeepers");
  const api = url === "/branches" ? method === "GET" || method === "POST" : branch !== null && method === "DELETE";
  if (!hook && !api) return answer(response, 404);
  if (!authorized(request)) return answer(response, 401);
  if (!ready) return answer(response, 503, { error: "neon-control is bootstrapping" });
  if (url === "/notify-attach") {
    // Every compute reaches the stack's one pageserver by its Service, whichever of
    // its nodes the tenant is attached to: an attach changes no branch's spec.
    log("compute hook /notify-attach", { body: JSON.stringify(await jsonBody(request)).slice(0, 2000) });
    return answer(response, 200);
  }
  if (url === "/notify-safekeepers") {
    // A timeline's safekeepers migrated, which alasio never starts itself: the branch's
    // spec and repairs follow them from now on, and its compute at its next start.
    const notified = safekeepersNotified(await jsonBody(request));
    await record.changing(async ({ branches, save }) => {
      const next = withNotifiedPlacement(branches, notified);
      if (next) await save(next);
      log("compute hook /notify-safekeepers", { timelineId: notified.timelineId, placement: notified.placement, recorded: next !== null });
    });
    return answer(response, 200);
  }
  if (method === "GET") return answer(response, 200, { branches: (await record.branches()).map(branchView) });
  if (method === "POST") {
    const asked = branchRequest(await jsonBody(request));
    const { status, branch: created } = await record.changing((changing) => createBranch(changing, asked));
    return answer(response, status, branchView(created));
  }
  await record.changing((changing) => deleteBranch(changing, decodeURIComponent(branch?.[1] ?? "")));
  return answer(response, 204);
}

const server = createServer((request, response) => {
  route(request, response).catch((error: unknown) => {
    if (error instanceof RecordBusy) return answer(response, 503, { error: errorText(error) });
    if (!(error instanceof BranchError)) log("request failed", { method: request.method, url: request.url, error: errorText(error) });
    answer(response, error instanceof BranchError ? error.status : 500, { error: errorText(error) });
  });
});
server.listen(8080, "0.0.0.0", () => log("listening", { port: 8080 }));

// As PID 1 in its container, Node would otherwise ignore the SIGTERM that stops it.
process.on("SIGTERM", () => server.close(() => void record.end().finally(() => process.exit(0))));

bootstrap().catch((error: unknown) => {
  log("bootstrap failed", { error: errorText(error) });
  process.exit(1);
});
