/**
 * neon-control: alasio's small stand-in for Neon's control plane, the one part
 * of Neon that is not open source. Runs as a service of the stack, as many replicas as
 * its scale says, which share its record of the branches in the storage controller's
 * Postgres (./record.ts).
 *
 * On start it bootstraps what the storage services need and cannot do
 * themselves, idempotently: registers the safekeepers with the storage
 * controller, creates alasio's tenant once a pageserver registers, and its timeline, the
 * branch `main`, once, and configures the tenant to keep a day of history
 * (HISTORY_DAYS), with a warm secondary location on another pageserver where there are
 * several; it is ready then, whichever pageservers serve. While every pageserver serves,
 * the replica that leads splits the tenant into as many shards as configured, once that
 * is more than it has, and spreads its shards over the pageservers where all are on one:
 * Neon merges no shards, so fewer are refused (by alasio up, which asks the storage
 * controller first), and alasio removes no pageserver. It then manages branches, child
 * timelines of main or of each other (./branches.ts), serves each branch's
 * compute and its read replica their specs, and reports healthy once main's compute can
 * start. Each spec names the pageserver of each of the tenant's shards (ShardLayout), as
 * the storage controller last notified them, as recorded, which any replica may have
 * recorded: on a notification (/notify-attach), after a split, a migration or a
 * pageserver lost, one replica at a time gives every running compute its spec anew
 * (./reconfigure.ts), through compute_ctl's /configure, which reloads it live, and answers
 * 423 while any did not take it, which the storage controller notifies again. It is the
 * control plane of Neon's proxy (./endpoints.ts), which every client connects through: it
 * answers the proxy for roles' secrets, main's from the stack's secrets alone and a
 * branch's from the record, or as last read while the record cannot be, and wakes a
 * compute that sleeps; it serves a compute its spec, likewise, as last read. The replica
 * that leads (./record.ts) repairs every branch's safekeepers, and puts to sleep each
 * compute no client has had a session on for as long as its Deployment says
 * (./computes.ts).
 *
 * A compute fetches its spec as Neon's compute_ctl does from a control plane
 * (`--control-plane-uri`): `GET /compute/api/v2/computes/<id>/spec`, with its
 * compute's own token as its bearer (401 for no compute's, 403 for another's);
 * its id is `alasio` for main's, `branch-<name>` for a branch's. neon-control
 * serves a branch's spec; it does not run its compute.
 *
 * Its API takes a token of the admin scope, as the storage controller's hooks
 * do, or for `GET /branches` alone one of the `branches` scope, the lake's, or for
 * the proxy's paths one of the `proxy` scope, and answers 401 without one, and 503
 * until bootstrapped. It speaks JSON; a refusal is `{ "error": "<why>" }` with its
 * status (the proxy's paths answer with Neon's errors, ./endpoints.ts):
 *
 * - `POST /branches`, `{ name, parent?, lsn?, compute }`: creates the branch
 *   `name` of `parent` (main if not given) at `lsn`, its compute's credentials
 *   `compute` (`{ verifiers, tokenSha256 }`, ./branches.ts), and answers
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
 * - `POST /computes/<id>/wake`: wakes the compute `id` (`alasio`, or `branch-<name>`),
 *   as the proxy's `wake_compute` does, for alasio's command line, which works in it
 *   (200, `{ address }`); 404 for a compute of no ready branch, or not deployed.
 * - `GET /computes`: every ready branch's compute and read replica, its namespace, when it
 *   was last asked to wake and put to sleep, and a replica's lag (200, `{ computes }`),
 *   for `alasio status`.
 * - `GET /tenant`: the tenant's shards, their stripe, and each pageserver, with how many of
 *   them it holds (200, `{ shards, stripeSize, pageservers }`), for `alasio status`.
 *
 * 503, `{ error }`, too, for a lock of the record another replica held for longer than its
 * patience (./record.ts), when it may be asked again. `GET /metrics` takes no token: its
 * metrics (./metrics.ts), for the stack's collector.
 *
 * A branch answered is `{ name, parent, timelineId, lsn, createdAt, state,
 * safekeepers?, computeId }`, its state `creating`, `ready` or `deleting`.
 *
 * Environment: CONTROLLER_URL; NEON_SAFEKEEPER_HOSTS, the safekeepers' hosts in
 * the order of their ids, comma-separated; NEON_PAGESERVERS, how many pageservers run;
 * NEON_SHARDS, how many shards the tenant is split into; NEON_CONTROLLER_DB_HOST,
 * the storage controller's Postgres, which its record is in; NAMESPACE, its own, main's
 * compute's; and NEON_COMPUTE_NAME, the name of every compute's Deployment and Service,
 * in main's namespace or a branch environment's. Mounts: /secrets (secrets.json, the
 * signing key), /keys (the public key).
 */
import { createHmac, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

import { branchNamespace } from "../../src/branch/names.ts";

import {
  attachNotified,
  type Branch,
  BranchError,
  branchNamed,
  branchPoint,
  branchRequest,
  type BranchRequest,
  branchView,
  callScopes,
  COMPUTE_PORT,
  type ComputeCredentials,
  type ComputeStack,
  computeConfig,
  computeId,
  computeNamed,
  computeOfToken,
  creationRefused,
  deleting,
  type DescribedShard,
  existingBranch,
  HISTORY_DAYS,
  layoutOf,
  MAIN,
  pageserversServing,
  placementOf,
  PROXY_PATHS,
  type ReadyBranch,
  REPLICA_SUFFIX,
  replicaId,
  type Safekeeper,
  safekeepersNotified,
  sameLayout,
  type ShardLayout,
  shardMoves,
  type StorageNode,
  type TimelineCreated,
  withBranch,
  withNotifiedPlacement,
  withoutBranch,
} from "./branches.ts";
import {
  type ComputeDeployment,
  computeActivity,
  computeDeployment,
  computeLsn,
  type ComputePod,
  computePods,
  computePodsLeft,
  type ComputeRef,
  computeCtlStatus,
  computeRuns,
  configureCompute,
  reaches,
  restartCompute,
  routedPods,
  scaleCompute,
  setServing,
} from "./computes.ts";
import {
  type Activity,
  computeAddress,
  controlPlaneError,
  type Idleness,
  idleness,
  lsnDistance,
  proxyQuery,
  roleSecret,
  suspendCompute,
  suspendDue,
  type Use,
  wakeCompute,
  withdrawCompute,
  woken,
} from "./endpoints.ts";
import { bearsScope, computeCtlToken, publicJwks, signToken } from "./jwt.ts";
import { createControlMetrics } from "./metrics.ts";
import { reconcile, type Reconfigured, reconfigure, settledLayout, tookLayout } from "./reconfigure.ts";
import { BranchRecord, type Changing, RecordBusy } from "./record.ts";
import { scramVerifier } from "./scram.ts";
import { MONITOR_ROLE, type StackSecrets } from "./secrets.ts";

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

/** Where the tenant's shards are, as the storage controller's locate answers. */
interface Located {
  shards: { shard_id: string; node_id: number; listen_pg_addr: string; listen_pg_port: number }[];
  shard_params: { count: number; stripe_size: number };
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
const PAGESERVERS = Number(setting("NEON_PAGESERVERS"));
const SHARDS = Number(setting("NEON_SHARDS"));
const CONTROLLER_DB_HOST = setting("NEON_CONTROLLER_DB_HOST");
const NAMESPACE = setting("NAMESPACE");
const COMPUTE_NAME = setting("NEON_COMPUTE_NAME");

// Written by the setup job (./kube-setup.ts).
const secrets: StackSecrets = JSON.parse(readFileSync("/secrets/secrets.json", "utf8"));
const privateKeyPem = readFileSync("/secrets/auth_private_key.pem", "utf8");
const publicKeyPem = readFileSync("/keys/auth_public_key.pem", "utf8");
// The storage controller's /control and /debug APIs take the admin scope.
const adminToken = signToken(privateKeyPem, "admin");
const safekeeperToken = signToken(privateKeyPem, "safekeeperdata");
/** How often the replica that leads repairs safekeepers, and counts each running compute's sessions. */
const LEAD_INTERVAL_MS = 30_000;
/** How long a wake may take in all, its compute's lock waited for, and its pod for, beyond any client's patience. */
const WAKE_PATIENCE_MS = 180_000;
/** How long a compute put to sleep is waited for to be gone. */
const SUSPEND_PATIENCE_MS = 120_000;
/** How long a compute's Service is waited for to reach none of the pods a sleep took off it. */
const WITHDRAW_PATIENCE_MS = 15_000;
/** How often a wake or a sleep looks again at what it waits for. */
const POLL_MS = 250;
/** How long a call of the storage controller may take: longer than its own waits for the pageserver. */
const CONTROLLER_PATIENCE_MS = 120_000;
/**
 * How long a notification of the shards' layout is answered within: before the storage
 * controller stops waiting for it, at 10 s (storage_controller/src/compute_hook.rs's
 * NOTIFY_REQUEST_TIMEOUT at release-9129), with a 423 if the computes are not given it yet.
 */
const NOTIFY_PATIENCE_MS = 8_000;
/** How long a call of a safekeeper may take. */
const SAFEKEEPER_PATIENCE_MS = 30_000;

/** The password of neon-control's own role on the compute `id`: of that compute's alone, drawn from the stack's monitor key, which no client holds. */
const monitorPassword = (id: string) => createHmac("sha256", secrets.monitorKey).update(`${MONITOR_ROLE}:${id}`).digest("base64url");

/** What every branch's compute spec shares. */
const stack: ComputeStack = {
  tenantId: secrets.tenantId,
  safekeepers: SAFEKEEPERS,
  // Main's clients' roles': the verifiers alasio sets them to (src/neon/), and main's spec alasio's.
  verifiers: secrets.verifiers,
  monitor: { role: MONITOR_ROLE, verifier: (id) => scramVerifier(monitorPassword(id)) },
  storageAuthToken: signToken(privateKeyPem, "tenant", secrets.tenantId),
  jwks: publicJwks(publicKeyPem),
};

const metrics = createControlMetrics();

const record = new BranchRecord(
  `postgresql://storage_controller:${encodeURIComponent(secrets.controllerDbPassword)}@${CONTROLLER_DB_HOST}:5432/storage_controller`,
  { busy: (lock) => metrics.lockTimedOut(lock) },
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
 * Creates alasio's tenant once, on a pageserver once one registers, and gives it its
 * configuration, whole, each time: the storage controller keeps it, and gives it the
 * pageservers with the tenant.
 */
async function ensureTenant(): Promise<void> {
  const found = await controller("GET", `/control/v1/tenant/${secrets.tenantId}`, { allow: [404] });
  if (found.status === 404) {
    await until("a pageserver to register", async () => {
      // The controller answers with its nodes; a failure here is retried.
      const nodes = (await controller<StorageNode[]>("GET", "/control/v1/node")).body!;
      return nodes.some((node) => node.availability === "Active");
    });
    await controller("POST", "/v1/tenant", {
      body: { new_tenant_id: secrets.tenantId, placement_policy: { Attached: 0 } },
    });
    log("tenant created", { tenantId: secrets.tenantId });
  }
  await controller("PUT", "/v1/tenant/config", { body: { tenant_id: secrets.tenantId, pitr_interval: `${HISTORY_DAYS}d` } });
  // A warm secondary location of each shard on another pageserver, where there is one, so
  // losing a pageserver fails its shards over without downloading their layers anew.
  await controller("PUT", `/control/v1/tenant/${secrets.tenantId}/policy`, { body: { placement: { Attached: PAGESERVERS > 1 ? 1 : 0 } } });
}

/** The tenant's shard count: an unsharded tenant's, 0 to the storage controller, is 1. */
const shardCount = ({ shard_params }: Located): number => Math.max(1, shard_params.count);

/**
 * Splits the tenant into SHARDS shards, where it has fewer, and waits for the split; one
 * with more stays as it is, as Neon merges no shards (alasio up refuses fewer, saying so).
 * A split the storage controller cannot make now (409, 503) is asked for again at the
 * next round.
 */
async function ensureShards(): Promise<void> {
  const count = shardCount(await located());
  if (count > SHARDS) {
    log("the tenant keeps more shards than configured, as Neon merges none", { shards: count, configured: SHARDS });
    return;
  }
  if (count === SHARDS) return;
  const split = await controller("PUT", `/control/v1/tenant/${secrets.tenantId}/shard_split`, { body: { new_shard_count: SHARDS }, allow: [409, 503] });
  log(split.status === 200 ? "tenant split" : "tenant not split yet", { from: count, to: SHARDS, status: split.status });
}

/**
 * Spreads the tenant's shards over the pageservers where they are all on one (shardMoves).
 * The storage controller leaves a split's shards where the tenant was, and its optimizer
 * is not relied on to move them: with every pageserver in one zone (PAGESERVER_ZONE) it
 * moved neither of two shards in ten minutes, twice in CI. Each move is graceful, cut over
 * once the shard's secondary there is warm; one the storage controller refuses, to a
 * pageserver it scores worse (412), or cannot make now (409, 503), is left as it is, so
 * neon-control never works against it.
 */
async function spreadShards(): Promise<void> {
  const described = (await controller<{ shards: DescribedShard[] }>("GET", `/control/v1/tenant/${secrets.tenantId}`)).body!;
  for (const move of shardMoves(described.shards, PAGESERVERS)) {
    const moved = await controller("PUT", `/control/v1/tenant/${move.shardId}/migrate`, { body: { node_id: move.to, origin_node_id: move.from }, allow: [409, 412, 503] });
    log(moved.status === 200 ? "shard moving" : "shard not moved", { ...move, status: moved.status });
  }
}

/**
 * The tenant split into as many shards as configured, and its shards spread over the
 * pageservers, by the replica that leads, once every pageserver serves (pageserversServing):
 * a split or a move needs each, and no other replica splits or moves beside it.
 */
async function balanceShards(): Promise<void> {
  const nodes = (await controller<StorageNode[]>("GET", "/control/v1/node")).body ?? [];
  if (!pageserversServing(nodes, PAGESERVERS)) return;
  await ensureShards();
  await spreadShards();
}

/**
 * The layout recorded, and every running compute's, set right by the replica that leads,
 * as ./reconfigure.ts's reconcile decides. Under the layout's lock, as a notification is,
 * from reading where the shards are on: a notification meanwhile is answered 423 and sent
 * again, so neither records a layout the other read before it.
 */
async function reconcileLayout(): Promise<void> {
  const done = await record.tryLayoutLocked(() => reconcile({
    shards: async () => (await controller<{ shards: DescribedShard[] }>("GET", `/control/v1/tenant/${secrets.tenantId}`)).body!.shards,
    located: async () => layoutLocated(await located()),
    recorded: () => record.layout(secrets.tenantId),
    record: learnLayout,
    configure: async (layout) => configureRunning(await record.branches(), layout),
  }));
  const reconciled = done?.value;
  if (!reconciled) return;
  const { layout, recorded, pending } = reconciled;
  if (recorded) log("layout recorded as the storage controller locates the shards", { pageservers: layout.pageservers, stripeSize: layout.stripeSize });
  if (pending > 0) log("computes not given the layout yet", { pending });
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

/** A compute of a ready branch's: its primary, or its read replica. */
interface Compute {
  name: string;
  replica: boolean;
}

/** The compute's id: the primary's computeId, a replica's replicaId. */
const idOf = ({ name, replica }: Compute): string => (replica ? replicaId(name) : computeId(name));

/** The name of every compute's Deployment and Service, a replica's. */
const REPLICA_NAME = `${COMPUTE_NAME}${REPLICA_SUFFIX}`;

/** The compute's Deployment: main's in neon-control's namespace, any other's in its branch environment's. */
const computeOf = ({ name, replica }: Compute): ComputeRef => ({
  namespace: name === MAIN ? NAMESPACE : branchNamespace(name),
  name: replica ? REPLICA_NAME : COMPUTE_NAME,
});

/** Where the compute's Service is, from any namespace. */
const hostOf = (compute: Compute): string => {
  const { namespace, name } = computeOf(compute);
  return `${name}.${namespace}.svc.cluster.local`;
};

/**
 * What the clients of the compute are doing, counted as neon-control's own role there
 * (./computes.ts), with its primary's password, which a replica has of its primary, at
 * `host`, its Service unless given; fails while it cannot log in.
 */
function activityOn(compute: Compute, host = hostOf(compute)): Promise<Activity> {
  return computeActivity(host, COMPUTE_PORT, MONITOR_ROLE, monitorPassword(computeId(compute.name)));
}

/** Waits until the compute has no pod, one stopping included, by `deadline`. */
async function untilGone(ref: ComputeRef, deployment: ComputeDeployment, deadline: number): Promise<void> {
  while ((await computePods(ref, deployment)) > 0) {
    if (Date.now() > deadline) throw new Error(`${ref.namespace}'s ${ref.name} did not stop in time`);
    await sleep(POLL_MS);
  }
}

/**
 * Takes the compute's pods off its Service (./endpoints.ts's withdrawCompute), and waits
 * until its EndpointSlices list none of them, and a connection to its address, as Neon's
 * proxy makes one, is refused, by WITHDRAW_PATIENCE_MS: those pods, each at its own
 * address, which neon-control counts it on.
 */
function withdraw(compute: Compute, deployment: ComputeDeployment): Promise<readonly ComputePod[]> {
  const ref = computeOf(compute);
  const deadline = Date.now() + WITHDRAW_PATIENCE_MS;
  return withdrawCompute({
    pods: () => computePodsLeft(ref, deployment),
    takeOff: async (pod) => {
      if (pod.serving) await setServing(ref, pod.name, false);
    },
    listed: async (pods) => {
      const routed = await routedPods(ref);
      return pods.some(({ name }) => routed.has(name));
    },
    reached: () => reaches(hostOf(compute), COMPUTE_PORT),
    pause: async (still) => {
      if (Date.now() > deadline) throw new Error(`${ref.namespace}'s ${ref.name}: ${still}`);
      await sleep(POLL_MS);
    },
  });
}

/** Gives the compute's pods left back to its Service: those a sleep took off it. */
async function giveBack(ref: ComputeRef): Promise<void> {
  const deployment = await computeDeployment(ref);
  if (!deployment) return;
  for (const pod of await computePodsLeft(ref, deployment)) if (!pod.serving) await setServing(ref, pod.name, true);
}

/** Ends the compute's stop: its pods given back to its Service, and it recorded stopping no longer. */
async function resume(compute: Compute): Promise<void> {
  await giveBack(computeOf(compute));
  await record.endedStopping(idOf(compute));
}

/** The wakes under way, by compute: concurrent wakes of one compute share one. */
const waking = new Map<string, Promise<boolean>>();

/**
 * Wakes the compute (wakeCompute), and records it asked to wake, which keeps it awake a
 * while longer (suspendIdle), where the record can be written, as a compute that runs
 * needs nothing of it to be reached. One that sleeps, or that a sleep is stopping, it
 * scales to one under the compute's lock, waited for as long as the wake may take, longer
 * than a sleep holds it, so it never finds the pod a sleep is stopping, once the pod put to
 * sleep is gone, so no two serve the timeline at once. Then it waits until neon-control
 * logs in to it, so its clients may (./computes.ts), for as long as WAKE_PATIENCE_MS in
 * all. Whether it slept. A wake asked again, by the proxy after a restart of neon-control,
 * finds it scaling, and waits again.
 */
function wake(compute: Compute): Promise<boolean> {
  const id = idOf(compute);
  const running = waking.get(id);
  if (running) return running;
  const started = Date.now();
  const deadline = started + WAKE_PATIENCE_MS;
  const ref = computeOf(compute);
  const deployment = async (): Promise<ComputeDeployment> => {
    const found = await computeDeployment(ref);
    if (!found) throw new BranchError(404, `${id} is not deployed`);
    return found;
  };
  const woke = wakeCompute({
    deployment,
    asked: () => record.woke(id).catch((error: unknown) => log("wake not recorded", { compute: id, error: errorText(error) })),
    stopping: () => record.stopping().then((stopping) => stopping.has(id), () => false),
    locked: (scaling) => record.computeLocked(id, scaling, deadline - Date.now()),
    untilGone: (now) => untilGone(ref, now, deadline),
    resume: async () => {
      await resume(compute);
      log("compute's stop ended by a wake, its sleep never finished", { compute: id });
    },
    scaleUp: () => scaleCompute(ref, 1),
    accepts: () =>
      activityOn(compute).then(() => true, (error: unknown) => {
        if (Date.now() > deadline) throw new Error(`${id} did not take connections in ${WAKE_PATIENCE_MS} ms: ${errorText(error)}`);
        return false;
      }),
    pause: () => sleep(POLL_MS),
  }).then((slept) => {
    const seconds = (Date.now() - started) / 1000;
    metrics.woke(slept ? "cold" : "warm", seconds);
    if (slept) log("compute woken", { compute: id, ms: Math.round(seconds * 1000) });
    return slept;
  }, (error: unknown) => {
    // A compute not deployed is no failure of the wake's, nor a lock given up, which the record counts.
    if (!(error instanceof BranchError) && !(error instanceof RecordBusy)) metrics.wakeFailed();
    throw error;
  }).finally(() => waking.delete(id));
  waking.set(id, woke);
  return woke;
}

/** Each running compute's idleness, as the replica that leads last counted it. */
const idle = new Map<string, Idleness>();

/** What the compute's clients have done, counted at `host`, its Service unless given, and when it was last asked to wake. */
async function useOf(compute: Compute, host?: string): Promise<Use> {
  const id = idOf(compute);
  const [activity, computes] = await Promise.all([activityOn(compute, host), record.computes()]);
  return { ...activity, wokenAt: computes.find((compute) => compute.id === id)?.wokenAt ?? null };
}

/**
 * Puts to sleep each running compute that has had no client session, nor been asked to
 * wake, for as long as its Deployment says, as ./endpoints.ts's suspendCompute does: under
 * the compute's lock, marked stopping and taken off its Service, until a connection to it is
 * refused, then counted again on its pod, so a session opened since is not cut, nor a wake
 * asked for since undone, and waiting until its pod is gone, so a wake after finds it gone.
 * One whose lock a wake holds is left until the next round. A stop no sleep is under way
 * in, its neon-control stopped in it, is ended first. Says how many replicas each compute
 * runs.
 */
async function suspendIdle(computes: readonly Compute[]): Promise<void> {
  const replicas = new Map<string, number>();
  // A compute deleted since is counted no more.
  const ids = new Set(computes.map(idOf));
  for (const id of idle.keys()) if (!ids.has(id)) idle.delete(id);
  // None read where the record cannot be, as no compute's lock can be taken then either.
  const stopping = await record.stopping().catch(() => new Set<string>());
  for (const compute of computes) {
    const id = idOf(compute);
    try {
      if (stopping.has(id)) {
        const ended = await record.tryComputeLocked(id, () => resume(compute));
        if (ended) log("compute's stop ended by the leader's round, its sleep never finished", { compute: id });
      }
      const ref = computeOf(compute);
      const deployment = await computeDeployment(ref);
      if (deployment) replicas.set(id, deployment.replicas);
      if (!deployment || deployment.suspendAfterMs === null || deployment.replicas === 0 || deployment.ready === 0) {
        idle.delete(id);
        continue;
      }
      const counted = idleness(idle.get(id), await useOf(compute), Date.now());
      idle.set(id, counted);
      if (!suspendDue(counted, Date.now(), deployment.suspendAfterMs)) continue;
      const suspendAfterMs = deployment.suspendAfterMs;
      const slept = await record.tryComputeLocked(id, async () => {
        const asleep = await suspendCompute({
          mark: () => record.beganStopping(id),
          withdraw: () => withdraw(compute, deployment),
          used: async (pods) => {
            // One, as it runs one replica, its pod made anew only once the last is gone.
            const [pod, ...others] = pods;
            if (!pod?.ip || others.length > 0) throw new Error(`${ref.namespace}'s ${ref.name} has ${pods.length} pods left, not one with an address, to count it on`);
            const recounted = idleness(counted, await useOf(compute, pod.ip), Date.now());
            idle.set(id, recounted);
            return recounted.since === null;
          },
          restore: () => resume(compute),
          scaleDown: async () => {
            await scaleCompute(ref, 0);
            idle.delete(id);
          },
          untilGone: () => untilGone(ref, deployment, Date.now() + SUSPEND_PATIENCE_MS),
          unmark: () => record.endedStopping(id),
        });
        if (asleep) await record.suspended(id);
        return asleep;
      });
      if (!slept?.value) continue;
      replicas.set(id, 0);
      metrics.suspended();
      log("compute suspended", { compute: id, idleMs: Date.now() - (counted.since ?? Date.now()), suspendAfterMs });
    } catch (error) {
      log("compute not checked for idleness", { compute: id, error: errorText(error) });
    }
  }
  metrics.replicas(replicas);
}

/** Every ready branch's computes: its primary, and its read replica, where it runs one. */
const computesOf = (branches: readonly Branch[]): Compute[] =>
  branches.filter(({ state }) => state === "ready").flatMap(({ name }) => [{ name, replica: false }, { name, replica: true }]);

/**
 * How far each read replica trails its primary, by the replica's id, in bytes of WAL, where
 * both run: the primary's flushed WAL's end against what the replica has replayed.
 */
async function replicaLag(branches: readonly Branch[]): Promise<Map<string, number>> {
  // Every replica's at once, as each may wait on a compute that does not answer.
  const read = await Promise.all(computesOf(branches).filter(({ replica }) => replica).map(async (compute): Promise<[string, number] | null> => {
    const primary = { name: compute.name, replica: false };
    try {
      const [replicaDeployment, primaryDeployment] = await Promise.all([computeDeployment(computeOf(compute)), computeDeployment(computeOf(primary))]);
      if (!replicaDeployment?.ready || !primaryDeployment?.ready) return null;
      const password = monitorPassword(computeId(compute.name));
      // The replica's first, so a lag is never under what it is.
      const replayed = await computeLsn(hostOf(compute), COMPUTE_PORT, MONITOR_ROLE, password, true);
      const flushed = await computeLsn(hostOf(primary), COMPUTE_PORT, MONITOR_ROLE, password, false);
      return [idOf(compute), lsnDistance(replayed, flushed)];
    } catch (error) {
      log("replica lag not read", { compute: idOf(compute), error: errorText(error) });
      return null;
    }
  }));
  return new Map(read.filter((entry): entry is [string, number] => entry !== null));
}

/** Whether this replica led at its last round, which it says when that changes. */
let led = false;
/** Whether a round of leading is under way: one never starts beside another. */
let leading = false;

/**
 * What the replica that leads does, now and then: each branch's safekeepers repaired,
 * each compute idle long enough put to sleep, each read replica's lag read, the tenant's
 * shards split and spread as configured (balanceShards), and the computes' layout set
 * right (reconcileLayout).
 */
async function lead(): Promise<void> {
  if (leading) return;
  leading = true;
  try {
    const leads = await record.leads();
    if (leads !== led) log(leads ? "leading" : "no longer leading");
    led = leads;
    if (!leads) {
      metrics.replicas(new Map());
      metrics.replicaLag(new Map());
      return;
    }
    const branches = await record.branches();
    await repairEverySafekeeper(branches);
    await suspendIdle(computesOf(branches));
    metrics.replicaLag(await replicaLag(branches));
    await balanceShards();
    await reconcileLayout();
  } finally {
    leading = false;
  }
}

/** The branches as last read: what answers the proxy and the computes while the record cannot be read. */
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

/** A pageserver's address, as computes reach it from any namespace: its Service's name, qualified. */
const pageserverAddress = (host: string, port: number): string => `${host.includes(".") ? host : `${host}.${NAMESPACE}.svc.cluster.local`}:${port}`;

/** Where the tenant's shards are, as the storage controller locates them now. */
async function located(): Promise<Located> {
  return (await controller<Located>("GET", `/debug/v1/tenant/${secrets.tenantId}/locate`)).body!;
}

/** The layout of the shards as `answered` locates them. */
function layoutLocated(answered: Located): ShardLayout {
  const pageservers = answered.shards.map(({ listen_pg_addr, listen_pg_port }) => pageserverAddress(listen_pg_addr, listen_pg_port));
  return { pageservers, stripeSize: pageservers.length > 1 ? answered.shard_params.stripe_size : null };
}

/** Where the tenant's shards are, as last read from the record, or recorded. */
let lastLayout: ShardLayout | null = null;

/**
 * Where the tenant's shards are, as recorded now, which any replica may have recorded last
 * (/notify-attach); while the record cannot be read, as last read, as knownBranches answers.
 */
async function currentLayout(): Promise<ShardLayout> {
  if (lastLayout !== null && Date.now() < unreadUntil) return lastLayout;
  try {
    const recorded = await record.layout(secrets.tenantId);
    if (recorded === null) throw new Error("the record has no layout of the tenant's shards");
    lastLayout = recorded;
  } catch (error) {
    if (lastLayout === null) throw error;
    unreadUntil = Date.now() + UNREAD_PAUSE_MS;
    log("the record was not read; answering from the last read", { error: errorText(error) });
  }
  return lastLayout;
}

/** Records where the tenant's shards are now. */
async function learnLayout(next: ShardLayout): Promise<void> {
  await record.saveLayout(secrets.tenantId, next);
  lastLayout = next;
}

async function bootstrap(): Promise<void> {
  await until("the storage controller", async () => (await controller("GET", "/status")).status === 200);
  await record.ensureSchema();
  await registerSafekeepers();
  await ensureTenant();
  // Where the shards are, recorded once: from then on as notified, under the layout's lock.
  await record.seedLayout(secrets.tenantId, layoutLocated(await located()));
  await record.changing(ensureMain);
  lastRead = await record.branches();
  ready = true;
  log("ready");
  const round = () => lead().catch((error: unknown) => log("leading failed", { error: errorText(error) }));
  void round();
  setInterval(() => void round(), LEAD_INTERVAL_MS).unref();
}

/**
 * Gives every compute last given another layout than `layout` its spec anew, with it, all
 * at once, each as ./reconfigure.ts decides: how many have not taken it yet.
 */
async function configureRunning(branches: readonly Branch[], layout: ShardLayout): Promise<number> {
  const given = await record.givenLayouts();
  const stale = computesOf(branches).filter((compute) => {
    const had = given.get(idOf(compute));
    return !(had && sameLayout(had, layout));
  });
  // Ready, as computesOf gave only those.
  const outcomes = await Promise.all(stale.map((compute) => giveSpec(compute, branchNamed(branches, compute.name) as ReadyBranch, layout)));
  return outcomes.filter((reconfigured) => !tookLayout(reconfigured)).length;
}

/** Gives `compute`, of `branch`, its spec anew, with `layout`, as ./reconfigure.ts decides, saying where it did not take it. */
async function giveSpec(compute: Compute, branch: ReadyBranch, layout: ShardLayout): Promise<Reconfigured> {
  const id = idOf(compute);
  const ref = computeOf(compute);
  const token = computeCtlToken(privateKeyPem, id);
  const reconfigured = await reconfigure(layout, {
    awake: async () => (await computeDeployment(ref))?.replicas === 1,
    running: async () => {
      const deployment = await computeDeployment(ref);
      return deployment !== null && await computeRuns(ref, deployment);
    },
    configure: (to) => configureCompute(hostOf(compute), token, computeConfig(branch, stack, to, { replica: compute.replica })),
    status: () => computeCtlStatus(hostOf(compute), token),
    restart: () => restartCompute(ref),
    gave: (to) => record.gaveLayout(id, to),
  });
  if (!tookLayout(reconfigured)) log("compute not given its spec anew yet", { compute: id, ...reconfigured });
  return reconfigured;
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

/**
 * The compute the proxy names by `endpoint`: main's from the stack's secrets alone, so its
 * clients reach it while the record cannot be read, its read replica's likewise; a
 * branch's from the record, or as last read. Its credentials are its primary's.
 */
async function proxiedCompute(endpoint: string): Promise<{ compute: Compute; credentials: ComputeCredentials | null } | undefined> {
  for (const replica of [false, true]) {
    if (endpoint === idOf({ name: MAIN, replica })) return { compute: { name: MAIN, replica }, credentials: null };
  }
  const named = computeNamed(await knownBranches(), endpoint);
  return named && { compute: { name: named.branch.name, replica: named.replica }, credentials: named.branch.compute };
}

/**
 * Answers Neon's proxy (./endpoints.ts) at `path`: a role's secret, or a compute woken.
 * Every refusal is Neon's error; a wake that fails is one the proxy asks again.
 */
async function answerProxy(request: IncomingMessage, response: ServerResponse, path: string): Promise<void> {
  const query = proxyQuery(request.url ?? "");
  const named = query ? await proxiedCompute(query.endpoint) : undefined;
  if (!query || !named) return answer(response, 404, controlPlaneError("ENDPOINT_NOT_FOUND", `no endpoint ${query?.endpoint ?? ""}`));
  if (path === "/get_endpoint_access_control") {
    const secret = query.role === null ? null : roleSecret(named.credentials, query.role, stack.verifiers);
    if (secret === null) return answer(response, 404, controlPlaneError("RESOURCE_NOT_FOUND", `no role ${query.role ?? ""} on ${query.endpoint}`));
    return answer(response, 200, { role_secret: secret, project_id: "alasio", allowed_ips: null });
  }
  try {
    const slept = await wake(named.compute);
    const { name, namespace } = computeOf(named.compute);
    return answer(response, 200, woken(computeAddress(name, namespace), named.compute.name, query.endpoint, slept));
  } catch (error) {
    if (error instanceof BranchError && error.status === 404) return answer(response, 404, controlPlaneError("ENDPOINT_NOT_FOUND", error.message));
    log("wake failed", { compute: query.endpoint, error: errorText(error) });
    return answer(response, 503, controlPlaneError("RUNNING_OPERATIONS", `${query.endpoint} did not wake: ${errorText(error)}`, 1000));
  }
}

/**
 * Every ready branch's compute and read replica, where it runs, when it was last asked to
 * wake and put to sleep (./record.ts), and a replica's lag, read now, as any replica of
 * neon-control answers, for alasio's command line.
 */
async function computesView(): Promise<object> {
  const [branches, times] = await Promise.all([record.branches(), record.computes()]);
  const lag = await replicaLag(branches);
  const at = (ms: number | null | undefined) => (ms === null || ms === undefined ? null : new Date(ms).toISOString());
  // A read replica where its environment runs one, as the lake runs there.
  const computes = computesOf(branches);
  const run = await Promise.all(computes.map(async (compute) => !compute.replica || (await computeDeployment(computeOf(compute))) !== null));
  return {
    computes: computes.filter((_, index) => run[index]).map((compute) => {
      const id = idOf(compute);
      const recorded = times.find((each) => each.id === id);
      return {
        id,
        namespace: computeOf(compute).namespace,
        name: computeOf(compute).name,
        wokenAt: at(recorded?.wokenAt),
        suspendedAt: at(recorded?.suspendedAt),
        ...(compute.replica ? { lagBytes: lag.get(id) ?? null } : {}),
      };
    }),
  };
}

/** The tenant's shards and pageservers, as the storage controller has them now. */
async function tenantView(): Promise<object> {
  const [answered, nodes] = await Promise.all([located(), controller<StorageNode[]>("GET", "/control/v1/node")]);
  return {
    shards: shardCount(answered),
    stripeSize: answered.shard_params.stripe_size,
    pageservers: (nodes.body ?? []).map(({ id, availability, listen_pg_addr, listen_pg_port }) => ({
      id,
      address: pageserverAddress(listen_pg_addr, listen_pg_port),
      availability,
      shards: answered.shards.filter(({ node_id }) => node_id === id).length,
    })),
  };
}

const SPEC = /^\/compute\/api\/v2\/computes\/([^/]+)\/spec$/u;
const WAKE = /^\/computes\/([^/]+)\/wake$/u;
const BRANCH = /^\/branches\/([^/]+)$/u;

async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const { method, url = "" } = request;
  if (method === "GET" && url === "/healthz") {
    response.writeHead(ready ? 200 : 503).end(ready ? "ready\n" : "bootstrapping\n");
    return;
  }
  if (method === "GET" && url === "/metrics") {
    response.writeHead(200, { "content-type": "text/plain; version=0.0.4" }).end(metrics.render());
    return;
  }
  const spec = method === "GET" ? SPEC.exec(url) : null;
  if (spec) {
    // Until bootstrapped there is no spec yet, nor perhaps a record; compute_ctl retries,
    // and then its container is restarted.
    if (!ready) return answer(response, 503);
    // A compute's token is its own: main's the stack's, a branch's the one it was created
    // with, which its read replica holds too.
    const id = decodeURIComponent(spec[1] ?? "");
    const branches = await knownBranches();
    const holder = computeOfToken(branches, request.headers.authorization, secrets.computeControlToken);
    if (holder === null) return answer(response, 401);
    const named = computeNamed(branches, id);
    if (holder !== (named ? computeId(named.branch.name) : id)) return answer(response, 403, { error: `the token is not the compute ${id}'s` });
    if (!named) return answer(response, 404, { error: `no ready branch has the compute ${id}` });
    // Where its spec says the shards are, recorded for a new layout's (configureRunning) before
    // it is answered, and read again after, so one notified meanwhile is in this spec or in
    // the record of it; if the record can be written.
    const layout = await settledLayout(currentLayout, (given) => record.gaveLayout(id, given)).catch(async (error: unknown) => {
      log("layout given not recorded", { compute: id, error: errorText(error) });
      return currentLayout();
    });
    return answer(response, 200, { ...computeConfig(named.branch, stack, layout, { replica: named.replica }), status: "attached" });
  }
  const path = new URL(url, "http://neon-control").pathname;
  const proxied = method === "GET" && PROXY_PATHS.includes(path);
  const wakeAsked = method === "POST" ? WAKE.exec(url) : null;
  const branch = BRANCH.exec(url);
  const hook = method === "PUT" && (url === "/notify-attach" || url === "/notify-safekeepers");
  const api = url === "/branches"
    ? method === "GET" || method === "POST"
    : url === "/computes" || url === "/tenant"
    ? method === "GET"
    : branch !== null && method === "DELETE";
  if (!proxied && !wakeAsked && !hook && !api) return answer(response, 404);
  if (!authorized(request)) return answer(response, 401);
  if (!ready) {
    // The proxy reads Neon's errors alone, and asks again after the delay given.
    return proxied
      ? answer(response, 503, controlPlaneError("RUNNING_OPERATIONS", "neon-control is bootstrapping", 1000))
      : answer(response, 503, { error: "neon-control is bootstrapping" });
  }
  if (proxied) return answerProxy(request, response, path);
  if (wakeAsked) {
    const id = decodeURIComponent(wakeAsked[1] ?? "");
    const named = computeNamed(await record.branches(), id);
    if (!named) return answer(response, 404, { error: `no ready branch has the compute ${id}` });
    const compute = { name: named.branch.name, replica: named.replica };
    await wake(compute);
    const { name, namespace } = computeOf(compute);
    return answer(response, 200, { address: computeAddress(name, namespace) });
  }
  if (url === "/computes") return answer(response, 200, await computesView());
  if (url === "/tenant") return answer(response, 200, await tenantView());
  if (url === "/notify-attach") {
    // The shards placed anew, after a split, a migration or a pageserver lost: every spec
    // names their pageservers from now on, and every running compute is given its own, by
    // one replica at a time, so two notifications never restart the same compute twice.
    const notified = attachNotified(await jsonBody(request), secrets.tenantId);
    const nodes = (await controller<StorageNode[]>("GET", "/control/v1/node")).body ?? [];
    const next = layoutOf(notified, new Map(nodes.map(({ id, listen_pg_addr, listen_pg_port }) => [id, pageserverAddress(listen_pg_addr, listen_pg_port)])));
    if (!next) return answer(response, 423, { error: "a shard is on a pageserver not yet listed; notify again" });
    const giving = record.tryLayoutLocked(async () => {
      await learnLayout(next);
      return configureRunning(await record.branches(), next);
    });
    // Answered before the storage controller gives up waiting; the computes are given it all the same.
    const given = await Promise.race([giving, sleep(NOTIFY_PATIENCE_MS).then(() => "late" as const)]);
    if (given === "late") {
      giving.then(
        (done) => log("compute hook /notify-attach, answered before done", { pageservers: next.pageservers, pending: done?.value ?? null }),
        (error: unknown) => log("compute hook /notify-attach failed, answered before done", { error: errorText(error) }),
      );
      return answer(response, 423, { error: "the computes are being given the new layout; notify again" });
    }
    if (given === null) return answer(response, 423, { error: "another replica of neon-control is giving the computes a layout; notify again" });
    log("compute hook /notify-attach", { pageservers: next.pageservers, stripeSize: next.stripeSize, pending: given.value });
    return given.value === 0 ? answer(response, 200) : answer(response, 423, { error: `${given.value} computes did not take the new layout yet; notify again` });
  }
  if (url === "/notify-safekeepers") {
    // A timeline's safekeepers migrated, which alasio never starts itself: the branch's
    // spec and repairs follow them from now on, and its compute at its next start; its read
    // replica, which streams from them, is given its spec anew now, naming them.
    const notified = safekeepersNotified(await jsonBody(request));
    const placed = await record.changing(async ({ branches, save }) => {
      const next = withNotifiedPlacement(branches, notified);
      if (next) await save(next);
      log("compute hook /notify-safekeepers", { timelineId: notified.timelineId, placement: notified.placement, recorded: next !== null });
      return (next ?? branches).find((branch): branch is ReadyBranch => branch.state === "ready" && branch.timelineId === notified.timelineId) ?? null;
    });
    // Asked again until it takes it, as notified again after a 423.
    const replica = placed && await giveSpec({ name: placed.name, replica: true }, placed, await currentLayout());
    if (replica && !tookLayout(replica)) return answer(response, 423, { error: `${placed.name}'s read replica did not take its safekeepers yet; notify again` });
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
