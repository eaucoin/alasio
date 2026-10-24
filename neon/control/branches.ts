/**
 * neon-control's branches: alasio's timeline, the branch `main`, and the child timelines
 * branched from it or from each other, each with a compute neon-control serves a spec.
 * What is decided about them, without I/O: names, requests, the record of each, what
 * the storage controller's answers mean, and the spec of a branch's compute. The service
 * (./service.ts) does the I/O.
 *
 * A branch is `creating` from the moment its timeline id is chosen until the storage
 * controller has placed it, `ready` from then, and `deleting` from when its deletion
 * starts until the storage controller has deleted it, when its record goes. Its id and
 * branch point are recorded before the storage controller is first asked, so a creation
 * asked again, after a failure or a restart, asks for exactly the same timeline.
 *
 * A branch's compute has credentials of its own, which whoever asks for the branch makes
 * and neon-control is given only the hashes of: the SCRAM verifiers of the passwords of
 * the roles its clients connect as (alasio's, which its spec gives the role, and the
 * lake's and the harness host's, which its alasio sets), which neon-control answers Neon's proxy with, and the
 * SHA-256 of the token it fetches its spec with, which fetches its spec alone. So what a
 * branch is given reaches neither main's compute nor another branch's spec. main's
 * compute keeps the stack's.
 */
import { createHash, timingSafeEqual } from "node:crypto";

import { SESSIONS_SUFFIX } from "../../src/branch/names.ts";
import type { JsonWebKeySet } from "./jwt.ts";
import { CODEX_LOGIN_ROLE, DATABASE, HARNESS_ROLE, LAKE_ROLE, ROLE } from "./secrets.ts";

/** alasio's own branch: its first timeline, bootstrapped, never deleted. */
export const MAIN = "main";

/**
 * The longest name a branch may have. Branch environments name objects
 * `alasio-branch-<name>-<part>`; at this length one with a part of up to 18 characters
 * still fits the 63 characters Kubernetes allows a namespace, a Service or a label value.
 */
export const MAX_NAME_LENGTH = 30;

/**
 * How many days of each timeline's history the pageserver keeps, alasio's tenant's
 * `pitr_interval`: so how far back a branch point, or a moment read as it was, can be.
 * The layers only older moments need are garbage collected.
 */
export const HISTORY_DAYS = 1;

/** HISTORY_DAYS, as alasio says it. */
export const HISTORY = HISTORY_DAYS === 1 ? "a day" : `${HISTORY_DAYS} days`;

/** Where the storage controller placed a timeline: its safekeepers, by id, at a generation. */
export interface Placement {
  generation: number;
  ids: number[];
}

/** The roles a branch's clients connect as: alasio's, its lake's, its harness host's, and its Codex login relay's. */
export const BRANCH_ROLES = [ROLE, LAKE_ROLE, HARNESS_ROLE, CODEX_LOGIN_ROLE] as const;

/** A branch's compute's own credentials, as neon-control keeps them: hashes alone. */
export interface ComputeCredentials {
  /** The SCRAM verifier (./scram.ts) of the password of each of BRANCH_ROLES on the branch, by role. */
  verifiers: Record<(typeof BRANCH_ROLES)[number], string>;
  /** The SHA-256, in hexadecimal, of the token its compute fetches its spec with. */
  tokenSha256: string;
}

/** What every branch records, whatever its state. */
interface BranchFields {
  name: string;
  /** The branch it was branched from; null for main. */
  parent: string | null;
  timelineId: string;
  /** Its branch point on its parent's timeline; null for main. */
  lsn: string | null;
  /** When neon-control first recorded it. */
  createdAt: string;
  /** Its compute's own credentials; null for main, whose compute has the stack's. */
  compute: ComputeCredentials | null;
}

/** A branch, as neon-control records it, and as its API answers it. */
export type Branch = BranchFields & ({ state: "creating" | "deleting" } | { state: "ready"; safekeepers: Placement });

/** A branch whose timeline is placed, which its compute can run on. */
export type ReadyBranch = Extract<Branch, { state: "ready" }>;

/** A branch asked for: its name, its parent, its branch point (the parent's end if not given), and its compute's credentials. */
export interface BranchRequest {
  name: string;
  parent: string;
  lsn: string | null;
  compute: ComputeCredentials;
}

/**
 * The scope of the lake's token, which lists the branches and does nothing else: Neon's
 * own services do not know it.
 */
export const BRANCHES_SCOPE = "branches";

/** The scope of Neon's proxy's token, which asks for roles' secrets and wakes computes, and does nothing else. */
export const PROXY_SCOPE = "proxy";

/** The paths of neon-control's API that Neon's proxy calls, as its control plane (./endpoints.ts). */
export const PROXY_PATHS: readonly string[] = ["/get_endpoint_access_control", "/wake_compute"];

/**
 * The scopes of the tokens a call of neon-control's API (`method` on `url`) is taken
 * with: the admin's, as the storage controller's hooks bear it; for a list of the
 * branches the lake's too, which keeps its files while any branch may read them; and for
 * what the proxy calls, the proxy's alone.
 */
export function callScopes(method: string, url: string): readonly string[] {
  if (method === "GET" && PROXY_PATHS.includes(new URL(url, "http://neon-control").pathname)) return [PROXY_SCOPE];
  return method === "GET" && url === "/branches" ? ["admin", BRANCHES_SCOPE] : ["admin"];
}

/** A refusal for the API's caller: its HTTP status, and what it says. */
export class BranchError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** What is wrong with `name` as a branch's name, or null if nothing is. */
export function nameProblem(name: string): string | null {
  if (name === MAIN) return `${MAIN} is alasio's own branch`;
  if (name.length > MAX_NAME_LENGTH) return `a branch's name is at most ${MAX_NAME_LENGTH} characters`;
  // A DNS-1123 label, as the names of the objects named after it must be.
  if (!/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/u.test(name)) return "a branch's name is lowercase letters, digits and inner hyphens";
  if (name.endsWith(REPLICA_SUFFIX)) return `a branch's name does not end in ${REPLICA_SUFFIX}, which names a read replica's compute`;
  // Its namespace would be another branch's sessions', whose pods run untrusted code.
  if (name.endsWith(SESSIONS_SUFFIX)) return `a branch's name does not end in ${SESSIONS_SUFFIX}, which names a branch's sessions' namespace`;
  return null;
}

/**
 * The branch point `text` names, an LSN written `<high>/<low>` in hexadecimal, as Neon
 * writes it, and aligned up to 8 bytes, as the pageserver branches at it; null if it
 * names none.
 */
export function branchPoint(text: string): string | null {
  const match = /^([0-9a-f]{1,8})\/([0-9a-f]{1,8})$/iu.exec(text);
  if (!match) return null;
  const [, high = "", low = ""] = match;
  const aligned = ((BigInt(`0x${high}`) << 32n) + BigInt(`0x${low}`) + 7n) & ~7n;
  return `${(aligned >> 32n).toString(16).toUpperCase()}/${(aligned & 0xffffffffn).toString(16).toUpperCase()}`;
}

/** The SHA-256 of `token`, in hexadecimal: what neon-control keeps of a compute's token. */
export const tokenSha256 = (token: string): string => createHash("sha256").update(token).digest("hex");

/** The credentials `value` gives a branch's compute, checked. */
function computeCredentials(value: unknown): ComputeCredentials {
  const object = (candidate: unknown) => (typeof candidate === "object" && candidate !== null ? candidate as Record<string, unknown> : {});
  const { verifiers, tokenSha256: sha, ...rest } = object(value);
  const given = object(verifiers);
  const verifier = (role: string) => typeof given[role] === "string" && given[role].startsWith("SCRAM-SHA-256$");
  if (
    Object.keys(rest).length > 0 || Object.keys(given).length !== BRANCH_ROLES.length || !BRANCH_ROLES.every(verifier) ||
    typeof sha !== "string" || !/^[0-9a-f]{64}$/u.test(sha)
  ) {
    throw new BranchError(400, `a branch's compute is given as { verifiers, tokenSha256 }: a SCRAM-SHA-256 verifier of each of ${BRANCH_ROLES.join(", ")}, and a SHA-256 in hexadecimal`);
  }
  // Each checked above to be a verifier.
  return {
    verifiers: { [ROLE]: given[ROLE] as string, [LAKE_ROLE]: given[LAKE_ROLE] as string, [HARNESS_ROLE]: given[HARNESS_ROLE] as string, [CODEX_LOGIN_ROLE]: given[CODEX_LOGIN_ROLE] as string },
    tokenSha256: sha,
  };
}

/** The branch `body` asks for: `{ name, parent?, lsn?, compute }`, its parent main if not given. */
export function branchRequest(body: unknown): BranchRequest {
  if (typeof body !== "object" || body === null) throw new BranchError(400, "a branch is asked for as { name, parent?, lsn?, compute }");
  const { name, parent = MAIN, lsn = null, compute, ...rest } = body as Record<string, unknown>;
  const unknown = Object.keys(rest);
  if (unknown.length > 0) throw new BranchError(400, `a branch has no ${unknown.join(", ")}`);
  if (typeof name !== "string") throw new BranchError(400, "a branch is asked for by its name");
  const problem = nameProblem(name);
  if (problem) throw new BranchError(400, `${name} is no branch's name: ${problem}`);
  if (typeof parent !== "string") throw new BranchError(400, "a branch's parent is a branch's name");
  const credentials = computeCredentials(compute);
  if (lsn === null) return { name, parent, lsn, compute: credentials };
  const point = typeof lsn === "string" ? branchPoint(lsn) : null;
  if (point === null) throw new BranchError(400, `${String(lsn)} is no LSN: an LSN is written <high>/<low>, in hexadecimal`);
  return { name, parent, lsn: point, compute: credentials };
}

/** The id of the compute of the branch `name`: `alasio` for main's, `branch-<name>` for any other's. */
export function computeId(name: string): string {
  return name === MAIN ? "alasio" : `branch-${name}`;
}

/** The ready branch whose compute is `id`, if any. */
export function branchOfCompute(branches: readonly Branch[], id: string): ReadyBranch | undefined {
  return branches.find((branch): branch is ReadyBranch => branch.state === "ready" && computeId(branch.name) === id);
}

/** What ends the id of a branch's read replica's compute, after its primary's. */
export const REPLICA_SUFFIX = "-ro";

/** The id of the read replica's compute of the branch `name`: `<computeId>-ro`. */
export const replicaId = (name: string): string => `${computeId(name)}${REPLICA_SUFFIX}`;

/** The compute `id` names, a ready branch's primary or its read replica, if any. */
export function computeNamed(branches: readonly Branch[], id: string): { branch: ReadyBranch; replica: boolean } | undefined {
  const replica = id.endsWith(REPLICA_SUFFIX);
  const branch = branchOfCompute(branches, replica ? id.slice(0, -REPLICA_SUFFIX.length) : id);
  return branch && { branch, replica };
}

/** The compute whose token `authorization` bears, by its id: main's, the stack's `mainToken`, or a branch's own; null for none. */
export function computeOfToken(branches: readonly Branch[], authorization: string | undefined, mainToken: string): string | null {
  const token = /^Bearer (.+)$/u.exec(authorization ?? "")?.[1];
  if (!token) return null;
  const same = (a: string, b: string) => a.length === b.length && timingSafeEqual(Buffer.from(a), Buffer.from(b));
  if (mainToken && same(token, mainToken)) return computeId(MAIN);
  const sha = tokenSha256(token);
  const owner = branches.find((branch) => branch.compute !== null && same(sha, branch.compute.tokenSha256));
  return owner ? computeId(owner.name) : null;
}

/** A branch as the API answers it: its record but its compute's credentials, and its compute's id. */
export type BranchView = (Branch extends infer Each ? (Each extends Branch ? Omit<Each, "compute"> : never) : never) & { computeId: string };

/** `branch` as the API answers it (BranchView). */
export function branchView({ compute: _credentials, ...branch }: Branch): BranchView {
  return { ...branch, computeId: computeId(branch.name) };
}

/** The branch named `name`, if any. */
export const branchNamed = (branches: readonly Branch[], name: string): Branch | undefined => branches.find((branch) => branch.name === name);

/** `branches` with `branch` in place of the one of its name, or added after them. */
export function withBranch(branches: readonly Branch[], branch: Branch): Branch[] {
  const index = branches.findIndex(({ name }) => name === branch.name);
  return index === -1 ? [...branches, branch] : branches.map((existing, at) => (at === index ? branch : existing));
}

/** `branches` without the one named `name`. */
export const withoutBranch = (branches: readonly Branch[], name: string): Branch[] => branches.filter((branch) => branch.name !== name);

/**
 * What asking for `request` comes to, given `branches`: the branch of that name already
 * there, ready or still being created, when it is the one asked for; null when there is
 * none, and its parent is ready to branch from. Refuses a name taken by another branch,
 * or one being deleted, and a parent that is not ready.
 */
export function existingBranch(branches: readonly Branch[], request: BranchRequest): Branch | null {
  const existing = branchNamed(branches, request.name);
  if (existing) {
    const same = existing.parent === request.parent && (request.lsn === null || existing.lsn === request.lsn);
    if (existing.state === "deleting") throw new BranchError(409, `${request.name} is being deleted`);
    if (!same) throw new BranchError(409, `${request.name} is a branch of ${existing.parent} at ${existing.lsn} already`);
    return existing;
  }
  const parent = branchNamed(branches, request.parent);
  if (!parent) throw new BranchError(404, `there is no branch ${request.parent} to branch from`);
  if (parent.state !== "ready") throw new BranchError(409, `${request.parent} is ${parent.state}, and is branched from only once ready`);
  return null;
}

/**
 * Starts deleting the branch `name`: `branches` with it `deleting`, which neither serves
 * its compute nor is repaired. Refuses main, a branch there is not, and one that is the
 * parent of another, which the pageserver would refuse to delete.
 */
export function deleting(branches: readonly Branch[], name: string): Branch[] {
  if (name === MAIN) throw new BranchError(400, `${MAIN} is alasio's own branch, which is never deleted`);
  const branch = branchNamed(branches, name);
  if (!branch) throw new BranchError(404, `there is no branch ${name}`);
  const children = branches.filter((child) => child.parent === name).map((child) => child.name);
  if (children.length > 0) throw new BranchError(412, `${name} has branches of its own, deleted first: ${children.join(", ")}`);
  return withBranch(branches, { name, parent: branch.parent, timelineId: branch.timelineId, lsn: branch.lsn, createdAt: branch.createdAt, compute: branch.compute, state: "deleting" });
}

/** The storage controller's answer to creating a timeline, as far as neon-control reads it. */
export interface TimelineCreated {
  timeline_id?: unknown;
  ancestor_timeline_id?: unknown;
  ancestor_lsn?: unknown;
  safekeepers?: { generation: number; safekeepers: { id: number }[] } | null;
}

/**
 * The placement of `branch`'s timeline the storage controller created, as its answer
 * says, once that answer is checked to be of the timeline asked for. The request is an
 * untagged union whose last form is a timeline bootstrapped anew, which a request for a
 * branch the controller did not read as one becomes; so a branch's answer must name its
 * parent's timeline and its branch point.
 */
export function placementOf(branch: Pick<Branch, "name" | "timelineId" | "lsn">, parentTimelineId: string | null, created: TimelineCreated): Placement {
  const lsn = typeof created.ancestor_lsn === "string" ? branchPoint(created.ancestor_lsn) : null;
  const made = `the storage controller made timeline ${String(created.timeline_id)}, of ${String(created.ancestor_timeline_id ?? "no ancestor")} at ${lsn ?? "no LSN"}`;
  if (created.timeline_id !== branch.timelineId || (created.ancestor_timeline_id ?? null) !== parentTimelineId || (parentTimelineId !== null && lsn !== branch.lsn)) {
    throw new BranchError(502, `${made}, not ${branch.name}'s ${branch.timelineId} of ${parentTimelineId ?? "no ancestor"} at ${branch.lsn ?? "no LSN"}`);
  }
  if (!created.safekeepers) throw new BranchError(502, `the storage controller placed ${branch.name}'s timeline on no safekeepers`);
  return { generation: created.safekeepers.generation, ids: created.safekeepers.safekeepers.map((sk) => sk.id) };
}

/**
 * What the storage controller's refusal to create `branch` means, its `status` (null when
 * it did not answer) and `message`: a branch point its parent's history no longer holds,
 * or does not reach, is the caller's to change (406); any other is the caller's to ask
 * again, for the same timeline. The controller answers a pageserver's refusal, other than
 * a 404 or 503, as a 409 that names the pageserver's status.
 */
export function creationRefused(branch: Pick<Branch, "name" | "parent" | "lsn">, status: number | null, message: string): BranchError {
  if (status === 406 || (status === 409 && message.includes("406 Not Acceptable"))) {
    return new BranchError(406, `${branch.parent} has no ${branch.lsn} to branch ${branch.name} from: it is before the history it keeps, which goes ${HISTORY} back, or past its end (${message})`);
  }
  const answered = status === null ? message : `${status} ${message}`;
  return new BranchError(503, `the storage controller did not create ${branch.name}, which may be asked for again: ${answered}`);
}

/** A timeline's safekeepers as the storage controller notifies them (/notify-safekeepers). */
export interface SafekeepersNotified {
  timelineId: string;
  placement: Placement;
}

/** The notification `body` is, checked. */
export function safekeepersNotified(body: unknown): SafekeepersNotified {
  const { timeline_id, generation, safekeepers } = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const ids = Array.isArray(safekeepers) ? safekeepers.map((sk: unknown) => (typeof sk === "object" && sk !== null ? (sk as Record<string, unknown>)["id"] : undefined)) : [];
  if (typeof timeline_id !== "string" || typeof generation !== "number" || ids.length === 0 || !ids.every((id) => typeof id === "number")) {
    throw new BranchError(400, "safekeepers are notified as { tenant_id, timeline_id, generation, safekeepers: [{ id }] }");
  }
  return { timelineId: timeline_id, placement: { generation, ids: ids as number[] } };
}

/**
 * `branches` with the ready branch of the notified timeline placed where it says, when
 * it is of a later generation than its own: what its compute's spec names, and what the
 * safekeepers are repaired with, from then on. Null when no branch changes.
 */
export function withNotifiedPlacement(branches: readonly Branch[], { timelineId, placement }: SafekeepersNotified): Branch[] | null {
  const branch = branches.find((candidate): candidate is ReadyBranch => candidate.state === "ready" && candidate.timelineId === timelineId);
  if (!branch || placement.generation <= branch.safekeepers.generation) return null;
  return withBranch(branches, { ...branch, safekeepers: placement });
}

/** The tenant's shards as the storage controller notifies them (/notify-attach): each one's pageserver, by its node id, in shard order, and their stripe. */
export interface AttachNotified {
  nodeIds: number[];
  stripeSize: number | null;
}

/**
 * The notification `body` is, of the tenant `tenantId`'s shards, checked: `{ tenant_id,
 * stripe_size, shards: [{ node_id, shard_number }] }`, its shards in any order.
 */
export function attachNotified(body: unknown, tenantId: string): AttachNotified {
  const record = (value: unknown) => (typeof value === "object" && value !== null ? value as Record<string, unknown> : {});
  const { tenant_id, stripe_size, shards } = record(body);
  const placed = Array.isArray(shards) ? shards.map((shard: unknown) => record(shard)) : [];
  const numbers = placed.map(({ shard_number }) => shard_number);
  if (
    typeof tenant_id !== "string" || (stripe_size !== null && typeof stripe_size !== "number") || placed.length === 0 ||
    !placed.every(({ node_id, shard_number }) => typeof node_id === "number" && typeof shard_number === "number") ||
    [...numbers].sort((a, b) => Number(a) - Number(b)).some((number, index) => number !== index)
  ) {
    throw new BranchError(400, "shards are notified as { tenant_id, stripe_size, shards: [{ node_id, shard_number }] }, numbered from 0");
  }
  if (tenant_id !== tenantId) throw new BranchError(400, `the shards of the tenant ${tenant_id} were notified, which is not alasio's`);
  const ordered = [...placed].sort((a, b) => Number(a["shard_number"]) - Number(b["shard_number"]));
  return { nodeIds: ordered.map(({ node_id }) => node_id as number), stripeSize: placed.length > 1 ? (stripe_size as number | null) : null };
}

/**
 * The layout `notified` places the shards in, each pageserver's address `addresses` has by
 * its node id; null where it names a node `addresses` has not, which the storage
 * controller notifies again.
 */
export function layoutOf({ nodeIds, stripeSize }: AttachNotified, addresses: ReadonlyMap<number, string>): ShardLayout | null {
  const pageservers = nodeIds.map((id) => addresses.get(id));
  return pageservers.every((address): address is string => address !== undefined) ? { pageservers, stripeSize } : null;
}

/** Whether the layouts `a` and `b` place the shards alike. */
export const sameLayout = (a: ShardLayout, b: ShardLayout): boolean =>
  a.stripeSize === b.stripeSize && a.pageservers.length === b.pageservers.length && a.pageservers.every((pageserver, index) => pageserver === b.pageservers[index]);

/** A shard to move: its id, the pageserver it is attached to, and the one it belongs on. */
export interface ShardMove {
  shardId: string;
  from: number;
  to: number;
}

/** A shard as the storage controller describes it (GET /control/v1/tenant/:id): where it is attached, and whether it is being moved or split. */
export interface DescribedShard {
  tenant_shard_id: string;
  node_attached: number | null;
  is_reconciling: boolean;
  is_splitting: boolean;
}

/**
 * The moves that spread the tenant's `shards`, in shard order, over `pageservers`
 * pageservers numbered from 1, shard n onto pageserver n mod `pageservers`, plus one:
 * where every shard is attached to one pageserver, at rest, as a split leaves them, and
 * as a failover does once every pageserver is back. None otherwise: any other placement
 * is the storage controller's own, as its drain and fill make it, which neon-control
 * never undoes.
 */
export function shardMoves(shards: readonly DescribedShard[], pageservers: number): ShardMove[] {
  const attached = new Set(shards.map(({ node_attached }) => node_attached));
  const [from] = attached;
  if (shards.length < 2 || pageservers < 2 || !shardsAtRest(shards) || attached.size !== 1 || from === null || from === undefined) return [];
  return shards.flatMap(({ tenant_shard_id }, index) => {
    const to = (index % pageservers) + 1;
    return from === to ? [] : [{ shardId: tenant_shard_id, from, to }];
  });
}

/** Whether none of `shards` is being moved or split, so where they are attached is where they stay. */
export const shardsAtRest = (shards: readonly DescribedShard[]): boolean => shards.every(({ is_reconciling, is_splitting }) => !is_reconciling && !is_splitting);

/** A pageserver as the storage controller lists it (GET /control/v1/node), as far as neon-control reads it. */
export interface StorageNode {
  id: number;
  /** Active, WarmingUp or Offline. */
  availability: string;
  /** Active, or paused, draining or filling. */
  scheduling: string;
  listen_pg_addr: string;
  listen_pg_port: number;
}

/** Whether each of `pageservers` pageservers, numbered from 1, is registered, available and takes shards, among `nodes`. */
export const pageserversServing = (nodes: readonly StorageNode[], pageservers: number): boolean =>
  Array.from({ length: pageservers }, (_, index) => index + 1)
    .every((id) => nodes.some((node) => node.id === id && node.availability === "Active" && node.scheduling === "Active"));

/** A safekeeper of the stack, by its id. */
export interface Safekeeper {
  id: number;
  host: string;
  pgPort: number;
  httpPort: number;
}

/** What every compute's spec shares: the stack, and the role and database on every timeline. */
export interface ComputeStack {
  tenantId: string;
  safekeepers: readonly Safekeeper[];
  /** The verifiers of the passwords of the roles main's clients connect as, by role: alasio's, which main's spec gives it, among them. */
  verifiers: Readonly<Record<string, string>> & { readonly alasio: string };
  /** neon-control's own role on every compute, which it counts the compute's sessions as, and a verifier of its password on the compute of an id, made afresh for each spec. */
  monitor: { readonly role: string; readonly verifier: (computeId: string) => string };
  /** The tenant-scoped token the compute reaches the pageserver and safekeepers with. */
  storageAuthToken: string;
  /** The keys compute_ctl verifies its API's tokens with. */
  jwks: JsonWebKeySet;
}

type SettingType = "string" | "integer" | "enum" | "bool";

/** A Postgres setting in a compute spec. */
interface ComputeSetting {
  name: string;
  value: string;
  vartype: SettingType;
}

/** The compute's spec, as compute_ctl reads it (compute_api's ComputeSpec). */
export interface ComputeSpec {
  format_version: number;
  suspend_timeout_seconds: number;
  cluster: {
    cluster_id: string;
    name: string;
    roles: { name: string; encrypted_password: string; options: null }[];
    databases: { name: string; owner: string; options: null }[];
    settings: ComputeSetting[];
    /** Lines added to postgresql.conf as they are: a replica's streaming of WAL. */
    postgresql_conf?: string;
  };
  delta_operations: unknown[];
  tenant_id: string;
  timeline_id: string;
  mode: "Primary" | "Replica";
  pageserver_connstring: string;
  /** The pages of each stripe a key's shard is chosen by, where the tenant has several shards. */
  shard_stripe_size?: number;
  safekeepers_generation: number;
  safekeeper_connstrings: string[];
  storage_auth_token: string;
}

/** What compute_ctl is given: its spec and its own configuration. */
export interface ComputeConfig {
  spec: ComputeSpec;
  compute_ctl_config: { jwks: JsonWebKeySet };
}

/** The port every compute serves Postgres on. */
export const COMPUTE_PORT = 55433;

/** The port of compute_ctl's API, and its metrics, on every compute. */
export const COMPUTE_CTL_PORT = 3080;

/**
 * Where the tenant's shards are, as the storage controller placed them: each shard's
 * pageserver, `<host>:<port>` as computes reach it, in shard order, and the pages of each
 * stripe, where there are several shards (null for one).
 */
export interface ShardLayout {
  pageservers: string[];
  stripeSize: number | null;
}

function setting(name: string, value: string | number, vartype: SettingType): ComputeSetting {
  return { name, value: String(value), vartype };
}

/** `value` as a postgresql.conf string, quoted. */
const confString = (value: string): string => `'${value.replaceAll("'", "''")}'`;

/**
 * The spec of `branch`'s compute: a primary on its timeline, at its safekeepers, reading
 * each shard's pages from the pageserver `layout` places it on. The role and database are
 * main's, which every branch inherits with its data; the role's password is the branch's
 * own.
 *
 * With `replica`, its read replica's (`<computeId>-ro`): Neon's Replica mode, its
 * basebackup the pageserver's latest, streaming the timeline's WAL from the branch's
 * safekeepers, which Neon's compute_ctl leaves to its control plane to say, as neon_local
 * does (control_plane/src/endpoint.rs at Neon's release-9129), with the storage token as
 * the safekeepers' password. It makes no role, as a replica writes nothing: its roles are
 * its primary's, neon-control's own among them, with its primary's password.
 *
 * Its storage token is the tenant's, as the pageserver and safekeepers take no narrower
 * one: a timeline is not a scope of theirs. A branch's compute could read main's timeline
 * with it, which only Neon's storage services take, and only from the stack's pods.
 */
export function computeConfig(branch: ReadyBranch, stack: ComputeStack, layout: ShardLayout, { replica = false } = {}): ComputeConfig {
  // The timeline was placed on safekeepers of the stack, so each id is one of theirs.
  const placed = branch.safekeepers.ids.map((id) => stack.safekeepers.find((sk) => sk.id === id)!);
  const streaming = [
    `host=${placed.map(({ host }) => host).join(",")}`,
    `port=${placed.map(({ pgPort }) => pgPort).join(",")}`,
    `options='-c timeline_id=${branch.timelineId} tenant_id=${stack.tenantId}'`,
    "application_name=replica",
    "replication=true",
    `password=${stack.storageAuthToken}`,
  ].join(" ");
  return {
    spec: {
      format_version: 1.0,
      suspend_timeout_seconds: -1,
      cluster: {
        cluster_id: "alasio",
        name: "alasio",
        roles: replica
          ? []
          : [
            { name: ROLE, encrypted_password: branch.compute?.verifiers.alasio ?? stack.verifiers.alasio, options: null },
            // After alasio's: compute_ctl applies them in order, so once it may log in, alasio may too.
            { name: stack.monitor.role, encrypted_password: stack.monitor.verifier(computeId(branch.name)), options: null },
          ],
        databases: replica ? [] : [{ name: DATABASE, owner: ROLE, options: null }],
        settings: [
          setting("listen_addresses", "0.0.0.0", "string"),
          setting("port", COMPUTE_PORT, "integer"),
          setting("max_connections", 100, "integer"),
          // A replica serves the lake's loader alone.
          setting("shared_buffers", replica ? "64MB" : "128MB", "string"),
          setting("password_encryption", "scram-sha-256", "enum"),
          // Durability is the safekeepers': a commit waits for their quorum,
          // and the compute's own disk is rebuilt on every start.
          setting("fsync", "off", "bool"),
          setting("synchronous_standby_names", "walproposer", "string"),
          setting("shared_preload_libraries", "neon", "string"),
          setting("wal_level", "replica", "enum"),
          setting("wal_log_hints", "off", "bool"),
          setting("wal_keep_size", 0, "integer"),
          // After a checkpoint, the first change of each page writes it to the WAL whole,
          // and transcript search's indexes change the same pages all day: so an hour
          // between checkpoints, not five minutes. It lengthens no recovery, as the
          // compute starts from the pageserver, never from its own WAL. Enough WAL for a
          // checkpoint before the hour is about 1GB, three times the busiest hour's yet.
          setting("checkpoint_timeout", "1h", "string"),
          setting("max_wal_size", "2GB", "string"),
          setting("wal_sender_timeout", "5s", "string"),
          setting("max_wal_senders", 10, "integer"),
          setting("max_replication_slots", 10, "integer"),
          setting("max_replication_write_lag", "15MB", "string"),
          setting("max_replication_flush_lag", "10GB", "string"),
          setting("restart_after_crash", "off", "bool"),
        ],
        ...(replica
          ? {
            postgresql_conf: [
              `primary_conninfo = ${confString(streaming)}`,
              `primary_slot_name = ${confString(`repl_${branch.timelineId}_`)}`,
              "hot_standby = on",
              // The loader's reads hold back the cleanup of what they read, on the primary, rather
              // than be cancelled by its replay; and WAL that would cancel one waits for it a while.
              "hot_standby_feedback = on",
              "max_standby_streaming_delay = 5min",
              "recovery_prefetch = off",
              "",
            ].join("\n"),
          }
          : {}),
      },
      delta_operations: [],
      tenant_id: stack.tenantId,
      timeline_id: branch.timelineId,
      mode: replica ? "Replica" : "Primary",
      pageserver_connstring: layout.pageservers.map((pageserver) => `postgresql://no_user@${pageserver}`).join(","),
      ...(layout.stripeSize === null ? {} : { shard_stripe_size: layout.stripeSize }),
      safekeepers_generation: branch.safekeepers.generation,
      // A replica syncs with no safekeeper: it streams from them (postgresql_conf).
      safekeeper_connstrings: replica ? [] : placed.map(({ host, pgPort }) => `${host}:${pgPort}`),
      storage_auth_token: stack.storageAuthToken,
    },
    compute_ctl_config: { jwks: stack.jwks },
  };
}
