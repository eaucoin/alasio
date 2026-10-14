/**
 * The secrets alasio's Neon runs on, and the configuration rendered from them, for the
 * setup job (./kube-setup.ts): made once and kept, completed when a deployment predates
 * one, never rotated.
 */
import { randomBytes, randomUUID } from "node:crypto";

export const PAGESERVER_ID = 1;
export const DATABASE = "alasio";
export const ROLE = "alasio";

/** Who holds S3 credentials: Neon's storage, the stack's admin, the lake, its reader, workspace storage's JuiceFS, and the log. */
export type S3Identity = "neon" | "admin" | "lake" | "lakeReader" | "workspaces" | "log";

/**
 * The log's principals besides its admin, each with a password of its own: the collector's
 * gateway, which writes the stack's telemetry to it; the lake's intake, which reads that;
 * and KEDA, which reads how much the intake has yet to.
 */
export const LOG_PRINCIPALS = ["gateway", "intake", "scaler"] as const;
export type LogPrincipal = (typeof LOG_PRINCIPALS)[number];

export interface S3Credentials {
  accessKey: string;
  secretKey: string;
}

/** The stack's secrets, as the root Secret's secrets.json keeps them. */
export interface StackSecrets {
  tenantId: string;
  timelineId: string;
  s3: Record<S3Identity, S3Credentials>;
  controllerDbPassword: string;
  alasioPassword: string;
  lakePassword: string;
  computeControlToken: string;
  /** Workspace storage's Valkey's. */
  valkeyPassword: string;
  /** The bundled object store's filer store's, its filers' Valkey: its users', the filers', the collector's and the backups'. */
  filerStoreFilerPassword: string;
  filerStoreCollectorPassword: string;
  filerStoreBackupPassword: string;
  /** The lake's query endpoint's: its reader's role's, and the token its queries carry. */
  lakeReaderPassword: string;
  lakeQueryToken: string;
  /** Grafana's: its role's, its admin's, and the key it encrypts what it keeps secret with. */
  grafanaPassword: string;
  grafanaAdminPassword: string;
  grafanaSecretKey: string;
  /** What alasio signs each branch environment's token to fork its sessions with (src/branch/names.ts). */
  branchForkKey: string;
  /** The log's: the id KRaft formats its nodes with, and each principal's password, its admin's among them. */
  logClusterId: string;
  log: Record<"admin" | LogPrincipal, string>;
}

/** The stack's secrets as an earlier release may have kept them: any may be absent. */
export type StoredSecrets = Partial<Omit<StackSecrets, "s3" | "log">> & { s3?: Partial<Record<S3Identity, S3Credentials>>; log?: Partial<StackSecrets["log"]> };

/** Where Neon keeps its files: an S3 bucket at an endpoint. */
export interface RemoteStorageLocation {
  endpoint: string;
  bucket: string;
  region?: string;
}

/** The buckets of the bundled SeaweedFS, by who uses each; workspace storage's null when it is off. */
export interface Buckets {
  neon: string;
  lake: string;
  workspaces: string | null;
  log: string;
}

/** SeaweedFS's S3 configuration (its s3.json). */
export interface SeaweedS3Config {
  identities: SeaweedIdentity[];
}

export interface SeaweedIdentity {
  name: S3Identity;
  credentials: S3Credentials[];
  actions: string[];
}

/** The pageserver's configuration, which pageserverToml renders. */
export interface PageserverConfig {
  brokerUrl: string;
  controllerUrl: string;
  token: string;
  publicKeyPath: string;
  /** Its remote storage, as remoteStorage renders it. */
  storage: string;
}

function hexId(): string {
  return randomUUID().replaceAll("-", "");
}

function secret(): string {
  return randomBytes(24).toString("base64url");
}

/** A KRaft cluster's id, as Kafka writes one: 16 random bytes, in base64url. */
function clusterId(): string {
  return randomBytes(16).toString("base64url");
}

/**
 * The stack's secrets, `existing` completed: made on first run and then only added to,
 * never rotated. Each secret has a maker, and any absent is filled in, so a deployment
 * that predates a new secret gains it on the next install or upgrade without disturbing
 * the rest. Returns `{ secrets, changed }`.
 */
export function completeSecrets(existing: StoredSecrets = {}): { secrets: StackSecrets; changed: boolean } {
  const stored = structuredClone(existing);
  let changed = false;
  const kept = <T>(value: T | undefined, make: () => T): T => {
    if (value !== undefined) return value;
    changed = true;
    return make();
  };
  // A secret already held keeps its place; one made is added after them, in this order.
  const s3: Partial<Record<S3Identity, S3Credentials>> = kept(stored.s3, () => ({}));
  const log: Partial<StackSecrets["log"]> = kept(stored.log, () => ({}));
  const secrets: StackSecrets = {
    ...stored,
    tenantId: kept(stored.tenantId, () => hexId()),
    timelineId: kept(stored.timelineId, () => hexId()),
    s3: {
      ...s3,
      neon: kept(s3.neon, () => ({ accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() })),
      admin: kept(s3.admin, () => ({ accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() })),
      lake: kept(s3.lake, () => ({ accessKey: `lake${hexId().slice(0, 12)}`, secretKey: secret() })),
      lakeReader: kept(s3.lakeReader, () => ({ accessKey: `lakereader${hexId().slice(0, 12)}`, secretKey: secret() })),
      workspaces: kept(s3.workspaces, () => ({ accessKey: `workspaces${hexId().slice(0, 12)}`, secretKey: secret() })),
      log: kept(s3.log, () => ({ accessKey: `log${hexId().slice(0, 12)}`, secretKey: secret() })),
    },
    controllerDbPassword: kept(stored.controllerDbPassword, () => secret()),
    alasioPassword: kept(stored.alasioPassword, () => secret()),
    lakePassword: kept(stored.lakePassword, () => secret()),
    computeControlToken: kept(stored.computeControlToken, () => secret()),
    valkeyPassword: kept(stored.valkeyPassword, () => secret()),
    lakeReaderPassword: kept(stored.lakeReaderPassword, () => secret()),
    lakeQueryToken: kept(stored.lakeQueryToken, () => secret()),
    grafanaPassword: kept(stored.grafanaPassword, () => secret()),
    grafanaAdminPassword: kept(stored.grafanaAdminPassword, () => secret()),
    grafanaSecretKey: kept(stored.grafanaSecretKey, () => secret()),
    branchForkKey: kept(stored.branchForkKey, () => secret()),
    filerStoreFilerPassword: kept(stored.filerStoreFilerPassword, () => secret()),
    filerStoreCollectorPassword: kept(stored.filerStoreCollectorPassword, () => secret()),
    filerStoreBackupPassword: kept(stored.filerStoreBackupPassword, () => secret()),
    logClusterId: kept(stored.logClusterId, () => clusterId()),
    log: {
      ...log,
      admin: kept(log.admin, () => secret()),
      ...Object.fromEntries(LOG_PRINCIPALS.map((principal) => [principal, kept(log[principal], () => secret())])) as Record<LogPrincipal, string>,
    },
  };
  return { secrets, changed };
}

function toml(value: string): string {
  return `'${String(value).replaceAll("'", "")}'`;
}

/** Neon's S3 settings, under `prefix` of `bucket` at `endpoint`, as a TOML inline table. */
export function remoteStorage(prefix: string, { endpoint, bucket, region = "us-east-1" }: RemoteStorageLocation): string {
  return `{ endpoint="${endpoint}", bucket_name="${bucket}", bucket_region="${region}", prefix_in_bucket="${prefix}" }`;
}

/**
 * SeaweedFS's S3 identities: Neon, the lake, the log and, when it is on, workspace
 * storage, each on their own bucket of `buckets`; the lake's reader, which may only read
 * the lake's; and an admin, which makes the buckets and keeps the backups.
 */
export function s3Identities(secrets: StackSecrets, buckets: Buckets): SeaweedS3Config {
  const on = (name: string) => [`Read:${name}`, `List:${name}`, `Tagging:${name}`, `Write:${name}`];
  return {
    identities: [
      { name: "neon", credentials: [secrets.s3.neon], actions: on(buckets.neon) },
      { name: "admin", credentials: [secrets.s3.admin], actions: ["Admin", "Read", "List", "Tagging", "Write"] },
      // Branch environments' lakes are given it too: a branch's lake reads main's files where they are (DuckLake keeps
      // every file's path relative to one data path) and writes beside them, and SeaweedFS's Write, which writing
      // needs, deletes as well, so it cannot be narrowed to a branch's own files (cli/src/branches.ts).
      { name: "lake", credentials: [secrets.s3.lake], actions: on(buckets.lake) },
      { name: "lakeReader", credentials: [secrets.s3.lakeReader], actions: [`Read:${buckets.lake}`, `List:${buckets.lake}`] },
      ...(buckets.workspaces === null ? [] : [{ name: "workspaces" as const, credentials: [secrets.s3.workspaces], actions: on(buckets.workspaces) }]),
      { name: "log", credentials: [secrets.s3.log], actions: on(buckets.log) },
    ],
  };
}

/**
 * When the pageserver evicts layers from its disk, which it downloads again when read:
 * only once the node's disk is nearly full. It measures the whole filesystem its volume
 * is on, which for a volume on the node's own disk is shared with everything else there.
 * Neon's default, at 80 % used or under 2 GB free, had it evict every layer it could each
 * minute, each a minute after downloading it again, while the stack's disk sat at 86 %
 * for other reasons: 6 to 52 GiB an hour through the object store, whose memory it took
 * past its limit. At 94 % used or under 8 GiB free it evicts just before the floors below
 * which the disk is defended harder: kubelet evicts pods under 5 % free
 * (cli/src/cluster/k3s.ts), and the bundled SeaweedFS stops writing under its
 * minFreeSpace, 5 GiB by default.
 */
const LAYER_EVICTION = { maxUsagePct: 94, minAvailBytes: 8 * 1024 ** 3 };

/**
 * The pageserver's configuration: where the broker and the storage controller are, its
 * remote storage, the token it calls the controller with, and when it evicts layers
 * (LAYER_EVICTION). Its key is read from `publicKeyPath`.
 */
export function pageserverToml({ brokerUrl, controllerUrl, token, publicKeyPath, storage }: PageserverConfig): string {
  return [
    `broker_endpoint=${toml(brokerUrl)}`,
    `pg_distrib_dir=${toml("/usr/local/")}`,
    `listen_pg_addr=${toml("0.0.0.0:6400")}`,
    `listen_http_addr=${toml("0.0.0.0:9898")}`,
    `availability_zone=${toml("az-pageserver")}`,
    `control_plane_api=${toml(`${controllerUrl}/upcall/v1/`)}`,
    `control_plane_api_token=${toml(token)}`,
    `http_auth_type='NeonJWT'`,
    `pg_auth_type='NeonJWT'`,
    `auth_validation_public_key_path=${toml(publicKeyPath)}`,
    `remote_storage=${storage}`,
    `disk_usage_based_eviction={ max_usage_pct=${LAYER_EVICTION.maxUsagePct}, min_avail_bytes=${LAYER_EVICTION.minAvailBytes} }`,
    "",
  ].join("\n");
}

/** How the storage controller and computes reach the pageserver at `host`; it registers itself with these. */
export function pageserverMetadata(host: string): string {
  return JSON.stringify({ host, port: 6400, http_host: host, http_port: 9898, availability_zone_id: "az-pageserver" }) + "\n";
}
