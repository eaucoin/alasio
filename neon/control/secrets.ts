/**
 * The secrets alasio's Neon runs on, and the configuration rendered from them, for the
 * setup job (./kube-setup.ts): made once and kept, completed when a deployment predates
 * one, never rotated.
 */
import { randomBytes, randomUUID } from "node:crypto";

export const PAGESERVER_ID = 1;
export const DATABASE = "alasio";
export const ROLE = "alasio";

/** Who holds S3 credentials: Neon's storage, the stack's admin, the lake, and workspace storage's JuiceFS. */
export type S3Identity = "neon" | "admin" | "lake" | "workspaces";

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
}

/** The stack's secrets as an earlier release may have kept them: any may be absent. */
export type StoredSecrets = Partial<Omit<StackSecrets, "s3">> & { s3?: Partial<Record<S3Identity, S3Credentials>> };

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
  const secrets: StackSecrets = {
    ...stored,
    tenantId: kept(stored.tenantId, () => hexId()),
    timelineId: kept(stored.timelineId, () => hexId()),
    s3: {
      ...s3,
      neon: kept(s3.neon, () => ({ accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() })),
      admin: kept(s3.admin, () => ({ accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() })),
      lake: kept(s3.lake, () => ({ accessKey: `lake${hexId().slice(0, 12)}`, secretKey: secret() })),
      workspaces: kept(s3.workspaces, () => ({ accessKey: `workspaces${hexId().slice(0, 12)}`, secretKey: secret() })),
    },
    controllerDbPassword: kept(stored.controllerDbPassword, () => secret()),
    alasioPassword: kept(stored.alasioPassword, () => secret()),
    lakePassword: kept(stored.lakePassword, () => secret()),
    computeControlToken: kept(stored.computeControlToken, () => secret()),
    valkeyPassword: kept(stored.valkeyPassword, () => secret()),
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
 * SeaweedFS's S3 identities: Neon, the lake and, when it is on, workspace storage, each
 * on their own bucket of `buckets`, and an admin, which makes the buckets and keeps the
 * backups.
 */
export function s3Identities(secrets: StackSecrets, buckets: Buckets): SeaweedS3Config {
  const on = (name: string) => [`Read:${name}`, `List:${name}`, `Tagging:${name}`, `Write:${name}`];
  return {
    identities: [
      { name: "neon", credentials: [secrets.s3.neon], actions: on(buckets.neon) },
      { name: "admin", credentials: [secrets.s3.admin], actions: ["Admin", "Read", "List", "Tagging", "Write"] },
      { name: "lake", credentials: [secrets.s3.lake], actions: on(buckets.lake) },
      ...(buckets.workspaces === null ? [] : [{ name: "workspaces" as const, credentials: [secrets.s3.workspaces], actions: on(buckets.workspaces) }]),
    ],
  };
}

/**
 * The pageserver's configuration: where the broker and the storage controller are, its
 * remote storage, and the token it calls the controller with. Its key is read from
 * `publicKeyPath`.
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
    "",
  ].join("\n");
}

/** How the storage controller and computes reach the pageserver at `host`; it registers itself with these. */
export function pageserverMetadata(host: string): string {
  return JSON.stringify({ host, port: 6400, http_host: host, http_port: 9898, availability_zone_id: "az-pageserver" }) + "\n";
}
