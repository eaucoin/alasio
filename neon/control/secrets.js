/**
 * The secrets alasio's Neon runs on, and the configuration rendered from them, for the
 * setup job (./kube-setup.js): made once and kept, completed when a deployment predates
 * one, never rotated.
 */
import { randomBytes, randomUUID } from "node:crypto";

export const PAGESERVER_ID = 1;
export const DATABASE = "alasio";
export const ROLE = "alasio";

function hexId() {
  return randomUUID().replaceAll("-", "");
}

function secret() {
  return randomBytes(24).toString("base64url");
}

/**
 * The stack's secrets, `existing` completed: made on first run and then only added to,
 * never rotated. Each secret has a maker, and any absent is filled in, so a deployment
 * that predates a new secret gains it on the next install or upgrade without disturbing
 * the rest. Returns `{ secrets, changed }`.
 */
export function completeSecrets(existing = {}) {
  const secrets = structuredClone(existing);
  const makers = {
    tenantId: () => hexId(),
    timelineId: () => hexId(),
    s3: () => ({}),
    controllerDbPassword: () => secret(),
    alasioPassword: () => secret(),
    lakePassword: () => secret(),
    computeControlToken: () => secret(),
  };
  const s3Makers = {
    neon: () => ({ accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() }),
    admin: () => ({ accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() }),
    lake: () => ({ accessKey: `lake${hexId().slice(0, 12)}`, secretKey: secret() }),
  };
  let changed = false;
  for (const [key, make] of Object.entries(makers)) {
    if (secrets[key] === undefined) {
      secrets[key] = make();
      changed = true;
    }
  }
  for (const [name, make] of Object.entries(s3Makers)) {
    if (secrets.s3[name] === undefined) {
      secrets.s3[name] = make();
      changed = true;
    }
  }
  return { secrets, changed };
}

function toml(value) {
  return `'${String(value).replaceAll("'", "")}'`;
}

/** Neon's S3 settings, under `prefix` of `bucket` at `endpoint`, as a TOML inline table. */
export function remoteStorage(prefix, { endpoint, bucket, region = "us-east-1" }) {
  return `{ endpoint="${endpoint}", bucket_name="${bucket}", bucket_region="${region}", prefix_in_bucket="${prefix}" }`;
}

/**
 * SeaweedFS's S3 identities: Neon and the lake each on their own bucket of `buckets`
 * (`{ neon, lake }`), and an admin, which makes the buckets and keeps the backups.
 */
export function s3Identities(secrets, buckets) {
  const on = (name) => [`Read:${name}`, `List:${name}`, `Tagging:${name}`, `Write:${name}`];
  return {
    identities: [
      { name: "neon", credentials: [secrets.s3.neon], actions: on(buckets.neon) },
      { name: "admin", credentials: [secrets.s3.admin], actions: ["Admin", "Read", "List", "Tagging", "Write"] },
      { name: "lake", credentials: [secrets.s3.lake], actions: on(buckets.lake) },
    ],
  };
}

/**
 * The pageserver's configuration: where the broker and the storage controller are, its
 * remote storage, and the token it calls the controller with. Its key is read from
 * `publicKeyPath`.
 */
export function pageserverToml({ brokerUrl, controllerUrl, token, publicKeyPath, storage }) {
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
export function pageserverMetadata(host) {
  return JSON.stringify({ host, port: 6400, http_host: host, http_port: 9898, availability_zone_id: "az-pageserver" }) + "\n";
}
