/**
 * Prepares alasio's Neon stack on the host, before compose starts it: the
 * secrets it runs on, made once and kept, and every service's configuration,
 * rendered from them each time. Idempotent.
 *
 * Layout, under `<alasio state>/neon/`:
 *   secrets/            0700: the signing key and secrets.json; neon-control only
 *   keys/               the public key, and the safekeepers' peer token (0600)
 *   compose.env         0600: what compose substitutes into compose.yml
 *   seaweedfs/          s3.json, and data/
 *   controller-db/      the storage controller's Postgres
 *   pageserver/         its workdir: pageserver.toml, identity.toml, metadata.json
 *   safekeeper-{1,2,3}/ their data
 *   control/            neon-control's record of what it bootstrapped, and the compute's spec
 *   backups/            daily logical dumps of alasio's database
 */
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { generateKeyPair, signToken } from "./jwt.js";

export const SAFEKEEPER_IDS = [1, 2, 3];
export const PAGESERVER_ID = 1;
export const DATABASE = "alasio";
export const ROLE = "alasio";
export const BUCKET = "neon";
// The session-filesystem subsystem shares this stack's SeaweedFS (its own bucket) and
// adds a Valkey for JuiceFS metadata. Session hosts join this compose project's network
// and reach both by service name; the secrets below are materialised as files the alasio
// process points its ALASIO_SANDBOX_*_FILE knobs at (see src/sandbox/config.js).
export const BUCKET_SESSIONS = "sessions";
// The analytics lake (neon/lake/README.md) keeps its Parquet files in a bucket of its
// own, and its catalog in a database of its own on the compute, both as role `lake`,
// which alasio makes (src/neon/lake.js) with the password made here.
export const BUCKET_LAKE = "lake";
export const DEFAULT_COMPUTE_PORT = 55433;
// The Linux bridge the stack's network gets, named rather than Docker's br-<id> so host
// tooling and firewall rules can name the interface the stack and its session hosts
// share across the network being recreated. A test stack running alongside gets its
// own (stack.js).
export const DEFAULT_BRIDGE = "alasio-neon0";

const CONTROL_DIR = dirname(fileURLToPath(import.meta.url));
export const COMPOSE_FILE = resolve(CONTROL_DIR, "..", "compose.yml");
const LAKE_DIR = resolve(CONTROL_DIR, "..", "lake");

/** Where the stack keeps everything, under alasio's state directory. */
export function neonLayout(stateDir) {
  const root = join(stateDir, "neon");
  return {
    root,
    secrets: join(root, "secrets"),
    secretsFile: join(root, "secrets", "secrets.json"),
    privateKey: join(root, "secrets", "auth_private_key.pem"),
    keys: join(root, "keys"),
    publicKey: join(root, "keys", "auth_public_key.pem"),
    composeEnv: join(root, "compose.env"),
    seaweedfs: join(root, "seaweedfs"),
    controllerDb: join(root, "controller-db"),
    pageserver: join(root, "pageserver"),
    safekeeper: (id) => join(root, `safekeeper-${id}`),
    control: join(root, "control"),
    backups: join(root, "backups"),
    databaseUrlFile: join(root, "secrets", "alasio-database-url"),
    valkey: join(root, "valkey"),
    sandboxMetadataPasswordFile: join(root, "secrets", "sandbox-metadata-password"),
    sandboxS3KeyFile: join(root, "secrets", "sandbox-s3-key"),
    sandboxS3SecretFile: join(root, "secrets", "sandbox-s3-secret"),
    lakePasswordFile: join(root, "secrets", "lake-database-password"),
    otelCollectorConfig: join(root, "otel-collector.yaml"),
  };
}

/** What the telemetry collector scrapes, by the service name its metrics carry. */
const SCRAPE_TARGETS = {
  pageserver: ["pageserver:9898"],
  safekeeper: SAFEKEEPER_IDS.map((id) => `safekeeper-${id}:7676`),
  "storage-controller": ["storage-controller:1234"],
  "storage-broker": ["storage-broker:50051"],
  compute: ["compute:3080"],
  seaweedfs: ["seaweedfs:9327"],
};

/** The lake's metrics, scraped too while it runs. */
const LAKE_SCRAPE_TARGET = { lake: ["lake:9464"] };

/**
 * The telemetry collector's configuration (JSON, which is YAML): each service's
 * Prometheus metrics, scraped every 30s, sent to `otlp.endpoint` as OTLP over HTTP
 * with `otlp.headers`.
 */
function collectorConfig(otlp, { lake }) {
  const jobs = { ...SCRAPE_TARGETS, ...(lake ? LAKE_SCRAPE_TARGET : {}) };
  return {
    receivers: {
      prometheus: {
        config: {
          scrape_configs: Object.entries(jobs).map(([job, targets]) => ({
            job_name: job,
            scrape_interval: "30s",
            static_configs: [{ targets }],
          })),
        },
      },
    },
    processors: {
      resource: { attributes: [{ key: "service.namespace", value: "alasio-neon", action: "upsert" }] },
      batch: {},
    },
    exporters: { otlphttp: { endpoint: otlp.endpoint, headers: otlp.headers } },
    service: {
      telemetry: { metrics: { level: "none" } },
      pipelines: { metrics: { receivers: ["prometheus"], processors: ["resource", "batch"], exporters: ["otlphttp"] } },
    },
  };
}

function hexId() {
  return randomUUID().replaceAll("-", "");
}

function secret() {
  return randomBytes(24).toString("base64url");
}

function writePrivate(path, content) {
  writeFileSync(path, content, { mode: 0o600 });
  chmodSync(path, 0o600);
}

function writePublic(path, content) {
  writeFileSync(path, content, { mode: 0o644 });
}

/**
 * The stack's secrets, `existing` completed: made on first run and then only added to,
 * never rotated. Each secret has a maker, and any absent is filled in, so a deployment
 * that predates a new secret (the session-filesystem ones, say) gains it on the next
 * start without disturbing the rest. Returns `{ secrets, changed }`.
 */
export function completeSecrets(existing = {}) {
  const secrets = structuredClone(existing);
  const makers = {
    tenantId: () => hexId(),
    timelineId: () => hexId(),
    s3: () => ({
      neon: { accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() },
      admin: { accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() },
      sessions: { accessKey: `sessions${hexId().slice(0, 12)}`, secretKey: secret() },
      lake: { accessKey: `lake${hexId().slice(0, 12)}`, secretKey: secret() },
    }),
    controllerDbPassword: () => secret(),
    alasioPassword: () => secret(),
    sandboxMetadataPassword: () => secret(),
    lakePassword: () => secret(),
    computeControlToken: () => secret(),
  };
  const s3Makers = {
    neon: () => ({ accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() }),
    admin: () => ({ accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() }),
    sessions: () => ({ accessKey: `sessions${hexId().slice(0, 12)}`, secretKey: secret() }),
    lake: () => ({ accessKey: `lake${hexId().slice(0, 12)}`, secretKey: secret() }),
  };
  let changed = false;
  for (const [key, make] of Object.entries(makers)) {
    if (secrets[key] === undefined) {
      secrets[key] = make();
      changed = true;
    }
  }
  // s3 gained members (sessions, lake) after some deployments were made; backfill within it.
  for (const [name, make] of Object.entries(s3Makers)) {
    if (secrets.s3[name] === undefined) {
      secrets.s3[name] = make();
      changed = true;
    }
  }
  return { secrets, changed };
}

/** The stack's secrets on this machine: made once under `layout`, then completed. */
function ensureSecrets(layout) {
  mkdirSync(layout.secrets, { recursive: true, mode: 0o700 });
  chmodSync(layout.secrets, 0o700);
  if (!existsSync(layout.privateKey)) {
    const { privateKeyPem, publicKeyPem } = generateKeyPair();
    writePrivate(layout.privateKey, privateKeyPem);
    mkdirSync(layout.keys, { recursive: true });
    writePublic(layout.publicKey, publicKeyPem);
  }
  const existing = existsSync(layout.secretsFile) ? JSON.parse(readFileSync(layout.secretsFile, "utf8")) : {};
  const { secrets, changed } = completeSecrets(existing);
  if (changed) {
    writePrivate(layout.secretsFile, JSON.stringify(secrets, null, 2) + "\n");
  }
  return secrets;
}

function toml(value) {
  return `'${String(value).replaceAll("'", "")}'`;
}

/**
 * Neon's S3 settings as a TOML inline table, double-quoted so it can travel
 * through compose.env's single-quoted values unchanged.
 */
export function remoteStorage(prefix, { endpoint = "http://seaweedfs:8333", bucket = BUCKET, region = "us-east-1" } = {}) {
  return `{ endpoint="${endpoint}", bucket_name="${bucket}", bucket_region="${region}", prefix_in_bucket="${prefix}" }`;
}

/** SeaweedFS's S3 identities: each of Neon, the lake and session volumes on its own bucket, and an admin. */
export function s3Identities(secrets) {
  const bucket = (name) => [`Read:${name}`, `List:${name}`, `Tagging:${name}`, `Write:${name}`];
  return {
    identities: [
      { name: "neon", credentials: [secrets.s3.neon], actions: bucket(BUCKET) },
      { name: "admin", credentials: [secrets.s3.admin], actions: ["Admin", "Read", "List", "Tagging", "Write"] },
      { name: "sessions", credentials: [secrets.s3.sessions], actions: bucket(BUCKET_SESSIONS) },
      { name: "lake", credentials: [secrets.s3.lake], actions: bucket(BUCKET_LAKE) },
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

/**
 * A digest of neon-control's code. neon-control, and the compute whose spec
 * it writes, carry it in their environment, so compose recreates them when
 * the code changes rather than keep running the old.
 */
export function controlRevision(dir = CONTROL_DIR) {
  const hash = createHash("sha256");
  for (const name of readdirSync(dir).filter((file) => file.endsWith(".js")).sort()) {
    hash.update(name).update("\0").update(readFileSync(join(dir, name))).update("\0");
  }
  return hash.digest("hex").slice(0, 16);
}

/** What the lake's image is built from, relative to its directory. */
function lakeImageInputs(dir) {
  return [
    "Dockerfile",
    ".dockerignore",
    "package.json",
    "package-lock.json",
    ...readdirSync(join(dir, "src")).filter((file) => file.endsWith(".js")).sort().map((file) => `src/${file}`),
  ];
}

/**
 * A digest of what the lake's image is built from. The image is tagged with it
 * (alasio-neon-lake:<revision>), so a change to the lake builds a new image and
 * compose replaces the running one with it.
 */
export function lakeRevision(dir = LAKE_DIR) {
  const hash = createHash("sha256");
  for (const name of lakeImageInputs(dir)) {
    hash.update(name).update("\0").update(readFileSync(join(dir, name))).update("\0");
  }
  return hash.digest("hex").slice(0, 16);
}

/** The lake's image, as compose names it. */
export function lakeImage(dir = LAKE_DIR) {
  return `alasio-neon-lake:${lakeRevision(dir)}`;
}

/** Renders every service's configuration from the secrets. */
function renderConfig(layout, secrets, { computePort, bridgeName, otlp, lake }) {
  const privateKeyPem = readFileSync(layout.privateKey, "utf8");
  const token = (scope, tenantId) => signToken(privateKeyPem, scope, tenantId);

  mkdirSync(layout.keys, { recursive: true });
  // Safekeepers present this to each other when recovering WAL from a peer.
  writePrivate(join(layout.keys, "safekeeper_peer_token"), token("safekeeperdata"));

  mkdirSync(join(layout.seaweedfs, "data"), { recursive: true });
  writePrivate(join(layout.seaweedfs, "s3.json"), JSON.stringify(s3Identities(secrets), null, 2) + "\n");

  mkdirSync(layout.controllerDb, { recursive: true });

  mkdirSync(layout.pageserver, { recursive: true });
  writePublic(join(layout.pageserver, "identity.toml"), `id=${PAGESERVER_ID}\n`);
  writePublic(join(layout.pageserver, "auth_public_key.pem"), readFileSync(layout.publicKey, "utf8"));
  // How the storage controller and computes reach this pageserver; it
  // registers itself with these on every start.
  writePublic(join(layout.pageserver, "metadata.json"), pageserverMetadata("pageserver"));
  writePrivate(
    join(layout.pageserver, "pageserver.toml"),
    pageserverToml({
      brokerUrl: "http://storage-broker:50051",
      controllerUrl: "http://storage-controller:1234",
      token: token("generations_api"),
      publicKeyPath: "/data/.neon/auth_public_key.pem",
      storage: remoteStorage("pageserver"),
    }),
  );

  for (const id of SAFEKEEPER_IDS) mkdirSync(layout.safekeeper(id), { recursive: true });
  mkdirSync(layout.backups, { recursive: true });
  mkdirSync(join(layout.control, "compute"), { recursive: true });

  // Valkey's data, and the session-filesystem secrets as files. The metadata password
  // is Valkey's requirepass (through compose.env) and, as a file, what session hosts
  // read; the S3 keys as files back the alasio process's ALASIO_SANDBOX_S3_*_FILE knobs.
  mkdirSync(layout.valkey, { recursive: true });
  writePrivate(layout.sandboxMetadataPasswordFile, secrets.sandboxMetadataPassword);
  writePrivate(layout.sandboxS3KeyFile, secrets.s3.sessions.accessKey);
  writePrivate(layout.sandboxS3SecretFile, secrets.s3.sessions.secretKey);
  // The lake's database password, as a file alasio sets role `lake`'s password from.
  writePrivate(layout.lakePasswordFile, secrets.lakePassword);

  // The telemetry collector runs only with somewhere to send to; its configuration can
  // hold the backend's credentials (headers), so it is private.
  const collector = otlp ? JSON.stringify(collectorConfig(otlp, { lake }), null, 2) + "\n" : null;
  if (collector) {
    writePrivate(layout.otelCollectorConfig, collector);
  }

  const env = {
    COMPOSE_PROFILES: [collector && "telemetry", lake && "lake"].filter(Boolean).join(","),
    ALASIO_NEON_TELEMETRY_REVISION: collector ? createHash("sha256").update(collector).digest("hex").slice(0, 16) : "",
    // SeaweedFS reads its identities only as it starts; it is recreated when they change.
    ALASIO_NEON_S3_REVISION: createHash("sha256").update(readFileSync(join(layout.seaweedfs, "s3.json"))).digest("hex").slice(0, 16),
    ALASIO_NEON_LAKE_IMAGE: lakeImage(),
    ALASIO_NEON_DIR: layout.root,
    ALASIO_NEON_CONTROL_SOURCE: CONTROL_DIR,
    ALASIO_NEON_CONTROL_REVISION: controlRevision(),
    ALASIO_NEON_COMPUTE_PORT: String(computePort),
    ALASIO_NEON_BRIDGE: bridgeName,
    ALASIO_NEON_UID: String(process.getuid()),
    ALASIO_NEON_GID: String(process.getgid()),
    NEON_S3_ACCESS_KEY: secrets.s3.neon.accessKey,
    NEON_S3_SECRET_KEY: secrets.s3.neon.secretKey,
    NEON_S3_ADMIN_ACCESS_KEY: secrets.s3.admin.accessKey,
    NEON_S3_ADMIN_SECRET_KEY: secrets.s3.admin.secretKey,
    ALASIO_DATABASE_PASSWORD: secrets.alasioPassword,
    NEON_SAFEKEEPER_REMOTE_STORAGE: remoteStorage("safekeeper"),
    CONTROLLER_DB_PASSWORD: secrets.controllerDbPassword,
    SANDBOX_METADATA_PASSWORD: secrets.sandboxMetadataPassword,
    LAKE_DATABASE_PASSWORD: secrets.lakePassword,
    LAKE_S3_ACCESS_KEY: secrets.s3.lake.accessKey,
    LAKE_S3_SECRET_KEY: secrets.s3.lake.secretKey,
    PAGESERVER_JWT_TOKEN: token("pageserverapi"),
    SAFEKEEPER_JWT_TOKEN: token("safekeeperdata"),
    CONTROL_PLANE_JWT_TOKEN: token("admin"),
    PEER_JWT_TOKEN: token("admin"),
  };
  writePrivate(
    layout.composeEnv,
    Object.entries(env)
      .map(([name, value]) => `${name}='${value.replaceAll("'", "")}'`)
      .join("\n") + "\n",
  );
  writePrivate(
    layout.databaseUrlFile,
    `postgresql://${ROLE}:${encodeURIComponent(secrets.alasioPassword)}@127.0.0.1:${computePort}/${DATABASE}\n`,
  );
}

/**
 * Makes the stack's secrets if it has none, and renders its configuration.
 * `otlp` (`{ endpoint, headers }`, or null) is where the stack's telemetry goes, as
 * the stack's network reaches it; `lake` runs the analytics lake. Returns the layout.
 */
export function setupNeon(stateDir, { computePort = DEFAULT_COMPUTE_PORT, bridgeName = DEFAULT_BRIDGE, otlp = null, lake = false } = {}) {
  if (!/^[A-Za-z0-9_.-]{1,15}$/.test(bridgeName)) {
    throw new Error(`bridge name must be 1 to 15 of [A-Za-z0-9_.-] (a Linux interface name), got ${JSON.stringify(bridgeName)}`);
  }
  const layout = neonLayout(stateDir);
  mkdirSync(layout.root, { recursive: true });
  const secrets = ensureSecrets(layout);
  renderConfig(layout, secrets, { computePort, bridgeName, otlp, lake });
  return layout;
}
