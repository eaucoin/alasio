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
import { chmodSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomBytes, randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { generateKeyPair, signToken } from "./jwt.js";

export const SAFEKEEPER_IDS = [1, 2, 3];
export const PAGESERVER_ID = 1;
export const DATABASE = "alasio";
export const ROLE = "alasio";
export const BUCKET = "neon";
export const DEFAULT_COMPUTE_PORT = 55433;

const CONTROL_DIR = dirname(fileURLToPath(import.meta.url));
export const COMPOSE_FILE = resolve(CONTROL_DIR, "..", "compose.yml");

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

/** The stack's secrets: made on first run, then only ever read. */
function ensureSecrets(layout) {
  mkdirSync(layout.secrets, { recursive: true, mode: 0o700 });
  chmodSync(layout.secrets, 0o700);
  if (!existsSync(layout.privateKey)) {
    const { privateKeyPem, publicKeyPem } = generateKeyPair();
    writePrivate(layout.privateKey, privateKeyPem);
    mkdirSync(layout.keys, { recursive: true });
    writePublic(layout.publicKey, publicKeyPem);
  }
  if (!existsSync(layout.secretsFile)) {
    writePrivate(
      layout.secretsFile,
      JSON.stringify(
        {
          tenantId: hexId(),
          timelineId: hexId(),
          s3: {
            neon: { accessKey: `neon${hexId().slice(0, 12)}`, secretKey: secret() },
            admin: { accessKey: `admin${hexId().slice(0, 12)}`, secretKey: secret() },
          },
          controllerDbPassword: secret(),
          alasioPassword: secret(),
        },
        null,
        2,
      ) + "\n",
    );
  }
  return JSON.parse(readFileSync(layout.secretsFile, "utf8"));
}

function toml(value) {
  return `'${String(value).replaceAll("'", "")}'`;
}

/**
 * Neon's S3 settings as a TOML inline table, double-quoted so it can travel
 * through compose.env's single-quoted values unchanged.
 */
function remoteStorage(prefix) {
  return `{ endpoint="http://seaweedfs:8333", bucket_name="${BUCKET}", bucket_region="us-east-1", prefix_in_bucket="${prefix}" }`;
}

/** Renders every service's configuration from the secrets. */
function renderConfig(layout, secrets, { computePort }) {
  const privateKeyPem = readFileSync(layout.privateKey, "utf8");
  const token = (scope, tenantId) => signToken(privateKeyPem, scope, tenantId);

  mkdirSync(layout.keys, { recursive: true });
  // Safekeepers present this to each other when recovering WAL from a peer.
  writePrivate(join(layout.keys, "safekeeper_peer_token"), token("safekeeperdata"));

  mkdirSync(join(layout.seaweedfs, "data"), { recursive: true });
  writePrivate(
    join(layout.seaweedfs, "s3.json"),
    JSON.stringify(
      {
        identities: [
          {
            name: "neon",
            credentials: [secrets.s3.neon],
            actions: [`Read:${BUCKET}`, `List:${BUCKET}`, `Tagging:${BUCKET}`, `Write:${BUCKET}`],
          },
          {
            name: "admin",
            credentials: [secrets.s3.admin],
            actions: ["Admin", "Read", "List", "Tagging", "Write"],
          },
        ],
      },
      null,
      2,
    ) + "\n",
  );

  mkdirSync(layout.controllerDb, { recursive: true });

  mkdirSync(layout.pageserver, { recursive: true });
  writePublic(join(layout.pageserver, "identity.toml"), `id=${PAGESERVER_ID}\n`);
  writePublic(join(layout.pageserver, "auth_public_key.pem"), readFileSync(layout.publicKey, "utf8"));
  // How the storage controller and computes reach this pageserver; it
  // registers itself with these on every start.
  writePublic(
    join(layout.pageserver, "metadata.json"),
    JSON.stringify({
      host: "pageserver",
      port: 6400,
      http_host: "pageserver",
      http_port: 9898,
      availability_zone_id: "az-pageserver",
    }) + "\n",
  );
  writePrivate(
    join(layout.pageserver, "pageserver.toml"),
    [
      `broker_endpoint=${toml("http://storage-broker:50051")}`,
      `pg_distrib_dir=${toml("/usr/local/")}`,
      `listen_pg_addr=${toml("0.0.0.0:6400")}`,
      `listen_http_addr=${toml("0.0.0.0:9898")}`,
      `availability_zone=${toml("az-pageserver")}`,
      `control_plane_api=${toml("http://storage-controller:1234/upcall/v1/")}`,
      `control_plane_api_token=${toml(token("generations_api"))}`,
      `http_auth_type='NeonJWT'`,
      `pg_auth_type='NeonJWT'`,
      `auth_validation_public_key_path=${toml("/data/.neon/auth_public_key.pem")}`,
      `remote_storage=${remoteStorage("pageserver")}`,
      "",
    ].join("\n"),
  );

  for (const id of SAFEKEEPER_IDS) mkdirSync(layout.safekeeper(id), { recursive: true });
  mkdirSync(layout.backups, { recursive: true });
  mkdirSync(join(layout.control, "compute"), { recursive: true });

  const env = {
    ALASIO_NEON_DIR: layout.root,
    ALASIO_NEON_CONTROL_SOURCE: CONTROL_DIR,
    ALASIO_NEON_COMPUTE_PORT: String(computePort),
    ALASIO_NEON_UID: String(process.getuid()),
    ALASIO_NEON_GID: String(process.getgid()),
    NEON_S3_ACCESS_KEY: secrets.s3.neon.accessKey,
    NEON_S3_SECRET_KEY: secrets.s3.neon.secretKey,
    NEON_S3_ADMIN_ACCESS_KEY: secrets.s3.admin.accessKey,
    NEON_S3_ADMIN_SECRET_KEY: secrets.s3.admin.secretKey,
    ALASIO_DATABASE_PASSWORD: secrets.alasioPassword,
    NEON_SAFEKEEPER_REMOTE_STORAGE: remoteStorage("safekeeper"),
    CONTROLLER_DB_PASSWORD: secrets.controllerDbPassword,
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
 * Returns the layout.
 */
export function setupNeon(stateDir, { computePort = DEFAULT_COMPUTE_PORT } = {}) {
  const layout = neonLayout(stateDir);
  mkdirSync(layout.root, { recursive: true });
  const secrets = ensureSecrets(layout);
  renderConfig(layout, secrets, { computePort });
  return layout;
}
