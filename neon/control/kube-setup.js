/**
 * Prepares alasio's Neon on Kubernetes, as a Helm hook Job before the stack starts: the
 * secrets it runs on, made once and kept in one Secret, and what each service is given
 * from them, as a Secret of its own, rendered from them on every install and upgrade.
 * Idempotent, and never rotating: the Kubernetes counterpart of ./setup.js, from whose
 * makers and renderers it builds.
 *
 * Environment: NAMESPACE, SECRET_PREFIX (the release's full name, which every Secret's
 * name begins with); NEON_BROKER_URL, NEON_CONTROLLER_URL, NEON_PAGESERVER_HOST,
 * NEON_COMPUTE_HOST, NEON_CONTROLLER_DB_HOST; S3_ENDPOINT and S3_REGION, and
 * S3_BUCKET_NEON and S3_BUCKET_LAKE; S3_EXTERNAL=1 with S3_ACCESS_KEY and S3_SECRET_KEY
 * when the object store is the operator's own rather than the bundled SeaweedFS, whose
 * identities are made here otherwise.
 */
import { generateKeyPair, signToken } from "./jwt.js";
import { completeSecrets, DATABASE, pageserverMetadata, pageserverToml, PAGESERVER_ID, remoteStorage, ROLE, s3Identities } from "./setup.js";

const PUBLIC_KEY = "auth_public_key.pem";

/** Every Secret's name, by what it is for. */
export function secretNames(prefix) {
  return {
    root: `${prefix}-neon-root`,
    safekeeper: `${prefix}-neon-safekeeper`,
    pageserver: `${prefix}-neon-pageserver`,
    storageController: `${prefix}-neon-storage-controller`,
    controllerDb: `${prefix}-neon-controller-db`,
    seaweedfs: `${prefix}-neon-seaweedfs`,
    s3Admin: `${prefix}-neon-s3-admin`,
    compute: `${prefix}-neon-compute`,
    database: `${prefix}-database`,
    lake: `${prefix}-lake`,
  };
}

/**
 * What each service is given, by Secret name, from the stack's `secrets` and keys and
 * the deployment's `config` (the environment above, as an object). Pure, for tests.
 */
export function renderSecrets({ secrets, privateKeyPem, publicKeyPem, config }) {
  const names = secretNames(config.prefix);
  const token = (scope, tenantId) => signToken(privateKeyPem, scope, tenantId);
  const s3 = (identity) => (config.external
    ? { accessKey: config.accessKey, secretKey: config.secretKey }
    : secrets.s3[identity]);
  const aws = (identity) => ({ AWS_ACCESS_KEY_ID: s3(identity).accessKey, AWS_SECRET_ACCESS_KEY: s3(identity).secretKey });
  const storage = (prefix) => remoteStorage(prefix, { endpoint: config.s3Endpoint, bucket: config.neonBucket, region: config.s3Region });
  const rendered = {
    [names.root]: {
      "secrets.json": JSON.stringify(secrets, null, 2) + "\n",
      "auth_private_key.pem": privateKeyPem,
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.safekeeper]: {
      ...aws("neon"),
      REMOTE_STORAGE: storage("safekeeper"),
      // Safekeepers present this to each other when recovering WAL from a peer.
      safekeeper_peer_token: token("safekeeperdata"),
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.pageserver]: {
      ...aws("neon"),
      // What it presents to the safekeepers it streams WAL from.
      NEON_AUTH_TOKEN: token("safekeeperdata"),
      "identity.toml": `id=${PAGESERVER_ID}\n`,
      "metadata.json": pageserverMetadata(config.pageserverHost),
      "pageserver.toml": pageserverToml({
        brokerUrl: config.brokerUrl,
        controllerUrl: config.controllerUrl,
        token: token("generations_api"),
        publicKeyPath: `/data/.neon/${PUBLIC_KEY}`,
        storage: storage("pageserver"),
      }),
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.storageController]: {
      DATABASE_URL: `postgresql://storage_controller:${encodeURIComponent(secrets.controllerDbPassword)}@${config.controllerDbHost}:5432/storage_controller`,
      PAGESERVER_JWT_TOKEN: token("pageserverapi"),
      SAFEKEEPER_JWT_TOKEN: token("safekeeperdata"),
      CONTROL_PLANE_JWT_TOKEN: token("admin"),
      PEER_JWT_TOKEN: token("admin"),
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.controllerDb]: { POSTGRES_PASSWORD: secrets.controllerDbPassword },
    [names.s3Admin]: aws("admin"),
    [names.compute]: { NEON_CONTROL_PLANE_TOKEN: secrets.computeControlToken },
    [names.database]: {
      url: `postgresql://${ROLE}:${encodeURIComponent(secrets.alasioPassword)}@${config.computeHost}:55433/${DATABASE}`,
      password: secrets.alasioPassword,
      "lake-password": secrets.lakePassword,
    },
    [names.lake]: {
      LAKE_DATABASE_PASSWORD: secrets.lakePassword,
      LAKE_S3_KEY: s3("lake").accessKey,
      LAKE_S3_SECRET: s3("lake").secretKey,
    },
  };
  if (!config.external) {
    rendered[names.seaweedfs] = { "s3.json": JSON.stringify(s3Identities(secrets), null, 2) + "\n" };
  }
  return rendered;
}

/** The deployment's configuration, from the environment, checked. */
export function setupConfig(env = process.env) {
  const required = (key) => {
    const value = env[key]?.trim();
    if (!value) throw new Error(`${key} must be set`);
    return value;
  };
  const external = env.S3_EXTERNAL === "1";
  return {
    namespace: required("NAMESPACE"),
    prefix: required("SECRET_PREFIX"),
    brokerUrl: required("NEON_BROKER_URL"),
    controllerUrl: required("NEON_CONTROLLER_URL"),
    pageserverHost: required("NEON_PAGESERVER_HOST"),
    computeHost: required("NEON_COMPUTE_HOST"),
    controllerDbHost: required("NEON_CONTROLLER_DB_HOST"),
    s3Endpoint: required("S3_ENDPOINT"),
    s3Region: env.S3_REGION?.trim() || "us-east-1",
    neonBucket: env.S3_BUCKET_NEON?.trim() || "neon",
    external,
    ...(external ? { accessKey: required("S3_ACCESS_KEY"), secretKey: required("S3_SECRET_KEY") } : {}),
  };
}

const decode = (secret, key) => (secret?.data?.[key] ? Buffer.from(secret.data[key], "base64").toString("utf8") : null);

/**
 * Makes or completes the root Secret, then writes every service's Secret, replacing
 * what an earlier release wrote. `kube` is src/kube/client.js's client.
 */
export async function setupKube({ kube, config, log = console.log }) {
  const names = secretNames(config.prefix);
  const labels = { "app.kubernetes.io/managed-by": "alasio-neon-setup", "app.kubernetes.io/part-of": "alasio-neon" };
  const root = await kube.read("v1", "Secret", config.namespace, names.root);
  const keys = root
    ? { privateKeyPem: decode(root, "auth_private_key.pem"), publicKeyPem: decode(root, PUBLIC_KEY) }
    : generateKeyPair();
  if (!keys.privateKeyPem || !keys.publicKeyPem) throw new Error(`${names.root} is missing its keys`);
  const existing = JSON.parse(decode(root, "secrets.json") ?? "{}");
  const { secrets, changed } = completeSecrets(existing);
  log(root ? (changed ? "completing the stack's secrets" : "the stack's secrets are complete") : "making the stack's secrets");

  for (const [name, stringData] of Object.entries(renderSecrets({ secrets, ...keys, config }))) {
    const object = {
      apiVersion: "v1",
      kind: "Secret",
      metadata: { name, namespace: config.namespace, labels },
      type: "Opaque",
      stringData,
    };
    const current = name === names.root ? root : await kube.read("v1", "Secret", config.namespace, name);
    if (current) {
      // The root's secrets are only ever added to; everything else is rendered whole.
      await kube.replace({ ...object, metadata: { ...object.metadata, resourceVersion: current.metadata.resourceVersion } });
    } else {
      await kube.create(object);
    }
    log(`${current ? "updated" : "made"} ${name}`);
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { createKubeClient } = await import("../../src/kube/client.js");
  setupKube({ kube: createKubeClient(), config: setupConfig() }).catch((error) => {
    console.error(`setting up the stack failed: ${error.message}`);
    process.exit(1);
  });
}
