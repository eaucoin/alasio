/**
 * Prepares alasio's Neon on Kubernetes, as a Helm hook Job before the stack starts: the
 * secrets it runs on, made once and kept in one Secret, and what each service is given
 * from them, as a Secret of its own, rendered from them on every install and upgrade.
 * Idempotent, and never rotating.
 *
 * Environment: NAMESPACE, SECRET_PREFIX (the release's full name, which every Secret's
 * name begins with); NEON_BROKER_URL, NEON_CONTROLLER_URL, NEON_PAGESERVER_HOST,
 * NEON_COMPUTE_HOST, NEON_CONTROLLER_DB_HOST; S3_ENDPOINT and S3_REGION, and
 * S3_BUCKET_NEON and S3_BUCKET_LAKE (neon and lake unless set); S3_EXTERNAL=1 with S3_ACCESS_KEY and S3_SECRET_KEY
 * when the object store is the operator's own rather than the bundled SeaweedFS, whose
 * identities are made here otherwise.
 */
import type { KubernetesObject, V1ObjectMeta, V1Secret } from "@kubernetes/client-node";

import { generateKeyPair, type KeyPair, signToken } from "./jwt.ts";
import {
  completeSecrets,
  DATABASE,
  pageserverMetadata,
  pageserverToml,
  PAGESERVER_ID,
  remoteStorage,
  ROLE,
  s3Identities,
  type S3Credentials,
  type S3Identity,
  type StackSecrets,
  type StoredSecrets,
} from "./secrets.ts";

const PUBLIC_KEY = "auth_public_key.pem";

/** Every Secret's name, by what it is for. */
export interface SecretNames {
  root: string;
  safekeeper: string;
  pageserver: string;
  storageController: string;
  controllerDb: string;
  seaweedfs: string;
  s3Admin: string;
  compute: string;
  database: string;
  lake: string;
}

/** The deployment's configuration, from the environment the module comment lists. */
export type SetupConfig = SetupConfigBase & (
  | { external: false }
  | { external: true; accessKey: string; secretKey: string }
);

interface SetupConfigBase {
  namespace: string;
  prefix: string;
  brokerUrl: string;
  controllerUrl: string;
  pageserverHost: string;
  computeHost: string;
  controllerDbHost: string;
  s3Endpoint: string;
  s3Region: string;
  neonBucket: string;
  lakeBucket: string;
}

/** What renderSecrets renders from. */
export interface RenderSecretsOptions extends KeyPair {
  secrets: StackSecrets;
  config: SetupConfig;
}

/** Each Secret's string data, by the Secret's name. */
export type RenderedSecrets = Record<string, Record<string, string>>;

/** A Secret as the API server returns it, which always has its metadata and version. */
export type StoredSecret = V1Secret & { metadata: V1ObjectMeta & { resourceVersion: string } };

/** What setupKube does through src/kube/client.ts's client: reads Secrets, and writes them without reading back what it wrote. */
export interface SetupKubeClient {
  read(apiVersion: string, kind: string, namespace: string, name: string): Promise<KubernetesObject | null>;
  create(object: V1Secret): Promise<unknown>;
  replace(object: V1Secret): Promise<unknown>;
}

export interface SetupKubeOptions {
  kube: SetupKubeClient;
  config: SetupConfig;
  log?: (message: string) => void;
}

/** Every Secret's name, by what it is for. */
export function secretNames(prefix: string): SecretNames {
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
export function renderSecrets({ secrets, privateKeyPem, publicKeyPem, config }: RenderSecretsOptions): RenderedSecrets {
  const names = secretNames(config.prefix);
  const token = (scope: string, tenantId?: string) => signToken(privateKeyPem, scope, tenantId);
  const s3 = (identity: S3Identity): S3Credentials => (config.external
    ? { accessKey: config.accessKey, secretKey: config.secretKey }
    : secrets.s3[identity]);
  const aws = (identity: S3Identity) => ({ AWS_ACCESS_KEY_ID: s3(identity).accessKey, AWS_SECRET_ACCESS_KEY: s3(identity).secretKey });
  const storage = (prefix: string) => remoteStorage(prefix, { endpoint: config.s3Endpoint, bucket: config.neonBucket, region: config.s3Region });
  const rendered: RenderedSecrets = {
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
    rendered[names.seaweedfs] = { "s3.json": JSON.stringify(s3Identities(secrets, { neon: config.neonBucket, lake: config.lakeBucket }), null, 2) + "\n" };
  }
  return rendered;
}

/** The deployment's configuration, from the environment, checked. */
export function setupConfig(env: NodeJS.ProcessEnv = process.env): SetupConfig {
  const required = (key: string) => {
    const value = env[key]?.trim();
    if (!value) throw new Error(`${key} must be set`);
    return value;
  };
  const external = env["S3_EXTERNAL"] === "1";
  const base: SetupConfigBase = {
    namespace: required("NAMESPACE"),
    prefix: required("SECRET_PREFIX"),
    brokerUrl: required("NEON_BROKER_URL"),
    controllerUrl: required("NEON_CONTROLLER_URL"),
    pageserverHost: required("NEON_PAGESERVER_HOST"),
    computeHost: required("NEON_COMPUTE_HOST"),
    controllerDbHost: required("NEON_CONTROLLER_DB_HOST"),
    s3Endpoint: required("S3_ENDPOINT"),
    s3Region: env["S3_REGION"]?.trim() || "us-east-1",
    neonBucket: env["S3_BUCKET_NEON"]?.trim() || "neon",
    lakeBucket: env["S3_BUCKET_LAKE"]?.trim() || "lake",
  };
  return external
    ? { ...base, external, accessKey: required("S3_ACCESS_KEY"), secretKey: required("S3_SECRET_KEY") }
    : { ...base, external };
}

const decode = (secret: StoredSecret | null, key: string) => {
  const value = secret?.data?.[key];
  return value ? Buffer.from(value, "base64").toString("utf8") : null;
};

/**
 * Makes or completes the root Secret, then writes every service's Secret, replacing
 * what an earlier release wrote. `kube` is src/kube/client.ts's client.
 */
export async function setupKube({ kube, config, log = console.log }: SetupKubeOptions): Promise<void> {
  const names = secretNames(config.prefix);
  const labels = { "app.kubernetes.io/managed-by": "alasio-neon-setup", "app.kubernetes.io/part-of": "alasio-neon" };
  // What the API server returns for a Secret is the Secret, as it stores it.
  const readSecret = async (name: string) => (await kube.read("v1", "Secret", config.namespace, name)) as StoredSecret | null;
  const root = await readSecret(names.root);
  const { privateKeyPem, publicKeyPem } = root
    ? { privateKeyPem: decode(root, "auth_private_key.pem"), publicKeyPem: decode(root, PUBLIC_KEY) }
    : generateKeyPair();
  if (!privateKeyPem || !publicKeyPem) throw new Error(`${names.root} is missing its keys`);
  // The secrets.json this job wrote, by this release or an earlier one.
  const existing: StoredSecrets = JSON.parse(decode(root, "secrets.json") ?? "{}");
  const { secrets, changed } = completeSecrets(existing);
  log(root ? (changed ? "completing the stack's secrets" : "the stack's secrets are complete") : "making the stack's secrets");

  for (const [name, stringData] of Object.entries(renderSecrets({ secrets, privateKeyPem, publicKeyPem, config }))) {
    const object = {
      apiVersion: "v1",
      kind: "Secret",
      metadata: { name, namespace: config.namespace, labels },
      type: "Opaque",
      stringData,
    } satisfies V1Secret;
    const current = name === names.root ? root : await readSecret(name);
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
  const { Effect } = await import("effect");
  const { KubeClient } = await import("../../src/kube/client.ts");
  // The job is one call after another, each run as the promise setupKube awaits.
  const kube = Effect.runSync(Effect.provide(KubeClient, KubeClient.layer));
  const client: SetupKubeClient = {
    read: (apiVersion, kind, namespace, name) => Effect.runPromise(kube.read(apiVersion, kind, namespace, name)),
    create: (object) => Effect.runPromise(kube.create(object)),
    replace: (object) => Effect.runPromise(kube.replace(object)),
  };
  setupKube({ kube: client, config: setupConfig() }).catch((error: unknown) => {
    console.error(`setting up the stack failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  });
}
