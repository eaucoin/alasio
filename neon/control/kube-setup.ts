/**
 * Prepares alasio's stack on Kubernetes, as a Job that runs before it starts: the secrets
 * it runs on, made once and kept in one Secret, and what each service is given from them,
 * as a Secret of its own, rendered from them on every install and upgrade: the log's, the
 * webhook intake's, Neon's, the object store's beside it, workspace storage's and the node
 * agents'. Idempotent, and never rotating.
 *
 * Environment: NAMESPACE, SECRET_PREFIX (the installation's name, which every Secret's
 * name begins with); NEON_BROKER_URL, NEON_CONTROLLER_URL, NEON_PROXY_HOST and
 * NEON_CONTROLLER_DB_HOST; S3_ENDPOINT and S3_REGION, and
 * S3_BUCKET_NEON, S3_BUCKET_LAKE and S3_BUCKET_BACKUPS (neon, lake and backups unless set); S3_EXTERNAL=1 with S3_ACCESS_KEY and S3_SECRET_KEY
 * when the object store is the operator's own rather than the bundled SeaweedFS, whose
 * identities, and its filers' store's users' passwords, are made here otherwise, and
 * S3_SNAPSHOTS_ACCESS_KEY and S3_SNAPSHOTS_SECRET_KEY, the store's key
 * for the snapshots bucket alone, which nothing but it may write: the setup refuses a main
 * key that can (refuseMainKeyOnSnapshots). WORKSPACES_NAME (workspace storage's JuiceFS
 * file system's), WORKSPACES_BUCKET and WORKSPACES_BUCKET_URL (its bucket's name
 * and URL), WORKSPACES_TRASH_DAYS, and VALKEY_ADDRESS (its metadata engine's host:port).
 * SNAPSHOTS_BUCKET (the bucket session sandboxes' snapshots are kept in). WEBHOOK_ADDRESS, the address
 * Telegram delivers to, which the webhook intake's certificate is made for
 * (./certificate.ts).
 */
import type { KubernetesObject, V1ObjectMeta, V1Secret } from "@kubernetes/client-node";
import { Cause, Effect, Exit } from "effect";

import { environmentKey } from "../../src/binder/names.ts";
import { MAIN_ENVIRONMENT_NAME } from "../../src/branch/names.ts";
import { makeS3Bucket, type S3Bucket, S3Error } from "../../src/node-agent/s3.ts";

import { BRANCHES_SCOPE, computeId, MAIN, PROXY_SCOPE } from "./branches.ts";
import { type Certificate, webhookCertificate } from "./certificate.ts";
import { generateKeyPair, type KeyPair, signToken } from "./jwt.ts";
import {
  completeSecrets,
  DATABASE,
  GRAFANA_ROLE,
  HARNESS_ROLE,
  LAKE_READER_ROLE,
  LAKE_ROLE,
  LOG_PRINCIPALS,
  type LogPrincipal,
  pageserverToml,
  remoteStorage,
  ROLE,
  s3Identities,
  type S3Credentials,
  type S3Identity,
  type StackSecrets,
  type StoredSecrets,
  WORKSPACE_QUOTAS_USER,
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
  filerStoreFiler: string;
  filerStoreCollector: string;
  filerStoreBackup: string;
  s3Admin: string;
  s3Backups: string;
  compute: string;
  proxy: string;
  database: string;
  /** The harness host's database URL, as its own role (HARNESS_ROLE), for the harness pools to mount. */
  harnessDatabase: string;
  lake: string;
  grafana: string;
  branches: string;
  valkey: string;
  workspaces: string;
  workspaceQuotas: string;
  snapshots: string;
  nodeAgent: string;
  sandboxGateway: string;
  sessionsCaller: string;
  /** The log's own (its cluster's id and its admin's password), its access applier's (every principal's password), and each principal's. */
  log: string;
  logAccess: string;
  logPrincipals: Record<LogPrincipal, string>;
  /** The webhook intake's certificate and key. */
  webhookTls: string;
}

/** Workspace storage, as the setup makes its Secrets. */
export interface WorkspacesConfig {
  name: string;
  bucket: string;
  bucketUrl: string;
  trashDays: string;
  valkeyAddress: string;
}

/** The deployment's configuration, from the environment the module comment lists. */
export type SetupConfig = SetupConfigBase & (
  | { external: false }
  | { external: true; accessKey: string; secretKey: string; snapshots: S3Credentials }
);

/** Where Neon's services are, as its Secrets are made for. */
export interface NeonHosts {
  brokerUrl: string;
  controllerUrl: string;
  /** Where every client reaches Neon's proxy, from any namespace. */
  proxyHost: string;
  controllerDbHost: string;
}

interface SetupConfigBase {
  namespace: string;
  prefix: string;
  neon: NeonHosts;
  s3Endpoint: string;
  s3Region: string;
  neonBucket: string;
  lakeBucket: string;
  backupsBucket: string;
  workspaces: WorkspacesConfig;
  snapshotsBucket: string;
  /** The address Telegram delivers to, which the webhook intake's certificate is made for. */
  webhookAddress: string;
}

/** What renderSecrets renders from. */
export interface RenderSecretsOptions extends KeyPair {
  secrets: StackSecrets;
  config: SetupConfig;
  /** The webhook intake's certificate, which setupKube keeps or makes anew (./certificate.ts). */
  webhookTls: Certificate;
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

/**
 * The URL alasio's database is reached at, as `role` (alasio's own unless given) with
 * `password`, through Neon's proxy at `host`, on the compute `endpoint`: named by the libpq
 * option the proxy reads, as no TLS carries a name in the cluster.
 */
export function databaseUrl(host: string, password: string, endpoint: string, role: string = ROLE): string {
  const url = new URL(`postgresql://${host}:5432/${DATABASE}`);
  url.username = role;
  url.password = password;
  url.searchParams.set("options", `endpoint=${endpoint}`);
  return url.toString();
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
    filerStoreFiler: `${prefix}-seaweedfs-filer-store-filer`,
    filerStoreCollector: `${prefix}-seaweedfs-filer-store-collector`,
    filerStoreBackup: `${prefix}-seaweedfs-filer-store-backup`,
    s3Admin: `${prefix}-neon-s3-admin`,
    s3Backups: `${prefix}-neon-s3-backups`,
    compute: `${prefix}-neon-compute`,
    proxy: `${prefix}-neon-proxy`,
    database: `${prefix}-database`,
    harnessDatabase: `${prefix}-harness-database`,
    lake: `${prefix}-lake`,
    grafana: `${prefix}-grafana`,
    branches: `${prefix}-branches`,
    valkey: `${prefix}-valkey`,
    workspaces: `${prefix}-workspaces-juicefs`,
    workspaceQuotas: `${prefix}-workspaces-quotas`,
    snapshots: `${prefix}-snapshots`,
    nodeAgent: `${prefix}-node-agent`,
    sandboxGateway: `${prefix}-sandbox-gateway`,
    sessionsCaller: `${prefix}-sessions-caller`,
    log: `${prefix}-log`,
    logAccess: `${prefix}-log-access`,
    logPrincipals: Object.fromEntries(LOG_PRINCIPALS.map((principal) => [principal, `${prefix}-log-${principal}`])) as Record<LogPrincipal, string>,
    webhookTls: `${prefix}-webhook-tls`,
  };
}

/**
 * What each service is given, by Secret name, from the stack's `secrets` and keys and
 * the deployment's `config` (the environment above, as an object). Pure, for tests.
 */
export function renderSecrets({ secrets, privateKeyPem, publicKeyPem, config, webhookTls }: RenderSecretsOptions): RenderedSecrets {
  const names = secretNames(config.prefix);
  const token = (scope: string, tenantId?: string) => signToken(privateKeyPem, scope, tenantId);
  // The snapshots bucket is reached as an identity of its own, of an external store's too:
  // what writes it writes what the node agents restore as root.
  const s3 = (identity: S3Identity): S3Credentials => {
    if (!config.external) return secrets.s3[identity];
    if (identity === "snapshots") return config.snapshots;
    return { accessKey: config.accessKey, secretKey: config.secretKey };
  };
  const aws = (identity: S3Identity) => ({ AWS_ACCESS_KEY_ID: s3(identity).accessKey, AWS_SECRET_ACCESS_KEY: s3(identity).secretKey });
  const storage = (prefix: string) => remoteStorage(prefix, { endpoint: config.s3Endpoint, bucket: config.neonBucket, region: config.s3Region });
  /** Neon's services', the lake's, Grafana's, branch environments' and the object store's admin's. */
  const neonSecrets = (neon: NeonHosts): RenderedSecrets => ({
    [names.s3Admin]: aws("admin"),
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
      "pageserver.toml": pageserverToml({
        brokerUrl: neon.brokerUrl,
        controllerUrl: neon.controllerUrl,
        token: token("generations_api"),
        publicKeyPath: `/data/.neon/${PUBLIC_KEY}`,
        storage: storage("pageserver"),
      }),
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.storageController]: {
      DATABASE_URL: `postgresql://storage_controller:${encodeURIComponent(secrets.controllerDbPassword)}@${neon.controllerDbHost}:5432/storage_controller`,
      PAGESERVER_JWT_TOKEN: token("pageserverapi"),
      SAFEKEEPER_JWT_TOKEN: token("safekeeperdata"),
      CONTROL_PLANE_JWT_TOKEN: token("admin"),
      PEER_JWT_TOKEN: token("admin"),
      [PUBLIC_KEY]: publicKeyPem,
    },
    [names.controllerDb]: { POSTGRES_PASSWORD: secrets.controllerDbPassword },
    [names.s3Backups]: aws("backups"),
    [names.compute]: { NEON_CONTROL_PLANE_TOKEN: secrets.computeControlToken },
    [names.proxy]: { NEON_PROXY_TO_CONTROLPLANE_TOKEN: token(PROXY_SCOPE) },
    [names.database]: {
      // Main's compute, through the proxy, which names it by the option.
      url: databaseUrl(neon.proxyHost, secrets.alasioPassword, computeId(MAIN)),
      password: secrets.alasioPassword,
      // What alasio sets the roles it makes to: the verifiers neon-control answers for them.
      "lake-verifier": secrets.verifiers[LAKE_ROLE],
      "lake-reader-verifier": secrets.verifiers[LAKE_READER_ROLE],
      "grafana-verifier": secrets.verifiers[GRAFANA_ROLE],
      "harness-verifier": secrets.verifiers[HARNESS_ROLE],
    },
    // The harness host's, which reaches main's compute as its own role, and no further than that role may.
    [names.harnessDatabase]: { url: databaseUrl(neon.proxyHost, secrets.harnessPassword, computeId(MAIN), HARNESS_ROLE) },
    // The lake service's, and its query endpoint's, which each read their own.
    [names.lake]: {
      LAKE_DATABASE_PASSWORD: secrets.lakePassword,
      LAKE_S3_KEY: s3("lake").accessKey,
      LAKE_S3_SECRET: s3("lake").secretKey,
      LAKE_READER_PASSWORD: secrets.lakeReaderPassword,
      LAKE_READER_S3_KEY: s3("lakeReader").accessKey,
      LAKE_READER_S3_SECRET: s3("lakeReader").secretKey,
      LAKE_QUERY_TOKEN: secrets.lakeQueryToken,
      // What it asks neon-control which branches there are with, which can do nothing else.
      LAKE_BRANCHES_TOKEN: token(BRANCHES_SCOPE),
    },
    [names.grafana]: {
      GF_DATABASE_PASSWORD: secrets.grafanaPassword,
      GF_SECURITY_ADMIN_PASSWORD: secrets.grafanaAdminPassword,
      GF_SECURITY_SECRET_KEY: secrets.grafanaSecretKey,
      LAKE_QUERY_TOKEN: secrets.lakeQueryToken,
    },
    // Branch environments': the token alasio's command line manages Neon's branches with,
    // and the key alasio signs each one's token to fork its sessions with.
    [names.branches]: {
      "control-token": token("admin"),
      "fork-key": secrets.branchForkKey,
    },
  });
  const rendered: RenderedSecrets = {
    [names.root]: {
      "secrets.json": JSON.stringify(secrets, null, 2) + "\n",
      "auth_private_key.pem": privateKeyPem,
      [PUBLIC_KEY]: publicKeyPem,
    },
    // The log's, which its nodes are formatted and run with, and which its access applier
    // logs in as its admin with; that applier's, every principal's password, which it gives
    // each principal; and each principal's, which what logs in as it is given (KEDA's with
    // the SASL mechanism, as its Kafka scaler reads it).
    [names.log]: {
      KAFKA_CLUSTER_ID: secrets.logClusterId,
      LOG_ADMIN_PASSWORD: secrets.log.admin,
    },
    [names.logAccess]: Object.fromEntries(LOG_PRINCIPALS.map((principal) => [principal, secrets.log[principal]])),
    ...Object.fromEntries(LOG_PRINCIPALS.map((principal) => [names.logPrincipals[principal], {
      username: principal,
      password: secrets.log[principal],
      ...(principal === "scaler" ? { sasl: "scram_sha512" } : {}),
    }])),
    // The webhook intake's: its certificate, which Telegram is given too, and its key.
    [names.webhookTls]: { "tls.crt": webhookTls.cert, "tls.key": webhookTls.key },
  };
  const { neon, workspaces } = config;
  Object.assign(rendered, neonSecrets(neon));
  rendered[names.valkey] = { password: secrets.valkeyPassword };
  // The file system's credentials, as alasio's JuiceFS clients read them (the nodes'
  // mount and the admin jobs); the format options are applied as it is first made.
  rendered[names.workspaces] = {
    name: workspaces.name,
    // Its database 1, leaving the others free for a restore to be loaded into.
    metaurl: `redis://:${encodeURIComponent(secrets.valkeyPassword)}@${workspaces.valkeyAddress}/1`,
    storage: "s3",
    bucket: workspaces.bucketUrl,
    "access-key": s3("workspaces").accessKey,
    "secret-key": s3("workspaces").secretKey,
    "format-options": `trash-days=${workspaces.trashDays}`,
  };
  // The node agents', which set workspaces' quotas on its metadata as a Valkey user of their
  // own, of no command Valkey calls dangerous but what JuiceFS asks, and no key of the object store.
  rendered[names.workspaceQuotas] = {
    password: secrets.valkeyQuotasPassword,
    metaurl: `redis://${WORKSPACE_QUOTAS_USER}:${encodeURIComponent(secrets.valkeyQuotasPassword)}@${workspaces.valkeyAddress}/1`,
  };
  const { snapshotsBucket } = config;
  // The node agents': where they keep session sandboxes' snapshots, and as whom, which
  // they take whole as their environment.
  rendered[names.snapshots] = {
    S3_ENDPOINT: config.s3Endpoint,
    S3_REGION: config.s3Region,
    S3_BUCKET: snapshotsBucket,
    ...aws("snapshots"),
  };
  // The token alasio presents to the node agents, which they check: both read it here.
  rendered[names.nodeAgent] = { token: secrets.nodeAgentToken };
  // The sandbox gateway's keys, beside the node agents it attaches workspaces through.
  rendered[names.sandboxGateway] = { "caller-key": secrets.gatewayCallerKey, "binding-key": secrets.gatewayBindingKey };
  // main's alasio's key to it, its own of the gateway's caller key, which signs main's sessions' tokens alone; a branch's is `alasio branch create`'s to write.
  rendered[names.sessionsCaller] = { "caller-key": environmentKey(Buffer.from(secrets.gatewayCallerKey), MAIN_ENVIRONMENT_NAME).toString() };
  if (!config.external) {
    const buckets = { neon: config.neonBucket, lake: config.lakeBucket, backups: config.backupsBucket, workspaces: workspaces.bucket, snapshots: snapshotsBucket };
    rendered[names.seaweedfs] = { "s3.json": JSON.stringify(s3Identities(secrets, buckets), null, 2) + "\n" };
    // The filers' store's users', which its Valkey requires: the filers', each filer is
    // given, the collector's, which reads its metrics, and the backups'.
    rendered[names.filerStoreFiler] = { password: secrets.filerStoreFilerPassword };
    rendered[names.filerStoreCollector] = { password: secrets.filerStoreCollectorPassword };
    rendered[names.filerStoreBackup] = { password: secrets.filerStoreBackupPassword };
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
    neon: {
      brokerUrl: required("NEON_BROKER_URL"),
      controllerUrl: required("NEON_CONTROLLER_URL"),
      proxyHost: required("NEON_PROXY_HOST"),
      controllerDbHost: required("NEON_CONTROLLER_DB_HOST"),
    },
    s3Endpoint: required("S3_ENDPOINT"),
    s3Region: env["S3_REGION"]?.trim() || "us-east-1",
    neonBucket: env["S3_BUCKET_NEON"]?.trim() || "neon",
    lakeBucket: env["S3_BUCKET_LAKE"]?.trim() || "lake",
    backupsBucket: env["S3_BUCKET_BACKUPS"]?.trim() || "backups",
    webhookAddress: required("WEBHOOK_ADDRESS"),
    workspaces: {
      name: required("WORKSPACES_NAME"),
      bucket: required("WORKSPACES_BUCKET"),
      bucketUrl: required("WORKSPACES_BUCKET_URL"),
      trashDays: required("WORKSPACES_TRASH_DAYS"),
      valkeyAddress: required("VALKEY_ADDRESS"),
    },
    snapshotsBucket: required("SNAPSHOTS_BUCKET"),
  };
  return external
    ? {
      ...base,
      external,
      accessKey: required("S3_ACCESS_KEY"),
      secretKey: required("S3_SECRET_KEY"),
      snapshots: { accessKey: required("S3_SNAPSHOTS_ACCESS_KEY"), secretKey: required("S3_SNAPSHOTS_SECRET_KEY") },
    }
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

  // The webhook intake's certificate: kept while it serves its address, made anew near its end.
  const tls = await readSecret(names.webhookTls);
  const current = decode(tls, "tls.crt") && decode(tls, "tls.key") ? { cert: decode(tls, "tls.crt") ?? "", key: decode(tls, "tls.key") ?? "" } : null;
  const { certificate: webhookTls, made } = webhookCertificate(config.webhookAddress, current);
  if (made) log(`making the webhook intake's certificate for ${config.webhookAddress}`);

  for (const [name, stringData] of Object.entries(renderSecrets({ secrets, privateKeyPem, publicKeyPem, config, webhookTls }))) {
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

/** What an external store's main key is refused for, should it write the snapshots bucket. */
const MAIN_KEY_WRITES_SNAPSHOTS =
  "the object store's main key (S3_ACCESS_KEY) can write the snapshots bucket, whose images the node agents restore as root: " +
  "give it no right to that bucket, which the snapshots' own key alone may write";

/**
 * Refuses an external store's main key that can write the snapshots bucket, `bucket` as that
 * key reaches it: whatever may write it writes the images the node agents restore as root.
 * It writes an object there: refused (403), the key cannot; written, the object is deleted,
 * and the setup fails, saying so, as it does when it cannot tell.
 */
export async function refuseMainKeyOnSnapshots(bucket: S3Bucket): Promise<void> {
  const probe = ".alasio-setup-probe";
  const written = await Effect.runPromiseExit(bucket.putJson(probe, { written: "by alasio's setup, to see whether the main key can" }));
  if (Exit.isSuccess(written)) {
    await Effect.runPromise(bucket.remove(probe)).catch(() => undefined);
    throw new Error(MAIN_KEY_WRITES_SNAPSHOTS);
  }
  const error = Cause.squash(written.cause);
  const reason = error instanceof S3Error ? error.reason : String(error);
  if (!reason.startsWith("403")) throw new Error(`whether the object store's main key can write the snapshots bucket could not be told: ${reason}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { KubeClient } = await import("../../src/kube/client.ts");
  // The job is one call after another, each run as the promise setupKube awaits.
  const kube = Effect.runSync(Effect.provide(KubeClient, KubeClient.layer));
  const client: SetupKubeClient = {
    read: (apiVersion, kind, namespace, name) => Effect.runPromise(kube.read(apiVersion, kind, namespace, name)),
    create: (object) => Effect.runPromise(kube.create(object)),
    replace: (object) => Effect.runPromise(kube.replace(object)),
  };
  const config = setupConfig();
  const refused = config.external
    ? refuseMainKeyOnSnapshots(makeS3Bucket({ endpoint: config.s3Endpoint, region: config.s3Region, bucket: config.snapshotsBucket, accessKeyId: config.accessKey, secretAccessKey: config.secretKey }))
    : Promise.resolve();
  refused.then(() => setupKube({ kube: client, config })).catch((error: unknown) => {
    console.error(`setting up the stack failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  });
}
