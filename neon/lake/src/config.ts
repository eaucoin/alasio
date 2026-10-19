/**
 * The lake's configuration, from its environment, which its Deployment gives it
 * (cli/src/manifests/lake.ts). Every setting has a stated meaning and those
 * without a sensible default are required, so a misconfigured lake fails at once
 * rather than mid-cycle.
 *
 * The loader (./service.ts), which loads the lake from alasio's database:
 *
 *   LAKE_DATABASE_HOST, LAKE_DATABASE_PORT  Neon's proxy, as the stack's network reaches it
 *   LAKE_DATABASE_ENDPOINT                  the compute it connects to through the proxy,
 *                                           which it names by `options=endpoint=`; none
 *                                           when unset, as straight to a Postgres
 *   LAKE_DATABASE_PASSWORD                  role `lake`'s: read-only on alasio's sources,
 *                                           owner of the `lake` database, the catalog
 *   LAKE_SOURCE_DATABASE                    alasio's database (default alasio)
 *   LAKE_SOURCE_ENDPOINT                    the compute the source is read on through the
 *                                           proxy, its branch's read replica; none when
 *                                           unset, as straight to a Postgres
 *   LAKE_CATALOG_DATABASE                   the catalog's database (default lake)
 *   LAKE_DATA_PATH                          where the Parquet files go: s3://lake/ on the stack
 *   LAKE_S3_ENDPOINT, LAKE_S3_KEY, LAKE_S3_SECRET  for an s3:// data path
 *   LAKE_MEMORY_LIMIT, LAKE_THREADS         DuckDB's (default 1GB, 2)
 *   LAKE_EXTENSION_DIRECTORY                the extensions the image installed; with it
 *                                           set, none is ever downloaded
 *   LAKE_HTTP_PORT                          health and Prometheus metrics (default 9464)
 *   LOG_BROKERS                             the log's brokers, comma-separated host:port
 *   LOG_USERNAME, LOG_PASSWORD              its principal on the log, `loader-<environment>`
 *   LAKE_WAKE_TOPIC, LAKE_WAKE_GROUP        the topic alasio says it wrote transcripts on,
 *                                           `lake.<environment>`, and the loader's group
 *
 * Maintenance (./maintain.ts), a process a pass, main's alone:
 *
 *   LAKE_DATABASE_*, LAKE_SOURCE_DATABASE, LAKE_SOURCE_ENDPOINT, LAKE_CATALOG_DATABASE,
 *   LAKE_DATA_PATH, LAKE_S3_*, LAKE_MEMORY_LIMIT, LAKE_THREADS, LAKE_EXTENSION_DIRECTORY
 *                                           as the loader's: the reconciliation loads from
 *                                           alasio's database, as the loader does
 *   LAKE_BRANCHES_URL, LAKE_BRANCHES_TOKEN  neon-control's list of Neon's branches, and the
 *                                           token it is read with: while it lists any but
 *                                           main, maintenance deletes no file (sync.ts)
 *   LAKE_RETENTION_DAYS                     the days of telemetry kept (default 30)
 *   LAKE_BRANCH                             the branch environment whose lake it is; unset
 *                                           for main's, whose telemetry a branch's is
 *   LAKE_STORE_FILER_URL, LAKE_STORE_MASTER_URL  the bundled object store's filer and masters,
 *                                           which the reconciliation asks whether a file the
 *                                           store fails a read of has its data still; unset
 *                                           for another store (reconcile.ts's StoreParts)
 *   LAKE_STORE_VOLUME_SERVERS               how many volume servers it runs, every one of
 *                                           which its masters are to know before their not
 *                                           knowing a volume says it is gone (default 1)
 *
 * The telemetry intake (./intake-service.ts), which writes the lake's `otel` schema alone:
 *
 *   LAKE_DATABASE_HOST, LAKE_DATABASE_PORT, LAKE_DATABASE_PASSWORD, LAKE_CATALOG_DATABASE,
 *   LAKE_DATA_PATH, LAKE_S3_*, LAKE_THREADS, LAKE_EXTENSION_DIRECTORY, LAKE_HTTP_PORT
 *                                           as above
 *   LOG_BROKERS                             the log's brokers, comma-separated host:port
 *   LOG_USERNAME, LOG_PASSWORD              its principal on the log, `intake`
 *   LOG_RETENTION_MS                        how long the log keeps the telemetry topics'
 *                                           records (log.retention), which it says beside
 *                                           how long they keep them in fact; none if unset
 *
 * The lake's query endpoint (./endpoint.ts), which only reads it, as its reader:
 *
 *   LAKE_DATABASE_HOST, LAKE_DATABASE_PORT, LAKE_DATABASE_ENDPOINT,
 *   LAKE_CATALOG_DATABASE, LAKE_DATA_PATH, LAKE_S3_ENDPOINT,
 *   LAKE_EXTENSION_DIRECTORY                     as above
 *   LAKE_READER_PASSWORD                    role `lake_reader`'s, which may only read the catalog
 *   LAKE_READER_S3_KEY, LAKE_READER_S3_SECRET  the object store's read-only identity's
 *   LAKE_QUERY_TOKEN                        the bearer token a query must carry
 *   LAKE_QUERY_PORT                         where it takes queries (default 8090)
 *   LAKE_QUERY_PEERS                        the name every reader resolves at, their
 *                                           headless Service's, whose queues /pool sums;
 *                                           unset, its own alone
 *   LAKE_MEMORY_LIMIT, LAKE_THREADS         its DuckDB's (default 256MB, 2)
 *
 * Their own telemetry is configured by OpenTelemetry's standard variables (./telemetry.ts).
 */

/** A Postgres database the lake connects to, as `user`. */
export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  /** The options each connection sends, as libpq's `options`: `endpoint=<id>` for Neon's proxy, or none (empty). */
  options: string;
}

/** The object store an s3:// data path is in. */
export interface S3Config {
  endpoint: string;
  key: string;
  secret: string;
}

/** neon-control's list of Neon's branches, and the bearer token it is read with. */
export interface BranchesSource {
  url: string;
  token: string;
}

/** What the lake is opened with: its catalog and data path, whom it is opened as there, and its DuckDB's settings. */
export interface LakeAccess {
  /** DuckLake's catalog. */
  catalog: DatabaseConfig;
  dataPath: string;
  s3: S3Config | null;
  memoryLimit: string;
  threads: number;
  extensionDirectory: string | null;
}

/** alasio's database, as the lake reads it: its name, and the options each connection sends, as DatabaseConfig's. */
export interface SourceConfig {
  database: string;
  options: string;
}

/** The log as a client reaches it: its brokers, and the principal it is reached as. */
export interface LogConfig {
  brokers: string[];
  username: string;
  password: string;
}

export interface LakeConfig extends LakeAccess {
  /** alasio's database, which the lake loads from: through the catalog's server, read as the catalog's role. */
  source: SourceConfig;
  httpPort: number;
  /** The log, and what the loader is woken by on it: its topic, and its group. */
  log: LogConfig;
  wake: { topic: string; group: string };
}

export interface MaintenanceConfig extends LakeAccess {
  /** alasio's database, which the reconciliation loads what the transcripts lost from, as the loader does. */
  source: SourceConfig;
  /** Where the lake learns of Neon's branches, which may read its files; null where it is not told. */
  branches: BranchesSource | null;
  retentionDays: number;
  /** The bundled object store's filer and masters, which say whether a file's data is gone; null for another store. */
  store: StoreParts | null;
  /** The branch environment whose lake this is; null for main's. */
  branch: string | null;
}

/** The bundled object store's filer and masters, by their HTTP addresses, and how many volume servers it runs. */
export interface StoreParts {
  filer: string;
  master: string;
  volumeServers: number;
}

export interface IntakeConfig extends LakeAccess {
  httpPort: number;
  log: LogConfig;
  /** How long the log keeps the telemetry topics' records, as log.retention says; null where it is not said. */
  logRetentionMs: number | null;
}

export interface EndpointConfig extends LakeAccess {
  token: string;
  port: number;
  /** The name every reader of the pool resolves at, as their headless Service; null for this one alone. */
  peers: string | null;
}

/** The roles the lake connects as: the lake service's, and the query endpoint's. */
export const ROLE = "lake";
export const READER_ROLE = "lake_reader";

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]?.trim();
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

function optional(env: NodeJS.ProcessEnv, name: string, fallback: string): string {
  return env[name]?.trim() || fallback;
}

function positive(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) throw new Error(`${name} must be a positive number, got ${JSON.stringify(raw)}`);
  return value;
}

/** The lake as `user` opens it, with the password and object store credentials of the variables `secrets` names. */
function access(
  env: NodeJS.ProcessEnv,
  user: string,
  secrets: { password: string; s3Key: string; s3Secret: string },
  memoryLimit: string,
): LakeAccess {
  const dataPath = required(env, "LAKE_DATA_PATH");
  return {
    catalog: {
      host: required(env, "LAKE_DATABASE_HOST"),
      port: positive(env, "LAKE_DATABASE_PORT", 5432),
      user,
      password: required(env, secrets.password),
      database: optional(env, "LAKE_CATALOG_DATABASE", "lake"),
      options: env["LAKE_DATABASE_ENDPOINT"]?.trim() ? `endpoint=${env["LAKE_DATABASE_ENDPOINT"].trim()}` : "",
    },
    dataPath,
    s3: dataPath.startsWith("s3://")
      ? { endpoint: required(env, "LAKE_S3_ENDPOINT"), key: required(env, secrets.s3Key), secret: required(env, secrets.s3Secret) }
      : null,
    memoryLimit: optional(env, "LAKE_MEMORY_LIMIT", memoryLimit),
    threads: positive(env, "LAKE_THREADS", 2),
    extensionDirectory: env["LAKE_EXTENSION_DIRECTORY"]?.trim() || null,
  };
}

/** The lake as its writers open it: as role `lake`, with the object store's identity of the lake's. */
const writerAccess = (env: NodeJS.ProcessEnv, memoryLimit: string): LakeAccess =>
  access(env, ROLE, { password: "LAKE_DATABASE_PASSWORD", s3Key: "LAKE_S3_KEY", s3Secret: "LAKE_S3_SECRET" }, memoryLimit);

/** The log, as LOG_BROKERS, LOG_USERNAME and LOG_PASSWORD say it. */
const logConfig = (env: NodeJS.ProcessEnv): LogConfig => ({
  brokers: required(env, "LOG_BROKERS").split(",").map((broker) => broker.trim()).filter(Boolean),
  username: required(env, "LOG_USERNAME"),
  password: required(env, "LOG_PASSWORD"),
});

/** alasio's database, as LAKE_SOURCE_DATABASE and LAKE_SOURCE_ENDPOINT say it. */
const sourceConfig = (env: NodeJS.ProcessEnv): SourceConfig => ({
  database: optional(env, "LAKE_SOURCE_DATABASE", "alasio"),
  options: env["LAKE_SOURCE_ENDPOINT"]?.trim() ? `endpoint=${env["LAKE_SOURCE_ENDPOINT"].trim()}` : "",
});

/** The loader's configuration. */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): LakeConfig {
  return {
    ...writerAccess(env, "1GB"),
    source: sourceConfig(env),
    httpPort: positive(env, "LAKE_HTTP_PORT", 9464),
    log: logConfig(env),
    wake: { topic: required(env, "LAKE_WAKE_TOPIC"), group: required(env, "LAKE_WAKE_GROUP") },
  };
}

/** Maintenance's configuration. */
export function loadMaintenanceConfig(env: NodeJS.ProcessEnv = process.env): MaintenanceConfig {
  const branchesUrl = env["LAKE_BRANCHES_URL"]?.trim();
  return {
    ...writerAccess(env, "1GB"),
    source: sourceConfig(env),
    branches: branchesUrl ? { url: branchesUrl, token: required(env, "LAKE_BRANCHES_TOKEN") } : null,
    retentionDays: positive(env, "LAKE_RETENTION_DAYS", 30),
    store: storeParts(env),
    branch: env["LAKE_BRANCH"]?.trim() || null,
  };
}

/** The bundled object store's parts, where both are given; neither, for another store; one alone is a mistake. */
function storeParts(env: NodeJS.ProcessEnv): StoreParts | null {
  const filer = env["LAKE_STORE_FILER_URL"]?.trim();
  const master = env["LAKE_STORE_MASTER_URL"]?.trim();
  if (!filer && !master) return null;
  return { filer: required(env, "LAKE_STORE_FILER_URL"), master: required(env, "LAKE_STORE_MASTER_URL"), volumeServers: positive(env, "LAKE_STORE_VOLUME_SERVERS", 1) };
}

/** The telemetry intake's configuration. */
export function loadIntakeConfig(env: NodeJS.ProcessEnv = process.env): IntakeConfig {
  return {
    ...writerAccess(env, "256MB"),
    httpPort: positive(env, "LAKE_HTTP_PORT", 9464),
    log: logConfig(env),
    logRetentionMs: env["LOG_RETENTION_MS"]?.trim() ? positive(env, "LOG_RETENTION_MS", 0) : null,
  };
}

/** The query endpoint's configuration. */
export function loadEndpointConfig(env: NodeJS.ProcessEnv = process.env): EndpointConfig {
  return {
    ...access(env, READER_ROLE, { password: "LAKE_READER_PASSWORD", s3Key: "LAKE_READER_S3_KEY", s3Secret: "LAKE_READER_S3_SECRET" }, "256MB"),
    token: required(env, "LAKE_QUERY_TOKEN"),
    port: positive(env, "LAKE_QUERY_PORT", 8090),
    peers: env["LAKE_QUERY_PEERS"]?.trim() || null,
  };
}
