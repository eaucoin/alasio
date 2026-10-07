/**
 * The lake's configuration, from its environment, which its Deployment gives it
 * (cli/src/manifests/lake.ts). Every setting has a stated meaning and those
 * without a sensible default are required, so a misconfigured lake fails at once
 * rather than mid-cycle.
 *
 * The lake service (./service.ts), which writes the lake:
 *
 *   LAKE_DATABASE_HOST, LAKE_DATABASE_PORT  the compute, as the stack's network reaches it
 *   LAKE_DATABASE_PASSWORD                  role `lake`'s: read-only on alasio's sources,
 *                                           owner of the `lake` database, the catalog
 *   LAKE_SOURCE_DATABASE                    alasio's database (default alasio)
 *   LAKE_CATALOG_DATABASE                   the catalog's database (default lake)
 *   LAKE_DATA_PATH                          where the Parquet files go: s3://lake/ on the stack
 *   LAKE_S3_ENDPOINT, LAKE_S3_KEY, LAKE_S3_SECRET  for an s3:// data path
 *   LAKE_INTERVAL_SECONDS                   between loads (default 300)
 *   LAKE_MAINTENANCE_HOURS                  between maintenance passes (default 24)
 *   LAKE_RETENTION_DAYS                     the days of telemetry kept (default 30)
 *   LAKE_MEMORY_LIMIT, LAKE_THREADS         DuckDB's (default 1GB, 2)
 *   LAKE_EXTENSION_DIRECTORY                the extensions the image installed; with it
 *                                           set, none is ever downloaded
 *   LAKE_HTTP_PORT                          health and Prometheus metrics (default 9464)
 *   LAKE_INTAKE_PORT                        the telemetry intake, OTLP over HTTP (default 4318)
 *
 * The lake's query endpoint (./endpoint.ts), which only reads it, as its reader:
 *
 *   LAKE_DATABASE_HOST, LAKE_DATABASE_PORT, LAKE_CATALOG_DATABASE, LAKE_DATA_PATH,
 *   LAKE_S3_ENDPOINT, LAKE_EXTENSION_DIRECTORY   as above
 *   LAKE_READER_PASSWORD                    role `lake_reader`'s, which may only read the catalog
 *   LAKE_READER_S3_KEY, LAKE_READER_S3_SECRET  the object store's read-only identity's
 *   LAKE_QUERY_TOKEN                        the bearer token a query must carry
 *   LAKE_QUERY_PORT                         where it takes queries (default 8090)
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
}

/** The object store an s3:// data path is in. */
export interface S3Config {
  endpoint: string;
  key: string;
  secret: string;
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

export interface LakeConfig extends LakeAccess {
  /** alasio's database, which the lake loads from. */
  source: DatabaseConfig;
  intervalMs: number;
  maintenanceIntervalMs: number;
  retentionDays: number;
  httpPort: number;
  intakePort: number;
}

export interface EndpointConfig extends LakeAccess {
  token: string;
  port: number;
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

/** The lake service's configuration. */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): LakeConfig {
  const lake = access(env, ROLE, { password: "LAKE_DATABASE_PASSWORD", s3Key: "LAKE_S3_KEY", s3Secret: "LAKE_S3_SECRET" }, "1GB");
  return {
    ...lake,
    source: { ...lake.catalog, database: optional(env, "LAKE_SOURCE_DATABASE", "alasio") },
    intervalMs: positive(env, "LAKE_INTERVAL_SECONDS", 300) * 1000,
    maintenanceIntervalMs: positive(env, "LAKE_MAINTENANCE_HOURS", 24) * 3_600_000,
    retentionDays: positive(env, "LAKE_RETENTION_DAYS", 30),
    httpPort: positive(env, "LAKE_HTTP_PORT", 9464),
    intakePort: positive(env, "LAKE_INTAKE_PORT", 4318),
  };
}

/** The query endpoint's configuration. */
export function loadEndpointConfig(env: NodeJS.ProcessEnv = process.env): EndpointConfig {
  return {
    ...access(env, READER_ROLE, { password: "LAKE_READER_PASSWORD", s3Key: "LAKE_READER_S3_KEY", s3Secret: "LAKE_READER_S3_SECRET" }, "256MB"),
    token: required(env, "LAKE_QUERY_TOKEN"),
    port: positive(env, "LAKE_QUERY_PORT", 8090),
  };
}
