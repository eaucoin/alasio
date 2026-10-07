/**
 * The lake's configuration, from its environment, which its Deployment gives it
 * (cli/src/manifests/lake.ts). Every setting has a stated meaning and those
 * without a sensible default are required, so a misconfigured lake fails at once
 * rather than mid-cycle.
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
 */

/** A Postgres database the lake connects to, as role `lake`. */
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

export interface LakeConfig {
  /** alasio's database, which the lake loads from. */
  source: DatabaseConfig;
  /** DuckLake's catalog. */
  catalog: DatabaseConfig;
  dataPath: string;
  s3: S3Config | null;
  intervalMs: number;
  maintenanceIntervalMs: number;
  retentionDays: number;
  memoryLimit: string;
  threads: number;
  extensionDirectory: string | null;
  httpPort: number;
  intakePort: number;
}

const ROLE = "lake";

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

export function loadConfig(env: NodeJS.ProcessEnv = process.env): LakeConfig {
  const database = (name: string): DatabaseConfig => ({
    host: required(env, "LAKE_DATABASE_HOST"),
    port: positive(env, "LAKE_DATABASE_PORT", 5432),
    user: ROLE,
    password: required(env, "LAKE_DATABASE_PASSWORD"),
    database: name,
  });
  const dataPath = required(env, "LAKE_DATA_PATH");
  return {
    source: database(optional(env, "LAKE_SOURCE_DATABASE", "alasio")),
    catalog: database(optional(env, "LAKE_CATALOG_DATABASE", "lake")),
    dataPath,
    s3: dataPath.startsWith("s3://")
      ? {
          endpoint: required(env, "LAKE_S3_ENDPOINT"),
          key: required(env, "LAKE_S3_KEY"),
          secret: required(env, "LAKE_S3_SECRET"),
        }
      : null,
    intervalMs: positive(env, "LAKE_INTERVAL_SECONDS", 300) * 1000,
    maintenanceIntervalMs: positive(env, "LAKE_MAINTENANCE_HOURS", 24) * 3_600_000,
    retentionDays: positive(env, "LAKE_RETENTION_DAYS", 30),
    memoryLimit: optional(env, "LAKE_MEMORY_LIMIT", "1GB"),
    threads: positive(env, "LAKE_THREADS", 2),
    extensionDirectory: env["LAKE_EXTENSION_DIRECTORY"]?.trim() || null,
    httpPort: positive(env, "LAKE_HTTP_PORT", 9464),
    intakePort: positive(env, "LAKE_INTAKE_PORT", 4318),
  };
}
