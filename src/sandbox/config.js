/**
 * Configuration for session filesystems, read from the environment. The feature is off
 * unless `ALASIO_SANDBOX_ENABLED=1`, so a deployment without the Valkey, the S3 bucket,
 * and the built images behaves exactly as before. `loadSandboxConfig`
 * returns null when off, and throws with the missing key named when on but incomplete,
 * so a half-configured deployment fails loudly at startup rather than mid-session.
 */
import { readFileSync } from "node:fs";

function required(env, key) {
  const value = env[key]?.trim();
  if (!value) throw new Error(`session filesystems are enabled but ${key} is not set`);
  return value;
}

/** Reads a secret from a file when `<key>_FILE` is set, else from `<key>`. */
function secret(env, key) {
  const file = env[`${key}_FILE`]?.trim();
  if (file) return readFileSync(file, "utf8").trim();
  return required(env, key);
}

export function loadSandboxConfig(env = process.env) {
  if (env.ALASIO_SANDBOX_ENABLED !== "1") return null;
  const int = (key, fallback) => {
    const raw = env[key]?.trim();
    if (!raw) return fallback;
    const n = Number.parseInt(raw, 10);
    if (!Number.isFinite(n) || n <= 0) throw new Error(`${key} must be a positive integer, got ${JSON.stringify(raw)}`);
    return n;
  };
  return {
    metadata: {
      url: required(env, "ALASIO_SANDBOX_METADATA_URL"),
      databases: int("ALASIO_SANDBOX_METADATA_DATABASES", 4096),
      passwordFile: required(env, "ALASIO_SANDBOX_METADATA_PASSWORD_FILE"),
    },
    s3: {
      endpoint: required(env, "ALASIO_SANDBOX_S3_ENDPOINT"),
      bucket: required(env, "ALASIO_SANDBOX_S3_BUCKET"),
      accessKey: secret(env, "ALASIO_SANDBOX_S3_KEY"),
      secretKey: secret(env, "ALASIO_SANDBOX_S3_SECRET"),
    },
    host: {
      network: required(env, "ALASIO_SANDBOX_NETWORK"),
      agentImage: env.ALASIO_SANDBOX_AGENT_IMAGE?.trim() || "alasio/agent",
      sessionHostImage: env.ALASIO_SANDBOX_SESSION_HOST_IMAGE?.trim() || "alasio/session-host",
      memoryMb: int("ALASIO_SANDBOX_MEMORY_MB", 2048),
      cpus: int("ALASIO_SANDBOX_CPUS", 2),
      pidsLimit: int("ALASIO_SANDBOX_PIDS", 512),
      cacheMb: int("ALASIO_SANDBOX_CACHE_MB", 1024),
      hostPublicIp: env.ALASIO_SANDBOX_HOST_PUBLIC_IP?.trim() || null,
    },
  };
}
