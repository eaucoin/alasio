/**
 * Configuration for session filesystems, read from the environment. The feature is off
 * unless `ALASIO_SANDBOX_ENABLED=1`, so a deployment without the Valkey, the S3 bucket,
 * the built images, and a model-login token behaves exactly as before. `loadSandboxConfig`
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
    gateway: {
      ip: required(env, "ALASIO_SANDBOX_GATEWAY_IP"),
      port: int("ALASIO_SANDBOX_GATEWAY_PORT", 8080),
      // The upstreams and login tokens. A missing token leaves the gateway live but
      // answering 503 until the operator supplies one (gateway.js); it is not required.
      anthropicUpstream: env.ALASIO_SANDBOX_ANTHROPIC_UPSTREAM?.trim() || "https://api.anthropic.com",
      anthropicTokenFile: env.ALASIO_SANDBOX_ANTHROPIC_TOKEN_FILE?.trim() || null,
      // A subscription login instead of an API key: the local `claude` credentials JSON,
      // read live so its refreshes are picked up and injected as a Bearer (gateway.js).
      anthropicOAuthFile: env.ALASIO_SANDBOX_ANTHROPIC_OAUTH_FILE?.trim() || null,
      openaiUpstream: env.ALASIO_SANDBOX_OPENAI_UPSTREAM?.trim() || "https://api.openai.com",
      openaiTokenFile: env.ALASIO_SANDBOX_OPENAI_TOKEN_FILE?.trim() || null,
    },
  };
}
