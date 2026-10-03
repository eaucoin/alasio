// @ts-nocheck
import { homedir } from "node:os";
import { join } from "node:path";

import { getDefaultHarness } from "./harness/names.ts";
import { resolveHookPort } from "./shared/runtime-constants.ts";

export function requireEnv(key) {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

/**
 * Where alasio keeps its SQLite state. Defaults next to the pre-mounted working
 * directory for existing deployments, otherwise under the operator's home so a
 * second bot never shares a database with the first.
 */
export function resolveStateDir(env = process.env) {
  if (env.ALASIO_STATE_DIR?.trim()) {
    return env.ALASIO_STATE_DIR.trim();
  }
  if (env.WORKING_DIRECTORY?.trim()) {
    return join(env.WORKING_DIRECTORY.trim(), ".alasio");
  }
  return join(homedir(), ".alasio");
}

export function loadAlasioConfig(env = process.env) {
  const stateDir = resolveStateDir(env);
  return {
    telegramBotToken: requireEnv("TELEGRAM_BOT_TOKEN"),
    allowedUserIds: env.TELEGRAM_ALLOWED_USER_IDS ?? "",
    // Optional pre-mount for new conversations; unset means the operator picks a folder in Telegram.
    workingDirectory: env.WORKING_DIRECTORY?.trim() || null,
    // Every folder chosen or created from Telegram must live directly under this root.
    workspaceRoot: env.ALASIO_WORKSPACE_ROOT?.trim() || homedir(),
    stateDir,
    dbPath: join(stateDir, "alasio.sqlite"),
    hookPort: resolveHookPort(env),
    warmLinkedSessions: env.ALASIO_WARM_LINKED_SESSIONS === "1",
    defaultHarness: getDefaultHarness(env),
  };
}

export function getCodexTransportMode() {
  return process.env.ALASIO_CODEX_TRANSPORT === "exec" ? "exec" : "app-server";
}

export function getCodexBinaryOverride() {
  return process.env.ALASIO_CODEX_BIN || null;
}

export function readPositiveIntEnv(key, fallback, env = process.env) {
  const raw = env[key];
  if (!raw?.trim()) {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
