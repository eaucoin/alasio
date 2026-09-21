export function requireEnv(key) {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function loadAlasioConfig() {
  return {
    telegramBotToken: requireEnv("TELEGRAM_BOT_TOKEN"),
    allowedUserIds: process.env.TELEGRAM_ALLOWED_USER_IDS ?? "",
    workingDirectory: process.env.WORKING_DIRECTORY ?? "/home/operator/monorepo",
    warmLinkedSessions: process.env.ALASIO_WARM_LINKED_SESSIONS === "1",
  };
}

export function getCodexTransportMode() {
  return process.env.ALASIO_CODEX_TRANSPORT === "exec" ? "exec" : "app-server";
}

export function getCodexBinaryOverride() {
  return process.env.ALASIO_CODEX_BIN || null;
}

export function readPositiveIntEnv(key, fallback) {
  const raw = process.env[key];
  if (!raw?.trim()) {
    return fallback;
  }
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}
