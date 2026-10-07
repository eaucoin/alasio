import { homedir } from "node:os";
import { join } from "node:path";

import { getDefaultHarness, type HarnessName } from "./harness/names.ts";
import { resolveHookPort } from "./shared/runtime-constants.ts";

/** alasio's configuration, as its environment sets it. */
export interface AlasioConfig {
  readonly telegramBotToken: string;
  readonly allowedUserIds: string;
  readonly workingDirectory: string | null;
  readonly workspaceRoot: string;
  readonly stateDir: string;
  /** Whether Codex's login is kept in alasio's store, as its Codex home does not outlast its pod. */
  readonly keepCodexLogin: boolean;
  readonly hookPort: number;
  readonly warmLinkedSessions: boolean;
  readonly defaultHarness: HarnessName | null;
}

/** How alasio talks to Codex: through `codex exec` or a Codex app-server. */
export type CodexTransportMode = "exec" | "app-server";

function requireEnv(key: string): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function loadAlasioConfig(env: NodeJS.ProcessEnv = process.env): AlasioConfig {
  return {
    telegramBotToken: requireEnv("TELEGRAM_BOT_TOKEN"),
    allowedUserIds: env["TELEGRAM_ALLOWED_USER_IDS"] ?? "",
    // Optional pre-mount for new conversations; unset means the operator picks a folder in Telegram.
    workingDirectory: env["WORKING_DIRECTORY"]?.trim() || null,
    // Every folder chosen or created from Telegram must live directly under this root.
    workspaceRoot: env["ALASIO_WORKSPACE_ROOT"]?.trim() || homedir(),
    // What alasio writes to disk as it runs (received files, the session filesystems'
    // Codex home), none of which outlasts it: its state is in Neon.
    stateDir: env["ALASIO_STATE_DIR"]?.trim() || join(homedir(), ".alasio"),
    keepCodexLogin: env["ALASIO_KEEP_CODEX_LOGIN"] === "1",
    hookPort: resolveHookPort(env),
    warmLinkedSessions: env["ALASIO_WARM_LINKED_SESSIONS"] === "1",
    defaultHarness: getDefaultHarness(env),
  };
}

export function getCodexTransportMode(): CodexTransportMode {
  return process.env["ALASIO_CODEX_TRANSPORT"] === "exec" ? "exec" : "app-server";
}

export function getCodexBinaryOverride(): string | null {
  return process.env["ALASIO_CODEX_BIN"] || null;
}

