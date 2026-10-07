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
  /**
   * The branch environment this alasio is (`alasio branch create`), on a copy of another
   * alasio's data, its parent's; null for main.
   */
  readonly branch: BranchEnvironment | null;
  /** The key branch environments' tokens are signed with, where alasio forks its sessions for them (src/branch/fork.ts); null for none. */
  readonly branchForkKeyFile: string | null;
}

/** A branch environment: its name, and where it asks its parent to fork the sessions it inherited, with the token it is given. */
export interface BranchEnvironment {
  readonly name: string;
  readonly parentForks: string;
  readonly tokenFile: string;
}

/** How alasio talks to Codex: through `codex exec` or a Codex app-server. */
export type CodexTransportMode = "exec" | "app-server";

function requireEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key];
  if (!value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function loadAlasioConfig(env: NodeJS.ProcessEnv = process.env): AlasioConfig {
  return {
    telegramBotToken: requireEnv(env, "TELEGRAM_BOT_TOKEN"),
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
    branch: env["ALASIO_BRANCH"]?.trim()
      ? { name: env["ALASIO_BRANCH"].trim(), parentForks: requireEnv(env, "ALASIO_PARENT_FORKS_URL"), tokenFile: requireEnv(env, "ALASIO_BRANCH_FORK_TOKEN_FILE") }
      : null,
    branchForkKeyFile: env["ALASIO_BRANCH_FORK_KEY_FILE"]?.trim() || null,
  };
}

export function getCodexTransportMode(): CodexTransportMode {
  return process.env["ALASIO_CODEX_TRANSPORT"] === "exec" ? "exec" : "app-server";
}

export function getCodexBinaryOverride(): string | null {
  return process.env["ALASIO_CODEX_BIN"] || null;
}

