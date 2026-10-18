import { homedir } from "node:os";
import { join } from "node:path";

import { ALASIO_GATEWAY_CALLER_KEY_VARIABLE, ALASIO_GATEWAY_ENVIRONMENT_VARIABLE, ALASIO_GATEWAY_URL_VARIABLE } from "./gateway/names.ts";
import { getDefaultHarness, type HarnessName } from "./harness/names.ts";
import { type WakeLog, wakeLogOf } from "./neon/lake-wake.ts";
import type { GatewayWorkspacesOptions } from "./sandbox/gateway-client.ts";
import { type TelegramLog, telegramLogOf } from "./telegram/update-consumer.ts";

/** alasio's configuration, as its environment sets it. */
export interface AlasioConfig {
  readonly telegramBotToken: string;
  readonly allowedUserIds: string;
  /** Where the bot's updates are: its environment's topic in the log, which alasio consumes. */
  readonly telegramLog: TelegramLog;
  /** Where alasio tells the lake's loader it wrote transcripts: its environment's wake topic in the log; null where the lake does not run. */
  readonly lakeWake: WakeLog | null;
  readonly workingDirectory: string | null;
  readonly workspaceRoot: string;
  readonly stateDir: string;
  /** Whether Codex's login is kept in alasio's store, as its Codex home does not outlast its pod. */
  readonly keepCodexLogin: boolean;
  readonly warmLinkedSessions: boolean;
  readonly defaultHarness: HarnessName | null;
  /**
   * The branch environment this alasio is (`alasio branch create`), on a copy of another
   * alasio's data, its parent's; null for main.
   */
  readonly branch: BranchEnvironment | null;
  /** The key branch environments' tokens are signed with, where alasio forks its sessions for them (src/branch/fork.ts); null for none. */
  readonly branchForkKeyFile: string | null;
  /**
   * The sandbox gateway alasio's session filesystems are reached through (src/sandbox/index.ts):
   * its address, alasio's environment's name and its own key to it; null where the deployment
   * gives none, and offers no session filesystems.
   */
  readonly sessionsGateway: GatewayWorkspacesOptions | null;
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
    telegramLog: telegramLogOf(env),
    lakeWake: wakeLogOf(env),
    // Optional pre-mount for new conversations; unset means the operator picks a folder in Telegram.
    workingDirectory: env["WORKING_DIRECTORY"]?.trim() || null,
    // Every folder chosen or created from Telegram must live directly under this root.
    workspaceRoot: env["ALASIO_WORKSPACE_ROOT"]?.trim() || homedir(),
    // What alasio writes to disk as it runs (received files, the session filesystems'
    // Codex home), none of which outlasts it: its state is in Neon.
    stateDir: env["ALASIO_STATE_DIR"]?.trim() || join(homedir(), ".alasio"),
    keepCodexLogin: env["ALASIO_KEEP_CODEX_LOGIN"] === "1",
    warmLinkedSessions: env["ALASIO_WARM_LINKED_SESSIONS"] === "1",
    defaultHarness: getDefaultHarness(env),
    branch: env["ALASIO_BRANCH"]?.trim()
      ? { name: env["ALASIO_BRANCH"].trim(), parentForks: requireEnv(env, "ALASIO_PARENT_FORKS_URL"), tokenFile: requireEnv(env, "ALASIO_BRANCH_FORK_TOKEN_FILE") }
      : null,
    branchForkKeyFile: env["ALASIO_BRANCH_FORK_KEY_FILE"]?.trim() || null,
    sessionsGateway: env[ALASIO_GATEWAY_URL_VARIABLE]?.trim()
      ? {
        url: env[ALASIO_GATEWAY_URL_VARIABLE].trim(),
        environment: requireEnv(env, ALASIO_GATEWAY_ENVIRONMENT_VARIABLE),
        callerKey: Buffer.from(requireEnv(env, ALASIO_GATEWAY_CALLER_KEY_VARIABLE).trim()),
      }
      : null,
  };
}

export function getCodexTransportMode(): CodexTransportMode {
  return process.env["ALASIO_CODEX_TRANSPORT"] === "exec" ? "exec" : "app-server";
}

export function getCodexBinaryOverride(): string | null {
  return process.env["ALASIO_CODEX_BIN"] || null;
}

