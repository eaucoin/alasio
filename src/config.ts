import { homedir } from "node:os";
import { join } from "node:path";

import { MAIN_ENVIRONMENT_NAME } from "./branch/names.ts";
import type { EventsLog } from "./events/events.ts";
import { eventsGroup, eventsTopic } from "./events/names.ts";
import { ALASIO_GATEWAY_CALLER_KEY_VARIABLE, ALASIO_GATEWAY_ENVIRONMENT_VARIABLE, ALASIO_GATEWAY_FOLDERS_VARIABLE, ALASIO_GATEWAY_URL_VARIABLE } from "./gateway/names.ts";
import { getDefaultHarness, type HarnessName } from "./harness/names.ts";
import { type WakeLog, wakeLogOf } from "./neon/lake-wake.ts";
import type { GatewayWorkspacesOptions } from "./sandbox/gateway-client.ts";
import type { TopicLog } from "./log/topic.ts";
import { type TelegramLog, telegramLogOf } from "./telegram/update-consumer.ts";

/** alasio's configuration, as its environment sets it. */
export interface AlasioConfig {
  readonly telegramBotToken: string;
  readonly allowedUserIds: string;
  /** Where the bot's updates are: its environment's topic in the log, which alasio consumes. */
  readonly telegramLog: TelegramLog;
  /** The log alasio's environment's events and its branches' forks are on: its brokers, and alasio's principal on them. */
  readonly log: EventsLog;
  /** Its environment's events topic on the log, and the group it reads it in (src/events/consume.ts). */
  readonly events: TopicLog;
  /** Where alasio tells the lake's loader it wrote transcripts: its environment's wake topic in the log; null where the lake does not run. */
  readonly lakeWake: WakeLog | null;
  readonly workingDirectory: string | null;
  readonly workspaceRoot: string;
  readonly stateDir: string;
  /** Whether Codex's login is kept in alasio's store, as its Codex home does not outlast its pod. */
  readonly keepCodexLogin: boolean;
  readonly defaultHarness: HarnessName | null;
  /**
   * The branch environment this alasio is (`alasio branch create`), on a copy of another
   * alasio's data, its parent's; null for main.
   */
  readonly branch: BranchEnvironment | null;
  /** The key branch environments' tokens are signed with, where alasio forks its sessions for them (src/branch/fork.ts); null for none. */
  readonly branchForkKeyFile: string | null;
  /**
   * The sandbox gateway alasio's conversations' harnesses are bound through, each in a pod of
   * its harness's pool (src/harness/pooled-host.ts), as are its session filesystems
   * (src/sandbox/index.ts): its address, alasio's environment's name and its own key to it.
   */
  readonly sessionsGateway: GatewayWorkspacesOptions;
  /** Whether the sandbox gateway serves folder workspaces' instances (src/gateway/folders.ts), as the deployment renders the host profile. */
  readonly folders: boolean;
  /** Where alasio's harness pools' pods reach its environment's relay of its Codex login (src/codex/relay.ts). */
  readonly codexLoginUrl: string;
  /**
   * alasio's own pod's name, as its Deployment gives it (ALASIO_POD), which it holds its
   * conversations' claims as (src/codex/worker-claims.ts); without it, alasio does not start.
   */
  readonly pod: string;
  /** Whether alasio runs in a pod of Kubernetes, which it holds busy while a turn runs (src/kube/busy.ts): one loadAlasioConfig reads always does, a test's not. */
  readonly inKubernetes: boolean;
}

/** A branch environment: its name, and the token it is given to ask its parent to fork the sessions it inherited with (src/branch/fork.ts). */
export interface BranchEnvironment {
  readonly name: string;
  readonly tokenFile: string;
}

/**
 * alasio's own variables: its configuration and its secrets, as its deployment gives them
 * (cli/src/manifests/alasio.ts alasioEnv), the bot's token and the sandbox gateway's caller key
 * among them, which would let whatever a harness runs speak as alasio or mint a caller's token
 * of any key of its environment. None is a harness's to have (withoutAlasio).
 */
const ALASIO_VARIABLE = /^(?:ALASIO_|TELEGRAM_)|^WORKING_DIRECTORY$/u;

/** An environment without alasio's own variables (ALASIO_VARIABLE): what a harness's process, and all it runs, is given of alasio's. */
export function withoutAlasio(env: Readonly<NodeJS.ProcessEnv>): NodeJS.ProcessEnv {
  return Object.fromEntries(Object.entries(env).filter(([name]) => !ALASIO_VARIABLE.test(name)));
}

/** The variable `key` of `env`, failing where it is unset or blank. */
function requireEnv(env: NodeJS.ProcessEnv, key: string): string {
  const value = env[key];
  if (!value?.trim()) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

export function loadAlasioConfig(env: NodeJS.ProcessEnv = process.env): AlasioConfig {
  const log: EventsLog = { brokers: requireEnv(env, "ALASIO_LOG_BROKERS").split(","), username: requireEnv(env, "ALASIO_LOG_USERNAME"), passwordFile: requireEnv(env, "ALASIO_LOG_PASSWORD_FILE") };
  const branch = env["ALASIO_BRANCH"]?.trim() || null;
  const environment = branch ?? MAIN_ENVIRONMENT_NAME;
  return {
    telegramBotToken: requireEnv(env, "TELEGRAM_BOT_TOKEN"),
    allowedUserIds: env["TELEGRAM_ALLOWED_USER_IDS"] ?? "",
    telegramLog: telegramLogOf(env),
    log,
    events: { ...log, topic: eventsTopic(environment), group: eventsGroup(environment) },
    lakeWake: wakeLogOf(env),
    // Optional pre-mount for new conversations; unset means the operator picks a folder in Telegram.
    workingDirectory: env["WORKING_DIRECTORY"]?.trim() || null,
    // Every folder chosen or created from Telegram must live directly under this root.
    workspaceRoot: env["ALASIO_WORKSPACE_ROOT"]?.trim() || homedir(),
    // What alasio writes to disk as it runs (received files, the session filesystems'
    // Codex home), none of which outlasts it: its state is in Neon.
    stateDir: env["ALASIO_STATE_DIR"]?.trim() || join(homedir(), ".alasio"),
    keepCodexLogin: env["ALASIO_KEEP_CODEX_LOGIN"] === "1",
    defaultHarness: getDefaultHarness(env),
    branch: branch ? { name: branch, tokenFile: requireEnv(env, "ALASIO_BRANCH_FORK_TOKEN_FILE") } : null,
    branchForkKeyFile: env["ALASIO_BRANCH_FORK_KEY_FILE"]?.trim() || null,
    sessionsGateway: {
      url: requireEnv(env, ALASIO_GATEWAY_URL_VARIABLE).trim(),
      environment: requireEnv(env, ALASIO_GATEWAY_ENVIRONMENT_VARIABLE),
      callerKey: Buffer.from(requireEnv(env, ALASIO_GATEWAY_CALLER_KEY_VARIABLE).trim()),
    },
    folders: env[ALASIO_GATEWAY_FOLDERS_VARIABLE] === "1",
    codexLoginUrl: requireEnv(env, "ALASIO_CODEX_LOGIN_URL"),
    pod: requireEnv(env, "ALASIO_POD").trim(),
    inKubernetes: true,
  };
}

export function getCodexBinaryOverride(): string | null {
  return process.env["ALASIO_CODEX_BIN"] || null;
}

