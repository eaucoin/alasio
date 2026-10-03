/**
 * Claude Code runtime adapter for alasio turns.
 *
 * Each mounted session is served by one long-lived Claude Code process fed
 * through streaming input (see live-sessions.ts), the way the Codex adapter
 * keeps app-server threads warm. A Telegram prompt is pushed into that
 * process and its turn ends on the result that names it; tool activity is
 * projected into the same persisted response blocks, and the SDK `result` is
 * the only operator-visible final answer. Background work Claude Code starts
 * keeps running between prompts, and the turns it starts on its own when that
 * work settles are delivered as their own replies.
 */
import { randomUUID } from "node:crypto";
import { query, type McpServerConfig, type Options, type SessionStore } from "@anthropic-ai/claude-agent-sdk";
import type { BaymaEndpoint } from "../../kube/sandboxes.ts";
import type { ModelChoice } from "../../persistence/conversation-repository.ts";
import type { TurnParams, TurnResult } from "../index.ts";
import type { ClaudeLiveSessions } from "./live-sessions.ts";
import type { ClaudeSessionApi } from "./sessions.ts";
import { appendBlock, isVisibleCodexItem, mapItemToBlocks } from "../../codex/event-projection.ts";
import { createTurnTimer } from "../../codex/turn-timing.ts";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  createCommandEventPolicy,
} from "../../codex/command-event-policy.ts";
import { isBlockedDbCommand } from "../../policy/db-guardrail.ts";
import { createLogger } from "../../shared/log.ts";
import { buildClaudeEnv } from "./env.ts";
import {
  cacheReadTokensFromUsage,
  projectAssistantMessageToItems,
  projectResultMessage,
} from "./event-projection.ts";
import { claudeMcpServers } from "./mcp.ts";
import { sessionFsQueryOptions } from "./sessionfs.ts";
import { REPLY_INSTRUCTIONS } from "../reply-instructions.ts";
import { getClaudeBinaryOverride, getClaudeEffort, getClaudeModel } from "./model.ts";
import {
  buildClaudeUserMessage,
  createPromptChannel,
  instrumentPromptChannel,
  promptUuidsAnsweredBy,
} from "./prompt-channel.ts";
import { mirrorOnly } from "./session-store.ts";

const log = createLogger("claude-runtime");

/** Starts a Claude Code query: the SDK's `query`, or a test's stand-in. */
export type ClaudeQueryFactory = typeof query;

/** What buildClaudeQueryOptions is given. */
export interface ClaudeQueryOptionsInput {
  readonly workingDirectory: string;
  readonly claudeEnv: NonNullable<Options["env"]>;
  /** None for a session filesystem, whose one server comes with its confinement. */
  readonly mcpServers?: Record<string, McpServerConfig> | undefined;
  readonly resumeSession?: string | null | undefined;
  readonly resumeExists?: boolean | undefined;
  readonly controller: AbortController;
  readonly hooks: NonNullable<Options["hooks"]>;
  readonly env?: Readonly<NodeJS.ProcessEnv>;
  readonly modelChoice?: ModelChoice | null;
  readonly sessionStore?: SessionStore | null;
  readonly sessionFsBayma?: BaymaEndpoint | null;
}

/** A turn as the adapter hands it on, with the live sessions that run it. */
export interface ClaudeTurnRequest extends TurnParams {
  readonly sessions?: ClaudeSessionApi;
  readonly liveSessions: ClaudeLiveSessions;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isIntentionalTurnInterrupt(reason: unknown): boolean {
  const message = typeof reason === "string" ? reason : getErrorMessage(reason);
  return message === "Interrupted from Telegram" || message === "Telegram swerve";
}

export function startFreshClaudeSession({ threadKey }: { readonly threadKey: string }): string {
  const sessionId = randomUUID();
  log.info(`new_session.reserved thread_key=${JSON.stringify(threadKey)} session=${JSON.stringify(sessionId.slice(0, 8))}`);
  return sessionId;
}

/**
 * Built-in tools removed from the model's context entirely. Shell and file
 * search go through bayma, which alasio always mounts: its sessions persist,
 * and a Bun shell there does everything these did.
 */
export const CLAUDE_DISALLOWED_TOOLS = Object.freeze(["Bash", "Monitor", "Grep", "Glob"]);

/** The bayma tool that runs code (and so shell commands) in a session. */
export const BAYMA_EXEC_TOOL = "mcp__bayma__exec";

export function buildClaudeQueryOptions({
  workingDirectory,
  claudeEnv,
  mcpServers,
  resumeSession,
  resumeExists,
  controller,
  hooks,
  env = process.env,
  modelChoice = null,
  sessionStore = null,
  sessionFsBayma = null,
}: ClaudeQueryOptionsInput): Options {
  const options: Options = {
    cwd: workingDirectory,
    env: claudeEnv,
    abortController: controller,
    permissionMode: "bypassPermissions",
    allowDangerouslySkipPermissions: true,
    systemPrompt: { type: "preset", preset: "claude_code", append: REPLY_INSTRUCTIONS },
    includePartialMessages: false,
    persistSession: true,
    disallowedTools: [...CLAUDE_DISALLOWED_TOOLS],
    hooks,
  };
  if (mcpServers !== undefined) {
    options.mcpServers = mcpServers;
  }
  // A session filesystem's CLI keeps only the tools that stay off this machine, and
  // reaches its workspace through bayma alone (sessionfs.ts).
  if (sessionFsBayma) {
    delete options.disallowedTools;
    Object.assign(options, sessionFsQueryOptions(sessionFsBayma));
  }
  // Every transcript write is mirrored to the store as it is written, not at
  // the end of a turn, which can run for hours; a resume still runs from the
  // local transcript in the real Claude home (see session-store.ts).
  if (sessionStore) {
    options.sessionStore = mirrorOnly(sessionStore);
    options.sessionStoreFlush = "eager";
  }
  if (resumeSession) {
    if (resumeExists) {
      options.resume = resumeSession;
    } else {
      options.sessionId = resumeSession;
    }
  }
  const model = getClaudeModel(env, modelChoice);
  if (model) {
    options.model = model;
  }
  const effort = getClaudeEffort(env, modelChoice);
  if (effort) {
    options.effort = effort;
  }
  const binary = getClaudeBinaryOverride(env);
  if (binary) {
    options.pathToClaudeCodeExecutable = binary;
  }
  return options;
}

/** How long a turn waits for a steered prompt the CLI has not answered once its own prompt is answered. */
export const UNANSWERED_PROMPT_GRACE_MS = 15_000;

export function isOperatorInterrupt(reason: unknown): boolean {
  return isIntentionalTurnInterrupt(reason);
}

export {
  appendBlock,
  buildClaudeEnv,
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  cacheReadTokensFromUsage,
  claudeMcpServers,
  createCommandEventPolicy,
  createTurnTimer,
  getErrorMessage,
  isBlockedDbCommand,
  isVisibleCodexItem,
  mapItemToBlocks,
  projectAssistantMessageToItems,
  projectResultMessage,
  promptUuidsAnsweredBy,
  buildClaudeUserMessage,
  createPromptChannel,
  instrumentPromptChannel,
  query as defaultQueryFactory,
};

/**
 * Run one operator prompt on the conversation's live Claude Code process,
 * starting or replacing that process when the mounted session, folder or
 * model differs from the one it serves.
 */
export async function executeClaudeTurn(params: ClaudeTurnRequest): Promise<TurnResult> {
  const liveSessions = params.liveSessions;
  if (!liveSessions) {
    throw new Error("executeClaudeTurn requires the adapter's live session registry");
  }
  return await liveSessions.runTurn(params);
}
