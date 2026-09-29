/**
 * Claude Code runtime adapter for alasio turns.
 *
 * Each mounted session is served by one long-lived Claude Code process fed
 * through streaming input (see live-sessions.js), the way the Codex adapter
 * keeps app-server threads warm. A Telegram prompt is pushed into that
 * process and its turn ends on the result that names it; tool activity is
 * projected into the same persisted response blocks, and the SDK `result` is
 * the only operator-visible final answer. Background work Claude Code starts
 * keeps running between prompts, and the turns it starts on its own when that
 * work settles are delivered as their own replies.
 */
import { randomUUID } from "node:crypto";
import { CLAUDE_HARNESS } from "../names.js";
import { query } from "@anthropic-ai/claude-agent-sdk";
import { appendBlock, isVisibleCodexItem, mapItemToBlocks } from "../../codex/event-projection.js";
import { createTurnTimer } from "../../codex/turn-timing.js";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  createCommandEventPolicy,
} from "../../codex/command-event-policy.js";
import { isBlockedDbCommand } from "../../policy/db-guardrail.js";
import { ensureBaymaReady } from "../../mcp/bayma.js";
import { createLogger } from "../../shared/log.js";
import { buildClaudeEnv } from "./env.js";
import {
  cacheReadTokensFromUsage,
  projectAssistantMessageToItems,
  projectResultMessage,
} from "./event-projection.js";
import { claudeMcpServers } from "./mcp.js";
import { getClaudeBinaryOverride, getClaudeEffort, getClaudeModel } from "./model.js";
import {
  buildClaudeUserMessage,
  createPromptChannel,
  instrumentPromptChannel,
  promptUuidsAnsweredBy,
} from "./prompt-channel.js";
import { mirrorOnly } from "./session-store.js";

const log = createLogger("claude-runtime");

function getErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function isIntentionalTurnInterrupt(reason) {
  const message = typeof reason === "string" ? reason : getErrorMessage(reason);
  return message === "Interrupted from Telegram" || message === "Telegram swerve";
}

export function startFreshClaudeSession({ threadKey }) {
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
  sandboxSession = null,
}) {
  const options = {
    // A session filesystem runs the CLI inside its gVisor sandbox, where the working
    // directory is /workspace and the process is spawned through the sandbox (E5).
    cwd: sandboxSession ? "/workspace" : workingDirectory,
    env: claudeEnv,
    abortController: controller,
    permissionMode: "bypassPermissions",
    allowDangerouslySkipPermissions: true,
    systemPrompt: { type: "preset", preset: "claude_code" },
    includePartialMessages: false,
    persistSession: true,
    disallowedTools: [...CLAUDE_DISALLOWED_TOOLS],
    hooks,
    mcpServers,
  };
  if (sandboxSession) {
    options.spawnClaudeCodeProcess = sandboxSession.spawn;
  }
  // Every transcript write is mirrored to the store as it is written, not at
  // the end of a turn, which can run for hours; a resume still runs from the
  // local transcript in the real Claude home (see session-store.js).
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

export function isOperatorInterrupt(reason) {
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
  ensureBaymaReady,
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
export async function executeClaudeTurn(params) {
  const liveSessions = params.liveSessions;
  if (!liveSessions) {
    throw new Error("executeClaudeTurn requires the adapter's live session registry");
  }
  return await liveSessions.runTurn(params);
}
