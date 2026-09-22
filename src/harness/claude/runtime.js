/**
 * Claude Code runtime adapter for alasio turns.
 *
 * Mirrors `codex/runtime.js`: one Telegram prompt becomes one Claude Agent SDK
 * query over the mounted session, tool activity is projected into the same
 * persisted response blocks, and the SDK `result` is the only operator-visible
 * final answer.
 */
import { randomUUID } from "node:crypto";
import { query } from "@anthropic-ai/claude-agent-sdk";
import { appendBlock, isVisibleCodexItem, mapItemToBlocks } from "../../codex/event-projection.js";
import { createTurnTimer } from "../../codex/turn-timing.js";
import {
  buildDbGuardrailFallbackText,
  buildDbGuardrailSyntheticText,
  createCommandEventPolicy,
} from "../../codex/command-event-policy.js";
import { isBlockedDbCommand } from "../../policy/db-guardrail.js";
import { buildCodexConfig, getMcpServerCount } from "../../mcp/server-config.js";
import { preflightConfiguredMcpServers } from "../../mcp/preflight.js";
import { createLogger } from "../../shared/log.js";
import { buildClaudeEnv } from "./env.js";
import {
  cacheReadTokensFromUsage,
  projectAssistantMessageToItems,
  projectResultMessage,
} from "./event-projection.js";
import { toClaudeMcpServers } from "./mcp.js";
import { getClaudeBinaryOverride, getClaudeEffort, getClaudeModel } from "./model.js";
import {
  buildClaudeUserMessage,
  createPromptChannel,
  instrumentPromptChannel,
} from "./prompt-channel.js";

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

export function buildClaudeQueryOptions({
  workingDirectory,
  claudeEnv,
  mcpServers,
  resumeSession,
  resumeExists,
  controller,
  hooks,
  env = process.env,
}) {
  const options = {
    cwd: workingDirectory,
    env: claudeEnv,
    abortController: controller,
    permissionMode: "bypassPermissions",
    allowDangerouslySkipPermissions: true,
    systemPrompt: { type: "preset", preset: "claude_code" },
    includePartialMessages: false,
    persistSession: true,
    hooks,
  };
  if (mcpServers && Object.keys(mcpServers).length > 0) {
    options.mcpServers = mcpServers;
  }
  if (resumeSession) {
    if (resumeExists) {
      options.resume = resumeSession;
    } else {
      options.sessionId = resumeSession;
    }
  }
  const model = getClaudeModel(env);
  if (model) {
    options.model = model;
  }
  const effort = getClaudeEffort(env);
  if (effort) {
    options.effort = effort;
  }
  const binary = getClaudeBinaryOverride(env);
  if (binary) {
    options.pathToClaudeCodeExecutable = binary;
  }
  return options;
}

export async function executeClaudeTurn(params) {
  const {
    prompt,
    resumeSession,
    threadKey,
    chatId,
    messageId,
    workingDirectory,
    persistence,
    activeQueries,
    onStarted,
    sessions,
  } = params;
  const queryFactory = params.queryFactory ?? query;
  const turnTimer = createTurnTimer({ threadKey, resumeSession, prompt, log });
  log.info(`Querying Claude Code (resume=${resumeSession})`);
  turnTimer("query.start");
  onStarted?.();
  const blockSequence = [];
  let sessionId = resumeSession ?? null;
  let interrupted = false;
  let responseCompleted = false;
  const pendingResponseId = persistence.createPendingResponse(chatId, messageId, resumeSession);
  persistence.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
  const controller = new AbortController();
  const channel = instrumentPromptChannel(createPromptChannel(), { threadKey, log });
  // The operator prompt is queued before any await so early steering cannot precede it.
  channel.push(buildClaudeUserMessage(prompt));
  let resolveFinished;
  const finished = new Promise((resolve) => {
    resolveFinished = resolve;
  });
  const activeQuery = {
    abort: async (reason) => {
      controller.abort(reason);
      channel.end();
      await finished;
    },
    steer: async (steerPrompt) => {
      if (channel.ended) {
        return false;
      }
      return channel.push(buildClaudeUserMessage(steerPrompt));
    },
  };
  activeQueries.set(threadKey, activeQuery);
  const commandPolicy = createCommandEventPolicy({
    persistence,
    threadKey,
    chatId,
    messageId,
    controller: { abort: () => undefined },
    log,
  });
  let blockedGuardrailCommand = null;
  const bashHook = async (input) => {
    if (input?.hook_event_name !== "PreToolUse") {
      return {};
    }
    const command = typeof input.tool_input?.command === "string" ? input.tool_input.command : "";
    if (!command) {
      return {};
    }
    // Timestamped so a cancelled tool call can be lined up against the
    // transcript and the prompt-channel log.
    log.info(`bash-hook seen thread=${threadKey} at=${new Date().toISOString()} command=${JSON.stringify(command.slice(0, 120))}`);
    if (isBlockedDbCommand(command)) {
      blockedGuardrailCommand = blockedGuardrailCommand ?? command;
      log.warn("DB guardrail denied a Claude Code Bash command");
      return {
        hookSpecificOutput: {
          hookEventName: "PreToolUse",
          permissionDecision: "deny",
          permissionDecisionReason: buildDbGuardrailSyntheticText(command),
        },
      };
    }
    commandPolicy.inspectCommand({ command, sessionId });
    return {};
  };
  let activeSdkQuery = null;
  try {
    const claudeEnv = buildClaudeEnv();
    turnTimer("env.built");
    const mcpConfig = await buildCodexConfig(claudeEnv, threadKey);
    turnTimer("config.built", { mcp_servers: getMcpServerCount(mcpConfig) });
    await preflightConfiguredMcpServers(claudeEnv, mcpConfig, workingDirectory);
    turnTimer("mcp.preflight.done");
    const resumeExists = resumeSession ? await sessions.sessionExists(resumeSession) : false;
    const options = buildClaudeQueryOptions({
      workingDirectory,
      claudeEnv,
      mcpServers: toClaudeMcpServers(mcpConfig.mcp_servers),
      resumeSession,
      resumeExists,
      controller,
      hooks: {
        PreToolUse: [{ matcher: "Bash", hooks: [bashHook] }],
      },
    });
    activeSdkQuery = queryFactory({ prompt: channel.iterable, options });
    turnTimer("query.created", { mode: resumeSession ? (resumeExists ? "resume" : "reserved") : "start" });
    let firstEventLogged = false;
    let firstVisibleItemLogged = false;
    for await (const message of activeSdkQuery) {
      if (!firstEventLogged) {
        firstEventLogged = true;
        turnTimer("first_event", { event_type: `${message.type}${message.subtype ? `.${message.subtype}` : ""}` });
      }
      onStarted?.();
      if (message.type === "system" && message.subtype === "init") {
        sessionId = message.session_id ?? sessionId;
        if (sessionId) {
          persistence.updatePendingSessionId(pendingResponseId, sessionId);
          persistence.updateActiveTurnSessionId(threadKey, sessionId);
        }
        params.onTransportStarted?.({ sessionId, turnId: null });
        continue;
      }
      if (message.type === "assistant") {
        for (const item of projectAssistantMessageToItems(message)) {
          if (!firstVisibleItemLogged && isVisibleCodexItem(item)) {
            firstVisibleItemLogged = true;
            turnTimer("first_visible_item", { item_type: item.type });
          }
          mapItemToBlocks(item, { blockSequence, persistence, pendingResponseId });
        }
        continue;
      }
      if (message.type === "result") {
        const projected = projectResultMessage(message);
        sessionId = message.session_id ?? sessionId;
        if (projected?.ok) {
          turnTimer("turn.completed");
          if (projected.text?.trim()) {
            appendBlock(blockSequence, persistence, pendingResponseId, {
              type: "text",
              content: projected.text,
              phase: "final_answer",
            });
          }
          persistence.markPendingResponseComplete(pendingResponseId);
          responseCompleted = true;
          params.onTransportCompleted?.({ sessionId, turnId: null });
        } else {
          turnTimer("turn.failed", { error: projected?.error ?? "unknown" });
          appendBlock(blockSequence, persistence, pendingResponseId, {
            type: "text",
            content: `Error: ${projected?.error ?? "Claude did not complete"}`,
          });
        }
        const cacheRead = cacheReadTokensFromUsage(projected?.usage);
        if (sessionId && cacheRead !== undefined) {
          persistence.updateSessionUsage(sessionId, { cacheReadInputTokens: cacheRead });
        }
        channel.end();
        continue;
      }
      if (message.type === "auth_status" && message.error) {
        turnTimer("event.error", { error: message.error });
        appendBlock(blockSequence, persistence, pendingResponseId, {
          type: "text",
          content: `Error: ${message.error}`,
        });
      }
    }
  } catch (err) {
    const reason = controller.signal.aborted ? controller.signal.reason : err;
    const errMsg = getErrorMessage(reason);
    if (controller.signal.aborted && isIntentionalTurnInterrupt(reason)) {
      interrupted = true;
      blockSequence.length = 0;
      turnTimer("query.interrupted", { reason: errMsg });
      log.info(`Claude Code turn interrupted by operator control: ${errMsg}`);
    } else {
      turnTimer("query.error", { error: errMsg });
      log.error(`Error querying Claude Code: ${errMsg}`);
      appendBlock(blockSequence, persistence, pendingResponseId, {
        type: "text",
        content: `Error: ${errMsg}`,
      });
    }
  } finally {
    channel.end();
    try {
      activeSdkQuery?.close?.();
    } catch {
      // The generator may already be closed.
    }
    activeQueries.delete(threadKey);
    resolveFinished?.();
    turnTimer("query.finished", { guardrail_blocked: Boolean(blockedGuardrailCommand) });
  }
  if (blockedGuardrailCommand && !responseCompleted && !interrupted) {
    appendBlock(blockSequence, persistence, pendingResponseId, {
      type: "text",
      content: buildDbGuardrailFallbackText(blockedGuardrailCommand),
    });
  }
  log.info(`turn.done completed=${responseCompleted} interrupted=${interrupted}`);
  return {
    blockSequence,
    sessionId,
    pendingResponseId,
    interrupted,
    responseCompleted,
  };
}
