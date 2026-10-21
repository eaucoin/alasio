/**
 * The Claude Agent SDK's messages and queries as the Claude harness's tests stand them in:
 * whole messages, with whatever a test leaves out given a neutral value.
 */
import assert from "node:assert/strict";
import { randomUUID, type UUID } from "node:crypto";

import type {
  NonNullableUsage,
  SDKAssistantMessage,
  SDKBackgroundTasksChangedMessage,
  SDKMessage,
  SDKResultError,
  SDKResultSuccess,
  SDKSystemMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";

import type { ClaudeQuery } from "../../src/harness/claude/runtime.ts";

/** A block of an assistant message, as the Messages API carries it. */
export type ContentBlock = SDKAssistantMessage["message"]["content"][number];
export type TextBlock = Extract<ContentBlock, { type: "text" }>;
export type ToolUseBlock = Extract<ContentBlock, { type: "tool_use" }>;

/** A prompt alasio pushed, which it stamps with a uuid (prompt-channel.ts). */
export type StampedPrompt = SDKUserMessage & { readonly uuid: UUID };

function isStamped(message: SDKUserMessage): message is StampedPrompt {
  return message.uuid !== undefined;
}

/** `message`, which must be a prompt alasio pushed. */
export function stamped(message: SDKUserMessage | undefined): StampedPrompt {
  assert.ok(message && isStamped(message), "a prompt alasio pushed, with its uuid");
  return message;
}

/** The next prompt a stand-in CLI reads from its streaming input, which must not have ended. */
export async function readPrompt(prompts: AsyncIterator<SDKUserMessage>): Promise<StampedPrompt> {
  const next = await prompts.next();
  assert.ok(!next.done, "a prompt, not the end of the stream");
  return stamped(next.value);
}

/** A turn's token usage, every count zero but those given. */
export function usage(fields: Partial<NonNullableUsage> = {}): NonNullableUsage {
  return {
    cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    fallback_credit: { status: { type: "not_applied", reason: "not_enabled" } },
    inference_geo: "",
    input_tokens: 0,
    iterations: [],
    output_tokens: 0,
    output_tokens_details: { thinking_tokens: 0 },
    server_tool_use: { web_fetch_requests: 0, web_search_requests: 0 },
    service_tier: "standard",
    speed: "standard",
    ...fields,
  };
}

/** The message a Claude Code process starts with, naming its session. */
export function initMessage(sessionId: string): SDKSystemMessage {
  return {
    type: "system",
    subtype: "init",
    apiKeySource: "none",
    claude_code_version: "0.0.0",
    cwd: "/work",
    tools: [],
    mcp_servers: [],
    model: "claude-test",
    permissionMode: "bypassPermissions",
    slash_commands: [],
    output_style: "default",
    skills: [],
    plugins: [],
    uuid: randomUUID(),
    session_id: sessionId,
  };
}

/** A turn's result that answered with `result`. */
export function successResult(fields: Pick<SDKResultSuccess, "result" | "session_id"> & Partial<SDKResultSuccess>): SDKResultSuccess {
  return {
    type: "result",
    subtype: "success",
    duration_ms: 0,
    duration_api_ms: 0,
    is_error: false,
    num_turns: 1,
    stop_reason: null,
    total_cost_usd: 0,
    usage: usage(),
    modelUsage: {},
    permission_denials: [],
    uuid: randomUUID(),
    ...fields,
  };
}

/** A turn's result that failed with `errors`. */
export function errorResult(fields: Pick<SDKResultError, "subtype" | "errors" | "session_id"> & Partial<SDKResultError>): SDKResultError {
  return {
    type: "result",
    duration_ms: 0,
    duration_api_ms: 0,
    is_error: true,
    num_turns: 1,
    stop_reason: null,
    total_cost_usd: 0,
    usage: usage(),
    modelUsage: {},
    permission_denials: [],
    uuid: randomUUID(),
    ...fields,
  };
}

export function text(value: string): TextBlock {
  return { type: "text", text: value, citations: null };
}

export function toolUse(id: string, name: string, input: Readonly<Record<string, unknown>>): ToolUseBlock {
  return { type: "tool_use", id, name, input };
}

/** An assistant message of the main thread, unless `fields` says otherwise. */
export function assistantMessage(content: ContentBlock[], fields: Partial<SDKAssistantMessage> = {}): SDKAssistantMessage {
  return {
    type: "assistant",
    message: {
      id: "msg-1",
      container: null,
      content,
      context_management: null,
      diagnostics: null,
      model: "claude-test",
      role: "assistant",
      stop_details: null,
      stop_reason: null,
      stop_sequence: null,
      type: "message",
      usage: usage(),
    },
    parent_tool_use_id: null,
    uuid: randomUUID(),
    session_id: "s-1",
    ...fields,
  };
}

/** Claude Code's report of the background tasks still running. */
export function backgroundTasksChanged(tasks: SDKBackgroundTasksChangedMessage["tasks"]): SDKBackgroundTasksChangedMessage {
  return { type: "system", subtype: "background_tasks_changed", tasks, uuid: randomUUID(), session_id: "s-1" };
}

/**
 * A query streaming `messages`, with the close and interrupt given. Without an interrupt
 * of its own it cannot interrupt a turn, and live-sessions.ts closes its process instead.
 */
export function fakeQuery(
  messages: AsyncGenerator<SDKMessage, void>,
  { close = () => undefined, interrupt = cannotInterrupt }: Partial<Pick<ClaudeQuery, "close" | "interrupt">> = {},
): ClaudeQuery {
  return Object.assign(messages, { close, interrupt });
}

async function cannotInterrupt(): Promise<undefined> {
  throw new Error("this stand-in cannot interrupt a turn");
}
