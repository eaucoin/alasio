// @ts-nocheck
/**
 * Project Claude Agent SDK messages into the Codex-shaped items that
 * `codex/event-projection.ts` already knows how to persist as response blocks.
 *
 * Claude has no explicit "final answer" phase; the SDK `result` message is the
 * only text delivered to the operator, and intermediate assistant text stays
 * internal commentary exactly like Codex commentary.
 */
const EDIT_TOOL_NAMES = new Set(["Edit", "Write", "MultiEdit", "NotebookEdit"]);
const SHELL_TOOL_NAMES = new Set(["Bash", "PowerShell"]);
const MCP_TOOL_PREFIX = "mcp__";

export function parseMcpToolName(toolName) {
  if (typeof toolName !== "string" || !toolName.startsWith(MCP_TOOL_PREFIX)) {
    return null;
  }
  const rest = toolName.slice(MCP_TOOL_PREFIX.length);
  const separator = rest.indexOf("__");
  if (separator <= 0) {
    return { server: rest, tool: rest };
  }
  return {
    server: rest.slice(0, separator),
    tool: rest.slice(separator + 2),
  };
}

function textFromContent(content) {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .filter((block) => block && block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n");
}

export function projectToolUseToItem(block) {
  const name = typeof block?.name === "string" ? block.name : "";
  const input = block?.input && typeof block.input === "object" ? block.input : {};
  if (SHELL_TOOL_NAMES.has(name)) {
    return {
      type: "command_execution",
      id: block.id,
      command: typeof input.command === "string" ? input.command : "",
    };
  }
  if (EDIT_TOOL_NAMES.has(name)) {
    return {
      type: "file_change",
      id: block.id,
      changes: [{ kind: "update", path: typeof input.file_path === "string" ? input.file_path : "" }],
    };
  }
  if (name === "WebSearch") {
    return { type: "web_search", id: block.id };
  }
  if (name === "AskUserQuestion") {
    return {
      type: "mcp_tool_call",
      id: block.id,
      server: "claude",
      tool: "AskUserQuestion",
      arguments: { questions: Array.isArray(input.questions) ? input.questions : [] },
    };
  }
  const mcp = parseMcpToolName(name);
  if (mcp) {
    return {
      type: "mcp_tool_call",
      id: block.id,
      server: mcp.server,
      tool: mcp.tool,
      arguments: input,
    };
  }
  if (!name) {
    return null;
  }
  return {
    type: "mcp_tool_call",
    id: block.id,
    server: "claude",
    tool: name,
    arguments: input,
  };
}

export function projectAssistantMessageToItems(message) {
  if (!message || message.type !== "assistant" || message.parent_tool_use_id) {
    return [];
  }
  const content = message.message?.content;
  if (!Array.isArray(content)) {
    return [];
  }
  const items = [];
  for (const block of content) {
    if (!block || typeof block !== "object") {
      continue;
    }
    if (block.type === "text" && typeof block.text === "string" && block.text.trim()) {
      items.push({ type: "agent_message", text: block.text, phase: "commentary" });
    } else if (block.type === "tool_use") {
      const item = projectToolUseToItem(block);
      if (item) {
        items.push(item);
      }
    }
  }
  return items;
}

export function projectResultMessage(message) {
  if (!message || message.type !== "result") {
    return null;
  }
  const errors = Array.isArray(message.errors) ? message.errors.filter((value) => typeof value === "string" && value.trim()) : [];
  if (message.subtype !== "success" || message.is_error) {
    const detail = errors.length > 0
      ? errors.join("\n")
      : (typeof message.result === "string" && message.result.trim() ? message.result : `Claude ended with ${message.subtype}`);
    return { ok: false, error: detail, usage: message.usage ?? null };
  }
  return {
    ok: true,
    text: typeof message.result === "string" ? message.result : textFromContent(message.result),
    usage: message.usage ?? null,
  };
}

export function cacheReadTokensFromUsage(usage) {
  const value = usage?.cache_read_input_tokens;
  return Number.isFinite(value) ? value : undefined;
}
