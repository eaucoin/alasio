/**
 * Claude Code for session-filesystem workspaces. The CLI runs in alasio like a folder
 * workspace's, on the operator's login and Claude home (so resume, the Neon mirror, and
 * search work as ever), in the workspace's harness directory, an empty one of its own.
 * What makes it isolated is what it is given:
 *
 * - `tools` names every built-in it keeps, none of which touches this machine's files or
 *   runs a process here: subagents (which get the same tools), web search (run by
 *   Anthropic), and the task list. Web fetch is not among them: it fetches from this
 *   machine, which would reach its loopback.
 * - `settingSources: []` and `strictMcpConfig` keep the operator's settings, hooks,
 *   skills, plugins, and MCP servers out.
 * - bayma, reached at the session's Sandbox with its token (sandbox/index.js), is the one
 *   MCP server, and so the agent's only way into the workspace.
 */
import { SESSION_FS_AGENT_INSTRUCTIONS } from "../workspace-instructions.js";

/** The built-in tools a session-filesystem Claude Code keeps. */
export const SESSION_FS_CLAUDE_TOOLS = Object.freeze(["Agent", "WebSearch", "TaskCreate", "TaskGet", "TaskList", "TaskUpdate"]);

/** The query options that confine Claude Code to the workspace reached through `bayma` (`{ url, headers }`). */
export function sessionFsQueryOptions(bayma) {
  return {
    tools: [...SESSION_FS_CLAUDE_TOOLS],
    settingSources: [],
    strictMcpConfig: true,
    mcpServers: { bayma: { type: "http", url: bayma.url, headers: bayma.headers } },
    systemPrompt: { type: "preset", preset: "claude_code", append: SESSION_FS_AGENT_INSTRUCTIONS },
  };
}
