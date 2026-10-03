import type { McpServerConfig } from "@anthropic-ai/claude-agent-sdk";

import { BAYMA_SERVER_NAME, type BaymaMcpServer } from "../../mcp/bayma.ts";

/**
 * The MCP servers alasio adds to a Claude Code query in a folder workspace: `bayma`, the
 * conversation's server (../../mcp/bayma.ts HostBayma), which is already in
 * Claude Code's shape. Claude Code loads the operator's own servers (user and project
 * `.mcp.json` files and claude.ai connectors) alongside it.
 */
export function claudeMcpServers(bayma: BaymaMcpServer): Record<string, McpServerConfig> {
  return { [BAYMA_SERVER_NAME]: bayma };
}
