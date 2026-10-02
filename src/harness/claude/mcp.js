import { BAYMA_SERVER_NAME } from "../../mcp/bayma.js";

/**
 * The MCP servers alasio adds to a Claude Code query in a folder workspace: `bayma`, the
 * conversation's server (../../mcp/bayma.js folderBaymaServer), which is already in
 * Claude Code's shape. Claude Code loads the operator's own servers (user and project
 * `.mcp.json` files and claude.ai connectors) alongside it.
 */
export function claudeMcpServers(bayma) {
  return { [BAYMA_SERVER_NAME]: bayma };
}
