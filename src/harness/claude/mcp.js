import { BAYMA_SERVER_NAME, baymaLaunch } from "../../mcp/bayma.js";
import { CLAUDE_HARNESS } from "../names.js";

/**
 * The MCP servers alasio adds to a Claude Code query: bayma. Claude Code loads
 * the operator's own servers (user and project `.mcp.json` files and claude.ai
 * connectors) alongside it.
 */
export function claudeMcpServers({ threadKey, env }) {
  return {
    [BAYMA_SERVER_NAME]: {
      type: "stdio",
      ...baymaLaunch({ harness: CLAUDE_HARNESS, threadKey, env }),
    },
  };
}
