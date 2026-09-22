import { BAYMA_SERVER_NAME, baymaLaunch } from "../../mcp/bayma.js";
import { CLAUDE_HARNESS } from "../names.js";

/**
 * Claude Code's MCP servers: bayma alone. The query options pair this with
 * `strictMcpConfig`, which keeps Claude Code from adding the servers in the
 * operator's own configuration (user and project `.mcp.json` files and
 * claude.ai connectors).
 */
export function claudeMcpServers({ threadKey, env }) {
  return {
    [BAYMA_SERVER_NAME]: {
      type: "stdio",
      ...baymaLaunch({ harness: CLAUDE_HARNESS, threadKey, env }),
    },
  };
}
