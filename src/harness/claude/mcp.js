/**
 * Convert the Codex-shaped MCP server table alasio already materializes
 * (`mcp_servers` from `~/.codex/config.toml` plus `ALASIO_MCP_SERVERS_JSON`)
 * into the Claude Agent SDK `mcpServers` option so both harnesses expose the
 * same tool inventory.
 */
export function toClaudeMcpServers(mcpServers) {
  if (!mcpServers || typeof mcpServers !== "object" || Array.isArray(mcpServers)) {
    return {};
  }
  const out = {};
  for (const [serverName, serverConfig] of Object.entries(mcpServers)) {
    if (!serverConfig || typeof serverConfig !== "object" || Array.isArray(serverConfig)) {
      continue;
    }
    if (serverConfig.enabled === false) {
      continue;
    }
    if (typeof serverConfig.url === "string" && serverConfig.url) {
      out[serverName] = {
        type: "http",
        url: serverConfig.url,
        ...(serverConfig.headers && typeof serverConfig.headers === "object" ? { headers: serverConfig.headers } : {}),
      };
      continue;
    }
    if (typeof serverConfig.command !== "string" || !serverConfig.command) {
      continue;
    }
    const entry = {
      type: "stdio",
      command: serverConfig.command,
    };
    if (Array.isArray(serverConfig.args)) {
      entry.args = serverConfig.args.map((value) => String(value));
    }
    if (serverConfig.env && typeof serverConfig.env === "object" && !Array.isArray(serverConfig.env)) {
      entry.env = Object.fromEntries(
        Object.entries(serverConfig.env)
          .filter(([, value]) => value != null)
          .map(([key, value]) => [key, String(value)]),
      );
    }
    out[serverName] = entry;
  }
  return out;
}
