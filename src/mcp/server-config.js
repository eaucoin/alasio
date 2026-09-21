import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseMcpServersFromToml } from "./codex-config-toml.js";
import { materializeMcpServerConfig } from "./bayma-state.js";

const DEFAULT_CODEX_CONFIG = {
  project_doc_max_bytes: 32768,
};

export function getMcpServerCount(codexConfig) {
  const mcpServers = codexConfig.mcp_servers;
  if (!mcpServers || typeof mcpServers !== "object" || Array.isArray(mcpServers)) {
    return 0;
  }
  return Object.keys(mcpServers).length;
}

function resolveCodexHome(env) {
  return env.CODEX_HOME || join(env.HOME || homedir(), ".codex");
}

async function loadMcpServersFromCodexConfig(env) {
  const configPath = join(resolveCodexHome(env), "config.toml");
  try {
    const rawConfig = await readFile(configPath, "utf8");
    return parseMcpServersFromToml(rawConfig);
  } catch {
    return {};
  }
}

function loadMcpServersFromEnv(env) {
  const raw = env.ALASIO_MCP_SERVERS_JSON;
  if (!raw?.trim()) {
    return {};
  }
  const parsed = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("ALASIO_MCP_SERVERS_JSON must be a JSON object");
  }
  return parsed;
}

function mergeMcpServerConfigs(baseConfig, overrideConfig) {
  const merged = { ...baseConfig };
  for (const [serverName, serverConfig] of Object.entries(overrideConfig)) {
    if (serverConfig && typeof serverConfig === "object" && !Array.isArray(serverConfig)) {
      merged[serverName] = {
        ...(merged[serverName] ?? {}),
        ...serverConfig,
      };
    } else {
      merged[serverName] = serverConfig;
    }
  }
  return merged;
}

export async function buildCodexConfig(env, threadKey) {
  const config = { ...DEFAULT_CODEX_CONFIG };
  const fileMcpServers = await loadMcpServersFromCodexConfig(env);
  const envMcpServers = loadMcpServersFromEnv(env);
  const mergedMcpServers = mergeMcpServerConfigs(fileMcpServers, envMcpServers);
  if (Object.keys(mergedMcpServers).length > 0) {
    config.mcp_servers = Object.fromEntries(Object.entries(mergedMcpServers).map(([serverName, serverConfig]) => [
      serverName,
      materializeMcpServerConfig(serverName, serverConfig, env, threadKey),
    ]));
  }
  return config;
}
