import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { setTimeout as sleep } from "node:timers/promises";
import { readPositiveIntEnv } from "../config.js";
import { createLogger } from "../shared/log.js";
import {
  resolveBreadbutterPythonCapability,
  runBreadbutterPythonCapability,
} from "./breadbutter-python-capability.js";
import {
  resolveBreadbutterRustCapability,
  provisionBreadbutterRustCapability,
  runBreadbutterRustCapability,
} from "./breadbutter-rust-capability.js";
import { isolatePreflightState } from "./preflight-state.js";

const log = createLogger("mcp-preflight");

const DEFAULT_MCP_PREFLIGHT_TIMEOUT_MS = 15000;
const DEFAULT_MCP_PREFLIGHT_CACHE_TTL_MS = 10 * 60 * 1000;
const MCP_PREFLIGHT_CACHE_TTL_MS = readPositiveIntEnv("ALASIO_MCP_PREFLIGHT_CACHE_TTL_MS", DEFAULT_MCP_PREFLIGHT_CACHE_TTL_MS);
const mcpPreflightCache = new Map();

async function withTimeout(promise, timeoutMs, label) {
  const timeoutController = new AbortController();
  try {
    return await Promise.race([
      promise,
      sleep(timeoutMs, undefined, { signal: timeoutController.signal }).then(
        () => {
          throw new Error(`${label} timed out after ${timeoutMs}ms`);
        },
      ),
    ]);
  } finally {
    timeoutController.abort();
  }
}

function normalizeStringRecord(candidate) {
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    return {};
  }
  const normalized = {};
  for (const [key, value] of Object.entries(candidate)) {
    if (typeof value === "string") {
      normalized[key] = value;
    }
  }
  return normalized;
}

async function preflightStdioMcpServer(
  serverName,
  serverConfig,
  env,
  workingDirectory,
) {
  if (serverConfig?.disabled === true) {
    log.info(`Skipping disabled MCP server ${serverName}`);
    return;
  }
  if (typeof serverConfig?.command !== "string" || !serverConfig.command.trim()) {
    log.info(`Skipping non-stdio MCP server ${serverName} preflight`);
    return;
  }
  const breadbutterCapability = await resolveBreadbutterPythonCapability({
    serverName,
    serverConfig,
    workingDirectory,
  });
  const breadbutterRustCapability = await resolveBreadbutterRustCapability({
    serverName,
    serverConfig,
    workingDirectory,
  });
  const cacheKey = JSON.stringify({
    serverName,
    command: serverConfig.command,
    args: Array.isArray(serverConfig.args) ? serverConfig.args : [],
    env: normalizeStringRecord(serverConfig.env),
    startup_timeout_sec: serverConfig.startup_timeout_sec ?? null,
    breadbutterCapability: breadbutterCapability
      ? {
          fingerprint: breadbutterCapability.fingerprint,
          repoRoot: breadbutterCapability.repoRoot,
        }
      : null,
    breadbutterRustCapability: breadbutterRustCapability
      ? {
          fingerprint: breadbutterRustCapability.fingerprint,
          repoRoot: breadbutterRustCapability.repoRoot,
        }
      : null,
  });
  const cachedAt = mcpPreflightCache.get(cacheKey);
  if (typeof cachedAt === "number" && (Date.now() - cachedAt) < MCP_PREFLIGHT_CACHE_TTL_MS) {
    return;
  }
  if (breadbutterRustCapability) {
    const evidence = await provisionBreadbutterRustCapability(breadbutterRustCapability);
    log.info(`Provisioned Breadbutter Rust Cargo graph under ${evidence.cargo_home}`);
  }
  const timeoutMs = Number.isFinite(serverConfig.startup_timeout_sec)
    ? Math.max(1000, Number(serverConfig.startup_timeout_sec) * 1000)
    : DEFAULT_MCP_PREFLIGHT_TIMEOUT_MS;
  const client = new Client({
    name: `alasio-preflight-${serverName}`,
    version: "1.0.0",
  });
  const isolated = await isolatePreflightState(serverConfig);
  const preflightConfig = isolated.serverConfig;
  try {
    const transport = new StdioClientTransport({
      command: preflightConfig.command,
      args: Array.isArray(preflightConfig.args)
        ? preflightConfig.args.map((value) => String(value))
        : [],
      env: {
        ...env,
        ...normalizeStringRecord(preflightConfig.env),
      },
      stderr: "pipe",
    });
    log.info(`Preflighting MCP server ${serverName}`);
    await withTimeout(client.connect(transport), timeoutMs, `MCP server ${serverName} connect`);
    const [tools, templates, resources] = await withTimeout(Promise.all([
      client.listTools(),
      client.listResourceTemplates(),
      client.listResources(),
    ]), timeoutMs, `MCP server ${serverName} inventory`);
    const toolCount = tools.tools.length;
    const templateCount = templates.resourceTemplates.length;
    const resourceCount = resources.resources.length;
    if (toolCount === 0 && templateCount === 0 && resourceCount === 0) {
      throw new Error(`MCP server ${serverName} exposed no tools, resource templates, or resources`);
    }
    if (breadbutterCapability) {
      const evidence = await withTimeout(
        runBreadbutterPythonCapability(client, breadbutterCapability),
        timeoutMs,
        `MCP server ${serverName} Breadbutter Python capability`,
      );
      log.info(
        `MCP server ${serverName} loaded Breadbutter Python ${evidence.python} (${evidence.packages} packages)`,
      );
    }
    if (breadbutterRustCapability) {
      const evidence = await withTimeout(
        runBreadbutterRustCapability(client, breadbutterRustCapability),
        Math.max(timeoutMs, 5 * 60 * 1000),
        `MCP server ${serverName} Breadbutter Rust capability`,
      );
      log.info(
        `MCP server ${serverName} loaded Breadbutter Rust ${evidence.rust} package workbench`,
      );
    }
    mcpPreflightCache.set(cacheKey, Date.now());
    log.info(`MCP server ${serverName} ready (tools=${toolCount}, templates=${templateCount}, resources=${resourceCount})`);
  } finally {
    await client.close().catch(() => {});
    await isolated.cleanup().catch((error) => {
      log.warn(`Failed to clean up MCP preflight state for ${serverName}: ${error instanceof Error ? error.message : String(error)}`);
    });
  }
}

export async function preflightConfiguredMcpServers(
  env,
  codexConfig,
  workingDirectory,
) {
  const mcpServers = codexConfig.mcp_servers;
  if (!mcpServers || typeof mcpServers !== "object" || Array.isArray(mcpServers)) {
    return;
  }
  const preflightTasks = [];
  for (const [serverName, serverConfig] of Object.entries(mcpServers)) {
    if (!serverConfig || typeof serverConfig !== "object" || Array.isArray(serverConfig)) {
      continue;
    }
    preflightTasks.push(
      preflightStdioMcpServer(
        serverName,
        serverConfig,
        env,
        workingDirectory,
      ),
    );
  }
  if (preflightTasks.length > 0) {
    await Promise.all(preflightTasks);
  }
}
