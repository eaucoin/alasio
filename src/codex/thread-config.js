/**
 * The config overrides alasio sends with every Codex thread it starts or resumes.
 *
 * Codex merges these over `$CODEX_HOME/config.toml` instead of replacing it,
 * so an MCP server defined there, or the ChatGPT apps connector, would reach
 * the agent alongside bayma. The overrides switch each of those off, which
 * leaves bayma as the only MCP server a alasio thread has.
 */
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { parse as parseToml } from "smol-toml";

import { CODEX_HARNESS } from "../harness/names.js";
import { BAYMA_SERVER_NAME, BAYMA_STARTUP_TIMEOUT_MS, baymaLaunch } from "../mcp/bayma.js";

async function ambientMcpServerNames(codexEnv) {
  let raw;
  try {
    raw = await readFile(join(codexEnv.CODEX_HOME, "config.toml"), "utf8");
  } catch {
    return [];
  }
  return Object.keys(parseToml(raw).mcp_servers ?? {});
}

export async function buildCodexThreadConfig({ codexEnv, threadKey }) {
  const ambientServers = await ambientMcpServerNames(codexEnv);
  return {
    project_doc_max_bytes: 32768,
    features: { apps: false },
    mcp_servers: {
      ...Object.fromEntries(ambientServers.map((name) => [name, { enabled: false }])),
      [BAYMA_SERVER_NAME]: {
        ...baymaLaunch({ harness: CODEX_HARNESS, threadKey, env: codexEnv }),
        startup_timeout_sec: BAYMA_STARTUP_TIMEOUT_MS / 1000,
      },
    },
  };
}
