/**
 * The config overrides alasio sends with every Codex thread it starts or resumes.
 *
 * Codex merges these over `$CODEX_HOME/config.toml`, so the operator's own MCP
 * servers and the ChatGPT apps connector stay available and bayma is added
 * alongside them.
 */
import { withReplyInstructions } from "../harness/reply-instructions.js";
import { BAYMA_SERVER_NAME, BAYMA_STARTUP_TIMEOUT_MS } from "../mcp/bayma.js";
import { operatorDeveloperInstructions } from "./config-toml.js";
import { codexHome } from "./env.js";

/** A bayma server (../mcp/bayma.js folderBaymaServer) in Codex's MCP config shape. */
export function codexMcpServer(server) {
  const startup = { startup_timeout_sec: BAYMA_STARTUP_TIMEOUT_MS / 1000 };
  if (server.type === "http") return { url: server.url, http_headers: server.headers, ...startup };
  const { type: _stdio, ...command } = server;
  return { ...command, ...startup };
}

/** The overrides for a folder workspace's thread, whose bayma server is `bayma`. */
export function buildCodexThreadConfig({ codexEnv, bayma }) {
  return {
    project_doc_max_bytes: 32768,
    // An override replaces the config's own, so the operator's are kept, then alasio's.
    developer_instructions: withReplyInstructions(operatorDeveloperInstructions(codexHome(codexEnv))),
    mcp_servers: {
      [BAYMA_SERVER_NAME]: codexMcpServer(bayma),
    },
  };
}
