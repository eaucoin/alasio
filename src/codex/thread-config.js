/**
 * The config overrides alasio sends with every Codex thread it starts or resumes.
 *
 * Codex merges these over `$CODEX_HOME/config.toml`, so the operator's own MCP
 * servers and the ChatGPT apps connector stay available and bayma is added
 * alongside them.
 */
import { CODEX_HARNESS } from "../harness/names.js";
import { withReplyInstructions } from "../harness/reply-instructions.js";
import { BAYMA_SERVER_NAME, BAYMA_STARTUP_TIMEOUT_MS, baymaLaunch } from "../mcp/bayma.js";
import { operatorDeveloperInstructions } from "./config-toml.js";
import { codexHome } from "./env.js";

export function buildCodexThreadConfig({ codexEnv, threadKey }) {
  return {
    project_doc_max_bytes: 32768,
    // An override replaces the config's own, so the operator's are kept, then alasio's.
    developer_instructions: withReplyInstructions(operatorDeveloperInstructions(codexHome(codexEnv))),
    mcp_servers: {
      [BAYMA_SERVER_NAME]: {
        ...baymaLaunch({ harness: CODEX_HARNESS, threadKey, env: codexEnv }),
        startup_timeout_sec: BAYMA_STARTUP_TIMEOUT_MS / 1000,
      },
    },
  };
}
