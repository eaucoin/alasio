import { homedir } from "node:os";

import { withoutAlasio } from "../../config.ts";
import { withoutTelemetry } from "../../telemetry/index.ts";

/**
 * Build the process environment handed to the Claude Code subprocess.
 *
 * Claude Code reads its login and settings from `CLAUDE_CONFIG_DIR` (default
 * `~/.claude`), so the service inherits whichever account the operator has
 * logged in with on this machine rather than requiring an API key.
 *
 * alasio's own variables are left out, its secrets among them (../../config.ts
 * withoutAlasio), and so are its telemetry settings: they would relabel or redirect Claude
 * Code's and reach bayma and everything run through it. The process itself gets telemetry
 * settings of its own (./telemetry.ts). What is left is the machine's, and what the
 * operator's configuration adds for it, Claude Code's login (CLAUDE_LOGIN) among it.
 *
 * The login's claude.ai connectors are never loaded (CLAUDEAI_CONNECTORS): a turn's tools
 * are the MCP servers alasio gives it, and nothing the account connected on claude.ai,
 * which the CLI would otherwise reach through Anthropic's MCP proxy.
 */
export function buildClaudeEnv(): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(withoutAlasio(withoutTelemetry(process.env)))) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  env["HOME"] = env["HOME"] || homedir();
  env[CLAUDEAI_CONNECTORS] = "false";
  return env;
}

/** The CLI's switch for loading the login's claude.ai connectors, as its own help names it (or its disableClaudeAiConnectors setting). */
export const CLAUDEAI_CONNECTORS = "ENABLE_CLAUDEAI_MCP_SERVERS";

/** The variable Claude Code's login is in, from `claude setup-token`, as alasio's deployment gives it (cli/src/manifests/alasio.ts). */
export const CLAUDE_LOGIN = "CLAUDE_CODE_OAUTH_TOKEN";
