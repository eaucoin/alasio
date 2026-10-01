import { homedir } from "node:os";

import { withoutTelemetry } from "../../telemetry/index.js";

/**
 * Build the process environment handed to the Claude Code subprocess.
 *
 * Claude Code reads its login and settings from `CLAUDE_CONFIG_DIR` (default
 * `~/.claude`), so the service inherits whichever account the operator has
 * logged in with on this machine rather than requiring an API key.
 *
 * alasio's own telemetry settings are left out: they would relabel or redirect Claude
 * Code's and reach bayma and everything run through it. The process itself gets
 * telemetry settings of its own (./telemetry.js).
 */
export function buildClaudeEnv() {
  const env = {};
  for (const [key, value] of Object.entries(withoutTelemetry(process.env))) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  env.HOME = env.HOME || homedir();
  return env;
}
