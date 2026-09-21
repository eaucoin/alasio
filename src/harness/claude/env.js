import { homedir } from "node:os";

/**
 * Build the process environment handed to the Claude Code subprocess.
 *
 * Claude Code reads its login and settings from `CLAUDE_CONFIG_DIR` (default
 * `~/.claude`), so the service inherits whichever account the operator has
 * logged in with on this machine rather than requiring an API key.
 */
export function buildClaudeEnv() {
  const env = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  env.HOME = env.HOME || homedir();
  return env;
}
