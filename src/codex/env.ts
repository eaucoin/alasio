import { homedir } from "node:os";
import { join } from "node:path";

import { withoutAlasio } from "../config.ts";
import { CLAUDE_LOGIN } from "../harness/claude/env.ts";
import { withoutTelemetry } from "../telemetry/index.ts";

/** The environment a Codex process runs with. */
export type CodexEnv = Readonly<Record<string, string>>;

/** Where Codex keeps its state, as Codex resolves it: `$CODEX_HOME`, else `~/.codex`. */
export function codexHome(env: Readonly<NodeJS.ProcessEnv> = process.env): string {
  return env["CODEX_HOME"] || join(env["HOME"] || homedir(), ".codex");
}

/**
 * The environment Codex runs with: alasio's, but for alasio's own variables, its secrets among
 * them (../config.ts withoutAlasio), Claude Code's login, which is no Codex's, and alasio's
 * telemetry settings, which would relabel or redirect Codex's and reach every command Codex
 * runs; the app-server gets telemetry settings of its own (./app-server/telemetry.ts).
 */
export function buildCodexEnv(): CodexEnv {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(withoutAlasio(withoutTelemetry(process.env)))) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  env["HOME"] = env["HOME"] || homedir();
  env["CODEX_HOME"] = codexHome(env);
  delete env["CODEX_DISABLE_PROJECT_DOC"];
  delete env[CLAUDE_LOGIN];
  return env;
}
