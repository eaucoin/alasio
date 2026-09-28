import { homedir } from "node:os";
import { join } from "node:path";

/** Where Codex keeps its state, as Codex resolves it: `$CODEX_HOME`, else `~/.codex`. */
export function codexHome(env = process.env) {
  return env.CODEX_HOME || join(env.HOME || homedir(), ".codex");
}

export function buildCodexEnv() {
  const env = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  env.HOME = env.HOME || homedir();
  env.CODEX_HOME = codexHome(env);
  delete env.CODEX_DISABLE_PROJECT_DOC;
  return env;
}
