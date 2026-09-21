import { homedir } from "node:os";
import { join } from "node:path";

export function buildCodexEnv() {
  const env = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }
  const home = env.HOME || homedir();
  env.HOME = home;
  env.CODEX_HOME = env.CODEX_HOME || join(home, ".codex");
  delete env.CODEX_DISABLE_PROJECT_DOC;
  return env;
}
