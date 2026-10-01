import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { getCodexBinaryOverride } from "../../config.js";
import { appServerLog as log } from "./log.js";
import { codexTelemetryArgs, codexTelemetryEnv } from "./telemetry.js";

function codexBinPath() {
  const here = dirname(fileURLToPath(import.meta.url));
  return getCodexBinaryOverride() || join(here, "..", "..", "..", "node_modules", ".bin", "codex");
}

export function elapsedMs(startedAt) {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}

export function startAppServerProcess({ cwd, env, onLine, onExit, onError }) {
  const binPath = codexBinPath();
  if (!existsSync(binPath)) {
    throw new Error(`Codex binary not found at ${binPath}`);
  }
  const child = spawn(binPath, ["app-server", "--disable", "plugins", ...codexTelemetryArgs(), "--listen", "stdio://"], {
    cwd,
    env: { ...env, ...codexTelemetryEnv() },
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stderr?.on("data", (data) => {
    const text = data.toString().trimEnd();
    if (text) {
      log.warn(`stderr ${text}`);
    }
  });
  child.on("exit", (code, signal) => onExit(code, signal));
  child.on("error", (error) => onError(error));
  const readline = createInterface({ input: child.stdout, crlfDelay: Infinity });
  readline.on("line", onLine);
  return {
    child,
    readline,
    stop() {
      readline.close();
      child.kill("SIGTERM");
    },
  };
}
