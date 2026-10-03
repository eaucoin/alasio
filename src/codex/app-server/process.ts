import { type ChildProcessByStdio, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { createInterface, type Interface } from "node:readline";
import type { Readable, Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import { getCodexBinaryOverride } from "../../config.ts";
import type { CodexEnv } from "../env.ts";
import { appServerLog as log } from "./log.ts";
import { codexTelemetryArgs, codexTelemetryEnv } from "./telemetry.ts";

/** How an app-server process is started, and what its output and end are reported to. */
export interface AppServerProcessOptions {
  readonly cwd: string;
  readonly env: CodexEnv;
  readonly onLine: (line: string) => void;
  readonly onExit: (code: number | null, signal: NodeJS.Signals | null) => void;
  readonly onError: (error: Error) => void;
}

/** A running app-server: its process, the reader of its output lines, and its stop. */
export interface AppServerProcess {
  readonly child: ChildProcessByStdio<Writable, Readable, Readable>;
  readonly readline: Interface;
  stop(): void;
}

/** Starts an app-server process. */
export type SpawnAppServer = (options: AppServerProcessOptions) => AppServerProcess;

function codexBinPath() {
  const here = dirname(fileURLToPath(import.meta.url));
  return getCodexBinaryOverride() || join(here, "..", "..", "..", "node_modules", ".bin", "codex");
}

export function elapsedMs(startedAt: bigint): number {
  return Number(process.hrtime.bigint() - startedAt) / 1_000_000;
}

export function startAppServerProcess({ cwd, env, onLine, onExit, onError }: AppServerProcessOptions): AppServerProcess {
  const binPath = codexBinPath();
  if (!existsSync(binPath)) {
    throw new Error(`Codex binary not found at ${binPath}`);
  }
  const child = spawn(binPath, ["app-server", "--disable", "plugins", ...codexTelemetryArgs(), "--listen", "stdio://"], {
    cwd,
    env: { ...env, ...codexTelemetryEnv() },
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stderr?.on("data", (data: Buffer) => {
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
