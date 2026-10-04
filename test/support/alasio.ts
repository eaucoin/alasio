/**
 * alasio, running for a test. alasio runs as a process of its own (./alasio-main.ts, the
 * one place that knows how alasio is assembled), so stopping and restarting it are what
 * they are in production, and it talks to stand-ins at its boundaries: Telegram
 * (./telegram.ts), the Codex app-server it spawns (./codex.ts), Claude Code's queries
 * (./claude.ts), and a folder workspace's bayma. Tests talk to those stand-ins, and to
 * alasio's SQLite state through its store where a scenario is about durability; to
 * nothing inside alasio.
 *
 * ALASIO_TEST_LOG=1 shows alasio's own log as it runs.
 */
import { type ChildProcess, fork } from "node:child_process";
import { mkdirSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import type { BaymaMcpServer } from "../../src/mcp/bayma.ts";
import { SqliteStore } from "../../src/persistence/store.ts";
import { FakeClaude } from "./claude.ts";
import { FakeCodexAppServer } from "./codex.ts";
import { type AlasioProcessConfig, READY } from "./alasio-main.ts";
import { OPERATOR_ID, TelegramStandIn } from "./telegram.ts";

const ALASIO_MAIN = fileURLToPath(new URL("./alasio-main.ts", import.meta.url));

/** The bayma a folder workspace's agent is given. */
export const FOLDER_BAYMA: BaymaMcpServer = { type: "http", url: "http://bayma.test:7290/mcp", headers: { Authorization: "Bearer folder-bayma" } };

/** The environment of the test's own that alasio would read, and so does not inherit. */
const WITHHELD_ENV = [
  "NODE_TEST_CONTEXT",
  "WORKING_DIRECTORY",
  "ALASIO_CODEX_TRANSPORT",
  "ALASIO_DEFAULT_HARNESS",
  "ALASIO_DEPLOYMENT",
  "ALASIO_NAMESPACE",
  "ALASIO_CLAUDE_MODEL",
  "ALASIO_CLAUDE_EFFORT",
  "ALASIO_CLAUDE_BIN",
  "ALASIO_KUBE_TEMPLATES",
  "ALASIO_HOOK_PORT",
  "ALASIO_WARM_LINKED_SESSIONS",
];

export interface AlasioOptions {
  /** Folders made under the workspace root before alasio starts. */
  readonly folders?: readonly string[];
}

/** How an alasio process ended. */
export interface AlasioExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

export interface RunningAlasio {
  readonly telegram: TelegramStandIn;
  readonly codex: FakeCodexAppServer;
  readonly claude: FakeClaude;
  /** The directory every folder workspace is under. */
  readonly workspaceRoot: string;
  /** The path of the folder `name` under the workspace root, as alasio names it. */
  folder(name: string): string;
  /** Stops alasio as SIGTERM does in production; the stand-ins stay up. How its process ended. */
  stop(): Promise<AlasioExit>;
  /** Starts alasio again, on the same state directory. */
  start(): Promise<void>;
  /** Reads alasio's SQLite state through its store, for scenarios about durability. */
  readStore<T>(read: (store: SqliteStore) => T): T;
  /** Stops alasio if it runs, and takes everything down. */
  close(): Promise<void>;
}

/** alasio, started with stand-ins at its boundaries, answering the operator in chat OPERATOR_ID. */
export async function startAlasio({ folders = [] }: AlasioOptions = {}): Promise<RunningAlasio> {
  const root = realpathSync(mkdtempSync(join(tmpdir(), "alasio-app-")));
  const workspaceRoot = join(root, "workspaces");
  const stateDir = join(root, "state");
  for (const directory of [workspaceRoot, stateDir, join(root, "codex-home"), join(root, "claude-config")]) {
    mkdirSync(directory, { recursive: true });
  }
  for (const name of folders) mkdirSync(join(workspaceRoot, name));
  const telegram = await TelegramStandIn.start();
  const codex = await FakeCodexAppServer.start(root);
  const claude = await FakeClaude.start(join(root, "claude.sock"));

  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const key of WITHHELD_ENV) delete env[key];
  Object.assign(env, {
    TELEGRAM_API_ROOT: telegram.apiRoot,
    ALASIO_CODEX_BIN: codex.bin,
    CODEX_HOME: join(root, "codex-home"),
    CLAUDE_CONFIG_DIR: join(root, "claude-config"),
  });
  const config: AlasioProcessConfig = {
    stateDir,
    workspaceRoot,
    allowedUserIds: String(OPERATOR_ID),
    claudeSocket: claude.socketPath,
    folderBayma: FOLDER_BAYMA,
  };
  const verbose = process.env["ALASIO_TEST_LOG"] === "1";
  let output = "";
  let child: ChildProcess | null = null;
  let exited: Promise<AlasioExit> = Promise.resolve({ code: 0, signal: null });
  /** How alasio ended when it ended without being stopped. */
  let crashed: AlasioExit | null = null;

  async function start(): Promise<void> {
    if (child) throw new Error("alasio is already running");
    const started = fork(ALASIO_MAIN, [JSON.stringify(config)], { env, execArgv: [], stdio: ["ignore", "pipe", "pipe", "ipc"] });
    child = started;
    for (const stream of [started.stdout, started.stderr]) {
      stream?.on("data", (chunk: Buffer) => {
        output += chunk.toString();
        if (verbose) process.stdout.write(chunk);
      });
    }
    exited = new Promise((resolve) => started.on("exit", (code, signal) => {
      if (child === started) crashed = { code, signal };
      resolve({ code, signal });
    }));
    await new Promise<void>((resolve, reject) => {
      started.on("message", (message) => {
        if (message === READY) resolve();
      });
      void exited.then(({ code, signal }) => reject(new Error(`alasio exited (code ${code}, signal ${signal}) before it started:\n${output.slice(-4000)}`)));
    });
  }

  async function stop(): Promise<AlasioExit> {
    const running = child;
    if (!running) throw new Error("alasio is not running");
    child = null;
    running.kill("SIGTERM");
    const exit = await exited;
    await codex.allExited();
    return exit;
  }

  await start();
  return {
    telegram,
    codex,
    claude,
    workspaceRoot,
    folder: (name) => join(workspaceRoot, name),
    stop,
    start,
    readStore(read) {
      const store = new SqliteStore(stateDir, join(stateDir, "alasio.sqlite"));
      try {
        return read(store);
      } finally {
        store.close();
      }
    },
    async close() {
      try {
        if (crashed) throw new Error(`alasio exited on its own (code ${crashed.code}, signal ${crashed.signal}):\n${output.slice(-4000)}`);
        if (child) {
          const exit = await stop();
          if (exit.code !== 0) throw new Error(`alasio exited with code ${exit.code} as it stopped:\n${output.slice(-4000)}`);
        }
      } finally {
        await telegram.close();
        await codex.close();
        await claude.close();
        rmSync(root, { recursive: true, force: true });
      }
    },
  };
}
