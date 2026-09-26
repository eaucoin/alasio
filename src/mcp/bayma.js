/**
 * bayma, the MCP server alasio itself gives its agents, next to whatever
 * servers the operator has configured for each harness on this machine.
 *
 * bayma is a pinned npm dependency, so the server alasio launches is the
 * version in package-lock.json, run by alasio's own Node, with runtimes from
 * the payload its postinstall put in place; nothing is looked up on PATH.
 * Each harness adapter turns `baymaLaunch` into its own MCP config shape.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { resolveStateDir } from "../config.js";
import { createLogger } from "../shared/log.js";

export const BAYMA_SERVER_NAME = "bayma";

/** First launch may unpack the runtime payload, so allow well beyond a warm start. */
export const BAYMA_STARTUP_TIMEOUT_MS = 60_000;

const log = createLogger("bayma");
const require = createRequire(import.meta.url);

function resolveBaymaBin() {
  const manifestPath = require.resolve("@bayma-repl/bayma/package.json");
  return join(dirname(manifestPath), require(manifestPath).bin.bayma);
}

function sanitizePathToken(value) {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/**
 * Sessions are checkpointed rather than bayma's default, ephemeral, which
 * deletes them when the server stops, so a alasio restart would take every
 * session with it. A checkpointed session outlives the server as suspended,
 * with its history, and resumes with what its code checkpointed.
 */
function stdioCommand(stateDir) {
  return {
    command: process.execPath,
    args: [resolveBaymaBin(), "mcp-stdio", "--default-durability", "checkpointed", "--state-dir", stateDir],
  };
}

/**
 * The command that serves bayma to one conversation under one harness.
 *
 * bayma leases its state directory to a single server process, and a Codex
 * thread keeps its server alive after the conversation switches to Claude, so
 * the directory is keyed by harness as well as conversation. It lives under
 * alasio's state directory and is stable across restarts, so the next server
 * finds the previous one's sessions there.
 */
export function baymaLaunch({ harness, threadKey, env = process.env }) {
  return stdioCommand(join(
    resolveStateDir(env),
    "bayma",
    sanitizePathToken(harness),
    sanitizePathToken(threadKey),
  ));
}

async function checkBayma(env) {
  const stateDir = await mkdtemp(join(tmpdir(), "alasio-bayma-check-"));
  const client = new Client({ name: "alasio-bayma-check", version: "1.0.0" });
  try {
    const { command, args } = stdioCommand(stateDir);
    const timeout = { timeout: BAYMA_STARTUP_TIMEOUT_MS };
    await client.connect(new StdioClientTransport({ command, args, env, stderr: "ignore" }), timeout);
    const { tools } = await client.listTools(undefined, timeout);
    if (tools.length === 0) {
      throw new Error("bayma started but exposed no tools");
    }
    log.info(`ready (tools=${tools.length})`);
  } finally {
    await client.close().catch(() => {});
    await rm(stateDir, { recursive: true, force: true });
  }
}

let readiness = null;

/**
 * Prove once per alasio process that bayma starts and serves its tools, so a
 * harness never begins a turn believing it has a REPL it cannot reach. The
 * check runs against a throwaway state directory; a failure is retried on the
 * next turn.
 */
export function ensureBaymaReady(env) {
  readiness ??= checkBayma(env).catch((error) => {
    readiness = null;
    throw new Error(`bayma is unavailable: ${error instanceof Error ? error.message : String(error)}`);
  });
  return readiness;
}
