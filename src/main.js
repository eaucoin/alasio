import { loadAlasioConfig } from "./config.js";
import { codexHome } from "./codex/env.js";
import { startCodexRollouts } from "./codex/rollouts/index.js";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "./codex/rollouts/store.js";
import { sessionFsCodexHome } from "./codex/sessionfs.js";
import { startTranscriptSearch } from "./harness/claude/search/index.js";
import { loadSandboxConfig } from "./sandbox/config.js";
import { syncLakeReads } from "./neon/lake.js";
import { startNeon } from "./neon/stack.js";
import { createLogger } from "./shared/log.js";
import { stopTelemetry } from "./telemetry/start.js";
import { TelegramCodexApp } from "./telegram/app.js";

const log = createLogger("index");
const config = loadAlasioConfig();
let neon = null;
let app = null;
let search = null;
let codexRollouts = null;
let sessionFsCodexRollouts = null;

async function main() {
  // Claude Code's transcripts and Codex's rollouts are kept in alasio's Neon,
  // which runs before anything is served, and Codex's are mirrored from the
  // start so no turn runs unmirrored.
  neon = await startNeon({ stateDir: config.stateDir });
  codexRollouts = startCodexRollouts({ store: neon.rollouts, home: codexHome() });
  // Session filesystems (an empty, isolated workspace per session) are off unless
  // configured; the app builds the subsystem from this with its own store. Their Codex
  // runs from a home of its own, mirrored the same way into a schema of its own.
  const sandboxConfig = loadSandboxConfig();
  if (sandboxConfig) {
    const store = new NeonRolloutStore(neon.pool, { schema: SESSION_FS_SCHEMA });
    await store.ensureSchema();
    // The analytics lake loads this home too, once it may read it.
    await syncLakeReads(neon.pool, neon.lake);
    sessionFsCodexRollouts = startCodexRollouts({ store, home: sessionFsCodexHome(config.stateDir) });
  }
  app = new TelegramCodexApp({ ...config, sessionStore: neon.store, codexRollouts, sessionFsCodexRollouts, sandboxConfig });
  await app.start();
  log.info("Telegram Alasio bot is running");
  // Once the bot serves: the indexer's first pass reads every stored entry.
  search = startTranscriptSearch({ pool: neon.pool });
}

async function shutdown(signal) {
  log.info(`Received ${signal}, shutting down...`);
  try {
    await app?.stop();
    await codexRollouts?.close();
    await sessionFsCodexRollouts?.close();
    await search?.close();
    await neon?.close();
  } catch (error) {
    log.error(`Error during shutdown: ${error}`);
  }
  await stopTelemetry();
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch(async (error) => {
  log.error(`Fatal error: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
  await stopTelemetry();
  process.exit(1);
});
