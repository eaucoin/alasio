import "dotenv/config";
import { loadAlasioConfig } from "./config.js";
import { codexHome } from "./codex/env.js";
import { startCodexRollouts } from "./codex/rollouts/index.js";
import { startTranscriptSearch } from "./harness/claude/search/index.js";
import { startNeon } from "./neon/stack.js";
import { createLogger } from "./shared/log.js";
import { TelegramCodexApp } from "./telegram/app.js";

const log = createLogger("index");
const config = loadAlasioConfig();
let neon = null;
let app = null;
let search = null;
let codexRollouts = null;

async function main() {
  // Claude Code's transcripts and Codex's rollouts are kept in alasio's Neon,
  // which runs before anything is served, and Codex's are mirrored from the
  // start so no turn runs unmirrored.
  neon = await startNeon({ stateDir: config.stateDir });
  codexRollouts = startCodexRollouts({ store: neon.rollouts, home: codexHome() });
  app = new TelegramCodexApp({ ...config, sessionStore: neon.store, codexRollouts });
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
    await search?.close();
    await neon?.close();
  } catch (error) {
    log.error(`Error during shutdown: ${error}`);
  }
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

main().catch((error) => {
  log.error(`Fatal error: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
  process.exit(1);
});
