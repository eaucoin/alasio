import { loadAlasioConfig } from "./config.js";
import { codexHome } from "./codex/env.js";
import { startCodexRollouts } from "./codex/rollouts/index.js";
import { NeonRolloutStore, SESSION_FS_SCHEMA } from "./codex/rollouts/store.js";
import { sessionFsCodexHome } from "./codex/sessionfs.js";
import { startTranscriptSearch } from "./harness/claude/search/index.js";
import { loadKubeTemplates } from "./kube/config.js";
import { syncLakeReads } from "./neon/lake.js";
import { connectNeon } from "./neon/connect.js";
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
  // What the deployment makes workspaces from, checked before anything else starts.
  const kubeTemplates = loadKubeTemplates();
  // Claude Code's transcripts and Codex's rollouts are kept in alasio's Neon, which the
  // deployment runs, and Codex's are mirrored from the start so no turn runs unmirrored.
  neon = await connectNeon();
  codexRollouts = startCodexRollouts({ store: neon.rollouts, home: codexHome() });
  // Session filesystems (an empty, isolated workspace per session) are on when the
  // deployment renders their template. Their Codex runs from a home of its own,
  // mirrored the same way into a schema of its own.
  if (kubeTemplates.sessions) {
    const store = new NeonRolloutStore(neon.pool, { schema: SESSION_FS_SCHEMA });
    await store.ensureSchema();
    // The analytics lake loads this home too, once it may read it.
    await syncLakeReads(neon.pool, neon.lake);
    sessionFsCodexRollouts = startCodexRollouts({ store, home: sessionFsCodexHome(config.stateDir) });
  }
  app = new TelegramCodexApp({ ...config, sessionStore: neon.store, codexRollouts, sessionFsCodexRollouts, kubeTemplates });
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
