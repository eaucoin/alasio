import "dotenv/config";
import { loadAlasioConfig } from "./config.js";
import { createLogger } from "./shared/log.js";
import { TelegramCodexApp } from "./telegram/app.js";

const log = createLogger("index");
const config = loadAlasioConfig();
const app = new TelegramCodexApp(config);

async function main() {
  await app.start();
  log.info("Telegram Alasio bot is running");
}

async function shutdown(signal) {
  log.info(`Received ${signal}, shutting down...`);
  try {
    await app.stop();
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
