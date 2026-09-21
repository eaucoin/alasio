import "dotenv/config";
import { loadAlasioConfig } from "../config.js";
import { Client } from "./client.js";
import { SqliteStore } from "../persistence/store.js";

const args = new Set(process.argv.slice(2));
const config = loadAlasioConfig();
const client = new Client(config.telegramBotToken);
const store = new SqliteStore(config.workingDirectory);

try {
  const me = await client.getMe();
  const webhookInfo = await client.call("getWebhookInfo");
  console.log(JSON.stringify({
    ok: true,
    bot: {
      id: me.id,
      username: me.username ?? null,
      can_join_groups: me.can_join_groups ?? null,
      can_read_all_group_messages: me.can_read_all_group_messages ?? null,
      supports_inline_queries: me.supports_inline_queries ?? null,
    },
    webhook: {
      url_set: Boolean(webhookInfo.url),
      pending_update_count: webhookInfo.pending_update_count ?? null,
    },
    sqlite: {
      path: store.dbPath,
      schema_version: store.getState("schema_version"),
      telegram_bootstrap_user_id_set: Boolean(store.getState("telegram_bootstrap_user_id")),
      telegram_update_offset: store.getTelegramOffset() ?? null,
    },
  }, null, 2));

  if (args.has("--delete-webhook")) {
    await client.deleteWebhook(false);
    console.log("Deleted webhook without dropping pending updates.");
  }
} finally {
  store.close();
}
