import "dotenv/config";
import { Effect } from "effect";
import { loadAlasioConfig } from "../config.ts";
import { TelegramClient } from "./client.ts";
import { Store } from "../persistence/store.ts";

const args = new Set(process.argv.slice(2));
const config = loadAlasioConfig();

const doctor = Effect.gen(function*() {
  const client = yield* TelegramClient;
  const store = yield* Store;
  const me = yield* client.getMe;
  const webhookInfo = yield* client.call("getWebhookInfo");
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
    yield* client.deleteWebhook(false);
    console.log("Deleted webhook without dropping pending updates.");
  }
});

await Effect.runPromise(doctor.pipe(Effect.provide([TelegramClient.layer(config.telegramBotToken), Store.layer(config)])));
