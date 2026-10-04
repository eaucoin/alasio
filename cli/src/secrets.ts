/**
 * alasio's Secrets: what an operator gives alasio that is written nowhere but the
 * cluster, each in a Secret of alasio's own in its namespace, under a fixed name its
 * install configuration names (./config.ts): the bot's token and the Telegram users
 * allowed to use it, and Claude Code's token. In memory the tokens are Redacted, and only
 * the API server is sent them.
 */
import type { V1Secret } from "@kubernetes/client-node";
import { Effect, Redacted } from "effect";

import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { NAMESPACE } from "./manifests/common.ts";

/** The bot's Secret: its token (`token`) and the users allowed to use it (`allowedUserIds`, comma-separated). */
export const TELEGRAM_SECRET = "alasio-telegram";
/** Claude Code's Secret: its token (`token`), from `claude setup-token`. */
export const CLAUDE_SECRET = "alasio-claude";
/** The annotation of the bot's Secret that names the bot, which is no secret. */
const BOT_ANNOTATION = "alasio.dev/bot-username";

const SECRET = kind("Secret");

/** The bot alasio is, and who may use it. */
export interface TelegramBot {
  readonly token: Redacted.Redacted;
  readonly allowedUserIds: readonly string[];
  /** The bot's username, as Telegram said it when its token was given. */
  readonly username: string;
}

/** A Secret of alasio's holding `data`. */
function secret(name: string, data: Readonly<Record<string, string>>, annotations: Readonly<Record<string, string>> = {}): V1Secret {
  return {
    apiVersion: "v1",
    kind: "Secret",
    metadata: { name, namespace: NAMESPACE, labels: { "app.kubernetes.io/managed-by": "alasio", "app.kubernetes.io/part-of": "alasio" }, annotations },
    type: "Opaque",
    data: Object.fromEntries(Object.entries(data).map(([key, value]) => [key, Buffer.from(value, "utf8").toString("base64")])),
  };
}

/** The value of `key` in `found`'s data, or null when it has none. */
const valueOf = (found: V1Secret | null, key: string): string | null => {
  const encoded = found?.data?.[key];
  return encoded === undefined ? null : Buffer.from(encoded, "base64").toString("utf8");
};

/** Writes the bot's Secret. */
export const writeTelegramBot = (bot: TelegramBot): Effect.Effect<void, KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) =>
    kube.apply(secret(TELEGRAM_SECRET, { token: Redacted.value(bot.token), allowedUserIds: bot.allowedUserIds.join(",") }, { [BOT_ANNOTATION]: bot.username }))).pipe(
      Effect.asVoid,
    );

/** Writes Claude Code's Secret. */
export const writeClaudeToken = (token: Redacted.Redacted): Effect.Effect<void, KubeApiError, KubeApi> =>
  Effect.flatMap(KubeApi, (kube) => kube.apply(secret(CLAUDE_SECRET, { token: Redacted.value(token) }))).pipe(Effect.asVoid);

/** The bot of the bot's Secret, or null when there is none. */
export const readTelegramBot: Effect.Effect<TelegramBot | null, KubeApiError, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const found = yield* kube.get<V1Secret>({ ...SECRET, namespace: NAMESPACE, name: TELEGRAM_SECRET });
  const token = valueOf(found, "token");
  if (token === null) return null;
  return {
    token: Redacted.make(token),
    allowedUserIds: (valueOf(found, "allowedUserIds") ?? "").split(",").filter(Boolean),
    username: found?.metadata?.annotations?.[BOT_ANNOTATION] ?? "",
  };
});

/** Claude Code's token, or null when there is none. */
export const readClaudeToken: Effect.Effect<Redacted.Redacted | null, KubeApiError, KubeApi> = Effect.gen(function*() {
  const kube = yield* KubeApi;
  const token = valueOf(yield* kube.get<V1Secret>({ ...SECRET, namespace: NAMESPACE, name: CLAUDE_SECRET }), "token");
  return token === null ? null : Redacted.make(token);
});
