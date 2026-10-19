/**
 * Telegram's Bot API, as far as alasio's command line asks it: which bot a token is
 * (getMe), so a token is checked before it is kept; where Telegram delivers a bot's
 * updates (setWebhook, deleteWebhook, getWebhookInfo), and what its menu offers
 * (setMyCommands, setChatMenuButton), which the command line alone says, as it registers
 * each environment's bot (./webhook.ts). It never sends a message. The API is at
 * TELEGRAM_API_ROOT, as for alasio itself, or Telegram's own.
 */
import { Config, Context, Effect, Layer, Redacted, Schema } from "effect";

import { BOT_COMMANDS } from "../../src/telegram/bot-commands.ts";

/** A Bot API answer of `result`'s shape, or its refusal. */
const Answer = <S extends Schema.Top>(result: S) =>
  Schema.Union([
    Schema.Struct({ ok: Schema.Literal(true), result }),
    Schema.Struct({ ok: Schema.Literal(false), description: Schema.String }),
  ]);

const GetMeAnswer = Answer(Schema.Struct({ username: Schema.String }));
const DoneAnswer = Answer(Schema.Boolean);
const WebhookInfoAnswer = Answer(Schema.Struct({
  url: Schema.String,
  pending_update_count: Schema.Number,
  last_error_date: Schema.optionalKey(Schema.Number),
  last_error_message: Schema.optionalKey(Schema.String),
}));

/** Where Telegram delivers a bot's updates, and how it fares. */
export type WebhookInfo = (typeof WebhookInfoAnswer.Type & { ok: true })["result"];

/** A bot token could not be checked, or Telegram refused it. */
export class BotTokenError extends Schema.TaggedError<BotTokenError>()("BotTokenError", {
  message: Schema.String,
}) {}

/** Telegram could not be asked about a bot's webhook, or refused what it was asked. */
export class WebhookError extends Schema.TaggedError<WebhookError>()("WebhookError", {
  message: Schema.String,
}) {}

/** Where a bot's updates are delivered: the URL, the certificate it serves (PEM), the secret token, and the address, where the URL names a host Telegram should not resolve. */
export interface Webhook {
  readonly url: string;
  readonly certificate: string;
  readonly secretToken: Redacted.Redacted;
  readonly ipAddress?: string | undefined;
}

/** The updates a bot is sent: those alasio acts on, as the webhook intake passes them on. */
export const ALLOWED_UPDATES = ["message", "callback_query"] as const;

/** Why `cause`, a failed fetch, failed: its own cause's message, as a fetch's says only that it failed. */
const fetchFailure = (cause: unknown): string =>
  cause instanceof Error ? (cause.cause instanceof Error ? cause.cause.message : cause.message) : String(cause);

export class TelegramBotApi extends Context.Service<TelegramBotApi, {
  /** The username of the bot `token` is. */
  readonly getMe: (token: Redacted.Redacted) => Effect.Effect<string, BotTokenError>;
  /**
   * Has Telegram deliver the bot's updates to `webhook`, those alasio acts on alone, a few
   * connections at a time, keeping what it holds undelivered: with the certificate uploaded,
   * as Telegram trusts a self-signed one so alone.
   */
  readonly setWebhook: (token: Redacted.Redacted, webhook: Webhook) => Effect.Effect<void, WebhookError>;
  /** Has Telegram deliver the bot's updates nowhere, keeping what it holds undelivered. */
  readonly deleteWebhook: (token: Redacted.Redacted) => Effect.Effect<void, WebhookError>;
  readonly getWebhookInfo: (token: Redacted.Redacted) => Effect.Effect<WebhookInfo, WebhookError>;
  /** Has the bot's menu offer alasio's commands (../../src/telegram/bot-commands.ts), its menu button opening them. */
  readonly setMenu: (token: Redacted.Redacted) => Effect.Effect<void, WebhookError>;
}>()("alasio/TelegramBotApi") {
  static readonly layer: Layer.Layer<TelegramBotApi, Config.ConfigError> = Layer.effect(
    TelegramBotApi,
    Effect.map(Config.String("TELEGRAM_API_ROOT").pipe(Config.withDefault("https://api.telegram.org")), (configured) => {
      const root = configured.replace(/\/$/u, "");
      /**
       * The bot `token`'s `method`, sent `body`, its answer as `answer` reads it; `failed`
       * makes the error of one not answered (`why` it was not) or answered as the Bot API does not.
       */
      const call = <S extends Schema.Top, E>(token: Redacted.Redacted, method: string, body: FormData | null, answer: S, failed: (why: string | null) => E) =>
        Effect.tryPromise({
          try: async (signal) => (await fetch(`${root}/bot${Redacted.value(token)}/${method}`, { method: body ? "POST" : "GET", signal, ...(body ? { body } : {}) })).json() as Promise<unknown>,
          // Told by its cause alone, as the URL holds the token.
          catch: (cause) => failed(fetchFailure(cause)),
        }).pipe(Effect.flatMap((answered) => Schema.decodeUnknownEffect(answer)(answered).pipe(Effect.mapError(() => failed(null)))));
      const done = (method: string) => (answered: typeof DoneAnswer.Type) =>
        answered.ok ? Effect.void : Effect.fail(new WebhookError({ message: `Telegram refused ${method}: ${answered.description}` }));
      const webhookError = (method: string) => (why: string | null) =>
        new WebhookError({ message: why === null ? `${root} answered ${method} as Telegram's Bot API does not` : `Telegram could not be asked to ${method}: ${why}` });
      return TelegramBotApi.of({
        getMe: (token) =>
          call(token, "getMe", null, GetMeAnswer, (why) =>
            new BotTokenError({ message: why === null ? `${root} answered what Telegram's Bot API does not` : `Telegram could not be asked about the bot token: ${why}` })).pipe(
            Effect.flatMap((answer) =>
              answer.ok ? Effect.succeed(answer.result.username) : Effect.fail(new BotTokenError({ message: `Telegram refused the bot token: ${answer.description}` }))
            ),
          ),
        setWebhook: (token, { url, certificate, secretToken, ipAddress }) => {
          const form = new FormData();
          form.set("url", url);
          form.set("certificate", new Blob([certificate], { type: "application/x-pem-file" }), "certificate.pem");
          form.set("secret_token", Redacted.value(secretToken));
          form.set("allowed_updates", JSON.stringify(ALLOWED_UPDATES));
          form.set("max_connections", "4");
          form.set("drop_pending_updates", "false");
          if (ipAddress) form.set("ip_address", ipAddress);
          return call(token, "setWebhook", form, DoneAnswer, webhookError("setWebhook")).pipe(Effect.flatMap(done("setWebhook")));
        },
        deleteWebhook: (token) => {
          const form = new FormData();
          form.set("drop_pending_updates", "false");
          return call(token, "deleteWebhook", form, DoneAnswer, webhookError("deleteWebhook")).pipe(Effect.flatMap(done("deleteWebhook")));
        },
        setMenu: (token) => {
          const commands = new FormData();
          commands.set("commands", JSON.stringify(BOT_COMMANDS));
          const button = new FormData();
          button.set("menu_button", JSON.stringify({ type: "commands" }));
          return call(token, "setMyCommands", commands, DoneAnswer, webhookError("setMyCommands")).pipe(
            Effect.flatMap(done("setMyCommands")),
            Effect.andThen(call(token, "setChatMenuButton", button, DoneAnswer, webhookError("setChatMenuButton"))),
            Effect.flatMap(done("setChatMenuButton")),
          );
        },
        getWebhookInfo: (token) =>
          call(token, "getWebhookInfo", null, WebhookInfoAnswer, webhookError("getWebhookInfo")).pipe(
            Effect.flatMap((answer) => (answer.ok ? Effect.succeed(answer.result) : Effect.fail(new WebhookError({ message: `Telegram refused getWebhookInfo: ${answer.description}` })))),
          ),
      });
    }),
  );
}
