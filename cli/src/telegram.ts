/**
 * Telegram's Bot API, as far as alasio's command line asks it: which bot a token is
 * (getMe), so a token is checked before it is kept. It only reads; it never sends a
 * message. The API is at TELEGRAM_API_ROOT, as for alasio itself, or Telegram's own.
 */
import { Config, Context, Effect, Layer, Redacted, Schema } from "effect";

const GetMeAnswer = Schema.Union([
  Schema.Struct({ ok: Schema.Literal(true), result: Schema.Struct({ username: Schema.String }) }),
  Schema.Struct({ ok: Schema.Literal(false), description: Schema.String }),
]);

/** A bot token could not be checked, or Telegram refused it. */
export class BotTokenError extends Schema.TaggedError<BotTokenError>()("BotTokenError", {
  message: Schema.String,
}) {}

/** Why `cause`, a failed fetch, failed: its own cause's message, as a fetch's says only that it failed. */
const fetchFailure = (cause: unknown): string =>
  cause instanceof Error ? (cause.cause instanceof Error ? cause.cause.message : cause.message) : String(cause);

export class TelegramBotApi extends Context.Service<TelegramBotApi, {
  /** The username of the bot `token` is. */
  readonly getMe: (token: Redacted.Redacted) => Effect.Effect<string, BotTokenError>;
}>()("alasio/TelegramBotApi") {
  static readonly layer: Layer.Layer<TelegramBotApi, Config.ConfigError> = Layer.effect(
    TelegramBotApi,
    Effect.map(Config.String("TELEGRAM_API_ROOT").pipe(Config.withDefault("https://api.telegram.org")), (root) =>
      TelegramBotApi.of({
        getMe: (token) =>
          Effect.tryPromise({
            try: async (signal) => (await fetch(`${root.replace(/\/$/u, "")}/bot${Redacted.value(token)}/getMe`, { signal })).json() as Promise<unknown>,
            // Told by its cause alone, as the URL holds the token.
            catch: (cause) => new BotTokenError({ message: `Telegram could not be asked about the bot token: ${fetchFailure(cause)}` }),
          }).pipe(
            Effect.flatMap((answer) =>
              Schema.decodeUnknownEffect(GetMeAnswer)(answer).pipe(
                Effect.mapError(() => new BotTokenError({ message: `${root} answered what Telegram's Bot API does not` })),
              )
            ),
            Effect.flatMap((answer) =>
              answer.ok ? Effect.succeed(answer.result.username) : Effect.fail(new BotTokenError({ message: `Telegram refused the bot token: ${answer.description}` }))
            ),
          ),
      })),
  );
}
