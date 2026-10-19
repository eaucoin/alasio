import { createHash } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { readFileSync } from "node:fs";
import { basename, extname } from "node:path";
import { setDefaultAutoSelectFamily } from "node:net";
import type {
  ApiMethods,
  ApiResponse,
  File,
  InputRichMessage,
  Message,
  Opts,
} from "@grammyjs/types";
import { Clock, Config, Context, Effect, Layer, Schema, Semaphore } from "effect";
import { FetchHttpClient } from "effect/http";
import { withRpcCall } from "../telemetry/index.ts";
import { renderTelegramHtml } from "./markdown.ts";
import { splitRichMarkdown, toRichMarkdown } from "./rich-markdown.ts";
import { type MediaKind, mediaIdsIn, withoutMediaLines } from "./rich-media.ts";
import { splitTelegramText } from "./text.ts";

// Telegram publishes IPv6 answers, but the admin server only has a working IPv4
// route to the Bot API. Keep Node fetch on the same address family as curl -4.
setDefaultResultOrder("ipv4first");
setDefaultAutoSelectFamily(false);

// Files go up as multipart parts, referenced from the fields as `attach://<name>`
// strings, never as InputFile objects, hence `never` for the Bot API's file type.
type BotApi = ApiMethods<never>;

/** A Bot API method's name. */
export type BotMethod = keyof BotApi;

/** The parameters of a Bot API method. */
export type BotParams<M extends BotMethod> = Opts<never>[M];

/** What a Bot API method returns. */
export type BotResult<M extends BotMethod> = ReturnType<BotApi[M]>;

export type ChatId = BotParams<"sendMessage">["chat_id"];

/**
 * How text is rendered: "markdown" (a Markdown subset as Telegram HTML), "plain", or
 * "rich" (Markdown as a Telegram rich message; sendMessage only).
 */
export type TextFormat = "markdown" | "plain" | "rich";

/** The Bot API fields a text message is sent or edited with, besides its chat, id, and text. */
type TextMessageParams = Pick<BotParams<"editMessageText">, "parse_mode" | "entities" | "link_preview_options" | "reply_markup">;

/** How a text message is sent or edited: its format, and Bot API fields passed through. */
export interface TextMessageOptions extends TextMessageParams {
  readonly format?: TextFormat | undefined;
}

/** A file a rich reply shows, as codex/reply-media.ts reads it for delivery. */
export interface MediaAttachment {
  /** The id its media line references (`tg://photo?id=<id>`), and its upload's name. */
  readonly id: string;
  readonly kind: MediaKind;
  /** Whether it is a GIF, which Telegram plays as an animation. */
  readonly animation: boolean;
  /** The file name it is uploaded under. */
  readonly fileName: string;
  readonly content: Uint8Array;
}

export interface SendMessageOptions extends TextMessageOptions {
  /** The files a "rich" message's media lines show (see telegram/rich-media.ts). */
  readonly media?: readonly MediaAttachment[] | undefined;
}

/** A text message's Bot API fields, with its text rendered as its format says. */
interface TextPayload extends TextMessageParams {
  chat_id: ChatId;
  text: string;
  // Superseded by link_preview_options, which is all @grammyjs/types describes; the
  // Bot API still honours it.
  disable_web_page_preview: boolean;
}

/** An upload of a multipart call: `content`, as the part `name`, under the file name `fileName`. */
export interface Upload {
  readonly name: string;
  readonly fileName: string;
  readonly content: Uint8Array;
}

/** A Telegram file downloaded, whole. */
export interface DownloadedFile {
  /** A name for it, safe to write it under: the one preferred, with the file's extension. */
  readonly name: string;
  readonly content: Uint8Array;
  readonly sha256: string;
  /** The file as getFile described it. */
  readonly remote: File;
}

/** A request's body, and the headers it needs. */
interface RequestBody {
  readonly headers?: Record<string, string>;
  readonly body: string | FormData;
}

/** How a Bot API call is posted. */
interface SendOptions {
  /** Whether the call is a span of its own, which records its HTTP status and rate limits. */
  readonly traced: boolean;
  /** How many times a call Telegram rate-limits is retried after the wait it asks for. */
  readonly rateLimitRetries: number;
  /** How long the call waits for Telegram's answer before it fails, as a call that reached Telegram not. */
  readonly timeoutMs: number;
  /** The message the call is a part of, which knows whether Telegram took a call of it yet: a call of none, by itself. */
  readonly message?: MessageSending | undefined;
}

/**
 * A message that goes as several calls (its text in chunks, its rich parts, its media),
 * and whether Telegram has taken one of them yet.
 */
interface MessageSending {
  accepted: boolean;
}

/** How long a call waits for Telegram, and one that uploads files. */
const CALL_TIMEOUT_MS = 30_000;
const UPLOAD_TIMEOUT_MS = 120_000;

/** Telegram refused a call: its HTTP status, what it answered, and the wait it asked for. */
export class TelegramApiError extends Schema.TaggedError<TelegramApiError>()("TelegramApiError", {
  method: Schema.String,
  status: Schema.Number,
  /** Telegram's description of the refusal, when it gave one. */
  description: Schema.optional(Schema.String),
  /** The wait Telegram asked for before a retry, when it rate-limited the call; 0 otherwise. */
  retryAfterMs: Schema.Number,
  message: Schema.String,
}) {
  /** The refusal `data` (null for a body that was not JSON) answered `method` with. */
  static fromResponse(method: string, status: number, data: ApiResponse<unknown> | null): TelegramApiError {
    const refusal = data && !data.ok ? data : undefined;
    return new TelegramApiError({
      method,
      status,
      ...(refusal?.description === undefined ? {} : { description: refusal.description }),
      retryAfterMs: Number(refusal?.parameters?.retry_after ?? 0) * 1000,
      message: `Telegram ${method} failed: HTTP ${status} ${JSON.stringify(data)}`,
    });
  }
}

/** A call that never reached Telegram, or whose answer never came back: fetch failed, or its upload could not be read. */
export class TelegramTransportError extends Schema.TaggedError<TelegramTransportError>()("TelegramTransportError", {
  method: Schema.String,
  message: Schema.String,
  cause: Schema.Defect(),
}) {
  static of(method: string, cause: unknown): TelegramTransportError {
    return new TelegramTransportError({ method, message: cause instanceof Error ? cause.message : String(cause), cause });
  }

  // Read as the failure it wraps, as the logs and the outbox's last_error always have.
  override toString(): string {
    return String(this.cause);
  }
}

/** A Telegram file that could not be downloaded. */
export class TelegramFileError extends Schema.TaggedError<TelegramFileError>()("TelegramFileError", {
  message: Schema.String,
}) {}

/** How a Bot API call fails. */
export type TelegramError = TelegramApiError | TelegramTransportError;

function buildTextPayload(chatId: ChatId, text: string, options: TextMessageOptions): TextPayload {
  const { format = "markdown", ...telegramOptions } = options;
  const payload: TextPayload = {
    chat_id: chatId,
    text: text || " ",
    disable_web_page_preview: true,
    ...telegramOptions,
  };
  if (format === "markdown" && !payload.parse_mode) {
    payload.text = renderTelegramHtml(text || " ");
    payload.parse_mode = "HTML";
  }
  return payload;
}

function jsonBody(payload: object = {}): RequestBody {
  return { headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };
}

/** A media item as its upload, the part its id names. */
function uploadOf(item: MediaAttachment): Upload {
  return { name: item.id, fileName: item.fileName, content: item.content };
}

/** The Bot API input media type for a prepared media item. */
function telegramMediaType(item: MediaAttachment): "animation" | "photo" | "video" {
  if (item.animation) return "animation";
  return item.kind === "photo" ? "photo" : "video";
}

/** Whether Telegram refused Markdown rendered as HTML, which goes again as plain text. */
const shouldRetryAsPlainText = (options: TextMessageOptions) => (error: TelegramError): boolean =>
  (options.format ?? "markdown") === "markdown" && !options.parse_mode
  && error._tag === "TelegramApiError" && error.description?.includes("can't parse entities") === true;

function isExpiredCallbackQueryError(error: TelegramError): boolean {
  return error._tag === "TelegramApiError" && error.description?.includes("query is too old and response timeout expired or query ID is invalid") === true;
}

/** Whether Telegram refused a call as a bad request (HTTP 400). */
function isBadRequest(error: TelegramError): boolean {
  return error._tag === "TelegramApiError" && error.status === 400;
}

/** Records on the call's span that Telegram rate-limited it. */
const recordRateLimit = (retryAfterMs: number): Effect.Effect<void> =>
  Effect.currentSpan.pipe(
    Effect.flatMap((span) => Clock.currentTimeNanos.pipe(
      Effect.map((now) => span.event("rate_limited", now, { "telegram.retry_after_ms": retryAfterMs })),
    )),
    Effect.catchTag("NoSuchElementError", () => Effect.void),
  );

/**
 * The Bot API server alasio talks to: Telegram's own unless TELEGRAM_API_ROOT names
 * another, such as a self-hosted telegram-bot-api server or a test's stand-in.
 */
const TelegramApiRoot: Config.Config<string> = Config.String("TELEGRAM_API_ROOT").pipe(
  Config.withDefault(""),
  Config.map((root) => (root.trim() || "https://api.telegram.org").replace(/\/+$/u, "")),
);

/** The Telegram Bot API, as alasio calls it. */
export class TelegramClient extends Context.Service<TelegramClient, {
  /** A JSON call. */
  readonly call: <M extends BotMethod>(method: M, payload?: BotParams<M>) => Effect.Effect<BotResult<M>, TelegramError>;
  /**
   * A call with files: `fields` are sent as form fields (objects JSON-encoded, as the Bot
   * API reads them) and each of `files` as an upload, referenced from the fields as
   * `attach://<name>`.
   */
  readonly callMultipart: <M extends BotMethod>(method: M, fields: BotParams<M>, files: readonly Upload[]) => Effect.Effect<BotResult<M>, TelegramError>;
  readonly getMe: Effect.Effect<BotResult<"getMe">, TelegramError>;
  /**
   * Sends text, after the messages sent or edited before it. `format` is "markdown" (the
   * default: a Markdown subset as Telegram HTML), "plain", or "rich": Markdown as Telegram
   * rich messages, which render tables, headings, lists, and code natively, in parts of
   * up to ~30k characters.
   */
  readonly sendMessage: (chatId: ChatId, text: string, options?: SendMessageOptions) => Effect.Effect<readonly Message[], TelegramError>;
  /** Edits a message's text, after the messages sent or edited before it. */
  readonly editMessageText: (chatId: ChatId, messageId: number, text: string, options?: TextMessageOptions) => Effect.Effect<BotResult<"editMessageText">, TelegramError>;
  readonly deleteMessage: (chatId: ChatId, messageId: number) => Effect.Effect<BotResult<"deleteMessage">, TelegramError>;
  /** Answers a callback query; null when the query is too old to answer. */
  readonly answerCallbackQuery: (callbackQueryId: string, text?: string) => Effect.Effect<BotResult<"answerCallbackQuery"> | null, TelegramError>;
  readonly getFile: (fileId: string) => Effect.Effect<File, TelegramError>;
  readonly downloadTelegramFile: (file: Pick<File, "file_id">, preferredName?: string) => Effect.Effect<DownloadedFile, TelegramError | TelegramFileError>;
  /** A document upload, sent as it is: no span, and no retry when Telegram rate-limits it. */
  readonly sendDocument: (chatId: ChatId, filePath: string, caption?: string) => Effect.Effect<BotResult<"sendDocument">, TelegramError>;
}>()("alasio/telegram/TelegramClient") {
  /** The Bot API for the bot `token`, at TELEGRAM_API_ROOT, called with FetchHttpClient.Fetch. */
  static readonly layer = (token: string): Layer.Layer<TelegramClient> => Layer.effect(TelegramClient, makeTelegramClient(token));
}

const makeTelegramClient = Effect.fnUntraced(function*(token: string): Effect.fn.Return<TelegramClient["Service"]> {
  if (!token) {
    return yield* Effect.die(new Error("TELEGRAM_BOT_TOKEN is required"));
  }
  const apiRoot = yield* Effect.orDie(TelegramApiRoot);
  const fetch = yield* FetchHttpClient.Fetch;
  const apiBase = `${apiRoot}/bot${token}`;
  const fileBase = `${apiRoot}/file/bot${token}`;
  // Messages go out one at a time, in the order they are sent or edited.
  const outbound = yield* Semaphore.make(1);

  /**
   * Posts `method` to the Bot API, waiting out and retrying the rate limits it reports,
   * each attempt failing once it has waited `timeoutMs` for Telegram's answer. The URL
   * holds the bot token, so only the method, never the URL, reaches the call's span, if it
   * has one.
   *
   * A rate limit's wait may be interrupted even where the call is not (the outbox delivers
   * each entry so) as long as Telegram has taken nothing of the message the call is part
   * of: an interruption then sends none of it twice. Once it has taken a part, the wait is
   * as the call is, so the message is not cut short to go again whole. An attempt is never
   * interrupted mid-way where the call is not, for Telegram may have taken it: its timeout
   * bounds it.
   */
  const send = <M extends BotMethod>(method: M, makeBody: () => RequestBody, { traced, rateLimitRetries, timeoutMs, message }: SendOptions): Effect.Effect<BotResult<M>, TelegramError> => {
    const attempt = (retriesLeft: number): Effect.Effect<BotResult<M>, TelegramError> => Effect.gen(function*() {
      const body = yield* Effect.try({ try: makeBody, catch: (cause) => TelegramTransportError.of(method, cause) });
      const response = yield* Effect.tryPromise({
        try: (signal) => fetch(`${apiBase}/${method}`, { method: "POST", ...body, signal: AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) }),
        catch: (cause) => TelegramTransportError.of(method, cause),
      });
      if (traced) yield* Effect.annotateCurrentSpan("http.response.status_code", response.status);
      // The Bot API answers every call with an ApiResponse; anything else reads as null.
      const data = yield* Effect.promise(() => response.json().catch(() => null) as Promise<ApiResponse<BotResult<M>> | null>);
      if (response.ok && data?.ok) {
        if (message) message.accepted = true;
        return data.result;
      }
      const error = TelegramApiError.fromResponse(method, response.status, data);
      if (!error.retryAfterMs || retriesLeft === 0) {
        return yield* error;
      }
      if (traced) yield* recordRateLimit(error.retryAfterMs);
      const wait = Effect.sleep(error.retryAfterMs + 100);
      yield* message?.accepted ? wait : Effect.interruptible(wait);
      return yield* attempt(retriesLeft - 1);
    });
    return attempt(rateLimitRetries);
  };

  /** A Bot API call, as a client span and a duration named by its method; part of `message`, where given. */
  const request = <M extends BotMethod>(method: M, makeBody: () => RequestBody, timeoutMs: number, message?: MessageSending): Effect.Effect<BotResult<M>, TelegramError> =>
    send(method, makeBody, { traced: true, rateLimitRetries: 3, timeoutMs, message }).pipe(withRpcCall({ system: "telegram", service: "telegram", method }));

  const call = <M extends BotMethod>(method: M, payload?: BotParams<M>, message?: MessageSending): Effect.Effect<BotResult<M>, TelegramError> =>
    request(method, () => jsonBody(payload), CALL_TIMEOUT_MS, message);

  const callMultipart = <M extends BotMethod>(method: M, fields: BotParams<M>, files: readonly Upload[], message?: MessageSending): Effect.Effect<BotResult<M>, TelegramError> =>
    request(method, () => {
      const form = new FormData();
      for (const [key, value] of Object.entries(fields)) {
        if (value === undefined) continue;
        form.append(key, typeof value === "object" ? JSON.stringify(value) : String(value));
      }
      for (const file of files) {
        form.append(file.name, new Blob([file.content]), file.fileName);
      }
      return { body: form };
    }, UPLOAD_TIMEOUT_MS, message);

  const sendTextChunks = Effect.fnUntraced(function*(chatId: ChatId, text: string, options: TextMessageOptions, message: MessageSending): Effect.fn.Return<Message[], TelegramError> {
    const sent: Message[] = [];
    for (const chunk of splitTelegramText(text)) {
      sent.push(yield* call("sendMessage", buildTextPayload(chatId, chunk, options), message).pipe(
        Effect.catchIf(shouldRetryAsPlainText(options), () => call("sendMessage", buildTextPayload(chatId, chunk, { ...options, format: "plain" }), message)),
      ));
    }
    return sent;
  });

  const sendRichPart = (chatId: ChatId, part: string, partMedia: readonly MediaAttachment[], telegramOptions: TextMessageParams, message: MessageSending): Effect.Effect<Message, TelegramError> => {
    const richMessage: InputRichMessage<never> = { markdown: toRichMarkdown(part) || " " };
    if (!partMedia.length) {
      return call("sendRichMessage", { chat_id: chatId, rich_message: richMessage, ...telegramOptions }, message);
    }
    richMessage.media = partMedia.map((item) => ({ id: item.id, media: { type: telegramMediaType(item), media: `attach://${item.id}` } }));
    return callMultipart(
      "sendRichMessage",
      { chat_id: chatId, rich_message: richMessage, ...telegramOptions },
      partMedia.map(uploadOf),
      message,
    );
  };

  /** One media item as its own message: a photo, video, or animation, or a document when Telegram will not take it as media. */
  const sendMediaItem = (chatId: ChatId, item: MediaAttachment, message: MessageSending): Effect.Effect<Message, TelegramError> => {
    const media = `attach://${item.id}`;
    const files = [uploadOf(item)];
    const asMedia = (): Effect.Effect<Message, TelegramError> => {
      switch (telegramMediaType(item)) {
        case "photo":
          return callMultipart("sendPhoto", { chat_id: chatId, photo: media, disable_notification: true }, files, message);
        case "video":
          return callMultipart("sendVideo", { chat_id: chatId, video: media, disable_notification: true }, files, message);
        case "animation":
          return callMultipart("sendAnimation", { chat_id: chatId, animation: media, disable_notification: true }, files, message);
      }
    };
    return asMedia().pipe(
      Effect.catchIf(isBadRequest, () => callMultipart("sendDocument", { chat_id: chatId, document: media, disable_notification: true }, files, message)),
    );
  };

  /**
   * Media sent as their own messages (the fallback): one as a photo, video, or animation,
   * several as albums of up to ten, silently, since the text before them notified.
   */
  const sendMediaClassic = Effect.fnUntraced(function*(chatId: ChatId, items: readonly MediaAttachment[], message: MessageSending): Effect.fn.Return<Message[], TelegramError> {
    const sent: Message[] = [];
    // Albums take photos and videos; an animation goes alone.
    const albumable = items.filter((item) => !item.animation);
    for (const item of items.filter((item) => item.animation)) sent.push(yield* sendMediaItem(chatId, item, message));
    for (let i = 0; i < albumable.length; i += 10) {
      const group = albumable.slice(i, i + 10);
      const [only] = group;
      if (only !== undefined && group.length === 1) {
        sent.push(yield* sendMediaItem(chatId, only, message));
        continue;
      }
      sent.push(...yield* callMultipart(
        "sendMediaGroup",
        { chat_id: chatId, media: group.map((item) => ({ type: item.kind, media: `attach://${item.id}` })), disable_notification: true },
        group.map(uploadOf),
        message,
      ).pipe(
        Effect.catchIf(isBadRequest, () => Effect.forEach(group, (item) => sendMediaItem(chatId, item, message))),
      ));
    }
    return sent;
  });

  /**
   * Each part goes as a rich message, uploading the media it shows (`options.media`, see
   * telegram/rich-media.ts) with it. A part Telegram rejects as a request (HTTP 400: a
   * construct it will not take, or a limit the estimate missed) goes the classic way
   * instead, its media following as photos, videos, or an album, so a response is never
   * lost to its formatting.
   */
  const sendRichParts = Effect.fnUntraced(function*(chatId: ChatId, text: string, options: SendMessageOptions, message: MessageSending): Effect.fn.Return<Message[], TelegramError> {
    const { format: _format, media = [], ...telegramOptions } = options;
    const byId = new Map(media.map((item) => [item.id, item]));
    const sent: Message[] = [];
    for (const part of splitRichMarkdown(text || " ")) {
      const partMedia = [...new Set(mediaIdsIn(part))].map((id) => byId.get(id)).filter((item): item is MediaAttachment => Boolean(item));
      sent.push(...yield* sendRichPart(chatId, part, partMedia, telegramOptions, message).pipe(
        Effect.map((sentPart) => [sentPart]),
        Effect.catchIf(isBadRequest, () => Effect.all([
          sendTextChunks(chatId, withoutMediaLines(part), { ...telegramOptions, format: "markdown" }, message),
          sendMediaClassic(chatId, partMedia, message),
        ]).pipe(Effect.map(([texts, media]) => [...texts, ...media]))),
      ));
    }
    return sent;
  });

  const getFile = (fileId: string): Effect.Effect<File, TelegramError> => call("getFile", { file_id: fileId });

  return TelegramClient.of({
    call,
    callMultipart,
    getMe: call("getMe"),
    sendMessage: (chatId, text, options = {}) =>
      Effect.suspend(() => {
        const message: MessageSending = { accepted: false };
        return outbound.withPermit(options.format === "rich" ? sendRichParts(chatId, text, options, message) : sendTextChunks(chatId, text, options, message));
      }),
    editMessageText: (chatId, messageId, text, options = {}) => {
      const chunk = splitTelegramText(text)[0] || " ";
      const edit = (format: TextMessageOptions) => call("editMessageText", { ...buildTextPayload(chatId, chunk, format), message_id: messageId });
      return outbound.withPermit(edit(options).pipe(
        Effect.catchIf(shouldRetryAsPlainText(options), () => edit({ ...options, format: "plain" })),
      ));
    },
    deleteMessage: (chatId, messageId) => call("deleteMessage", { chat_id: chatId, message_id: messageId }),
    answerCallbackQuery: (callbackQueryId, text = "") => {
      const payload: BotParams<"answerCallbackQuery"> = { callback_query_id: callbackQueryId };
      if (text) {
        payload.text = text;
      }
      return call("answerCallbackQuery", payload).pipe(
        Effect.catchIf(isExpiredCallbackQueryError, () => Effect.succeed(null)),
      );
    },
    getFile,
    downloadTelegramFile: Effect.fnUntraced(function*(file: Pick<File, "file_id">, preferredName = "download"): Effect.fn.Return<DownloadedFile, TelegramError | TelegramFileError> {
      const remote = yield* getFile(file.file_id);
      const filePath = remote.file_path;
      if (!filePath) {
        return yield* new TelegramFileError({ message: `Telegram file ${file.file_id} had no file_path` });
      }
      const ext = extname(preferredName) || extname(filePath) || "";
      const safeBase = basename(preferredName, ext).replace(/[^A-Za-z0-9_.-]+/g, "-") || "download";
      const response = yield* Effect.tryPromise({
        try: (signal) => fetch(`${fileBase}/${filePath}`, { signal: AbortSignal.any([signal, AbortSignal.timeout(UPLOAD_TIMEOUT_MS)]) }),
        catch: (cause) => TelegramTransportError.of("getFile", cause),
      });
      if (!response.ok || !response.body) {
        return yield* new TelegramFileError({ message: `Telegram file download failed: HTTP ${response.status}` });
      }
      const content = new Uint8Array(yield* Effect.tryPromise({
        try: () => response.arrayBuffer(),
        catch: (cause) => new TelegramFileError({ message: cause instanceof Error ? cause.message : String(cause) }),
      }));
      return { name: `${safeBase}${ext}`, content, sha256: createHash("sha256").update(content).digest("hex"), remote };
    }),
    sendDocument: (chatId, filePath, caption) =>
      send("sendDocument", () => {
        const form = new FormData();
        form.append("chat_id", String(chatId));
        if (caption) {
          form.append("caption", caption.slice(0, 1024));
        }
        form.append("document", new Blob([readFileSync(filePath)]), basename(filePath));
        return { body: form };
      }, { traced: false, rateLimitRetries: 0, timeoutMs: UPLOAD_TIMEOUT_MS }),
  });
});
