import { createHash } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { createWriteStream, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { setDefaultAutoSelectFamily } from "node:net";
import { pipeline } from "node:stream/promises";
import type { Span } from "@opentelemetry/api";
import type {
  ApiMethods,
  ApiResponse,
  BotCommand,
  File,
  InputRichMessage,
  MenuButton,
  Message,
  Opts,
  Update,
} from "@grammyjs/types";
import { rpcCall } from "../telemetry/index.ts";
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

/** A file a rich reply shows, as codex/reply-media.ts copies it for delivery. */
export interface MediaAttachment {
  /** The id its media line references (`tg://photo?id=<id>`), and its upload's name. */
  readonly id: string;
  readonly kind: MediaKind;
  /** Whether it is a GIF, which Telegram plays as an animation. */
  readonly animation: boolean;
  /** The path of the copy to upload. */
  readonly file: string;
}

export interface SendMessageOptions extends TextMessageOptions {
  /** The files a "rich" message's media lines show (see telegram/rich-media.ts). */
  readonly media?: readonly MediaAttachment[] | undefined;
  /** A directory of media copied for the reply, removed once it is delivered (telegram/outbox.ts). */
  readonly mediaDir?: string | undefined;
}

/** A text message's Bot API fields, with its text rendered as its format says. */
interface TextPayload extends TextMessageParams {
  chat_id: ChatId;
  text: string;
  // Superseded by link_preview_options, which is all @grammyjs/types describes; the
  // Bot API still honours it.
  disable_web_page_preview: boolean;
}

/** How a Bot API call is made. */
export interface RequestOptions {
  /** How many times a call Telegram rate-limits is retried after the wait it asks for. */
  readonly rateLimitRetries?: number | undefined;
  readonly signal?: AbortSignal | undefined;
}

export interface ClientOptions {
  readonly apiRoot?: string | undefined;
}

/** An upload of a multipart call: the file at `path`, as the part `name`. */
export interface Upload {
  readonly name: string;
  readonly path: string;
}

export interface GetUpdatesOptions {
  readonly offset?: number | undefined;
  readonly timeout?: number | undefined;
  readonly allowedUpdates?: BotParams<"getUpdates">["allowed_updates"] | undefined;
  readonly signal?: AbortSignal | undefined;
}

/** A Telegram file downloaded to a temporary directory of its own. */
export interface DownloadedFile {
  readonly localPath: string;
  readonly sha256: string;
  /** The file as getFile described it. */
  readonly remote: File;
}

/** A request's body, and the headers it needs. */
interface RequestBody {
  readonly headers?: Record<string, string>;
  readonly body: string | FormData;
}

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

/** The Bot API input media type for a prepared media item. */
function telegramMediaType(item: MediaAttachment): "animation" | "photo" | "video" {
  if (item.animation) return "animation";
  return item.kind === "photo" ? "photo" : "video";
}

function shouldRetryAsPlainText(error: unknown, options: TextMessageOptions): boolean {
  return (options.format ?? "markdown") === "markdown" && !options.parse_mode && String(error).includes("can't parse entities");
}

function isExpiredCallbackQueryError(error: unknown): boolean {
  return String(error).includes("query is too old and response timeout expired or query ID is invalid");
}

/** Whether Telegram refused a call as a bad request (HTTP 400), as a TelegramApiError's status says. */
function isBadRequest(error: unknown): boolean {
  return typeof error === "object" && error !== null && "status" in error && error.status === 400;
}

export class TelegramApiError extends Error {
  readonly status: number;
  /** The wait Telegram asked for before a retry, when it rate-limited the call; 0 otherwise. */
  readonly retryAfterMs: number;

  constructor(method: string, response: Pick<Response, "status">, data: ApiResponse<unknown> | null) {
    super(`Telegram ${method} failed: HTTP ${response.status} ${JSON.stringify(data)}`);
    this.name = "TelegramApiError";
    this.status = response.status;
    const parameters = data && !data.ok ? data.parameters : undefined;
    this.retryAfterMs = Number(parameters?.retry_after ?? 0) * 1000;
  }
}

/**
 * The Bot API server alasio talks to: Telegram's own unless TELEGRAM_API_ROOT names
 * another, such as a self-hosted telegram-bot-api server or a test's stand-in.
 */
export function telegramApiRoot(env: NodeJS.ProcessEnv = process.env): string {
  return (env["TELEGRAM_API_ROOT"]?.trim() || "https://api.telegram.org").replace(/\/+$/u, "");
}

export class Client {
  readonly token: string;
  private readonly apiBase: string;
  private readonly fileBase: string;
  private outboundTail: Promise<unknown>;

  constructor(token: string, { apiRoot = telegramApiRoot() }: ClientOptions = {}) {
    if (!token) {
      throw new Error("TELEGRAM_BOT_TOKEN is required");
    }
    this.token = token;
    this.apiBase = `${apiRoot}/bot${token}`;
    this.fileBase = `${apiRoot}/file/bot${token}`;
    this.outboundTail = Promise.resolve();
  }

  enqueueOutbound<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.outboundTail.then(operation, operation);
    this.outboundTail = result.catch(() => undefined);
    return result;
  }

  async call<M extends BotMethod>(method: M, payload?: BotParams<M>, options: RequestOptions = {}): Promise<BotResult<M>> {
    return await this.request(method, () => jsonBody(payload), options);
  }

  /**
   * A call with files: `fields` are sent as form fields (objects JSON-encoded, as the Bot
   * API reads them) and each of `files` (`{ name, path }`) as an upload, referenced from
   * the fields as `attach://<name>`.
   */
  async callMultipart<M extends BotMethod>(method: M, fields: BotParams<M>, files: readonly Upload[], options: RequestOptions = {}): Promise<BotResult<M>> {
    return await this.request(method, () => {
      const form = new FormData();
      for (const [key, value] of Object.entries(fields)) {
        if (value === undefined) continue;
        form.append(key, typeof value === "object" ? JSON.stringify(value) : String(value));
      }
      for (const file of files) {
        form.append(file.name, new Blob([readFileSync(file.path)]), basename(file.path));
      }
      return { body: form };
    }, options);
  }

  /** A Bot API call, as a client span and a duration named by its method. */
  async request<M extends BotMethod>(method: M, makeBody: () => RequestBody, options: RequestOptions = {}): Promise<BotResult<M>> {
    return await rpcCall({ system: "telegram", service: "telegram", method }, (span) => this.send(method, makeBody, options, span));
  }

  /**
   * Posts `method` to the Bot API, waiting out and retrying the rate limits it reports.
   * The URL holds the bot token, so only the method, never the URL, reaches the call's
   * `span`, if it has one.
   */
  async send<M extends BotMethod>(method: M, makeBody: () => RequestBody, options: RequestOptions = {}, span: Span | null = null): Promise<BotResult<M>> {
    const maxRateLimitRetries = options.rateLimitRetries ?? 3;
    for (let attempt = 0; ; attempt += 1) {
      const response = await fetch(`${this.apiBase}/${method}`, {
        method: "POST",
        ...makeBody(),
        signal: options.signal ?? null,
      });
      span?.setAttribute("http.response.status_code", response.status);
      // The Bot API answers every call with an ApiResponse; anything else reads as null.
      const data = await response.json().catch(() => null) as ApiResponse<BotResult<M>> | null;
      if (response.ok && data?.ok) {
        return data.result;
      }
      const error = new TelegramApiError(method, response, data);
      if (!error.retryAfterMs || attempt >= maxRateLimitRetries) {
        throw error;
      }
      span?.addEvent("rate_limited", { "telegram.retry_after_ms": error.retryAfterMs });
      await new Promise<void>((resolve, reject) => {
        let onAbort: (() => void) | undefined;
        const finish = () => {
          if (onAbort) {
            options.signal?.removeEventListener("abort", onAbort);
          }
          resolve();
        };
        const timer = setTimeout(finish, error.retryAfterMs + 100);
        if (options.signal) {
          onAbort = () => {
            clearTimeout(timer);
            reject(options.signal?.reason ?? new Error("Telegram request aborted"));
          };
          if (options.signal.aborted) {
            onAbort();
            return;
          }
          options.signal.addEventListener("abort", onAbort, { once: true });
        }
      });
    }
  }

  async getMe(): Promise<BotResult<"getMe">> {
    return this.call("getMe");
  }

  async deleteWebhook(dropPendingUpdates = false): Promise<BotResult<"deleteWebhook">> {
    return this.call("deleteWebhook", { drop_pending_updates: dropPendingUpdates });
  }

  async setMyCommands(commands: readonly BotCommand[]): Promise<BotResult<"setMyCommands">> {
    return this.call("setMyCommands", { commands });
  }

  async setChatMenuButton(menuButton: MenuButton = { type: "commands" }): Promise<BotResult<"setChatMenuButton">> {
    return this.call("setChatMenuButton", { menu_button: menuButton });
  }

  /**
   * A long poll: it lasts as long as Telegram has nothing to deliver, which says nothing
   * of the Bot API's latency, so it is sent without a span or a duration.
   */
  async getUpdates({ offset, timeout = 50, allowedUpdates = ["message", "callback_query"], signal }: GetUpdatesOptions = {}): Promise<Update[]> {
    return this.send("getUpdates", () => jsonBody({ offset, timeout, allowed_updates: allowedUpdates }), { signal });
  }

  /**
   * Send text. `format` is "markdown" (the default: a Markdown subset as Telegram HTML),
   * "plain", or "rich": Markdown as Telegram rich messages, which render tables, headings,
   * lists, and code natively, in parts of up to ~30k characters.
   */
  async sendMessage(chatId: ChatId, text: string, options: SendMessageOptions = {}): Promise<Message[]> {
    return await this.enqueueOutbound(async () => {
      if (options.format === "rich") {
        return await this.sendRichParts(chatId, text, options);
      }
      return await this.sendTextChunks(chatId, text, options);
    });
  }

  async sendTextChunks(chatId: ChatId, text: string, options: TextMessageOptions): Promise<Message[]> {
    const sent: Message[] = [];
    for (const chunk of splitTelegramText(text)) {
      const payload = buildTextPayload(chatId, chunk, options);
      try {
        sent.push(await this.call("sendMessage", payload));
      } catch (error) {
        if (!shouldRetryAsPlainText(error, options)) {
          throw error;
        }
        sent.push(await this.call("sendMessage", buildTextPayload(chatId, chunk, { ...options, format: "plain" })));
      }
    }
    return sent;
  }

  /**
   * Each part goes as a rich message, uploading the media it shows (`options.media`, see
   * telegram/rich-media.ts) with it. A part Telegram rejects as a request (HTTP 400: a
   * construct it will not take, or a limit the estimate missed) goes the classic way
   * instead, its media following as photos, videos, or an album, so a response is never
   * lost to its formatting.
   */
  async sendRichParts(chatId: ChatId, text: string, options: SendMessageOptions): Promise<Message[]> {
    const { format: _format, media = [], mediaDir: _mediaDir, ...telegramOptions } = options;
    const byId = new Map(media.map((item) => [item.id, item]));
    const sent: Message[] = [];
    for (const part of splitRichMarkdown(text || " ")) {
      const partMedia = [...new Set(mediaIdsIn(part))].map((id) => byId.get(id)).filter((item): item is MediaAttachment => Boolean(item));
      try {
        sent.push(await this.sendRichPart(chatId, part, partMedia, telegramOptions));
      } catch (error) {
        if (!isBadRequest(error)) {
          throw error;
        }
        sent.push(...await this.sendTextChunks(chatId, withoutMediaLines(part), { ...telegramOptions, format: "markdown" }));
        sent.push(...await this.sendMediaClassic(chatId, partMedia));
      }
    }
    return sent;
  }

  async sendRichPart(chatId: ChatId, part: string, partMedia: readonly MediaAttachment[], telegramOptions: TextMessageParams): Promise<BotResult<"sendRichMessage">> {
    const richMessage: InputRichMessage<never> = { markdown: toRichMarkdown(part) || " " };
    if (!partMedia.length) {
      return await this.call("sendRichMessage", { chat_id: chatId, rich_message: richMessage, ...telegramOptions });
    }
    richMessage.media = partMedia.map((item) => ({ id: item.id, media: { type: telegramMediaType(item), media: `attach://${item.id}` } }));
    return await this.callMultipart(
      "sendRichMessage",
      { chat_id: chatId, rich_message: richMessage, ...telegramOptions },
      partMedia.map((item) => ({ name: item.id, path: item.file })),
    );
  }

  /**
   * Media sent as their own messages (the fallback): one as a photo, video, or animation,
   * several as albums of up to ten, silently, since the text before them notified. A file
   * Telegram will not take as media goes as a document.
   */
  async sendMediaClassic(chatId: ChatId, items: readonly MediaAttachment[]): Promise<Message[]> {
    const sent: Message[] = [];
    const single = async (item: MediaAttachment): Promise<Message> => {
      const media = `attach://${item.id}`;
      const files = [{ name: item.id, path: item.file }];
      try {
        switch (telegramMediaType(item)) {
          case "photo":
            return await this.callMultipart("sendPhoto", { chat_id: chatId, photo: media, disable_notification: true }, files);
          case "video":
            return await this.callMultipart("sendVideo", { chat_id: chatId, video: media, disable_notification: true }, files);
          case "animation":
            return await this.callMultipart("sendAnimation", { chat_id: chatId, animation: media, disable_notification: true }, files);
        }
      } catch (error) {
        if (!isBadRequest(error)) throw error;
        return await this.callMultipart("sendDocument", { chat_id: chatId, document: media, disable_notification: true }, files);
      }
    };
    // Albums take photos and videos; an animation goes alone.
    const albumable = items.filter((item) => !item.animation);
    for (const item of items.filter((item) => item.animation)) sent.push(await single(item));
    for (let i = 0; i < albumable.length; i += 10) {
      const group = albumable.slice(i, i + 10);
      if (group.length === 1) {
        // A group of one has its item.
        sent.push(await single(group[0]!));
        continue;
      }
      try {
        sent.push(...await this.callMultipart(
          "sendMediaGroup",
          { chat_id: chatId, media: group.map((item) => ({ type: item.kind, media: `attach://${item.id}` })), disable_notification: true },
          group.map((item) => ({ name: item.id, path: item.file })),
        ));
      } catch (error) {
        if (!isBadRequest(error)) throw error;
        for (const item of group) sent.push(await single(item));
      }
    }
    return sent;
  }

  async editMessageText(chatId: ChatId, messageId: number, text: string, options: TextMessageOptions = {}): Promise<BotResult<"editMessageText">> {
    return await this.enqueueOutbound(async () => {
      const chunk = splitTelegramText(text)[0] || " ";
      const payload = {
        ...buildTextPayload(chatId, chunk, options),
        message_id: messageId,
      };
      try {
        return await this.call("editMessageText", payload);
      } catch (error) {
        if (!shouldRetryAsPlainText(error, options)) {
          throw error;
        }
        return this.call("editMessageText", {
          ...buildTextPayload(chatId, chunk, { ...options, format: "plain" }),
          message_id: messageId,
        });
      }
    });
  }

  async deleteMessage(chatId: ChatId, messageId: number): Promise<BotResult<"deleteMessage">> {
    return this.call("deleteMessage", {
      chat_id: chatId,
      message_id: messageId,
    });
  }

  /** Answers a callback query; null when the query is too old to answer. */
  async answerCallbackQuery(callbackQueryId: string, text = ""): Promise<BotResult<"answerCallbackQuery"> | null> {
    const payload: BotParams<"answerCallbackQuery"> = {
      callback_query_id: callbackQueryId,
    };
    if (text) {
      payload.text = text;
    }
    try {
      return await this.call("answerCallbackQuery", payload);
    } catch (error) {
      if (!isExpiredCallbackQueryError(error)) {
        throw error;
      }
      return null;
    }
  }

  async getFile(fileId: string): Promise<File> {
    return this.call("getFile", { file_id: fileId });
  }

  async downloadTelegramFile(file: Pick<File, "file_id">, preferredName = "download"): Promise<DownloadedFile> {
    const remote = await this.getFile(file.file_id);
    if (!remote.file_path) {
      throw new Error(`Telegram file ${file.file_id} had no file_path`);
    }
    const ext = extname(preferredName) || extname(remote.file_path) || "";
    const safeBase = basename(preferredName, ext).replace(/[^A-Za-z0-9_.-]+/g, "-") || "download";
    const tmpDir = mkdtempSync(join(tmpdir(), "telegram-file-"));
    const localPath = join(tmpDir, `${safeBase}${ext}`);
    const response = await fetch(`${this.fileBase}/${remote.file_path}`);
    if (!response.ok || !response.body) {
      throw new Error(`Telegram file download failed: HTTP ${response.status}`);
    }
    await pipeline(response.body, createWriteStream(localPath));
    const hash = createHash("sha256").update(readFileSync(localPath)).digest("hex");
    return { localPath, sha256: hash, remote };
  }

  async sendDocument(chatId: ChatId, filePath: string, caption?: string): Promise<BotResult<"sendDocument">> {
    const form = new FormData();
    form.append("chat_id", String(chatId));
    if (caption) {
      form.append("caption", caption.slice(0, 1024));
    }
    const file = new Blob([readFileSync(filePath)]);
    form.append("document", file, basename(filePath));
    const response = await fetch(`${this.apiBase}/sendDocument`, {
      method: "POST",
      body: form,
    });
    // The Bot API answers every call with an ApiResponse; anything else reads as null.
    const data = await response.json().catch(() => null) as ApiResponse<BotResult<"sendDocument">> | null;
    if (!response.ok || !data?.ok) {
      throw new Error(`Telegram sendDocument failed: HTTP ${response.status} ${JSON.stringify(data)}`);
    }
    return data.result;
  }
}
