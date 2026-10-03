// @ts-nocheck
import { createHash } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { createWriteStream, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { setDefaultAutoSelectFamily } from "node:net";
import { pipeline } from "node:stream/promises";
import { rpcCall } from "../telemetry/index.ts";
import { renderTelegramHtml } from "./markdown.ts";
import { splitRichMarkdown, toRichMarkdown } from "./rich-markdown.ts";
import { mediaIdsIn, withoutMediaLines } from "./rich-media.ts";
import { splitTelegramText } from "./text.ts";

// Telegram publishes IPv6 answers, but the admin server only has a working IPv4
// route to the Bot API. Keep Node fetch on the same address family as curl -4.
setDefaultResultOrder("ipv4first");
setDefaultAutoSelectFamily(false);

function buildTextPayload(chatId, text, options) {
  const { format = "markdown", ...telegramOptions } = options;
  const payload = {
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

function jsonBody(payload) {
  return { headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) };
}

/** The Bot API input media type for a prepared media item. */
function telegramMediaType(item) {
  if (item.animation) return "animation";
  return item.kind === "photo" ? "photo" : "video";
}

function shouldRetryAsPlainText(error, options) {
  return (options.format ?? "markdown") === "markdown" && !options.parse_mode && String(error).includes("can't parse entities");
}

function isExpiredCallbackQueryError(error) {
  return String(error).includes("query is too old and response timeout expired or query ID is invalid");
}

export class TelegramApiError extends Error {
  constructor(method, response, data) {
    super(`Telegram ${method} failed: HTTP ${response.status} ${JSON.stringify(data)}`);
    this.name = "TelegramApiError";
    this.status = response.status;
    this.retryAfterMs = Number(data?.parameters?.retry_after ?? 0) * 1000;
  }
}

/**
 * The Bot API server alasio talks to: Telegram's own unless TELEGRAM_API_ROOT names
 * another, such as a self-hosted telegram-bot-api server or a test's stand-in.
 */
export function telegramApiRoot(env = process.env) {
  return (env.TELEGRAM_API_ROOT?.trim() || "https://api.telegram.org").replace(/\/+$/u, "");
}

export class Client {
  constructor(token, { apiRoot = telegramApiRoot() } = {}) {
    if (!token) {
      throw new Error("TELEGRAM_BOT_TOKEN is required");
    }
    this.token = token;
    this.apiBase = `${apiRoot}/bot${token}`;
    this.fileBase = `${apiRoot}/file/bot${token}`;
    this.outboundTail = Promise.resolve();
  }

  enqueueOutbound(operation) {
    const result = this.outboundTail.then(operation, operation);
    this.outboundTail = result.catch(() => undefined);
    return result;
  }

  async call(method, payload = {}, options = {}) {
    return await this.request(method, () => jsonBody(payload), options);
  }

  /**
   * A call with files: `fields` are sent as form fields (objects JSON-encoded, as the Bot
   * API reads them) and each of `files` (`{ name, path }`) as an upload, referenced from
   * the fields as `attach://<name>`.
   */
  async callMultipart(method, fields, files, options = {}) {
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
  async request(method, makeBody, options = {}) {
    return await rpcCall({ system: "telegram", service: "telegram", method }, (span) => this.send(method, makeBody, options, span));
  }

  /**
   * Posts `method` to the Bot API, waiting out and retrying the rate limits it reports.
   * The URL holds the bot token, so only the method, never the URL, reaches the call's
   * `span`, if it has one.
   */
  async send(method, makeBody, options = {}, span = null) {
    const maxRateLimitRetries = options.rateLimitRetries ?? 3;
    for (let attempt = 0; ; attempt += 1) {
      const response = await fetch(`${this.apiBase}/${method}`, {
        method: "POST",
        ...makeBody(),
        signal: options.signal,
      });
      span?.setAttribute("http.response.status_code", response.status);
      const data = await response.json().catch(() => null);
      if (response.ok && data?.ok) {
        return data.result;
      }
      const error = new TelegramApiError(method, response, data);
      if (!error.retryAfterMs || attempt >= maxRateLimitRetries) {
        throw error;
      }
      span?.addEvent("rate_limited", { "telegram.retry_after_ms": error.retryAfterMs });
      await new Promise((resolve, reject) => {
        let onAbort;
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
            reject(options.signal.reason ?? new Error("Telegram request aborted"));
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

  async getMe() {
    return this.call("getMe");
  }

  async deleteWebhook(dropPendingUpdates = false) {
    return this.call("deleteWebhook", { drop_pending_updates: dropPendingUpdates });
  }

  async setMyCommands(commands) {
    return this.call("setMyCommands", { commands });
  }

  async setChatMenuButton(menuButton = { type: "commands" }) {
    return this.call("setChatMenuButton", { menu_button: menuButton });
  }

  /**
   * A long poll: it lasts as long as Telegram has nothing to deliver, which says nothing
   * of the Bot API's latency, so it is sent without a span or a duration.
   */
  async getUpdates({ offset, timeout = 50, allowedUpdates = ["message", "callback_query"], signal } = {}) {
    return this.send("getUpdates", () => jsonBody({ offset, timeout, allowed_updates: allowedUpdates }), { signal });
  }

  /**
   * Send text. `format` is "markdown" (the default: a Markdown subset as Telegram HTML),
   * "plain", or "rich": Markdown as Telegram rich messages, which render tables, headings,
   * lists, and code natively, in parts of up to ~30k characters.
   */
  async sendMessage(chatId, text, options = {}) {
    return await this.enqueueOutbound(async () => {
      if (options.format === "rich") {
        return await this.sendRichParts(chatId, text, options);
      }
      return await this.sendTextChunks(chatId, text, options);
    });
  }

  async sendTextChunks(chatId, text, options) {
    const sent = [];
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
  async sendRichParts(chatId, text, options) {
    const { format: _format, media = [], mediaDir: _mediaDir, ...telegramOptions } = options;
    const byId = new Map(media.map((item) => [item.id, item]));
    const sent = [];
    for (const part of splitRichMarkdown(text || " ")) {
      const partMedia = [...new Set(mediaIdsIn(part))].map((id) => byId.get(id)).filter(Boolean);
      try {
        sent.push(await this.sendRichPart(chatId, part, partMedia, telegramOptions));
      } catch (error) {
        if (error?.status !== 400) {
          throw error;
        }
        sent.push(...await this.sendTextChunks(chatId, withoutMediaLines(part), { ...telegramOptions, format: "markdown" }));
        sent.push(...await this.sendMediaClassic(chatId, partMedia));
      }
    }
    return sent;
  }

  async sendRichPart(chatId, part, partMedia, telegramOptions) {
    const richMessage = { markdown: toRichMarkdown(part) || " " };
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
  async sendMediaClassic(chatId, items) {
    const sent = [];
    const single = async (item) => {
      const type = telegramMediaType(item);
      const method = { photo: "sendPhoto", video: "sendVideo", animation: "sendAnimation" }[type];
      try {
        return await this.callMultipart(method, { chat_id: chatId, [type]: `attach://${item.id}`, disable_notification: true }, [{ name: item.id, path: item.file }]);
      } catch (error) {
        if (error?.status !== 400) throw error;
        return await this.callMultipart("sendDocument", { chat_id: chatId, document: `attach://${item.id}`, disable_notification: true }, [{ name: item.id, path: item.file }]);
      }
    };
    // Albums take photos and videos; an animation goes alone.
    const albumable = items.filter((item) => !item.animation);
    for (const item of items.filter((item) => item.animation)) sent.push(await single(item));
    for (let i = 0; i < albumable.length; i += 10) {
      const group = albumable.slice(i, i + 10);
      if (group.length === 1) {
        sent.push(await single(group[0]));
        continue;
      }
      try {
        sent.push(...await this.callMultipart(
          "sendMediaGroup",
          { chat_id: chatId, media: group.map((item) => ({ type: item.kind, media: `attach://${item.id}` })), disable_notification: true },
          group.map((item) => ({ name: item.id, path: item.file })),
        ));
      } catch (error) {
        if (error?.status !== 400) throw error;
        for (const item of group) sent.push(await single(item));
      }
    }
    return sent;
  }

  async editMessageText(chatId, messageId, text, options = {}) {
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

  async deleteMessage(chatId, messageId) {
    return this.call("deleteMessage", {
      chat_id: chatId,
      message_id: messageId,
    });
  }

  async answerCallbackQuery(callbackQueryId, text = "") {
    const payload = {
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

  async getFile(fileId) {
    return this.call("getFile", { file_id: fileId });
  }

  async downloadTelegramFile(file, preferredName = "download") {
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

  async sendDocument(chatId, filePath, caption) {
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
    const data = await response.json().catch(() => null);
    if (!response.ok || !data?.ok) {
      throw new Error(`Telegram sendDocument failed: HTTP ${response.status} ${JSON.stringify(data)}`);
    }
    return data.result;
  }
}
