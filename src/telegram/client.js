import { createHash } from "node:crypto";
import { setDefaultResultOrder } from "node:dns";
import { createWriteStream, readFileSync } from "node:fs";
import { basename, extname, join } from "node:path";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { setDefaultAutoSelectFamily } from "node:net";
import { pipeline } from "node:stream/promises";
import { renderTelegramHtml } from "./markdown.js";
import { splitRichMarkdown, toRichMarkdown } from "./rich-markdown.js";
import { splitTelegramText } from "./text.js";

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

export class Client {
  constructor(token) {
    if (!token) {
      throw new Error("TELEGRAM_BOT_TOKEN is required");
    }
    this.token = token;
    this.apiBase = `https://api.telegram.org/bot${token}`;
    this.fileBase = `https://api.telegram.org/file/bot${token}`;
    this.outboundTail = Promise.resolve();
  }

  enqueueOutbound(operation) {
    const result = this.outboundTail.then(operation, operation);
    this.outboundTail = result.catch(() => undefined);
    return result;
  }

  async call(method, payload = {}, options = {}) {
    const maxRateLimitRetries = options.rateLimitRetries ?? 3;
    for (let attempt = 0; ; attempt += 1) {
      const response = await fetch(`${this.apiBase}/${method}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: options.signal,
      });
      const data = await response.json().catch(() => null);
      if (response.ok && data?.ok) {
        return data.result;
      }
      const error = new TelegramApiError(method, response, data);
      if (!error.retryAfterMs || attempt >= maxRateLimitRetries) {
        throw error;
      }
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

  async getUpdates({ offset, timeout = 50, allowedUpdates = ["message", "callback_query"], signal } = {}) {
    return this.call("getUpdates", {
      offset,
      timeout,
      allowed_updates: allowedUpdates,
    }, { signal });
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
   * Each part goes as a rich message; a part Telegram rejects as a request (HTTP 400: a
   * construct it will not take, or a limit the estimate missed) goes the classic way
   * instead, so a response is never lost to its formatting.
   */
  async sendRichParts(chatId, text, options) {
    const { format: _format, ...telegramOptions } = options;
    const sent = [];
    for (const part of splitRichMarkdown(text || " ")) {
      try {
        sent.push(await this.call("sendRichMessage", {
          chat_id: chatId,
          rich_message: { markdown: toRichMarkdown(part) || " " },
          ...telegramOptions,
        }));
      } catch (error) {
        if (error?.status !== 400) {
          throw error;
        }
        sent.push(...await this.sendTextChunks(chatId, part, { ...telegramOptions, format: "markdown" }));
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
