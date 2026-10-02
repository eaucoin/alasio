/**
 * A stand-in for Telegram's Bot API, for alasio's end-to-end tests: alasio talks to it
 * through TELEGRAM_API_ROOT as it talks to Telegram, and the test drives it as the
 * operator would, by sending messages and pressing buttons, and reads what alasio sent.
 *
 *   POST /bot<token>/<method>        the Bot API: getUpdates long-polls what the test
 *                                    queued; every other method succeeds and is recorded
 *   POST /control/message            { text, chatId?, userId? } queues a message
 *   POST /control/callback           { data, chatId?, userId?, messageId? } queues a press
 *   GET  /control/calls?since=<n>    what alasio has called since the nth call
 *
 * Dependency-free, so it runs on any Node: `node telegram-stub.mjs [port]`.
 */
import { createServer } from "node:http";

const PORT = Number(process.argv[2] ?? process.env.PORT ?? 8081);
const CHAT = 1001;
const USER = 1001;

const updates = [];
const calls = [];
const waiters = new Set();
let updateId = 0;
let messageId = 100;
let callbackId = 0;

function queue(update) {
  updates.push({ update_id: ++updateId, ...update });
  for (const wake of waiters) wake();
}

function message({ text, chatId = CHAT, userId = USER }) {
  const from = { id: userId, is_bot: false, first_name: "Operator", username: "operator" };
  return {
    message_id: ++messageId,
    from,
    chat: { id: chatId, type: "private", first_name: "Operator" },
    date: Math.floor(Date.now() / 1000),
    text,
    ...(text.startsWith("/") ? { entities: [{ type: "bot_command", offset: 0, length: text.split(" ")[0].length }] } : {}),
  };
}

async function readJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const body = Buffer.concat(chunks);
  const type = request.headers["content-type"] ?? "";
  if (type.startsWith("application/json")) return JSON.parse(body.toString("utf8") || "{}");
  // Multipart uploads (sendDocument) are recorded by size only.
  return { multipartBytes: body.length };
}

/** A sent message's echo, as Telegram returns it. */
function sent(payload) {
  return {
    message_id: ++messageId,
    chat: { id: Number(payload.chat_id ?? CHAT), type: "private" },
    date: Math.floor(Date.now() / 1000),
    text: payload.text ?? "",
  };
}

async function botApi(method, payload) {
  switch (method) {
    case "getMe":
      return { id: 42, is_bot: true, first_name: "alasio", username: "alasio_test_bot" };
    case "getUpdates": {
      const offset = Number(payload.offset ?? 0);
      // Updates before the offset are confirmed, and forgotten.
      while (updates.length > 0 && updates[0].update_id < offset) updates.shift();
      if (updates.length === 0) {
        const timeout = Math.min(Number(payload.timeout ?? 0), 25) * 1000;
        await new Promise((resolve) => {
          const wake = () => { waiters.delete(wake); clearTimeout(timer); resolve(); };
          const timer = setTimeout(wake, timeout);
          waiters.add(wake);
        });
      }
      return updates.slice();
    }
    case "getWebhookInfo":
      return { url: "", has_custom_certificate: false, pending_update_count: 0 };
    case "sendMessage":
    case "sendRichMessage":
    case "sendDocument":
    case "sendPhoto":
    case "sendVideo":
      return sent(payload);
    case "editMessageText":
      return { ...sent(payload), message_id: Number(payload.message_id) };
    case "getFile":
      return { file_id: payload.file_id, file_path: "files/none" };
    default:
      return true;
  }
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://stub");
  const reply = (status, body) => response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
  try {
    const bot = /^\/bot[^/]+\/([A-Za-z]+)$/u.exec(url.pathname);
    if (bot && request.method === "POST") {
      const payload = await readJson(request);
      if (bot[1] !== "getUpdates") calls.push({ method: bot[1], payload, at: Date.now() });
      return reply(200, { ok: true, result: await botApi(bot[1], payload) });
    }
    if (url.pathname === "/control/message" && request.method === "POST") {
      const sentMessage = message(await readJson(request));
      queue({ message: sentMessage });
      return reply(200, { ok: true, message: sentMessage });
    }
    if (url.pathname === "/control/callback" && request.method === "POST") {
      const { data, chatId = CHAT, userId = USER, messageId: pressed = messageId } = await readJson(request);
      queue({
        callback_query: {
          id: String(++callbackId),
          from: { id: userId, is_bot: false, first_name: "Operator" },
          message: { message_id: Number(pressed), chat: { id: chatId, type: "private" }, date: 0, text: "" },
          chat_instance: "1",
          data,
        },
      });
      return reply(200, { ok: true });
    }
    if (url.pathname === "/control/calls" && request.method === "GET") {
      return reply(200, { calls: calls.slice(Number(url.searchParams.get("since") ?? 0)), total: calls.length });
    }
    return reply(404, { ok: false, description: "Not Found" });
  } catch (error) {
    return reply(500, { ok: false, description: error.message });
  }
});

server.listen(PORT, "0.0.0.0", () => console.log(`telegram stub on ${PORT}`));
process.on("SIGTERM", () => server.close(() => process.exit(0)));
