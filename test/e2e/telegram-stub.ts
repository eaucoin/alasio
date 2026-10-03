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
 * Dependency-free, so it runs on any Node: `node telegram-stub.ts [port]`.
 */
import { createServer, type IncomingMessage } from "node:http";
import type { CallbackQuery, Chat, Message, Opts, Update, WebhookInfo } from "@grammyjs/types";

/** The parameters of a Bot API method; files go up as multipart, hence `never`. */
type BotParams<M extends keyof Opts<never>> = Opts<never>[M];

/**
 * A Bot API call's parameters, as far as the stand-in and the tests read them; the
 * others are recorded as alasio sent them, and a multipart upload by its size alone.
 */
export interface BotApiPayload
  extends Partial<
    Pick<BotParams<"getUpdates">, "offset" | "timeout">
      & Pick<BotParams<"sendMessage">, "chat_id" | "text" | "reply_markup">
      & Pick<BotParams<"editMessageText">, "message_id">
      & Pick<BotParams<"getFile">, "file_id">
  > {
  readonly multipartBytes?: number;
}

/** A Bot API call alasio made, as /control/calls lists it. */
export interface RecordedCall {
  readonly method: string;
  readonly payload: BotApiPayload;
  readonly at: number;
}

/** What /control/calls answers: the calls since the asked-for one, and how many there were in all. */
export interface CallsListing {
  readonly calls: readonly RecordedCall[];
  readonly total: number;
}

/** What /control/message is posted: a message the operator sends. */
export interface ControlMessage {
  readonly text: string;
  readonly chatId?: number;
  readonly userId?: number;
}

/** What /control/callback is posted: a button the operator presses. */
export interface ControlCallback {
  readonly data?: string | undefined;
  readonly chatId?: number;
  readonly userId?: number;
  readonly messageId?: number;
}

/** A message as the stand-in echoes it: Telegram's, without the chat's names, which it does not know. */
interface Echo extends Pick<Message.TextMessage, "message_id" | "date" | "text"> {
  readonly chat: Pick<Chat.PrivateChat, "id" | "type">;
}

/** A button press as the stand-in delivers it: on an echoed message, with what the test pressed. */
interface Press extends Omit<CallbackQuery, "message" | "data"> {
  readonly message: Echo;
  readonly data: string | undefined;
}

/** An update the test queued: a message the operator sent, or a button they pressed. */
type QueuedUpdate = { readonly message: Message.TextMessage } | { readonly callback_query: Press };

const PORT = Number(process.argv[2] ?? process.env["PORT"] ?? 8081);
const CHAT = 1001;
const USER = 1001;

const updates: (QueuedUpdate & Pick<Update, "update_id">)[] = [];
const calls: RecordedCall[] = [];
const waiters = new Set<() => void>();
let updateId = 0;
let messageId = 100;
let callbackId = 0;

function queue(update: QueuedUpdate) {
  updates.push({ update_id: ++updateId, ...update });
  for (const wake of waiters) wake();
}

function message({ text, chatId = CHAT, userId = USER }: ControlMessage): Message.TextMessage {
  const from = { id: userId, is_bot: false, first_name: "Operator", username: "operator" };
  return {
    message_id: ++messageId,
    from,
    chat: { id: chatId, type: "private", first_name: "Operator" },
    date: Math.floor(Date.now() / 1000),
    text,
    // split always returns at least one part.
    ...(text.startsWith("/") ? { entities: [{ type: "bot_command", offset: 0, length: text.split(" ")[0]!.length }] } : {}),
  };
}

// Bodies are as their senders speak them: the Bot API's parameters from alasio, and the
// control endpoints' from the tests; each caller names the shape it reads.
async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  // With no encoding set, a request reads as Buffers.
  for await (const chunk of request as AsyncIterable<Buffer>) chunks.push(chunk);
  const body = Buffer.concat(chunks);
  const type = request.headers["content-type"] ?? "";
  if (type.startsWith("application/json")) return JSON.parse(body.toString("utf8") || "{}");
  // Multipart uploads (sendDocument) are recorded by size only.
  return { multipartBytes: body.length };
}

/** A sent message's echo, as Telegram returns it. */
function sent(payload: BotApiPayload): Echo {
  return {
    message_id: ++messageId,
    chat: { id: Number(payload.chat_id ?? CHAT), type: "private" },
    date: Math.floor(Date.now() / 1000),
    text: payload.text ?? "",
  };
}

async function botApi(method: string, payload: BotApiPayload) {
  switch (method) {
    case "getMe":
      return { id: 42, is_bot: true, first_name: "alasio", username: "alasio_test_bot" };
    case "getUpdates": {
      const offset = Number(payload.offset ?? 0);
      // Updates before the offset are confirmed, and forgotten.
      while (updates.length > 0 && updates[0]!.update_id < offset) updates.shift(); // length > 0: there is a first
      if (updates.length === 0) {
        const timeout = Math.min(Number(payload.timeout ?? 0), 25) * 1000;
        await new Promise<void>((resolve) => {
          const wake = () => { waiters.delete(wake); clearTimeout(timer); resolve(); };
          const timer = setTimeout(wake, timeout);
          waiters.add(wake);
        });
      }
      return updates.slice();
    }
    case "getWebhookInfo":
      return { url: "", has_custom_certificate: false, pending_update_count: 0 } satisfies WebhookInfo;
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
  // A server's requests always carry their URL.
  const url = new URL(request.url!, "http://stub");
  const reply = (status: number, body: unknown) => response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
  try {
    const bot = /^\/bot[^/]+\/([A-Za-z]+)$/u.exec(url.pathname);
    if (bot?.[1] !== undefined && request.method === "POST") {
      const method = bot[1];
      const payload = (await readJson(request)) as BotApiPayload; // alasio calls the Bot API
      if (method !== "getUpdates") calls.push({ method, payload, at: Date.now() });
      return reply(200, { ok: true, result: await botApi(method, payload) });
    }
    if (url.pathname === "/control/message" && request.method === "POST") {
      const sentMessage = message((await readJson(request)) as ControlMessage); // the test posts one
      queue({ message: sentMessage });
      return reply(200, { ok: true, message: sentMessage });
    }
    if (url.pathname === "/control/callback" && request.method === "POST") {
      const { data, chatId = CHAT, userId = USER, messageId: pressed = messageId } = (await readJson(request)) as ControlCallback; // the test posts one
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
      return reply(200, { calls: calls.slice(Number(url.searchParams.get("since") ?? 0)), total: calls.length } satisfies CallsListing);
    }
    return reply(404, { ok: false, description: "Not Found" });
  } catch (error) {
    return reply(500, { ok: false, description: error instanceof Error ? error.message : undefined });
  }
});

server.listen(PORT, "0.0.0.0", () => console.log(`telegram stub on ${PORT}`));
process.on("SIGTERM", () => server.close(() => process.exit(0)));
