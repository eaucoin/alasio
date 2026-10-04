/**
 * A stand-in for Telegram's Bot API: alasio talks to it through TELEGRAM_API_ROOT as it
 * talks to Telegram, and a test drives it as the operator would, by sending messages and
 * pressing buttons, and reads what alasio sent.
 *
 *   POST|GET /bot<token>/<method>    the Bot API: getUpdates long-polls what the test
 *                                    queued; every other method succeeds and is recorded,
 *                                    unless the test made it fail
 *   GET  /file/bot<token>/<path>     a file the test gave, as getFile names it
 *   POST /control/message            { text, chatId?, userId? } queues a message
 *   POST /control/callback           { data, chatId?, userId?, messageId? } queues a press
 *   GET  /control/calls?since=<n>    what alasio has called since the nth call
 *
 * The end-to-end tests run it on its own in the cluster, `node telegram-stub.ts [port]`,
 * and so it is dependency-free, for any Node; alasio's own tests run one in their process
 * through createTelegramStub (test/support/telegram.ts).
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { CallbackQuery, Chat, Message, Opts, PhotoSize, Update, User, WebhookInfo } from "@grammyjs/types";

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

/** A Bot API call alasio made, as /control/calls lists it, with Telegram's answer: its HTTP status and result. */
export interface RecordedCall {
  readonly method: string;
  readonly payload: BotApiPayload;
  readonly at: number;
  readonly status: number;
  readonly result?: unknown;
}

/** What /control/calls answers: the calls since the asked-for one, and how many there were in all. */
export interface CallsListing {
  readonly calls: readonly RecordedCall[];
  readonly total: number;
}

/**
 * What /control/message is posted: a message the operator sends. A photo is the id of a
 * file given to the stand-in; photos of one album share a media group.
 */
export interface ControlMessage {
  readonly text?: string;
  readonly chatId?: number;
  readonly userId?: number;
  readonly photo?: string;
  readonly mediaGroupId?: string;
}

/** What /control/callback is posted: a button the operator presses. */
export interface ControlCallback {
  readonly data?: string | undefined;
  readonly chatId?: number;
  readonly userId?: number;
  readonly messageId?: number;
}

/** How Telegram refuses a call: its HTTP status and description, and the wait it asks for when it rate-limits. */
export interface StubFailure {
  readonly status: number;
  readonly description: string;
  readonly retryAfter?: number;
}

/** A message as the stand-in echoes it: Telegram's, without the chat's names, which it does not know. */
interface Echo extends Pick<Message.TextMessage, "message_id" | "date" | "text"> {
  readonly chat: Pick<Chat.PrivateChat, "id" | "type">;
}

/** A button press as the stand-in delivers it: on an echoed message, with what the test pressed. */
export interface Press extends Omit<CallbackQuery, "message" | "data"> {
  readonly message: Echo;
  readonly data: string | undefined;
}

/** A message the operator sends: text, or a photo with an optional caption. */
export type OperatorMessage = Message.TextMessage | Message.PhotoMessage;

/** An update the test queued: a message the operator sent, or a button they pressed. */
type QueuedUpdate = { readonly message: OperatorMessage } | { readonly callback_query: Press };

/** A Bot API stand-in: its server, which the caller listens with, and the operator's side of it. */
export interface TelegramStub {
  readonly server: Server;
  /** Queues a message the operator sends, and returns it as alasio will receive it. */
  message(sent: ControlMessage): OperatorMessage;
  /** Queues a press of a button, and returns it as alasio will receive it. */
  callback(pressed: ControlCallback): Press;
  /** What alasio has called since the nth call. */
  calls(since?: number): CallsListing;
  /** Gives the stand-in a file, which getFile then describes and the file route serves. */
  file(fileId: string, content: Buffer): void;
  /** Has the next call to `method` fail as `failure` says. */
  fail(method: string, failure: StubFailure): void;
}

const CHAT = 1001;
const USER = 1001;

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

export function createTelegramStub(): TelegramStub {
  const updates: (QueuedUpdate & Pick<Update, "update_id">)[] = [];
  const recorded: RecordedCall[] = [];
  const waiters = new Set<() => void>();
  const files = new Map<string, Buffer>();
  const failures = new Map<string, StubFailure[]>();
  let updateId = 0;
  let messageId = 100;
  let callbackId = 0;

  function queue(update: QueuedUpdate) {
    updates.push({ update_id: ++updateId, ...update });
    for (const wake of waiters) wake();
  }

  function message({ text, chatId = CHAT, userId = USER, photo, mediaGroupId }: ControlMessage): OperatorMessage {
    const from: User = { id: userId, is_bot: false, first_name: "Operator", username: "operator" };
    const base = {
      message_id: ++messageId,
      from,
      chat: { id: chatId, type: "private", first_name: "Operator" },
      date: Math.floor(Date.now() / 1000),
      ...(mediaGroupId === undefined ? {} : { media_group_id: mediaGroupId }),
    } as const;
    if (photo !== undefined) {
      const size: PhotoSize = { file_id: photo, file_unique_id: `unique-${photo}`, width: 1, height: 1, file_size: files.get(photo)?.length ?? 0 };
      return { ...base, photo: [size], ...(text === undefined ? {} : { caption: text }) };
    }
    const body = text ?? "";
    return {
      ...base,
      text: body,
      // split always returns at least one part.
      ...(body.startsWith("/") ? { entities: [{ type: "bot_command", offset: 0, length: body.split(" ")[0]!.length }] } : {}),
    };
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
      case "getFile": {
        const fileId = String(payload.file_id);
        return { file_id: fileId, file_unique_id: `unique-${fileId}`, file_size: files.get(fileId)?.length ?? 0, file_path: files.has(fileId) ? `files/${fileId}` : "files/none" };
      }
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
      // As Telegram's does, the Bot API answers a GET with its query as well as a POST with its body.
      if (bot?.[1] !== undefined && (request.method === "POST" || request.method === "GET")) {
        const method = bot[1];
        const payload = (request.method === "GET" ? Object.fromEntries(url.searchParams) : await readJson(request)) as BotApiPayload; // alasio calls the Bot API
        const failure = failures.get(method)?.shift();
        if (failure) {
          recorded.push({ method, payload, at: Date.now(), status: failure.status });
          return reply(failure.status, {
            ok: false,
            error_code: failure.status,
            description: failure.description,
            ...(failure.retryAfter === undefined ? {} : { parameters: { retry_after: failure.retryAfter } }),
          });
        }
        const result = await botApi(method, payload);
        if (method !== "getUpdates") recorded.push({ method, payload, at: Date.now(), status: 200, result });
        return reply(200, { ok: true, result });
      }
      const download = /^\/file\/bot[^/]+\/files\/(.+)$/u.exec(url.pathname);
      const content = download?.[1] === undefined ? undefined : files.get(decodeURIComponent(download[1]));
      if (content && request.method === "GET") {
        return response.writeHead(200, { "content-type": "application/octet-stream" }).end(content);
      }
      if (url.pathname === "/control/message" && request.method === "POST") {
        const sentMessage = message((await readJson(request)) as ControlMessage); // the test posts one
        queue({ message: sentMessage });
        return reply(200, { ok: true, message: sentMessage });
      }
      if (url.pathname === "/control/callback" && request.method === "POST") {
        const press = callback((await readJson(request)) as ControlCallback); // the test posts one
        return reply(200, { ok: true, callback_query: press });
      }
      if (url.pathname === "/control/calls" && request.method === "GET") {
        return reply(200, calls(Number(url.searchParams.get("since") ?? 0)) satisfies CallsListing);
      }
      return reply(404, { ok: false, description: "Not Found" });
    } catch (error) {
      return reply(500, { ok: false, description: error instanceof Error ? error.message : undefined });
    }
  });

  // A closed stand-in answers its long polls at once, rather than holding its process open.
  server.on("close", () => {
    for (const wake of waiters) wake();
  });

  function callback({ data, chatId = CHAT, userId = USER, messageId: pressed = messageId }: ControlCallback): Press {
    const press: Press = {
      id: String(++callbackId),
      from: { id: userId, is_bot: false, first_name: "Operator" },
      message: { message_id: Number(pressed), chat: { id: chatId, type: "private" }, date: 0, text: "" },
      chat_instance: "1",
      data,
    };
    queue({ callback_query: press });
    return press;
  }

  function calls(since = 0): CallsListing {
    return { calls: recorded.slice(since), total: recorded.length };
  }

  return {
    server,
    message(sentMessage) {
      const queued = message(sentMessage);
      queue({ message: queued });
      return queued;
    },
    callback,
    calls,
    file(fileId, content) {
      files.set(fileId, content);
    },
    fail(method, failure) {
      failures.set(method, [...(failures.get(method) ?? []), failure]);
    },
  };
}

if (import.meta.main) {
  const port = Number(process.argv[2] ?? process.env["PORT"] ?? 8081);
  const { server } = createTelegramStub();
  server.listen(port, "0.0.0.0", () => console.log(`telegram stub on ${port}`));
  process.on("SIGTERM", () => server.close(() => process.exit(0)));
}
