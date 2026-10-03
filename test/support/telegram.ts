/**
 * Telegram as alasio's tests see it: the Bot API stand-in of the end-to-end run
 * (test/e2e/telegram-stub.ts), served in the test's own process on a port of its own.
 * The test is the operator: it sends messages and presses buttons, and reads every Bot
 * API call alasio made, typed as the Bot API types it.
 */
import type { AddressInfo } from "node:net";
import type { ApiMethods, InlineKeyboardButton, InlineKeyboardMarkup, Opts } from "@grammyjs/types";

import { type OperatorMessage, type Press, type RecordedCall, type StubFailure, createTelegramStub } from "../e2e/telegram-stub.ts";
import { eventually } from "./wait.ts";

export type { OperatorMessage, Press, StubFailure };

/** A Bot API method's name. */
export type BotMethod = keyof ApiMethods<never>;

/** A Bot API call alasio made: its method and parameters, Telegram's answer, and the message it sent or edited. */
export type BotCall = {
  [M in BotMethod]: {
    readonly method: M;
    readonly params: Opts<never>[M];
    /** Telegram's HTTP status: 200, or the failure the test asked for. */
    readonly status: number;
    readonly at: number;
    /** The message the call sent or edited, when it is one of alasio's text messages. */
    readonly messageId: number | null;
  };
}[BotMethod];

/** A call to `M`. */
export type CallTo<M extends BotMethod> = Extract<BotCall, { readonly method: M }>;

/**
 * What a call showed the operator, for exact assertions on what alasio said in order: the
 * text it sent, edited a message to, or answered a press with, and the labels of the
 * buttons under it, row by row.
 */
export interface Shown {
  readonly method: BotMethod;
  readonly text: string;
  readonly buttons?: readonly (readonly string[])[];
}

/** The methods that show the operator something, as `shownBy` reads them. */
const SHOWING: ReadonlySet<BotMethod> = new Set<BotMethod>(["sendMessage", "sendRichMessage", "editMessageText", "answerCallbackQuery"]);

/** The operator: the chat alasio talks to, and the user in it. */
export const OPERATOR_ID = 1001;

function isCallTo<M extends BotMethod>(call: BotCall, method: M): call is CallTo<M> {
  return call.method === method;
}

function keyboardOf(call: BotCall): InlineKeyboardButton[][] {
  const params: object = call.params;
  const markup = "reply_markup" in params ? params.reply_markup : undefined;
  // A markup with an inline keyboard is one, as alasio sends the Bot API's.
  return typeof markup === "object" && markup !== null && "inline_keyboard" in markup ? (markup as InlineKeyboardMarkup).inline_keyboard : [];
}

/** What `call` showed the operator, or null for a call that shows nothing. */
function shownBy(call: BotCall): Shown | null {
  if (!SHOWING.has(call.method)) return null;
  const text = isCallTo(call, "sendRichMessage") ? call.params.rich_message.markdown ?? "" : textOf(call);
  const rows = keyboardOf(call).map((row) => row.map((button) => button.text));
  return rows.length > 0 ? { method: call.method, text, buttons: rows } : { method: call.method, text };
}

function textOf(call: BotCall): string {
  const params: object = call.params;
  return "text" in params && typeof params.text === "string" ? params.text : "";
}

export class TelegramStandIn {
  readonly apiRoot: string;
  private readonly stub: ReturnType<typeof createTelegramStub>;

  private constructor(stub: ReturnType<typeof createTelegramStub>, apiRoot: string) {
    this.stub = stub;
    this.apiRoot = apiRoot;
  }

  static async start(): Promise<TelegramStandIn> {
    const stub = createTelegramStub();
    await new Promise<void>((resolve) => stub.server.listen(0, "127.0.0.1", resolve));
    // A server listening on a TCP port has an address of its own.
    const { port } = stub.server.address() as AddressInfo;
    return new TelegramStandIn(stub, `http://127.0.0.1:${port}`);
  }

  async close(): Promise<void> {
    this.stub.server.closeAllConnections();
    await new Promise((resolve) => this.stub.server.close(resolve));
  }

  /** The operator sends `text`. */
  say(text: string): OperatorMessage {
    return this.stub.message({ text });
  }

  /** The operator sends a photo, `content`, with a caption and in an album when given. */
  sendPhoto(fileId: string, content: Buffer, { caption, mediaGroupId }: { readonly caption?: string; readonly mediaGroupId?: string } = {}): OperatorMessage {
    this.stub.file(fileId, content);
    return this.stub.message({ photo: fileId, ...(caption === undefined ? {} : { text: caption }), ...(mediaGroupId === undefined ? {} : { mediaGroupId }) });
  }

  /** The operator presses the newest button labelled `label` that alasio has shown. */
  press(label: string): Press {
    for (const call of this.calls().toReversed()) {
      const button = keyboardOf(call).flat().find((candidate) => candidate.text === label);
      if (button && "callback_data" in button && call.messageId !== null) {
        return this.stub.callback({ data: button.callback_data, messageId: call.messageId });
      }
    }
    throw new Error(`alasio has shown no button labelled ${JSON.stringify(label)}; it showed ${JSON.stringify(this.calls().flatMap((call) => keyboardOf(call).flat().map((button) => button.text)))}`);
  }

  /** Has the next call to `method` fail as `failure` says. */
  fail(method: BotMethod, failure: StubFailure): void {
    this.stub.fail(method, failure);
  }

  /** Every Bot API call alasio has made but getUpdates, in order. */
  calls(): BotCall[] {
    return this.stub.calls().calls.map(toBotCall);
  }

  /** How many calls alasio has made so far, to read only later ones with `since`. */
  mark(): number {
    return this.stub.calls().total;
  }

  /** The calls alasio made after `mark`. */
  since(mark: number): BotCall[] {
    return this.stub.calls(mark).calls.map(toBotCall);
  }

  /** The calls to `method` alasio made after `mark`. */
  callsTo<M extends BotMethod>(method: M, mark = 0): CallTo<M>[] {
    return this.since(mark).filter((call): call is CallTo<M> => isCallTo(call, method));
  }

  /** What alasio showed the operator after `mark`, in order. */
  shown(mark = 0): Shown[] {
    return this.since(mark).map(shownBy).filter((entry): entry is Shown => entry !== null);
  }

  /** The first call to `method` after `mark` that `matches`, once alasio has made it. */
  async waitFor<M extends BotMethod>(method: M, matches: (call: CallTo<M>) => boolean = () => true, { mark = 0, timeoutMs = 5_000 }: { readonly mark?: number; readonly timeoutMs?: number } = {}): Promise<CallTo<M>> {
    return await eventually(`alasio to call ${method}`, () => this.callsTo(method, mark).find(matches), { timeoutMs, seen: () => this.shown(mark) });
  }

  /** What alasio has shown the operator after `mark`, once it has shown `count` things. */
  async waitForShown(count: number, { mark = 0, timeoutMs = 5_000 }: { readonly mark?: number; readonly timeoutMs?: number } = {}): Promise<Shown[]> {
    return await eventually(`alasio to show ${count} things`, () => {
      const entries = this.shown(mark);
      return entries.length >= count ? entries : undefined;
    }, { timeoutMs, seen: () => this.shown(mark) });
  }
}

// The stand-in records what alasio sent the Bot API, which is each method's parameters.
function toBotCall(call: RecordedCall): BotCall {
  const sentMessage = messageIdOf(call);
  return { method: call.method, params: call.payload, status: call.status, at: call.at, messageId: sentMessage } as BotCall;
}

/** The message a call sent (its result's id) or edited (its parameters' id). */
function messageIdOf(call: RecordedCall): number | null {
  if (call.method === "editMessageText") return Number(call.payload.message_id);
  const result = call.result;
  if (typeof result === "object" && result !== null && "message_id" in result && typeof result.message_id === "number") return result.message_id;
  return null;
}
