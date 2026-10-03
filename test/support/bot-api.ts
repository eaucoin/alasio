/**
 * A stand-in for the Telegram Bot API behind a real Client: its JSON and multipart
 * calls are answered by the test, which sees each as a BotCall.
 */
import assert from "node:assert/strict";

import type { BotMethod, BotParams, BotResult, Client, Upload } from "../../src/telegram/client.ts";

/** A Bot API call a client made: the method, what it sent, and a multipart call's uploads. */
export type BotCall<M extends BotMethod = BotMethod> = {
  [K in M]: {
    readonly method: K;
    /** A JSON call's payload, or a multipart call's fields. */
    readonly params: BotParams<K> | undefined;
    readonly files?: readonly Upload[] | undefined;
  };
}[M];

/** How the test answers a call: with Telegram's result, or by throwing as Telegram refuses it. */
export type BotAnswer = (call: BotCall) => Promise<unknown>;

/** Has `answer` answer every Bot API call `client` makes, in place of Telegram. */
export function answerBotCalls(client: Client, answer: BotAnswer): void {
  const answered = async <M extends BotMethod>(call: BotCall<M>): Promise<BotResult<M>> =>
    // A call to M is BotCall's member for M, which TypeScript does not pick out of the
    // union for a generic M. Telegram's result is whatever the test answers, as the client
    // trusts the real Bot API's; a test answers what the calls it drives read.
    await answer(call as BotCall) as BotResult<M>;
  client.call = async (method, payload) => await answered({ method, params: payload });
  client.callMultipart = async (method, fields, files) => await answered({ method, params: fields, files });
}

/** What `call` sent, checked to be a call to `method`. */
export function paramsOf<M extends BotMethod>(call: BotCall | undefined, method: M): BotParams<M> {
  assert.equal(call?.method, method);
  assert.ok(call.params);
  // Checked just above: the call is to `method`, so what it sent is that method's params.
  return call.params as BotParams<M>;
}

/** An error as Telegram's refusal of a call reads to the client: its HTTP status. */
export function botApiError(message: string, status: number): Error & { readonly status: number } {
  return Object.assign(new Error(message), { status });
}
