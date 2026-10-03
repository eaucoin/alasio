/**
 * A stand-in for the Telegram Bot API behind a real TelegramClient: the calls it posts
 * are answered by the test, which sees each as a BotCall.
 */
import assert from "node:assert/strict";

import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";

import { type BotMethod, type BotParams, TelegramClient } from "../../src/telegram/client.ts";

/** A Bot API call a client made: the method, what it sent, and a multipart call's uploads, by name. */
export type BotCall<M extends BotMethod = BotMethod> = {
  [K in M]: {
    readonly method: K;
    /** A JSON call's payload, or a multipart call's fields. */
    readonly params: BotParams<K> | undefined;
    readonly files?: readonly string[] | undefined;
  };
}[M];

/** How the test answers a call: with Telegram's result, or by throwing as Telegram refuses it (botApiError). */
export type BotAnswer = (call: BotCall) => Promise<unknown>;

/** A refusal of a call, as Telegram answers it: an HTTP status and a description. */
class BotApiRefusal extends Error {
  readonly status: number;

  constructor(description: string, status: number) {
    super(description);
    this.status = status;
  }
}

/** What a test throws from its BotAnswer to have Telegram refuse the call with `status`. */
export function botApiError(description: string, status: number): Error {
  return new BotApiRefusal(description, status);
}

/** A multipart field as the client encoded it: objects as JSON, everything else as text. */
function fieldValue(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

/** What a call posted: its JSON payload, or its form's fields and the names of its uploads. */
function sentBy(body: RequestInit["body"]): { readonly params: unknown; readonly files?: readonly string[] } {
  if (typeof body === "string") return { params: JSON.parse(body) };
  assert.ok(body instanceof FormData, "a Bot API call posts JSON or a form");
  const params: Record<string, unknown> = {};
  const files: string[] = [];
  for (const [name, value] of body) {
    if (typeof value === "string") params[name] = fieldValue(value);
    else files.push(name);
  }
  return { params, files };
}

/** A fetch that answers Bot API calls as `answer` does, and is aborted as fetch is. */
function botApiFetch(answer: BotAnswer): typeof globalThis.fetch {
  return async (input, init) => {
    const method = String(input).split("/").at(-1);
    const { params, files } = sentBy(init?.body);
    // The client posts each method's params as that method's; the test reads what it drives.
    const call = { method, params, ...(files ? { files } : {}) } as BotCall;
    const aborted = new Promise<never>((_, reject) => {
      init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
    });
    try {
      const result = await Promise.race([answer(call), aborted]);
      return Response.json({ ok: true, result });
    } catch (error) {
      if (!(error instanceof BotApiRefusal)) throw error;
      return Response.json({ ok: false, error_code: error.status, description: error.message }, { status: error.status });
    }
  };
}

/** A TelegramClient whose calls `answer` answers, in place of Telegram. */
export function botApiLayer(answer: BotAnswer): Layer.Layer<TelegramClient> {
  return TelegramClient.layer("test-token").pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, botApiFetch(answer))));
}

/** A TelegramClient whose calls `answer` answers, made for a test to run its calls with Effect.runPromise. */
export function botApiClient(answer: BotAnswer): Promise<TelegramClient["Service"]> {
  return Effect.runPromise(Effect.provide(TelegramClient, botApiLayer(answer)));
}

/** What `call` sent, checked to be a call to `method`. */
export function paramsOf<M extends BotMethod>(call: BotCall | undefined, method: M): BotParams<M> {
  assert.equal(call?.method, method);
  assert.ok(call.params);
  // Checked just above: the call is to `method`, so what it sent is that method's params.
  return call.params as BotParams<M>;
}
