/**
 * The intake's handling of a request, in order, each step before the next is taken:
 *
 * 1. a request from a source address within its rate, or 429, the body never read;
 * 2. a POST to an environment's route, or 404, the body never read;
 * 3. the route's secret token in `X-Telegram-Bot-Api-Secret-Token`, compared in constant
 *    time, or 401, the body never read;
 * 4. a body of at most MAX_BODY_BYTES, by its declared length and as it is read, or 413;
 * 5. JSON, or 400;
 * 6. an update the environment's alasio would act on (./updates.ts), or 200, dropped;
 * 7. written to the environment's topic, keyed by its chat, so a chat's updates stay in
 *    order: 200 once the log has it, 503 when it does not, which Telegram sends again.
 */
import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

import type { Route } from "./routes.ts";
import { admitted, senderOf } from "./updates.ts";

/** The largest update the intake takes: far more than Telegram sends, a message's text being 4,096 characters at most. */
export const MAX_BODY_BYTES = 1_048_576;
/** How long the log is given to take an update before Telegram is told to send it again. */
export const PRODUCE_TIMEOUT_MS = 10_000;

/** What a request came to, as the intake counts them. */
export type Outcome = "unrouted" | "unauthenticated" | "limited" | "too large" | "malformed" | "dropped" | "written" | "unwritten";

/** What the intake handles requests with. */
export interface IntakeOptions {
  /** The routes it serves now, by path. */
  readonly routes: () => ReadonlyMap<string, Route>;
  /** Writes `value` to `topic` under `key`, resolving once the log has it. */
  readonly produce: (topic: string, key: string, value: Buffer) => Promise<void>;
  /** Whether a request from `address` is within its rate. */
  readonly allow: (address: string) => boolean;
  /** Counts what a request came to, with why, where it was not written. */
  readonly counted: (outcome: Outcome, environment: string | null, why?: string) => void;
}

const SECRET_HEADER = "x-telegram-bot-api-secret-token";

/** Whether `given` is `expected`, in a time that tells nothing of either. */
function sameSecret(given: string | string[] | undefined, expected: string): boolean {
  if (typeof given !== "string") return false;
  const [a, b] = [Buffer.from(given), Buffer.from(expected)];
  // Of unequal lengths, compared with itself, so the comparison takes as long.
  return timingSafeEqual(a, a.length === b.length ? b : a) && a.length === b.length;
}

/** The request's body, as long as it is at most MAX_BODY_BYTES; null for one longer. */
function bodyOf(request: IncomingMessage): Promise<Buffer | null> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let length = 0;
    request.on("data", (chunk: Buffer) => {
      length += chunk.length;
      if (length > MAX_BODY_BYTES) {
        resolve(null);
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks)));
    request.on("error", reject);
  });
}

/** Within `ms`, or rejected. */
function within<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`the log did not take it within ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/** Answers `status`, closing the connection where the body was left unread, so nothing more of it is taken. */
function answer(response: ServerResponse, status: number, unread = false): void {
  response.writeHead(status, { "content-type": "text/plain", ...(unread ? { connection: "close" } : {}) }).end();
}

/** The intake's handler of a request, as its options say. */
export function intakeHandler({ routes, produce, allow, counted }: IntakeOptions): (request: IncomingMessage, response: ServerResponse) => Promise<void> {
  return async (request, response) => {
    if (!allow(request.socket.remoteAddress ?? "")) {
      counted("limited", null);
      return answer(response, 429, true);
    }
    const path = (request.url ?? "").split("?")[0] ?? "";
    const route = request.method === "POST" ? routes().get(path) : undefined;
    if (!route) {
      counted("unrouted", null);
      return answer(response, 404, true);
    }
    const { environment } = route;
    if (!sameSecret(request.headers[SECRET_HEADER], route.secretToken)) {
      counted("unauthenticated", environment);
      return answer(response, 401, true);
    }
    const declared = Number(request.headers["content-length"] ?? 0);
    if (declared > MAX_BODY_BYTES) {
      counted("too large", environment);
      return answer(response, 413, true);
    }
    const body = await bodyOf(request).catch(() => null);
    if (body === null) {
      counted("too large", environment);
      return answer(response, 413, true);
    }
    let update: unknown;
    try {
      update = JSON.parse(body.toString("utf8"));
    } catch {
      counted("malformed", environment);
      return answer(response, 400);
    }
    const sender = senderOf(update);
    if (!admitted(sender, route.allowedUserIds)) {
      counted("dropped", environment);
      return answer(response, 200);
    }
    try {
      await within(produce(route.topic, sender.chatId, body), PRODUCE_TIMEOUT_MS);
    } catch (error) {
      counted("unwritten", environment, error instanceof Error ? error.message : String(error));
      return answer(response, 503);
    }
    counted("written", environment);
    return answer(response, 200);
  };
}

/**
 * A rate per source address: a bucket of `burst` requests each, filled again at `perSecond`;
 * buckets full again forgotten, so they hold no more than the addresses of a moment.
 */
export function rateLimiter(perSecond: number, burst: number, now: () => number = Date.now): (address: string) => boolean {
  const buckets = new Map<string, { tokens: number; at: number }>();
  return (address) => {
    const at = now();
    for (const [key, bucket] of buckets) if (bucket.tokens + ((at - bucket.at) / 1000) * perSecond >= burst) buckets.delete(key);
    const bucket = buckets.get(address) ?? { tokens: burst, at };
    const tokens = Math.min(burst, bucket.tokens + ((at - bucket.at) / 1000) * perSecond);
    if (tokens < 1) {
      buckets.set(address, { tokens, at });
      return false;
    }
    buckets.set(address, { tokens: tokens - 1, at });
    return true;
  };
}
