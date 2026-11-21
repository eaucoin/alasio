/**
 * The Telegram stand-in as the end-to-end runs and the benchmark reach it (./telegram-stub.ts):
 * through a port forwarded to its Service, and the operator it stands in front of, who says
 * things, presses buttons and reads replies.
 */
import { request as httpRequest } from "node:http";

import type { InlineKeyboardButton } from "@grammyjs/types";

import { type Forward, kube } from "./harness.ts";
import { STAND_INS, type StandIn } from "./stand-ins.ts";
import type { CallsListing, ControlCallback, ControlMessage, RecordedCall } from "./telegram-stub.ts";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A local port forwarded to the stand-in's Service, once the stand-in answers there. */
export async function forward(standIn: StandIn): Promise<Forward> {
  const forwarded = await kube.forward(STAND_INS, standIn.service, standIn.port);
  for (let attempt = 0; attempt < 60; attempt++) {
    if (await fetch(`${forwarded.base}${standIn.ready}`).then(() => true, () => false)) return forwarded;
    await sleep(500);
  }
  forwarded.close();
  throw new Error(`the ${standIn.name} stand-in did not answer`);
}

/**
 * `method` of `url`, through a forward, with `body` as JSON, its answer's JSON: on a
 * connection of its own, and so a forward of its own, which no request after it reuses, as
 * a kept-alive connection that the stand-in, or the forward through the API server, ends
 * while idle may be closing as the next request is sent on it. One that closes under a
 * read anyway is asked again, up to three times; a say or a press, which the stand-in may
 * have taken, is not.
 */
export async function throughForward(method: "GET" | "POST", url: string, body?: unknown): Promise<unknown> {
  const once = () =>
    new Promise<unknown>((resolve, reject) => {
      const sent = body === undefined ? undefined : JSON.stringify(body);
      const request = httpRequest(url, { method, agent: false, headers: sent === undefined ? {} : { "content-type": "application/json" } }, (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer) => chunks.push(chunk));
        response.on("end", () => {
          try {
            resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
          } catch (error) {
            reject(error);
          }
        });
        response.on("error", reject);
      });
      request.on("error", reject);
      request.end(sent);
    });
  for (let attempt = 1; ; attempt++) {
    try {
      return await once();
    } catch (error) {
      // Node says a connection closed under a request so, a "socket hang up" among them.
      if (method !== "GET" || attempt >= 3 || (error as NodeJS.ErrnoException).code !== "ECONNRESET") throw error;
      await sleep(500);
    }
  }
}

/** The operator, through the Telegram stand-in: says things, presses buttons, reads replies. */
export function operator(base: string) {
  let seen = 0;
  const post = (path: string, body: ControlMessage | ControlCallback) => throughForward("POST", `${base}${path}`, body);
  const calls = async () => {
    // The stand-in answers what it was called with.
    const listed = (await throughForward("GET", `${base}/control/calls?since=${seen}`)) as CallsListing;
    seen = listed.total;
    return listed.calls;
  };
  const buttons = (call: RecordedCall): InlineKeyboardButton[] => {
    const markup = call.payload.reply_markup;
    return (markup && "inline_keyboard" in markup ? markup.inline_keyboard : []).flat();
  };
  /** What `call` sent, as a failed wait says it: its method and the start of its text, a rich message's markdown for one. */
  const sent = (call: RecordedCall) => [call.method, (call.payload.text ?? call.payload.rich_message?.markdown)?.slice(0, 240)];
  return {
    buttons,
    /** Says `text` as the operator, or as `user`, another the bot is for, each in their own chat. */
    say: (text: string, user?: number) => post("/control/message", { text, ...(user === undefined ? {} : { chatId: user, userId: user }) }),
    press: (data: string | undefined) => post("/control/callback", { data }),
    calls,
    /** What alasio sends until `predicate` holds for one, which is returned. */
    async waitFor(predicate: (call: RecordedCall) => boolean, timeoutMs = 300_000): Promise<RecordedCall> {
      const deadline = Date.now() + timeoutMs;
      const got: RecordedCall[] = [];
      while (Date.now() < deadline) {
        got.push(...(await calls()));
        const hit = got.find(predicate);
        if (hit) return hit;
        await sleep(500);
      }
      throw new Error(`alasio sent nothing that matched; it sent ${JSON.stringify(got.map(sent))}`);
    },
    /**
     * What alasio sends until each of `predicates` holds for one of it, which may come together,
     * as two replies sent at once do: the first each holds for, in their order.
     */
    async waitForEach(predicates: readonly ((call: RecordedCall) => boolean)[], timeoutMs = 300_000): Promise<RecordedCall[]> {
      const deadline = Date.now() + timeoutMs;
      const got: RecordedCall[] = [];
      while (Date.now() < deadline) {
        got.push(...(await calls()));
        const hits = predicates.map((predicate) => got.find(predicate));
        if (hits.every((hit) => hit !== undefined)) return hits as RecordedCall[];
        await sleep(500);
      }
      throw new Error(`alasio sent nothing that matched each; it sent ${JSON.stringify(got.map(sent))}`);
    },
    /** Presses the button labelled like `label` in what alasio sends next. */
    async choose(label: RegExp) {
      const call = await this.waitFor((candidate) => buttons(candidate).some((button) => label.test(button.text)));
      const button = buttons(call).find((candidate) => label.test(candidate.text));
      await this.press(button && "callback_data" in button ? button.callback_data : undefined);
    },
  };
}
