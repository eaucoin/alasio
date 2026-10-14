/**
 * The webhook intake (webhook/src): what it reads of each update, its routes, and its
 * handling of a request, step by step, on a server of its handler's.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";

import { intakeHandler, MAX_BODY_BYTES, type Outcome, rateLimiter } from "../webhook/src/intake.ts";
import { parseRoute, readRoutes, type Route } from "../webhook/src/routes.ts";
import { admitted, senderOf } from "../webhook/src/updates.ts";

const ALLOWED = new Set(["1001"]);
const SECRET = "s".repeat(40);
const PATH = "/telegram/main/abcdefghijklmnopqrstuv";

const message = (from: number, chat: number, type = "private") => ({ update_id: 1, message: { message_id: 1, date: 0, text: "hi", from: { id: from, is_bot: false, first_name: "a" }, chat: { id: chat, type } } });

describe("updates", () => {
  test("a message's sender and chat are its own; a pressed button's, its presser and the chat of the message it is under", () => {
    assert.deepEqual(senderOf(message(1001, 1001)), { userId: "1001", chatId: "1001", chatType: "private" });
    assert.deepEqual(senderOf({ update_id: 2, callback_query: { id: "q", from: { id: 1001 }, message: { chat: { id: 1001, type: "private" } } } }), {
      userId: "1001",
      chatId: "1001",
      chatType: "private",
    });
    // An update of another type, or one that says neither, has none.
    for (const update of [{ update_id: 3, edited_message: message(1001, 1001).message }, { update_id: 4, channel_post: {} }, { update_id: 5, message: { chat: { id: 1 } } }, null, "x", 7]) {
      assert.equal(senderOf(update), undefined, JSON.stringify(update));
    }
    assert.equal(senderOf({ update_id: 6, message: { from: { id: "1001" }, chat: { id: 1001, type: "private" } } }), undefined);
  });

  test("passes on what alasio would act on alone: an allowed user, in their own private chat", () => {
    assert.equal(admitted(senderOf(message(1001, 1001)), ALLOWED), true);
    assert.equal(admitted(senderOf(message(1002, 1002)), ALLOWED), false);
    assert.equal(admitted(senderOf(message(1001, -5, "group")), ALLOWED), false);
    // A private chat that is another user's, as no update of Telegram's says, is not the sender's.
    assert.equal(admitted(senderOf(message(1001, 1002)), ALLOWED), false);
    assert.equal(admitted(undefined, ALLOWED), false);
  });
});

describe("routes", () => {
  const route = (fields: Record<string, unknown>) => JSON.stringify({ path: PATH, secretToken: SECRET, allowedUserIds: ["1001"], ...fields });

  test("an environment's route: its path its own, a secret token Telegram takes, users it allows, and its topic", () => {
    const parsed = parseRoute("main", route({}));
    assert.deepEqual([parsed.path, parsed.secretToken, [...parsed.allowedUserIds], parsed.topic], [PATH, SECRET, ["1001"], "telegram.main"]);
    assert.throws(() => parseRoute("try", route({})), /the route of try has no path of its own/u);
    assert.throws(() => parseRoute("main", route({ path: "/telegram/main/short" })), /no path of its own/u);
    assert.throws(() => parseRoute("main", route({ secretToken: "short" })), /no secret token of 32 characters or more/u);
    assert.throws(() => parseRoute("main", route({ secretToken: `${"s".repeat(40)}!` })), /no secret token/u);
    assert.throws(() => parseRoute("main", route({ allowedUserIds: [] })), /allows no user/u);
    assert.throws(() => parseRoute("main", route({ allowedUserIds: ["x"] })), /allows no user/u);
    assert.throws(() => parseRoute("Main", route({})), /Main is no environment's name/u);
  });

  test("are read from a Secret's files as the kubelet keeps them, a file that is no route said and left out", () => {
    const directory = mkdtempSync(join(tmpdir(), "alasio-routes-"));
    try {
      const data = join(directory, "..2026_10_14");
      mkdirSync(data);
      writeFileSync(join(data, "main"), route({}));
      writeFileSync(join(data, "try"), "{");
      symlinkSync("..2026_10_14", join(directory, "..data"));
      for (const key of ["main", "try"]) symlinkSync(join("..data", key), join(directory, key));
      const refused: string[] = [];
      const routes = readRoutes(directory, (environment) => refused.push(environment));
      assert.deepEqual([...routes.keys()], [PATH]);
      assert.deepEqual(refused, ["try"]);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

describe("a request", () => {
  const route: Route = parseRoute("main", JSON.stringify({ path: PATH, secretToken: SECRET, allowedUserIds: ["1001"] }));
  const produced: { topic: string; key: string; value: string }[] = [];
  const outcomes: Outcome[] = [];
  let failing = false;
  let limited = false;
  let base = "";
  const server = createServer(intakeHandler({
    routes: () => new Map([[PATH, route]]),
    produce: async (topic, key, value) => {
      if (failing) throw new Error("the log is down");
      produced.push({ topic, key, value: value.toString("utf8") });
    },
    allow: () => !limited,
    counted: (outcome) => outcomes.push(outcome),
  }));

  before(async () => {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  after(() => {
    server.close();
  });

  /** What the intake answers a request of `body`, with `headers`, to `path`, and what it came to. */
  const send = async (body: string, { path = PATH, method = "POST", headers = { "x-telegram-bot-api-secret-token": SECRET } }: { path?: string; method?: string; headers?: Record<string, string> } = {}) => {
    outcomes.length = 0;
    const answered = await fetch(`${base}${path}`, { method, headers: { "content-type": "application/json", ...headers }, ...(method === "POST" ? { body } : {}) });
    return [answered.status, outcomes[0]];
  };

  test("an allowed user's update is written to the environment's topic, keyed by its chat, and answered once the log has it", async () => {
    const update = JSON.stringify(message(1001, 1001));
    assert.deepEqual(await send(update), [200, "written"]);
    assert.deepEqual(produced.at(-1), { topic: "telegram.main", key: "1001", value: update });
  });

  test("one the log does not take is answered 503, which Telegram sends again", async () => {
    failing = true;
    try {
      assert.deepEqual(await send(JSON.stringify(message(1001, 1001))), [503, "unwritten"]);
    } finally {
      failing = false;
    }
  });

  test("another user's, a group's, or one of a type alasio does not act on is answered 200 and never reaches the log", async () => {
    const before = produced.length;
    for (const update of [message(1002, 1002), message(1001, -5, "group"), { update_id: 9, edited_message: message(1001, 1001).message }]) {
      assert.deepEqual(await send(JSON.stringify(update)), [200, "dropped"]);
    }
    assert.equal(produced.length, before);
  });

  test("is refused, in order, by path and method, then by secret, before its body is read; then by its size, and as JSON", async () => {
    const update = JSON.stringify(message(1001, 1001));
    assert.deepEqual(await send(update, { path: "/telegram/main/other" }), [404, "unrouted"]);
    assert.deepEqual(await send(update, { method: "GET" }), [404, "unrouted"]);
    assert.deepEqual(await send(update, { headers: {} }), [401, "unauthenticated"]);
    assert.deepEqual(await send(update, { headers: { "x-telegram-bot-api-secret-token": `${SECRET}x` } }), [401, "unauthenticated"]);
    assert.deepEqual(await send(update, { headers: { "x-telegram-bot-api-secret-token": "t".repeat(40) } }), [401, "unauthenticated"]);
    assert.deepEqual(await send("x".repeat(MAX_BODY_BYTES + 1)), [413, "too large"]);
    assert.deepEqual(await send("{"), [400, "malformed"]);
  });

  test("a body longer than it says is cut off as it is read, beyond the most the intake takes", async () => {
    outcomes.length = 0;
    const status = await new Promise<number>((resolve) => {
      const sent = httpRequest(`${base}${PATH}`, { method: "POST", headers: { "x-telegram-bot-api-secret-token": SECRET, "transfer-encoding": "chunked" } }, (answered) => resolve(answered.statusCode ?? 0));
      sent.on("error", () => resolve(0));
      sent.write("x".repeat(MAX_BODY_BYTES));
      sent.end("x".repeat(16));
    });
    // Answered 413, or its connection closed as the body was cut off.
    assert.ok(status === 413 || status === 0, String(status));
    assert.deepEqual(outcomes, ["too large"]);
  });

  test("a source past its rate is refused before anything else", async () => {
    limited = true;
    try {
      assert.deepEqual(await send(JSON.stringify(message(1001, 1001)), { headers: {} }), [429, "limited"]);
    } finally {
      limited = false;
    }
  });
});

test("a source's rate: a burst, then as fast as its bucket fills, each source its own", () => {
  let now = 0;
  const allow = rateLimiter(2, 3, () => now);
  assert.deepEqual([allow("a"), allow("a"), allow("a"), allow("a"), allow("b")], [true, true, true, false, true]);
  now = 500;
  assert.deepEqual([allow("a"), allow("a")], [true, false]);
});
