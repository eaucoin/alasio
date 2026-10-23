/**
 * An environment's relay of its Codex login as a component of its own (src/codex/relay.ts): on
 * the store, which keeps the login, its home a copy of the store's, a request let through only
 * with a token alasio gave a conversation's Codex turn, and sent on with the login the store
 * keeps now.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

import { Effect, Exit, Layer, Logger, Scope } from "effect";

import { mintToken } from "../src/binder/tokens.ts";
import { CodexLoginFreshness, makeCodexLoginFreshness } from "../src/codex/login-refresh.ts";
import { loginUpstream, poolLoginKey } from "../src/codex/login-relay.ts";
import { startRelay } from "../src/codex/relay.ts";
import { harnessKey } from "../src/gateway/names.ts";
import { Store } from "../src/persistence/store.ts";
import { run, testStore } from "./support/store.ts";

const CALLER_KEY = Buffer.from("the environment's key");
const dir = mkdtempSync(join(tmpdir(), "alasio-codex-relay-"));
after(() => rmSync(dir, { recursive: true, force: true }));

/** When the tests' access tokens stop: a day from the file's start, in seconds. */
const STARTED = Date.now();
const STOPS = Math.floor(STARTED / 1000) + 86_400;
/** A ChatGPT access token named `name`, stopping at STOPS, as a JWT is. */
const accessToken = (name: string) => `${name}.${Buffer.from(JSON.stringify({ exp: STOPS })).toString("base64url")}.signature`;
/** A ChatGPT login of the access token `name`, last refreshed `hoursAgo` before the file's start, so none is due, as Codex writes auth.json. */
const login = (name: string, hoursAgo: number) =>
  JSON.stringify({ tokens: { access_token: accessToken(name), account_id: "account" }, last_refresh: new Date(STARTED - hoursAgo * 3_600_000).toISOString() });

/** What the relay at `port` answers a request bearing `bearer` with: its status, and the authorization it was sent on with. */
const ask = (port: number, bearer: string | null): Promise<{ readonly status: number; readonly sentWith: string }> =>
  new Promise((resolve, reject) => {
    const asked = request({ hostname: "127.0.0.1", port, path: "/v1/models", method: "GET", headers: bearer ? { authorization: `Bearer ${bearer}` } : {} }, (response) => {
      const chunks: Buffer[] = [];
      response.on("data", (chunk: Buffer) => chunks.push(chunk));
      response.on("end", () => resolve({ status: response.statusCode ?? 0, sentWith: Buffer.concat(chunks).toString("utf8") }));
    });
    asked.on("error", reject);
    asked.end();
  });

test("a relay of its own home takes the store's login as it starts, and the one the store kept since before each request it lets through, a turn's token alone, and keeps what Codex writes there in the store", async () => {
  const store = await testStore();
  await run(store.setCodexLogin(login("first", 48)));
  const home = join(dir, "own", ".codex");
  // An upstream that answers with the authorization it was sent, as the relay sends the login on.
  const upstream = createServer((incoming, outgoing) => void outgoing.writeHead(200).end(String(incoming.headers.authorization)));
  await new Promise<void>((listening) => upstream.listen(0, "127.0.0.1", listening));
  const origin = `http://127.0.0.1:${(upstream.address() as { port: number }).port}`;
  const scope = Effect.runSync(Scope.make());
  try {
    // Its freshness as the relay's own, over the store; no login here is due, so none is refreshed.
    const freshness = Layer.effect(CodexLoginFreshness, makeCodexLoginFreshness({ home, refresh: Effect.fail({ message: "no refresh is due" }), reload: Effect.void }));
    const services = Layer.provideMerge(freshness, Layer.succeed(Store, store));
    const port = await Effect.runPromise(startRelay({ callerKey: CALLER_KEY, keepCodexLogin: true, home, port: 0 }, {
      upstreamFor: (file, path) => {
        const upstreamOf = loginUpstream(file, path);
        return upstreamOf && { ...upstreamOf, origin };
      },
    }).pipe(Effect.provide(services), Effect.provide(Logger.layer([])), Scope.provide(scope)));
    // Written from the store as it starts, its home holding none.
    assert.equal(readFileSync(join(home, "auth.json"), "utf8"), login("first", 48));
    const token = mintToken(poolLoginKey(CALLER_KEY), { key: harnessKey("telegram:1"), expires: Date.now() + 60_000 });
    // None but a conversation's Codex turn's.
    assert.equal((await ask(port, null)).status, 401);
    assert.equal((await ask(port, mintToken(CALLER_KEY, { key: harnessKey("telegram:1"), expires: Date.now() + 60_000 }))).status, 401);
    assert.deepEqual(await ask(port, token), { status: 200, sentWith: `Bearer ${accessToken("first")}` });
    // A login made anew in alasio's pod, as `alasio login codex` makes one, kept in the store: sent on with the next request.
    await run(store.setCodexLogin(login("anew", 1)));
    assert.deepEqual(await ask(port, token), { status: 200, sentWith: `Bearer ${accessToken("anew")}` });
    // One Codex refreshes in the home itself, as the relay stops.
    writeFileSync(join(home, "auth.json"), login("refreshed", 0));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    upstream.close();
  }
  // What Codex wrote in the home kept in the store as the relay stops, as it keeps a home of its own's.
  assert.equal(await run(store.getCodexLogin), login("refreshed", 0));
});
