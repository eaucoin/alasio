/** The workspaces' bayma at alasio's loopback (src/mcp/bayma-proxy.ts): each key's requests passed on with a bearer got as each goes. */
import assert from "node:assert/strict";
import { createServer, type IncomingMessage } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Context, Effect, Exit, Layer, Scope } from "effect";
import { TestClock } from "effect/testing";

import { environmentKey } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import { makeBaymaProxy } from "../src/mcp/bayma-proxy.ts";
import { makeGatewayWorkspaces, TOKEN_TTL_MS } from "../src/sandbox/gateway-client.ts";

/** What a stand-in bayma was asked: each request's method, path, Authorization and body. */
interface Asked {
  readonly method: string;
  readonly path: string;
  readonly authorization: string | undefined;
  readonly body: string;
}

/**
 * A stand-in bayma on the loopback: it records each request, and answers with server-sent
 * events, the last once `finish` is called; or, where `accepts` refuses its Authorization, 401.
 */
async function standInBayma(accepts: (authorization: string | undefined) => boolean = () => true) {
  const asked: Asked[] = [];
  const waiting: (() => void)[] = [];
  const server = createServer(async (incoming: IncomingMessage, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming as AsyncIterable<Buffer>) chunks.push(chunk);
    asked.push({ method: incoming.method ?? "", path: incoming.url ?? "", authorization: incoming.headers.authorization, body: Buffer.concat(chunks).toString() });
    if (!accepts(incoming.headers.authorization)) return void outgoing.writeHead(401).end();
    outgoing.writeHead(200, { "content-type": "text/event-stream", "mcp-session-id": "session-1" });
    outgoing.write("data: first\n\n");
    waiting.push(() => outgoing.end("data: last\n\n"));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    base: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    asked,
    finish: () => waiting.splice(0).forEach((end) => end()),
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

/** The loopback, in a scope of its own, with what its endpoints' upstreams run with. */
async function loopback(context: Context.Context<never> = Context.empty()) {
  const scope = Effect.runSync(Scope.make());
  const proxy = await Effect.runPromise(Scope.provide(makeBaymaProxy, scope).pipe(Effect.provideContext(context)));
  return { proxy, close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
}

/** A request of `endpoint`'s, with its bearer unless `authorization` says another. */
const ask = (endpoint: { readonly url: string; readonly headers: Readonly<Record<string, string>> }, init: RequestInit & { readonly authorization?: string } = {}) =>
  fetch(endpoint.url, { method: "POST", body: "{}", ...init, headers: { authorization: init.authorization ?? endpoint.headers["Authorization"] ?? "" } });

test("a workspace's bayma at the loopback passes each request on, streamed, with the bearer its upstream gives as the request goes in place of the loopback's own", async () => {
  const bayma = await standInBayma();
  const { proxy, close } = await loopback();
  try {
    let minted = 0;
    const upstream = Effect.sync(() => ({ url: `${bayma.base}/mcp`, headers: { Authorization: `Bearer minted-${(minted += 1)}` } }));
    const endpoint = await Effect.runPromise(proxy.reach("sessionfs:fs-one", upstream));
    assert.match(endpoint.url, /^http:\/\/127\.0\.0\.1:\d+\/mcp\/[\w-]+$/u);
    assert.match(endpoint.headers["Authorization"] ?? "", /^Bearer [\w-]{43}$/u);
    const answered = await fetch(`${endpoint.url}?x=1`, { method: "POST", headers: { authorization: endpoint.headers["Authorization"] ?? "", "content-type": "application/json" }, body: '{"jsonrpc":"2.0"}' });
    assert.deepEqual([answered.status, answered.headers.get("mcp-session-id")], [200, "session-1"]);
    // Its answer streams: the first event read before the last is sent.
    const reader = answered.body!.pipeThrough(new TextDecoderStream()).getReader();
    assert.equal((await reader.read()).value, "data: first\n\n");
    bayma.finish();
    let rest = "";
    for (let read = await reader.read(); !read.done; read = await reader.read()) rest += read.value;
    assert.equal(rest, "data: last\n\n");
    assert.deepEqual(bayma.asked, [{ method: "POST", path: "/mcp?x=1", authorization: "Bearer minted-1", body: '{"jsonrpc":"2.0"}' }]);

    // Asked for again, the same endpoint; each request after with a bearer got as it goes.
    assert.deepEqual(await Effect.runPromise(proxy.reach("sessionfs:fs-one", upstream)), endpoint);
    const again = ask(endpoint, { method: "GET", body: null });
    for (let waited = 0; bayma.asked.length < 2 && waited < 2000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
    bayma.finish();
    await (await again).text();
    assert.deepEqual(bayma.asked[1], { method: "GET", path: "/mcp", authorization: "Bearer minted-2", body: "" });
  } finally {
    await close();
    await bayma.close();
  }
});

test("a workspace's bayma at the loopback is reached by its own bearer alone: another workspace's, or none, is refused 401, a path of no workspace's 404, and an upstream not reached 502", async () => {
  const bayma = await standInBayma();
  const { proxy, close } = await loopback();
  try {
    const one = await Effect.runPromise(proxy.reach("sessionfs:fs-one", Effect.succeed({ url: `${bayma.base}/mcp`, headers: {} })));
    const other = await Effect.runPromise(proxy.reach("sessionfs:fs-two", Effect.succeed({ url: "http://127.0.0.1:1/mcp", headers: {} })));
    assert.notEqual(one.url, other.url);
    assert.equal((await ask(one, { authorization: other.headers["Authorization"] ?? "" })).status, 401);
    assert.equal((await ask(one, { authorization: "" })).status, 401);
    assert.equal((await ask({ ...one, url: one.url.replace(/[\w-]+$/u, "not-a-workspace") })).status, 404);
    assert.equal((await ask({ ...one, url: one.url.replace(/\/mcp\/.*$/u, "/other") })).status, 404);
    assert.deepEqual(bayma.asked, [], "none passed on");
    const unreached = await ask(other);
    assert.equal(unreached.status, 502);
    assert.match(((await unreached.json()) as { readonly error: string }).error, /^bayma was not reached: /u);
  } finally {
    await close();
    await bayma.close();
  }
});

test("a workspace's bayma at the loopback ends its caller's answer where bayma goes mid-answer, never as if it were answered whole", async () => {
  // A bayma that sends its answer's headers and first event, then drops its connection.
  const dropping = createServer((_incoming, outgoing) => {
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    outgoing.write("data: first\n\n", () => setTimeout(() => outgoing.socket?.destroy(), 50));
  });
  await new Promise<void>((resolve) => dropping.listen(0, "127.0.0.1", resolve));
  const { proxy, close } = await loopback();
  try {
    const endpoint = await Effect.runPromise(proxy.reach("sessionfs:fs-one", Effect.succeed({ url: `http://127.0.0.1:${(dropping.address() as AddressInfo).port}/mcp`, headers: {} })));
    const answered = await ask(endpoint);
    assert.equal(answered.status, 200);
    const reader = answered.body!.pipeThrough(new TextDecoderStream()).getReader();
    assert.equal((await reader.read()).value, "data: first\n\n");
    // Neither hung, nor ended as if whole.
    const next = await Promise.race([reader.read().then(({ done }) => (done ? "ended" : "read more"), () => "failed"), new Promise((resolve) => setTimeout(() => resolve("hung"), 5_000))]);
    assert.equal(next, "failed");
  } finally {
    await close();
    await new Promise<void>((resolve) => dropping.close(() => resolve()));
  }
});

test("a workspace's bayma at the loopback answers past the life of the first bearer it was reached with, each request's minted as it goes, with no endpoint given anew", async () => {
  const master = Buffer.from("the gateway's caller key");
  // The gateway's clock and alasio's, moved on together.
  let now = 0;
  const bearers: string[] = [];
  const bayma = await standInBayma((authorization) => {
    const bearer = (authorization ?? "").replace(/^Bearer /u, "");
    bearers.push(bearer);
    return verifyToken(environmentKey(master, "try"), bearer, now)?.key === "folder:claude:telegram:1";
  });
  const clocked = await Effect.runPromise(Layer.build(TestClock.layer()).pipe(Scope.provide(Effect.runSync(Scope.make()))));
  const { proxy, close } = await loopback(clocked);
  try {
    const { folders } = makeGatewayWorkspaces({ url: bayma.base, environment: "try", callerKey: environmentKey(master, "try") });
    const scope = { harness: "claude", threadKey: "telegram:1" };
    const endpoint = await Effect.runPromise(proxy.reach("folder:claude:telegram:1", folders.endpoint(scope)));
    const reached = async () => {
      const before = bayma.asked.length;
      const answered = ask(endpoint);
      for (let waited = 0; bayma.asked.length === before && waited < 2000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
      bayma.finish();
      const response = await answered;
      await response.text();
      return response.status;
    };
    assert.equal(await reached(), 200);
    const first = bearers[0] ?? "";
    // Past the first bearer's life, with no turn to give the process its bayma anew.
    now += TOKEN_TTL_MS + 60_000;
    await Effect.runPromise(TestClock.adjust(TOKEN_TTL_MS + 60_000).pipe(Effect.provideContext(clocked)));
    assert.equal(verifyToken(environmentKey(master, "try"), first, now), null, "the first bearer has lapsed");
    assert.equal(await reached(), 200);
    assert.notEqual(bearers[1], first);
  } finally {
    await close();
    await bayma.close();
  }
});
