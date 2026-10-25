/** The workspaces' bayma at alasio's loopback (src/mcp/bayma-proxy.ts): each key's requests passed on with a bearer got as each goes. */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer, type IncomingMessage, request } from "node:http";
import { type AddressInfo, connect } from "node:net";
import { test } from "node:test";

import { Context, Effect, Exit, Layer, Scope } from "effect";
import { TestClock } from "effect/testing";

import { environmentKey, NOT_PASSED_HEADER } from "../src/binder/names.ts";
import { verifyToken } from "../src/binder/tokens.ts";
import { type BaymaProxyOptions, makeBaymaProxy } from "../src/mcp/bayma-proxy.ts";
import { makeGatewayWorkspaces, TOKEN_TTL_MS } from "../src/sandbox/gateway-client.ts";

/** What a stand-in bayma was asked: each request's method, path, Authorization and body. */
interface Asked {
  readonly method: string;
  readonly path: string;
  readonly authorization: string | undefined;
  readonly body: string;
}

/**
 * A stand-in bayma on the loopback, at `port` where given: it records each request, and answers
 * with server-sent events, the last once `finish` is called; or, where `accepts` refuses its
 * Authorization, 401.
 */
async function standInBayma(accepts: (authorization: string | undefined) => boolean = () => true, port = 0) {
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
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  return {
    base: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    asked,
    finish: () => waiting.splice(0).forEach((end) => end()),
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

/** The loopback, in a scope of its own, with what its endpoints' upstreams run with, made as `options` says. */
async function loopback(context: Context.Context<never> = Context.empty(), options: BaymaProxyOptions = {}) {
  const scope = Effect.runSync(Scope.make());
  const proxy = await Effect.runPromise(Scope.provide(makeBaymaProxy(options), scope).pipe(Effect.provideContext(context)));
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

test("a workspace's bayma at the loopback is reached by its own bearer alone: another workspace's, or none, is refused 401, a path of no workspace's 404, and an upstream not reached within its bound 502", async () => {
  const bayma = await standInBayma();
  const { proxy, close } = await loopback(Context.empty(), { unreachedForMs: 1_500 });
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

test("a request to a gateway not reached, as while it starts again, is passed again until it is, a call and its stream's GET alike; one past the bound is answered 502", async () => {
  // A port no one listens on yet, as the gateway's Service's while no pod of it is ready.
  const free = createServer();
  await new Promise<void>((resolve) => free.listen(0, "127.0.0.1", resolve));
  const port = (free.address() as AddressInfo).port;
  await new Promise<void>((resolve) => free.close(() => resolve()));
  const { proxy, close } = await loopback(Context.empty(), { unreachedForMs: 10_000 });
  let gateway: Awaited<ReturnType<typeof standInBayma>> | null = null;
  try {
    const endpoint = await Effect.runPromise(proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${port}/mcp`, headers: {} })));
    const call = ask(endpoint, { body: '{"id":1}' });
    const stream = ask(endpoint, { method: "GET", body: null });
    // Started again 2 s on, at the same address.
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    gateway = await standInBayma(() => true, port);
    for (let waited = 0; gateway.asked.length < 2 && waited < 5_000; waited += 20) await new Promise((resolve) => setTimeout(resolve, 20));
    gateway.finish();
    assert.deepEqual([(await call).status, (await stream).status], [200, 200]);
    assert.deepEqual(gateway.asked.map(({ method, body }) => [method, body]).sort(), [["GET", ""], ["POST", '{"id":1}']]);
  } finally {
    await close();
    await gateway?.close();
  }
  // Not reached within its bound: 502, as before.
  const bounded = await loopback(Context.empty(), { unreachedForMs: 1_500 });
  try {
    const endpoint = await Effect.runPromise(bounded.proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${port}/mcp`, headers: {} })));
    const started = Date.now();
    assert.equal((await ask(endpoint)).status, 502);
    assert.ok(Date.now() - started >= 1_000, `answered in ${Date.now() - started} ms`);
  } finally {
    await bounded.close();
  }
});

/**
 * A port on the loopback whose connections are never made, as one whose SYN nothing answers:
 * listened on with no room for a connection not yet taken, and that room filled, so the kernel
 * drops each SYN after. Its listener a Python's, which, unlike Node's, takes none of them.
 */
async function unanswering(): Promise<{ readonly port: number; readonly close: () => void }> {
  const listener = spawn("python3", ["-c", "import socket, sys, time\ns = socket.socket()\ns.bind(('127.0.0.1', 0))\ns.listen(0)\nprint(s.getsockname()[1], flush=True)\nsys.stdin.read()"], { stdio: ["pipe", "pipe", "inherit"] });
  const port = Number((await once(listener.stdout, "data"))[0].toString().trim());
  const filling = [1, 2, 3].map(() => connect(port, "127.0.0.1").on("error", () => {}));
  await new Promise((resolve) => setTimeout(resolve, 300));
  return { port, close: () => (filling.forEach((socket) => socket.destroy()), listener.kill()) };
}

test("a request to a gateway whose connection is never made, its SYN unanswered, as one routed to a pod going away, is answered 502 within the bound, never waited on past it", { timeout: 30_000 }, async () => {
  const gone = await unanswering();
  const { proxy, close } = await loopback(Context.empty(), { unreachedForMs: 3_000, connectWithinMs: 1_000 });
  try {
    const endpoint = await Effect.runPromise(proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${gone.port}/mcp`, headers: {} })));
    const started = Date.now();
    const answered = await ask(endpoint);
    const took = Date.now() - started;
    assert.equal(answered.status, 502);
    assert.match(((await answered.json()) as { readonly error: string }).error, /not made within/u);
    // Asked again within the bound, each connection given up on, then answered at the bound.
    assert.ok(took >= 2_000 && took < 4_500, `answered in ${took} ms`);
  } finally {
    await close();
  }
  // A connection waited for no longer than the bound has left, however long a connection may take.
  const bounded = await loopback(Context.empty(), { unreachedForMs: 2_000, connectWithinMs: 20_000 });
  try {
    const endpoint = await Effect.runPromise(bounded.proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${gone.port}/mcp`, headers: {} })));
    const started = Date.now();
    assert.equal((await ask(endpoint)).status, 502);
    assert.ok(Date.now() - started < 3_500, `answered in ${Date.now() - started} ms`);
  } finally {
    await bounded.close();
    gone.close();
  }
});

test("a request to a gateway not reached whose body streams, of no stated length, so cannot be read whole first, is answered 502 at once, never passed again", async () => {
  const free = createServer();
  await new Promise<void>((resolve) => free.listen(0, "127.0.0.1", resolve));
  const port = (free.address() as AddressInfo).port;
  await new Promise<void>((resolve) => free.close(() => resolve()));
  const { proxy, close } = await loopback(Context.empty(), { unreachedForMs: 10_000 });
  let gateway: Awaited<ReturnType<typeof standInBayma>> | null = null;
  try {
    const endpoint = await Effect.runPromise(proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${port}/mcp`, headers: {} })));
    const started = Date.now();
    // Its body sent in chunks, of no Content-Length.
    const status = await new Promise<number | undefined>((resolve, reject) => {
      const sending = request(endpoint.url, { method: "POST", headers: { authorization: endpoint.headers["Authorization"] ?? "" } }, (answered) => {
        answered.resume();
        resolve(answered.statusCode);
      });
      sending.on("error", reject);
      sending.write('{"id":');
      sending.end("1}");
    });
    assert.equal(status, 502);
    assert.ok(Date.now() - started < 1_000, `answered in ${Date.now() - started} ms`);
    // The gateway there again, nothing of it is passed on after.
    gateway = await standInBayma(() => true, port);
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    assert.deepEqual(gateway.asked, []);
  } finally {
    await close();
    await gateway?.close();
  }
});

test("a request whose connection to the gateway was made, then cut before its answer, as the gateway stopped mid-call, is sent once and answered 502, never passed again", async () => {
  let asked = 0;
  const cutting = createServer((incoming) => {
    asked += 1;
    incoming.resume();
    incoming.on("end", () => incoming.socket.destroy());
  });
  await new Promise<void>((resolve) => cutting.listen(0, "127.0.0.1", resolve));
  const { proxy, close } = await loopback(Context.empty(), { unreachedForMs: 10_000 });
  try {
    const endpoint = await Effect.runPromise(proxy.reach("folder:claude:telegram:1", Effect.succeed({ url: `http://127.0.0.1:${(cutting.address() as AddressInfo).port}/mcp`, headers: {} })));
    assert.equal((await ask(endpoint, { body: '{"id":1}' })).status, 502);
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    assert.equal(asked, 1);
  } finally {
    await close();
    await new Promise<void>((resolve) => cutting.close(() => resolve()));
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

test("a request the gateway answers 503 saying it passed it to no pod is passed again after its Retry-After, its body whole; one it may have passed on, too large to read first or of no stated length, or past the bound, is answered as the gateway answered it, sent once", async () => {
  const asked: string[] = [];
  let refusals: { readonly retryAfter?: string; readonly notPassed?: boolean }[] = [];
  const bayma = createServer(async (incoming, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming as AsyncIterable<Buffer>) chunks.push(chunk);
    asked.push(Buffer.concat(chunks).toString());
    const refusal = refusals.shift();
    if (refusal) return void outgoing.writeHead(503, { ...(refusal.retryAfter ? { "retry-after": refusal.retryAfter } : {}), ...(refusal.notPassed ? { [NOT_PASSED_HEADER]: "true" } : {}) }).end("not yet");
    outgoing.writeHead(200).end("answered");
  });
  await new Promise<void>((resolve) => bayma.listen(0, "127.0.0.1", resolve));
  const { proxy, close } = await loopback();
  try {
    const endpoint = await Effect.runPromise(proxy.reach("sessionfs:fs-one", Effect.succeed({ url: `http://127.0.0.1:${(bayma.address() as AddressInfo).port}/mcp`, headers: {} })));
    const call = async (body: string) => {
      const answered = await ask(endpoint, { body });
      return [answered.status, await answered.text()];
    };
    // As the gateway answers a bearer held to a binding it has not heard of yet.
    refusals = [{ retryAfter: "1", notPassed: true }, { retryAfter: "1", notPassed: true }];
    assert.deepEqual(await call('{"id":1}'), [200, "answered"]);
    assert.deepEqual(asked, ['{"id":1}', '{"id":1}', '{"id":1}']);
    // As it answers a pod not reached that may have taken it, with a Retry-After or none: sent once.
    refusals = [{ retryAfter: "1" }];
    assert.deepEqual(await call('{"id":2}'), [503, "not yet"]);
    refusals = [{}];
    assert.deepEqual(await call('{"id":3}'), [503, "not yet"]);
    // Past the bound: as answered.
    refusals = [{ retryAfter: "60", notPassed: true }];
    assert.deepEqual(await call('{"id":4}'), [503, "not yet"]);
    assert.deepEqual(asked.slice(3), ['{"id":2}', '{"id":3}', '{"id":4}']);
    // One too large to read whole first, or of no stated length: sent once, as it comes, never read for another.
    refusals = [{ retryAfter: "1", notPassed: true }];
    const large = "x".repeat(1024 ** 2 + 1);
    assert.deepEqual(await call(large), [503, "not yet"]);
    refusals = [{ retryAfter: "1", notPassed: true }];
    const chunked = await fetch(endpoint.url, {
      method: "POST",
      headers: { authorization: endpoint.headers["Authorization"] ?? "" },
      body: new ReadableStream({ start: (controller) => (controller.enqueue(new TextEncoder().encode('{"id":5}')), controller.close()) }),
      duplex: "half",
    } as RequestInit);
    assert.deepEqual([chunked.status, await chunked.text()], [503, "not yet"]);
    assert.deepEqual(asked.slice(6).map((body) => (body.length > 100 ? `${body.length} bytes` : body)), [`${large.length} bytes`, '{"id":5}']);
  } finally {
    await close();
    await new Promise<void>((resolve) => bayma.close(() => resolve()));
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
