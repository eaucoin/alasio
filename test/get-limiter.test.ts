/**
 * The object store's S3 gateway's GET limiter (src/object-store/get-limiter.ts): its gate,
 * first come first served, and the limiter in front of a stand-in for `weed s3`.
 */
import assert from "node:assert/strict";
import { Agent, createServer, type IncomingMessage, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { fifoGate, serveGetLimiter } from "../src/object-store/get-limiter.ts";

test("a gate lets in at most its places, the waiting first come first served, and one that gives up its turn leaves its place to the next", async () => {
  const gate = fifoGate(2);
  const order: string[] = [];
  const enter = (name: string, signal = new AbortController().signal) =>
    gate.enter(signal).then((release) => {
      order.push(name);
      return release;
    });
  const a = await enter("a");
  const b = await enter("b");
  const leaving = new AbortController();
  const c = enter("c");
  const d = enter("d", leaving.signal);
  const e = enter("e");
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual([gate.inside(), gate.waiting(), order], [2, 3, ["a", "b"]]);
  // d gives its turn up while it waits: it is refused, and leaves its place in the queue.
  leaving.abort(new Error("gone"));
  await assert.rejects(d, /gone/u);
  assert.equal(gate.waiting(), 2);
  // Each place freed goes to who waited longest, never more inside than its places.
  a();
  a();
  const releaseC = await c;
  assert.deepEqual([gate.inside(), gate.waiting(), order], [2, 1, ["a", "b", "c"]]);
  b();
  const releaseE = await e;
  assert.deepEqual([gate.inside(), gate.waiting(), order], [2, 0, ["a", "b", "c", "e"]]);
  releaseC();
  releaseE();
  assert.equal(gate.inside(), 0);
  // A signal aborted already never waits.
  const gone = new AbortController();
  gone.abort(new Error("before"));
  await assert.rejects(gate.enter(gone.signal), /before/u);
  assert.throws(() => fifoGate(0), /at least one place/u);
});

/** `method` of `path` at `port`, on a connection of its own, or of `agent`'s: its status and its body, with `body` sent. */
function ask(port: number, method: string, path: string, body?: Buffer, agent: Agent | false = false): Promise<{ readonly status: number; readonly body: Buffer }> {
  return new Promise((resolve, reject) => {
    const asked = httpRequest({ host: "127.0.0.1", port, method, path, agent, headers: { host: "store.test", ...(body ? { "content-length": String(body.length) } : {}) } }, (answer) => {
      const parts: Buffer[] = [];
      answer.on("data", (chunk: Buffer) => parts.push(chunk));
      answer.on("end", () => resolve({ status: answer.statusCode ?? 0, body: Buffer.concat(parts) }));
    });
    asked.on("error", reject);
    asked.end(body);
  });
}

test("the limiter lets at most its streams of GETs reach the gateway at once, in turn, each streamed whole, the rest straight through, and a GET whose client leaves as it waits never reaches it", async () => {
  // A stand-in for `weed s3`: a GET's answer, 1 MiB, in pieces over 200 ms; a PUT's echoed.
  let gets = 0;
  let most = 0;
  const seen: string[] = [];
  const upstream = createServer((request, response) => {
    seen.push(`${request.method} ${request.url} ${request.headers.host}`);
    if (request.method === "GET") {
      gets += 1;
      most = Math.max(most, gets);
      response.writeHead(200, { "content-length": String(1 << 20) });
      let sent = 0;
      const timer = setInterval(() => {
        response.write(Buffer.alloc(1 << 18, sent));
        sent += 1;
        if (sent === 4) {
          clearInterval(timer);
          gets -= 1;
          response.end();
        }
      }, 50);
      return;
    }
    const parts: Buffer[] = [];
    request.on("data", (chunk: Buffer) => parts.push(chunk));
    request.on("end", () => response.writeHead(201).end(Buffer.concat(parts)));
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const limiter = await serveGetLimiter({ port: 0, host: "127.0.0.1", upstream: { host: "127.0.0.1", port: (upstream.address() as AddressInfo).port }, streams: 3 });
  try {
    const answers = Array.from({ length: 9 }, (_, index) => ask(limiter.port, "GET", `/bucket/object-${index}?x-id=GetObject`));
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.deepEqual([limiter.gate.inside(), limiter.gate.waiting()], [3, 6]);
    // Anything else goes straight through, however many GETs wait.
    const put = await ask(limiter.port, "PUT", "/bucket/put", Buffer.from("written"));
    assert.deepEqual([put.status, put.body.toString()], [201, "written"]);
    // A GET whose client gives up as it waits leaves its place, and never reaches the gateway.
    const leaving = httpRequest({ host: "127.0.0.1", port: limiter.port, method: "GET", path: "/bucket/left", agent: false, headers: { host: "store.test" } });
    leaving.on("error", () => undefined);
    leaving.end();
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(limiter.gate.waiting(), 7);
    leaving.destroy();
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(limiter.gate.waiting(), 6);
    for (const answer of await Promise.all(answers)) assert.deepEqual([answer.status, answer.body.length], [200, 1 << 20]);
    assert.equal(most, 3);
    assert.deepEqual([limiter.gate.inside(), limiter.gate.waiting()], [0, 0]);
    // In the order they came, as they came, the host they named kept, as a signature signs it.
    const asked = seen.filter((line) => line.startsWith("GET"));
    assert.deepEqual(asked, Array.from({ length: 9 }, (_, index) => `GET /bucket/object-${index}?x-id=GetObject store.test`));
  } finally {
    await limiter.close();
    await new Promise((resolve) => upstream.close(resolve));
  }
});

test("the limiter serves GETs and PUTs over connections its clients keep, as S3's clients do, each answer whole", async () => {
  const upstream = createServer((request, response) => {
    request.resume();
    request.on("end", () => response.writeHead(200, { "content-length": "65536" }).end(Buffer.alloc(65536, 7)));
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const limiter = await serveGetLimiter({ port: 0, host: "127.0.0.1", upstream: { host: "127.0.0.1", port: (upstream.address() as AddressInfo).port }, streams: 4 });
  const agent = new Agent({ keepAlive: true, maxSockets: 8 });
  try {
    for (let round = 0; round < 20; round += 1) {
      const answers = await Promise.all(Array.from({ length: 20 }, (_, index) =>
        index % 3 === 0 ? ask(limiter.port, "PUT", `/bucket/put-${index}`, Buffer.alloc(1000), agent) : ask(limiter.port, "GET", `/bucket/get-${index}`, undefined, agent)
      ));
      for (const answer of answers) assert.deepEqual([answer.status, answer.body.length], [200, 65536]);
    }
    assert.deepEqual([limiter.gate.inside(), limiter.gate.waiting()], [0, 0]);
  } finally {
    agent.destroy();
    await limiter.close();
    await new Promise((resolve) => upstream.close(resolve));
  }
});

/** How many bytes `write` took of `total`, waiting on each `false` for `drain`, before `stop`. */
function writeWhileTaken(stream: { write: (chunk: Buffer) => boolean; once: (event: "drain", listener: () => void) => unknown; end: () => unknown }, total: number, stop: Promise<unknown>): { readonly taken: () => number } {
  const piece = Buffer.alloc(1 << 16);
  let taken = 0;
  let stopped = false;
  void stop.then(() => {
    stopped = true;
  });
  const send = () => {
    while (!stopped && taken < total) {
      taken += piece.length;
      if (!stream.write(piece)) return void stream.once("drain", send);
    }
    if (taken >= total) stream.end();
  };
  send();
  return { taken: () => taken };
}

test("the limiter holds of an answer or a body only what is under way, as far as the one it passes it to takes it", async () => {
  const BODY = 128 << 20;
  // As far as the sockets' buffers, theirs and the limiter's both ways, and a piece or two of its own.
  const UNDER_WAY = 40 << 20;
  const settle = () => new Promise((resolve) => setTimeout(resolve, 1000));
  let answering: { readonly taken: () => number } | undefined;
  let held: IncomingMessage | undefined;
  const upstream = createServer((request, response) => {
    if (request.method === "GET") {
      response.writeHead(200, { "content-length": String(BODY) });
      answering = writeWhileTaken(response, BODY, settle());
    } else {
      // A gateway that takes no more of the body.
      request.pause();
      held = request;
    }
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const limiter = await serveGetLimiter({ port: 0, host: "127.0.0.1", upstream: { host: "127.0.0.1", port: (upstream.address() as AddressInfo).port }, streams: 1 });
  try {
    // A client that reads none of its answer: the gateway is let write only so far.
    const reading = httpRequest({ host: "127.0.0.1", port: limiter.port, path: "/bucket/big", agent: false }, (answer) => answer.pause());
    reading.on("error", () => undefined);
    reading.end();
    await settle();
    assert.ok(answering && answering.taken() > 0 && answering.taken() < UNDER_WAY, `the gateway wrote ${answering?.taken()} bytes of an answer read of none`);
    reading.destroy();
    // A gateway that reads none of a body: the client is let write only so far.
    const writing = httpRequest({ host: "127.0.0.1", port: limiter.port, method: "PUT", path: "/bucket/big", agent: false, headers: { "content-length": String(BODY) } });
    writing.on("error", () => undefined);
    const sending = writeWhileTaken(writing, BODY, settle());
    await settle();
    assert.ok(held && sending.taken() < UNDER_WAY, `the client wrote ${sending.taken()} bytes of a body taken of none`);
    writing.destroy();
  } finally {
    held?.destroy();
    await limiter.close();
    upstream.closeAllConnections();
    await new Promise((resolve) => upstream.close(resolve));
  }
});

test("the limiter keeps a client's connection between its requests as weed s3 does, past node's own 5 s, so its next request is not sent on one closing", async () => {
  const upstream = createServer((request, response) => {
    request.resume();
    request.on("end", () => response.writeHead(200, { "content-length": "2" }).end("ok"));
  });
  await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  const limiter = await serveGetLimiter({ port: 0, host: "127.0.0.1", upstream: { host: "127.0.0.1", port: (upstream.address() as AddressInfo).port }, streams: 1 });
  const agent = new Agent({ keepAlive: true, maxSockets: 1 });
  const reused = () =>
    new Promise<boolean>((resolve, reject) => {
      const asked = httpRequest({ host: "127.0.0.1", port: limiter.port, path: "/bucket/kept", agent }, (answer) => {
        answer.resume();
        answer.on("end", () => resolve(asked.reusedSocket));
      });
      asked.on("error", reject);
      asked.end();
    });
  try {
    assert.equal(await reused(), false);
    await new Promise((resolve) => setTimeout(resolve, 6000));
    assert.equal(await reused(), true);
  } finally {
    agent.destroy();
    await limiter.close();
    await new Promise((resolve) => upstream.close(resolve));
  }
});
