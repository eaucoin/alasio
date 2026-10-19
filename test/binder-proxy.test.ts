import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type IncomingMessage, request, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { type Proxied, proxy, type Route } from "../src/binder/proxy.ts";

// The binder's proxy, between a caller and a stand-in for a bound pod, each a server of the
// loopback: the front one passes every request to `route` and records what each came to.

async function listening(handle: (incoming: IncomingMessage, outgoing: ServerResponse) => void): Promise<{ server: Server; port: number }> {
  const server = createServer(handle);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { server, port: (server.address() as AddressInfo).port };
}

async function front(route: () => Route): Promise<{ port: number; came: Proxied[]; close: () => void }> {
  const came: Proxied[] = [];
  const { server, port } = await listening((incoming, outgoing) => {
    void proxy(incoming, outgoing, route()).then((proxied) => {
      came.push(proxied);
      if (!proxied.reached) outgoing.writeHead(502).end();
    });
  });
  return { port, came, close: () => server.close() };
}

test("a request reaches the pod as it was sent, with the binding's token for the caller's, and none of its connection's headers", async () => {
  const seen: { method: string | undefined; url: string | undefined; headers: IncomingMessage["headers"]; body: string }[] = [];
  const pod = await listening((incoming, outgoing) => {
    let body = "";
    incoming.on("data", (chunk: Buffer) => void (body += chunk.toString("utf8")));
    incoming.on("end", () => {
      seen.push({ method: incoming.method, url: incoming.url, headers: incoming.headers, body });
      outgoing.writeHead(201, { "content-type": "application/json", "mcp-session-id": "session", connection: "close" }).end('{"ok":true}');
    });
  });
  const proxying = await front(() => ({ address: "127.0.0.1", port: pod.port, token: "binding-token" }));
  try {
    const response = await fetch(`http://127.0.0.1:${proxying.port}/mcp?x=1`, {
      method: "POST",
      headers: { authorization: "Bearer caller-token", "content-type": "application/json", "mcp-session-id": "session", "proxy-authorization": "secret" },
      body: '{"jsonrpc":"2.0"}',
    });
    assert.deepEqual([response.status, response.headers.get("mcp-session-id"), await response.text()], [201, "session", '{"ok":true}']);
    const [request] = seen;
    assert.deepEqual([request?.method, request?.url, request?.body], ["POST", "/mcp?x=1", '{"jsonrpc":"2.0"}']);
    assert.equal(request?.headers.authorization, "Bearer binding-token");
    assert.equal(request?.headers["proxy-authorization"], undefined);
    assert.equal(request?.headers["mcp-session-id"], "session");
    assert.deepEqual(proxying.came, [{ reached: true, status: 201 }]);
  } finally {
    proxying.close();
    pod.server.close();
  }
});

test("a request and its answer stream both ways, each part passed on as it comes", async () => {
  let release = () => {};
  const firstPartIn = new Promise<void>((resolve) => void (release = resolve));
  const pod = await listening((incoming, outgoing) => {
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    outgoing.write("event: one\n\n");
    incoming.once("data", () => release());
    incoming.on("end", () => outgoing.end("event: two\n\n"));
  });
  const proxying = await front(() => ({ address: "127.0.0.1", port: pod.port, token: "t" }));
  try {
    const caller = request({ host: "127.0.0.1", port: proxying.port, method: "POST", path: "/mcp" });
    caller.write("first part");
    const [response] = (await once(caller, "response")) as [IncomingMessage];
    // The answer's first event, before the request is whole, and the request's first part, before its end.
    const [first] = (await once(response, "data")) as [Buffer];
    assert.equal(first.toString("utf8"), "event: one\n\n");
    await firstPartIn;
    caller.end("last part");
    const [last] = (await once(response, "data")) as [Buffer];
    assert.equal(last.toString("utf8"), "event: two\n\n");
  } finally {
    proxying.close();
    pod.server.close();
  }
});

test("a pod that is not reached is said so, and nothing is answered for it", async () => {
  const gone = await listening(() => {});
  gone.server.close();
  const proxying = await front(() => ({ address: "127.0.0.1", port: gone.port, token: "t" }));
  try {
    const response = await fetch(`http://127.0.0.1:${proxying.port}/mcp`, { method: "POST", body: "{}" });
    // The front's own answer, as it decides for a pod not reached.
    assert.equal(response.status, 502);
    assert.equal(proxying.came.length, 1);
    assert.equal(proxying.came[0]?.reached, false);
  } finally {
    proxying.close();
  }
});

test("a caller that goes away ends its request to the pod, and a pod that goes away ends the caller's", async () => {
  let podSaw = () => {};
  const podSawClose = new Promise<void>((resolve) => void (podSaw = resolve));
  const pod = await listening((incoming, outgoing) => {
    outgoing.writeHead(200, { "content-type": "text/event-stream" });
    outgoing.write("event: open\n\n");
    if (incoming.url === "/ends") setTimeout(() => outgoing.destroy(), 100);
    else outgoing.on("close", () => podSaw());
  });
  const proxying = await front(() => ({ address: "127.0.0.1", port: pod.port, token: "t" }));
  try {
    const leaving = new AbortController();
    const response = await fetch(`http://127.0.0.1:${proxying.port}/stays`, { signal: leaving.signal });
    assert.equal(response.status, 200);
    leaving.abort();
    await podSawClose;

    const ended = await fetch(`http://127.0.0.1:${proxying.port}/ends`);
    await assert.rejects(ended.text());
  } finally {
    proxying.close();
    pod.server.close();
  }
});
