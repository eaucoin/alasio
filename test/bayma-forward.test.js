import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { connect as connectTcp } from "node:net";
import { after, before, test } from "node:test";

import { startBaymaForward } from "../src/sandbox/bayma-forward.js";

// bayma's stand-in: an HTTP server that answers with the path it was asked for.
let bayma;
let baymaPort;
before(async () => {
  bayma = createServer((req, res) => res.writeHead(200, { "content-type": "text/plain" }).end(`bayma saw ${req.method} ${req.url}`));
  await new Promise((resolve) => bayma.listen(0, "127.0.0.1", resolve));
  baymaPort = bayma.address().port;
});
after(() => new Promise((resolve) => bayma.close(resolve)));

// Each connection is a child process piping stdio to bayma's port, as agent-connect does
// inside a sandbox.
const PIPE = 'const s=require("node:net").connect(Number(process.argv[1]),"127.0.0.1");process.stdin.pipe(s);s.pipe(process.stdout);s.on("error",()=>process.exit(1));s.on("close",()=>process.exit(0));';

async function startForward() {
  const connections = [];
  const forward = await startBaymaForward({
    bearer: "forward-token",
    connect: () => {
      const child = spawn(process.execPath, ["-e", PIPE, String(baymaPort)], { stdio: ["pipe", "pipe", "ignore"] });
      connections.push(child);
      return child;
    },
  });
  return { forward, connections };
}

/** A raw HTTP exchange on one connection, so keep-alive and refusals are seen as they are. */
function exchange(port, requests) {
  return new Promise((resolve, reject) => {
    const socket = connectTcp(port, "127.0.0.1");
    let data = "";
    socket.on("data", (chunk) => { data += chunk; });
    socket.on("error", reject);
    socket.on("close", () => resolve(data));
    socket.on("connect", async () => {
      for (const request of requests) {
        socket.write(request);
        await new Promise((r) => setTimeout(r, 100));
      }
      socket.end();
    });
  });
}

const request = (path, auth) =>
  `GET ${path} HTTP/1.1\r\nhost: localhost\r\n${auth ? `authorization: ${auth}\r\n` : ""}\r\n`;

test("the forward carries an authenticated harness's requests to bayma, keep-alive included", async () => {
  const { forward, connections } = await startForward();
  try {
    assert.match(forward.url, /^http:\/\/127\.0\.0\.1:\d+\/mcp$/);
    assert.deepEqual(forward.headers, { Authorization: "Bearer forward-token" });
    const response = await fetch(forward.url, { headers: forward.headers });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "bayma saw GET /mcp");

    // Later requests on an authenticated connection go through as they are.
    const port = new URL(forward.url).port;
    const replies = await exchange(port, [request("/first", "Bearer forward-token"), request("/second")]);
    assert.match(replies, /bayma saw GET \/first/);
    assert.match(replies, /bayma saw GET \/second/);
    assert.equal(connections.length, 2); // one per connection, not per request
  } finally {
    await forward.close();
  }
});

test("a connection without the forward's bearer is refused before anything reaches the sandbox", async () => {
  const { forward, connections } = await startForward();
  try {
    const port = new URL(forward.url).port;
    assert.equal((await fetch(forward.url)).status, 401);
    assert.equal((await fetch(forward.url, { headers: { Authorization: "Bearer guessed" } })).status, 401);
    assert.match(await exchange(port, [request("/mcp", "Bearer forward-token-but-longer")]), /^HTTP\/1\.1 401/);
    assert.equal(connections.length, 0); // no docker exec was made for any of them
  } finally {
    await forward.close();
  }
});

test("closing the forward stops it listening and ends its open connections", async () => {
  const { forward } = await startForward();
  const port = new URL(forward.url).port;
  const socket = connectTcp(port, "127.0.0.1");
  await new Promise((resolve) => socket.on("connect", resolve));
  const closed = new Promise((resolve) => socket.on("close", resolve));
  await forward.close();
  await closed;
  await assert.rejects(fetch(forward.url, { headers: forward.headers }));
});
