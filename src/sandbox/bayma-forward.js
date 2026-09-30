/**
 * The harness's one door into a session: a listener on alasio's own loopback that carries
 * each TCP connection into the session's sandbox, to bayma's MCP port on the sandbox's
 * loopback. Like `kubectl port-forward`, every connection is its own `docker exec`
 * (SessionHost.connect), so nothing listens inside the session host and no firewall is
 * opened; a session host that restarts is simply reached by the next connection.
 *
 * bayma itself has no authentication, and this listener is on a loopback other local
 * processes share, so a connection must open with this forward's bearer in its first
 * request's `Authorization` header. The header is checked before anything reaches the
 * sandbox, and a connection that fails it is answered 401 and closed. Later requests on
 * the same keep-alive connection come from the same client and pass straight through.
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { createServer } from "node:net";
import { createLogger } from "../shared/log.js";

const log = createLogger("bayma-forward");

/** The port bayma serves on inside every sandbox (entrypoint.sh's BAYMA_HTTP_PORT). */
export const SANDBOX_BAYMA_PORT = 7290;

// A request head larger than this is not an MCP client's; the connection is refused.
const MAX_HEAD_BYTES = 64 * 1024;
const HEAD_END = Buffer.from("\r\n\r\n");

/** The value of `name` in an HTTP request head (a Buffer up to its blank line), or null. */
function headerValue(head, name) {
  for (const line of head.toString("latin1").split("\r\n").slice(1)) {
    const colon = line.indexOf(":");
    if (colon > 0 && line.slice(0, colon).trim().toLowerCase() === name) return line.slice(colon + 1).trim();
  }
  return null;
}

function sameSecret(presented, expected) {
  const a = Buffer.from(presented ?? "");
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * Start forwarding to bayma in one session. `connect()` returns a child process whose
 * stdin/stdout are a connection to bayma (SessionHost.connect). Returns `{ url, headers,
 * close }`: the MCP endpoint a harness is given, the headers it must send, and a close
 * that stops listening and drops every open connection.
 */
export async function startBaymaForward({ connect, bearer = randomBytes(32).toString("hex") }) {
  const expected = `Bearer ${bearer}`;
  const sockets = new Set();
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => socket.destroy());
    let head = Buffer.alloc(0);
    const onData = (chunk) => {
      head = Buffer.concat([head, chunk]);
      const end = head.indexOf(HEAD_END);
      if (end < 0) {
        if (head.length > MAX_HEAD_BYTES) socket.destroy();
        return;
      }
      socket.off("data", onData);
      socket.pause();
      if (!sameSecret(headerValue(head.subarray(0, end), "authorization"), expected)) {
        socket.end("HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
        return;
      }
      // Each direction ends the other: the child's stdout ending (bayma or the session
      // host gone) ends the socket through the pipe, and the socket closing ends the child.
      const child = connect();
      const drop = () => { socket.destroy(); child.kill(); };
      child.on("error", drop);
      child.stdin.on("error", drop);
      socket.on("close", () => child.kill());
      child.stdin.write(head);
      socket.pipe(child.stdin);
      child.stdout.pipe(socket);
      socket.resume();
    };
    socket.on("data", onData);
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const { port } = server.address();
  log.info(`forwarding 127.0.0.1:${port} to bayma in the sandbox`);
  return {
    url: `http://127.0.0.1:${port}/mcp`,
    headers: { Authorization: expected },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve) => server.close(() => resolve()));
    },
  };
}
