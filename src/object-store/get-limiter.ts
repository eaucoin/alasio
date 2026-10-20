/**
 * The object store's S3 gateway's GET limiter, in front of SeaweedFS's `weed s3` in the
 * gateway's pod (cli/src/manifests/object-store.ts): it serves the gateway's S3 port and
 * passes every request to `weed s3` on the pod's own, as it came and streamed both ways,
 * nothing held whole; but it lets at most `streams` GETs be under way there at once, the
 * others waiting their turn, first come first served.
 *
 * SeaweedFS 4.48's gateway reads the chunks its GETs read into 256 slots shared by them all,
 * each GET holding the chunk it reads and four ahead: past about 50 GETs at once, a GET
 * needing a slot takes one from another GET, even one that GET is reading, which reads it
 * again and may lose it again. Under the box's load in CI (test/gateway-memory.test.ts),
 * 68 at once, a GET of 16 MiB took up to 22 minutes. Every client of the store goes through
 * here, the restores, the node agents, Neon, the lake and the backups, so the bound holds
 * however many there are, on however many nodes.
 *
 * A GET is under way from when it is let through until its answer has been given whole,
 * or its client or `weed s3` ends it; one whose client goes away while it waits leaves its
 * place. Anything else, the uploads among it, `weed s3` bounds itself.
 *
 * Its connections are kept as `weed s3` keeps its own (its -idleTimeout, 120 s): one nothing
 * moves on for IDLE_MS is closed, a client's kept between its requests too, and none closed
 * sooner, which would have a client's next request on it fail as it sent it.
 *
 * It runs as ./get-limiter-main.ts.
 */
import { Agent, createServer, type IncomingHttpHeaders, type IncomingMessage, request as httpRequest, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

/** A first come first served gate of `limit` places: who is under way, and who waits. */
export interface FifoGate {
  /**
   * Resolves with the place's release once `signal`'s holder has one; rejects, its turn
   * given up, where `signal` aborts first.
   */
  readonly enter: (signal: AbortSignal) => Promise<() => void>;
  /** How many hold a place now. */
  readonly inside: () => number;
  /** How many wait for one. */
  readonly waiting: () => number;
}

/** A gate of `limit` places, as FifoGate says. */
export function fifoGate(limit: number): FifoGate {
  if (!Number.isInteger(limit) || limit < 1) throw new Error(`a gate has at least one place, not ${limit}`);
  let inside = 0;
  const queue: { readonly admit: () => void; readonly signal: AbortSignal }[] = [];
  const release = (): (() => void) => {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      inside -= 1;
      const next = queue.shift();
      if (next) {
        inside += 1;
        next.admit();
      }
    };
  };
  return {
    enter: (signal) =>
      new Promise((resolve, reject) => {
        if (signal.aborted) return reject(signal.reason);
        if (inside < limit && queue.length === 0) {
          inside += 1;
          return resolve(release());
        }
        const waiter = {
          admit: () => {
            signal.removeEventListener("abort", leave);
            resolve(release());
          },
          signal,
        };
        const leave = () => {
          const at = queue.indexOf(waiter);
          if (at >= 0) queue.splice(at, 1);
          reject(signal.reason);
        };
        signal.addEventListener("abort", leave, { once: true });
        queue.push(waiter);
      }),
    inside: () => inside,
    waiting: () => queue.length,
  };
}

/** How long a connection may go with nothing moving on it before it is closed, as `weed s3`'s -idleTimeout's default. */
const IDLE_MS = 120_000;

/** The headers of one hop alone, which a proxy does not pass on (RFC 9110 7.6.1), and Expect, which it answers itself. */
const HOP_BY_HOP = new Set(["connection", "keep-alive", "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade", "expect"]);

/** `headers` but those of one hop alone. */
function passedOn(headers: IncomingHttpHeaders): IncomingHttpHeaders {
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !HOP_BY_HOP.has(name)));
}

/** What serveGetLimiter is given. */
export interface GetLimiterOptions {
  /** The port it serves; 0 for any. */
  readonly port: number;
  readonly host?: string;
  /** `weed s3`'s address. */
  readonly upstream: { readonly host: string; readonly port: number };
  /** How many GETs may be under way at `weed s3` at once. */
  readonly streams: number;
}

/** The limiter, serving: its address, its gate, and its stop. */
export interface GetLimiter {
  readonly port: number;
  readonly gate: FifoGate;
  readonly close: () => Promise<void>;
}

/** Serves the limiter, as the module says. */
export async function serveGetLimiter({ port, host = "0.0.0.0", upstream, streams }: GetLimiterOptions): Promise<GetLimiter> {
  const gate = fifoGate(streams);
  // Its own kept connections to `weed s3` closed as they idle, before `weed s3` closes them.
  const agent = new Agent({ keepAlive: true, timeout: IDLE_MS / 2 });
  const pass = (request: IncomingMessage, response: ServerResponse, done: () => void): void => {
    const asked = httpRequest({ host: upstream.host, port: upstream.port, method: request.method, path: request.url, headers: passedOn(request.headers), agent }, (answer) => {
      response.writeHead(answer.statusCode ?? 502, answer.statusMessage, passedOn(answer.headers));
      answer.pipe(response);
      answer.on("error", () => response.destroy());
    });
    asked.on("error", (error) => {
      if (!response.headersSent) response.writeHead(502, { "content-type": "text/plain" }).end(`the gateway did not answer: ${error.message}`);
      else response.destroy();
    });
    // The client's end ends what it asked of the gateway, and its place with it.
    response.on("close", () => {
      if (!response.writableFinished) asked.destroy();
      done();
    });
    // A GET has no body to send, and may have been read to its end as it waited its turn,
    // which a pipe would wait on for good.
    if (request.method === "GET") {
      request.resume();
      asked.end();
    } else {
      request.pipe(asked);
    }
  };
  const server: Server = createServer((request, response) => {
    if (request.method !== "GET") return pass(request, response, () => {});
    const left = new AbortController();
    const leave = () => left.abort(new Error("its client went away"));
    response.on("close", leave);
    gate.enter(left.signal).then(
      (release) => {
        response.off("close", leave);
        pass(request, response, release);
      },
      () => undefined,
    );
  });
  // Nothing moving for IDLE_MS closes a connection, between requests or in one; no request
  // has a deadline of its own, as at `weed s3`.
  server.keepAliveTimeout = IDLE_MS;
  server.timeout = IDLE_MS;
  server.requestTimeout = 0;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => resolve());
  });
  return {
    port: (server.address() as AddressInfo).port,
    gate,
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
        agent.destroy();
      }),
  };
}
