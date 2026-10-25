/**
 * A workspace's bayma, as a harness's process reaches it: an MCP endpoint of alasio's own on
 * the loopback, which passes each request on to the workspace's bayma through the sandbox
 * gateway, with a bearer got as the request goes. A harness's process is told its MCP servers
 * once, as it starts (Claude Code's query options, a Codex thread's config as it is loaded),
 * and outlives any one of the gateway's caller tokens (TOKEN_TTL_MS): its background tasks,
 * the turns it starts on its own and a turn that runs for hours reach bayma past it, with no
 * turn's start to give it a fresh one. Pointed at this, its configuration never changes, and
 * no bearer it holds lapses.
 *
 * Each workspace's key is reached at a path of its own, with a bearer of its own, random and
 * kept for as long as alasio runs, as its harnesses' processes are: a process reaches its own
 * workspace's bayma alone, and nothing else on the machine's loopback reaches any.
 *
 * A request is passed on whole, streamed both ways, as MCP over HTTP streams its answers
 * (server-sent events): its method, path below the endpoint, query, headers and body, but its
 * Authorization, which the gateway's bearer replaces, and the headers of the hop alone. One
 * of a path no key has is answered 404; one without its key's bearer, 401; one whose bayma is
 * not reached, 502. It is passed to the gateway as ../sandbox/gateway-passing.ts says: passed
 * again while the gateway is not reached, as while it starts again, for GATEWAY_RESTART_MS from
 * when it came, or after its Retry-After where the gateway says it passed it to no pod, where
 * its body could be read whole first; otherwise answered as the gateway answered it, as an MCP
 * call a pod was given may have run. So a call a harness makes while the gateway starts again
 * is answered once it is, as is the GET its MCP client makes to reopen its stream, rather than
 * each answered 502. A caller that goes stops its request on, and a bayma that goes mid-answer
 * ends the caller's, as the binder's proxy does (../binder/proxy.ts).
 */
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingHttpHeaders, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { Context, Effect, Layer, type Scope } from "effect";

import type { BaymaEndpoint } from "../sandbox/gateway-client.ts";
import { type GatewayPassing, passToGateway } from "../sandbox/gateway-passing.ts";

/** The path the loopback's endpoints are below, as an MCP server over HTTP is reached. */
const BAYMA_PROXY_PATH = "/mcp";

/**
 * The largest body of a request the loopback reads whole before it passes it on, so it may pass
 * it again: an MCP call's, with room. One larger, or of no stated length, is passed once, as it
 * comes.
 */
const REPLAYED_BYTES = 1024 ** 2;

/** The headers of one hop, which go no further than it (RFC 9110 7.6.1), and the Authorization the loopback is reached with. */
const HOP_HEADERS = new Set(["connection", "keep-alive", "proxy-connection", "transfer-encoding", "te", "trailer", "upgrade", "host", "authorization"]);

/** A workspace's endpoint on the loopback: its path, its bearer, and what gives the bayma it passes requests on to. */
interface Route {
  readonly path: string;
  readonly authorization: Buffer;
  upstream: Effect.Effect<BaymaEndpoint, { readonly message: string }>;
}

/** The headers of `incoming` to pass on, with `given` in place of its Authorization. */
function passedOn(incoming: IncomingHttpHeaders, given: Readonly<Record<string, string>>): Record<string, string | string[]> {
  const headers: Record<string, string | string[]> = {};
  for (const [name, value] of Object.entries(incoming)) {
    if (value !== undefined && !HOP_HEADERS.has(name.toLowerCase())) headers[name] = value;
  }
  for (const [name, value] of Object.entries(given)) headers[name.toLowerCase()] = value;
  return headers;
}

/** Whether `given`, a request's Authorization, is `route`'s, compared in constant time. */
function authorized(route: Route, given: string | undefined): boolean {
  const bytes = Buffer.from(given ?? "");
  return bytes.length === route.authorization.length && timingSafeEqual(bytes, route.authorization);
}

/** What the loopback is made with: how it passes requests to the gateway, as ../sandbox/gateway-passing.ts's defaults but for tests. */
export type BaymaProxyOptions = GatewayPassing;

/** The loopback's endpoints, as the module says, served until the scope closes. */
export const makeBaymaProxy = (passing: BaymaProxyOptions = {}): Effect.Effect<BaymaProxy["Service"], never, Scope.Scope> => Effect.gen(function*() {
  /** Each workspace's endpoint, by its key, and by its path. */
  const routes = new Map<string, Route>();
  const paths = new Map<string, Route>();
  const run = Effect.runPromiseWith(yield* Effect.context<never>());

  const handle = (incoming: IncomingMessage, outgoing: ServerResponse): void => {
    const answer = (status: number, error: string) => {
      if (!outgoing.headersSent) outgoing.writeHead(status, { "content-type": "application/json" });
      outgoing.end(JSON.stringify({ error }));
    };
    const asked = new URL(incoming.url ?? "/", "http://127.0.0.1");
    const [path = "", ...below] = asked.pathname.slice(BAYMA_PROXY_PATH.length + 1).split("/");
    const route = asked.pathname.startsWith(`${BAYMA_PROXY_PATH}/`) ? paths.get(path) : undefined;
    if (route === undefined) return answer(404, "no workspace's bayma is served here");
    if (!authorized(route, incoming.headers.authorization)) return answer(401, "not this workspace's bearer");
    // bayma not reached: answered so; gone once its answer began: the caller's request ends with
    // it, never as if it were answered whole.
    const failed = (error: Error) => (outgoing.headersSent ? outgoing.destroy(error) : answer(502, `bayma was not reached: ${error.message}`));
    /** Passes the request on, its body `body`, whole where it was read whole first, as ../sandbox/gateway-passing.ts says. */
    const pass = (body: Buffer | IncomingMessage): void => {
      const stop = passToGateway({
        ...passing,
        method: incoming.method ?? "GET",
        body,
        // Got as each pass goes, so its bearer is fresh however long its process has lived.
        target: () =>
          run(route.upstream).then((endpoint) => {
            const url = new URL(endpoint.url);
            url.pathname = [url.pathname.replace(/\/$/u, ""), ...below].join("/");
            url.search = asked.search;
            return { url, headers: passedOn(incoming.headers, endpoint.headers) };
          }, (error: { readonly message: string }) => Promise.reject(new Error(error.message))),
        answered: (answered) => {
          outgoing.writeHead(answered.statusCode ?? 502, passedOn(answered.headers, {}));
          outgoing.flushHeaders();
          answered.on("error", failed);
          answered.pipe(outgoing);
        },
        failed,
      });
      // A caller that goes stops its request on, as a stream it no longer reads.
      outgoing.on("close", () => {
        if (!outgoing.writableFinished) stop();
      });
    };
    // One whose body may be read whole first, as an MCP call's, may be passed again; another is passed once, as it comes.
    if (incoming.headers["transfer-encoding"] !== undefined || Number(incoming.headers["content-length"] ?? 0) > REPLAYED_BYTES) return pass(incoming);
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => pass(Buffer.concat(chunks)));
  };

  const server = createServer(handle);
  // A request holds its stream as long as its call runs, as MCP's do.
  server.requestTimeout = 0;
  yield* Effect.acquireRelease(
    Effect.callback<void>((resume) => void server.listen(0, "127.0.0.1", () => resume(Effect.void))),
    () =>
      Effect.callback<void>((resume) => {
        server.close(() => resume(Effect.void));
        server.closeAllConnections();
      }),
  );
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}${BAYMA_PROXY_PATH}`;

  return BaymaProxy.of({
    reach: (key, upstream) =>
      Effect.sync(() => {
        let route = routes.get(key);
        if (route === undefined) {
          const bearer = randomBytes(32).toString("base64url");
          route = { path: randomUUID(), authorization: Buffer.from(`Bearer ${bearer}`), upstream };
          routes.set(key, route);
          paths.set(route.path, route);
        }
        route.upstream = upstream;
        return { url: `${base}/${route.path}`, headers: { Authorization: route.authorization.toString() } };
      }),
  });
});

/** The loopback's endpoints of the workspaces' bayma, as the module says. */
export class BaymaProxy extends Context.Service<BaymaProxy, {
  /**
   * The endpoint on the loopback of the workspace `key`'s bayma, each request to it passed on
   * to the endpoint `upstream` gives as the request goes: the same address and bearer each
   * time it is asked for, its requests passed on by the `upstream` given last.
   */
  readonly reach: (key: string, upstream: Effect.Effect<BaymaEndpoint, { readonly message: string }>) => Effect.Effect<BaymaEndpoint>;
}>()("alasio/mcp/BaymaProxy") {
  static readonly layer: Layer.Layer<BaymaProxy> = Layer.effect(BaymaProxy, makeBaymaProxy());
}
