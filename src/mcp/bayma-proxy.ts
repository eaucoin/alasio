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
 * not reached, 502. One the gateway answers 503 saying it passed it to no pod
 * (../binder/names.ts NOT_PASSED_HEADER), as it answers a bearer held to a binding it has not
 * heard of yet, is passed again after its Retry-After, for RETRY_WITHIN_MS, where its body
 * could be read whole first; any other is answered as the gateway answered it, as an MCP call
 * a pod was given may have run. A caller that goes stops its request on, and a
 * bayma that goes mid-answer ends the caller's, as the binder's proxy does (../binder/proxy.ts).
 *
 * The gateway not reached at all, its connection never made, as while it starts again, a
 * request is passed again every UNREACHED_EVERY_MS, for `unreachedForMs`
 * (GATEWAY_RESTART_MS) from when it came, where its body could be read whole first: no pod was
 * given it. So a call a harness makes while the gateway starts again is answered once it is,
 * as is the GET its MCP client makes to reopen its stream, rather than each answered 502.
 */
import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { type ClientRequest, createServer, type IncomingHttpHeaders, type IncomingMessage, request as httpRequest, type ServerResponse } from "node:http";
import { request as httpsRequest } from "node:https";
import type { AddressInfo } from "node:net";

import { Context, Effect, Layer, type Scope } from "effect";

import { NOT_PASSED_HEADER } from "../binder/names.ts";
import type { BaymaEndpoint } from "../sandbox/gateway-client.ts";

/** The path the loopback's endpoints are below, as an MCP server over HTTP is reached. */
const BAYMA_PROXY_PATH = "/mcp";

/**
 * The largest body of a request the loopback reads whole before it passes it on, so it may pass
 * it again: an MCP call's, with room. One larger, or of no stated length, is passed once, as it
 * comes.
 */
const REPLAYED_BYTES = 1024 ** 2;

/** How long a request answered 503 with a Retry-After is passed again for, from when it came. */
const RETRY_WITHIN_MS = 30_000;

/**
 * How long a request whose connection to the gateway was never made is passed again for, from
 * when it came: past a restart of the gateway's, which the end-to-end test measures (the gateway
 * reached by none of its pods 12 to 25 s, on k3s in Docker and on a host alike), with room; and
 * within a harness's own wait for a call (Claude Code's 60 s), so that one given up is answered
 * the loopback's 502, its call never passed on, before its harness times it out unsure.
 */
export const GATEWAY_RESTART_MS = 45_000;

/** How often a request is passed again while its connection to the gateway is never made. */
const UNREACHED_EVERY_MS = 1_000;

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

/** What the loopback is made with: how long a request whose connection to the gateway is never made is passed again for, GATEWAY_RESTART_MS but for tests. */
export interface BaymaProxyOptions {
  readonly unreachedForMs?: number | undefined;
}

/** The loopback's endpoints, as the module says, served until the scope closes. */
export const makeBaymaProxy = ({ unreachedForMs = GATEWAY_RESTART_MS }: BaymaProxyOptions = {}): Effect.Effect<BaymaProxy["Service"], never, Scope.Scope> => Effect.gen(function*() {
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
    const until = Date.now() + RETRY_WITHIN_MS;
    const unreachedUntil = Date.now() + unreachedForMs;
    let current: ClientRequest | null = null;
    // A caller that goes stops its request on, as a stream it no longer reads.
    outgoing.on("close", () => {
      if (!outgoing.writableFinished) current?.destroy();
    });
    /**
     * Passes the request on, its body `body` where it was read whole first: then, answered 503
     * by a gateway that says it passed it to no pod (NOT_PASSED_HEADER), as it answers a bearer
     * held to a binding it does not know of yet, passed again after its Retry-After, until
     * RETRY_WITHIN_MS; its connection to the gateway never made, passed again every
     * UNREACHED_EVERY_MS until `unreachedForMs`; otherwise as answered, as a pod given it may
     * have run it.
     */
    const pass = (body: Buffer | null): void =>
      // Got as the request goes, so its bearer is fresh however long its process has lived.
      void run(route.upstream).then((endpoint) => {
        if (outgoing.destroyed) return;
        const target = new URL(endpoint.url);
        target.pathname = [target.pathname.replace(/\/$/u, ""), ...below].join("/");
        target.search = asked.search;
        const send = target.protocol === "https:" ? httpsRequest : httpRequest;
        current = send(target, { method: incoming.method ?? "GET", headers: passedOn(incoming.headers, endpoint.headers) }, (answered) => {
          const after = Number(answered.headers["retry-after"] ?? 1);
          if (answered.statusCode === 503 && answered.headers[NOT_PASSED_HEADER] !== undefined && body !== null && Number.isFinite(after) && Date.now() + after * 1000 < until) {
            answered.resume();
            return void setTimeout(() => pass(body), after * 1000);
          }
          outgoing.writeHead(answered.statusCode ?? 502, passedOn(answered.headers, {}));
          outgoing.flushHeaders();
          answered.on("error", failed);
          answered.pipe(outgoing);
        });
        /** Whether its connection to the gateway was made: one that was may have passed it on. */
        let connected = false;
        current.on("socket", (socket) => {
          if (socket.connecting) socket.once("connect", () => void (connected = true));
          else connected = true;
        });
        current.on("error", (error) => {
          if (connected || body === null || Date.now() + UNREACHED_EVERY_MS >= unreachedUntil || outgoing.destroyed) return failed(error);
          setTimeout(() => pass(body), UNREACHED_EVERY_MS);
        });
        if (body === null) incoming.pipe(current);
        else current.end(body);
      }, (error: { readonly message: string }) => answer(502, `bayma was not reached: ${error.message}`));
    // One whose body may be read whole first, as an MCP call's, may be passed again; another is passed once, as it comes.
    if (incoming.headers["transfer-encoding"] !== undefined || Number(incoming.headers["content-length"] ?? 0) > REPLAYED_BYTES) return pass(null);
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
