/**
 * The login relay: how the session-filesystem Codex app-server (./sessionfs.ts) uses the
 * operator's Codex login without holding a copy of it.
 *
 * That app-server has a Codex home of its own, so none of the operator's configuration
 * (skills, memories, plugins, MCP servers) reaches an isolated workspace. A Codex home is
 * also where Codex keeps its login, and a copied login would refresh on its own and
 * invalidate the operator's (a ChatGPT refresh token is single-use). So that home's one
 * model provider is this relay, listening on alasio's loopback. It accepts only the bearer
 * the app-server was started with, allows only the model API's paths, and sends each
 * request on with the operator's login read fresh from their `auth.json`, so a token the
 * operator's Codex refreshes is picked up at once.
 *
 * A ChatGPT login is sent to the ChatGPT Codex backend, which serves the Responses API
 * under /backend-api/codex, with the login's account id; an API-key login is sent to the
 * OpenAI API as it is.
 *
 * Where alasio's harnesses run in pools of their own, alasio serves the same relay to its
 * environment's harness pools' pods, on a port of its pod's (CODEX_LOGIN_PORT), each request
 * bearing a token alasio gives each Codex turn it starts there, signed by its pools' key
 * (poolLoginKey), good for minutes and renewed as the turn runs (../harness/pooled-host.ts),
 * checked in constant time, its refusal said why in alasio's log alone; and a pod relays on
 * its own loopback to it, with the latest token given (../harness/given-codex-login.ts), so
 * only alasio ever holds or refreshes the login (./login-refresh.ts), as a ChatGPT refresh
 * token is single-use; alasio's relay keeps the login fresh before each request it passes on
 * (`fresh`).
 */
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import httpClient, { createServer, type IncomingMessage, type OutgoingHttpHeaders, type RequestOptions, type ServerResponse } from "node:http";
import httpsClient from "node:https";
import type { AddressInfo } from "node:net";

import { Effect, Schema, type Scope } from "effect";

import { verifyToken } from "../binder/tokens.ts";
import { conversationOfHarnessKey } from "../gateway/names.ts";
export { CODEX_LOGIN_PORT } from "../harness/names.ts";
import { withLogScope } from "../shared/log.ts";

/** Codex's `auth.json`, as the relay reads it: any of its fields may be missing or of another type. */
interface CodexAuthFile {
  readonly tokens?: { readonly access_token?: unknown; readonly account_id?: unknown } | null;
  readonly OPENAI_API_KEY?: unknown;
}

/** Where a request is sent on to, and the login headers it is sent with. */
export interface LoginUpstream {
  readonly origin: string;
  readonly path: string;
  readonly headers: Readonly<Record<string, string>>;
}

export interface LoginRelayOptions {
  readonly authFile: string;
  /** Run before each request is passed on, as it keeps the login fresh (./login-refresh.ts); none where the relay passes another's on. */
  readonly fresh?: Effect.Effect<void> | undefined;
  readonly bearer?: string;
  /** What a request's Authorization must be: why it is refused, or null where it may go on; the bearer's alone unless given. */
  readonly authorize?: ((authorization: string | undefined) => string | null) | undefined;
  /** Where it listens: alasio's loopback, on a port of its own, unless given. */
  readonly listen?: { readonly host: string; readonly port: number } | undefined;
  readonly upstreamFor?: (authFile: string, path: string) => LoginUpstream | null;
}

/** The key alasio's pool login tokens are signed with: its environment's own key of the gateway's caller key's, made apart, so a token of one is never the other. */
export const poolLoginKey = (environmentKey: Buffer): Buffer => createHmac("sha256", environmentKey).update("codex-login").digest();

/**
 * Why a request of a harness pool's pod bearing `authorization` is refused, or null where it
 * may go on: its bearer a token signed by `key` (poolLoginKey), compared in constant time, of a
 * conversation's harness's turn, not stopped by `now`.
 */
export const poolLoginRefusal = (key: Buffer, now: () => number) => (authorization: string | undefined): string | null => {
  const bearer = /^Bearer (\S+)$/u.exec(authorization ?? "")?.[1];
  if (!bearer) return "it bears no token";
  const claims = verifyToken(key, bearer, now());
  if (!claims) return "its token is none of alasio's, or has stopped";
  if (claims.expires === undefined || conversationOfHarnessKey(claims.key) === null) return `its token is of ${claims.key}, no conversation's turn's`;
  return null;
};

/** A running relay: the base URL a Codex model provider is given, and the bearer it presents. */
export interface LoginRelay {
  readonly url: string;
  readonly bearer: string;
}

/** The paths a Codex model provider calls: the Responses API (with its subpaths) and the model list. */
const ALLOWED_PATHS = [/^\/v1\/responses(?:[/?]|$)/, /^\/v1\/models(?:[/?]|$)/];

/**
 * Whether the request of `url` is one a Codex model provider makes, as it is written and so as
 * it is sent on: of ALLOWED_PATHS, its path with no segment `.` or `..`, and no dot, slash or
 * backslash encoded, nor a backslash, which a server further on may read as one, so that
 * `/v1/responses/../../backend-api/…` goes nowhere but where it says.
 */
function allowedPath(url: string): boolean {
  const query = url.indexOf("?");
  const path = query < 0 ? url : url.slice(0, query);
  if (/%2e|%2f|%5c|\\/iu.test(path) || path.split("/").some((segment) => segment === "." || segment === "..")) return false;
  return ALLOWED_PATHS.some((re) => re.test(url));
}

/**
 * The upstream request for the login in `authFile`, read now: `{ origin, path, headers }`
 * for a request to `path`, or null when there is no usable login.
 */
export function loginUpstream(authFile: string, path: string): LoginUpstream | null {
  // Codex writes auth.json; each field read from it is checked below.
  let auth: CodexAuthFile | null;
  try {
    auth = JSON.parse(readFileSync(authFile, "utf8"));
  } catch {
    return null;
  }
  const tokens = auth?.tokens;
  if (typeof tokens?.access_token === "string" && tokens.access_token) {
    return {
      origin: "https://chatgpt.com",
      path: path.replace(/^\/v1(?=\/|$)/, "/backend-api/codex"),
      headers: {
        authorization: `Bearer ${tokens.access_token}`,
        ...(typeof tokens.account_id === "string" && tokens.account_id ? { "chatgpt-account-id": tokens.account_id } : {}),
      },
    };
  }
  if (typeof auth?.OPENAI_API_KEY === "string" && auth.OPENAI_API_KEY) {
    return { origin: "https://api.openai.com", path, headers: { authorization: `Bearer ${auth.OPENAI_API_KEY}` } };
  }
  return null;
}

function sameSecret(presented: string | undefined, expected: string) {
  const a = Buffer.from(presented ?? "");
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/** The relay could not listen on alasio's loopback. */
export class LoginRelayError extends Schema.TaggedError<LoginRelayError>()("LoginRelayError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return this.cause instanceof Error ? this.cause.message : String(this.cause);
  }
}

/** Starts the relay of a login: startLoginRelay, or a test's stand-in for it. */
export type StartLoginRelay = (options: LoginRelayOptions) => Effect.Effect<LoginRelay, LoginRelayError, Scope.Scope>;

/**
 * The relay for the login in `authFile`, listening until the scope closes: the base URL
 * a Codex model provider is given (its `/v1` included), and the bearer it must present.
 * `upstreamFor` resolves the upstream request, for tests.
 */
export const startLoginRelay: StartLoginRelay = Effect.fnUntraced(function*({
  authFile,
  bearer = randomBytes(32).toString("hex"),
  authorize,
  listen = { host: "127.0.0.1", port: 0 },
  upstreamFor = loginUpstream,
  fresh = Effect.void,
}: LoginRelayOptions) {
  const request = (options: RequestOptions, onResponse: (response: IncomingMessage) => void) => (options.protocol === "http:" ? httpClient : httpsClient).request(options, onResponse);
  const expected = `Bearer ${bearer}`;
  const refusalOf = authorize ?? ((authorization: string | undefined) => (sameSecret(authorization, expected) ? null : "unknown bearer"));
  const runFork = Effect.runForkWith(yield* Effect.context<never>());
  const server = createServer((req, res) => {
    const refusal = refusalOf(req.headers.authorization);
    if (refusal !== null) {
      // Why, to alasio's log alone: the caller is told it is refused, and nothing of why.
      runFork(Effect.logWarning(`refused a request of ${req.url ?? "/"}: ${refusal}`).pipe(withLogScope("codex-login-relay")));
      res.writeHead(401).end("refused");
      return;
    }
    // A request an http.Server receives always has a url.
    const url = req.url!;
    if (!allowedPath(url)) { res.writeHead(403).end("path not allowed"); return; }
    // The login read once it is fresh, the request held meanwhile; answered 502 where keeping it fresh died, not left waiting.
    req.pause();
    void Effect.runPromiseWith(context)(fresh).then(() => passOn(req, res, url), (defect: unknown) => {
      runFork(Effect.logError(`could not keep the Codex login fresh for a request of ${url}: ${defect instanceof Error ? defect.message : String(defect)}`).pipe(withLogScope("codex-login-relay")));
      req.resume();
      res.writeHead(502).end("the Codex login could not be kept fresh");
    });
  });
  const context = yield* Effect.context<never>();
  /** Passes `req`, of `url`, on with the login in `authFile`, its answer to `res`. */
  const passOn = (req: IncomingMessage, res: ServerResponse, url: string) => {
    const upstream = upstreamFor(authFile, url);
    if (!upstream) { res.writeHead(503).end(`no Codex login in ${authFile}`); return; }
    const origin = new URL(upstream.origin);
    const headers: OutgoingHttpHeaders = { ...req.headers, host: origin.host };
    delete headers.authorization;
    delete headers["x-api-key"];
    Object.assign(headers, upstream.headers);
    const upstreamReq = request(
      { protocol: origin.protocol, hostname: origin.hostname, port: origin.port || undefined, path: upstream.path, method: req.method, headers },
      (up) => { res.writeHead(up.statusCode ?? 502, up.headers); up.pipe(res); },
    );
    upstreamReq.on("error", (error) => {
      if (!res.headersSent) res.writeHead(502);
      res.end(String(error.message));
    });
    req.pipe(upstreamReq);
    req.resume();
  };
  yield* Effect.acquireRelease(
    Effect.callback<void, LoginRelayError>((resume) => {
      server.once("error", (cause) => resume(Effect.fail(new LoginRelayError({ cause }))));
      server.listen(listen.port, listen.host, () => resume(Effect.void));
    }),
    () => Effect.callback<void>((resume) => {
      server.close(() => resume(Effect.void));
    }),
  );
  // A server listening on a TCP port has an AddressInfo address.
  const { port } = server.address() as AddressInfo;
  yield* Effect.logInfo(`relaying the Codex login in ${authFile} on ${listen.host}:${port}`);
  return { url: `http://${listen.host}:${port}/v1`, bearer };
}, withLogScope("codex-login-relay"));
