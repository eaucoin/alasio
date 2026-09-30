/**
 * The login relay: how the session-filesystem Codex app-server (./sessionfs.js) uses the
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
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { readFileSync } from "node:fs";
import httpClient, { createServer } from "node:http";
import httpsClient from "node:https";
import { createLogger } from "../shared/log.js";

const log = createLogger("codex-login-relay");

/** The paths a Codex model provider calls: the Responses API (with its subpaths) and the model list. */
const ALLOWED_PATHS = [/^\/v1\/responses(?:[/?]|$)/, /^\/v1\/models(?:[/?]|$)/];

/**
 * The upstream request for the login in `authFile`, read now: `{ origin, path, headers }`
 * for a request to `path`, or null when there is no usable login.
 */
export function loginUpstream(authFile, path) {
  let auth;
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

function sameSecret(presented, expected) {
  const a = Buffer.from(presented ?? "");
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/**
 * Start the relay for the login in `authFile`. Returns `{ url, bearer, close }`: the base
 * URL a Codex model provider is given (its `/v1` included), the bearer it must present,
 * and a close. `upstreamFor` resolves the upstream request, for tests.
 */
export async function startLoginRelay({ authFile, bearer = randomBytes(32).toString("hex"), upstreamFor = loginUpstream }) {
  const request = (options, onResponse) => (options.protocol === "http:" ? httpClient : httpsClient).request(options, onResponse);
  const expected = `Bearer ${bearer}`;
  const server = createServer((req, res) => {
    if (!sameSecret(req.headers.authorization, expected)) { res.writeHead(401).end("unknown bearer"); return; }
    if (!ALLOWED_PATHS.some((re) => re.test(req.url))) { res.writeHead(403).end("path not allowed"); return; }
    const upstream = upstreamFor(authFile, req.url);
    if (!upstream) { res.writeHead(503).end(`no Codex login in ${authFile}`); return; }
    const origin = new URL(upstream.origin);
    const headers = { ...req.headers, host: origin.host };
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
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const { port } = server.address();
  log.info(`relaying the Codex login in ${authFile} on 127.0.0.1:${port}`);
  return {
    url: `http://127.0.0.1:${port}/v1`,
    bearer,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}
