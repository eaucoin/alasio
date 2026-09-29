/**
 * The session gateway: the one network peer a sandboxed agent may always reach (the
 * network confines it to this address in "none" mode, and to this plus the internet in
 * "full" mode; see ../sandbox/session-host/entrypoint.sh and session-fs-research E7).
 *
 * It is the credential boundary. The real model login lives only here, outside every
 * sandbox. A session holds a per-session bearer this gateway issued; the gateway
 * accepts only known bearers, allows only the providers' API paths, and swaps the
 * bearer for the real credential on the way out. A bearer is not a credential: it is
 * valid only here, only for its session, and is revoked when the session ends, so even
 * if the agent leaks or echoes it, it is worthless elsewhere. This is why the gateway,
 * not credential masking, is the design (session-fs-research notes/srt.md, E8).
 *
 * One gateway address serves both providers, chosen by request path (Claude Code calls
 * /v1/messages, Codex calls /v1/responses), so a session points its harness's base URL
 * at this one address. A provider whose login the operator has not supplied answers
 * 503; the mechanism (bearer validation, path allowlist) stays live regardless.
 */
import { createServer } from "node:http";
import httpClient from "node:http";
import httpsClient from "node:https";
import { createLogger } from "../shared/log.js";

const log = createLogger("session-gateway");

/** Per provider: which request paths reach the API, and how the credential is carried. */
const PROVIDER_SPECS = {
  anthropic: {
    allow: [/^\/api\/hello$/, /^\/v1\/messages(?:\?|$)/, /^\/v1\/messages\/count_tokens(?:\?|$)/],
    // An API key travels as x-api-key; a subscription OAuth token (from the local
    // `claude` login) travels as a Bearer, the shape Claude Code itself already sends,
    // so its anthropic-beta oauth header passes through unchanged.
    inject: (headers, credential, oauth) => {
      if (oauth) { delete headers["x-api-key"]; headers.authorization = `Bearer ${credential}`; }
      else { delete headers.authorization; headers["x-api-key"] = credential; }
    },
  },
  openai: {
    allow: [/^\/v1\/responses(?:\?|$)/, /^\/v1\/chat\/completions(?:\?|$)/],
    inject: (headers, credential) => { delete headers["x-api-key"]; headers.authorization = `Bearer ${credential}`; },
  },
};

const bearerOf = (req) =>
  (req.headers.authorization ?? "").replace(/^Bearer\s+/i, "") || (req.headers["x-api-key"] ?? "");

export class SessionGateway {
  #bearers = new Map(); // bearer -> sessionId
  #server = null;

  /**
   * `providers` maps a provider name ("anthropic"/"openai") to `{ upstream,
   * credentialSource }`: the real API origin and a function returning the real
   * credential or null when none is configured yet. `newBearer` makes an unguessable
   * token.
   */
  constructor({ providers, newBearer = () => crypto.randomUUID() + crypto.randomUUID() }) {
    this.providers = Object.entries(providers).map(([name, p]) => {
      const spec = PROVIDER_SPECS[name];
      if (!spec) throw new Error(`unknown gateway provider: ${name}`);
      return { name, spec, upstream: new URL(p.upstream), credentialSource: p.credentialSource, oauth: p.oauth ?? false };
    });
    this.newBearer = newBearer;
  }

  /** Mint a bearer for a session; the session presents it, never the real credential. */
  issueBearer(sessionId) {
    const bearer = this.newBearer();
    this.#bearers.set(bearer, sessionId);
    return bearer;
  }

  /** Drop a session's bearer; a leaked bearer stops working the moment its session ends. */
  revokeSession(sessionId) {
    for (const [bearer, owner] of this.#bearers) {
      if (owner === sessionId) this.#bearers.delete(bearer);
    }
  }

  /** Node's request handler, exposed for tests; the server uses it. */
  handler = (req, res) => {
    if (!this.#bearers.has(bearerOf(req))) { res.writeHead(401).end("unknown session bearer"); return; }
    const provider = this.providers.find((p) => p.spec.allow.some((re) => re.test(req.url)));
    if (!provider) { res.writeHead(403).end("path not allowed"); return; }
    const credential = provider.credentialSource();
    if (!credential) { res.writeHead(503).end(`${provider.name} login not configured`); return; }
    const headers = { ...req.headers, host: provider.upstream.host };
    delete headers["x-api-key"];
    delete headers.authorization;
    provider.spec.inject(headers, credential, provider.oauth);
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const client = provider.upstream.protocol === "https:" ? httpsClient : httpClient;
      const upstreamReq = client.request(
        { hostname: provider.upstream.hostname, port: provider.upstream.port, path: req.url, method: req.method, headers },
        (up) => { res.writeHead(up.statusCode ?? 502, up.headers); up.pipe(res); },
      );
      upstreamReq.on("error", (e) => { res.writeHead(502).end(String(e.message)); });
      upstreamReq.end(Buffer.concat(chunks));
    });
  };

  /** Start listening. Returns `{ port, close }`. */
  async listen(port, host = "0.0.0.0") {
    this.#server = createServer(this.handler);
    await new Promise((resolve) => this.#server.listen(port, host, resolve));
    const actual = this.#server.address().port;
    log.info(`session gateway listening on ${host}:${actual} for ${this.providers.map((p) => p.name).join(", ")}`);
    return { port: actual, close: () => new Promise((r) => this.#server.close(r)) };
  }
}
