/**
 * The snapshots bucket as a restore's checkpoint gofer reads it from the node: through the
 * node agent, which the gofer, on the node's own network outside the cluster's, reaches at
 * its pod's address, as a node reaches the pods it runs, and which reaches the bucket as a
 * pod of the cluster, its network policies admitting the agent's pods alone.
 *
 * The gofer holds none of the bucket's credentials. Each restore from the bucket is given a
 * token of its own (mint): an access key naming it and a secret of 256 random bits, which
 * the gofer signs its requests with, as Signature Version 4 signs; kept in the agent's memory
 * alone, so that none outlives the agent, and revoked as the workspace's restore is done
 * with (revoke). A request the proxy takes is a GET of an object under that restore's
 * layers prefix, in the snapshots bucket, signed by its token, as the official signer
 * (@smithy/signature-v4) signs it, within SKEW_MS of now, its host, its date, its payload's
 * hash, its range and every header of Amazon's it sends among what it signs, for the
 * bucket's region and S3. Anything else is refused, as S3 refuses it. What it takes, it asks
 * of the bucket again, signed with the agent's own credentials, and streams the answer back
 * as the bucket gives it, its status and its body, a request its client ends ending the
 * bucket's too.
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, request as httpRequest, type Server, type ServerResponse } from "node:http";
import { request as httpsRequest } from "node:https";
import type { AddressInfo } from "node:net";

import { Hash } from "@smithy/hash-node";
import { SignatureV4 } from "@smithy/signature-v4";
import { Effect, Schema, type Scope } from "effect";

/** The bucket the proxy reads, and as whom. */
export interface ProxiedStore {
  /** The S3 API's URL, `http(s)://host[:port]`, as the agent reaches it. */
  readonly endpoint: string;
  readonly region: string;
  readonly bucket: string;
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
}

/** A restore's credentials for the proxy: what its gofer signs with. */
export interface StoreToken {
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
}

/** The snapshots bucket through the node agent, as the module says. */
export interface StoreProxy {
  /** The proxy's URL, `http://<address>:<port>`, which the gofer is given as its endpoint. */
  readonly endpoint: string;
  /** The bucket's region, which the gofer signs for. */
  readonly region: string;
  /** The bucket, which the gofer names. */
  readonly bucket: string;
  /**
   * A token for the restore `holder` names, of the objects under `prefix` alone; one it held
   * before is revoked.
   */
  readonly mint: (holder: string, prefix: string) => StoreToken;
  /** Revokes the token `holder` holds, if any: a request signed with it is refused from now. */
  readonly revoke: (holder: string) => void;
  /** Revokes the token `holder` holds, if any, in `ms`, unless it is revoked or minted anew before. */
  readonly revokeIn: (holder: string, ms: number) => void;
}

/** The proxy could not listen where it was told. */
export class StoreProxyError extends Schema.TaggedError<StoreProxyError>()("StoreProxyError", {
  reason: Schema.String,
}) {
  override get message(): string {
    return `the node agent cannot serve the snapshots bucket to its node: ${this.reason}`;
  }
}

/** How far a request's date may be from the proxy's clock. */
export const SKEW_MS = 5 * 60_000;

/** The headers of the bucket's answer the proxy gives back; the others are the connection's. */
const ANSWERED = ["content-length", "content-range", "content-type", "accept-ranges", "etag", "last-modified", "x-amz-request-id", "x-amz-id-2"];

const AUTHORIZATION = /^AWS4-HMAC-SHA256 Credential=([^/,\s]+)\/(\d{8})\/([^/,\s]+)\/([^/,\s]+)\/aws4_request,\s*SignedHeaders=([a-z0-9;-]+),\s*Signature=([0-9a-f]{64})$/u;
const AMZ_DATE = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/u;
/** The query a GetObject may carry: the operation's name, as the AWS SDKs add it. */
const QUERY_KEPT = new Set(["x-id"]);

/** `message` as S3 answers a refusal: its status, an error of `code`. */
function refuse(response: ServerResponse, status: number, code: string, message: string): void {
  const escaped = message.replace(/[&<>"']/gu, (char) => `&#${char.charCodeAt(0)};`);
  response.writeHead(status, { "content-type": "application/xml" }).end(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code><Message>${escaped}</Message></Error>`);
}

/** The time `amzDate` says (YYYYMMDDTHHMMSSZ), or null where it says none. */
function timeOf(amzDate: string): Date | null {
  const parts = AMZ_DATE.exec(amzDate);
  if (!parts) return null;
  const [, year, month, day, hour, minute, second] = parts.map(Number);
  const at = new Date(Date.UTC(year ?? 0, (month ?? 1) - 1, day, hour, minute, second));
  return Number.isNaN(at.getTime()) ? null : at;
}

/**
 * The key a path-style `path` names in `bucket`, decoded, or null where it names another
 * bucket, or is no key's: a segment empty, `.`, `..`, or holding, decoded, a slash, a
 * backslash, a NUL or a percent, as a path encoded twice would.
 */
export function keyOf(path: string, bucket: string): string | null {
  const segments = path.split("/");
  if (segments[0] !== "" || segments.length < 3) return null;
  const decoded: string[] = [];
  for (const segment of segments.slice(1)) {
    let plain: string;
    try {
      plain = decodeURIComponent(segment);
    } catch {
      return null;
    }
    if (plain === "" || plain === "." || plain === ".." || /[/\\\0%]/u.test(plain)) return null;
    decoded.push(plain);
  }
  const [named, ...key] = decoded;
  return named === bucket ? key.join("/") : null;
}

/**
 * The proxy of `store`, as the module says, listening on `host`:`port` (0 for any) for as
 * long as the scope lasts.
 */
export const makeStoreProxy = Effect.fnUntraced(function*({ store, host, port }: { readonly store: ProxiedStore; readonly host: string; readonly port: number }): Effect.fn.Return<StoreProxy, StoreProxyError, Scope.Scope> {
  const tokens = new Map<string, { readonly secretAccessKey: string; readonly prefix: string; readonly holder: string; until: number }>();
  const held = new Map<string, string>();
  const upstream = new URL(store.endpoint);
  const upstreamSigner = new SignatureV4({
    credentials: { accessKeyId: store.accessKeyId, secretAccessKey: store.secretAccessKey },
    region: store.region,
    service: "s3",
    sha256: Hash.bind(null, "sha256"),
    uriEscapePath: false,
  });

  /** Whether `request`, signed as its Authorization says, is signed by the token it names, as the official signer signs: the token's grant, or why not. */
  const verified = async (request: IncomingMessage, path: string, query: Record<string, string>): Promise<{ readonly prefix: string } | { readonly refused: string }> => {
    if (Object.keys(query).some((name) => name.toLowerCase().startsWith("x-amz-"))) return { refused: "a request signed in its query is not taken" };
    const said = AUTHORIZATION.exec(String(request.headers.authorization ?? ""));
    if (!said) return { refused: "the request is not signed with Signature Version 4" };
    const [, accessKeyId = "", day = "", region, service, signedList = "", signature = ""] = said;
    const token = tokens.get(accessKeyId);
    if (!token || Date.now() >= token.until) return { refused: "the request is signed by no token of a restore's" };
    if (region !== store.region || service !== "s3") return { refused: `the request is signed for ${region}'s ${service}, not ${store.region}'s s3` };
    const amzDate = String(request.headers["x-amz-date"] ?? "");
    const signingDate = timeOf(amzDate);
    if (!signingDate || amzDate.slice(0, 8) !== day) return { refused: "the request's date is not the one it is signed on" };
    if (Math.abs(Date.now() - signingDate.getTime()) > SKEW_MS) return { refused: `the request's date is more than ${SKEW_MS / 60_000} minutes from now` };
    const signed = signedList.split(";");
    const required = ["host", "x-amz-date", "x-amz-content-sha256", ...(request.headers.range === undefined ? [] : ["range"])];
    const unsigned = [...required, ...Object.keys(request.headers).filter((name) => name.startsWith("x-amz-"))].filter((name) => !signed.includes(name));
    if (unsigned.length > 0) return { refused: `the request does not sign ${[...new Set(unsigned)].join(", ")}` };
    const headers: Record<string, string> = {};
    for (const name of signed) {
      const value = request.headers[name];
      if (value === undefined) return { refused: `the request signs ${name}, which it does not send` };
      headers[name] = Array.isArray(value) ? value.join(",") : value;
    }
    const expected = await new SignatureV4({
      credentials: { accessKeyId, secretAccessKey: token.secretAccessKey },
      region: store.region,
      service: "s3",
      sha256: Hash.bind(null, "sha256"),
      uriEscapePath: false,
    }).sign(
      { method: request.method ?? "", protocol: "http:", hostname: headers["host"] ?? "", path, query, headers },
      { signingDate, signableHeaders: new Set(signed) },
    );
    const computed = /Signature=([0-9a-f]{64})/u.exec(String(expected.headers["authorization"]))?.[1] ?? "";
    if (computed.length !== signature.length || !timingSafeEqual(Buffer.from(computed), Buffer.from(signature))) return { refused: "the request's signature is not its token's" };
    return { prefix: token.prefix };
  };

  /** Asks the bucket for what `request` asked, as the agent, streaming its answer back. */
  const forward = async (request: IncomingMessage, response: ServerResponse, path: string, query: Record<string, string>): Promise<void> => {
    const headers: Record<string, string> = { host: upstream.host, "x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" };
    if (request.headers.range !== undefined) headers["range"] = String(request.headers.range);
    const signed = await upstreamSigner.sign({ method: "GET", protocol: upstream.protocol, hostname: upstream.hostname, ...(upstream.port === "" ? {} : { port: Number(upstream.port) }), path, query, headers });
    const search = new URLSearchParams(query).toString();
    const asked = (upstream.protocol === "https:" ? httpsRequest : httpRequest)({
      method: "GET",
      host: upstream.hostname,
      port: upstream.port || (upstream.protocol === "https:" ? 443 : 80),
      path: search === "" ? path : `${path}?${search}`,
      headers: signed.headers,
    });
    // The client's end ends the bucket's request: nothing is read for no one.
    const abandoned = () => asked.destroy();
    response.once("close", abandoned);
    asked.once("response", (answer) => {
      const kept: Record<string, string | string[]> = {};
      for (const name of ANSWERED) {
        const value = answer.headers[name];
        if (value !== undefined) kept[name] = value;
      }
      response.writeHead(answer.statusCode ?? 502, kept);
      answer.pipe(response);
      answer.once("error", () => response.destroy());
      answer.once("end", () => response.off("close", abandoned));
    });
    asked.once("error", (error) => {
      if (response.headersSent) response.destroy();
      else refuse(response, 502, "BadGateway", `the bucket did not answer: ${error.message}`);
    });
    asked.end();
  };

  const handle = (request: IncomingMessage, response: ServerResponse): void => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://proxy");
      const query = Object.fromEntries(url.searchParams);
      if (request.method !== "GET") return refuse(response, 405, "MethodNotAllowed", `${request.method} is not taken: a restore reads`);
      if (Object.keys(query).some((name) => !QUERY_KEPT.has(name) && !name.toLowerCase().startsWith("x-amz-"))) return refuse(response, 400, "InvalidRequest", "the request asks what a restore's read does not");
      const path = url.pathname;
      const key = keyOf(path, store.bucket);
      if (key === null) return refuse(response, 403, "AccessDenied", "the request names no object of the snapshots bucket");
      const grant = await verified(request, path, query);
      if ("refused" in grant) return refuse(response, 403, "AccessDenied", grant.refused);
      if (!key.startsWith(grant.prefix)) return refuse(response, 403, "AccessDenied", "the object is not one the restore reads");
      await forward(request, response, path, query);
    })().catch((error: unknown) => {
      if (response.headersSent) response.destroy();
      else refuse(response, 500, "InternalError", error instanceof Error ? error.message : String(error));
    });
  };

  const server: Server = yield* Effect.acquireRelease(
    Effect.callback<Server, StoreProxyError>((resume) => {
      const made = createServer(handle);
      made.once("error", (error) => resume(Effect.fail(new StoreProxyError({ reason: `${host}:${port}: ${error.message}` }))));
      made.listen(port, host, () => resume(Effect.succeed(made)));
    }),
    (made) => Effect.callback<void>((resume) => {
      made.closeAllConnections();
      made.close(() => resume(Effect.void));
    }),
  );
  const address = server.address() as AddressInfo;
  const shown = address.family === "IPv6" ? `[${address.address}]` : address.address;

  return {
    endpoint: `http://${shown}:${address.port}`,
    region: store.region,
    bucket: store.bucket,
    mint: (holder, prefix) => {
      const before = held.get(holder);
      if (before !== undefined) tokens.delete(before);
      const accessKeyId = `ALASIO${randomBytes(10).toString("hex").toUpperCase()}`;
      const secretAccessKey = randomBytes(32).toString("base64url");
      tokens.set(accessKeyId, { secretAccessKey, prefix, holder, until: Infinity });
      held.set(holder, accessKeyId);
      return { accessKeyId, secretAccessKey };
    },
    revoke: (holder) => {
      const accessKeyId = held.get(holder);
      if (accessKeyId === undefined) return;
      tokens.delete(accessKeyId);
      held.delete(holder);
    },
    revokeIn: (holder, ms) => {
      const token = tokens.get(held.get(holder) ?? "");
      if (token) token.until = Math.min(token.until, Date.now() + ms);
    },
  };
});
