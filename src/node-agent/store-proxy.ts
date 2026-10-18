/**
 * The snapshots bucket as a restore's checkpoint gofer reads it from the node: through the
 * node agent, which the gofer, on the node's own network outside the cluster's, reaches at
 * its pod's address, as a node reaches the pods it runs, and which reaches the bucket as a
 * pod of the cluster, its network policies admitting the agent's pods alone.
 *
 * The gofer holds none of the bucket's credentials. Each restore from the bucket is given a
 * token of its own (mint): an access key naming it and a secret of 256 random bits, which
 * the gofer signs its requests with, as Signature Version 4 signs; kept in the agent's memory
 * alone, so that none outlives the agent. A token lasts as long as a restore takes from its
 * prepare to its resume (PREPARED_FOR_MS), and from its resume as long as the agent says its
 * background reads may take (resumed); it is revoked before as the workspace's restore is
 * done with (revoke). A request the proxy takes is a GET of an object under that restore's
 * layers prefix, in the snapshots bucket, signed by its token, as the official signer
 * (@smithy/signature-v4) signs it, within SKEW_MS of now, its host, its date, its payload's
 * hash, its range and every header of Amazon's it sends among what it signs, for the
 * bucket's region and S3. Anything else is refused, as S3 refuses it. What it takes, it asks
 * of the bucket again, signed with the agent's own credentials, and streams the answer back
 * as the bucket gives it, its status and its body, a request its client ends ending the
 * bucket's too.
 *
 * A restore reads through the proxy until its sandbox has every page it was saved with,
 * which it reads in the background once it is resumed: an agent that stopped meanwhile would
 * leave it without the rest, for good (gate 8 of program/12-live-lazy.md). So the agent's stop
 * waits on the proxy's drain, which mints no token more, and ends once no restore reads
 * through it, within each token's time: none prepared and not resumed yet, none whose
 * request is under way, and none resumed that has not been served the bytes of pages its
 * image holds, where the agent told them (expect), counted once each, however they were
 * read, or, where it did not, whose last request ended within QUIET_MS. A background
 * load's reads may be spaced as long as its loader paces them, longer than any quiet: the
 * bytes say when it is done. A pages file's bytes are counted as they are handed to the
 * gofer's connection, from where the bucket's answer says they begin (its Content-Range, or
 * 0 for the whole object), an answer cut short counting what it gave, as the gofer keeps
 * those bytes and asks only for the rest.
 */
import { randomBytes, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, request as httpRequest, type Server, type ServerResponse } from "node:http";
import { request as httpsRequest } from "node:https";
import type { AddressInfo } from "node:net";

import { Hash } from "@smithy/hash-node";
import { SignatureV4 } from "@smithy/signature-v4";
import { Effect, FiberSet, Schedule, Schema, type Scope } from "effect";

import { RESTORE_PREPARED_FOR_SECONDS } from "./names.ts";

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
   * A token for the restore `holder` names, of the objects under `prefix` alone, for
   * PREPARED_FOR_MS; one it held before is revoked. None once the proxy drains.
   */
  readonly mint: (holder: string, prefix: string) => StoreToken | null;
  /** Revokes the token `holder` holds, if any: a request signed with it is refused from now. */
  readonly revoke: (holder: string) => void;
  /** The token `holder` holds, if any, lasts `ms` from now, as its restore's sandbox, resumed, reads the rest of its pages. */
  readonly resumed: (holder: string, ms: number) => void;
  /**
   * The restore of the token `accessKeyId`, if the proxy holds it still, reads `bytes` bytes
   * of the pages files under its prefix, its image's and its layers' (`.../pages.img`), as
   * runsc says of its image: until it has been served them all, it reads, however quiet;
   * Infinity while they are not known yet, null where they are not known (then its quiet
   * tells, as for none). By the token, not its holder, so that what is said of one restore
   * is never taken for a later one's of the same workspace.
   */
  readonly expect: (accessKeyId: string, bytes: number | null) => void;
  /** Mints no token more, and ends once no restore reads through the proxy, as the module says. */
  readonly drained: Effect.Effect<void>;
  /** How many restores read through the proxy, as its drain waits on them. */
  readonly reading: () => number;
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
/** How long a token lasts from its mint, unless its restore is resumed before (./names.ts RESTORE_PREPARED_FOR_SECONDS). */
export const PREPARED_FOR_MS = RESTORE_PREPARED_FOR_SECONDS * 1000;
/**
 * How long a resumed restore's reads have been quiet once its background reads are taken as
 * done: longer than the gofer waits between two attempts of a read, 20 s at most.
 */
export const QUIET_MS = 60_000;
/** How often the drain looks at what reads through the proxy. */
const DRAIN_POLL = "1 second";

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

/** What makeStoreProxy is given: the store, where it listens, and, unless told, PREPARED_FOR_MS and QUIET_MS (for a restore whose bytes the agent did not tell). */
export interface StoreProxyOptions {
  readonly store: ProxiedStore;
  readonly host: string;
  /** Its port; 0 for any. */
  readonly port: number;
  readonly preparedForMs?: number;
  readonly quietMs?: number;
}

/** A restore's token, as the proxy holds it, and how its reads go. */
interface HeldToken {
  readonly secretAccessKey: string;
  readonly prefix: string;
  readonly holder: string;
  /** When it ends, in milliseconds since the epoch. */
  until: number;
  /** Whether its restore's sandbox is resumed, reading the rest of its pages in the background. */
  resumed: boolean;
  /** How many of its requests are under way. */
  under: number;
  /** When its last request began or ended, or it was resumed. */
  lastAt: number;
  /** The bytes of pages files its restore reads, where the agent told them (expect). */
  expected: number | null;
  /** The byte ranges of each pages file it was served, each [start, end), sorted and apart. */
  readonly served: Map<string, [number, number][]>;
  /** How many bytes those ranges hold, together. */
  servedBytes: number;
  /** Whether it has been served every byte it was expected to, as the proxy logs once. */
  servedAll: boolean;
}

/** Whether `key` is a pages file of an image's, as a restore's background load reads them. */
const isPagesFile = (key: string): boolean => key.endsWith("/pages.img");

/**
 * Where the body of the bucket's answer `status`, of Content-Range `range`, begins in its
 * object: 0 for the whole object (200), its range's first byte for part of it (206); null for
 * any other, and for a part whose range does not say (bytes FIRST-LAST/SIZE), which the proxy
 * counts nothing of.
 */
export function answerStart(status: number, range: string | undefined): number | null {
  if (status === 200) return 0;
  if (status !== 206 || range === undefined) return null;
  const first = /^bytes (\d+)-\d+\/(?:\d+|\*)$/u.exec(range)?.[1];
  return first === undefined ? null : Number(first);
}

/** `ranges`, each [start, end), sorted and apart, with [start, end) added. */
export function withRange(ranges: readonly [number, number][], start: number, end: number): [number, number][] {
  if (end <= start) return [...ranges];
  const merged: [number, number][] = [];
  let [from, to] = [start, end];
  for (const [a, b] of ranges) {
    if (b < from || a > to) merged.push([a, b]);
    else [from, to] = [Math.min(a, from), Math.max(b, to)];
  }
  merged.push([from, to]);
  return merged.sort(([a], [b]) => a - b);
}

/** How many bytes `ranges` hold. */
const bytesOf = (ranges: readonly [number, number][]): number => ranges.reduce((sum, [a, b]) => sum + b - a, 0);

/** The proxy of `store`, as the module says, listening on `host`:`port` for as long as the scope lasts. */
export const makeStoreProxy = Effect.fnUntraced(function*({ store, host, port, preparedForMs = PREPARED_FOR_MS, quietMs = QUIET_MS }: StoreProxyOptions): Effect.fn.Return<StoreProxy, StoreProxyError, Scope.Scope> {
  const tokens = new Map<string, HeldToken>();
  const runFork = yield* FiberSet.makeRuntime();
  let draining = false;
  const held = new Map<string, string>();
  const upstream = new URL(store.endpoint);
  const upstreamSigner = new SignatureV4({
    credentials: { accessKeyId: store.accessKeyId, secretAccessKey: store.secretAccessKey },
    region: store.region,
    service: "s3",
    sha256: Hash.bind(null, "sha256"),
    uriEscapePath: false,
  });

  /** Forgets the token `accessKeyId`, `token`, its holder's no more. */
  const forget = (accessKeyId: string, token: HeldToken): void => {
    tokens.delete(accessKeyId);
    if (held.get(token.holder) === accessKeyId) held.delete(token.holder);
  };

  /** Whether `request`, signed as its Authorization says, is signed by the token it names, as the official signer signs: the token's grant, or why not. */
  const verified = async (request: IncomingMessage, path: string, query: Record<string, string>): Promise<{ readonly token: HeldToken } | { readonly refused: string }> => {
    if (Object.keys(query).some((name) => name.toLowerCase().startsWith("x-amz-"))) return { refused: "a request signed in its query is not taken" };
    const said = AUTHORIZATION.exec(String(request.headers.authorization ?? ""));
    if (!said) return { refused: "the request is not signed with Signature Version 4" };
    const [, accessKeyId = "", day = "", region, service, signedList = "", signature = ""] = said;
    const token = tokens.get(accessKeyId);
    if (token && Date.now() >= token.until) forget(accessKeyId, token);
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
    return { token };
  };

  /**
   * Asks the bucket for what `request` asked, as the agent, streaming its answer back: where
   * its body begins in the object given to `answered` (answerStart), and each part of it
   * handed to the client's connection, as it is, to `delivered`.
   */
  const forward = async (
    request: IncomingMessage,
    response: ServerResponse,
    path: string,
    query: Record<string, string>,
    answered: (start: number | null) => void = () => {},
    delivered: (bytes: number) => void = () => {},
  ): Promise<void> => {
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
      const status = answer.statusCode ?? 502;
      response.writeHead(status, kept);
      answered(answerStart(status, typeof answer.headers["content-range"] === "string" ? answer.headers["content-range"] : undefined));
      // As a pipe streams it, held back while the client's connection is, each part counted
      // once its connection has taken it.
      answer.on("data", (chunk: Buffer) => {
        const more = response.write(chunk, (error) => {
          if (!error) delivered(chunk.length);
        });
        if (!more) answer.pause();
      });
      response.on("drain", () => answer.resume());
      answer.once("end", () => {
        response.off("close", abandoned);
        response.end();
      });
      answer.once("error", () => response.destroy());
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
      const { token } = grant;
      if (!key.startsWith(token.prefix)) return refuse(response, 403, "AccessDenied", "the object is not one the restore reads");
      // Under way until its answer ends, however it ends; the bytes of a pages file it gave
      // counted as served then, from where the bucket said they begin, however much of it was
      // given, as its gofer asks again for the rest alone.
      token.under += 1;
      token.lastAt = Date.now();
      let start: number | null = null;
      let given = 0;
      response.once("close", () => {
        token.under -= 1;
        token.lastAt = Date.now();
        if (start !== null && given > 0) served(token, key, start, start + given);
      });
      await forward(request, response, path, query, (from) => {
        start = isPagesFile(key) ? from : null;
      }, (bytes) => {
        given += bytes;
      });
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

  /** `token`'s restore served [start, end) of the pages file `key`, its total kept as it goes. */
  const served = (token: HeldToken, key: string, start: number, end: number): void => {
    const before = token.served.get(key) ?? [];
    const after = withRange(before, start, end);
    token.served.set(key, after);
    token.servedBytes += bytesOf(after) - bytesOf(before);
    servedAllOnce(token);
  };

  /**
   * Logs, once, that `token`'s restore has been served every byte of pages it was expected
   * to: its reads through the proxy are done, as an agent stopped from then on need not wait.
   */
  const servedAllOnce = (token: HeldToken): void => {
    if (token.servedAll || token.expected === null || token.servedBytes < token.expected) return;
    token.servedAll = true;
    runFork(Effect.logInfo(`the restore of ${token.holder} has been served all ${token.expected} bytes of its pages through the node agent`));
  };

  /** How many restores read through the proxy now, as the module says. */
  const reading = (): number => {
    const now = Date.now();
    return [...tokens.values()].filter((token) =>
      now < token.until && (!token.resumed || token.under > 0 || (token.expected === null ? now - token.lastAt < quietMs : token.servedBytes < token.expected))
    ).length;
  };

  return {
    endpoint: `http://${shown}:${address.port}`,
    region: store.region,
    bucket: store.bucket,
    mint: (holder, prefix) => {
      if (draining) return null;
      const before = held.get(holder);
      if (before !== undefined) tokens.delete(before);
      const accessKeyId = `ALASIO${randomBytes(10).toString("hex").toUpperCase()}`;
      const secretAccessKey = randomBytes(32).toString("base64url");
      const now = Date.now();
      tokens.set(accessKeyId, { secretAccessKey, prefix, holder, until: now + preparedForMs, resumed: false, under: 0, lastAt: now, expected: null, served: new Map(), servedBytes: 0, servedAll: false });
      held.set(holder, accessKeyId);
      return { accessKeyId, secretAccessKey };
    },
    revoke: (holder) => {
      const accessKeyId = held.get(holder);
      if (accessKeyId === undefined) return;
      tokens.delete(accessKeyId);
      held.delete(holder);
    },
    resumed: (holder, ms) => {
      const token = tokens.get(held.get(holder) ?? "");
      if (!token) return;
      const now = Date.now();
      token.until = now + ms;
      token.resumed = true;
      token.lastAt = now;
    },
    expect: (accessKeyId, bytes) => {
      const token = tokens.get(accessKeyId);
      if (!token) return;
      token.expected = bytes;
      servedAllOnce(token);
    },
    drained: Effect.sync(() => {
      draining = true;
    }).pipe(
      Effect.andThen(Effect.sync(reading).pipe(Effect.repeat({ schedule: Schedule.spaced(DRAIN_POLL), until: (left) => left === 0 }))),
      Effect.asVoid,
    ),
    reading,
  };
});
