/**
 * The node agent's bucket: one bucket of an S3 API, reached by path-style URLs
 * (`<endpoint>/<bucket>/<key>`, as SeaweedFS serves them), every request signed with AWS
 * Signature Version 4 (sigV4Authorization). No SDK: the agent puts objects from files and
 * gets them into files, puts and reads small JSON objects, copies objects within the
 * bucket, says an object's size and ETag, deletes objects, and lists those under a
 * prefix, and nothing else.
 *
 * A file larger than a part (`partBytes`) is put as a multipart upload, its parts sent
 * `parallel` at a time, and got in ranges of a part, as many at a time: one stream of
 * either way is slower than the store, and S3 puts no object larger than 5 GiB in one
 * request. An upload that fails is aborted; what one left that was not, as the agent
 * stopped, the bucket's lifecycle aborts after a day (cli/src/manifests/object-store.ts).
 * An object larger than 5 GiB, which S3 copies in no one request either, is copied in
 * parts too.
 *
 * What the bucket gets is written to its file within a budget of the agent's memory, which
 * counts what is written and not yet on the disk, and cannot reclaim it before it is: at
 * most `parallel` gets at a time, every get of the bucket's together, each synced to the
 * disk whenever it has `syncBytes` written since (UNSYNCED_DOWNLOAD_BYTES, ./names.ts).
 */
import { createHash, createHmac } from "node:crypto";
import { createReadStream } from "node:fs";
import { type FileHandle, open, stat } from "node:fs/promises";
import { type ClientRequest, type IncomingMessage, request as httpRequest, type RequestOptions } from "node:http";
import { request as httpsRequest } from "node:https";
import { pipeline } from "node:stream/promises";

import { Effect, Schema } from "effect";

import { SNAPSHOT_PART_BYTES, SNAPSHOT_PARTS_AT_ONCE, SNAPSHOT_SYNC_BYTES } from "./names.ts";

/** The keys a request is signed with. */
export interface S3Credentials {
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
}

/**
 * Where the bucket is, and as whom it is reached; and, unless told, how large a part is,
 * how many go at a time, how much a get writes between syncs, and the most copied whole.
 */
export interface S3Options extends S3Credentials {
  /** The S3 API's URL, `http(s)://host[:port]`. */
  readonly endpoint: string;
  readonly region: string;
  readonly bucket: string;
  readonly partBytes?: number;
  readonly parallel?: number;
  readonly syncBytes?: number;
  readonly copyBytes?: number;
}

/** A part, unless told (./names.ts). */
const PART_BYTES = SNAPSHOT_PART_BYTES;
/** How many parts go at a time, and gets, unless told (./names.ts). */
const PARALLEL = SNAPSHOT_PARTS_AT_ONCE;
/** What a get writes between syncs, unless told (./names.ts). */
const SYNC_BYTES = SNAPSHOT_SYNC_BYTES;
/** The most S3 copies in one request: 5 GiB. */
const COPY_BYTES = 5 * 1024 ** 3;

/** A request as Signature Version 4 signs it. */
export interface SigningRequest {
  readonly method: string;
  /** Its path, encoded as it is sent. */
  readonly path: string;
  readonly query: Readonly<Record<string, string>>;
  /** Every header it is signed with, by lower-case name: `host` and `x-amz-date` among them. */
  readonly headers: Readonly<Record<string, string>>;
  /** Its payload's SHA-256, in hex, or UNSIGNED_PAYLOAD. */
  readonly payloadHash: string;
}

/** The scope a signature is made for: the region and service it is for. */
export interface SigningScope {
  readonly region: string;
  readonly service: string;
}

/** A request to the bucket failed: which, and why. */
export class S3Error extends Schema.TaggedError<S3Error>()("S3Error", {
  operation: Schema.String,
  key: Schema.String,
  reason: Schema.String,
}) {
  override get message(): string {
    return `the bucket's ${this.operation} of ${this.key} failed: ${this.reason}`;
  }
}

/**
 * An object as the bucket holds it: its size, and its ETag, which the request that wrote it
 * was answered with, and which names what it wrote: a later write answers another.
 */
export interface StoredObject {
  readonly bytes: number;
  readonly etag: string;
}

/** An object under a prefix: its key, and when it was last written, in milliseconds since the epoch. */
export interface ListedObject {
  readonly key: string;
  readonly writtenAt: number;
}

/** What getFile wrote: how many bytes, and their SHA-256 in hex. */
export interface Received {
  readonly bytes: number;
  readonly sha256: string;
}

/** The bucket, as makeS3Bucket reaches it. */
export interface S3Bucket {
  /**
   * Puts the file at `path` as the object `key`, streamed: the ETag the bucket answered. Each part of it, or the whole
   * of one put in one, is sent once `ready` has resolved, where given, as work that yields to another waits.
   */
  readonly putFile: (key: string, path: string, ready?: () => Promise<void>) => Effect.Effect<string, S3Error>;
  /** Writes the object `key`, of `bytes`, to the file at `path`, streamed: its size and SHA-256. */
  readonly getFile: (key: string, path: string, bytes: number) => Effect.Effect<Received, S3Error>;
  /** Puts `value` as the object `key`, as JSON. */
  readonly putJson: (key: string, value: unknown) => Effect.Effect<void, S3Error>;
  /** The object `key`, parsed as JSON, or null when there is none. */
  readonly getJson: (key: string) => Effect.Effect<unknown, S3Error>;
  /** Copies the object `from`, of `bytes`, as the object `to`, in the bucket, none of its bytes through this client. */
  readonly copy: (from: string, to: string, bytes: number) => Effect.Effect<void, S3Error>;
  /** The object `key`'s size and ETag, or null when there is none. */
  readonly head: (key: string) => Effect.Effect<StoredObject | null, S3Error>;
  /** Deletes the object `key`; one there is not is gone already. */
  readonly remove: (key: string) => Effect.Effect<void, S3Error>;
  /** Every object under `prefix`, in the order of their keys. */
  readonly list: (prefix: string) => Effect.Effect<readonly ListedObject[], S3Error>;
}

/** The payload hash of a request whose payload is not signed, as a streamed upload's is. */
export const UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

/** How long a request may wait for its next bytes, either way. */
const IDLE_TIMEOUT_MS = 60_000;
/** The most of a failed response's body read for its reason. */
const ERROR_BODY_BYTES = 64 * 1024;
/** The most a JSON object of the bucket's may hold: a manifest, which names a snapshot's few files, is a few KiB. */
const JSON_BYTES = 1024 * 1024;

const sha256Hex = (data: string | Buffer): string => createHash("sha256").update(data).digest("hex");
const hmac = (key: string | Buffer, data: string): Buffer => createHmac("sha256", key).update(data).digest();

/** `value` encoded as Signature Version 4 encodes a URI's parts: every byte but RFC 3986's unreserved ones. */
function uriEncode(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/gu, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

/** A query, as its canonical form and as it is sent: its parameters encoded and in order. */
function canonicalQuery(query: Readonly<Record<string, string>>): string {
  return Object.entries(query)
    .map(([name, value]) => [uriEncode(name), uriEncode(value)] as const)
    .sort(([a, x], [b, y]) => (a < b ? -1 : a > b ? 1 : x < y ? -1 : x > y ? 1 : 0))
    .map(([name, value]) => `${name}=${value}`)
    .join("&");
}

/**
 * The `Authorization` header of `request`, signed with AWS Signature Version 4 for
 * `scope` with `credentials`, at the time its `x-amz-date` header says.
 */
export function sigV4Authorization(request: SigningRequest, scope: SigningScope, credentials: S3Credentials): string {
  const names = Object.keys(request.headers).sort();
  const signedHeaders = names.join(";");
  const canonicalRequest = [
    request.method,
    request.path,
    canonicalQuery(request.query),
    ...names.map((name) => `${name}:${(request.headers[name] ?? "").trim().replace(/\s+/gu, " ")}`),
    "",
    signedHeaders,
    request.payloadHash,
  ].join("\n");
  const time = request.headers["x-amz-date"] ?? "";
  const day = time.slice(0, 8);
  const credentialScope = `${day}/${scope.region}/${scope.service}/aws4_request`;
  const stringToSign = ["AWS4-HMAC-SHA256", time, credentialScope, sha256Hex(canonicalRequest)].join("\n");
  const signingKey = hmac(hmac(hmac(hmac(`AWS4${credentials.secretAccessKey}`, day), scope.region), scope.service), "aws4_request");
  const signature = createHmac("sha256", signingKey).update(stringToSign).digest("hex");
  return `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
}

/** A time as Signature Version 4 writes it: `YYYYMMDDTHHMMSSZ`. */
function amzDate(at: Date): string {
  return at.toISOString().replace(/[-:]|\.\d{3}/gu, "");
}

/** The characters XML escapes, by the entity of each. */
const XML_ENTITIES: Readonly<Record<string, string>> = { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" };

/** `text` as XML's text. */
const escapeXml = (text: string): string => text.replace(/[&<>"']/gu, (char) => `&#${char.charCodeAt(0)};`);

/** XML's text as the text it is. */
function xmlText(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/giu, (entity, name: string) =>
    name.startsWith("#x") || name.startsWith("#X")
      ? String.fromCodePoint(Number.parseInt(name.slice(2), 16))
      : name.startsWith("#")
      ? String.fromCodePoint(Number(name.slice(1)))
      : XML_ENTITIES[name] ?? entity);
}

/** The text of every element `name` of `xml`, which holds no other element. */
function elements(xml: string, name: string): string[] {
  return [...xml.matchAll(new RegExp(`<${name}>([^<]*)</${name}>`, "gu"))].map((match) => xmlText(match[1] ?? ""));
}

/** The XML within every element `name` of `xml`, which holds no element of its name. */
function elementsXml(xml: string, name: string): string[] {
  return [...xml.matchAll(new RegExp(`<${name}>(.*?)</${name}>`, "gsu"))].map((match) => match[1] ?? "");
}

/** A response's body, read whole up to `limit` bytes. */
async function bodyText(response: IncomingMessage, limit: number): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of response) {
    const bytes = chunk as Buffer;
    if (size < limit) chunks.push(bytes.subarray(0, limit - size));
    size += bytes.length;
  }
  return Buffer.concat(chunks).toString("utf8");
}

/** A response's body, which must hold at most `limit` bytes, read whole. */
async function boundedText(response: IncomingMessage, limit: number): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of response) {
    size += (chunk as Buffer).length;
    if (size > limit) {
      response.destroy();
      throw new Error(`it holds more than ${limit} bytes`);
    }
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

/** Why a response is a failure: its status, and S3's code and message for it, if it gave them. */
async function failureOf(response: IncomingMessage): Promise<string> {
  const body = await bodyText(response, ERROR_BODY_BYTES);
  const [code] = elements(body, "Code");
  const [message] = elements(body, "Message");
  return [`${response.statusCode}`, code, message].filter(Boolean).join(" ");
}

/** What a request sends: its query, its headers, signed or not, and its body. */
interface Sending {
  readonly query?: Readonly<Record<string, string>>;
  readonly headers?: Readonly<Record<string, string>>;
  /** Headers it is signed with beyond `host` and its date and payload hash, by lower-case name: every `x-amz-*` one, as S3 requires. */
  readonly signedHeaders?: Readonly<Record<string, string>>;
  /** A body in memory, signed; or a file, or `size` bytes of it from `start`, streamed and not signed. */
  readonly body?: { readonly bytes: Buffer } | { readonly file: string; readonly size: number; readonly start?: number };
}

/** Runs each work given it once fewer than `count` run: at most `count` at a time, in the order given. */
function limiter(count: number): <A>(work: () => Promise<A>) => Promise<A> {
  let free = count;
  const waiting: (() => void)[] = [];
  return async (work) => {
    if (free > 0) free -= 1;
    else await new Promise<void>((resolve) => waiting.push(resolve));
    try {
      return await work();
    } finally {
      const next = waiting.shift();
      if (next) next();
      else free += 1;
    }
  };
}

/**
 * Runs `work` for each of `count` items, by its index, `parallel` at a time, in order. The
 * first that fails stops the rest: none starts after it, and each running is aborted
 * through the signal it is given, as `signal` aborts them all; so what they hold, the
 * budget of gets among it, is let go of. Fails with that first failure, once each has ended.
 */
async function eachInParallel(count: number, parallel: number, signal: AbortSignal, work: (index: number, signal: AbortSignal) => Promise<void>): Promise<void> {
  const controller = new AbortController();
  const aborted = () => controller.abort(signal.reason);
  signal.addEventListener("abort", aborted, { once: true });
  let next = 0;
  const failed: { first: { readonly cause: unknown } | null } = { first: null };
  try {
    await Promise.all(Array.from({ length: Math.min(parallel, count) }, async () => {
      while (next < count && failed.first === null) {
        const index = next++;
        try {
          await work(index, controller.signal);
        } catch (cause) {
          if (failed.first === null) {
            failed.first = { cause };
            controller.abort(cause);
          }
        }
      }
    }));
  } finally {
    signal.removeEventListener("abort", aborted);
  }
  if (failed.first !== null) throw failed.first.cause;
}

/**
 * Writes what `source` gives into `file` from `start`, `length` bytes and no more, syncing
 * the file to the disk before it would have more than `syncBytes` written since it last did,
 * and once it is all written; each chunk is given to `seen` first. The bytes it wrote: a
 * source that gives more than `length` fails before any of the rest is written.
 */
async function writeSynced(file: FileHandle, source: AsyncIterable<Buffer>, start: number, length: number, syncBytes: number, seen: (chunk: Buffer) => void = () => {}): Promise<number> {
  let position = start;
  let unsynced = 0;
  for await (const chunk of source) {
    if (position - start + chunk.length > length) throw new Error(`it holds more than the ${length} bytes it is said to`);
    seen(chunk);
    for (let offset = 0; offset < chunk.length;) {
      if (unsynced === syncBytes) {
        await file.datasync();
        unsynced = 0;
      }
      const { bytesWritten } = await file.write(chunk, offset, Math.min(syncBytes - unsynced, chunk.length - offset), position);
      offset += bytesWritten;
      position += bytesWritten;
      unsynced += bytesWritten;
    }
  }
  await file.datasync();
  return position - start;
}

/** The bucket of `options`, reached over HTTP(S) as the module says. */
export function makeS3Bucket(options: S3Options): S3Bucket {
  const { partBytes = PART_BYTES, parallel = PARALLEL, syncBytes = SYNC_BYTES, copyBytes = COPY_BYTES } = options;
  // Every get of the bucket's, so what they hold not yet on the disk, together, is within the budget.
  const getting = limiter(parallel);
  const endpoint = new URL(options.endpoint);
  const send = endpoint.protocol === "https:" ? httpsRequest : httpRequest;
  const base = endpoint.pathname.replace(/\/+$/u, "");

  /** The object `key` as `/<bucket>/<key>`, encoded, or the bucket for null: a request's path, after the endpoint's, and a copy's source. */
  const inBucket = (key: string | null): string => `/${uriEncode(options.bucket)}${key === null ? "" : `/${key.split("/").map(uriEncode).join("/")}`}`;

  /** Sends a signed request, and answers its response once it begins. */
  const call = async (method: string, key: string | null, { query = {}, headers = {}, signedHeaders = {}, body }: Sending, signal: AbortSignal): Promise<IncomingMessage> => {
    const payloadHash = body === undefined ? sha256Hex("") : "bytes" in body ? sha256Hex(body.bytes) : UNSIGNED_PAYLOAD;
    const signed = { ...signedHeaders, host: endpoint.host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate(new Date()) };
    const path = `${base}${inBucket(key)}`;
    const authorization = sigV4Authorization({ method, path, query, headers: signed, payloadHash }, { region: options.region, service: "s3" }, options);
    const search = canonicalQuery(query);
    const length = body === undefined ? 0 : "bytes" in body ? body.bytes.length : body.size;
    const requestOptions: RequestOptions = {
      method,
      host: endpoint.hostname,
      port: endpoint.port,
      path: search ? `${path}?${search}` : path,
      headers: { ...headers, ...signed, authorization, "content-length": String(length) },
      signal,
    };
    return new Promise<IncomingMessage>((resolve, reject) => {
      const request: ClientRequest = send(requestOptions, resolve);
      request.setTimeout(IDLE_TIMEOUT_MS, () => request.destroy(new Error(`no answer within ${IDLE_TIMEOUT_MS / 1000}s`)));
      request.once("error", reject);
      if (body === undefined) request.end();
      else if ("bytes" in body) request.end(body.bytes);
      else pipeline(createReadStream(body.file, body.size === 0 ? {} : { start: body.start ?? 0, end: (body.start ?? 0) + body.size - 1 }), request).catch(reject);
    });
  };

  /** `run`'s answer, or why it failed, as the bucket's `operation` of `key`. */
  const attempt = <A>(operation: string, key: string, run: (signal: AbortSignal) => Promise<A>): Effect.Effect<A, S3Error> =>
    Effect.tryPromise({
      try: run,
      catch: (cause) => new S3Error({ operation, key, reason: cause instanceof Error ? cause.message : String(cause) }),
    });

  /** The response, read to its end, when it is a success; else why it is not. */
  const succeeded = async (response: IncomingMessage, also: readonly number[] = []): Promise<IncomingMessage> => {
    const status = response.statusCode ?? 0;
    if ((status >= 200 && status < 300) || also.includes(status)) return response;
    throw new Error(await failureOf(response));
  };

  /**
   * Puts the object `key` in `count` parts, each sent by `part(number, query, signal)`
   * (its number from 1, the query that names it, and what aborts it) and answering its
   * ETag, `parallel` at a time; aborted if any fails, the parts still being sent with it.
   * The ETag of the object the parts make.
   */
  const multipart = async (
    key: string,
    count: number,
    part: (number: number, query: Readonly<Record<string, string>>, signal: AbortSignal) => Promise<string>,
    signal: AbortSignal,
  ): Promise<string> => {
    const created = await bodyText(await succeeded(await call("POST", key, { query: { uploads: "" } }, signal)), Number.POSITIVE_INFINITY);
    const [uploadId] = elements(created, "UploadId");
    if (!uploadId) throw new Error("it began no multipart upload");
    try {
      const etags = new Array<string>(count);
      await eachInParallel(count, parallel, signal, async (index, partSignal) => {
        etags[index] = await part(index + 1, { partNumber: String(index + 1), uploadId }, partSignal);
      });
      const parts = etags.map((etag, index) => `<Part><PartNumber>${index + 1}</PartNumber><ETag>${escapeXml(etag)}</ETag></Part>`).join("");
      const bytes = Buffer.from(`<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>${parts}</CompleteMultipartUpload>`);
      // S3 may answer 200 with an error, the parts not put together.
      const completed = await bodyText(await succeeded(await call("POST", key, { query: { uploadId }, headers: { "content-type": "application/xml" }, body: { bytes } }, signal)), ERROR_BODY_BYTES);
      if (elements(completed, "Code").length > 0) throw new Error(`its parts were not put together: ${elements(completed, "Code").join(" ")} ${elements(completed, "Message").join(" ")}`);
      const [etag] = elements(completed, "ETag");
      if (!etag) throw new Error("its parts were put together with no ETag");
      return etag;
    } catch (cause) {
      await call("DELETE", key, { query: { uploadId } }, AbortSignal.timeout(IDLE_TIMEOUT_MS)).then((response) => bodyText(response, 0), () => undefined);
      throw cause;
    }
  };

  /** The ranges of an object of `size` bytes, a part each: their first byte and their length. */
  const ranges = (size: number): { readonly start: number; readonly length: number }[] =>
    Array.from({ length: Math.ceil(size / partBytes) }, (_, index) => ({ start: index * partBytes, length: Math.min(partBytes, size - index * partBytes) }));

  return {
    putFile: (key, path, ready) =>
      attempt("upload", key, async (signal) => {
        const { size } = await stat(path);
        if (size <= partBytes) {
          await ready?.();
          const response = await succeeded(await call("PUT", key, { body: { file: path, size } }, signal));
          await bodyText(response, 0);
          const etag = response.headers.etag;
          if (!etag) throw new Error("it was answered with no ETag");
          return etag;
        }
        const parts = ranges(size);
        return multipart(key, parts.length, async (number, query, partSignal) => {
          const { start, length } = parts[number - 1] ?? { start: 0, length: 0 };
          await ready?.();
          const response = await succeeded(await call("PUT", key, { query, body: { file: path, size: length, start } }, partSignal));
          await bodyText(response, 0);
          const etag = response.headers.etag;
          if (!etag) throw new Error(`its part ${number} was answered with no ETag`);
          return etag;
        }, signal);
      }),

    getFile: (key, path, size) =>
      attempt("download", key, async (signal) => {
        const file = await open(path, "w");
        try {
          if (size <= partBytes) {
            const hash = createHash("sha256");
            const bytes = await getting(async () => writeSynced(file, await succeeded(await call("GET", key, {}, signal)), 0, size, syncBytes, (chunk) => hash.update(chunk)));
            return { bytes, sha256: hash.digest("hex") };
          }
          // In ranges, each written where it goes, then read again for its hash.
          await file.truncate(size);
          const parts = ranges(size);
          await eachInParallel(parts.length, parallel, signal, async (index, rangeSignal) => {
            const { start, length } = parts[index] ?? { start: 0, length: 0 };
            await getting(async () => {
              const response = await succeeded(await call("GET", key, { headers: { range: `bytes=${start}-${start + length - 1}` } }, rangeSignal));
              if (response.statusCode !== 206 || Number(response.headers["content-length"]) !== length) {
                await bodyText(response, 0);
                throw new Error(`its range from byte ${start} was answered ${response.statusCode} with ${response.headers["content-length"]} bytes, not ${length}`);
              }
              await writeSynced(file, response, start, length, syncBytes);
            });
          });
        } finally {
          await file.close();
        }
        const hash = createHash("sha256");
        await pipeline(createReadStream(path), hash);
        return { bytes: (await stat(path)).size, sha256: hash.digest("hex") };
      }),

    putJson: (key, value) =>
      attempt("upload", key, async (signal) => {
        const bytes = Buffer.from(JSON.stringify(value));
        await bodyText(await succeeded(await call("PUT", key, { headers: { "content-type": "application/json" }, body: { bytes } }, signal)), 0);
      }),

    getJson: (key) =>
      attempt("read", key, async (signal) => {
        const response = await succeeded(await call("GET", key, {}, signal), [404]);
        if (response.statusCode === 404) {
          await bodyText(response, 0);
          return null;
        }
        const text = await boundedText(response, JSON_BYTES);
        try {
          return JSON.parse(text) as unknown;
        } catch {
          throw new Error("it is not JSON");
        }
      }),

    copy: (from, to, size) =>
      attempt("copy", `${from} to ${to}`, async (signal) => {
        const source = { "x-amz-copy-source": inBucket(from) };
        if (size <= copyBytes) return void (await bodyText(await succeeded(await call("PUT", to, { signedHeaders: source }, signal)), 0));
        // In parts of the most S3 copies at once.
        const parts = Array.from({ length: Math.ceil(size / copyBytes) }, (_, index) => ({ start: index * copyBytes, length: Math.min(copyBytes, size - index * copyBytes) }));
        await multipart(to, parts.length, async (number, query, partSignal) => {
          const { start, length } = parts[number - 1] ?? { start: 0, length: 0 };
          const range = { ...source, "x-amz-copy-source-range": `bytes=${start}-${start + length - 1}` };
          const answered = await bodyText(await succeeded(await call("PUT", to, { query, signedHeaders: range }, partSignal)), ERROR_BODY_BYTES);
          const [etag] = elements(answered, "ETag");
          if (!etag) throw new Error(`its part ${number} was answered with no ETag`);
          return etag;
        }, signal);
      }),

    head: (key) =>
      attempt("look", key, async (signal) => {
        const response = await succeeded(await call("HEAD", key, {}, signal), [404]);
        await bodyText(response, 0);
        if (response.statusCode === 404) return null;
        const etag = response.headers.etag;
        if (!etag) throw new Error("it was answered with no ETag");
        return { bytes: Number(response.headers["content-length"]), etag };
      }),

    remove: (key) =>
      attempt("deletion", key, async (signal) => {
        await bodyText(await succeeded(await call("DELETE", key, {}, signal), [404]), 0);
      }),

    list: (prefix) =>
      attempt("listing", `${prefix}*`, async (signal) => {
        const listed: ListedObject[] = [];
        let token: string | undefined;
        do {
          const query = { "list-type": "2", prefix, ...(token === undefined ? {} : { "continuation-token": token }) };
          const page = await bodyText(await succeeded(await call("GET", null, { query }, signal)), Number.POSITIVE_INFINITY);
          for (const contents of elementsXml(page, "Contents")) {
            const [key] = elements(contents, "Key");
            const writtenAt = Date.parse(elements(contents, "LastModified")[0] ?? "");
            if (key === undefined || Number.isNaN(writtenAt)) throw new Error(`it listed an object with no key or no time it was written: ${contents}`);
            listed.push({ key, writtenAt });
          }
          token = elements(page, "IsTruncated")[0] === "true" ? elements(page, "NextContinuationToken")[0] : undefined;
        } while (token !== undefined);
        return listed;
      }),
  };
}
