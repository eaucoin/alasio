/**
 * A stand-in for an S3 API, in process, serving one bucket by path-style URLs as
 * SeaweedFS does: PutObject, CopyObject, GetObject (of a range too), HeadObject,
 * DeleteObject, ListObjectsV2, and multipart uploads (CreateMultipartUpload, UploadPart, UploadPartCopy,
 * CompleteMultipartUpload, AbortMultipartUpload), every
 * request's Signature Version 4 checked against the credentials it is given, with every
 * `x-amz-*` header signed, and a signed payload's hash against the payload. What it was
 * asked is recorded in order, and a request can be made to fail.
 */
import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

import { type S3Credentials, sigV4Authorization, UNSIGNED_PAYLOAD } from "../../src/node-agent/s3.ts";

/** A running stand-in. */
export interface FakeS3 {
  readonly endpoint: string;
  /** The bucket's objects, by key. */
  readonly objects: Map<string, Buffer>;
  /**
   * When each object was last written, in milliseconds since the epoch, by key: now as it
   * is written, as the store would, but for one a test sets here.
   */
  readonly writtenAt: Map<string, number>;
  /**
   * What was asked, in order: `<METHOD> <key>`, a copy's as `PUT <key> from <source>`, the
   * bucket's own requests as `<METHOD> ?<query>`; of a multipart upload, `<METHOD> <key>?uploads`
   * as it begins, `... part <n>` for a part, `... complete` and `... abort`; of a range,
   * `GET <key> bytes=<first>-<last>`.
   */
  readonly requests: string[];
  /** The multipart uploads begun and neither completed nor aborted, by their ids. */
  readonly uploads: Map<string, { readonly key: string; readonly parts: Map<number, Buffer> }>;
  /** Answers a request (`asked` as `requests` records it) with the status it gives, as S3 would with an error, at once, rather than doing it; null does it. */
  refuse: (method: string, key: string, asked: string) => number | null;
  /** How long it waits before it answers a get of an object, so gets overlap as they would on a network. */
  getDelayMs: number;
  /** The most gets of objects it has answered at once. */
  readonly mostGetting: () => number;
  readonly close: () => Promise<void>;
}

/** What startFakeS3 is given: its bucket, region and credentials, and how many keys a listing's page holds. */
export interface FakeS3Options extends S3Credentials {
  readonly bucket: string;
  readonly region: string;
  readonly pageSize?: number;
}

const AUTHORIZATION = /^AWS4-HMAC-SHA256 Credential=[^/]+\/\d{8}\/[^/]+\/s3\/aws4_request, SignedHeaders=([a-z0-9;-]+), Signature=[0-9a-f]{64}$/u;

const escapeXml = (text: string): string => text.replace(/[&<>"']/gu, (char) => `&#${char.charCodeAt(0)};`);

function error(response: ServerResponse, status: number, code: string, message: string): void {
  response.writeHead(status, { "content-type": "application/xml" }).end(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>${code}</Code><Message>${escapeXml(message)}</Message></Error>`);
}

async function bodyOf(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
}

/** Starts the stand-in on a port of its own. */
export async function startFakeS3({ bucket, region, accessKeyId, secretAccessKey, pageSize = 2 }: FakeS3Options): Promise<FakeS3> {
  const objects = new Map<string, Buffer>();
  const writtenAt = new Map<string, number>();
  // What each object was answered with as it was written, as S3 does: a multipart one's
  // is not its bytes' MD5.
  const etags = new Map<string, string>();
  const write = (key: string, bytes: Buffer, etag: string): void => {
    objects.set(key, bytes);
    etags.set(key, etag);
    writtenAt.set(key, Date.now());
  };
  const requests: string[] = [];
  const uploads = new Map<string, { readonly key: string; readonly parts: Map<number, Buffer> }>();
  let getting = 0;
  let mostGetting = 0;
  const etagOf = (bytes: Buffer): string => `"${createHash("md5").update(bytes).digest("hex")}"`;

  const handle = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const method = request.method ?? "";
    const url = new URL(request.url ?? "/", "http://s3");
    const body = await bodyOf(request);
    const authorization = request.headers.authorization ?? "";
    const signed = AUTHORIZATION.exec(authorization);
    const payloadHash = String(request.headers["x-amz-content-sha256"] ?? "");
    if (!signed) return error(response, 403, "AccessDenied", "the request is not signed");
    const headers = Object.fromEntries((signed[1] ?? "").split(";").map((name) => [name, String(request.headers[name] ?? "")]));
    const expected = sigV4Authorization(
      { method, path: url.pathname, query: Object.fromEntries(url.searchParams), headers, payloadHash },
      { region, service: "s3" },
      { accessKeyId, secretAccessKey },
    );
    if (authorization !== expected) return error(response, 403, "SignatureDoesNotMatch", "the signature is not the request's");
    const unsigned = Object.keys(request.headers).find((name) => name.startsWith("x-amz-") && !(name in headers));
    if (unsigned !== undefined) return error(response, 403, "AccessDenied", `the header ${unsigned} is not signed`);
    if (payloadHash !== UNSIGNED_PAYLOAD && payloadHash !== createHash("sha256").update(body).digest("hex")) {
      return error(response, 400, "XAmzContentSHA256Mismatch", "the payload is not the one signed");
    }
    if (Number(request.headers["content-length"] ?? 0) !== body.length) return error(response, 400, "IncompleteBody", "the body is not as long as said");
    const [, named, ...path] = url.pathname.split("/");
    if (named !== bucket) return error(response, 404, "NoSuchBucket", `there is no bucket ${named}`);
    const key = path.map(decodeURIComponent).join("/");
    const copySource = request.headers["x-amz-copy-source"];
    const [, sourceBucket, ...sourcePath] = typeof copySource === "string" ? copySource.split("/") : [];
    if (sourceBucket !== undefined && sourceBucket !== bucket) return error(response, 404, "NoSuchBucket", `there is no bucket ${sourceBucket}`);
    const source = sourceBucket === undefined ? null : sourcePath.map(decodeURIComponent).join("/");
    const uploadId = url.searchParams.get("uploadId");
    const partNumber = url.searchParams.get("partNumber");
    const range = request.headers.range ?? request.headers["x-amz-copy-source-range"];
    const multipartOf = url.searchParams.has("uploads") ? "?uploads" : uploadId === null ? "" : partNumber !== null ? ` part ${partNumber}` : method === "POST" ? " complete" : " abort";
    const asked = !key ? `${method} ?${url.searchParams}` : `${method} ${key}${multipartOf}${source === null ? "" : ` from ${source}`}${typeof range === "string" && method === "GET" ? ` ${range}` : ""}`;
    requests.push(asked);
    const refused = fake.refuse(method, key, asked);
    if (refused !== null) return error(response, refused, "InternalError", "refused, as the test asks");
    if (key && method === "GET") {
      getting += 1;
      mostGetting = Math.max(mostGetting, getting);
      response.on("close", () => void (getting -= 1));
      if (fake.getDelayMs > 0) await new Promise((resolve) => setTimeout(resolve, fake.getDelayMs));
    }
    if (key && url.searchParams.has("uploads") && method === "POST") {
      const id = `upload-${uploads.size + requests.length}`;
      uploads.set(id, { key, parts: new Map() });
      response.writeHead(200, { "content-type": "application/xml" }).end(
        `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>${bucket}</Bucket><Key>${escapeXml(key)}</Key><UploadId>${id}</UploadId></InitiateMultipartUploadResult>`,
      );
      return;
    }
    if (key && uploadId !== null) {
      const upload = uploads.get(uploadId);
      if (!upload || upload.key !== key) return error(response, 404, "NoSuchUpload", "The specified multipart upload does not exist.");
      if (method === "PUT" && partNumber !== null) {
        let part = body;
        if (source !== null) {
          const object = objects.get(source);
          if (!object) return error(response, 404, "NoSuchKey", "The specified key does not exist.");
          const [, first = "0", last = String(object.length - 1)] = /^bytes=(\d+)-(\d+)$/u.exec(String(request.headers["x-amz-copy-source-range"] ?? "")) ?? [];
          part = object.subarray(Number(first), Number(last) + 1);
          upload.parts.set(Number(partNumber), Buffer.from(part));
          response.writeHead(200, { "content-type": "application/xml" }).end(`<?xml version="1.0" encoding="UTF-8"?><CopyPartResult><ETag>${escapeXml(etagOf(part))}</ETag></CopyPartResult>`);
          return;
        }
        upload.parts.set(Number(partNumber), part);
        response.writeHead(200, { etag: etagOf(part) }).end();
        return;
      }
      if (method === "POST") {
        const listed = [...body.toString("utf8").matchAll(/<Part><PartNumber>(\d+)<\/PartNumber><ETag>([^<]*)<\/ETag><\/Part>/gu)];
        const parts = listed.map(([, number = "", etag = ""]) => ({ number: Number(number), etag: etag.replaceAll("&#34;", '"') }));
        const whole = parts.every(({ number, etag }, index) => number === index + 1 && upload.parts.has(number) && etagOf(upload.parts.get(number) ?? Buffer.alloc(0)) === etag);
        if (!whole || parts.length !== upload.parts.size) return error(response, 400, "InvalidPart", "One or more of the specified parts could not be found.");
        const etag = `"${createHash("md5").update(Buffer.concat(parts.map(({ etag }) => Buffer.from(etag.slice(1, -1), "hex")))).digest("hex")}-${parts.length}"`;
        write(key, Buffer.concat(parts.map(({ number }) => upload.parts.get(number) ?? Buffer.alloc(0))), etag);
        uploads.delete(uploadId);
        response.writeHead(200, { "content-type": "application/xml" }).end(
          `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Key>${escapeXml(key)}</Key><ETag>${escapeXml(etag)}</ETag></CompleteMultipartUploadResult>`,
        );
        return;
      }
      if (method === "DELETE") {
        uploads.delete(uploadId);
        response.writeHead(204).end();
        return;
      }
    }
    if (!key && method === "GET" && url.searchParams.get("list-type") === "2") {
      const prefix = url.searchParams.get("prefix") ?? "";
      const keys = [...objects.keys()].filter((name) => name.startsWith(prefix)).sort();
      const start = Number(url.searchParams.get("continuation-token") ?? 0);
      const page = keys.slice(start, start + pageSize);
      const truncated = start + pageSize < keys.length;
      response.writeHead(200, { "content-type": "application/xml" }).end([
        `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>${bucket}</Name><Prefix>${escapeXml(prefix)}</Prefix>`,
        ...page.map((name) =>
          `<Contents><Key>${escapeXml(name)}</Key><LastModified>${new Date(writtenAt.get(name) ?? 0).toISOString()}</LastModified><Size>${objects.get(name)?.length}</Size></Contents>`
        ),
        `<IsTruncated>${truncated}</IsTruncated>${truncated ? `<NextContinuationToken>${start + pageSize}</NextContinuationToken>` : ""}</ListBucketResult>`,
      ].join(""));
      return;
    }
    if (method === "PUT" && source !== null) {
      const object = objects.get(source);
      if (!object) return error(response, 404, "NoSuchKey", "The specified key does not exist.");
      write(key, Buffer.from(object), etagOf(object));
      response.writeHead(200, { "content-type": "application/xml" }).end(
        `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>${escapeXml(etagOf(object))}</ETag></CopyObjectResult>`,
      );
      return;
    }
    if (method === "PUT") {
      write(key, body, etagOf(body));
      response.writeHead(200, { etag: etagOf(body) }).end();
      return;
    }
    if (method === "HEAD") {
      const object = objects.get(key);
      response.writeHead(object ? 200 : 404, object ? { "content-length": object.length, etag: etags.get(key) ?? "" } : {}).end();
      return;
    }
    if (method === "GET") {
      const object = objects.get(key);
      if (!object) return error(response, 404, "NoSuchKey", "The specified key does not exist.");
      const asRange = /^bytes=(\d+)-(\d+)$/u.exec(String(request.headers.range ?? ""));
      if (asRange) {
        const [first, last] = [Number(asRange[1]), Math.min(Number(asRange[2]), object.length - 1)];
        if (first >= object.length) return error(response, 416, "InvalidRange", "The requested range is not satisfiable");
        const part = object.subarray(first, last + 1);
        response.writeHead(206, { "content-length": part.length, "content-range": `bytes ${first}-${last}/${object.length}` }).end(part);
        return;
      }
      response.writeHead(200, { "content-length": object.length }).end(object);
      return;
    }
    if (method === "DELETE") {
      objects.delete(key);
      etags.delete(key);
      writtenAt.delete(key);
      response.writeHead(204).end();
      return;
    }
    error(response, 405, "MethodNotAllowed", `${method} is not served`);
  };

  const server = createServer((request, response) => {
    handle(request, response).catch((cause: unknown) => error(response, 500, "InternalError", String(cause)));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const fake: FakeS3 = {
    endpoint: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    objects,
    writtenAt,
    requests,
    uploads,
    refuse: () => null,
    getDelayMs: 0,
    mostGetting: () => mostGetting,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
  return fake;
}
