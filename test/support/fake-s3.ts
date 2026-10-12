/**
 * A stand-in for an S3 API, in process, serving one bucket by path-style URLs as
 * SeaweedFS does: PutObject, GetObject, DeleteObject and ListObjectsV2, every request's
 * Signature Version 4 checked against the credentials it is given, and a signed
 * payload's hash against the payload. What it was asked is recorded in order, and a
 * request can be made to fail.
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
  /** What was asked, in order: `<METHOD> <key>`, the bucket's own requests as `<METHOD> ?<query>`. */
  readonly requests: string[];
  /** Answers a request with the status it gives, as S3 would with an error, rather than doing it; null does it. */
  refuse: (method: string, key: string) => number | null;
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
  const requests: string[] = [];

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
    if (payloadHash !== UNSIGNED_PAYLOAD && payloadHash !== createHash("sha256").update(body).digest("hex")) {
      return error(response, 400, "XAmzContentSHA256Mismatch", "the payload is not the one signed");
    }
    if (Number(request.headers["content-length"] ?? 0) !== body.length) return error(response, 400, "IncompleteBody", "the body is not as long as said");
    const [, named, ...path] = url.pathname.split("/");
    if (named !== bucket) return error(response, 404, "NoSuchBucket", `there is no bucket ${named}`);
    const key = path.map(decodeURIComponent).join("/");
    requests.push(key ? `${method} ${key}` : `${method} ?${url.searchParams}`);
    const refused = fake.refuse(method, key);
    if (refused !== null) return error(response, refused, "InternalError", "refused, as the test asks");
    if (!key && method === "GET" && url.searchParams.get("list-type") === "2") {
      const prefix = url.searchParams.get("prefix") ?? "";
      const keys = [...objects.keys()].filter((name) => name.startsWith(prefix)).sort();
      const start = Number(url.searchParams.get("continuation-token") ?? 0);
      const page = keys.slice(start, start + pageSize);
      const truncated = start + pageSize < keys.length;
      response.writeHead(200, { "content-type": "application/xml" }).end([
        `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>${bucket}</Name><Prefix>${escapeXml(prefix)}</Prefix>`,
        ...page.map((name) => `<Contents><Key>${escapeXml(name)}</Key><Size>${objects.get(name)?.length}</Size></Contents>`),
        `<IsTruncated>${truncated}</IsTruncated>${truncated ? `<NextContinuationToken>${start + pageSize}</NextContinuationToken>` : ""}</ListBucketResult>`,
      ].join(""));
      return;
    }
    if (method === "PUT") {
      objects.set(key, body);
      response.writeHead(200).end();
      return;
    }
    if (method === "GET") {
      const object = objects.get(key);
      if (!object) return error(response, 404, "NoSuchKey", "The specified key does not exist.");
      response.writeHead(200, { "content-length": object.length }).end(object);
      return;
    }
    if (method === "DELETE") {
      objects.delete(key);
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
    requests,
    refuse: () => null,
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
  return fake;
}
