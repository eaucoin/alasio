/**
 * Spike: the store swept, as the system's client reaches it through its profile (./s3.ts's
 * environment), for the store experiments.
 *
 *   node sweep.ts sweep [--prefix bench-sweep/] [--object-mib 1024] [--connections 1,4,8,16,32,64] [--ranges-mib 4,16,64]
 *     an object of --object-mib put once, then, for each range size and connection count, the
 *     whole of it read in ranged GETs of that size, that many at once, and as many PUTs of that
 *     size put as parts of a multipart upload, that many at once: each cell's MB/s; and the
 *     first bytes of 200 GETs of 4 KiB, one at a time, their distribution.
 *   node sweep.ts conditional [--prefix bench-conditional/] [--races 20] [--racers 16] [--times 100]
 *     what the store answers to conditional PUTs (If-Match, If-None-Match), whether of racers
 *     each putting a key If-Match the ETag they all read one alone wins, and how long a small
 *     JSON PUT takes with and without a condition.
 *
 * Each prints one JSON object.
 */
import { randomBytes } from "node:crypto";
import { Agent } from "node:http";
import { parseArgs } from "node:util";

import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  UploadPartCommand,
} from "@aws-sdk/client-s3";
import { NodeHttpHandler } from "@smithy/node-http-handler";

import { nearestRank as rank } from "../aggregate/aggregate.ts";
import { clientConfig, storeFromEnvironment } from "./s3.ts";

const MIB = 1024 ** 2;
const seconds = (from: bigint) => Number(process.hrtime.bigint() - from) / 1e9;
const ms = (from: bigint) => seconds(from) * 1000;

/** A client of `connections` sockets at most. */
const clientOf = (connections: number): S3Client =>
  new S3Client({ ...clientConfig(), requestHandler: new NodeHttpHandler({ httpAgent: new Agent({ keepAlive: true, maxSockets: connections }) }) });

/** Runs `count` tasks, `limit` at a time. */
async function inParallel(count: number, limit: number, task: (index: number) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, count) }, async () => {
    while (next < count) await task(next++);
  }));
}

export async function sweep(prefix: string, objectMib: number, connections: readonly number[], rangesMib: readonly number[]) {
  const { bucket } = storeFromEnvironment();
  const object = `${prefix}object`;
  const size = objectMib * MIB;
  const body = randomBytes(size);
  const loader = clientOf(16);
  await multipart(loader, bucket, object, body, 32 * MIB, 8);

  const firstBytes: number[] = [];
  for (let each = 0; each < 200; each++) {
    const started = process.hrtime.bigint();
    const { Body } = await loader.send(new GetObjectCommand({ Bucket: bucket, Key: object, Range: `bytes=${each * 4096}-${each * 4096 + 4095}` }));
    // The answer's head comes with its first bytes, as toxiproxy delays them together.
    firstBytes.push(ms(started));
    await Body?.transformToByteArray();
  }
  firstBytes.sort((a, b) => a - b);

  const cells: Record<string, unknown>[] = [];
  for (const rangeMib of rangesMib) {
    for (const atOnce of connections) {
      const client = clientOf(atOnce);
      const range = rangeMib * MIB;
      const parts = Math.ceil(size / range);
      let started = process.hrtime.bigint();
      await inParallel(parts, atOnce, async (index) => {
        const start = index * range;
        const end = Math.min(size, start + range) - 1;
        const got = await (await client.send(new GetObjectCommand({ Bucket: bucket, Key: object, Range: `bytes=${start}-${end}` }))).Body?.transformToByteArray();
        if (got?.length !== end - start + 1) throw new Error(`${object} ${start}-${end} gave ${got?.length} bytes`);
      });
      const getMbS = size / 1e6 / seconds(started);
      started = process.hrtime.bigint();
      await multipart(client, bucket, `${prefix}put-${rangeMib}-${atOnce}`, body, Math.max(5 * MIB, range), atOnce);
      const putMbS = size / 1e6 / seconds(started);
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: `${prefix}put-${rangeMib}-${atOnce}` }));
      cells.push({ range_mib: rangeMib, connections: atOnce, get_mb_s: getMbS, put_mb_s: putMbS });
      client.destroy();
    }
  }
  await loader.send(new DeleteObjectCommand({ Bucket: bucket, Key: object }));
  return {
    object_mib: objectMib,
    first_byte_ms: { n: firstBytes.length, p50: rank(firstBytes, 0.5), p95: rank(firstBytes, 0.95), p99: rank(firstBytes, 0.99), min: firstBytes[0], max: firstBytes.at(-1) },
    cells,
  };
}

/** Puts `body` as `key` in a multipart upload of `partBytes` parts, `atOnce` at a time. */
async function multipart(client: S3Client, bucket: string, key: string, body: Buffer, partBytes: number, atOnce: number): Promise<void> {
  const { UploadId } = await client.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }));
  const count = Math.ceil(body.length / partBytes);
  const etags: string[] = [];
  try {
    await inParallel(count, atOnce, async (index) => {
      const { ETag } = await client.send(new UploadPartCommand({ Bucket: bucket, Key: key, UploadId, PartNumber: index + 1, Body: body.subarray(index * partBytes, Math.min(body.length, (index + 1) * partBytes)) }));
      etags[index] = ETag ?? "";
    });
    await client.send(new CompleteMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId, MultipartUpload: { Parts: etags.map((ETag, index) => ({ ETag, PartNumber: index + 1 })) } }));
  } catch (error) {
    await client.send(new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId })).catch(() => {});
    throw error;
  }
}

/** The HTTP status a PUT answered, 200 where it succeeded. */
async function statusOf(put: Promise<unknown>): Promise<number | string> {
  return put.then(() => 200, (error: { readonly $metadata?: { readonly httpStatusCode?: number }; readonly name?: string }) => error.$metadata?.httpStatusCode ?? error.name ?? "error");
}

export async function conditional(prefix: string, races: number, racers: number, times: number) {
  const { bucket } = storeFromEnvironment();
  const client = clientOf(racers);
  const key = `${prefix}current.json`;
  const json = (value: unknown) => JSON.stringify({ value, padding: "x".repeat(1024) });
  const put = (body: string, condition: { readonly IfMatch?: string; readonly IfNoneMatch?: string } = {}) => client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body, ContentType: "application/json", ...condition }));
  const etag = async () => (await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))).ETag ?? "";

  // What each condition is answered with.
  await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  const semantics: Record<string, unknown> = {};
  semantics["if_none_match_absent"] = await statusOf(put(json(0), { IfNoneMatch: "*" }));
  semantics["if_none_match_present"] = await statusOf(put(json(1), { IfNoneMatch: "*" }));
  const first = await etag();
  semantics["if_match_current"] = await statusOf(put(json(2), { IfMatch: first }));
  const second = await etag();
  semantics["if_match_stale"] = await statusOf(put(json(3), { IfMatch: first }));
  semantics["if_match_unquoted"] = await statusOf(put(json(4), { IfMatch: second.replaceAll('"', "") }));
  semantics["etags_differ"] = first !== second;
  semantics["held"] = JSON.parse(await (await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))).Body?.transformToString() ?? "{}").value;

  // Racers, each putting the key If-Match the ETag all of them read: how many won each race.
  const winners: number[] = [];
  const answers: Record<string, number> = {};
  for (let race = 0; race < races; race++) {
    const read = await etag();
    const said = await Promise.all(Array.from({ length: racers }, (_, racer) => statusOf(put(json({ race, racer }), { IfMatch: read }))));
    for (const status of said) answers[String(status)] = (answers[String(status)] ?? 0) + 1;
    winners.push(said.filter((status) => status === 200).length);
  }

  // A small JSON PUT's time, unconditional and If-Match, alternately.
  const plain: number[] = [];
  const matched: number[] = [];
  for (let each = 0; each < times; each++) {
    let started = process.hrtime.bigint();
    await put(json(each));
    plain.push(ms(started));
    const current = await etag();
    started = process.hrtime.bigint();
    await put(json(each), { IfMatch: current });
    matched.push(ms(started));
  }
  const stats = (values: number[]) => {
    values.sort((a, b) => a - b);
    return { n: values.length, p50: rank(values, 0.5), p95: rank(values, 0.95), p99: rank(values, 0.99), max: values.at(-1) };
  };
  await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  return { semantics, races: { races, racers, winners, answers }, put_ms: { unconditional: stats(plain), if_match: stats(matched) } };
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)sweep\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      prefix: { type: "string" },
      "object-mib": { type: "string", default: "1024" },
      connections: { type: "string", default: "1,4,8,16,32,64" },
      "ranges-mib": { type: "string", default: "4,16,64" },
      races: { type: "string", default: "20" },
      racers: { type: "string", default: "16" },
      times: { type: "string", default: "100" },
    },
  });
  const numbers = (text: string) => text.split(",").map(Number);
  const [command] = positionals;
  const said = command === "sweep"
    ? await sweep(values.prefix ?? "bench-sweep/", Number(values["object-mib"]), numbers(values.connections ?? ""), numbers(values["ranges-mib"] ?? ""))
    : command === "conditional"
    ? await conditional(values.prefix ?? "bench-conditional/", Number(values.races), Number(values.racers), Number(values.times))
    : null;
  if (said === null) throw new Error("usage: sweep.ts sweep [--prefix P] [--object-mib N] [--connections a,b] [--ranges-mib a,b] | conditional [--prefix P] [--races N] [--racers N] [--times N]");
  console.log(JSON.stringify(said));
}
