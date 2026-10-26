/**
 * The runner systems' one S3 client (DESIGN.md section 4), of the store the environment names:
 * S3_ENDPOINT, S3_BUCKET, S3_REGION (us-east-1 unless set), AWS_ACCESS_KEY_ID and
 * AWS_SECRET_ACCESS_KEY, path-style as SeaweedFS serves.
 *
 *   node s3.ts mkbucket
 *   node s3.ts upload <dir> <prefix>      every file under <dir> to <prefix><path>: multipart, 32 MiB parts, 4 at once
 *   node s3.ts download <prefix> <dir>    every object under <prefix> into <dir>: ranged GETs of 32 MiB, 4 at once
 *   node s3.ts bytes <prefix>             the bytes of the objects under <prefix>
 *   node s3.ts delete <prefix>            deletes the objects under <prefix>, a thousand a request
 *
 * --range-bytes and --at-once change the parts' and ranges' size and how many go at once, over
 * all of a transfer's files together, for a system whose documented practice cuts it otherwise.
 * Each prints one JSON object.
 */
import { type FileHandle, mkdir, open, readdir, stat } from "node:fs/promises";
import { dirname, join, relative } from "node:path";
import { parseArgs } from "node:util";

import {
  CompleteMultipartUploadCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  type S3ClientConfig,
  UploadPartCommand,
} from "@aws-sdk/client-s3";

/** A part, and a ranged GET, unless told: 32 MiB, as alasio's node agent puts and gets them (src/node-agent/names.ts). */
export const PART_BYTES = 32 * 1024 ** 2;
/** How many parts, or GETs, at once, unless told: 4, as alasio's node agent. */
export const AT_ONCE = 4;

/** How a transfer is cut: its parts' or ranges' size, and how many go at once, over all its files together. */
export interface Cut {
  readonly bytes: number;
  readonly atOnce: number;
}
const CUT: Cut = { bytes: PART_BYTES, atOnce: AT_ONCE };

function variable(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is not set`);
  return value;
}

export interface Store {
  readonly client: S3Client;
  readonly bucket: string;
}

/** The client's configuration, of the store the environment names. */
export function clientConfig(): S3ClientConfig {
  return {
    endpoint: variable("S3_ENDPOINT"),
    region: process.env["S3_REGION"] ?? "us-east-1",
    forcePathStyle: true,
    credentials: { accessKeyId: variable("AWS_ACCESS_KEY_ID"), secretAccessKey: variable("AWS_SECRET_ACCESS_KEY") },
    // The SDK's whole-object checksums: not every store takes them, and images carry their own.
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  };
}

export function storeFromEnvironment(): Store {
  return { client: new S3Client(clientConfig()), bucket: variable("S3_BUCKET") };
}

/** Runs every task, `limit` at a time. */
async function inParallel(tasks: readonly (() => Promise<void>)[], limit: number): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, tasks.length) }, async () => {
    while (next < tasks.length) await tasks[next++]?.();
  }));
}

/** The files under `dir`, by their paths relative to it. */
async function filesUnder(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true, recursive: true });
  return entries.filter((entry) => entry.isFile()).map((entry) => relative(dir, join(entry.parentPath, entry.name))).sort();
}

/** Each part's first byte and length, of a file of `size`. */
const ranges = (size: number, bytes: number) => Array.from({ length: Math.ceil(size / bytes) }, (_, index) => ({ start: index * bytes, length: Math.min(bytes, size - index * bytes) }));

/**
 * Puts every file under `dir` as `<prefix><path>`: one up to a part in one PUT, a larger one
 * as a multipart upload, its parts and every other file's `atOnce` at a time together.
 */
export async function upload({ client, bucket }: Store, dir: string, prefix: string, { bytes, atOnce }: Cut = CUT): Promise<{ files: number; bytes: number }> {
  const paths = await filesUnder(dir);
  const opened = await Promise.all(paths.map(async (path) => ({ key: `${prefix}${path}`, file: await open(join(dir, path), "r"), size: (await stat(join(dir, path))).size })));
  try {
    const read = async (file: FileHandle, start: number, length: number) => {
      const read = Buffer.allocUnsafe(length);
      await file.read(read, 0, length, start);
      return read;
    };
    const uploads = await Promise.all(opened.map(async ({ key, size }) => (size > bytes ? (await client.send(new CreateMultipartUploadCommand({ Bucket: bucket, Key: key }))).UploadId : undefined)));
    const etags = opened.map(() => [] as string[]);
    const tasks = opened.flatMap(({ key, file, size }, index) => {
      const uploadId = uploads[index];
      if (uploadId === undefined) return [async () => void (await client.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: await read(file, 0, size) })))];
      return ranges(size, bytes).map(({ start, length }, part) => async () => {
        const { ETag } = await client.send(new UploadPartCommand({ Bucket: bucket, Key: key, UploadId: uploadId, PartNumber: part + 1, Body: await read(file, start, length) }));
        (etags[index] ?? [])[part] = ETag ?? "";
      });
    });
    await inParallel(tasks, atOnce);
    await Promise.all(opened.map(async ({ key }, index) => {
      const uploadId = uploads[index];
      if (uploadId === undefined) return;
      const parts = (etags[index] ?? []).map((ETag, part) => ({ ETag, PartNumber: part + 1 }));
      await client.send(new CompleteMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId, MultipartUpload: { Parts: parts } }));
    }));
    return { files: opened.length, bytes: opened.reduce((sum, { size }) => sum + size, 0) };
  } finally {
    await Promise.all(opened.map(({ file }) => file.close()));
  }
}

/** Gets every object under `prefix` into `<dir>/<rest of its key>`: ranged GETs of `bytes`, `atOnce` at a time over all of them together, each written where it goes. */
export async function download(store: Store, prefix: string, dir: string, { bytes, atOnce }: Cut = CUT): Promise<{ files: number; bytes: number }> {
  const { client, bucket } = store;
  const objects = await list(store, prefix);
  const opened = await Promise.all(objects.map(async ({ key, bytes: size }) => {
    const path = join(dir, key.slice(prefix.length));
    await mkdir(dirname(path), { recursive: true });
    return { key, size, file: await open(path, "w") };
  }));
  try {
    const tasks = opened.flatMap(({ key, size, file }) =>
      ranges(size, bytes).map(({ start, length }) => async () => {
        const { Body } = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key, Range: `bytes=${start}-${start + length - 1}` }));
        if (!Body) throw new Error(`no body for ${key} ${start}+${length}`);
        const got = await Body.transformToByteArray();
        if (got.length !== length) throw new Error(`${key} ${start}+${length} gave ${got.length} bytes`);
        await file.write(got, 0, length, start);
      })
    );
    await inParallel(tasks, atOnce);
    return { files: opened.length, bytes: opened.reduce((sum, { size }) => sum + size, 0) };
  } finally {
    await Promise.all(opened.map(({ file }) => file.close()));
  }
}

/** Every object under `prefix`: its key and size. */
export async function list({ client, bucket }: Store, prefix: string): Promise<{ key: string; bytes: number }[]> {
  const found: { key: string; bytes: number }[] = [];
  let token: string | undefined;
  do {
    const page = await client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token }));
    for (const { Key, Size } of page.Contents ?? []) if (Key) found.push({ key: Key, bytes: Size ?? 0 });
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);
  return found;
}

/** Deletes every object under `prefix`, a thousand a request, as S3 takes them: how many. */
export async function remove(store: Store, prefix: string): Promise<{ prefix: string; deleted: number }> {
  if (prefix === "") throw new Error("deleting the whole bucket is not asked by an empty prefix");
  const keys = (await list(store, prefix)).map(({ key }) => ({ Key: key }));
  for (let at = 0; at < keys.length; at += 1000) {
    const { Errors } = await store.client.send(new DeleteObjectsCommand({ Bucket: store.bucket, Delete: { Objects: keys.slice(at, at + 1000), Quiet: true } }));
    if (Errors && Errors.length > 0) throw new Error(`${Errors.length} objects under ${prefix} were not deleted: ${Errors[0]?.Key} ${Errors[0]?.Message}`);
  }
  return { prefix, deleted: keys.length };
}

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)s3\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: { "at-once": { type: "string", default: String(AT_ONCE) }, "range-bytes": { type: "string", default: String(PART_BYTES) } } });
  const [command, first = "", second = ""] = positionals;
  const cut = { bytes: Number(values["range-bytes"]), atOnce: Number(values["at-once"]) };
  const store = storeFromEnvironment();
  const said =
    command === "mkbucket" ? await store.client.send(new CreateBucketCommand({ Bucket: store.bucket })).then(() => ({ bucket: store.bucket }), (error: { name?: string }) => {
      if (error.name === "BucketAlreadyOwnedByYou" || error.name === "BucketAlreadyExists") return { bucket: store.bucket };
      throw error;
    })
    : command === "upload" ? await upload(store, first, second, cut)
    : command === "download" ? await download(store, first, second, cut)
    : command === "bytes" ? { prefix: first, bytes: (await list(store, first)).reduce((sum, { bytes }) => sum + bytes, 0) }
    : command === "delete" ? await remove(store, first)
    : null;
  if (said === null) throw new Error("usage: s3.ts [--at-once N] [--range-bytes B] mkbucket | upload <dir> <prefix> | download <prefix> <dir> | bytes <prefix> | delete <prefix>");
  console.log(JSON.stringify(said));
}
