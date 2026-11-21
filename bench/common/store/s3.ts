/**
 * The benchmark's S3 client, of the store the environment names: S3_ENDPOINT, S3_BUCKET,
 * S3_REGION (us-east-1 unless set), AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, path-style as
 * SeaweedFS serves. The harness runs it in a node agent's network, as the agent's own client of
 * the snapshots bucket; ./up.sh makes the single server's bucket with it.
 *
 *   node s3.ts mkbucket
 *   node s3.ts bytes <prefix>             the bytes of the objects under <prefix>
 *   node s3.ts copy <from> <to>           copies the object <from> as <to>, in the bucket
 *
 * Each prints one JSON object.
 */
import { parseArgs } from "node:util";

import { CopyObjectCommand, CreateBucketCommand, ListObjectsV2Command, S3Client, type S3ClientConfig } from "@aws-sdk/client-s3";

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

// Run as a program, not imported, nor bundled into another's (../driver/build.ts).
if (/(^|\/)s3\.(ts|mjs)$/u.test(process.argv[1] ?? "")) {
  const { positionals } = parseArgs({ allowPositionals: true });
  const [command, first = "", second = ""] = positionals;
  const store = storeFromEnvironment();
  const said =
    command === "mkbucket" ? await store.client.send(new CreateBucketCommand({ Bucket: store.bucket })).then(() => ({ bucket: store.bucket }), (error: { name?: string }) => {
      if (error.name === "BucketAlreadyOwnedByYou" || error.name === "BucketAlreadyExists") return { bucket: store.bucket };
      throw error;
    })
    : command === "bytes" ? { prefix: first, bytes: (await list(store, first)).reduce((sum, { bytes }) => sum + bytes, 0) }
    : command === "copy" ? await store.client.send(new CopyObjectCommand({ Bucket: store.bucket, Key: second, CopySource: `${store.bucket}/${first}` })).then(() => ({ copied: first, to: second }))
    : null;
  if (said === null) throw new Error("usage: s3.ts mkbucket | bytes <prefix> | copy <from> <to>");
  console.log(JSON.stringify(said));
}
