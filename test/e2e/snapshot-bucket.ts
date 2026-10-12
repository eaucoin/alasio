/**
 * Run inside a node agent's pod, by node there (inNodeAgent in harness.ts), with the
 * snapshots bucket's settings the agent is given: `corrupt <environment> <workspace>`
 * overwrites the largest file of the workspace's current snapshot's image in the bucket with
 * as many zeros, through the agent's own client of the bucket, as a disk's rot would.
 * Prints one JSON line of the file overwritten.
 *
 * alasio's modules are imported by their paths in the repository, which inNodeAgent in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { writeFileSync } from "node:fs";

import { Effect, Schema } from "effect";

import { CURRENT_FILE, Manifest } from "../../src/node-agent/agent.ts";
import { makeS3Bucket } from "../../src/node-agent/s3.ts";

/** The file of the image overwritten: its snapshot, its path in the image, and its size. */
export interface Corrupted {
  readonly snapshot: string;
  readonly path: string;
  readonly bytes: number;
}

const [verb, environment, workspace] = process.argv.slice(2);
if (verb !== "corrupt" || !environment || !workspace) throw new Error("usage: snapshot-bucket.ts corrupt <environment> <workspace>");

/** The variable of the agent's environment `name`, which it is given. */
function variable(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`the node agent is given no ${name}`);
  return value;
}

const bucket = makeS3Bucket({
  endpoint: variable("S3_ENDPOINT"),
  region: variable("S3_REGION"),
  bucket: variable("S3_BUCKET"),
  accessKeyId: variable("AWS_ACCESS_KEY_ID"),
  secretAccessKey: variable("AWS_SECRET_ACCESS_KEY"),
});

const corrupted = await Effect.runPromise(Effect.gen(function*() {
  const manifest = yield* Schema.decodeUnknownEffect(Manifest)(yield* bucket.getJson(`${environment}/${workspace}/${CURRENT_FILE}`));
  // The largest, which is the sandbox's memory, never all zeros.
  const [file] = [...manifest.files].sort((a, b) => b.bytes - a.bytes);
  if (!file) return yield* Effect.die(new Error(`the snapshot ${manifest.snapshot} of ${environment}/${workspace} has no file`));
  const zeros = "/tmp/e2e-corrupted";
  writeFileSync(zeros, Buffer.alloc(file.bytes));
  yield* bucket.putFile(`${environment}/${workspace}/${manifest.snapshot}/${file.path}`, zeros);
  const done: Corrupted = { snapshot: manifest.snapshot, path: file.path, bytes: file.bytes };
  return done;
}));
console.log(JSON.stringify(corrupted));
