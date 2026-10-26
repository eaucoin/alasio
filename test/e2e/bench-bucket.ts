/**
 * Run inside a node agent's pod, by node there (inNodeAgent in harness.ts), for the
 * memory-snapshot benchmark (./snapshot-bench.test.ts), with the snapshots bucket's settings
 * the agent is given, through the agent's own client of the bucket:
 *
 * - `bytes <environment> <workspace>` prints the bytes of every object the workspace's
 *   snapshots hold in the bucket, as `{ bytes, objects }`.
 * - `recommit <environment> <workspace> <snapshot>` makes the snapshot the workspace's current
 *   one again, its manifest put back as `current.json`, as a checkpoint commits it, so that it
 *   is restored again from the same image. Prints `{ snapshot, release }`.
 *
 * alasio's modules are imported by their paths in the repository, which inNodeAgent in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { Effect, Schema } from "effect";

import { CURRENT_FILE, Manifest, MANIFESTS_DIR } from "../../src/node-agent/agent.ts";
import { makeS3Bucket } from "../../src/node-agent/s3.ts";

/** The bytes of a workspace's objects, and how many there are. */
export interface Held {
  readonly bytes: number;
  readonly objects: number;
}

/** The snapshot made current again, and the release that took it. */
export interface Recommitted {
  readonly snapshot: string;
  readonly release: string;
}

const [verb, environment, workspace, snapshot] = process.argv.slice(2);
if (!(verb === "bytes" || (verb === "recommit" && snapshot)) || !environment || !workspace) {
  throw new Error("usage: bench-bucket.ts bytes <environment> <workspace> | recommit <environment> <workspace> <snapshot>");
}

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

const prefix = `${environment}/${workspace}`;

const said = await Effect.runPromise(Effect.gen(function*() {
  if (verb === "bytes") {
    const listed = yield* bucket.list(`${prefix}/`);
    const sizes = yield* Effect.forEach(listed, ({ key }) => bucket.head(key), { concurrency: 8 });
    const held: Held = { bytes: sizes.reduce((sum, stored) => sum + (stored?.bytes ?? 0), 0), objects: listed.length };
    return held;
  }
  const manifest = yield* Schema.decodeUnknownEffect(Manifest)(yield* bucket.getJson(`${prefix}/${MANIFESTS_DIR}/${snapshot}.json`));
  yield* bucket.putJson(`${prefix}/${CURRENT_FILE}`, manifest);
  const done: Recommitted = { snapshot: manifest.snapshot, release: manifest.release };
  return done;
}));
console.log(JSON.stringify(said));
