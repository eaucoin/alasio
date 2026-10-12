/**
 * The node agent's API, which alasio calls on the agent's pod's address, with the token
 * both are given (`Authorization: Bearer <token>`): every request but `GET /healthz`
 * without it is answered 401. It speaks JSON; a refusal or a failure is
 * `{ "error": "<why>" }` with its status, 400 for a request that is not one, 404 for a
 * route there is not.
 *
 * - `POST /checkpoints`, `{ environment, workspace, release, containerId, hook, podTemplate }`:
 *   checkpoints the sandbox of the container `containerId` (containerd's id) under the
 *   gVisor `release`, running `hook` (its arguments, none holding whitespace) in it first,
 *   which ends the sandbox, and makes it the workspace's snapshot, recording
 *   `podTemplate`: its manifest (200). 500 when it failed, whose error says that the
 *   workspace's snapshot was forgotten, as it is.
 * - `POST /prepare`, `{ environment, workspace }`: the workspace's current snapshot in this
 *   node's cache, for a restore on this node: `{ manifest, imagePath }` (200). 404 when it
 *   has none; 409 when this node cannot restore it (its release is not installed here, or
 *   this node's CPU lacks a feature it was saved with); 422 when its image is not what its
 *   manifest says; 502 when the bucket failed.
 * - `GET /snapshots/<environment>/<workspace>`: the manifest of its current snapshot (200),
 *   or 404.
 * - `DELETE /snapshots/<environment>/<workspace>`: forgets its snapshot, in the bucket and
 *   in this node's cache (204).
 * - `GET /holdings`: `{ node, releases: [{ release, cpuFeatures }], cached: [{ environment,
 *   workspace, snapshot, bytes, usedAt }] }`: this node's gVisor releases, with the CPU
 *   features each sees on it, and the snapshots in its cache.
 * - `GET /healthz`: 200 once it serves.
 *
 * A manifest is ./agent.ts Manifest.
 */
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Cause, Config, Effect, Option, Redacted, Schema, type Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import { BearerToken, sameToken } from "../shared/tokens.ts";
import { CheckpointRequest, makeNodeAgent, type NodeAgent, WorkspaceRef } from "./agent.ts";
import { type CacheError, makeSnapshotCache } from "./cache.ts";
import {
  CONTAINERD,
  GVISOR_RELEASES,
  NODE_AGENT_PORT,
  NODE_AGENT_TOKEN_VARIABLE,
  RUNSC_ROOT,
  SNAPSHOT_CACHE_BYTES_VARIABLE,
  SNAPSHOT_CACHE_DIR,
} from "./names.ts";
import { makeRunsc } from "./runsc.ts";
import { makeS3Bucket } from "./s3.ts";

/** What serveNodeAgent is given: where it listens, the token it takes, and the agent it serves. */
export interface NodeAgentServerOptions {
  readonly port: number;
  readonly host?: string;
  readonly token: string;
  readonly agent: NodeAgent;
}

/** The status each failure of the agent's is answered with; any other is 500. */
const STATUS: Readonly<Record<string, number>> = {
  NoSnapshot: 404,
  Unrestorable: 409,
  SnapshotCorrupt: 422,
  S3Error: 502,
};

const SNAPSHOT = /^\/snapshots\/([^/]+)\/([^/]+)$/u;

const answer = (status: number, body: unknown): HttpServerResponse.HttpServerResponse => HttpServerResponse.jsonUnsafe(body, { status });

/** A request that is not one: why, answered 400. */
class BadRequest extends Schema.TaggedError<BadRequest>()("BadRequest", { message: Schema.String }) {}

/** The request's body, as `schema` decodes it from JSON. */
const body = <A>(schema: Schema.ConstraintDecoder<A, never>): Effect.Effect<A, BadRequest, HttpServerRequest.HttpServerRequest> =>
  HttpServerRequest.schemaBodyJson(schema).pipe(
    Effect.mapError((error) => new BadRequest({ message: error._tag === "SchemaError" ? error.message : "the body is not JSON" })),
  );

/** The workspace a path names. */
const workspaceOf = ([, environment = "", workspace = ""]: RegExpExecArray): Effect.Effect<WorkspaceRef, BadRequest> =>
  Schema.decodeUnknownEffect(WorkspaceRef)({ environment: decodeURIComponent(environment), workspace: decodeURIComponent(workspace) }).pipe(
    Effect.mapError((error) => new BadRequest({ message: error.message })),
  );

/** Serves the agent's API, as the module says, until the scope closes: the port it listens on, once it does. */
export const serveNodeAgent = Effect.fnUntraced(function*({
  port,
  host = "0.0.0.0",
  token,
  agent,
}: NodeAgentServerOptions): Effect.fn.Return<{ readonly port: number }, HttpServerError.ServeError, Scope.Scope> {
  const route = (request: HttpServerRequest.HttpServerRequest) => {
    const { method, url } = request;
    const snapshot = SNAPSHOT.exec(url);
    if (method === "POST" && url === "/checkpoints") {
      return body(CheckpointRequest).pipe(Effect.flatMap(agent.checkpoint), Effect.map((manifest) => answer(200, manifest)));
    }
    if (method === "POST" && url === "/prepare") return body(WorkspaceRef).pipe(Effect.flatMap(agent.prepare), Effect.map((prepared) => answer(200, prepared)));
    if (method === "GET" && snapshot) {
      return workspaceOf(snapshot).pipe(
        Effect.flatMap(agent.current),
        Effect.map((manifest) => (manifest === null ? answer(404, { error: "the workspace has no snapshot" }) : answer(200, manifest))),
      );
    }
    if (method === "DELETE" && snapshot) return workspaceOf(snapshot).pipe(Effect.flatMap(agent.forget), Effect.as(HttpServerResponse.empty({ status: 204 })));
    if (method === "GET" && url === "/holdings") return agent.holdings.pipe(Effect.map((holdings) => answer(200, holdings)));
    return Effect.succeed(answer(404, { error: `there is no ${method} ${url}` }));
  };

  const handle = Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.method === "GET" && request.url === "/healthz") return HttpServerResponse.text("ok");
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    if (Option.isNone(bearer) || !sameToken(bearer.value[1], token)) return answer(401, { error: "the request bears no token of alasio's" });
    return yield* route(request);
  }).pipe(
    Effect.catch((error) => {
      const status = error._tag === "BadRequest" ? 400 : STATUS[error._tag] ?? 500;
      return (status >= 500 ? Effect.logWarning(error.message) : Effect.void).pipe(Effect.as(answer(status, { error: error.message })));
    }),
    withLogScope("node-agent"),
  );

  const server = yield* NodeHttpServer.make(() => createServer(), { port, host });
  yield* server.serve(handle);
  // Listening on a TCP port, its address is an internet one.
  return { port: server.address._tag === "UnixPathAddress" ? port : server.address.port };
});

/** The bucket the agent keeps snapshots in, as its Secret gives it (S3_*, AWS_*). */
const BucketConfig = Config.all({
  endpoint: Config.String("S3_ENDPOINT"),
  region: Config.String("S3_REGION"),
  bucket: Config.String("S3_BUCKET"),
  accessKeyId: Config.String("AWS_ACCESS_KEY_ID"),
  secretAccessKey: Config.Redacted("AWS_SECRET_ACCESS_KEY"),
});

/** The most the node's cache keeps unless told: 5 GiB. */
const DEFAULT_CACHE_BYTES = 5 * 1024 ** 3;
/** How long a snapshot used is kept in the cache whatever its budget: a restore on this node reads it meanwhile. */
const EVICTION_GRACE_MS = 10 * 60_000;

/**
 * The node agent of the node it runs on (NODE_NAME), as its environment configures it:
 * the bucket (BucketConfig), the token alasio presents (NODE_AGENT_TOKEN_VARIABLE), and
 * the cache's budget in bytes (SNAPSHOT_CACHE_BYTES_VARIABLE). What a stopped agent left
 * half written in the cache is removed first. What stops it is logged.
 */
export const nodeAgent: Effect.Effect<void, Config.ConfigError | HttpServerError.ServeError | CacheError, Scope.Scope> = Effect.gen(function*() {
  const node = yield* Config.String("NODE_NAME");
  const token = yield* Config.Redacted(NODE_AGENT_TOKEN_VARIABLE);
  const budget = yield* Config.Int(SNAPSHOT_CACHE_BYTES_VARIABLE).pipe(Config.withDefault(DEFAULT_CACHE_BYTES));
  const { secretAccessKey, ...bucket } = yield* BucketConfig;
  const cache = makeSnapshotCache({ root: SNAPSHOT_CACHE_DIR, budget, graceMs: EVICTION_GRACE_MS });
  yield* cache.clean;
  const agent = yield* makeNodeAgent({
    node,
    runsc: makeRunsc({ releasesDir: GVISOR_RELEASES, containerdDir: CONTAINERD, root: RUNSC_ROOT }),
    cache,
    bucket: makeS3Bucket({ ...bucket, secretAccessKey: Redacted.value(secretAccessKey) }),
  });
  const served = yield* serveNodeAgent({ port: NODE_AGENT_PORT, token: Redacted.value(token), agent });
  yield* Effect.logInfo(`the node agent of ${node} serves on port ${served.port}, its cache within ${budget} bytes`);
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("node-agent"),
);
