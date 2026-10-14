/**
 * The node agent's API, which alasio calls on the agent's pod's address, with the token
 * both are given (`Authorization: Bearer <token>`): every request but `GET /healthz`
 * without it is answered 401. It speaks JSON; a refusal or a failure is
 * `{ "error": "<why>" }` with its status, 400 for a request that is not one, 404 for a
 * route there is not.
 *
 * - `POST /checkpoints`, `{ environment, workspace, namespace, pod, containerId, hook,
 *   podTemplate }`: starts checkpointing the sandbox of the container `containerId`
 *   (containerd's id) of the pod `namespace`/`pod`, under the gVisor release that runs it,
 *   running `hook` (its arguments, none holding whitespace) in it first, which ends the
 *   sandbox, and making it the workspace's snapshot, recording `podTemplate` (202), unless
 *   one of the workspace's runs, which goes on.
 * - `GET /checkpoints/<environment>/<workspace>`: how its last checkpoint stands (200):
 *   `{ state: "running" }`; `{ state: "done", value }`, `value` the snapshot's manifest; or
 *   `{ state: "failed", status, error }`, `status` 500, whose error says why, and that the
 *   workspace's snapshot was forgotten, as it is. 404
 *   when it has none the agent keeps (./operations.ts).
 * - `POST /prepare`, `{ environment, workspace }`: starts preparing the workspace's current
 *   snapshot in this node's cache, for a restore on this node (202), unless a prepare of
 *   it runs, which goes on.
 * - `GET /prepare/<environment>/<workspace>`: how its last prepare stands, as a
 *   checkpoint's does, `value` `{ manifest, imagePath }`; `status` 404 when it has no
 *   snapshot, 409 when this node cannot restore it (its release is not installed here, or
 *   this node's CPU lacks a feature it was saved with), 422 when its image is not what its
 *   manifest says, 502 when the bucket failed.
 * - `POST /copies`, `{ from: { environment, workspace }, to: { environment, workspace } }`:
 *   makes the current snapshot of `from` that of `to` too, copied in the bucket, as a
 *   session forked from another restores what the other was: `to`'s manifest (200). 404
 *   when `from` has none; 502 when the bucket failed, which leaves `to` without a snapshot.
 * - `POST /resume`, `{ environment, workspace, namespace, pod, podIPs, containerIds,
 *   policies }`: resumes the workspace's sandbox, restored paused in the pod
 *   `namespace`/`pod` on this node, once the pod's network policies, `policies`, are in
 *   force here for each of its addresses `podIPs` (./kube-router.ts): the connections tracked from
 *   them forgotten, then each of its containers `containerIds` (containerd's ids) resumed
 *   (204). 504 when its policies were not in force within the agent's wait, saying for
 *   which address what was missing, its sandbox left paused; 500 when a command failed.
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

import { SNAPSHOT_ID } from "../kube/sandbox-shape.ts";
import { withLogScope } from "../shared/log.ts";
import { BearerToken, sameToken } from "../shared/tokens.ts";
import {
  type CheckpointFailed,
  CheckpointRequest,
  CopyRequest,
  makeNodeAgent,
  type Manifest,
  type NodeAgent,
  type Prepared,
  type PrepareError,
  ResumeRequest,
  WorkspaceRef,
} from "./agent.ts";
import { type CacheError, makeSnapshotCache } from "./cache.ts";
import { makeHostCommands } from "./host.ts";
import {
  CONTAINERD,
  GVISOR_RELEASES,
  NODE_AGENT_PORT,
  NODE_AGENT_TOKEN_VARIABLE,
  RUNSC_ROOT,
  SNAPSHOT_CACHE_BYTES_VARIABLE,
  SNAPSHOT_CACHE_DIR,
} from "./names.ts";
import { type Failure, makeOperations, type Operations } from "./operations.ts";
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
  ContainerElsewhere: 409,
  SnapshotCorrupt: 422,
  S3Error: 502,
  PoliciesNotInForce: 504,
};

const SNAPSHOT = /^\/snapshots\/([^/]+)\/([^/]+)$/u;
const CHECKPOINT = /^\/checkpoints\/([^/]+)\/([^/]+)$/u;
const PREPARE = /^\/prepare\/([^/]+)\/([^/]+)$/u;
const ENVIRONMENT = /^\/snapshots\/([^/]+)$/u;
const CACHED = /^\/cache\/([^/]+)\/([^/?]+)(?:\?keep=([^&]*))?$/u;

const answer = (status: number, body: unknown): HttpServerResponse.HttpServerResponse => HttpServerResponse.jsonUnsafe(body, { status });

/** The key of a workspace's operations. */
const keyOf = ({ environment, workspace }: WorkspaceRef): string => `${environment}/${workspace}`;

/** How an operation that failed by `cause` is answered: as a request that failed so would be. */
const failureOf = (cause: Cause.Cause<{ readonly _tag: string; readonly message: string }>): Failure => {
  const error = Cause.findErrorOption(cause);
  return Option.isSome(error) ? { status: STATUS[error.value._tag] ?? 500, error: error.value.message } : { status: 500, error: Cause.pretty(cause) };
};

/** `operation`, a failure of the agent's own logged, as a request that failed so is answered. */
const logged = <A, E extends { readonly _tag: string; readonly message: string }>(operation: Effect.Effect<A, E>): Effect.Effect<A, E> =>
  operation.pipe(Effect.tapCause((cause) => {
    if (Cause.hasInterruptsOnly(cause)) return Effect.void;
    const { status, error } = failureOf(cause);
    return status >= 500 ? Effect.logWarning(error) : Effect.void;
  }));

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
  const scope = yield* Effect.scope;
  const checkpoints = yield* makeOperations<Manifest, CheckpointFailed>(scope, failureOf);
  const prepares = yield* makeOperations<Prepared, PrepareError>(scope, failureOf);
  const started = answer(202, { state: "running" });
  /** How the operation of `operations` for the workspace a path names stands. */
  const outcome = <A, E>(operations: Operations<A, E>, path: RegExpExecArray) =>
    workspaceOf(path).pipe(
      Effect.flatMap((workspace) => operations.outcome(keyOf(workspace))),
      Effect.map((found) => (found === null ? answer(404, { error: "the agent keeps no such operation of the workspace's" }) : answer(200, found))),
    );

  const route = (request: HttpServerRequest.HttpServerRequest) => {
    const { method, url } = request;
    const snapshot = SNAPSHOT.exec(url);
    const checkpoint = CHECKPOINT.exec(url);
    const prepare = PREPARE.exec(url);
    if (method === "POST" && url === "/checkpoints") {
      return body(CheckpointRequest).pipe(Effect.flatMap((asked) => checkpoints.start(keyOf(asked), logged(agent.checkpoint(asked)))), Effect.as(started));
    }
    if (method === "GET" && checkpoint) return outcome(checkpoints, checkpoint);
    if (method === "POST" && url === "/prepare") {
      return body(WorkspaceRef).pipe(Effect.flatMap((asked) => prepares.start(keyOf(asked), logged(agent.prepare(asked)))), Effect.as(started));
    }
    if (method === "GET" && prepare) return outcome(prepares, prepare);
    if (method === "POST" && url === "/copies") {
      return body(CopyRequest).pipe(Effect.flatMap(({ from, to }) => agent.copy(from, to)), Effect.map((manifest) => answer(200, manifest)));
    }
    if (method === "POST" && url === "/resume") {
      return body(ResumeRequest).pipe(Effect.flatMap(agent.resume), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "GET" && snapshot) {
      return workspaceOf(snapshot).pipe(
        Effect.flatMap(agent.current),
        Effect.map((manifest) => (manifest === null ? answer(404, { error: "the workspace has no snapshot" }) : answer(200, manifest))),
      );
    }
    if (method === "DELETE" && snapshot) return workspaceOf(snapshot).pipe(Effect.flatMap(agent.forget), Effect.as(HttpServerResponse.empty({ status: 204 })));
    if (method === "GET" && url === "/holdings") return agent.holdings.pipe(Effect.map((holdings) => answer(200, holdings)));
    const environment = ENVIRONMENT.exec(url);
    if (method === "GET" && environment) {
      return Schema.decodeUnknownEffect(WorkspaceRef.fields.environment)(decodeURIComponent(environment[1] ?? "")).pipe(
        Effect.mapError((error) => new BadRequest({ message: error.message })),
        Effect.flatMap(agent.workspaces),
        Effect.map((workspaces) => answer(200, { workspaces })),
      );
    }
    const cached = CACHED.exec(url);
    if (method === "DELETE" && cached) {
      const keep = decodeURIComponent(cached[3] ?? "");
      if (keep !== "" && !new RegExp(`^${SNAPSHOT_ID}$`, "u").test(keep)) return Effect.fail(new BadRequest({ message: `${keep} is no snapshot's id` }));
      return workspaceOf(cached).pipe(
        Effect.flatMap((ref) => agent.discard(ref, keep === "" ? null : keep)),
        Effect.map((discarded) => answer(200, { discarded })),
      );
    }
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
    host: makeHostCommands(),
  });
  const served = yield* serveNodeAgent({ port: NODE_AGENT_PORT, token: Redacted.value(token), agent });
  yield* Effect.logInfo(`the node agent of ${node} serves on port ${served.port}, its cache within ${budget} bytes`);
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("node-agent"),
);
