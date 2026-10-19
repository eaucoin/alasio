/**
 * The node agent's API, which the sandbox gateway calls on the agent's pod's address, with
 * the token both are given (`Authorization: Bearer <token>`): every request but `GET /healthz`
 * without it is answered 401. It speaks JSON; a refusal or a failure is
 * `{ "error": "<why>" }` with its status, 400 for a request that is not one, 404 for a
 * route there is not. A 503 says, by its Retry-After, when to ask again (RETRY_AFTER_S):
 * what it refuses so is of a moment, as workspace storage's mount is handed over to its
 * successor, or has not loaded a workspace's quota yet.
 *
 * - `POST /checkpoints`, `{ environment, workspace, namespace, pod, containerId, hook,
 *   podTemplate, live? }`: starts checkpointing the sandbox of the container
 *   `containerId` (containerd's id) of the pod `namespace`/`pod`, under the gVisor release
 *   that runs it, running `hook` (its arguments, none holding whitespace) in it first, which
 *   ends the sandbox, and making it the workspace's snapshot, recording `podTemplate`
 *   (202), unless one of the workspace's runs, which goes on. With `live`, `{ stateDir }`,
 *   bayma's state directory in the container, it is saved live, running on; its manifest
 *   records what bayma's hook said the save caught, and a restore from it resumes only
 *   while bayma's state generation is that one.
 * - `GET /checkpoints/<environment>/<workspace>`: how its last checkpoint stands (200):
 *   `{ state: "running" }`; `{ state: "done", value }`, `value` the snapshot's manifest; or
 *   `{ state: "failed", status, error }`, `status` 500, whose error says why, and that the
 *   workspace's snapshot was forgotten, as it is, or, of one saved live, kept; 410 where its
 *   sandbox can be saved no more, its restore having failed to read some of its pages, as
 *   every checkpoint of it after will. 404 when it has none the agent keeps (./operations.ts).
 * - `POST /prepare`, `{ environment, workspace, gofer? }`: starts preparing the workspace's
 *   current snapshot for a restore on this node (202), from this node's cache, or else from
 *   the bucket, which the restore reads as it goes, unless a prepare of it runs, which goes
 *   on; `gofer`, `{ transport: "socket" | "tcp", pagesRetry }`, says how its checkpoint gofer
 *   reads the bucket through the agent (./agent.ts PrepareRequest), over TCP unless given.
 * - `GET /prepare/<environment>/<workspace>`: how its last prepare stands, as a
 *   checkpoint's does, `value` `{ manifest, imagePath, source }`, `source` "cache" or
 *   "bucket"; `status` 404 when it has no snapshot, 409 when this node cannot restore it (its
 *   release is not installed here, or this node's CPU lacks a feature it was saved with, or
 *   its runsc does not restore it, as `runsc image verify --host` says), 422 when its image is
 *   not what its manifest says, 502 when the bucket failed or cannot be told from the node,
 *   503 when it would be read from the bucket and the agent is stopping (its node's next
 *   agent prepares it).
 * - `POST /copies`, `{ from: { environment, workspace }, to: { environment, workspace } }`:
 *   makes the current snapshot of `from` that of `to` too, copied in the bucket, as a
 *   session forked from another restores what the other was: `to`'s manifest (200). 404
 *   when `from` has none; 502 when the bucket failed, which leaves `to` without a snapshot.
 * - `POST /resume`, `{ environment, workspace, namespace, pod, podIPs, containerIds,
 *   policies, snapshot }`: resumes the workspace's sandbox, restored paused from
 *   `snapshot` in the pod `namespace`/`pod` on this node, once the pod's network policies, `policies`, are in
 *   force here for each of its addresses `podIPs` (./kube-router.ts): the connections tracked from
 *   them forgotten, then each of its containers `containerIds` (containerd's ids) resumed
 *   (204). 504 when its policies were not in force within the agent's wait, saying for
 *   which address what was missing, its sandbox left paused; 410 when it was restored from
 *   a snapshot taken live that holds the session no more, bayma's state generation having
 *   moved on since, or that cannot be told, or from one whose manifest no node or bucket
 *   holds, its sandbox left paused; 502 when the bucket failed to give that manifest, its
 *   sandbox left paused; 500 when a command failed.
 * - `POST /abandon`, `{ environment, workspace, namespace, pod, uid }`: abandons the
 *   workspace's restore into the pod `namespace`/`pod` of the uid `uid` on this node, which
 *   alasio refused before it resumed its sandbox: kills each process runsc runs for the pod's containers, their
 *   sandbox and gofers among them, that is still the one it found, so that the pod's
 *   teardown waits on none of them: `{ killed: [{ containerId, pid }] }` (200). 500 when
 *   runsc's containers, or the node's processes, could not be read, or one not killed.
 * - `GET /snapshots/<environment>/<workspace>`: the manifest of its current snapshot (200),
 *   or 404.
 * - `DELETE /snapshots/<environment>/<workspace>`: forgets its snapshots, in the bucket and
 *   in this node's cache (204).
 * - `DELETE /snapshots/<environment>/<workspace>/current?snapshot=<snapshot>`: makes its
 *   current snapshot current no more, if it is `snapshot` still, one its sandbox was
 *   restored from, or one taken live whose exec has ended, its images kept as the layers
 *   of its next (204).
 * - `POST /collect`, `{ environment, workspace }`: collects what of the workspace's in the
 *   bucket no snapshot needs any more: `{ kept }`, the digests of the images it keeps (200).
 * - `GET /snapshots/<environment>`: `{ workspaces }`, those the bucket holds anything of.
 * - `DELETE /cache/<environment>/<workspace>?keep=<digest>,...`: removes from this node's
 *   cache the workspace's images but those kept, unless a restore may still be reading
 *   them: `{ discarded }`, whether it did (200).
 * - `GET /holdings`: `{ node, releases: [{ release, cpuFeatures }], cached: [{ environment,
 *   workspace, images, bytes, usedAt }] }`: this node's gVisor releases, with the CPU
 *   features each sees on it, and the workspaces whose images its cache holds, by digest.
 * - `POST /workspaces`, `{ environment, workspace, quotaBytes }`: makes the workspace, its
 *   directory and its quota (./workspaces.ts) (204); again, sets its quota anew.
 * - `POST /workspaces/clones`, `{ from, to, quotaBytes }`: makes the workspace `to`, of any
 *   environment, a clone of `from`, its quota its own (204); 404 where `from` is not there,
 *   409 where `to` is.
 * - `DELETE /workspaces/<environment>/<workspace>`: deletes the workspace, its quota with it
 *   (204), as it does one that is not there.
 * - `GET /workspaces`: `{ environments }`, the names of the environments any workspace is of.
 * - `GET /workspaces/<environment>`: `{ workspaces }`, the names of the environment's workspaces.
 * - `GET /workspaces/<environment>/<workspace>/released`: `{ snapshot, release }`, the snapshot
 *   it was last released into, as its record beside its files says (./workspaces.ts) (200);
 *   404 where none is recorded.
 * - `PUT /workspaces/<environment>/<workspace>/released`, `{ snapshot, release }`: records the
 *   snapshot it is released into (204); 404 where there is no such workspace.
 * - `DELETE /workspaces/<environment>/<workspace>/released`: records none (204).
 * - `GET /workspaces/<environment>/<workspace>/quota`: `{ capacityBytes, usedBytes }`, its
 *   quota and what it holds (200); 404 where it has none.
 * - `POST /attach`, `{ environment, workspace, pod, token, env, made? }`: attaches the workspace's
 *   files into the directory of the sandbox pool pod `pod` on this node, of the environment's
 *   namespace, its binding's `token`
 *   and the variables its bayma runs with, `env`, beside them (./workspaces.ts) (204), the
 *   directory kubelet made, or, `made`, one made now for a restore's pod yet to be made; 409 when
 *   the pod's directory holds a mount, or, `made`, is there already, 404 when there is no such
 *   workspace.
 * - `POST /bindings`, `{ namespace, pod, uid, gid, binding }`: gives the free pod `pod` of a
 *   harness pool on this node, of `namespace`, its `binding`, the text of the file its host
 *   serves once it is there, its user's and group's (./workspaces.ts bindFree) (204); 404 where
 *   kubelet made it no directory, 409 where it has a binding already, or a mount.
 * - `GET /attachments`: `{ attachments: [{ namespace, pod, reachable }] }`, the pods of this node a
 *   workspace is attached into, each by whether what is attached is reachable still (./workspaces.ts).
 * - `POST /stop`, `{ namespace, pod, containerId, hook }`: has the bayma of the bound pod
 *   `namespace`/`pod` of a pool on this node, its container `containerId`, go on with the stop
 *   it holds, by `hook`, its `sandbox-hook stop`, run in it (./workspaces.ts) (204); 403 for a
 *   container that is not the bayma of a pool's pod here, 404 for one that runs not, 502 where
 *   the hook fails.
 * - `POST /sync`, `{ namespace, pod, containerId }`: has the sandbox of that bayma write what it
 *   holds of its files through to the node's mount, by `sync` run in it (./workspaces.ts)
 *   (204); refused as a stop is, 502 where it fails.
 * - `POST /detach`, `{ namespace, pod, containerIds }`: detaches what is attached into the directory of `namespace`/`pod`
 *   (204); 409 while any of `containerIds`, its sandbox's containers, runs.
 * - `GET /files/<environment>/<workspace>?path=<path>&max=<bytes>`: the workspace's file at
 *   `path`, `workspace/...` or `home/...`, its bytes (200); 403 where any part of its path is
 *   a symlink or it is no file, 404 where there is none, 413 where it is over `max` bytes.
 * - `PUT /files/<environment>/<workspace>?path=<path>`: writes the body, of MAX_WRITE_BYTES at
 *   most, as the workspace's file at `path`, its directories made (204); 403 where any part of
 *   its path is a symlink, 413 where the body is over MAX_WRITE_BYTES, 503 where the node's
 *   mount does not hold the workspace's quota yet.
 * - `GET /healthz`: 200 once it serves.
 *
 * A manifest is ./agent.ts Manifest.
 *
 * An agent that stops serves on until no restore reads the bucket through it (./store-proxy.ts
 * drained), as a resumed sandbox reads the rest of its pages there, then lets the checkpoints,
 * prepares and copies it runs end (./operations.ts).
 */
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Cause, Config, Effect, Option, Redacted, Schema, type Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import { BearerToken, sameToken } from "../shared/tokens.ts";
import {
  AbandonRequest,
  type CheckpointFailed,
  CheckpointRequest,
  CopyRequest,
  makeNodeAgent,
  type Manifest,
  type NodeAgent,
  type Prepared,
  type PrepareError,
  PrepareRequest,
  ResumeRequest,
  type SandboxUnsaveable,
  WorkspaceRef,
} from "./agent.ts";
import { type CacheError, DIGEST, makeSnapshotCache } from "./cache.ts";
import { makeHostCommands } from "./host.ts";
import { makeJuicefs } from "./juicefs.ts";
import { makeWorkspaces, Released, type Workspaces, WorkspaceRefused } from "./workspaces.ts";
import {
  BACKGROUND_READ_SECONDS_VARIABLE,
  CONTAINERD,
  GVISOR_RELEASES,
  META_URL_VARIABLE,
  NODE_AGENT_PORT,
  NODE_AGENT_TOKEN_VARIABLE,
  POD_IP_VARIABLE,
  RUNSC_ROOT,
  RESTORES_FROM_BUCKET_SINCE_VARIABLE,
  SNAPSHOT_CACHE_BYTES_VARIABLE,
  SNAPSHOT_CACHE_DIR,
  STORE_PROXY_PORT,
  NODE_AGENT_RUN_DIR,
  NODE_AGENT_RUN_DIR_VARIABLE,
  storeSocket,
  storeTokensFile,
  WORKSPACES_ROOT,
} from "./names.ts";
import { type Failure, makeOperations, type Operations } from "./operations.ts";
import { makeRunsc } from "./runsc.ts";
import { makeS3Bucket } from "./s3.ts";
import { makeStoreProxy, type StoreProxyError } from "./store-proxy.ts";
import { observeStorageUsage } from "./storage-usage.ts";

/** What serveNodeAgent is given: where it listens, the token it takes, and the agent and workspaces it serves. */
export interface NodeAgentServerOptions {
  readonly port: number;
  readonly host?: string;
  readonly token: string;
  readonly agent: NodeAgent;
  readonly workspaces: Workspaces;
}

/** The most a file written into a workspace may be: a received file's, Telegram's largest a bot gets, with room. */
export const MAX_WRITE_BYTES = 64 * 1024 ** 2;
/** The most a free pod's binding may be: a conversation's id and a token, with room. */
const MAX_BINDING_BYTES = 16 * 1024;

/** What `POST /workspaces` is given. */
const CreateRequest = Schema.Struct({ ...WorkspaceRef.fields, quotaBytes: Schema.Number });
/** What `POST /workspaces/clones` is given. */
const CloneRequest = Schema.Struct({ from: WorkspaceRef, to: WorkspaceRef, quotaBytes: Schema.Number });

/** What `POST /attach` is given. */
const AttachRequest = Schema.Struct({ ...WorkspaceRef.fields, pod: Schema.NonEmptyString, token: Schema.NonEmptyString, env: Schema.Record(Schema.String, Schema.String), made: Schema.optional(Schema.Boolean) });
/** What `POST /bindings` is given: a binding's text of MAX_BINDING_BYTES at most. */
const BindingRequest = Schema.Struct({
  namespace: Schema.NonEmptyString,
  pod: Schema.NonEmptyString,
  uid: Schema.Number,
  gid: Schema.Number,
  binding: Schema.NonEmptyString.check(Schema.isMaxLength(MAX_BINDING_BYTES)),
});
/** What `POST /detach` is given. */
const DetachRequest = Schema.Struct({ namespace: Schema.NonEmptyString, pod: Schema.NonEmptyString, containerIds: Schema.Array(Schema.NonEmptyString) });
/** What `POST /sync` is given. */
const SyncRequest = Schema.Struct({ namespace: Schema.NonEmptyString, pod: Schema.NonEmptyString, containerId: Schema.NonEmptyString });
/** What `POST /stop` is given. */
const StopRequest = Schema.Struct({ namespace: Schema.NonEmptyString, pod: Schema.NonEmptyString, containerId: Schema.NonEmptyString, hook: Schema.Array(Schema.String.check(Schema.isPattern(/^\S+$/u))).check(Schema.isMinLength(1)) });

/** The status each failure of the agent's is answered with; any other is 500. */
const STATUS: Readonly<Record<string, number>> = {
  NoSnapshot: 404,
  Unrestorable: 409,
  ContainerElsewhere: 409,
  SnapshotCorrupt: 422,
  S3Error: 502,
  AgentStopping: 503,
  SnapshotOutlived: 410,
  PoliciesNotInForce: 504,
  SandboxUnsaveable: 410,
  JuicefsError: 503,
};

const SNAPSHOT = /^\/snapshots\/([^/]+)\/([^/]+)$/u;
const SPENT = /^\/snapshots\/([^/]+)\/([^/]+)\/current\?snapshot=([^&]+)$/u;
const CHECKPOINT = /^\/checkpoints\/([^/]+)\/([^/]+)$/u;
const PREPARE = /^\/prepare\/([^/]+)\/([^/]+)$/u;
const ENVIRONMENT = /^\/snapshots\/([^/]+)$/u;
const CACHED = /^\/cache\/([^/]+)\/([^/?]+)(?:\?keep=([^&]*))?$/u;
const FILES = /^\/files\/([^/]+)\/([^/?]+)\?(.*)$/u;
const QUOTA = /^\/workspaces\/([^/]+)\/([^/]+)\/quota$/u;
const RELEASED = /^\/workspaces\/([^/]+)\/([^/]+)\/released$/u;
const WORKSPACE = /^\/workspaces\/([^/]+)\/([^/]+)$/u;
const WORKSPACES = /^\/workspaces\/([^/]+)$/u;

/** How long a 503 asks its caller to wait before it asks again, in seconds. */
const RETRY_AFTER_S = 2;

const answer = (status: number, body: unknown): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.jsonUnsafe(body, { status, ...(status === 503 ? { headers: { "retry-after": String(RETRY_AFTER_S) } } : {}) });

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
  workspaces,
}: NodeAgentServerOptions): Effect.fn.Return<{ readonly port: number }, HttpServerError.ServeError, Scope.Scope> {
  const scope = yield* Effect.scope;
  const checkpoints = yield* makeOperations<Manifest, CheckpointFailed | SandboxUnsaveable>(scope, failureOf);
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
      return body(PrepareRequest).pipe(Effect.flatMap((asked) => prepares.start(keyOf(asked), logged(agent.prepare(asked)))), Effect.as(started));
    }
    if (method === "GET" && prepare) return outcome(prepares, prepare);
    if (method === "POST" && url === "/copies") {
      return body(CopyRequest).pipe(Effect.flatMap(({ from, to }) => agent.copy(from, to)), Effect.map((manifest) => answer(200, manifest)));
    }
    if (method === "POST" && url === "/resume") {
      return body(ResumeRequest).pipe(Effect.flatMap(agent.resume), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "POST" && url === "/abandon") {
      return body(AbandonRequest).pipe(Effect.flatMap(agent.abandon), Effect.map((killed) => answer(200, { killed })));
    }
    if (method === "GET" && snapshot) {
      return workspaceOf(snapshot).pipe(
        Effect.flatMap(agent.current),
        Effect.map((manifest) => (manifest === null ? answer(404, { error: "the workspace has no snapshot" }) : answer(200, manifest))),
      );
    }
    if (method === "DELETE" && snapshot) return workspaceOf(snapshot).pipe(Effect.flatMap(agent.forget), Effect.as(HttpServerResponse.empty({ status: 204 })));
    const spent = SPENT.exec(url);
    if (method === "DELETE" && spent) {
      return workspaceOf(spent).pipe(
        Effect.flatMap((ref) => agent.spend(ref, decodeURIComponent(spent[3] ?? ""))),
        Effect.as(HttpServerResponse.empty({ status: 204 })),
      );
    }
    if (method === "POST" && url === "/collect") {
      return body(WorkspaceRef).pipe(Effect.flatMap(agent.collect), Effect.map((kept) => answer(200, { kept })));
    }
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
      const keep = decodeURIComponent(cached[3] ?? "").split(",").filter(Boolean);
      const other = keep.find((digest) => !DIGEST.test(digest));
      if (other !== undefined) return Effect.fail(new BadRequest({ message: `${other} is no image's digest` }));
      return workspaceOf(cached).pipe(
        Effect.flatMap((ref) => agent.discard(ref, keep)),
        Effect.map((discarded) => answer(200, { discarded })),
      );
    }
    if (method === "POST" && url === "/workspaces") {
      return body(CreateRequest).pipe(Effect.flatMap(({ quotaBytes, ...ref }) => workspaces.create(ref, quotaBytes)), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "POST" && url === "/workspaces/clones") {
      return body(CloneRequest).pipe(Effect.flatMap(({ from, to, quotaBytes }) => workspaces.clone(from, to, quotaBytes)), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "GET" && url === "/workspaces") return workspaces.environments.pipe(Effect.map((environments) => answer(200, { environments })));
    const listed = WORKSPACES.exec(url);
    if (method === "GET" && listed) {
      return workspaces.list(decodeURIComponent(listed[1] ?? "")).pipe(Effect.map((names) => answer(200, { workspaces: names })));
    }
    const workspace = WORKSPACE.exec(url);
    if (method === "DELETE" && workspace) return workspaceOf(workspace).pipe(Effect.flatMap(workspaces.remove), Effect.as(HttpServerResponse.empty({ status: 204 })));
    const quota = QUOTA.exec(url);
    if (method === "GET" && quota) {
      return workspaceOf(quota).pipe(
        Effect.flatMap(workspaces.quota),
        Effect.map((held) => (held === null ? answer(404, { error: "the workspace has no quota" }) : answer(200, held))),
      );
    }
    const released = RELEASED.exec(url);
    if (released && (method === "GET" || method === "PUT" || method === "DELETE")) {
      return workspaceOf(released).pipe(
        Effect.flatMap((ref) =>
          method === "GET"
            ? Effect.map(workspaces.released(ref), (record) => (record === null ? answer(404, { error: "the workspace records no release" }) : answer(200, record)))
            : Effect.flatMap(method === "PUT" ? body(Released) : Effect.succeed(null), (record) => workspaces.setReleased(ref, record)).pipe(Effect.as(HttpServerResponse.empty({ status: 204 })))
        ),
      );
    }
    if (method === "POST" && url === "/attach") {
      return body(AttachRequest).pipe(Effect.flatMap(({ pod, token: binding, env, made, ...ref }) => workspaces.attach(ref, pod, binding, env, { made: made === true })), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "POST" && url === "/bindings") {
      return body(BindingRequest).pipe(Effect.flatMap(({ namespace, pod, uid, gid, binding }) => workspaces.bindFree(namespace, pod, { uid, gid }, binding)), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "GET" && url === "/attachments") return workspaces.attachments.pipe(Effect.map((attachments) => answer(200, { attachments })));
    if (method === "POST" && url === "/sync") {
      return body(SyncRequest).pipe(Effect.flatMap(workspaces.sync), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "POST" && url === "/stop") {
      return body(StopRequest).pipe(Effect.flatMap(workspaces.stop), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    if (method === "POST" && url === "/detach") {
      return body(DetachRequest).pipe(Effect.flatMap(({ namespace, pod, containerIds }) => workspaces.detach(namespace, pod, containerIds)), Effect.as(HttpServerResponse.empty({ status: 204 })));
    }
    const files = FILES.exec(url);
    if (files && (method === "GET" || method === "PUT")) {
      const query = new URLSearchParams(files[3]);
      const path = query.get("path") ?? "";
      return workspaceOf(files).pipe(
        Effect.flatMap((ref) =>
          method === "GET"
            ? workspaces.read(ref, path, Number(query.get("max") ?? MAX_WRITE_BYTES)).pipe(Effect.map((bytes) => HttpServerResponse.uint8Array(bytes, { contentType: "application/octet-stream" })))
            : request.arrayBuffer.pipe(
              Effect.mapError(() => new BadRequest({ message: "the body could not be read" })),
              Effect.filterOrFail((bytes) => bytes.byteLength <= MAX_WRITE_BYTES, () => new WorkspaceRefused({ status: 413, reason: `a file written is ${MAX_WRITE_BYTES} bytes at most` })),
              Effect.flatMap((bytes) => workspaces.write(ref, path, new Uint8Array(bytes))),
              Effect.as(HttpServerResponse.empty({ status: 204 })),
            )
        ),
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
      const status = error._tag === "BadRequest" ? 400 : error._tag === "WorkspaceRefused" ? error.status : STATUS[error._tag] ?? 500;
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

/** Where the agent, of the node's processes, sees the node's filesystem: as its first process does. */
const NODE_ROOT = "/proc/1/root";
/** The most the node's cache keeps unless told: 5 GiB. */
const DEFAULT_CACHE_BYTES = 5 * 1024 ** 3;
/** How long a snapshot used is kept in the cache whatever its budget: a restore on this node reads it meanwhile. */
const EVICTION_GRACE_MS = 10 * 60_000;

/**
 * The node agent of the node it runs on (NODE_NAME), as its environment configures it:
 * the bucket (BucketConfig), the token the sandbox gateway presents (NODE_AGENT_TOKEN_VARIABLE), the
 * cache's budget in bytes (SNAPSHOT_CACHE_BYTES_VARIABLE), the first gVisor release that
 * restores straight from the bucket (RESTORES_FROM_BUCKET_SINCE_VARIABLE; none unless given,
 * every snapshot then fetched into the cache first), its pod's address (POD_IP_VARIABLE),
 * which it serves the bucket to its node's restores on (STORE_PROXY_PORT, ./store-proxy.ts),
 * as on its socket on the node, beside its restores' tokens (NODE_AGENT_RUN_DIR, unless
 * NODE_AGENT_RUN_DIR_VARIABLE says), and how long a resumed restore may read it there
 * (BACKGROUND_READ_SECONDS_VARIABLE). What a
 * stopped agent left half written in the cache is removed first. What stops it is logged,
 * and its stop waits on its restores' reads, as the module says.
 */
export const nodeAgent: Effect.Effect<void, Config.ConfigError | HttpServerError.ServeError | CacheError | StoreProxyError, Scope.Scope> = Effect.gen(function*() {
  const node = yield* Config.String("NODE_NAME");
  const token = yield* Config.Redacted(NODE_AGENT_TOKEN_VARIABLE);
  const budget = yield* Config.Int(SNAPSHOT_CACHE_BYTES_VARIABLE).pipe(Config.withDefault(DEFAULT_CACHE_BYTES));
  const restoresFromBucketSince = yield* Config.String(RESTORES_FROM_BUCKET_SINCE_VARIABLE).pipe(Config.withDefault(""));
  const backgroundReadSeconds = yield* Config.Int(BACKGROUND_READ_SECONDS_VARIABLE);
  const { secretAccessKey, ...bucket } = yield* BucketConfig;
  const cache = makeSnapshotCache({ root: SNAPSHOT_CACHE_DIR, budget, graceMs: EVICTION_GRACE_MS });
  yield* cache.clean;
  // The bucket, to the node's restores, on the pod's own address, which the node reaches.
  const podIP = yield* Config.String(POD_IP_VARIABLE);
  // And on its socket on the node, which a restore that asks for it dials whatever pod the
  // agent runs in, its tokens kept beside it, as an agent that starts again reads them back.
  const runDir = yield* Config.String(NODE_AGENT_RUN_DIR_VARIABLE).pipe(Config.withDefault(NODE_AGENT_RUN_DIR));
  const storeProxy = yield* makeStoreProxy({ store: { ...bucket, secretAccessKey: Redacted.value(secretAccessKey) }, host: podIP, port: STORE_PROXY_PORT, socket: storeSocket(runDir), tokensFile: storeTokensFile(runDir) });
  const runsc = makeRunsc({ releasesDir: GVISOR_RELEASES, containerdDir: CONTAINERD, root: RUNSC_ROOT });
  const host = makeHostCommands();
  const agent = yield* makeNodeAgent({
    node,
    nodeRoot: NODE_ROOT,
    runsc,
    cache,
    bucket: makeS3Bucket({ ...bucket, secretAccessKey: Redacted.value(secretAccessKey) }),
    storeProxy,
    host,
    restoresFromBucketSince,
    backgroundReadMs: backgroundReadSeconds * 1000,
  });
  // Workspace storage's metadata engine, where the agent is given it, which it sets quotas on alone.
  const metaUrl = yield* Config.option(Config.Redacted(META_URL_VARIABLE));
  const juicefs = Option.match(metaUrl, { onNone: () => null, onSome: (url) => makeJuicefs({ metaUrl: Redacted.value(url) }) });
  // What workspace storage holds, as the node's mount counts it, where the agent works with it.
  if (juicefs) yield* observeStorageUsage(WORKSPACES_ROOT);
  const served = yield* serveNodeAgent({ port: NODE_AGENT_PORT, token: Redacted.value(token), agent, workspaces: makeWorkspaces({ host, runsc, juicefs }) });
  // As it stops, before its API does: its restores' reads of the bucket done first.
  yield* Effect.addFinalizer(() =>
    Effect.gen(function*() {
      const reading = storeProxy.reading();
      if (reading > 0) yield* Effect.logInfo(`the node agent of ${node} stops once the restores reading the snapshots bucket through it are done: ${reading} read it now`);
      // Those through its socket wait for the agent that comes back, which reads their tokens back.
      yield* storeProxy.drained;
    })
  );
  yield* Effect.logInfo(`the node agent of ${node} serves on port ${served.port}, and the snapshots bucket to its node on ${new URL(storeProxy.endpoint).host}, its cache within ${budget} bytes`);
}).pipe(
  Effect.tapCause((cause) => Effect.logError(`Fatal error: ${Cause.pretty(cause)}`)),
  withLogScope("node-agent"),
);
