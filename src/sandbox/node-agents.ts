/**
 * The node agents (../node-agent/), as alasio calls them: each node's, found by its pod
 * there, as the profile's `snapshots.agents` select them, reached on its pod's address,
 * with the token alasio and they are given (ALASIO_NODE_AGENT_TOKEN).
 */
import type { V1Pod } from "@kubernetes/client-node";
import { Config, Duration, Effect, Redacted, Schedule, Schema } from "effect";
import { FetchHttpClient } from "effect/http";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { type CheckpointRequest, Manifest, POLICY_WAIT_MS, type ResumeRequest, type WorkspaceRef } from "../node-agent/agent.ts";
import { NODE_AGENT_TOKEN_VARIABLE, OPERATION_LIMIT_SECONDS } from "../node-agent/names.ts";

/** Where the node agents are: their pods' namespace and labels, and the port they serve on. */
export interface NodeAgentsLocation {
  readonly namespace: string;
  readonly selector: string;
  readonly port: number;
}

/** A node agent could not be asked, or refused: which node's, what, the status it answered with, and why. */
export class NodeAgentError extends Schema.TaggedError<NodeAgentError>()("NodeAgentError", {
  node: Schema.String,
  call: Schema.String,
  status: Schema.optional(Schema.Number),
  reason: Schema.String,
}) {
  override get message(): string {
    return `the node agent of ${this.node} could not ${this.call}: ${this.reason}`;
  }
}

/** What a node holds, as its agent says: its gVisor releases, with the CPU features each sees there, and the snapshots it has on its disk. */
export const Holdings = Schema.Struct({
  node: Schema.String,
  releases: Schema.Array(Schema.Struct({ release: Schema.String, cpuFeatures: Schema.Array(Schema.String) })),
  cached: Schema.Array(Schema.Struct({ environment: Schema.String, workspace: Schema.String, snapshot: Schema.String, bytes: Schema.Number, usedAt: Schema.String })),
});
export type Holdings = typeof Holdings.Type;

/** A snapshot prepared on a node: what it is, and where its image is there. */
const Prepared = Schema.Struct({ manifest: Manifest, imagePath: Schema.String });
export type Prepared = typeof Prepared.Type;

/** What an agent answers a call that fails with. */
const Refusal = Schema.Struct({ error: Schema.String });

/** What an agent answers a listing of an environment's snapshots, and a discard, with. */
const Listed = Schema.Struct({ workspaces: Schema.Array(Schema.String) });
const Discarded = Schema.Struct({ discarded: Schema.Array(Schema.String) });

/** How an agent says a checkpoint or a prepare of a workspace stands (../node-agent/operations.ts), its value as `value` reads it. */
const OutcomeOf = <A, I>(value: Schema.Codec<A, I>) =>
  Schema.Union([
    Schema.Struct({ state: Schema.Literal("running") }),
    Schema.Struct({ state: Schema.Literal("done"), value }),
    Schema.Struct({ state: Schema.Literal("failed"), status: Schema.Number, error: Schema.String }),
  ]);

/** The node agents; see makeNodeAgents. */
export interface NodeAgents {
  /** The nodes with a ready agent on them. */
  readonly nodes: Effect.Effect<readonly string[], KubeApiError>;
  /** Snapshots the sandbox of the container `request` names, on `node`, and commits it: its manifest. The sandbox ends. */
  readonly checkpoint: (node: string, request: CheckpointRequest) => Effect.Effect<Manifest, NodeAgentError | KubeApiError>;
  /** The workspace's current snapshot, prepared on `node` for a restore there. */
  readonly prepare: (node: string, workspace: WorkspaceRef) => Effect.Effect<Prepared, NodeAgentError | KubeApiError>;
  /** The workspace's current snapshot, or null when it has none, as an agent reads it. */
  readonly current: (workspace: WorkspaceRef) => Effect.Effect<Manifest | null, NodeAgentError | KubeApiError>;
  /** Makes the current snapshot of `from` that of `to` too: `to`'s manifest. An agent refuses one of `from` without a snapshot with 404. */
  readonly copy: (from: WorkspaceRef, to: WorkspaceRef) => Effect.Effect<Manifest, NodeAgentError | KubeApiError>;
  /** Forgets the workspace's current snapshot. */
  readonly forget: (workspace: WorkspaceRef) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Resumes the sandbox `request` names, restored paused in its pod on `node`, once the
   * pod's network policies are in force there. An agent refuses one whose policies are
   * not in force within its wait with 504, the sandbox left paused.
   */
  readonly resume: (node: string, request: ResumeRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** What `node` holds. */
  readonly holdings: (node: string) => Effect.Effect<Holdings, NodeAgentError | KubeApiError>;
  /** The workspaces of `environment` the bucket holds anything of. */
  readonly workspaces: (environment: string) => Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /** Has `node`'s cache discard the workspace's snapshots but `keep` that no restore there may be reading: their ids. */
  readonly discard: (node: string, workspace: WorkspaceRef, keep: string | null) => Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /**
   * Attaches the workspace's files into the directory of the sandbox pool pod `pod` on
   * `node`, its binding's `token` and the variables `env` beside them. An agent refuses one
   * whose directory holds a mount with 409.
   */
  readonly attach: (node: string, request: AttachRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Detaches what is attached into `pod`'s directory on `node`. An agent refuses while any of
   * `containerIds`, the pod's sandbox's containers by their runtime's ids, runs, with 409.
   */
  readonly detach: (node: string, pod: string, containerIds: readonly string[]) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** The workspace's file at `path`, `workspace/...` or `home/...`, of `maxBytes` at most, as any agent reads it. */
  readonly readFile: (workspace: WorkspaceRef, path: string, maxBytes: number) => Effect.Effect<Uint8Array, NodeAgentError | KubeApiError>;
  /** Writes `bytes` as the workspace's file at `path`, as any agent writes it. */
  readonly writeFile: (workspace: WorkspaceRef, path: string, bytes: Uint8Array) => Effect.Effect<void, NodeAgentError | KubeApiError>;
}

/** What an attach is of: the workspace, the pool pod it goes into, and the binding's token and the variables its bayma runs with beside it. */
export interface AttachRequest extends WorkspaceRef {
  readonly pod: string;
  readonly token: string;
  readonly env: Readonly<Record<string, string>>;
}

/** How long a checkpoint, a prepare or a copy may take. */
const LONG_CALL = `${OPERATION_LIMIT_SECONDS} seconds` satisfies Duration.Input;
/** How often alasio asks an agent how a checkpoint or a prepare it started stands. */
const OPERATION_POLL: Duration.Input = "500 millis";
/** How long a resume may take: the agent's wait for the pod's network policies, then runsc's resume of its containers. */
const RESUME_CALL: Duration.Input = Duration.sum(Duration.millis(POLICY_WAIT_MS), Duration.seconds(30));
/** How long any other call may take. */
const SHORT_CALL: Duration.Input = "30 seconds";

/** A call of an agent: what, with what (JSON, or bytes as they are), for how long; with `absent`, a 404 is an answer, not a failure. */
interface AgentCall {
  readonly method: "GET" | "POST" | "PUT" | "DELETE";
  readonly path: string;
  readonly body?: unknown;
  readonly bytes?: Uint8Array;
  readonly timeout: Duration.Input;
  readonly absent?: true;
}

/** Whether `pod` runs, ready, with an address. */
const readyAt = (pod: V1Pod): string | null =>
  pod.status?.podIP && pod.status.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ? pod.status.podIP : null;

/** The node agents at `location`, on the KubeClient, reached with FetchHttpClient.Fetch. */
export const makeNodeAgents = Effect.fnUntraced(function*(location: NodeAgentsLocation): Effect.fn.Return<NodeAgents, Config.ConfigError, KubeClient> {
  const kube = yield* KubeClient;
  const fetch = yield* FetchHttpClient.Fetch;
  const token = yield* Config.Redacted(NODE_AGENT_TOKEN_VARIABLE);

  /** Each node's agent's address, by its node. */
  const addresses: Effect.Effect<ReadonlyMap<string, string>, KubeApiError> = kube.list("v1", "Pod", location.namespace, location.selector).pipe(
    // Pods, as the API server lists them.
    Effect.map((pods) =>
      new Map((pods as readonly V1Pod[]).flatMap((pod) => {
        const address = readyAt(pod);
        return address && pod.spec?.nodeName ? [[pod.spec.nodeName, `http://${address}:${location.port}`] as const] : [];
      }))
    ),
  );

  /** A call of the agent of `node`, `what` it is asked as a sentence ends: its answer's status, bytes and text, or why it failed. */
  const send = (node: string, what: string, request: AgentCall): Effect.Effect<{ readonly status: number; readonly bytes: Uint8Array; readonly text: string }, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      const failed = (reason: string, status?: number) => new NodeAgentError({ node, call: what, reason, ...(status === undefined ? {} : { status }) });
      const base = (yield* addresses).get(node);
      if (!base) return yield* failed("no agent is ready there");
      const { status, ok, bytes } = yield* Effect.tryPromise({
        try: async (signal) => {
          const response = await fetch(`${base}${request.path}`, {
            method: request.method,
            headers: {
              authorization: `Bearer ${Redacted.value(token)}`,
              ...(request.body !== undefined ? { "content-type": "application/json" } : request.bytes !== undefined ? { "content-type": "application/octet-stream" } : {}),
            },
            ...(request.body !== undefined ? { body: JSON.stringify(request.body) } : request.bytes !== undefined ? { body: request.bytes } : {}),
            signal: AbortSignal.any([signal, AbortSignal.timeout(Duration.toMillis(request.timeout))]),
          });
          return { status: response.status, ok: response.ok, bytes: new Uint8Array(await response.arrayBuffer()) };
        },
        catch: (cause) => failed(cause instanceof Error ? cause.message : String(cause)),
      });
      const text = new TextDecoder().decode(bytes);
      if (ok || (status === 404 && request.absent)) return { status, bytes, text };
      const refusal = Schema.decodeUnknownOption(Schema.fromJsonString(Refusal))(text);
      return yield* failed(refusal._tag === "Some" ? refusal.value.error : `it answered ${status}`, status);
    });

  /** What the agent of `node` answers `request` with, as `schema` reads it. */
  const ask = <A, I>(node: string, what: string, request: AgentCall, schema: Schema.Codec<A, I>): Effect.Effect<A, NodeAgentError | KubeApiError> =>
    Effect.flatMap(send(node, what, request), ({ text }) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(text).pipe(
        Effect.mapError((error) => new NodeAgentError({ node, call: what, reason: `its answer is not one: ${error.message}` })),
      ));

  /** The path of a workspace's snapshot. */
  const snapshotPath = ({ environment, workspace }: WorkspaceRef) => `/snapshots/${encodeURIComponent(environment)}/${encodeURIComponent(workspace)}`;

  /**
   * What the agent of `node` comes to as it runs the operation of `path` (a checkpoint, a
   * prepare) of the workspace `request` names, which it runs whatever becomes of this
   * call: started, then asked how it stands until it has ended, within LONG_CALL.
   */
  const operate = <A, I>(node: string, what: string, path: string, request: WorkspaceRef, value: Schema.Codec<A, I>): Effect.Effect<A, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      yield* send(node, what, { method: "POST", path, body: request, timeout: SHORT_CALL });
      const standing = { method: "GET", path: `${path}/${encodeURIComponent(request.environment)}/${encodeURIComponent(request.workspace)}`, timeout: SHORT_CALL } as const;
      const ended = yield* ask(node, what, standing, OutcomeOf(value)).pipe(
        Effect.repeat({ schedule: Schedule.spaced(OPERATION_POLL), while: ({ state }) => state === "running" }),
        Effect.timeoutOrElse({ duration: LONG_CALL, orElse: () => Effect.fail(new NodeAgentError({ node, call: what, reason: `it had not ended within ${LONG_CALL}` })) }),
      );
      if (ended.state === "done") return ended.value;
      // Repeated while it ran, it ran no more.
      if (ended.state === "running") return yield* Effect.die("an operation ended running");
      return yield* new NodeAgentError({ node, call: what, reason: ended.error, status: ended.status });
    });

  /** Any node with a ready agent: each reads, copies and forgets in the bucket as well as another. */
  const anyNode: Effect.Effect<string, NodeAgentError | KubeApiError> = Effect.flatMap(addresses, (known) => {
    const [first] = known.keys();
    return first === undefined ? Effect.fail(new NodeAgentError({ node: "any node", call: "be reached", reason: "no agent is ready" })) : Effect.succeed(first);
  });

  /** The path of a workspace's file. */
  const filePath = ({ environment, workspace }: WorkspaceRef, path: string) => `/files/${encodeURIComponent(environment)}/${encodeURIComponent(workspace)}?path=${encodeURIComponent(path)}`;

  return {
    nodes: Effect.map(addresses, (known) => [...known.keys()]),
    attach: (node, request) => send(node, `attach ${request.environment}/${request.workspace} into ${request.pod}`, { method: "POST", path: "/attach", body: request, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    detach: (node, pod, containerIds) => send(node, `detach what is attached into ${pod}`, { method: "POST", path: "/detach", body: { pod, containerIds }, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    readFile: (workspace, path, maxBytes) =>
      Effect.flatMap(anyNode, (node) => send(node, `read ${path} of ${workspace.environment}/${workspace.workspace}`, { method: "GET", path: `${filePath(workspace, path)}&max=${maxBytes}`, timeout: SHORT_CALL })).pipe(
        Effect.map(({ bytes }) => bytes),
      ),
    writeFile: (workspace, path, bytes) =>
      Effect.flatMap(anyNode, (node) => send(node, `write ${path} of ${workspace.environment}/${workspace.workspace}`, { method: "PUT", path: filePath(workspace, path), bytes, timeout: SHORT_CALL })).pipe(Effect.asVoid),
    checkpoint: (node, request) => operate(node, `snapshot ${request.environment}/${request.workspace}`, "/checkpoints", request, Manifest),
    prepare: (node, workspace) => operate(node, `prepare ${workspace.environment}/${workspace.workspace}'s snapshot`, "/prepare", workspace, Prepared),
    current: (workspace) =>
      Effect.gen(function*() {
        const node = yield* anyNode;
        const what = `read ${workspace.environment}/${workspace.workspace}'s snapshot`;
        const { status, text } = yield* send(node, what, { method: "GET", path: snapshotPath(workspace), timeout: SHORT_CALL, absent: true });
        if (status === 404) return null;
        return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Manifest))(text).pipe(
          Effect.mapError((error) => new NodeAgentError({ node, call: what, reason: `its answer is not one: ${error.message}` })),
        );
      }),
    copy: (from, to) =>
      Effect.flatMap(anyNode, (node) =>
        ask(
          node,
          `copy ${from.environment}/${from.workspace}'s snapshot to ${to.environment}/${to.workspace}`,
          { method: "POST", path: "/copies", body: { from, to }, timeout: LONG_CALL },
          Manifest,
        )),
    forget: (workspace) =>
      Effect.flatMap(anyNode, (node) => send(node, `forget ${workspace.environment}/${workspace.workspace}'s snapshot`, { method: "DELETE", path: snapshotPath(workspace), timeout: SHORT_CALL })).pipe(
        Effect.asVoid,
      ),
    resume: (node, request) =>
      send(node, `resume ${request.environment}/${request.workspace}'s sandbox`, { method: "POST", path: "/resume", body: request, timeout: RESUME_CALL }).pipe(Effect.asVoid),
    holdings: (node) => ask(node, "say what it holds", { method: "GET", path: "/holdings", timeout: SHORT_CALL }, Holdings),
    workspaces: (environment) =>
      Effect.flatMap(anyNode, (node) =>
        ask(node, `list ${environment}'s snapshots`, { method: "GET", path: `/snapshots/${encodeURIComponent(environment)}`, timeout: SHORT_CALL }, Listed)).pipe(
          Effect.map(({ workspaces }) => workspaces),
        ),
    discard: (node, workspace, keep) =>
      ask(
        node,
        `discard ${workspace.environment}/${workspace.workspace}'s cached snapshots`,
        { method: "DELETE", path: `/cache/${encodeURIComponent(workspace.environment)}/${encodeURIComponent(workspace.workspace)}?keep=${encodeURIComponent(keep ?? "")}`, timeout: SHORT_CALL },
        Discarded,
      ).pipe(Effect.map(({ discarded }) => discarded)),
  };
});
