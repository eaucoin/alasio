/**
 * The node agents (../node-agent/), as the sandbox gateway calls them: each node's, found by
 * its pod there, as the profile's `snapshots.agents` select them, reached on its pod's
 * address, with the token the gateway and they are given (ALASIO_NODE_AGENT_TOKEN).
 *
 * The agents roll node by node as alasio is upgraded, so for a while a node's may be of the
 * release before the gateway's, as its pod's version label says. What every release's agents
 * answer alike goes to whichever serves the node; what this release's answer otherwise, a
 * restore's choice of its node, its prepare and its resume, goes to this release's alone
 * (ownRelease), as one to an agent of another is never sent.
 */
import type { V1DaemonSet, V1Pod } from "@kubernetes/client-node";
import { Config, Duration, Effect, Exit, Redacted, Schedule, Schema, type Scope } from "effect";
import { FetchHttpClient } from "effect/http";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { daemonPodNode } from "../kube/daemon-pods.ts";
import type { Gofer } from "../node-agent/names.ts";
import { type AbandonRequest, type CheckpointRequest, type Killed, Manifest, type PrepareRequest, type ResumeRequest, resumeMs, type WorkspaceRef } from "../node-agent/agent.ts";
import { HOLDINGS_WAIT_MS, NODE_AGENT_TOKEN_VARIABLE, OPERATION_LIMIT_SECONDS } from "../node-agent/names.ts";
import { type StandbyRestoreRequest, StandbyRestored } from "../node-agent/standby.ts";
import { type PodOwner, Released, type StopRequest, type SyncRequest } from "../node-agent/workspaces.ts";

/**
 * Where the node agents are: their pods' namespace and labels, their DaemonSet's name, and the port they serve on; and the
 * release the gateway is of, as their pods' version label names one.
 */
export interface NodeAgentsLocation {
  readonly namespace: string;
  readonly selector: string;
  readonly daemonSet: string;
  readonly port: number;
  readonly release: string;
}

/**
 * A node agent could not be asked, or refused: which node's, what, the status it answered with, when it asked to be asked
 * again (its Retry-After), why, and whether the call was never sent, as one to a node with no agent ready there is not.
 */
export class NodeAgentError extends Schema.TaggedError<NodeAgentError>()("NodeAgentError", {
  node: Schema.String,
  call: Schema.String,
  status: Schema.optional(Schema.Number),
  retryAfter: Schema.optional(Schema.String),
  reason: Schema.String,
  unsent: Schema.optional(Schema.Boolean),
}) {
  override get message(): string {
    return `the node agent of ${this.node} could not ${this.call}: ${this.reason}`;
  }
}

/** What a node holds, as its agent says: its gVisor releases, with the CPU features each sees there, and the workspaces whose images it has on its disk, by digest. */
export const Holdings = Schema.Struct({
  node: Schema.String,
  releases: Schema.Array(Schema.Struct({ release: Schema.String, cpuFeatures: Schema.Array(Schema.String) })),
  cached: Schema.Array(Schema.Struct({ environment: Schema.String, workspace: Schema.String, images: Schema.Array(Schema.String), bytes: Schema.Number, usedAt: Schema.String })),
});
export type Holdings = typeof Holdings.Type;

/** What a node holds, and the tag its agent says it with, which changes as it does (../node-agent/agent.ts holdingsTag). */
interface TaggedHoldings {
  readonly holdings: Holdings;
  readonly tag: string;
}

/** A snapshot prepared on a node: what it is, and where its image is there. */
const Prepared = Schema.Struct({ manifest: Manifest, imagePath: Schema.String, source: Schema.Literals(["cache", "bucket"]) });
export type Prepared = typeof Prepared.Type;

/** What an agent answers a call that fails with. */
const Refusal = Schema.Struct({ error: Schema.String });

/** What an agent answers a look at what is attached on its node with. */
const Attachments = Schema.Struct({ attachments: Schema.Array(Schema.Struct({ namespace: Schema.String, pod: Schema.String, reachable: Schema.Boolean })) });

/** What an agent answers a workspace's quota with. */
const Quota = Schema.Struct({ capacityBytes: Schema.Number, usedBytes: Schema.Number });
/** What an agent answers a listing of an environment's snapshots, a collection, and a discard, with. */
const Listed = Schema.Struct({ workspaces: Schema.Array(Schema.String) });
const ListedEnvironments = Schema.Struct({ environments: Schema.Array(Schema.String) });
const Collected = Schema.Struct({ kept: Schema.Array(Schema.String) });
const Discarded = Schema.Struct({ discarded: Schema.Boolean });
/** What an agent answers an abandon with: the processes of runsc's it killed. */
const Abandoned = Schema.Struct({ killed: Schema.Array(Schema.Struct({ containerId: Schema.String, pid: Schema.Number })) });

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
  /**
   * The nodes with a ready agent of the gateway's release on them, which alone restore; those whose ready agent is of
   * another; those an agent's pod is on, or is to be on, of which none is ready, as one starting, crashing, not yet scheduled
   * or on a node that is not Ready; and how many more nodes the agents' DaemonSet runs a pod on, or is to, have none listed
   * yet, as one whose old pod is gone and whose new one is not yet made.
   */
  readonly releaseNodes: Effect.Effect<{ readonly nodes: readonly string[]; readonly others: readonly string[]; readonly unready: readonly string[]; readonly unseen: number }, KubeApiError>;
  /** Snapshots the sandbox of the container `request` names, on `node`, and commits it: its manifest. The sandbox ends. */
  readonly checkpoint: (node: string, request: CheckpointRequest) => Effect.Effect<Manifest, NodeAgentError | KubeApiError>;
  /**
   * The workspace's snapshot of `manifest`, its current one as alasio read it, prepared on
   * `node` for a restore there, its checkpoint gofer as `gofer` says, where it says; the
   * agent's own way where not. Refused, with no status, as what may pass, where the agent
   * answered with a prepare of another snapshot of the workspace's, which ran as it was asked;
   * and, unsent, where the node's agent is not of the gateway's release.
   */
  readonly prepare: (node: string, workspace: WorkspaceRef, manifest: Manifest, gofer?: Gofer) => Effect.Effect<Prepared, NodeAgentError | KubeApiError>;
  /** The workspace's current snapshot, or null when it has none, as an agent reads it. */
  readonly current: (workspace: WorkspaceRef) => Effect.Effect<Manifest | null, NodeAgentError | KubeApiError>;
  /** Makes the current snapshot of `from` that of `to` too: `to`'s manifest. An agent refuses one of `from` without a snapshot with 404. */
  readonly copy: (from: WorkspaceRef, to: WorkspaceRef) => Effect.Effect<Manifest, NodeAgentError | KubeApiError>;
  /** Forgets the workspace's snapshots. */
  readonly forget: (workspace: WorkspaceRef) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Makes the workspace's current snapshot current no more, if it is `snapshot` still, by the
   * agent of `node`, the one its sandbox runs on: one its sandbox was restored from, or one
   * taken live whose exec has ended. Its images go on as the layers of its next.
   */
  readonly spend: (node: string, workspace: WorkspaceRef, snapshot: string) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** Collects what of the workspace's in the bucket no snapshot needs any more: the digests of the images kept. */
  readonly collect: (workspace: WorkspaceRef) => Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /**
   * Resumes the sandbox `request` names, restored paused in its pod on `node`, asked as the
   * pod is made: once the pod runs, and its network policies are in force there, from images
   * of `bytes` together, which its resume may wait on (resumeMs); answered once the pod accepts
   * connections on its bayma's port, as kubelet's readiness probe would find it ready. An agent
   * refuses one whose pod does not run, or whose policies are not in force, within its wait
   * with 504, the sandbox left paused. Refused unsent where the node's agent is not of the
   * gateway's release.
   */
  readonly resume: (node: string, request: ResumeRequest, bytes: number) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Restores the workspace's image `request` names, as the agent of `node` prepared it, into
   * the standby pod it names there, paused, until resumed as a restore's pod is: its containers,
   * and how long each took. An agent refuses one whose standby takes no restore with 409,
   * nothing of it run; one whose restore failed, which ends its sandbox, fails with 500.
   */
  readonly restoreStandby: (node: string, request: StandbyRestoreRequest) => Effect.Effect<StandbyRestored, NodeAgentError | KubeApiError>;
  /**
   * Abandons the restore `request` names, into its pod on `node`, of the uid it names,
   * refused before its sandbox resumed: the agent kills what runsc runs for the pod's containers there, so
   * that its teardown waits on none of it. What it killed.
   */
  readonly abandon: (node: string, request: AbandonRequest) => Effect.Effect<readonly Killed[], NodeAgentError | KubeApiError>;
  /** What `node` holds, as its agent says it now. */
  readonly holdings: (node: string) => Effect.Effect<Holdings, NodeAgentError | KubeApiError>;
  /**
   * What `node` holds, as its agent last said it: kept as it changes, by a request its agent
   * answers once it does (watchHoldings), so a restore's choice of its node asks no agent;
   * asked of its agent where none is kept yet, as of an agent that has just started; refused
   * unsent where it is not of the gateway's release.
   */
  readonly held: (node: string) => Effect.Effect<Holdings, NodeAgentError | KubeApiError>;
  /** The workspaces of `environment` the bucket holds anything of. */
  readonly workspaces: (environment: string) => Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /** Has `node`'s cache discard the workspace's images but `keep`, unless a restore there may be reading them: whether it did. */
  readonly discard: (node: string, workspace: WorkspaceRef, keep: readonly string[]) => Effect.Effect<boolean, NodeAgentError | KubeApiError>;
  /**
   * Attaches the workspace's files into the directory of the sandbox pool pod `pod` on
   * `node`, its binding's `token` and the variables `env` beside them. An agent refuses one
   * whose directory holds a mount with 409.
   */
  readonly attach: (node: string, request: AttachRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Gives the free pod `request.pod` of a harness pool, on `node`, its binding, its user's and
   * group's. An agent refuses one that has a binding already, as a pod serves one, with 409.
   */
  readonly bindFree: (node: string, request: BindingRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Detaches what is attached into `pod`'s directory on `node`. An agent refuses while any of
   * `containerIds`, the pod's sandbox's containers by their runtime's ids, runs, with 409.
   */
  readonly detach: (node: string, namespace: string, pod: string, containerIds: readonly string[]) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** The workspace's file at `path`, `workspace/...` or `home/...`, of `maxBytes` at most, as any agent reads it. */
  readonly readFile: (workspace: WorkspaceRef, path: string, maxBytes: number) => Effect.Effect<Uint8Array, NodeAgentError | KubeApiError>;
  /** Makes the workspace, its directory and its quota of `quotaBytes`, through any agent, each sees workspace storage as another; again, sets its quota anew. */
  readonly createWorkspace: (workspace: WorkspaceRef, quotaBytes: number) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** Makes the workspace `to` a clone of `from`, of `quotaBytes`, through any agent. */
  readonly cloneWorkspace: (from: WorkspaceRef, to: WorkspaceRef, quotaBytes: number) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** Deletes the workspace, its quota with it, through any agent; one not there is not an error. */
  readonly removeWorkspace: (workspace: WorkspaceRef) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** The workspaces of `environment` there are, by their names, through any agent. */
  readonly listWorkspaces: (environment: string) => Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /** The environments any workspace is of, by their names, through any agent. */
  readonly listEnvironments: Effect.Effect<readonly string[], NodeAgentError | KubeApiError>;
  /**
   * Has the bayma of the bound pod `request.pod`, on `node`, go on with the stop it holds,
   * by its hook `request.hook` (`sandbox-hook stop`), which its node's agent runs in its
   * container; an agent refuses one that is not the bayma of a pool's pod there with 403.
   */
  readonly stop: (node: string, request: StopRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /**
   * Has the sandbox of the bound pod `request.pod`'s bayma, on `node`, write what it holds of
   * its files through to the node's mount, by `sync`, which its node's agent runs in its
   * container; refused as a stop is.
   */
  readonly sync: (node: string, request: SyncRequest) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** The pods of `node` a workspace is attached into, each by whether what is attached is reachable still. */
  readonly attachments: (node: string) => Effect.Effect<readonly { readonly namespace: string; readonly pod: string; readonly reachable: boolean }[], NodeAgentError | KubeApiError>;
  /** The snapshot the workspace was last released into, as any agent reads its record of it; null where none is recorded. */
  readonly released: (workspace: WorkspaceRef) => Effect.Effect<Released | null, NodeAgentError | KubeApiError>;
  /** Records the snapshot the workspace is released into, or none, through any agent. */
  readonly setReleased: (workspace: WorkspaceRef, released: Released | null) => Effect.Effect<void, NodeAgentError | KubeApiError>;
  /** The workspace's quota, and what it holds, through any agent; null where it has none. */
  readonly quota: (workspace: WorkspaceRef) => Effect.Effect<{ readonly capacityBytes: number; readonly usedBytes: number } | null, NodeAgentError | KubeApiError>;
  /** Writes `bytes` as the workspace's file at `path`, as any agent writes it. */
  readonly writeFile: (workspace: WorkspaceRef, path: string, bytes: Uint8Array) => Effect.Effect<void, NodeAgentError | KubeApiError>;
}

/** What an attach is of: the workspace, the pool pod it goes into, and the binding's token and the variables its bayma runs with beside it. */
export interface AttachRequest extends WorkspaceRef {
  readonly pod: string;
  readonly token: string;
  readonly env: Readonly<Record<string, string>>;
  /** Whether the pod's directory is made now, for a restore's pod made once its workspace is there, not one kubelet made. */
  readonly made?: boolean;
}

/** What a free pod's binding is: the pod, its user and group, and the text of its binding's file. */
export interface BindingRequest extends PodOwner {
  readonly namespace: string;
  readonly pod: string;
  readonly binding: string;
}

/** How long a checkpoint, a prepare or a copy may take. */
const LONG_CALL = `${OPERATION_LIMIT_SECONDS} seconds` satisfies Duration.Input;
/**
 * How often alasio asks an agent how a checkpoint or a prepare it started stands, where its
 * start was not answered as it ended: 25 ms after it last asked, then twice as long each time,
 * half a second at most, as a prepare from the bucket may end within a second or two, which a
 * restore waits on, and a checkpoint takes seconds.
 */
const OPERATION_POLLS = Schedule.min([Schedule.exponential("25 millis"), Schedule.spaced("500 millis")]);
/**
 * How long the agents' addresses, as their pods were listed, and their DaemonSet's count of nodes, are taken as they are: a
 * restore's calls of its node's agent, a dozen within a second or two, list them once or
 * twice, and an agent started again on another address is found within it.
 */
const ADDRESSES_TTL: Duration.Input = "1 second";
/** How long any other call may take. */
const SHORT_CALL: Duration.Input = "30 seconds";
/** How long a request for what a node holds, asked as of a tag, may take: as long as its agent holds it, and as long as any other call beside. */
const HOLDINGS_CALL: Duration.Input = Duration.millis(HOLDINGS_WAIT_MS + Duration.toMillis(SHORT_CALL));
/** How long a sandbox's sync may take: what it writes through, which the agent waits a minute for. */
const SYNC_CALL: Duration.Input = "90 seconds";

/**
 * A call of an agent: what, with what (JSON, or bytes as they are), for how long; with
 * `absent`, a 404 is an answer, not a failure; with `seen`, asked as of what the tag names
 * (If-None-Match), a 304 is an answer too; with `ownRelease`, sent to an agent of the
 * gateway's release alone.
 */
interface AgentCall {
  readonly method: "GET" | "POST" | "PUT" | "DELETE";
  readonly path: string;
  readonly body?: unknown;
  readonly bytes?: Uint8Array;
  readonly timeout: Duration.Input;
  readonly absent?: true;
  readonly seen?: string;
  readonly ownRelease?: true;
}

/** The label an agent's pod says its release with. */
const VERSION_LABEL = "app.kubernetes.io/version";

/** A node's ready agent: where it is reached, and its release, as its pod's VERSION_LABEL says it; none where it says none. */
interface Agent {
  readonly base: string;
  readonly release: string | null;
}

/** Whether `pod` runs, ready, with an address. */
const readyAt = (pod: V1Pod): string | null =>
  pod.status?.podIP && pod.status.conditions?.some(({ type, status }) => type === "Ready" && status === "True") ? pod.status.podIP : null;

/** The node agents at `location`, on the KubeClient, reached with FetchHttpClient.Fetch, what each node holds watched while the scope lasts. */
export const makeNodeAgents = Effect.fnUntraced(function*(location: NodeAgentsLocation): Effect.fn.Return<NodeAgents, Config.ConfigError, KubeClient | Scope.Scope> {
  const kube = yield* KubeClient;
  const fetch = yield* FetchHttpClient.Fetch;
  const token = yield* Config.Redacted(NODE_AGENT_TOKEN_VARIABLE);
  const scope = yield* Effect.scope;

  /** The agents' pods, as the API server lists them. */
  const pods: Effect.Effect<readonly V1Pod[], KubeApiError> = Effect.map(kube.list("v1", "Pod", location.namespace, location.selector), (found) => found as readonly V1Pod[]);
  /**
   * Of the agents' pods `found`, each node's ready agent, by its node; and the nodes an agent's pod is on, or is to be on, of
   * which none is ready, as one starting, crashing, not yet scheduled or on a node that is not Ready is.
   */
  const agentsOf = (found: readonly V1Pod[]): { readonly ready: ReadonlyMap<string, Agent>; readonly unready: ReadonlySet<string> } => {
    const ready = new Map(found.flatMap((pod) => {
      const address = readyAt(pod);
      const agent: Agent = { base: `http://${address}:${location.port}`, release: pod.metadata?.labels?.[VERSION_LABEL] ?? null };
      return address && pod.spec?.nodeName ? [[pod.spec.nodeName, agent] as const] : [];
    }));
    return { ready, unready: new Set(found.map(daemonPodNode).filter((node): node is string => node !== null && !ready.has(node))) };
  };
  /** What is read within ADDRESSES_TTL is taken as it is; a read that failed is not kept. */
  const briefly = <A>(read: Effect.Effect<A, KubeApiError>) => Effect.cachedWithTTL(read, (exit) => (Exit.isSuccess(exit) ? ADDRESSES_TTL : Duration.zero));
  /** Each node's ready agent, by its node, as their pods were listed (agentsOf), which every call is sent to: of the pods alone. */
  const addresses: Effect.Effect<ReadonlyMap<string, Agent>, KubeApiError> = yield* briefly(Effect.map(pods, (found) => agentsOf(found).ready));
  /**
   * The agents, as their pods were listed (agentsOf), and how many nodes their DaemonSet runs a pod on, or is to, as it says
   * (none where there is no DaemonSet): the pods and it read together, so the count is of the moment the pods are, for a
   * restore's choice of its node alone, which a DaemonSet that cannot be read fails, and nothing else.
   */
  const counted: Effect.Effect<{ readonly ready: ReadonlyMap<string, Agent>; readonly unready: ReadonlySet<string>; readonly desired: number }, KubeApiError> = yield* briefly(
    Effect.map(
      Effect.all([pods, kube.read("apps/v1", "DaemonSet", location.namespace, location.daemonSet)], { concurrency: 2 }),
      ([found, set]) => ({ ...agentsOf(found), desired: (set as V1DaemonSet | null)?.status?.desiredNumberScheduled ?? 0 }),
    ),
  );

  /** A call of the agent of `node`, `what` it is asked as a sentence ends: its answer's status, bytes, text and tag (its ETag), or why it failed. */
  const send = (node: string, what: string, request: AgentCall): Effect.Effect<{ readonly status: number; readonly bytes: Uint8Array; readonly text: string; readonly tag: string | null }, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      const failed = (reason: string, status?: number, retryAfter?: string | null) =>
        new NodeAgentError({ node, call: what, reason, ...(status === undefined ? {} : { status }), ...(retryAfter === undefined || retryAfter === null ? {} : { retryAfter }) });
      const agent = (yield* addresses).get(node);
      if (!agent) return yield* new NodeAgentError({ node, call: what, reason: "no agent is ready there", unsent: true });
      if (request.ownRelease && agent.release !== location.release) {
        return yield* new NodeAgentError({ node, call: what, reason: `its agent is of ${agent.release === null ? "no release it says" : `release ${agent.release}`}, not this gateway's ${location.release}`, unsent: true });
      }
      const { base } = agent;
      const { status, ok, bytes, retryAfter, tag } = yield* Effect.tryPromise({
        try: async (signal) => {
          const response = await fetch(`${base}${request.path}`, {
            method: request.method,
            headers: {
              authorization: `Bearer ${Redacted.value(token)}`,
              ...(request.body !== undefined ? { "content-type": "application/json" } : request.bytes !== undefined ? { "content-type": "application/octet-stream" } : {}),
              ...(request.seen !== undefined ? { "if-none-match": `"${request.seen}"` } : {}),
            },
            ...(request.body !== undefined ? { body: JSON.stringify(request.body) } : request.bytes !== undefined ? { body: request.bytes } : {}),
            signal,
          });
          const etag = /^"([^"]*)"$/u.exec(response.headers.get("etag") ?? "")?.[1] ?? null;
          return { status: response.status, ok: response.ok, bytes: new Uint8Array(await response.arrayBuffer()), retryAfter: response.headers.get("retry-after"), tag: etag };
        },
        catch: (cause) => failed(cause instanceof Error ? cause.message : String(cause)),
      }).pipe(
        // Past it, as its fiber's clock tells, the request is aborted.
        Effect.timeoutOrElse({ duration: request.timeout, orElse: () => Effect.fail(failed(`it did not answer within ${Duration.toSeconds(request.timeout)} s`)) }),
      );
      const text = new TextDecoder().decode(bytes);
      if (ok || (status === 404 && request.absent) || (status === 304 && request.seen !== undefined)) return { status, bytes, text, tag };
      const refusal = Schema.decodeUnknownOption(Schema.fromJsonString(Refusal))(text);
      return yield* failed(refusal._tag === "Some" ? refusal.value.error : `it answered ${status}`, status, retryAfter);
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
   * call: started, answered as it ended where its start waited for it to end, as a short
   * prepare's does, else asked how it stands until it has ended, within LONG_CALL; each
   * call as `sent` says (ownRelease).
   */
  const operate = <A, I>(node: string, what: string, path: string, request: CheckpointRequest | PrepareRequest, value: Schema.Codec<A, I>, sent: Pick<AgentCall, "ownRelease"> = {}): Effect.Effect<A, NodeAgentError | KubeApiError> =>
    Effect.gen(function*() {
      const begun = yield* send(node, what, { method: "POST", path, body: request, timeout: SHORT_CALL, ...sent });
      const standing = { method: "GET", path: `${path}/${encodeURIComponent(request.environment)}/${encodeURIComponent(request.workspace)}`, timeout: SHORT_CALL, ...sent } as const;
      const ended = begun.status === 200
        ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(OutcomeOf(value)))(begun.text).pipe(
          Effect.mapError((error) => new NodeAgentError({ node, call: what, reason: `its answer is not one: ${error.message}` })),
        )
        : yield* ask(node, what, standing, OutcomeOf(value)).pipe(
          Effect.repeat({ schedule: OPERATION_POLLS, while: ({ state }) => state === "running" }),
          Effect.timeoutOrElse({ duration: LONG_CALL, orElse: () => Effect.fail(new NodeAgentError({ node, call: what, reason: `it had not ended within ${LONG_CALL}` })) }),
        );
      if (ended.state === "done") return ended.value;
      // Answered as it ended, or repeated while it ran, it ran no more.
      if (ended.state === "running") return yield* Effect.die("an operation ended running");
      return yield* new NodeAgentError({ node, call: what, reason: ended.error, status: ended.status });
    });

  const holdingsCall = "say what it holds";

  /** What the agent of `node` said it holds in `answered`, an answer of `GET /holdings`, and its tag. */
  const taggedIn = (node: string, { text, tag }: { readonly text: string; readonly tag: string | null }): Effect.Effect<TaggedHoldings, NodeAgentError> =>
    tag === null
      ? Effect.fail(new NodeAgentError({ node, call: holdingsCall, reason: "its answer bears no tag" }))
      : Schema.decodeUnknownEffect(Schema.fromJsonString(Holdings))(text).pipe(
        Effect.map((holdings) => ({ holdings, tag })),
        Effect.mapError((error) => new NodeAgentError({ node, call: holdingsCall, reason: `its answer is not one: ${error.message}` })),
      );

  /** What `node` holds, as its agent says it now. */
  const holdingsNow = (node: string): Effect.Effect<TaggedHoldings, NodeAgentError | KubeApiError> =>
    Effect.flatMap(send(node, holdingsCall, { method: "GET", path: "/holdings", timeout: SHORT_CALL, ownRelease: true }), (answered) => taggedIn(node, answered));

  /** What `node` holds once it is other than what the tag `seen` names, as its agent says it; null where it is not within HOLDINGS_WAIT_MS. */
  const holdingsSince = (node: string, seen: string): Effect.Effect<TaggedHoldings | null, NodeAgentError | KubeApiError> =>
    Effect.flatMap(
      send(node, holdingsCall, { method: "GET", path: "/holdings", seen, timeout: HOLDINGS_CALL, ownRelease: true }),
      (answered) => (answered.status === 304 ? Effect.succeed(null) : taggedIn(node, answered)),
    );

  /** What each node holds, as its agent last said it, by node, while it is watched (watchHoldings). */
  const kept = new Map<string, TaggedHoldings>();

  /**
   * Keeps what `node` holds, as kept now, as its agent says it each time it changes: asked as
   * of what is kept (holdingsSince), again as soon as it answers, in the background, until a
   * request fails, as one of an agent that stopped does; what is kept of it is then forgotten,
   * for the next restore to ask it anew.
   */
  const watchHoldings = (node: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      for (let seen = kept.get(node); seen !== undefined; seen = kept.get(node)) {
        const changed = yield* holdingsSince(node, seen.tag);
        if (changed !== null) kept.set(node, changed);
      }
    }).pipe(
      Effect.catch((error) => Effect.logInfo(`what node ${node} holds is asked of its agent anew by the next restore: ${error.message}`)),
      Effect.ensuring(Effect.sync(() => kept.delete(node))),
      Effect.forkIn(scope),
      Effect.asVoid,
    );

  /** Any node with a ready agent: each reads, copies and forgets in the bucket as well as another. */
  const anyNode: Effect.Effect<string, NodeAgentError | KubeApiError> = Effect.flatMap(addresses, (known) => {
    const [first] = known.keys();
    return first === undefined ? Effect.fail(new NodeAgentError({ node: "any node", call: "be reached", reason: "no agent is ready" })) : Effect.succeed(first);
  });

  /** The path of a workspace's file. */
  const releasedPath = ({ environment, workspace }: WorkspaceRef) => `/workspaces/${encodeURIComponent(environment)}/${encodeURIComponent(workspace)}/released`;
  const filePath = ({ environment, workspace }: WorkspaceRef, path: string) => `/files/${encodeURIComponent(environment)}/${encodeURIComponent(workspace)}?path=${encodeURIComponent(path)}`;

  return {
    nodes: Effect.map(addresses, (known) => [...known.keys()]),
    releaseNodes: Effect.map(counted, ({ ready, unready, desired }) => {
      const all = [...ready];
      return {
        nodes: all.flatMap(([node, { release }]) => (release === location.release ? [node] : [])),
        others: all.flatMap(([node, { release }]) => (release === location.release ? [] : [node])),
        unready: [...unready],
        unseen: Math.max(0, desired - ready.size - unready.size),
      };
    }),
    attach: (node, request) => send(node, `attach ${request.environment}/${request.workspace} into ${request.pod}`, { method: "POST", path: "/attach", body: request, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    bindFree: (node, request) => send(node, `give ${request.namespace}/${request.pod} its binding`, { method: "POST", path: "/bindings", body: request, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    detach: (node, namespace, pod, containerIds) => send(node, `detach what is attached into ${namespace}/${pod}`, { method: "POST", path: "/detach", body: { namespace, pod, containerIds }, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    readFile: (workspace, path, maxBytes) =>
      Effect.flatMap(anyNode, (node) => send(node, `read ${path} of ${workspace.environment}/${workspace.workspace}`, { method: "GET", path: `${filePath(workspace, path)}&max=${maxBytes}`, timeout: SHORT_CALL })).pipe(
        Effect.map(({ bytes }) => bytes),
      ),
    createWorkspace: (workspace, quotaBytes) =>
      Effect.flatMap(anyNode, (node) => send(node, `make ${workspace.environment}/${workspace.workspace}`, { method: "POST", path: "/workspaces", body: { ...workspace, quotaBytes }, timeout: SHORT_CALL })).pipe(Effect.asVoid),
    cloneWorkspace: (from, to, quotaBytes) =>
      Effect.flatMap(anyNode, (node) =>
        send(node, `clone ${from.environment}/${from.workspace} into ${to.environment}/${to.workspace}`, { method: "POST", path: "/workspaces/clones", body: { from, to, quotaBytes }, timeout: LONG_CALL })).pipe(Effect.asVoid),
    listWorkspaces: (environment) =>
      Effect.flatMap(anyNode, (node) =>
        ask(node, `list ${environment}'s workspaces`, { method: "GET", path: `/workspaces/${encodeURIComponent(environment)}`, timeout: SHORT_CALL }, Listed)).pipe(
          Effect.map(({ workspaces }) => workspaces),
        ),
    listEnvironments: Effect.flatMap(anyNode, (node) =>
      ask(node, "list the environments of workspaces", { method: "GET", path: "/workspaces", timeout: SHORT_CALL }, ListedEnvironments)).pipe(
        Effect.map(({ environments }) => environments),
      ),
    removeWorkspace: (workspace) =>
      Effect.flatMap(anyNode, (node) =>
        send(node, `delete ${workspace.environment}/${workspace.workspace}`, { method: "DELETE", path: `/workspaces/${encodeURIComponent(workspace.environment)}/${encodeURIComponent(workspace.workspace)}`, timeout: SHORT_CALL })).pipe(Effect.asVoid),
    attachments: (node) =>
      ask(node, "say what is attached on it", { method: "GET", path: "/attachments", timeout: SHORT_CALL }, Attachments).pipe(Effect.map(({ attachments }) => attachments)),
    stop: (node, request) => send(node, `tell ${request.namespace}/${request.pod}'s bayma to go on with its stop`, { method: "POST", path: "/stop", body: request, timeout: SHORT_CALL }).pipe(Effect.asVoid),
    sync: (node, request) =>
      send(node, `have ${request.namespace}/${request.pod}'s sandbox write its files through`, { method: "POST", path: "/sync", body: request, timeout: SYNC_CALL }).pipe(Effect.asVoid),
    released: (workspace) =>
      Effect.gen(function*() {
        const node = yield* anyNode;
        const what = `read ${workspace.environment}/${workspace.workspace}'s release`;
        const { status, text } = yield* send(node, what, { method: "GET", path: releasedPath(workspace), timeout: SHORT_CALL, absent: true });
        if (status === 404) return null;
        return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Released))(text).pipe(
          Effect.mapError((error) => new NodeAgentError({ node, call: what, reason: `its answer is not one: ${error.message}` })),
        );
      }),
    setReleased: (workspace, released) =>
      Effect.flatMap(anyNode, (node) =>
        send(node, `record ${workspace.environment}/${workspace.workspace}'s release into ${released?.snapshot ?? "none"}`, {
          method: released === null ? "DELETE" : "PUT",
          path: releasedPath(workspace),
          ...(released === null ? {} : { body: released }),
          timeout: SHORT_CALL,
        })).pipe(Effect.asVoid),
    quota: (workspace) =>
      Effect.gen(function*() {
        const node = yield* anyNode;
        const what = `read ${workspace.environment}/${workspace.workspace}'s quota`;
        const path = `/workspaces/${encodeURIComponent(workspace.environment)}/${encodeURIComponent(workspace.workspace)}/quota`;
        const { status, text } = yield* send(node, what, { method: "GET", path, timeout: SHORT_CALL, absent: true });
        if (status === 404) return null;
        return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Quota))(text).pipe(
          Effect.mapError((error) => new NodeAgentError({ node, call: what, reason: `its answer is not one: ${error.message}` })),
        );
      }),
    writeFile: (workspace, path, bytes) =>
      Effect.flatMap(anyNode, (node) => send(node, `write ${path} of ${workspace.environment}/${workspace.workspace}`, { method: "PUT", path: filePath(workspace, path), bytes, timeout: SHORT_CALL })).pipe(Effect.asVoid),
    checkpoint: (node, request) => operate(node, `snapshot ${request.environment}/${request.workspace}`, "/checkpoints", request, Manifest),
    prepare: (node, workspace, manifest, gofer) => {
      const what = `prepare ${workspace.environment}/${workspace.workspace}'s snapshot ${manifest.snapshot}`;
      return operate(node, what, "/prepare", { ...workspace, manifest, ...(gofer ? { gofer } : {}) }, Prepared, { ownRelease: true }).pipe(
        // Asked while one of another snapshot of the workspace's ran, which it is: refused, as one that may pass.
        Effect.filterOrFail(
          (prepared) => prepared.manifest.snapshot === manifest.snapshot,
          (prepared) => new NodeAgentError({ node, call: what, reason: `a prepare of its snapshot ${prepared.manifest.snapshot} ran as it was asked, which it came to` }),
        ),
      );
    },
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
      Effect.flatMap(anyNode, (node) => send(node, `forget ${workspace.environment}/${workspace.workspace}'s snapshots`, { method: "DELETE", path: snapshotPath(workspace), timeout: SHORT_CALL })).pipe(
        Effect.asVoid,
      ),
    spend: (node, workspace, snapshot) =>
      send(node, `spend ${workspace.environment}/${workspace.workspace}'s snapshot ${snapshot}`, {
        method: "DELETE",
        path: `${snapshotPath(workspace)}/current?snapshot=${encodeURIComponent(snapshot)}`,
        timeout: SHORT_CALL,
      }).pipe(Effect.asVoid),
    collect: (workspace) =>
      Effect.flatMap(anyNode, (node) =>
        ask(node, `collect ${workspace.environment}/${workspace.workspace}'s snapshots`, { method: "POST", path: "/collect", body: workspace, timeout: LONG_CALL }, Collected)).pipe(
          Effect.map(({ kept }) => kept),
        ),
    resume: (node, request, bytes) =>
      send(node, `resume ${request.environment}/${request.workspace}'s sandbox`, { method: "POST", path: "/resume", body: request, timeout: Duration.millis(resumeMs(bytes)), ownRelease: true }).pipe(Effect.asVoid),
    restoreStandby: (node, request) =>
      ask(node, `restore ${request.environment}/${request.workspace} into the standby ${request.namespace}/${request.pod}`, { method: "POST", path: "/standby/restore", body: request, timeout: LONG_CALL, ownRelease: true }, StandbyRestored),
    abandon: (node, request) =>
      ask(node, `abandon ${request.environment}/${request.workspace}'s restore`, { method: "POST", path: "/abandon", body: request, timeout: SHORT_CALL }, Abandoned).pipe(
        Effect.map(({ killed }) => killed),
      ),
    holdings: (node) => ask(node, "say what it holds", { method: "GET", path: "/holdings", timeout: SHORT_CALL }, Holdings),
    held: (node) =>
      Effect.suspend(() => {
        const known = kept.get(node);
        if (known) return Effect.succeed(known.holdings);
        return holdingsNow(node).pipe(
          Effect.tap((now) =>
            Effect.suspend(() => {
              // Watched once, however many restores asked at once.
              if (kept.has(node)) return Effect.void;
              kept.set(node, now);
              return watchHoldings(node);
            })
          ),
          Effect.map(({ holdings }) => holdings),
        );
      }),
    workspaces: (environment) =>
      Effect.flatMap(anyNode, (node) =>
        ask(node, `list ${environment}'s snapshots`, { method: "GET", path: `/snapshots/${encodeURIComponent(environment)}`, timeout: SHORT_CALL }, Listed)).pipe(
          Effect.map(({ workspaces }) => workspaces),
        ),
    discard: (node, workspace, keep) =>
      ask(
        node,
        `discard ${workspace.environment}/${workspace.workspace}'s cached images`,
        { method: "DELETE", path: `/cache/${encodeURIComponent(workspace.environment)}/${encodeURIComponent(workspace.workspace)}?keep=${keep.join(",")}`, timeout: SHORT_CALL },
        Discarded,
      ).pipe(Effect.map(({ discarded }) => discarded)),
  };
});
