/**
 * The sandbox gateway's session workspaces, on the binder (../binder/): a session's
 * workspace, `sessionfs:<volume id>`, bound to a pod of its environment's pool of its
 * internet (`none` or `full`), as its caller's token says (`environment`, `mode`), of the
 * environments there are (./environments.ts); the pod readied by the node agent of its node
 * attaching the workspace into it, with the binding's token beside it, which the pod's bayma
 * starts on; and released once its bayma has run no exec, and no request of it has come, for
 * `releaseAfterMs` (../binder/sweep.ts): its sandbox saved whole by its node's agent, which
 * ends it, the workspace detached once it has ended, then the pod deleted, so a pod serves one
 * binding. Whether a bound pod is idle is asked of its bayma at most once every `idlePollMs`
 * (IDLE_POLL_MS by default): at most four requests a minute of each bound pod's, and one a
 * release's wait for most.
 *
 * Its callers reach workspaces through it alone (operate), each of its own environment, as its
 * token names it, signed by that environment's key: the node agents are the gateway's. Under
 * /workspace, of the token's key's workspace:
 * - `POST /workspace`: makes it, of `quotaBytes` (204); again, sets its quota anew.
 * - `DELETE /workspace`: deletes it (204), as it does one there is not; 409 while it is bound.
 * - `GET /workspace/files?path=<path>&max=<bytes>`, `PUT /workspace/files?path=<path>`: its
 *   file at `path`, as the node agents read and write one (200, 204).
 * - `GET /workspace/quota`: `{ capacityBytes, usedBytes }` (200); 404 where it has none.
 * - `POST /workspace/forks`, `{ environment, workspace }`: makes `workspace` of `environment` a
 *   clone of it (204), where `environment` is the caller's own or one branched from it, as
 *   its namespace records (./environments.ts): main forks into its branches, and a branch
 *   into itself alone, holding no key of any other's; 403 elsewhere.
 * - `GET /workspace/all`: `{ workspaces }`, the names of the environment's workspaces (200).
 * What is refused is `{ error }` with its status; a node agent's refusal is its own, and
 * what failed otherwise 502.
 *
 * Its metrics: how long each attach and detach took (ATTACH_DURATION, DETACH_DURATION), and
 * each binding released, by why (RELEASES): `idle`, released by its sweep; `asked`, by its
 * caller; `lost`, its pod gone while it was bound; `deleted`, its pod ended by other than the
 * gateway, as a drain ends one, saved first where its bayma holds its stop (./snapshots.ts
 * saveStopping), its workspace detached once its containers stopped (drain). Each of its pool's
 * namespace.
 */
import { lookup } from "node:dns/promises";

import type { V1Pod } from "@kubernetes/client-node";
import { Clock, Effect, Schema } from "effect";

import { type Binder, BindStepFailed } from "../binder/binder.ts";
import { type BindingLease, heldLeases } from "../binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, type Pool, POOL_LABEL } from "../binder/names.ts";
import { type Answer, type Operation, type Placement, PlacementRefused } from "../binder/server.ts";
import { baymaIdleness, IDLE_POLL_MS, makeSweep } from "../binder/sweep.ts";
import { bindingToken, type TokenClaims } from "../binder/tokens.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import { BAYMA_CONTAINER } from "../kube/sandbox-shape.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import { isValidVolumeId, volumeIdOfKey } from "../sandbox/names.ts";
import type { Events, EventInput } from "../events/events.ts";
import { meter } from "../telemetry/index.ts";
import type { Environments } from "./environments.ts";
import type { GatewaySnapshots as GatewaySnapshotsConfig } from "./names.ts";
import { BaymaStatus, makeGatewaySnapshots } from "./snapshots.ts";

/** The metrics of the gateway's sessions, as the module says. */
export const ATTACH_DURATION = "alasio.gateway.attach.duration";
export const DETACH_DURATION = "alasio.gateway.detach.duration";
export const RELEASES = "alasio.gateway.releases";
const attachDuration = meter.createHistogram(ATTACH_DURATION, { description: "How long a node agent took to attach a workspace into its bound pod", unit: "s" });
const detachDuration = meter.createHistogram(DETACH_DURATION, { description: "How long a node agent took to detach a workspace from its released pod", unit: "s" });
const releases = meter.createCounter(RELEASES, { description: "Bindings released, by why: idle, asked by their caller, lost as their pod went, or deleted as their pod was ended by other than the gateway" });

/** Why a binding was released. */
type ReleaseReason = "idle" | "asked" | "lost" | "deleted" | "broken";
const released = (pool: Pool, reason: ReleaseReason) => Effect.sync(() => releases.add(1, { "k8s.namespace.name": pool.namespace, "alasio.gateway.release.reason": reason }));

/** `effect`, its duration, when it succeeds, recorded on `histogram` of `pool`'s namespace. */
const timed = <A, E, R>(histogram: typeof attachDuration, pool: Pool, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.gen(function*() {
    const started = yield* Clock.currentTimeMillis;
    const value = yield* effect;
    histogram.record(((yield* Clock.currentTimeMillis) - started) / 1000, { "k8s.namespace.name": pool.namespace });
    return value;
  });

/** A session's internet. */
export type NetMode = "none" | "full";

/** What the gateway's sessions are given: its environments, each pool's name by its internet, and when a pod is released. */
export interface SessionsOptions {
  readonly environments: Environments;
  readonly pools: Readonly<Record<NetMode, string>>;
  readonly releaseAfterMs: number;
  /** The port bound pods' bayma serves on. */
  readonly podPort: number;
  /** The key bindings' tokens are signed with. */
  readonly bindingKey: Buffer;
  /** How often a bound pod's bayma is asked whether it is idle, at most: IDLE_POLL_MS unless given. */
  readonly idlePollMs?: number;
  /**
   * Where bound pods' bayma exports its telemetry, where it does: the session telemetry
   * receiver's host and port, its address resolved as each pod is readied, as a session with
   * no internet resolves no name, and the variables that say what it exports.
   */
  readonly telemetry?: { readonly host: string; readonly port: number; readonly env: Readonly<Record<string, string>> } | undefined;
  /** What tells each environment's alasio of what happens to its sessions' bindings (../events/events.ts). */
  readonly events: Events["Service"];
  /** The quota of a workspace made or forked, in bytes. */
  readonly quotaBytes: number;
  /** How main's sessions are snapshotted (./snapshots.ts), none where they are not, and main's sessions' namespace. */
  readonly snapshots?: GatewaySnapshotsConfig | null;
  readonly mainNamespace?: string;
}

/** The most of a workspace's file read where the caller says no less: a received file's, as the node agents write one. */
const MAX_READ_BYTES = 64 * 1024 ** 2;

/** What a fork asks for: the environment, and the name of the workspace there, made a clone; null where `body` is not that. */
function forkOf(body: Uint8Array): { readonly environment: string; readonly workspace: string } | null {
  try {
    const asked: unknown = JSON.parse(Buffer.from(body).toString("utf8"));
    if (typeof asked !== "object" || asked === null) return null;
    const { environment, workspace } = asked as Record<string, unknown>;
    return typeof environment === "string" && isValidVolumeId(workspace) ? { environment, workspace } : null;
  } catch {
    return null;
  }
}

/** How long an ask of a bayma's status may take. */
const STATUS_TIMEOUT_MS = 5_000;

/** How long a pod's containers are waited for to stop, once its deletion is asked. */
const STOP_TIMEOUT_MS = 120_000;

/** The runtime's ids of `pod`'s containers, as its status names them. */
const containerIdsOf = (pod: V1Pod): string[] =>
  (pod.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^[a-z]+:\/\//u, "")] : []));

/** The gateway's sessions; see makeSessions. */
export interface Sessions {
  /** Where `key` is bound, by its caller's token's options, and how its pod is readied. */
  readonly place: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** Every pool the gateway binds in now, of every environment there is. */
  readonly pools: Effect.Effect<readonly Pool[]>;
  /** Releases each bound pod of the gateway's pools that is idle by `now`, as the module says, and says which, telling of each. */
  readonly sweep: Effect.Effect<readonly string[], KubeApiError>;
  /** Releases each bound pod of the gateway's pools that is ending though the gateway did not end it, as the module says, in the background: which keys, of those it began. */
  readonly drain: Effect.Effect<readonly string[], KubeApiError>;
  /** Looks at each bound pod for one whose bayma ended, and keeps the snapshot taken live of each that runs true to it, as the module says. */
  readonly live: Effect.Effect<void, KubeApiError>;
  /** Records what the binding `lease` of `pool`, whose pod went while it was bound, is released into, while its Lease holds still. */
  readonly recordLost: (pool: Pool, lease: BindingLease) => Effect.Effect<void>;
  /** Tells of the binding `lease` of `pool`, let go as its pod went while it was bound, as lost. */
  readonly lost: (pool: Pool, lease: BindingLease) => Effect.Effect<void>;
  /** Answers a caller's request of its workspaces, as the module says. */
  readonly operate: (claims: TokenClaims, operation: Operation) => Effect.Effect<Answer>;
}

/** The gateway's sessions, as the module says, on the node agents, the binder, and the KubeClient. */
export const makeSessions = Effect.fnUntraced(function*(
  { environments, pools: poolNames, releaseAfterMs, podPort, bindingKey, idlePollMs = IDLE_POLL_MS, telemetry, events, quotaBytes, snapshots: snapshotsConfig = null, mainNamespace = "" }: SessionsOptions,
  agents: NodeAgents,
  binder: Binder["Service"],
): Effect.fn.Return<Sessions, never, KubeClient> {
  const kube = yield* KubeClient;
  const snapshots = yield* makeGatewaySnapshots({ agents, snapshots: snapshotsConfig, mainNamespace });
  const pools = Effect.map(environments.current, (current) => current.flatMap(({ namespace }) => (Object.values(poolNames) as string[]).map((name): Pool => ({ name, namespace }))));
  /** The address `host` resolves to. */
  const addressOf = (host: string): Effect.Effect<string, BindStepFailed> =>
    Effect.tryPromise({ try: () => lookup(host), catch: (error) => new BindStepFailed({ reason: `${host} did not resolve: ${String(error)}` }) }).pipe(Effect.map(({ address }) => address));

  const place = (key: string, options: Readonly<Record<string, string>>): Effect.Effect<Placement, PlacementRefused> =>
    Effect.gen(function*() {
      const volumeId = volumeIdOfKey(key);
      if (!volumeId) return yield* new PlacementRefused({ reason: `${key} is no session workspace's` });
      const environment = options["environment"] === undefined ? null : yield* environments.named(options["environment"]);
      if (!environment) return yield* new PlacementRefused({ reason: `there is no environment ${options["environment"] ?? "named"} of sessions` });
      const mode = options["mode"];
      if (mode !== "none" && mode !== "full") return yield* new PlacementRefused({ reason: `a session's internet is none or full, not ${mode ?? "unsaid"}` });
      const pool = { name: poolNames[mode], namespace: environment.namespace };
      // Restored from the snapshot it was released into, where it was, by a pod of its own (./snapshots.ts).
      const restoring = snapshots.restoring(pool, volumeId, mode);
      return {
        pool,
        make: restoring.make,
        // Released as it is when idle: saved whole first, then detached, then deleted.
        release: ({ pod }) => endAndDetach(pool, volumeId, pod).pipe(Effect.tap(() => released(pool, "asked"))),
        // Its workspace attached, its binding's token beside it, on which its bayma starts.
        prepare: (got, binding) =>
          restoring.ready(got, (pod) =>
            Effect.gen(function*() {
              const env = telemetry ? { ...telemetry.env, OTEL_EXPORTER_OTLP_ENDPOINT: `http://${yield* addressOf(telemetry.host)}:${telemetry.port}` } : {};
              yield* timed(attachDuration, pool, agents.attach(pod.spec?.nodeName ?? "", {
                environment: environment.namespace,
                workspace: volumeId,
                pod: pod.metadata?.name ?? "",
                token: bindingToken(bindingKey, { key, binding, namespace: environment.namespace }),
                env,
              })).pipe(Effect.mapError((error) => new BindStepFailed({ reason: error.message })));
            })),
      };
    });

  /** Waits until `name`'s containers have all stopped, or it is gone. */
  const stopped = (pool: Pool, name: string): Effect.Effect<void, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      const deadline = (yield* Clock.currentTimeMillis) + STOP_TIMEOUT_MS;
      for (;;) {
        const ending = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
        if (!ending || (ending.status?.containerStatuses ?? []).every(({ state }) => state?.terminated)) return;
        if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindStepFailed({ reason: `${name}'s containers had not stopped within ${STOP_TIMEOUT_MS / 1000} s` });
        yield* Effect.sleep(500);
      }
    });

  /**
   * Ends the sandbox of `name`, the bound pod of the workspace `volumeId`, so its workspace
   * can be detached, then detaches it: saved whole first, its release recorded for its next
   * bind (./snapshots.ts); a save that leaves it running fails the release, which keeps the
   * binding. Then its pod's deletion ends what of it runs on, its containers stopped, and the
   * workspace is detached, for the binder to delete the pod.
   */
  const endAndDetach = (pool: Pool, volumeId: string, name: string): Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed> =>
    Effect.gen(function*() {
      const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
      if (!pod) return;
      const node = pod.spec?.nodeName ?? "";
      const containers = containerIdsOf(pod);
      if ((yield* snapshots.save(pool, volumeId, name)) === "keep") return yield* new BindStepFailed({ reason: `its sandbox was not saved, and runs on` });
      yield* kube.remove("v1", "Pod", pool.namespace, name);
      yield* stopped(pool, name);
      yield* timed(detachDuration, pool, agents.detach(node, name, containers));
    });

  /** Tells `pool`'s environment's alasio of what `of` says; where `pool`'s environment is gone, no one. */
  const tell = (pool: Pool, of: (environment: string) => EventInput): Effect.Effect<void> =>
    Effect.flatMap(environments.ofNamespace(pool.namespace), (environment) => (environment ? events.tell(of(environment.name)) : Effect.void));
  const subject = (key: string) => ({ kind: "workspace", key }) as const;

  const sweep = yield* makeSweep({
    pools,
    idleness: baymaIdleness({ podPort, releaseAfterMs, idlePollMs, bindingKey }),
    release: (pool: Pool, key: string, lease: BindingLease) =>
      binder.release(pool, key, ({ pod: name }) => endAndDetach(pool, volumeIdOfKey(key) ?? "", name)).pipe(
        Effect.tap((done) =>
          done
            ? Effect.andThen(released(pool, "idle"), tell(pool, (environment) => ({ kind: "workspace.released", environment, source: "sandbox-gateway", subject: subject(key), binding: lease.metadata?.uid ?? "", reason: "idle" })))
            : Effect.void
        ),
      ),
  });

  /** The bound pods ending though the gateway did not end them whose release runs, by their uid. */
  const draining = new Set<string>();
  /** Saves `pod`, bound to `workspace` of `pool` and ending, where its bayma holds its stop, then detaches its workspace once its containers stopped. */
  const drained = (pool: Pool, workspace: string, pod: V1Pod): Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed> =>
    Effect.gen(function*() {
      const name = pod.metadata?.name ?? "";
      yield* snapshots.saveStopping(pool, workspace, pod);
      yield* stopped(pool, name);
      yield* timed(detachDuration, pool, agents.detach(pod.spec?.nodeName ?? "", name, containerIdsOf(pod)));
    });
  const drain: Effect.Effect<readonly string[], KubeApiError> = Effect.gen(function*() {
    const started: string[] = [];
    for (const pool of yield* pools) {
      const pods = (yield* kube.list("v1", "Pod", pool.namespace, `${BINDING_LABEL},${POOL_LABEL}=${pool.name}`)) as V1Pod[];
      for (const pod of pods) {
        const uid = pod.metadata?.uid ?? "";
        const key = pod.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
        const workspace = volumeIdOfKey(key);
        if (pod.metadata?.deletionTimestamp === undefined || !workspace || draining.has(uid)) continue;
        draining.add(uid);
        started.push(key);
        // Released as the binder releases one, its Lease marked releasing first: a release of the gateway's own under way is one of none.
        let binding = "";
        yield* Effect.forkDetach(binder.release(pool, key, (bound) => (bound.pod === pod.metadata?.name ? ((binding = bound.binding), drained(pool, workspace, pod)) : Effect.void)).pipe(
          Effect.tap((done) =>
            done
              ? Effect.andThen(released(pool, "deleted"), tell(pool, (environment) => ({ kind: "workspace.released", environment, source: "sandbox-gateway", subject: subject(key), binding, reason: "deleted" })))
              : Effect.void
          ),
          Effect.catch((error) => Effect.logWarning(`releasing ${key}, as its pod ends, failed: ${error.message}`)),
          Effect.ensuring(Effect.sync(() => void draining.delete(uid))),
        ));
      }
    }
    return started;
  });

  /** What a bound pod's bayma says of its execs and its state generation, asked with its binding's token; null where it cannot be asked. */
  const statusOf = (lease: BindingLease, pod: V1Pod): Effect.Effect<BaymaStatus | null> => {
    const address = pod.status?.podIP;
    if (!address) return Effect.succeed(null);
    const token = bindingToken(bindingKey, { key: lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "", binding: lease.metadata?.uid ?? "", namespace: lease.metadata?.namespace ?? "" });
    return Effect.tryPromise(async (signal) => {
      const response = await fetch(`http://${address}:${podPort}/status`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.any([signal, AbortSignal.timeout(STATUS_TIMEOUT_MS)]) });
      if (!response.ok) throw new Error(`it answered ${response.status}`);
      return await response.text();
    }).pipe(Effect.flatMap((text) => Schema.decodeUnknownEffect(Schema.fromJsonString(BaymaStatus))(text)), Effect.orElseSucceed(() => null));
  };

  /** Releases the binding of `key` of `pool`, its pod `pod`, as `before` says, telling of it, as released for `reason`. */
  const releaseFor = (pool: Pool, key: string, pod: V1Pod, reason: "broken", before: (name: string) => Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed>) => {
    let binding = "";
    return binder.release(pool, key, (bound) => (bound.pod === pod.metadata?.name ? ((binding = bound.binding), before(bound.pod)) : Effect.void)).pipe(
      Effect.tap((done) =>
        done ? Effect.andThen(released(pool, reason), tell(pool, (environment) => ({ kind: "workspace.released", environment, source: "sandbox-gateway", subject: subject(key), binding, reason }))) : Effect.void
      ),
      Effect.catch((error) => Effect.logWarning(`releasing ${key} failed: ${error.message}`)),
    );
  };

  /** Ends what of `pod`, a bound pod of `pool`, runs on, then detaches its workspace once its containers stopped. */
  const endAndDetachUnsaved = (pool: Pool, pod: V1Pod) =>
    Effect.gen(function*() {
      const name = pod.metadata?.name ?? "";
      yield* kube.remove("v1", "Pod", pool.namespace, name);
      yield* stopped(pool, name);
      yield* timed(detachDuration, pool, agents.detach(pod.spec?.nodeName ?? "", name, containerIdsOf(pod)));
    });

  /**
   * Looks at each bound pod of the gateway's pools: one whose bayma ended, or was started again
   * in it, or whose workspace its node can reach no more (the node's mount of workspace storage
   * ended under it, as its mount's pod killed with no successor leaves it), which serves no more,
   * is released into the snapshot taken of it live, if any, as broken; and of each that runs, the snapshot taken of it live is kept true to it
   * (./snapshots.ts keepLive), one that can be saved no more released now.
   */
  const live: Effect.Effect<void, KubeApiError> = Effect.gen(function*() {
    // The workspaces bound in each namespace, of all its pools.
    const boundIn = new Map<string, Set<string>>();
    // The pods each node says what is attached into is unreachable in, asked once a look.
    const unreachableOn = new Map<string, ReadonlySet<string>>();
    const unreachable = (node: string) =>
      Effect.gen(function*() {
        const known = unreachableOn.get(node);
        if (known) return known;
        const pods = yield* agents.attachments(node).pipe(
          Effect.map((attachments) => new Set(attachments.filter(({ reachable }) => !reachable).map(({ pod }) => pod))),
          Effect.catch((error) => Effect.as(Effect.logWarning(`could not ask ${node} what is attached on it: ${error.message}`), new Set<string>())),
        );
        unreachableOn.set(node, pods);
        return pods;
      });
    for (const pool of yield* pools) {
      const bound = boundIn.get(pool.namespace) ?? new Set<string>();
      boundIn.set(pool.namespace, bound);
      for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube)) {
        const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
        const workspace = volumeIdOfKey(key);
        const pod = (yield* kube.read("v1", "Pod", pool.namespace, lease.spec?.holderIdentity ?? "")) as V1Pod | null;
        if (!workspace || !pod || pod.metadata?.deletionTimestamp !== undefined) continue;
        bound.add(workspace);
        const bayma = pod.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER);
        const ended = bayma !== undefined && (bayma.restartCount > 0 || bayma.state?.terminated !== undefined);
        if (ended || (yield* unreachable(pod.spec?.nodeName ?? "")).has(pod.metadata?.name ?? "")) {
          const why = ended ? "bayma ended in its pod" : "workspace is unreachable in its pod";
          yield* Effect.logWarning(`${key}'s ${why} ${pod.metadata?.name}, which serves its binding no more: it is released into the snapshot taken of it live, if any`);
          yield* Effect.forkDetach(releaseFor(pool, key, pod, "broken", () =>
            Effect.andThen(
              Effect.flatMap(snapshots.liveSnapshotOf(pool, workspace), (into) => agents.setReleased({ environment: pool.namespace, workspace }, into)),
              endAndDetachUnsaved(pool, pod),
            )));
          continue;
        }
        const status = yield* statusOf(lease, pod);
        if (status === null) continue;
        const unsaveable = releaseFor(pool, key, pod, "broken", () => Effect.andThen(snapshots.endUnsaveable(pool, workspace, pod.metadata?.name ?? ""), endAndDetachUnsaved(pool, pod)));
        yield* snapshots.keepLive(pool, workspace, pod, status, yield* Clock.currentTimeMillis, unsaveable);
      }
    }
    for (const [namespace, bound] of boundIn) yield* snapshots.forgetLive(namespace, bound);
  });

  /** Records what the binding `lease` of `pool`, whose pod went while it was bound, is released into: the snapshot taken of it live, if any. Run while its Lease holds still. */
  const recordLost = (pool: Pool, lease: BindingLease): Effect.Effect<void> => {
    const workspace = volumeIdOfKey(lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "");
    if (!workspace) return Effect.void;
    return Effect.flatMap(snapshots.liveSnapshotOf(pool, workspace), (into) => agents.setReleased({ environment: pool.namespace, workspace }, into)).pipe(
      Effect.catch((error) => Effect.logWarning(`could not record what ${pool.namespace}/${workspace}, whose pod went, is released into: ${error.message}`)),
    );
  };

  const lost = (pool: Pool, lease: BindingLease): Effect.Effect<void> => {
    const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
    return Effect.andThen(released(pool, "lost"), tell(pool, (environment) => ({
      kind: "workspace.lost",
      environment,
      source: "sandbox-gateway",
      subject: subject(key),
      binding: lease.metadata?.uid ?? "",
      pod: lease.spec?.holderIdentity ?? "",
      reason: "its pod went while it was bound",
    })));
  };

  /** A refusal of `status`, saying `error`. */
  const refused = (status: number, error: string): Answer => ({ status, json: { error } });
  /** What an agent's call answers; its refusal its own, what failed otherwise 502. */
  const answered = <A>(call: Effect.Effect<A, NodeAgentError | KubeApiError>, answer: (value: A) => Answer): Effect.Effect<Answer> =>
    call.pipe(
      Effect.map(answer),
      Effect.catch((error) =>
        Effect.succeed(error._tag === "NodeAgentError" && error.status !== undefined && error.status >= 400 && error.status < 500 ? refused(error.status, error.message) : refused(502, error.message))
      ),
    );
  const done: Answer = { status: 204 };

  const operate = (claims: TokenClaims, { method, path, query, body }: Operation): Effect.Effect<Answer> =>
    Effect.gen(function*() {
      const named = claims.options?.["environment"];
      const environment = named === undefined ? null : yield* environments.named(named);
      if (!environment) return refused(403, `there is no environment ${named ?? "named"} of sessions`);
      if (method === "GET" && path === "/workspace/all") return yield* answered(agents.listWorkspaces(environment.namespace), (workspaces) => ({ status: 200, json: { workspaces } }));
      const workspace = volumeIdOfKey(claims.key);
      if (!workspace) return refused(403, `${claims.key} is no session workspace's`);
      const ref = { environment: environment.namespace, workspace };
      const route = `${method} ${path}`;
      switch (route) {
        case "POST /workspace":
          return yield* answered(agents.createWorkspace(ref, quotaBytes), () => done);
        case "DELETE /workspace": {
          const bound = yield* Effect.forEach(Object.values(poolNames) as string[], (name) => binder.bound({ name, namespace: environment.namespace }, claims.key)).pipe(Effect.result);
          if (bound._tag === "Failure") return refused(502, bound.failure.message);
          if (bound.success.some((each) => each !== null)) return refused(409, `${workspace} is bound; it is deleted once released`);
          return yield* answered(agents.removeWorkspace(ref), () => done);
        }
        case "GET /workspace/files": {
          const maxBytes = Number(query.get("max") ?? MAX_READ_BYTES);
          if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) return refused(400, "max is a number of bytes");
          return yield* answered(agents.readFile(ref, query.get("path") ?? "", maxBytes), (bytes) => ({ status: 200, bytes }));
        }
        case "PUT /workspace/files":
          return yield* answered(agents.writeFile(ref, query.get("path") ?? "", body), () => done);
        case "GET /workspace/quota":
          return yield* answered(agents.quota(ref), (quota) => (quota ? { status: 200, json: quota } : refused(404, `${workspace} has no quota`)));
        case "POST /workspace/forks": {
          const asked = forkOf(body);
          if (!asked) return refused(400, "a fork is { environment, workspace }, a workspace's name");
          const target = yield* environments.named(asked.environment);
          // Its own environment, or one branched from it: never its parent's, nor a sibling's.
          if (!target || (target.name !== environment.name && target.parent !== environment.name)) {
            return refused(403, `${environment.name}'s sessions are forked into ${environment.name} and the environments branched from it alone, not ${asked.environment}`);
          }
          return yield* answered(agents.cloneWorkspace(ref, { environment: target.namespace, workspace: asked.workspace }, quotaBytes), () => done);
        }
        default:
          return refused(404, `there is no ${route}`);
      }
    });

  return { place, pools, sweep, drain, live, recordLost, lost, operate };
});
