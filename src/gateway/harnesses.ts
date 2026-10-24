/**
 * The sandbox gateway's conversations' harnesses, on the binder (../binder/): a
 * conversation's harness host, `alasio harness` (../harness/program.ts), the key
 * `harness:<conversation>` (./names.ts harnessKey), bound to a pod of its harness's pool, as
 * its caller's token says (`environment`, `harness`, `workspace`): a session conversation's in
 * its environment's own namespace, main's or a branch's, beside its alasio, not in its
 * sessions' namespace, whose pods run untrusted code under gVisor, held there by their
 * deny-all; a folder conversation's, main's alone, in main's namespace too, as the host
 * profile's user, where its host reaches the machine's folders as alasio's pod does.
 *
 * A pool's pod starts free, of no conversation, and has no endpoint of its own: it is readied
 * by its node's agent writing its binding beside it (../node-agent/workspaces.ts bindFree),
 * the conversation and its binding's token, which its host takes requests with, and serves
 * once it reads it. Its caller reaches it through the front alone, which passes each request
 * on with that token for the caller's (../binder/server.ts).
 *
 * Released once nothing goes on in it, and nothing has for `releaseAfterMs` (harnessIdleness),
 * its host asked at most once every `idlePollMs`, with its binding's token: its host drained
 * first (HarnessHost.drain: no turn after, its harness's session mirrored), for `drainMs` at
 * most, then its pod deleted, and what its node's agent wrote for it removed once its
 * containers stopped, its directory with it; one in which something goes on again as it is
 * drained, a turn about to run among it, is kept, its release given up, so neither waits on the
 * other. A pod whose host ended, or
 * was started again in it, which serves its binding no more (its served mark stops it), is
 * released as broken; one ended by other than the gateway, as it is ended, as deleted; and a
 * binding whose pod went while it was bound, as lost. A host the sweep cannot ask is kept, as
 * it may be working, said so as it first fails to answer and as why changes, until it has
 * answered no sweep for `unreachedMs`: then its pod is ended, undrained, and its binding let
 * go of as lost, why with it, as it serves its binding no more, so its conversation's next
 * prompt binds another. Each is told on its environment's events topic (`harness.released`,
 * `harness.lost`), and counted (./releases.ts). A release of a pod found ending, broken or
 * unreached releases that pod's binding alone, never one its key was bound to anew since.
 *
 * The pools are those of main's environment and of each branch environment that has them: one
 * made before the installation turned harness pools on has none, nor the gateway's Role in its
 * namespace, and is passed over, said so, until it is made again.
 *
 * Its front lists an environment's bound harnesses to that environment's caller (`GET
 * /harnesses`, operate), as `alasio status` says them: each one's conversation, harness, pool and
 * pod, since when it is bound, and what its host says now of a turn running, of work in the
 * background, and since when nothing has gone on in it, or why its host could not be asked.
 */
import type { V1Pod } from "@kubernetes/client-node";
import { Cause, Clock, Effect, Exit, Option, Schema } from "effect";

import { type Binder, BindStepFailed } from "../binder/binder.ts";
import { type BindingLease, heldLeases } from "../binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, type Pool, POOL_LABEL } from "../binder/names.ts";
import { type Answer, type Operation, type Placement, PlacementRefused } from "../binder/server.ts";
import { IDLE_POLL_MS, type Idleness, lingered, makeSweep, notIdle } from "../binder/sweep.ts";
import { bindingToken, type HeldTo, type TokenClaims } from "../binder/tokens.ts";
import { branchNamespace } from "../branch/names.ts";
import type { EventInput, Events } from "../events/events.ts";
import type { HarnessStatus } from "../harness/host.ts";
import { HarnessHostError } from "../harness/index.ts";
import { isHarnessName } from "../harness/names.ts";
import { type RemoteHost, remoteHost } from "../harness/remote-host.ts";
import { hasStatus, type KubeApiError, KubeClient } from "../kube/client.ts";
import type { PodOwner } from "../node-agent/workspaces.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import type { Environments } from "./environments.ts";
import { conversationOfHarnessKey, HARNESS_CONTAINER, type GatewayConfig } from "./names.ts";
import { released, type ReleaseReason } from "./releases.ts";

/** How long an ask of a harness host's may take, but its drain's. */
const ASK_TIMEOUT_MS = 5_000;
/** How long a bound host may answer no sweep before its binding is let go of as lost, by default. */
const UNREACHED_MS = 10 * 60_000;
/** How long whether a branch environment has the harnesses' pools is taken as it was last looked at. */
const POOLS_LOOKED_MS = 60_000;
/** How long a released pod's containers are waited for to stop, once its deletion is asked. */
const STOP_TIMEOUT_MS = 120_000;

/** A bound host something goes on in again as it is drained to be released, which is kept. */
class HostBusy extends Schema.TaggedError<HostBusy>()("HostBusy", { key: Schema.String, pod: Schema.String }) {
  override get message(): string {
    return `${this.key}'s host in ${this.pod} was not drained, as something goes on in it again: it is kept`;
  }
}

/** What the gateway's configuration says of the harnesses' pools. */
export type HarnessesConfig = NonNullable<GatewayConfig["harnesses"]>;

/** What makeHarnesses is given: the pools, as the configuration says them, the environments, main's name, and the key bindings' tokens are signed with. */
export interface HarnessesOptions {
  readonly config: HarnessesConfig;
  readonly environments: Environments;
  /** main's environment's name, whose callers alone have folder conversations. */
  readonly main: string;
  readonly bindingKey: Buffer;
  /** What tells each environment's alasio of what happens to its conversations' harnesses' bindings. */
  readonly events: Events["Service"];
  /** How often a bound host is asked whether it is idle, at most: IDLE_POLL_MS unless given. */
  readonly idlePollMs?: number;
  /** How long a bound host may answer no sweep before its binding is let go of as lost: UNREACHED_MS unless given. */
  readonly unreachedMs?: number;
}

/** The gateway's conversations' harnesses; see makeHarnesses. */
export interface Harnesses {
  /** Where the harness `key` is bound, by its caller's token's options, and how its pod is readied and released. */
  readonly place: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** Every pool the harnesses are bound in now, of every environment there is. */
  readonly pools: Effect.Effect<readonly Pool[]>;
  /** The namespaces those pools are in, and whether `namespace` is one of them. */
  readonly namespaces: Effect.Effect<readonly string[]>;
  readonly ownsNamespace: (namespace: string) => Effect.Effect<boolean>;
  /** Releases each bound host idle by now, as the module says: which keys. */
  readonly sweep: Effect.Effect<readonly string[], KubeApiError>;
  /** Releases each bound pod whose host ended, as broken, and each ended by other than the gateway, as deleted, in the background, as the module says. */
  readonly live: Effect.Effect<void, KubeApiError>;
  /** Tells of the binding `lease` of `pool`, let go as its pod went while it was bound, as lost. */
  readonly lost: (pool: Pool, lease: BindingLease) => Effect.Effect<void>;
  /** What the bound host of the binding `bound` tells, `body`, told its environment's alasio, as relayed says: how it is answered. */
  readonly relay: (bound: HeldTo, body: Uint8Array) => Effect.Effect<Answer>;
  /** What the front answers a caller of an environment itself, under /harnesses: its bound harnesses (`GET /harnesses`), as the module says. */
  readonly operate: (claims: TokenClaims, operation: Operation) => Effect.Effect<Answer>;
}

/**
 * A bound harness, as the front lists it: its conversation and harness, the pool and pod it is
 * bound in, since when, and, as its host says now, whether a turn runs in it, whether its
 * harness works in the background, and since when nothing has gone on in it; or why its host
 * could not be asked.
 */
export interface ListedHarness {
  readonly conversation: string;
  readonly harness: string;
  readonly pool: string;
  readonly pod: string;
  readonly boundAt: string | null;
  readonly host: { readonly running: boolean; readonly background: boolean; readonly idleSince: string | null } | { readonly unreached: string };
}

/** The kinds of event a bound host tells of its harness (../harness/host-events.ts). */
const HOST_KINDS = ["harness.idle", "harness.background-response"] as const;

/** What a bound host tells: an event's kind and its conversation, and nothing else, which the gateway makes the rest of. */
const Told = Schema.Struct({ kind: Schema.Literals(HOST_KINDS), subject: Schema.Struct({ kind: Schema.Literal("conversation"), key: Schema.String }) });
const decodeTold = Schema.decodeUnknownExit(Schema.fromJsonString(Told), { onExcessProperty: "error" });

/**
 * How what the bound host of `conversation`'s harness tells, `body`, is answered: refused (400)
 * where it is no telling, or says more than an event's kind and its conversation, and (403) of a
 * kind no host tells, or of another conversation than its own; told otherwise, by `tell`, as of
 * the host (source "harness"), with its record's id and time the gateway's (202).
 */
export const relayed = (conversation: string, body: Uint8Array, tell: (event: (typeof Told)["Type"]) => Effect.Effect<void>): Effect.Effect<Answer> =>
  Effect.gen(function*() {
    const text = Buffer.from(body).toString("utf8");
    let kind: unknown;
    try {
      kind = (JSON.parse(text) as { readonly kind?: unknown } | null)?.kind;
    } catch {
      return { status: 400, json: { error: "a telling is an event's JSON" } };
    }
    if (typeof kind !== "string" || !(HOST_KINDS as readonly string[]).includes(kind)) return { status: 403, json: { error: `a harness host tells of ${HOST_KINDS.join(" and ")} alone, not ${String(kind)}` } };
    const told = decodeTold(text);
    if (Exit.isFailure(told)) return { status: 400, json: { error: `a telling is an event's kind and its conversation alone: ${Cause.pretty(told.cause)}` } };
    if (told.value.subject.key !== conversation) return { status: 403, json: { error: `the host of ${conversation} tells of its own conversation alone, not ${told.value.subject.key}` } };
    yield* tell(told.value);
    return { status: 202 };
  });

/** What a host's harness has going on, as the sweep asks it: its status, and whether it works on its own between turns. */
export interface HostIdleFacts {
  readonly status: HarnessStatus;
  readonly backgroundWork: boolean;
}

/**
 * Whether a bound harness is idle at `now`, its host saying `facts` of it: not where a turn
 * runs or is about to, one its harness started on its own runs, a hold keeps it, or its
 * harness works on its own between turns (Claude Code's background tasks, Codex's background
 * terminals); and otherwise once nothing has gone on in it for `releaseAfterMs`, as its last
 * turn, the last sweep that found its harness working on its own (`workSeenAt`), the last call
 * of the control plane's that may bind it (its Lease's active stamp, which a call that only
 * reads what it has does not stamp), and its binding say (../binder/sweep.ts lingered). Work in
 * the background ending is something going on, as a turn ending is: its operator often comes
 * back to what it did, so its pod is kept its whole wait from then, not let go of at once.
 */
export function harnessIdleness({ status, backgroundWork }: HostIdleFacts, lease: BindingLease, pod: V1Pod | null, now: number, timing: { readonly releaseAfterMs: number; readonly idlePollMs: number }, workSeenAt = 0): Idleness {
  if (status.busy || status.running || status.cliInitiated || status.held || backgroundWork || status.idleSince === null) return notIdle(now, timing.idlePollMs);
  return lingered(Math.max(status.idleSince, workSeenAt), lease, pod, now, timing);
}

/** How `pod`'s host's container last ended, as kubelet says it: its exit code, its reason and its message, where it says them. */
const endedHow = (pod: V1Pod): string => {
  const host = pod.status?.containerStatuses?.find(({ name }) => name === HARNESS_CONTAINER);
  const ended = host?.state?.terminated ?? host?.lastState?.terminated;
  if (!ended) return `its pod's phase is ${pod.status?.phase ?? "unknown"}`;
  return [`exit code ${ended.exitCode}`, ended.reason, ended.message?.trim()].filter(Boolean).join(", ");
};

/** The memory limit of `pod`'s host where the kernel killed it for its memory, as kubelet says its container ended: null where it ended otherwise, or did not. */
const killedForMemory = (pod: V1Pod): string | null => {
  const host = pod.status?.containerStatuses?.find(({ name }) => name === HARNESS_CONTAINER);
  if ((host?.state?.terminated ?? host?.lastState?.terminated)?.reason !== "OOMKilled") return null;
  return pod.spec?.containers.find(({ name }) => name === HARNESS_CONTAINER)?.resources?.limits?.["memory"] ?? "its limit";
};

/** Whether `pod`'s host ended, or was started again in it, which serves its binding no more. */
const hostEnded = (pod: V1Pod): boolean => {
  const host = pod.status?.containerStatuses?.find(({ name }) => name === HARNESS_CONTAINER);
  return (host !== undefined && (host.restartCount > 0 || host.state?.terminated !== undefined)) || pod.status?.phase === "Failed" || pod.status?.phase === "Succeeded";
};

/** The gateway's conversations' harnesses, as the module says, on the node agents, the binder, and the KubeClient. */
export const makeHarnesses = Effect.fnUntraced(function*(
  { config, environments, main, bindingKey, events, idlePollMs = IDLE_POLL_MS, unreachedMs = UNREACHED_MS }: HarnessesOptions,
  agents: NodeAgents,
  binder: Binder["Service"],
): Effect.fn.Return<Harnesses, never, KubeClient> {
  const kube = yield* KubeClient;
  const releaseAfterMs = config.releaseAfterSeconds * 1000;
  const drainMs = config.drainSeconds * 1000;
  /** An environment's own namespace, its alasio's, which its harnesses' pools are in: main's as configured, a branch's its own. */
  const homeOf = (environment: string): string => (environment === main ? config.namespace : branchNamespace(environment));
  const sessionPools = (environment: string): Pool[] => Object.values(config.pools).map((name) => ({ name, namespace: homeOf(environment) }));
  const folderPools: Pool[] = config.folders ? Object.values(config.folders.pools).map((name) => ({ name, namespace: config.namespace })) : [];
  /** Whether each branch environment has the harnesses' pools, as last looked, and when. */
  const looked = new Map<string, { readonly at: number; readonly there: boolean }>();
  /**
   * Whether the branch environment `environment` has the harnesses' pools, as their Deployment
   * says, looked at again once POOLS_LOOKED_MS has passed: one made before they were turned on
   * has none, nor the gateway's Role there, and is passed over, as the others go on, said so as
   * it is first found so, with what it needs; one that cannot be looked at is taken as it was.
   */
  const hasPools = (environment: string): Effect.Effect<boolean> =>
    Effect.gen(function*() {
      const now = yield* Clock.currentTimeMillis;
      const last = looked.get(environment);
      if (last && now - last.at < POOLS_LOOKED_MS) return last.there;
      const [pool] = sessionPools(environment);
      const there = yield* kube.read("apps/v1", "Deployment", pool?.namespace ?? "", pool?.name ?? "").pipe(
        Effect.map((deployment) => deployment !== null),
        Effect.catchIf(hasStatus(403), () => Effect.succeed(false)),
        Effect.catch((error) => Effect.as(Effect.logWarning(`could not look whether ${environment} has the harnesses' pools: ${error.message}`), last?.there ?? true)),
      );
      if (!there && last?.there !== false) {
        yield* Effect.logWarning(`the branch environment ${environment} has no harnesses' pools, as it was made before they were turned on: its conversations' harnesses are bound nowhere until it is made again (alasio branch delete ${environment}, then alasio branch create ${environment})`);
      }
      looked.set(environment, { at: now, there });
      return there;
    });
  const pools = Effect.gen(function*() {
    const current = yield* environments.current;
    const pooled = yield* Effect.filter(current, ({ name }) => (name === main ? Effect.succeed(true) : hasPools(name)));
    return [...pooled.flatMap(({ name }) => sessionPools(name)), ...folderPools];
  });
  const namespaces = Effect.map(pools, (all) => [...new Set(all.map(({ namespace }) => namespace))]);
  /** The environment whose harnesses' pools are in `namespace`, of those there are; null where none's are. */
  const environmentOf = (namespace: string): Effect.Effect<string | null> =>
    Effect.gen(function*() {
      if (namespace === config.namespace) return main;
      const branch = namespace.startsWith(branchNamespace("")) ? namespace.slice(branchNamespace("").length) : "";
      return branch !== "" && (yield* environments.named(branch)) !== null ? branch : null;
    });
  const ownsNamespace = (namespace: string) => Effect.map(environmentOf(namespace), (environment) => environment !== null);

  /** The host of the binding `lease`'s pod `pod`, reached at its address with its binding's token; null where it has none. */
  const hostOf = (lease: BindingLease, pod: V1Pod): RemoteHost | null => {
    const address = pod.status?.podIP;
    if (!address) return null;
    const token = bindingToken(bindingKey, { key: lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "", binding: lease.metadata?.uid ?? "", namespace: lease.metadata?.namespace ?? "" });
    // The operator's panels are alasio's to reach, never the gateway's.
    return remoteHost({ url: `http://${address}:${config.podPort}`, token: Effect.succeed(token) });
  };

  /** Tells `pool`'s environment's alasio of what `of` says; where its environment is gone, no one. */
  const tell = (pool: Pool, of: (environment: string) => EventInput): Effect.Effect<void> =>
    Effect.flatMap(environmentOf(pool.namespace), (environment) => (environment === null ? Effect.void : events.tell(of(environment))));
  const subject = (key: string) => ({ kind: "conversation", key: conversationOfHarnessKey(key) ?? key }) as const;
  /** The bound hosts the sweep could not ask, by their binding: since when, and why, as last said. */
  const unreached = new Map<string, { readonly since: number; readonly reason: string }>();
  /** When the sweep last found each bound host's harness working on its own, by its binding (harnessIdleness). */
  const workSeen = new Map<string, number>();
  /** What the sweep keeps of the binding `binding`, let go of with it. */
  const forget = (binding: string) =>
    Effect.sync(() => {
      unreached.delete(binding);
      workSeen.delete(binding);
    });
  /** Counts and tells of `key`'s binding `binding` of `pool`, released for `reason`. */
  const releasedFor = (pool: Pool, key: string, binding: string, reason: ReleaseReason, memoryLimit?: string) =>
    forget(binding).pipe(
      Effect.andThen(released(pool, reason)),
      Effect.andThen(tell(pool, (environment) => ({ kind: "harness.released", environment, source: "sandbox-gateway", subject: subject(key), binding, reason, ...(memoryLimit === undefined ? {} : { memoryLimit }) }))),
    );
  /** Counts and tells of `key`'s binding `binding` of `pool`, its pod `pod`, let go of as lost, for `reason`. */
  const lostFor = (pool: Pool, key: string, binding: string, pod: string, reason: string) =>
    forget(binding).pipe(
      Effect.andThen(released(pool, "lost")),
      Effect.andThen(tell(pool, (environment) => ({ kind: "harness.lost", environment, source: "sandbox-gateway", subject: subject(key), binding, pod, reason }))),
    );

  /** Waits until `name`'s containers have all stopped, or it is gone. */
  const stopped = (pool: Pool, name: string): Effect.Effect<V1Pod | null, KubeApiError | BindStepFailed> =>
    Effect.gen(function*() {
      const deadline = (yield* Clock.currentTimeMillis) + STOP_TIMEOUT_MS;
      for (;;) {
        const ending = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
        if (!ending || (ending.status?.containerStatuses ?? []).every(({ state }) => state?.terminated)) return ending;
        if ((yield* Clock.currentTimeMillis) >= deadline) return yield* new BindStepFailed({ reason: `${name}'s containers had not stopped within ${STOP_TIMEOUT_MS / 1000} s` });
        yield* Effect.sleep(500);
      }
    });

  /** Removes what its node's agent wrote for `pod` of `pool`, on `node`, once its containers stopped, its directory with it. */
  const detached = (pool: Pool, node: string, name: string): Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed> =>
    Effect.andThen(stopped(pool, name), node ? agents.detach(node, pool.namespace, name, []) : Effect.void);

  /**
   * The bound pods whose release runs, or has run, by their uid: those the gateway ends, those
   * ending though it did not end them, and those whose host answers no sweep, so that none is
   * released again as it ends.
   */
  const releasing = new Set<string>();

  /** Ends `name` of `pool`, its containers stopped, and removes what was written for it. */
  const ended = (pool: Pool, name: string): Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed> =>
    Effect.gen(function*() {
      const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
      if (!pod) return;
      // Ending as the gateway ends it, not ending though it did not.
      releasing.add(pod.metadata?.uid ?? "");
      yield* kube.remove("v1", "Pod", pool.namespace, name);
      yield* detached(pool, pod.spec?.nodeName ?? "", name);
    });

  /**
   * Drains the host of `key`'s binding `binding`, its pod `name`, for drainMs at most, as the
   * module says: one that does not drain in time, or is not reached, is let go of all the same,
   * and said so; one in which something goes on again is kept (HostBusy).
   */
  const drained = (pool: Pool, key: string, binding: string, name: string): Effect.Effect<void, KubeApiError | HostBusy> =>
    Effect.gen(function*() {
      const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
      const host = pod ? hostOf({ metadata: { annotations: { [KEY_ANNOTATION]: key }, uid: binding, namespace: pool.namespace } }, pod) : null;
      const conversation = conversationOfHarnessKey(key);
      if (!host || !conversation) return;
      const done = yield* host.drain(conversation).pipe(Effect.timeoutOption(drainMs), Effect.result);
      if (done._tag === "Failure") yield* Effect.logWarning(`${key}'s host in ${name} could not be drained, and is let go of all the same: ${done.failure.message}`);
      else if (Option.isNone(done.success)) yield* Effect.logWarning(`${key}'s host in ${name} had not drained within ${drainMs / 1000} s, and is let go of all the same`);
      else if (!done.success.value) return yield* new HostBusy({ key, pod: name });
    });

  /**
   * Releases `key`'s binding of `pool`, of its pod `only` alone where it says, and none the
   * key was bound to anew since: its host drained first where `drain` says, then its pod ended;
   * told of, once released, as `told` says of its binding, as released for `reason` unless it
   * says otherwise. One whose host something goes on in again as it is drained is kept, as busy.
   */
  const releaseAs = (pool: Pool, key: string, reason: ReleaseReason, drain: boolean, only?: string, told = (binding: string) => releasedFor(pool, key, binding, reason)) => {
    let binding = "";
    return binder.release(pool, key, (bound) =>
      Effect.gen(function*() {
        binding = bound.binding;
        if (drain) yield* drained(pool, key, bound.binding, bound.pod);
        yield* ended(pool, bound.pod);
      }), only).pipe(
      Effect.tap((done) => (done === "released" ? told(binding) : Effect.void)),
      Effect.catchTag("HostBusy", (busy) => Effect.as(Effect.logInfo(busy.message), "busy" as const)),
    );
  };


  const place = (key: string, options: Readonly<Record<string, string>>): Effect.Effect<Placement, PlacementRefused> =>
    Effect.gen(function*() {
      const conversation = conversationOfHarnessKey(key);
      if (!conversation) return yield* new PlacementRefused({ reason: `${key} is no conversation's harness's` });
      const environment = options["environment"] === undefined ? null : yield* environments.named(options["environment"]);
      if (!environment) return yield* new PlacementRefused({ reason: `there is no environment ${options["environment"] ?? "named"}` });
      const harness = options["harness"];
      if (!isHarnessName(harness)) return yield* new PlacementRefused({ reason: `there is no harness ${harness ?? "named"}` });
      const workspace = options["workspace"];
      if (workspace !== "session" && workspace !== "folder") return yield* new PlacementRefused({ reason: `a conversation's workspace is a session or a folder, not ${workspace ?? "unsaid"}` });
      const folders = config.folders;
      if (workspace === "folder" && (environment.name !== main || !folders)) {
        return yield* new PlacementRefused({ reason: `a folder is the machine's own files, of main's alone where it has folder workspaces, not ${environment.name}'s` });
      }
      const [pool, owner]: [Pool, PodOwner] = workspace === "folder" && folders
        ? [{ name: folders.pools[harness], namespace: config.namespace }, folders.owner]
        : [{ name: config.pools[harness], namespace: homeOf(environment.name) }, config.owner];
      return {
        pool,
        // Its host's port, not bayma's, which the front passes its requests on to otherwise.
        port: config.podPort,
        // Its binding beside it, written by its node's agent; what was written for one whose readying failed, which its binder deletes, removed once its containers stopped.
        prepare: (pod, binding) =>
          Effect.gen(function*() {
            const name = pod.metadata?.name ?? "";
            const node = pod.spec?.nodeName ?? "";
            const token = bindingToken(bindingKey, { key, binding, namespace: pool.namespace });
            yield* agents.bindFree(node, { namespace: pool.namespace, pod: name, ...owner, binding: JSON.stringify({ conversationId: conversation, token }) }).pipe(
              Effect.mapError((error) => new BindStepFailed({ reason: error.message })),
              Effect.tapError(() => Effect.forkDetach(detached(pool, node, name).pipe(Effect.catch((error) => Effect.logWarning(`could not remove what was written for ${pool.namespace}/${name}, whose readying failed: ${error.message}`))))),
            );
          }),
      };
    });

  const sweep = yield* makeSweep({
    pools,
    idleness: (lease, pod, now) =>
      Effect.gen(function*() {
        const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
        const conversation = conversationOfHarnessKey(key);
        const host = pod ? hostOf(lease, pod) : null;
        if (!pod || !host || !conversation) return notIdle(now, idlePollMs);
        // Its background work asked only where its status says nothing else goes on.
        const facts = yield* host.status(conversation).pipe(
          Effect.flatMap((status) => (status.busy || status.running ? Effect.succeed({ status, backgroundWork: false }) : Effect.map(host.backgroundWork(conversation), (backgroundWork) => ({ status, backgroundWork })))),
          Effect.timeoutOrElse({ duration: ASK_TIMEOUT_MS, orElse: () => Effect.fail(new HarnessHostError({ reason: `it did not answer within ${ASK_TIMEOUT_MS / 1000} s` })) }),
          Effect.result,
        );
        const binding = lease.metadata?.uid ?? "";
        const name = pod.metadata?.name ?? "";
        if (facts._tag === "Success") {
          if (unreached.delete(binding)) yield* Effect.logInfo(`${key}'s host in ${name} answers again`);
          if (facts.success.backgroundWork) workSeen.set(binding, now);
          return harnessIdleness(facts.success, lease, pod, now, { releaseAfterMs, idlePollMs }, workSeen.get(binding));
        }
        // Not reached, it is not idle, as it may be working, until it has answered no sweep for unreachedMs.
        const reason = facts.failure.message;
        const missed = unreached.get(binding);
        const since = missed?.since ?? now;
        if (missed?.reason !== reason) {
          unreached.set(binding, { since, reason });
          yield* Effect.logWarning(`${key}'s host in ${name} could not be asked whether it is idle (${reason}): it is kept, and let go of as lost once it has answered no sweep for ${unreachedMs / 1000} s`);
        }
        const uid = pod.metadata?.uid ?? "";
        if (now - since >= unreachedMs && !releasing.has(uid)) {
          releasing.add(uid);
          const lostReason = `its host answered no sweep for ${unreachedMs / 1000} s: ${reason}`;
          yield* Effect.logWarning(`${key}'s host in ${name}: ${lostReason}; its pod is ended, and its binding let go of as lost`);
          const pool = { name: pod.metadata?.labels?.[POOL_LABEL] ?? "", namespace: lease.metadata?.namespace ?? "" };
          yield* Effect.forkDetach(releaseAs(pool, key, "lost", false, name, (released) => lostFor(pool, key, released, name, lostReason)).pipe(
            Effect.catch((error) => Effect.andThen(Effect.sync(() => void releasing.delete(uid)), Effect.logWarning(`releasing ${key} failed: ${error.message}`))),
          ));
        }
        return notIdle(now, idlePollMs);
      }),
    release: (pool: Pool, key: string) => releaseAs(pool, key, "idle", true),
  });

  const live: Effect.Effect<void, KubeApiError> = Effect.gen(function*() {
    const listed = new Set<string>();
    for (const pool of yield* pools) {
      const pods = (yield* kube.list("v1", "Pod", pool.namespace, `${BINDING_LABEL},${POOL_LABEL}=${pool.name}`)) as V1Pod[];
      for (const pod of pods) listed.add(pod.metadata?.uid ?? "");
      const held = new Set((yield* Effect.provideService(heldLeases(pool), KubeClient, kube)).map((lease) => lease.spec?.holderIdentity));
      for (const pod of pods) {
        const uid = pod.metadata?.uid ?? "";
        const name = pod.metadata?.name ?? "";
        const key = pod.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
        const deleted = pod.metadata?.deletionTimestamp !== undefined;
        if (!held.has(name) || releasing.has(uid) || !(deleted || hostEnded(pod))) continue;
        releasing.add(uid);
        yield* Effect.logWarning(deleted ? `${key}'s pod ${name} ends, though the gateway did not end it: it is released` : `${key}'s host ended in ${name} (${endedHow(pod)}), which serves its binding no more: it is released, the next bind making another`);
        // Its host drained where it may yet answer; one ending, or ended, is not. One the kernel
        // killed for its memory is told so, with the limit it ran out of, as what it ran would run
        // out of it again.
        const memoryLimit = deleted ? null : killedForMemory(pod);
        const reason = deleted ? "deleted" : memoryLimit === null ? "broken" : "oom";
        yield* Effect.forkDetach(releaseAs(pool, key, reason, false, name, (binding) => releasedFor(pool, key, binding, reason, memoryLimit ?? undefined)).pipe(
          Effect.catch((error) => Effect.andThen(Effect.sync(() => void releasing.delete(uid)), Effect.logWarning(`releasing ${key} failed: ${error.message}`))),
        ));
      }
    }
    for (const uid of releasing) if (!listed.has(uid)) releasing.delete(uid);
  });

  const lost = (pool: Pool, lease: BindingLease): Effect.Effect<void> =>
    lostFor(pool, lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "", lease.metadata?.uid ?? "", lease.spec?.holderIdentity ?? "", "its pod went while it was bound");

  // Of the binding's own conversation, told its environment's alasio; a binding of a namespace no environment's harnesses are in tells no one.
  const relay = (bound: HeldTo, body: Uint8Array): Effect.Effect<Answer> =>
    Effect.gen(function*() {
      const conversation = conversationOfHarnessKey(bound.key);
      const environment = yield* environmentOf(bound.namespace);
      if (!conversation || environment === null) return { status: 403, json: { error: `${bound.key} is no harness's of an environment here` } };
      return yield* relayed(conversation, body, ({ kind, subject }) => events.tell({ kind, environment, source: "harness", subject }));
    });

  /** Each pool's harness, by the pool's name, as the configuration names them. */
  const harnessOfPool = new Map<string, string>([...Object.entries(config.pools), ...Object.entries(config.folders?.pools ?? {})].map(([harness, pool]) => [pool, harness]));
  /** The bound harnesses of `environment`'s pools, each host asked now, at once, as the module says. */
  const listOf = (environment: string): Effect.Effect<readonly ListedHarness[], KubeApiError> =>
    Effect.gen(function*() {
      const of = [...sessionPools(environment), ...(environment === main ? folderPools : [])];
      const listed = yield* Effect.forEach(of, (pool) =>
        Effect.flatMap(Effect.provideService(heldLeases(pool), KubeClient, kube), (leases) =>
          Effect.forEach(leases, (lease) =>
            Effect.gen(function*() {
              const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
              const conversation = conversationOfHarnessKey(key) ?? key;
              const name = lease.spec?.holderIdentity ?? "";
              const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
              const host = pod ? hostOf(lease, pod) : null;
              const asked = host
                ? yield* Effect.all([host.status(conversation), host.backgroundWork(conversation)]).pipe(
                  Effect.timeoutOrElse({ duration: ASK_TIMEOUT_MS, orElse: () => Effect.fail(new HarnessHostError({ reason: `it did not answer within ${ASK_TIMEOUT_MS / 1000} s` })) }),
                  Effect.map(([status, background]): ListedHarness["host"] => ({ running: status.running, background, idleSince: status.idleSince === null ? null : new Date(status.idleSince).toISOString() })),
                  Effect.catch((error) => Effect.succeed<ListedHarness["host"]>({ unreached: error.message })),
                )
                : { unreached: pod ? "its pod has no address" : "its pod is gone" };
              const created = lease.metadata?.creationTimestamp;
              return { conversation, harness: harnessOfPool.get(pool.name) ?? "", pool: pool.name, pod: name, boundAt: created ? new Date(created).toISOString() : null, host: asked } satisfies ListedHarness;
            }), { concurrency: "unbounded" })), { concurrency: "unbounded" });
      return listed.flat();
    });

  /** `GET /harnesses` of the caller's environment, as the module says; 403 to one of no environment there is, and 404 to anything else under /harnesses. */
  const operate = (claims: TokenClaims, { method, path }: Operation): Effect.Effect<Answer> =>
    Effect.gen(function*() {
      const named = claims.options?.["environment"];
      const environment = named === undefined ? null : yield* environments.named(named);
      if (!environment) return { status: 403, json: { error: `there is no environment ${named ?? "named"}` } };
      if (method !== "GET" || path !== "/harnesses") return { status: 404, json: { error: `no ${method} ${path}` } };
      // A branch made before the harness pools were turned on has none, nor the gateway's Role there.
      const pooled = environment.name === main || (yield* hasPools(environment.name));
      return { status: 200, json: { harnesses: pooled ? yield* listOf(environment.name) : [] } };
    }).pipe(Effect.catch((error) => Effect.succeed({ status: 503, json: { error: `the bound harnesses could not be read: ${error.message}` } })));

  return { place, pools, namespaces, ownsNamespace, sweep, live, lost, relay, operate };
});
