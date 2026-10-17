/**
 * The sandbox gateway's session workspaces, on the binder (../binder/): a session's
 * workspace, `sessionfs:<volume id>`, bound to a pod of its environment's pool of its
 * internet (`none` or `full`), as its caller's token says (`environment`, `mode`), of the
 * environments there are (./environments.ts); the pod
 * readied by the node agent of its node attaching the workspace into it, with the binding's
 * token beside it, which the pod's bayma starts on; and released once its bayma has run no
 * exec, and no request of it has come, for `releaseAfterMs`: its sandbox saved whole by its
 * node's agent, which ends it, the workspace detached once it has ended, then the pod deleted,
 * so a pod serves one binding.
 *
 * Whether a bound pod is idle is asked of its bayma (`GET /status`), at most once every
 * IDLE_POLL_MS a pod, and not before it could be idle for long enough, as the last exec it
 * ran and the last request of it say: at most four requests a minute of each bound pod's,
 * and one a release's wait for most.
 */
import { lookup } from "node:dns/promises";

import type { V1Deployment, V1Pod } from "@kubernetes/client-node";
import { Clock, Effect, Schema } from "effect";

import { type Binder, BindStepFailed } from "../binder/binder.ts";
import { activeAt, type BindingLease, heldLeases, KEY_ANNOTATION, type Pool } from "../binder/bindings.ts";
import { type Placement, PlacementRefused } from "../binder/server.ts";
import { bindingToken } from "../binder/tokens.ts";
import { type KubeApiError, KubeClient } from "../kube/client.ts";
import type { NodeAgentError, NodeAgents } from "../sandbox/node-agents.ts";
import { BAYMA_CONTAINER } from "../kube/sandbox-shape.ts";
import { volumeIdOfKey } from "../sandbox/names.ts";
import { hookOf } from "../sandbox/snapshots.ts";
import type { Environments } from "./environments.ts";

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
}

/** How often a bound pod's bayma is asked whether it is idle, at most. */
export const IDLE_POLL_MS = 15_000;
/** How long an ask of a bayma's status may take. */
const STATUS_TIMEOUT_MS = 5_000;
/** How long a pod's containers are waited for to stop, once its deletion is asked. */
const STOP_TIMEOUT_MS = 120_000;

/** What bayma's /status says: its execs running and queued, and since when none has run, if none does. */
const BaymaStatus = Schema.Struct({ execsRunning: Schema.Number, execsQueued: Schema.Number, idleSinceMs: Schema.NullOr(Schema.Number) });

/** The runtime's ids of `pod`'s containers, as its status names them. */
const containerIdsOf = (pod: V1Pod): string[] =>
  (pod.status?.containerStatuses ?? []).flatMap(({ containerID }) => (containerID ? [containerID.replace(/^[a-z]+:\/\//u, "")] : []));

/** The gateway's sessions; see makeSessions. */
export interface Sessions {
  /** Where `key` is bound, by its caller's token's options, and how its pod is readied. */
  readonly place: (key: string, options: Readonly<Record<string, string>>) => Effect.Effect<Placement, PlacementRefused>;
  /** Every pool the gateway binds in now, of every environment there is. */
  readonly pools: Effect.Effect<readonly Pool[]>;
  /** Releases each bound pod of the gateway's pools that is idle by `now`, as the module says, and says which. */
  readonly sweep: Effect.Effect<readonly string[], KubeApiError>;
}

/** The gateway's sessions, as the module says, on the node agents, the binder, and the KubeClient. */
export const makeSessions = Effect.fnUntraced(function*(
  { environments, pools: poolNames, releaseAfterMs, podPort, bindingKey, idlePollMs = IDLE_POLL_MS, telemetry }: SessionsOptions,
  agents: NodeAgents,
  binder: Binder["Service"],
): Effect.fn.Return<Sessions, never, KubeClient> {
  const kube = yield* KubeClient;
  const pools = Effect.map(environments.current, (current) => current.flatMap(({ namespace }) => (Object.values(poolNames) as string[]).map((name): Pool => ({ name, namespace }))));
  /** The address `host` resolves to. */
  const addressOf = (host: string): Effect.Effect<string, BindStepFailed> =>
    Effect.tryPromise({ try: () => lookup(host), catch: (error) => new BindStepFailed({ reason: `${host} did not resolve: ${String(error)}` }) }).pipe(Effect.map(({ address }) => address));

  /** When each bound key is next asked whether it is idle, by its binding. */
  const due = new Map<string, number>();

  const place = (key: string, options: Readonly<Record<string, string>>): Effect.Effect<Placement, PlacementRefused> =>
    Effect.gen(function*() {
      const volumeId = volumeIdOfKey(key);
      if (!volumeId) return yield* new PlacementRefused({ reason: `${key} is no session workspace's` });
      const environment = options["environment"] === undefined ? null : yield* environments.named(options["environment"]);
      if (!environment) return yield* new PlacementRefused({ reason: `there is no environment ${options["environment"] ?? "named"} of sessions` });
      const mode = options["mode"];
      if (mode !== "none" && mode !== "full") return yield* new PlacementRefused({ reason: `a session's internet is none or full, not ${mode ?? "unsaid"}` });
      return {
        pool: { name: poolNames[mode], namespace: environment.namespace },
        // Its workspace attached, its binding's token beside it, on which its bayma starts.
        prepare: (pod, binding) =>
          Effect.gen(function*() {
            const env = telemetry ? { ...telemetry.env, OTEL_EXPORTER_OTLP_ENDPOINT: `http://${yield* addressOf(telemetry.host)}:${telemetry.port}` } : {};
            yield* agents.attach(pod.spec?.nodeName ?? "", {
              environment: environment.namespace,
              workspace: volumeId,
              pod: pod.metadata?.name ?? "",
              token: bindingToken(bindingKey, { key, binding, namespace: environment.namespace }),
              env,
            }).pipe(Effect.mapError((error) => new BindStepFailed({ reason: error.message })));
          }),
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
   * can be detached, then detaches it. Under gVisor, its node's agent saves it whole first
   * (its release's snapshot, bayma told by its hook), which ends it, its pool's template
   * recorded for its restore; a save that fails and leaves it running fails the release, which
   * keeps the binding; one that ended it anyway lost what ran in it but bayma's checkpoints,
   * and the release goes on. Under another runtime, its pod's deletion ends it. Then the
   * workspace is detached, its containers stopped, for the binder to delete the pod.
   */
  const endAndDetach = (pool: Pool, volumeId: string, name: string): Effect.Effect<void, KubeApiError | NodeAgentError | BindStepFailed> =>
    Effect.gen(function*() {
      const pod = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
      if (!pod) return;
      const node = pod.spec?.nodeName ?? "";
      const containers = containerIdsOf(pod);
      const bayma = pod.spec?.containers.find((container) => container.name === BAYMA_CONTAINER);
      const containerId = pod.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.containerID?.replace(/^[a-z]+:\/\//u, "");
      const hook = bayma ? hookOf(bayma) : null;
      if (pod.spec?.runtimeClassName && hook && containerId) {
        const deployment = (yield* kube.read("apps/v1", "Deployment", pool.namespace, pool.name)) as V1Deployment | null;
        const taken = yield* agents.checkpoint(node, {
          environment: pool.namespace,
          workspace: volumeId,
          namespace: pool.namespace,
          pod: name,
          containerId,
          hook,
          podTemplate: { ...deployment?.spec?.template },
        }).pipe(Effect.result);
        if (taken._tag === "Failure") {
          const after = (yield* kube.read("v1", "Pod", pool.namespace, name)) as V1Pod | null;
          const ended = after?.status?.containerStatuses?.find((status) => status.name === BAYMA_CONTAINER)?.state?.terminated !== undefined;
          if (!ended) return yield* new BindStepFailed({ reason: `its sandbox was not saved, and runs on: ${taken.failure.message}` });
          yield* Effect.logWarning(`${volumeId}'s sandbox was not saved, and ended: what ran in it comes back from bayma's checkpoints: ${taken.failure.message}`);
        }
      } else {
        yield* kube.remove("v1", "Pod", pool.namespace, name);
      }
      yield* stopped(pool, name);
      yield* agents.detach(node, name, containers);
    });

  /** Whether the binding `lease`, of its pod `pod`, is idle at `now`; where not, when it is next to be asked. */
  const idleness = (lease: BindingLease, pod: V1Pod | null, now: number): Effect.Effect<{ readonly idle: true } | { readonly idle: false; readonly askAt: number }> =>
    Effect.gen(function*() {
      const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
      const address = pod?.status?.podIP;
      if (!address) return { idle: false, askAt: now + idlePollMs } as const;
      const token = bindingToken(bindingKey, { key, binding: lease.metadata?.uid ?? "", namespace: lease.metadata?.namespace ?? "" });
      const status = yield* Effect.tryPromise({
        try: async (signal) => {
          const response = await fetch(`http://${address}:${podPort}/status`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.any([signal, AbortSignal.timeout(STATUS_TIMEOUT_MS)]) });
          if (!response.ok) throw new Error(`it answered ${response.status}`);
          return await response.text();
        },
        catch: (cause) => new BindStepFailed({ reason: cause instanceof Error ? cause.message : String(cause) }),
      }).pipe(Effect.flatMap((text) => Schema.decodeUnknownEffect(Schema.fromJsonString(BaymaStatus))(text)), Effect.option);
      if (status._tag === "None" || status.value.execsRunning > 0 || status.value.execsQueued > 0 || status.value.idleSinceMs === null) {
        return { idle: false, askAt: now + idlePollMs } as const;
      }
      // Idle since its last exec, its last request, or its binding, whichever was last.
      const bound = Date.parse(String(pod?.metadata?.creationTimestamp ?? "")) || 0;
      const since = Math.max(status.value.idleSinceMs, activeAt(lease) ?? 0, bound);
      return now - since >= releaseAfterMs ? ({ idle: true } as const) : ({ idle: false, askAt: Math.max(now + idlePollMs, since + releaseAfterMs) } as const);
    });

  const sweep: Effect.Effect<readonly string[], KubeApiError> = Effect.gen(function*() {
    const released: string[] = [];
    for (const pool of yield* pools) {
      for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube)) {
        const key = lease.metadata?.annotations?.[KEY_ANNOTATION];
        const binding = lease.metadata?.uid ?? "";
        const now = yield* Clock.currentTimeMillis;
        if (!key || (due.get(binding) ?? 0) > now) continue;
        const pod = (yield* kube.read("v1", "Pod", pool.namespace, lease.spec?.holderIdentity ?? "")) as V1Pod | null;
        const asked = yield* idleness(lease, pod, now);
        if (!asked.idle) {
          due.set(binding, asked.askAt);
          continue;
        }
        due.delete(binding);
        const done = yield* binder.release(pool, key, ({ pod: name }) => endAndDetach(pool, volumeIdOfKey(key) ?? "", name)).pipe(
          Effect.catch((error) => Effect.logWarning(`releasing ${key} failed: ${error.message}`).pipe(Effect.as(false))),
        );
        if (done) released.push(key);
      }
    }
    return released;
  });

  return { place, pools, sweep };
});
