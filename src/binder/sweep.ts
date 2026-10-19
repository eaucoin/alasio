/**
 * Releasing bindings once they are idle, for whatever binds through the binder: each held
 * Lease of the pools there are now looked at, its pod asked whether it is idle as `idleness`
 * says, and released, as `release` does, once it is; a pod that is not is asked again only
 * once the time it gives has come, so a sweep run often asks each pod seldom.
 *
 * How bayma says it is idle (baymaIdleness): its `GET /status`, with the binding's token, at
 * most once every `idlePollMs` a pod, and not before it could be idle for long enough, as the
 * last exec it ran, the last request of it (the Lease's active stamp), and its binding say.
 */
import type { V1Pod } from "@kubernetes/client-node";
import { Clock, Effect, Schema } from "effect";

import { type KubeApiError, KubeClient } from "../kube/client.ts";
import type { Released } from "./binder.ts";
import { activeAt, type BindingLease, heldLeases } from "./bindings.ts";
import { KEY_ANNOTATION, type Pool } from "./names.ts";
import { bindingToken } from "./tokens.ts";

/** Whether a bound pod is idle now; where not, when it is next to be asked. */
export type Idleness = { readonly idle: true } | { readonly idle: false; readonly askAt: number };

/** How often a bound pod's bayma is asked whether it is idle, at most, by default. */
export const IDLE_POLL_MS = 15_000;
/** How long an ask of a bayma's status may take. */
const STATUS_TIMEOUT_MS = 5_000;

/** What bayma's /status says: its execs running and queued, and since when none has run, if none does. */
const BaymaStatus = Schema.Struct({ execsRunning: Schema.Number, execsQueued: Schema.Number, idleSinceMs: Schema.NullOr(Schema.Number) });

/** What baymaIdleness is given: the port bound pods' bayma serves on, how long one is kept idle, how often it is asked at most, and the key its binding's token is signed with. */
export interface BaymaIdlenessOptions {
  readonly podPort: number;
  readonly releaseAfterMs: number;
  readonly idlePollMs?: number;
  readonly bindingKey: Buffer;
}

/** Whether the binding `lease`, of its pod `pod`, is idle at `now`, as its bayma says, as the module says. */
export const baymaIdleness = ({ podPort, releaseAfterMs, idlePollMs = IDLE_POLL_MS, bindingKey }: BaymaIdlenessOptions) =>
(lease: BindingLease, pod: V1Pod | null, now: number): Effect.Effect<Idleness> =>
  Effect.gen(function*() {
    const key = lease.metadata?.annotations?.[KEY_ANNOTATION] ?? "";
    const address = pod?.status?.podIP;
    if (!address) return { idle: false, askAt: now + idlePollMs } as const;
    const token = bindingToken(bindingKey, { key, binding: lease.metadata?.uid ?? "", namespace: lease.metadata?.namespace ?? "" });
    const status = yield* Effect.tryPromise(async (signal) => {
      const response = await fetch(`http://${address}:${podPort}/status`, { headers: { authorization: `Bearer ${token}` }, signal: AbortSignal.any([signal, AbortSignal.timeout(STATUS_TIMEOUT_MS)]) });
      if (!response.ok) throw new Error(`it answered ${response.status}`);
      return await response.text();
    }).pipe(Effect.flatMap((text) => Schema.decodeUnknownEffect(Schema.fromJsonString(BaymaStatus))(text)), Effect.option);
    if (status._tag === "None" || status.value.execsRunning > 0 || status.value.execsQueued > 0 || status.value.idleSinceMs === null) {
      return { idle: false, askAt: now + idlePollMs } as const;
    }
    // Idle since its last exec, its last request, or its binding, whichever was last.
    const bound = Date.parse(String(pod?.metadata?.creationTimestamp ?? "")) || 0;
    const since = Math.max(status.value.idleSinceMs, activeAt(lease) ?? 0, bound);
    return now - since >= releaseAfterMs ? ({ idle: true } as const) : ({ idle: false, askAt: Math.max(now + idlePollMs, since + releaseAfterMs) } as const);
  });

/** What makeSweep is given: the pools there are now, how a bound pod is found idle, and how a binding is released, and how that went. */
export interface SweepOptions<E> {
  readonly pools: Effect.Effect<readonly Pool[]>;
  readonly idleness: (lease: BindingLease, pod: V1Pod | null, now: number) => Effect.Effect<Idleness>;
  readonly release: (pool: Pool, key: string, lease: BindingLease) => Effect.Effect<Released, E>;
}

/** A sweep, as the module says: run, it releases each binding idle now, and says which keys; a release that fails is logged, and asked of again at the next. */
export const makeSweep = Effect.fnUntraced(function*<E extends { readonly message: string }>({ pools, idleness, release }: SweepOptions<E>): Effect.fn.Return<Effect.Effect<readonly string[], KubeApiError>, never, KubeClient> {
  const kube = yield* KubeClient;
  /** When each binding is next asked whether it is idle, by its Lease's uid. */
  const due = new Map<string, number>();
  return Effect.gen(function*() {
    const released: string[] = [];
    const seen = new Set<string>();
    for (const pool of yield* pools) {
      for (const lease of yield* Effect.provideService(heldLeases(pool), KubeClient, kube)) {
        const key = lease.metadata?.annotations?.[KEY_ANNOTATION];
        const binding = lease.metadata?.uid ?? "";
        seen.add(binding);
        const now = yield* Clock.currentTimeMillis;
        if (!key || (due.get(binding) ?? 0) > now) continue;
        const pod = (yield* kube.read("v1", "Pod", pool.namespace, lease.spec?.holderIdentity ?? "")) as V1Pod | null;
        const asked = yield* idleness(lease, pod, now);
        if (!asked.idle) {
          due.set(binding, asked.askAt);
          continue;
        }
        due.delete(binding);
        const done = yield* release(pool, key, lease).pipe(Effect.catch((error) => Effect.logWarning(`releasing ${key} failed: ${error.message}`).pipe(Effect.as("busy" as const))));
        if (done === "released") released.push(key);
      }
    }
    // Of a binding let go meanwhile, as by another replica, nothing more to ask.
    for (const binding of [...due.keys()]) if (!seen.has(binding)) due.delete(binding);
    return released;
  });
});
