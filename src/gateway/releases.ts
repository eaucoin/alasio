/**
 * The gateway's count of the bindings it released, of session workspaces' and of folder
 * instances' and conversations' harnesses' alike (./sessions.ts, ./folders.ts, ./harnesses.ts),
 * by their pool, as the binder's bind time is (../binder/binder.ts BIND_DURATION), their
 * namespace, and why; and what tells one released as `oom`.
 */
import type { V1Pod } from "@kubernetes/client-node";
import { Effect } from "effect";

import type { Pool } from "../binder/names.ts";
import { meter } from "../telemetry/index.ts";

export const RELEASES = "alasio.gateway.releases";
const releases = meter.createCounter(RELEASES, { description: "Bindings released, by pool and why: idle, asked by their caller, lost as their pod went, deleted as their pod was ended by other than the gateway, broken, or oom, as its main process was killed for its memory" });

/** Why a binding was released. */
export type ReleaseReason = "idle" | "asked" | "lost" | "deleted" | "broken" | "oom";

/** Counts a binding of `pool` released, for `reason`. */
export const released = (pool: Pool, reason: ReleaseReason): Effect.Effect<void> =>
  Effect.sync(() => releases.add(1, { "alasio.binder.pool": pool.name, "k8s.namespace.name": pool.namespace, "alasio.gateway.release.reason": reason }));

/**
 * Whether the kernel killed `pod`'s container `container`'s own process for its memory, as
 * kubelet says it last ended: OOMKilled, and of SIGKILL (137). containerd says OOMKilled of a
 * container one of whose processes the kernel killed so at any time, as one of its children
 * killed alone (cli/src/cluster/k3s.ts singleProcessOOMKill), however its own process ended
 * after, so one that ended otherwise is not taken for one killed for its memory.
 */
export const killedForMemory = (pod: V1Pod, container: string): boolean => {
  const status = pod.status?.containerStatuses?.find(({ name }) => name === container);
  const ended = status?.state?.terminated ?? status?.lastState?.terminated;
  return ended?.reason === "OOMKilled" && ended.exitCode === 137;
};

/** The memory limit of `pod`'s container `container`, as its spec gives it: null where it has none. */
export const memoryLimitOf = (pod: V1Pod, container: string): string | null =>
  pod.spec?.containers.find(({ name }) => name === container)?.resources?.limits?.["memory"] ?? null;
