/**
 * The gateway's count of the bindings it released, of session workspaces' and of folder
 * instances' and conversations' harnesses' alike (./sessions.ts, ./folders.ts, ./harnesses.ts),
 * by their pool, as the binder's bind time is (../binder/binder.ts BIND_DURATION), their
 * namespace, and why.
 */
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
