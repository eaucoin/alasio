/**
 * The gateway's count of the bindings it released, of session workspaces' and of folder
 * instances' alike (./sessions.ts, ./folders.ts), by their namespace and why.
 */
import { Effect } from "effect";

import type { Pool } from "../binder/names.ts";
import { meter } from "../telemetry/index.ts";

export const RELEASES = "alasio.gateway.releases";
const releases = meter.createCounter(RELEASES, { description: "Bindings released, by why: idle, asked by their caller, lost as their pod went, deleted as their pod was ended by other than the gateway, or broken" });

/** Why a binding was released. */
export type ReleaseReason = "idle" | "asked" | "lost" | "deleted" | "broken";

/** Counts a binding of `pool` released, for `reason`. */
export const released = (pool: Pool, reason: ReleaseReason): Effect.Effect<void> =>
  Effect.sync(() => releases.add(1, { "k8s.namespace.name": pool.namespace, "alasio.gateway.release.reason": reason }));
