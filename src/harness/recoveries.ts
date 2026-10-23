/**
 * alasio's count of the turns whose harness instance was lost under them, each taken up once
 * (Store.stageLostRecovery): by harness; by what found it lost, its binding's loss as the
 * sandbox gateway told it (./host-events.ts), or alasio finding its pod knew nothing of it, or
 * could not be reached, as it attached to it again (../codex/turns.ts); and by how it came
 * out: run again (`staged`), its reply delivered, as its pod completed it first
 * (`completed`), or given up, as a turn that was itself run again is (`given-up`).
 */
import { Effect } from "effect";

import type { LostRecoveryOutcome } from "../persistence/store.ts";
import { meter } from "../telemetry/index.ts";

export const RECOVERIES = "alasio.harness.recoveries";
const recoveries = meter.createCounter(RECOVERIES, {
  description: "Turns whose harness instance was lost under them, by harness, what found them lost (lost: their binding's loss; reattach: alasio attaching again), and outcome (staged: run again; completed: their reply delivered; given-up)",
  unit: "{turn}",
});

/** What found a turn's harness instance lost. */
export type RecoveredBy = "lost" | "reattach";

/** Counts `recovery`, found by `by`; nothing where there was no turn to take up. */
export const countRecovery = (recovery: LostRecoveryOutcome, by: RecoveredBy): Effect.Effect<void> =>
  recovery.outcome === "none"
    ? Effect.void
    : Effect.sync(() => recoveries.add(1, { "alasio.harness": recovery.turn.harness, "alasio.harness.recovery.by": by, "alasio.harness.recovery.outcome": recovery.outcome }));
