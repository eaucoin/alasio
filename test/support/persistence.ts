/**
 * A turn's writes, as the harnesses' tests stand them in for alasio's store: one that
 * keeps nothing, and the turns a harness starts on its own claimed onto a given one.
 */
import { Effect } from "effect";

import type { OwnTurns } from "../../src/harness/index.ts";
import type { TurnPersistence } from "../../src/persistence/store.ts";

/** A turn's persistence that keeps nothing: what the turn stores is not what a test looks at. */
export const forgetfulPersistence: TurnPersistence = {
  epoch: 1,
  createPendingResponse: () => Effect.succeed("pending-1"),
  updateActiveTurnPendingResponseId: () => Effect.void,
  updatePendingSessionId: () => Effect.void,
  updateActiveTurnSessionId: () => Effect.void,
  appendBlocksToPending: () => Effect.void,
  markPendingResponseComplete: () => Effect.void,
  markPendingAsPosted: () => Effect.void,
  updateSessionUsage: () => Effect.void,
  setWaiting: () => Effect.void,
  recordRestartEvent: () => Effect.void,
};

/** The turns a harness starts on its own, each claimed onto `persistence`, its end recorded in `ended` where given. */
export const ownTurnsOn = (persistence: TurnPersistence, ended: string[] = []): OwnTurns => ({
  claim: () => Effect.succeed({ persistence, end: (outcome) => Effect.sync(() => void ended.push(outcome)) }),
});
