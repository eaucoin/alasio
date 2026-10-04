import { Clock, Effect } from "effect";

import { buildRestartSyntheticText } from "../operator/restart-prompts.ts";
import type { SqliteStore } from "../persistence/store.ts";

/** Where the turns a restart cut short, and the restarts themselves, are kept: alasio's store. */
export type RestartRecoveryStore =
  & Pick<SqliteStore, "getActiveTurns" | "getRestartEvent" | "recordRestartEvent" | "markPendingAsPosted" | "clearActiveTurn" | "stageRestartRecovery">
  & Partial<Pick<SqliteStore, "getActiveHarness">>;

/**
 * What becomes, as alasio starts, of the turns the last alasio was running when it
 * stopped: a turn whose restart was recorded is continued by a prompt of its own, which
 * says what restarted it; one without is let go of.
 */
export const recoverInterruptedTurns = (store: RestartRecoveryStore): Effect.Effect<void> =>
  Effect.sync(() => {
    for (const turn of store.getActiveTurns()) {
      const conversationId = turn.thread_key;
      const restartEvent = store.getRestartEvent(conversationId);
      if (!restartEvent) {
        if (turn.pending_response_id) {
          store.markPendingAsPosted(turn.pending_response_id);
        }
        store.clearActiveTurn(conversationId);
        continue;
      }
      store.stageRestartRecovery({
        turn,
        prompt: buildRestartSyntheticText(restartEvent.cause, turn.harness ?? store.getActiveHarness?.(conversationId)),
      });
    }
  });

/**
 * Records that the conversation's running turn was cut short by a restart alasio cannot
 * attribute, unless what restarted it is already recorded (the agent's own command).
 */
export const recordExternalRestartEvent = Effect.fnUntraced(function*(store: RestartRecoveryStore, conversationId: string): Effect.fn.Return<void> {
  const turn = store.getActiveTurns().find((active) => active.thread_key === conversationId);
  if (!turn || store.getRestartEvent(conversationId)) {
    return;
  }
  store.recordRestartEvent({
    cause: "external_or_unknown",
    thread_key: conversationId,
    channel: turn.channel,
    thread_ts: turn.thread_ts,
    session_id: turn.session_id ?? null,
    timestamp: (yield* Clock.currentTimeMillis) / 1000,
  });
});
