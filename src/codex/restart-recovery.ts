import { Effect } from "effect";

import { buildRestartSyntheticText } from "../operator/restart-prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";

/** Where the turns a restart cut short, and the restarts themselves, are kept: alasio's store. */
export type RestartRecoveryStore = Pick<Store["Service"], "getActiveTurns" | "getRestartEvent" | "markPendingAsPosted" | "clearActiveTurn" | "stageRestartRecovery">;

/**
 * What becomes, as alasio starts, of the turns the last alasio was running when it
 * stopped: a turn whose restart was recorded is continued by a prompt of its own, which
 * says what restarted it; one without is let go of.
 */
export const recoverInterruptedTurns = Effect.fnUntraced(function*(store: RestartRecoveryStore): Effect.fn.Return<void, StoreError> {
  for (const turn of yield* store.getActiveTurns) {
    const conversationId = turn.thread_key;
    const restartEvent = yield* store.getRestartEvent(conversationId);
    if (!restartEvent) {
      if (turn.pending_response_id) {
        yield* store.markPendingAsPosted(turn.pending_response_id);
      }
      yield* store.clearActiveTurn(conversationId);
      continue;
    }
    yield* store.stageRestartRecovery({ turn, prompt: buildRestartSyntheticText(restartEvent.cause, turn.harness) });
  }
});
