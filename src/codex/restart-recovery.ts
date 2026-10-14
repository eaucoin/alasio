import { Effect } from "effect";

import { harnessDisplayName } from "../harness/names.ts";
import { buildRestartSyntheticText } from "../operator/restart-prompts.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import type { Turn } from "../persistence/turn-repository.ts";

/** Where the turns a restart cut short, and the restarts themselves, are kept: alasio's store. */
export type RestartRecoveryStore = Pick<Store["Service"], "getActiveTurns" | "getRestartEvent" | "markPendingAsPosted" | "endTurn" | "stageRestartRecovery">;

/**
 * What becomes, as alasio starts, of the turns the last alasio was running when it
 * stopped: a turn whose restart was recorded is continued by a prompt of its own, which
 * says what restarted it. One without, which alasio's stopping did not get to leave for
 * the restart (it was killed, or crashed), is ended as stopped, its response let go of,
 * and its status message, which still says it works, says so (`editStatus`).
 */
export const recoverInterruptedTurns = Effect.fnUntraced(function*(
  store: RestartRecoveryStore,
  editStatus: (turn: Turn, text: string) => Effect.Effect<void>,
): Effect.fn.Return<void, StoreError> {
  for (const turn of yield* store.getActiveTurns) {
    const conversationId = turn.thread_key;
    const restartEvent = yield* store.getRestartEvent(conversationId);
    if (!restartEvent) {
      if (turn.pending_response_id) {
        yield* store.markPendingAsPosted(turn.pending_response_id);
      }
      yield* store.endTurn(conversationId, Number(turn.epoch), "stopped");
      yield* editStatus(turn, `alasio stopped during this turn, and ${harnessDisplayName(turn.harness)} did not finish it. Send the message again to run it.`);
      continue;
    }
    yield* store.stageRestartRecovery({ turn, prompt: buildRestartSyntheticText(restartEvent.cause, turn.harness) });
  }
});
