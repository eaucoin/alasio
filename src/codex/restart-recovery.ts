import { buildRestartSyntheticText } from "../operator/restart-prompts.ts";
import type { SqliteStore } from "../persistence/store.ts";

/** Where the turns a restart cut short, and the restarts themselves, are kept: alasio's store. */
export type RestartRecoveryStore =
  & Pick<SqliteStore, "getActiveTurns" | "getRestartEvent" | "recordRestartEvent" | "markPendingAsPosted" | "clearActiveTurn" | "stageRestartRecovery">
  & Partial<Pick<SqliteStore, "getActiveHarness">>;

export class RestartRecovery {
  private readonly store: RestartRecoveryStore;

  constructor({ store }: { readonly store: RestartRecoveryStore }) {
    this.store = store;
  }

  async recoverInterruptedTurns(): Promise<void> {
    const activeTurns = this.store.getActiveTurns();
    for (const turn of activeTurns) {
      const conversationId = turn.thread_key;
      const restartEvent = this.store.getRestartEvent(conversationId);
      if (!restartEvent) {
        if (turn.pending_response_id) {
          this.store.markPendingAsPosted(turn.pending_response_id);
        }
        this.store.clearActiveTurn(conversationId);
        continue;
      }
      this.store.stageRestartRecovery({
        turn,
        prompt: buildRestartSyntheticText(restartEvent.cause, turn.harness ?? this.store.getActiveHarness?.(conversationId)),
      });
    }
  }

  recordExternalRestartEventsForActiveTurns(): void {
    for (const turn of this.store.getActiveTurns()) {
      const conversationId = turn.thread_key;
      if (this.store.getRestartEvent(conversationId)) {
        continue;
      }
      this.store.recordRestartEvent({
        cause: "external_or_unknown",
        thread_key: conversationId,
        channel: turn.channel,
        thread_ts: turn.thread_ts,
        session_id: turn.session_id ?? null,
        timestamp: Date.now() / 1000,
      });
    }
  }
}
