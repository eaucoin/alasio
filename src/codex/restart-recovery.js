import { buildRestartSyntheticText } from "../operator/restart-prompts.js";

export class RestartRecovery {
  constructor({ store }) {
    this.store = store;
  }

  async recoverInterruptedTurns() {
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

  recordExternalRestartEventsForActiveTurns() {
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
