import { CODEX_HARNESS, isHarnessName } from "../harness/names.js";

export class SqliteTurnRepository {
  constructor(db, conversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  upsertActiveTurn(turn) {
    const threadKey = turn.threadKey;
    const harness = isHarnessName(turn.harness) ? turn.harness : this.conversations.getActiveHarness(threadKey);
    this.db.prepare(`
      insert into turns (id, conversation_id, thread_key, channel, thread_ts, session_id, harness, pending_response_id, prompt, state, started_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
      on conflict(id) do update set
        conversation_id = excluded.conversation_id,
        thread_key = excluded.thread_key,
        channel = excluded.channel,
        thread_ts = excluded.thread_ts,
        session_id = excluded.session_id,
        harness = excluded.harness,
        pending_response_id = excluded.pending_response_id,
        prompt = excluded.prompt,
        state = 'active',
        started_at = excluded.started_at,
        completed_at = null
    `).run(
      threadKey,
      threadKey,
      threadKey,
      turn.chatId,
      turn.messageId,
      turn.sessionId ?? null,
      harness,
      turn.pendingResponseId ?? null,
      turn.prompt ?? null,
      turn.startedAt ?? Date.now() / 1000,
    );
  }

  updateActiveTurnSessionId(threadKey, sessionId) {
    this.db.prepare("update turns set session_id = ? where id = ? and state = 'active'").run(sessionId, threadKey);
    const activeTurn = this.db.prepare("select harness from turns where id = ? and state = 'active'").get(threadKey);
    const harness = isHarnessName(activeTurn?.harness) ? activeTurn.harness : this.conversations.getActiveHarness(threadKey);
    this.conversations.setHarnessSessionId(threadKey, harness ?? CODEX_HARNESS, sessionId);
  }

  updateActiveTurnPendingResponseId(threadKey, pendingResponseId) {
    this.db.prepare("update turns set pending_response_id = ? where id = ? and state = 'active'").run(pendingResponseId, threadKey);
  }

  clearActiveTurn(threadKey, pendingResponseId = null) {
    if (pendingResponseId) {
      this.db.prepare(`
        update turns set state = 'completed', completed_at = ?
        where id = ? and state = 'active' and pending_response_id = ?
      `).run(Date.now() / 1000, threadKey, pendingResponseId);
      return;
    }
    this.db.prepare("update turns set state = 'completed', completed_at = ? where id = ? and state = 'active'").run(Date.now() / 1000, threadKey);
  }

  getActiveTurns() {
    return this.db.prepare("select * from turns where state = 'active' order by started_at asc").all();
  }
}
