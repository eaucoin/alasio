import { newId } from "../shared/ids.js";

export class SqliteOutboxRepository {
  constructor(db, conversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  enqueueText({ chatId, text, options = {}, pendingResponseId = null }) {
    const conversationId = this.conversations.getConversationByChatId(chatId)?.id ?? null;
    const id = newId();
    const enqueue = this.db.transaction(() => {
      const inserted = this.db.prepare(`
        insert into telegram_outbox
          (id, conversation_id, chat_id, kind, text, options_json, pending_response_id, state, available_at)
        values (?, ?, ?, 'text', ?, ?, ?, 'pending', ?)
        on conflict (pending_response_id) where pending_response_id is not null do nothing
      `).run(id, conversationId, String(chatId), text, JSON.stringify(options), pendingResponseId, Date.now() / 1000);
      if (pendingResponseId) {
        this.db.prepare("update response_blocks set posted = 1 where pending_response_id = ?").run(pendingResponseId);
      }
      if (inserted.changes > 0) {
        return id;
      }
      return this.db.prepare("select id from telegram_outbox where pending_response_id = ?").pluck().get(pendingResponseId);
    });
    return enqueue();
  }

  getDue(limit = 20) {
    return this.db.prepare(`
      select * from telegram_outbox
      where state = 'pending' and available_at <= ?
      order by available_at asc, created_at asc
      limit ?
    `).all(Date.now() / 1000, limit).map((row) => ({
      ...row,
      options: JSON.parse(row.options_json || "{}"),
    }));
  }

  markSent(id) {
    this.db.prepare(`
      update telegram_outbox
      set state = 'sent', sent_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), last_error = null
      where id = ?
    `).run(id);
  }

  reschedule(id, error, delayMs) {
    this.db.prepare(`
      update telegram_outbox
      set state = 'pending', attempts = attempts + 1, available_at = ?, last_error = ?
      where id = ?
    `).run((Date.now() + delayMs) / 1000, String(error).slice(0, 2000), id);
  }

  getPendingCount() {
    return this.db.prepare("select count(*) count from telegram_outbox where state = 'pending'").get().count;
  }
}
