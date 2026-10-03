// @ts-nocheck
import { newId } from "../shared/ids.ts";
import { createLogger } from "../shared/log.ts";

const log = createLogger("sqlite-response-repository");

export class SqliteResponseRepository {
  constructor(db, conversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  createPendingResponse(chatId, messageId, sessionId = null) {
    const pendingResponseId = newId();
    const conversationId = this.conversations.getConversationByChatId(chatId)?.id ?? chatId;
    this.db.prepare(`
      update response_blocks
      set posted = 1
      where thread_ts = ? and posted = 0
    `).run(String(messageId));
    this.db.prepare(`
      insert into response_blocks (id, pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json, created_at)
      values (?, ?, ?, ?, ?, ?, 0, ?, ?)
    `).run(newId(), pendingResponseId, conversationId, chatId, String(messageId), sessionId, JSON.stringify({ type: "response_start" }), Date.now() / 1000);
    return pendingResponseId;
  }

  appendBlockToPending(pendingResponseId, block) {
    const last = this.db.prepare("select conversation_id, channel, thread_ts, session_id, max(sequence) as sequence from response_blocks where pending_response_id = ?").get(pendingResponseId);
    if (!last) {
      log.warn(`Pending response ${pendingResponseId} not found`);
      return;
    }
    this.db.prepare(`
      insert into response_blocks (id, pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(newId(), pendingResponseId, last.conversation_id, last.channel, last.thread_ts, last.session_id, Number(last.sequence ?? 0) + 1, JSON.stringify(block), Date.now() / 1000);
  }

  markPendingComplete(pendingResponseId) {
    const complete = this.db.transaction(() => {
      const existing = this.db.prepare(`
        select 1 from response_blocks
        where pending_response_id = ? and json_extract(block_json, '$.type') = 'response_complete'
        limit 1
      `).get(pendingResponseId);
      if (!existing) {
        this.appendBlockToPending(pendingResponseId, { type: "response_complete" });
      }
    });
    complete();
  }

  updatePendingSessionId(pendingResponseId, sessionId) {
    this.db.prepare("update response_blocks set session_id = ? where pending_response_id = ?").run(sessionId, pendingResponseId);
  }

  markPendingAsPosted(pendingResponseId) {
    this.db.prepare("update response_blocks set posted = 1 where pending_response_id = ?").run(pendingResponseId);
  }

  getCompletedResponsesPendingDelivery() {
    const rows = this.db.prepare(`
      select pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json
      from response_blocks
      where pending_response_id in (
        select pending_response_id
        from response_blocks
        where posted = 0 and json_extract(block_json, '$.type') = 'response_complete'
      )
      order by pending_response_id, sequence asc
    `).all();
    const grouped = new Map();
    for (const row of rows) {
      const current = grouped.get(row.pending_response_id) ?? {
        id: row.pending_response_id,
        chatId: row.channel,
        messageId: row.thread_ts,
        session_id: row.session_id,
        blocks: [],
        posted: false,
      };
      const block = JSON.parse(row.block_json);
      if (block.type !== "response_start") {
        current.blocks.push(block);
      }
      grouped.set(row.pending_response_id, current);
    }
    return [...grouped.values()];
  }
}
