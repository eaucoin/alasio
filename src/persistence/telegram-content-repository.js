import { newId } from "../shared/ids.js";

export class SqliteTelegramContentRepository {
  constructor(db) {
    this.db = db;
  }

  recordTelegramUpdate(update) {
    this.db.prepare(`
      insert or ignore into telegram_updates (update_id, payload_json)
      values (?, ?)
    `).run(update.update_id, JSON.stringify(update));
  }

  markTelegramUpdateProcessed(updateId) {
    this.db.prepare("update telegram_updates set processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where update_id = ?").run(updateId);
  }

  insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId }) {
    const id = newId();
    this.db.prepare(`
      insert into messages (id, conversation_id, direction, kind, transport_message_id, text, media_group_id, raw_json, codex_session_id, turn_id)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      conversationId,
      direction,
      kind,
      transportMessageId == null ? null : String(transportMessageId),
      text ?? null,
      mediaGroupId ?? null,
      raw ? JSON.stringify(raw) : null,
      sessionId ?? null,
      turnId ?? null,
    );
    return id;
  }

  insertFile({ conversationId, messageId, file, localPath, sha256 }) {
    const id = newId();
    this.db.prepare(`
      insert into files (
        id, message_id, conversation_id, telegram_file_id, telegram_file_unique_id,
        file_name, mime_type, file_size, local_path, sha256, raw_json, downloaded_at
      )
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    `).run(
      id,
      messageId,
      conversationId,
      file.file_id,
      file.file_unique_id ?? null,
      file.file_name ?? null,
      file.mime_type ?? null,
      file.file_size ?? null,
      localPath,
      sha256 ?? null,
      JSON.stringify(file),
    );
    return id;
  }

  upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs }) {
    this.db.prepare(`
      insert into media_groups (id, conversation_id, first_update_id, flush_after_ms, created_at_ms)
      values (?, ?, ?, ?, ?)
      on conflict(id) do update set flush_after_ms = excluded.flush_after_ms
    `).run(mediaGroupId, conversationId, updateId, flushAfterMs, Date.now());
  }

  markMediaGroupFlushed(mediaGroupId) {
    this.db.prepare("update media_groups set status = 'flushed', flushed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where id = ?").run(mediaGroupId);
  }

  getMediaGroupMessages(mediaGroupId) {
    return this.db.prepare(`
      select * from messages
      where media_group_id = ?
      order by cast(transport_message_id as integer) asc, created_at asc
    `).all(mediaGroupId);
  }

  getFilesForMessages(messageIds) {
    if (messageIds.length === 0) {
      return [];
    }
    const placeholders = messageIds.map(() => "?").join(",");
    return this.db.prepare(`
      select * from files
      where message_id in (${placeholders})
      order by created_at asc
    `).all(...messageIds);
  }

  getPendingMediaGroupsDue(ageMs) {
    const cutoff = Date.now() - ageMs;
    return this.db.prepare(`
      select * from media_groups
      where status = 'pending' and created_at_ms <= ?
      order by created_at_ms asc
    `).all(cutoff);
  }
}
