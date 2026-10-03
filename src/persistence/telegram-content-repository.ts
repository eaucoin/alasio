import type { Database } from "better-sqlite3";
import type { Message, Update } from "@grammyjs/types";
import { newId } from "../shared/ids.ts";

export type MessageDirection = "in" | "out";

/** Whether a message carried files or only text. */
export type MessageKind = "text" | "file";

/** A row of `messages`: a message of a conversation, as it came or went over Telegram. */
export interface StoredMessage {
  readonly id: string;
  readonly conversation_id: string;
  readonly direction: MessageDirection;
  readonly kind: MessageKind;
  readonly transport_message_id: string | null;
  readonly text: string | null;
  readonly media_group_id: string | null;
  readonly raw_json: string | null;
  readonly codex_session_id: string | null;
  readonly turn_id: string | null;
  readonly created_at: string;
}

export interface NewMessage {
  readonly conversationId: string;
  readonly direction: MessageDirection;
  readonly kind: MessageKind;
  readonly transportMessageId?: number | string | null | undefined;
  readonly text?: string | null | undefined;
  readonly mediaGroupId?: string | null | undefined;
  readonly raw?: Message | null | undefined;
  readonly sessionId?: string | null | undefined;
  readonly turnId?: string | null | undefined;
}

/**
 * A file a Telegram message carried (a document, a photo's largest size, a video, …),
 * as far as it is kept.
 */
export interface TelegramFile {
  readonly file_id: string;
  readonly file_unique_id?: string | undefined;
  readonly file_name?: string | undefined;
  readonly mime_type?: string | undefined;
  readonly file_size?: number | undefined;
}

/** A row of `files`: a file a message carried, and where it was downloaded to. */
export interface StoredFile {
  readonly id: string;
  readonly message_id: string | null;
  readonly conversation_id: string;
  readonly telegram_file_id: string;
  readonly telegram_file_unique_id: string | null;
  readonly file_name: string | null;
  readonly mime_type: string | null;
  readonly file_size: number | null;
  readonly local_path: string | null;
  readonly sha256: string | null;
  readonly raw_json: string | null;
  readonly created_at: string;
  readonly downloaded_at: string | null;
}

export interface NewFile {
  readonly conversationId: string;
  readonly messageId: string;
  readonly file: TelegramFile;
  readonly localPath: string;
  readonly sha256?: string | null | undefined;
}

export type MediaGroupStatus = "pending" | "flushed";

/** A row of `media_groups`: an album, buffered until all its messages have arrived. */
export interface MediaGroup {
  readonly id: string;
  readonly conversation_id: string;
  readonly status: MediaGroupStatus;
  readonly first_update_id: number | null;
  readonly flush_after_ms: number;
  readonly created_at_ms: number;
  readonly flushed_at: string | null;
}

export interface MediaGroupArrival {
  readonly mediaGroupId: string;
  readonly conversationId: string;
  readonly updateId: number;
  readonly flushAfterMs: number;
}

export class SqliteTelegramContentRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  recordTelegramUpdate(update: Update): void {
    this.db.prepare<[updateId: number, payloadJson: string]>(`
      insert or ignore into telegram_updates (update_id, payload_json)
      values (?, ?)
    `).run(update.update_id, JSON.stringify(update));
  }

  markTelegramUpdateProcessed(updateId: number): void {
    this.db.prepare<[number]>("update telegram_updates set processed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where update_id = ?").run(updateId);
  }

  insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId }: NewMessage): string {
    const id = newId();
    this.db.prepare<[
      id: string,
      conversationId: string,
      direction: MessageDirection,
      kind: MessageKind,
      transportMessageId: string | null,
      text: string | null,
      mediaGroupId: string | null,
      rawJson: string | null,
      codexSessionId: string | null,
      turnId: string | null,
    ]>(`
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

  insertFile({ conversationId, messageId, file, localPath, sha256 }: NewFile): string {
    const id = newId();
    this.db.prepare<[
      id: string,
      messageId: string,
      conversationId: string,
      telegramFileId: string,
      telegramFileUniqueId: string | null,
      fileName: string | null,
      mimeType: string | null,
      fileSize: number | null,
      localPath: string,
      sha256: string | null,
      rawJson: string,
    ]>(`
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

  upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs }: MediaGroupArrival): void {
    this.db.prepare<[id: string, conversationId: string, firstUpdateId: number, flushAfterMs: number, createdAtMs: number]>(`
      insert into media_groups (id, conversation_id, first_update_id, flush_after_ms, created_at_ms)
      values (?, ?, ?, ?, ?)
      on conflict(id) do update set flush_after_ms = excluded.flush_after_ms
    `).run(mediaGroupId, conversationId, updateId, flushAfterMs, Date.now());
  }

  markMediaGroupFlushed(mediaGroupId: string): void {
    this.db.prepare<[string]>("update media_groups set status = 'flushed', flushed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') where id = ?").run(mediaGroupId);
  }

  getMediaGroupMessages(mediaGroupId: string): StoredMessage[] {
    return this.db.prepare<[string], StoredMessage>(`
      select * from messages
      where media_group_id = ?
      order by cast(transport_message_id as integer) asc, created_at asc
    `).all(mediaGroupId);
  }

  getFilesForMessages(messageIds: readonly string[]): StoredFile[] {
    if (messageIds.length === 0) {
      return [];
    }
    const placeholders = messageIds.map(() => "?").join(",");
    return this.db.prepare<string[], StoredFile>(`
      select * from files
      where message_id in (${placeholders})
      order by created_at asc
    `).all(...messageIds);
  }

  getPendingMediaGroupsDue(ageMs: number): MediaGroup[] {
    const cutoff = Date.now() - ageMs;
    return this.db.prepare<[number], MediaGroup>(`
      select * from media_groups
      where status = 'pending' and created_at_ms <= ?
      order by created_at_ms asc
    `).all(cutoff);
  }
}
