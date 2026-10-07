import type { Message, Update } from "@grammyjs/types";
import { Effect } from "effect";

import { newId } from "../shared/ids.ts";
import type { IncomingFile } from "../telegram/message.ts";
import type { Sql, StoreError } from "./sql.ts";

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
  readonly raw: Message | null;
  readonly session_id: string | null;
  readonly turn_id: string | null;
  readonly created_at: Date;
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

/** A file a message carried, as `files` keeps it but for its content. */
export interface StoredFile {
  readonly id: string;
  readonly message_id: string | null;
  readonly conversation_id: string;
  readonly telegram_file_id: string;
  readonly telegram_file_unique_id: string | null;
  /** Its name as Telegram gave it, if it gave one. */
  readonly file_name: string | null;
  /** The name it is written under for the agent to read. */
  readonly name: string;
  readonly mime_type: string | null;
  readonly file_size: number;
  readonly sha256: string;
  readonly created_at: Date;
}

/** A file a message carried, with its content. */
export interface FileContent extends Pick<StoredFile, "id" | "name"> {
  readonly content: Uint8Array;
}

export interface NewFile {
  readonly conversationId: string;
  readonly messageId: string;
  /** The file as the message carried it; it is kept whole, as raw. */
  readonly file: IncomingFile;
  /** The name it is written under for the agent to read. */
  readonly name: string;
  readonly content: Uint8Array;
  readonly sha256: string;
}

export type MediaGroupStatus = "pending" | "flushed";

/** A row of `media_groups`: an album, buffered until all its messages have arrived. */
export interface MediaGroup {
  readonly id: string;
  readonly conversation_id: string;
  readonly status: MediaGroupStatus;
  readonly first_update_id: string;
  readonly created_at: Date;
  readonly flushed_at: Date | null;
}

export interface MediaGroupArrival {
  readonly mediaGroupId: string;
  readonly conversationId: string;
  readonly updateId: number;
}

/** The columns of `files` but its content. */
const FILE_COLUMNS = "id, message_id, conversation_id, telegram_file_id, telegram_file_unique_id, file_name, name, mime_type, file_size, sha256, created_at";

export class NeonTelegramContentRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  recordTelegramUpdate(update: Update): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.telegram_updates (update_id, payload) values ($1, $2) on conflict (update_id) do nothing`,
      [update.update_id, JSON.stringify(update)],
    ));
  }

  markTelegramUpdateProcessed(updateId: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.telegram_updates set processed_at = now() where update_id = $1`, [updateId]));
  }

  insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId }: NewMessage): Effect.Effect<string, StoreError> {
    const id = newId();
    return this.#sql.query(
      `insert into ${this.#schema}.messages (id, conversation_id, direction, kind, transport_message_id, text, media_group_id, raw, session_id, turn_id)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
      [
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
      ],
    ).pipe(Effect.as(id));
  }

  insertFile({ conversationId, messageId, file, name, content, sha256 }: NewFile): Effect.Effect<string, StoreError> {
    const id = newId();
    return this.#sql.query(
      `insert into ${this.#schema}.files
         (id, message_id, conversation_id, telegram_file_id, telegram_file_unique_id, file_name, name, mime_type, file_size, sha256, content, raw)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
      [
        id,
        messageId,
        conversationId,
        file.file_id,
        file.file_unique_id ?? null,
        file.file_name ?? null,
        name,
        file.mime_type ?? null,
        content.byteLength,
        sha256,
        content,
        JSON.stringify(file),
      ],
    ).pipe(Effect.as(id));
  }

  /** The files `ids` name, with their content. */
  getFileContents(ids: readonly string[]): Effect.Effect<FileContent[], StoreError> {
    return this.#sql.query<FileContent>(`select id, name, content from ${this.#schema}.files where id = any($1)`, [ids]);
  }

  upsertMediaGroup({ mediaGroupId, conversationId, updateId }: MediaGroupArrival): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.media_groups (id, conversation_id, first_update_id) values ($1, $2, $3) on conflict (id) do nothing`,
      [mediaGroupId, conversationId, updateId],
    ));
  }

  markMediaGroupFlushed(mediaGroupId: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.media_groups set status = 'flushed', flushed_at = now() where id = $1`, [mediaGroupId]));
  }

  getMediaGroupMessages(mediaGroupId: string): Effect.Effect<StoredMessage[], StoreError> {
    return this.#sql.query<StoredMessage>(
      `select * from ${this.#schema}.messages where media_group_id = $1 order by transport_message_id::bigint, created_at`,
      [mediaGroupId],
    );
  }

  getFilesForMessages(messageIds: readonly string[]): Effect.Effect<StoredFile[], StoreError> {
    return this.#sql.query<StoredFile>(
      `select ${FILE_COLUMNS} from ${this.#schema}.files where message_id = any($1) order by created_at`,
      [messageIds],
    );
  }

  /** Deletes the updates processed, and the albums handled, more than `ageMs` ago. */
  pruneHandled(ageMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with updates as (
         delete from ${this.#schema}.telegram_updates where processed_at < now() - make_interval(secs => $1)
       )
       delete from ${this.#schema}.media_groups where flushed_at < now() - make_interval(secs => $1)`,
      [ageMs / 1000],
    ));
  }

  /** The albums still waiting that arrived at least `ageMs` ago, oldest first. */
  getPendingMediaGroupsDue(ageMs: number): Effect.Effect<MediaGroup[], StoreError> {
    return this.#sql.query<MediaGroup>(
      `select * from ${this.#schema}.media_groups
       where status = 'pending' and created_at <= now() - make_interval(secs => $1)
       order by created_at`,
      [ageMs / 1000],
    );
  }
}
