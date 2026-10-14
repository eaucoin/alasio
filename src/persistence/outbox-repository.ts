import { Effect } from "effect";

import { newId } from "../shared/ids.ts";
import type { MediaAttachment, SendMessageOptions } from "../telegram/client.ts";
import type { Sql, StoreError } from "./sql.ts";

/**
 * The options a reply is sent with, as the Telegram client takes them, but its media,
 * which `outbox_media` keeps. They are kept as JSON, so they hold only what survives it.
 */
export type OutboxMessageOptions = Omit<SendMessageOptions, "media">;

export type OutboxState = "pending" | "sent";

/** What an entry is: a reply, a turn's status message, or an edit of a status message. */
export type OutboxKind = "text" | "status" | "edit";

/** A row of `telegram_outbox`: what waits for, or is done with, delivery to Telegram. */
export interface OutboxEntry {
  readonly id: string;
  readonly conversation_id: string | null;
  readonly chat_id: string;
  readonly kind: OutboxKind;
  /** Of an edit, the entry it edits. */
  readonly edits: string | null;
  /** Of a status message, its message's id once Telegram accepted it. */
  readonly telegram_message_id: string | null;
  readonly text: string;
  readonly options: OutboxMessageOptions;
  readonly pending_response_id: string | null;
  readonly state: OutboxState;
  readonly attempts: number;
  readonly available_at: Date;
  readonly last_error: string | null;
  /** The W3C traceparent of the turn whose reply this is. */
  readonly traceparent: string | null;
  readonly created_at: Date;
  readonly sent_at: Date | null;
}

/** An entry due for delivery: an edit with the message id of the status message it edits, null where that has none. */
export interface DueOutboxEntry extends OutboxEntry {
  readonly edited_message_id: string | null;
}

/** A turn's status message, or an edit of one, to queue. */
export interface NewOutboxStatus {
  readonly chatId: number | string;
  readonly text: string;
  readonly options?: OutboxMessageOptions | undefined;
  readonly traceparent?: string | null | undefined;
}

export interface NewOutboxText {
  readonly chatId: number | string;
  readonly text: string;
  readonly options?: SendMessageOptions | undefined;
  /** The pending response the reply delivers; one reply per pending response. */
  readonly pendingResponseId?: string | null | undefined;
  readonly traceparent?: string | null | undefined;
}

export class NeonOutboxRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /**
   * Queues a reply with its media, and marks the pending response it delivers as posted:
   * the id of the reply queued, or of the one already queued for that pending response.
   */
  enqueueText({ chatId, text, options = {}, pendingResponseId = null, traceparent = null }: NewOutboxText): Effect.Effect<string, StoreError> {
    const { media = [], ...kept } = options;
    // The statement sees the outbox as it was before it: a reply already queued for the
    // pending response is there, one queued by it is not.
    return this.#sql.query<{ id: string }>(
      `with queued as (
         insert into ${this.#schema}.telegram_outbox (id, conversation_id, chat_id, kind, text, options, pending_response_id, traceparent)
         select $1, (select id from ${this.#schema}.conversations where transport = 'telegram' and chat_id = $2), $2, 'text', $3, $4, $5, $6
         on conflict (pending_response_id) where pending_response_id is not null do nothing
         returning id
       ), media as (
         insert into ${this.#schema}.outbox_media (outbox_id, position, id, kind, animation, file_name, content)
         select queued.id, item.position, item.id, item.kind, item.animation, item.file_name, item.content
         from queued, unnest($7::text[], $8::text[], $9::boolean[], $10::text[], $11::bytea[]) with ordinality
           as item (id, kind, animation, file_name, content, position)
       ), posted as (
         update ${this.#schema}.responses set posted = true where id = $5
       )
       select id from queued
       union all
       select id from ${this.#schema}.telegram_outbox where pending_response_id = $5`,
      [
        newId(), String(chatId), text, JSON.stringify(kept), pendingResponseId, traceparent,
        media.map((item) => item.id), media.map((item) => item.kind), media.map((item) => item.animation),
        media.map((item) => item.fileName), media.map((item) => item.content),
      ],
    ).pipe(Effect.map(([row]) => row!.id));
  }

  /** Queues a turn's status message: its id, which an edit of it names. */
  enqueueStatus(status: NewOutboxStatus): Effect.Effect<string, StoreError> {
    return this.#enqueue("status", null, status);
  }

  /** Queues an edit of the status message `edits`, delivered after what the chat was sent before it. */
  enqueueEdit(edits: string, edit: NewOutboxStatus): Effect.Effect<string, StoreError> {
    return this.#enqueue("edit", edits, edit);
  }

  #enqueue(kind: "status" | "edit", edits: string | null, { chatId, text, options = {}, traceparent = null }: NewOutboxStatus): Effect.Effect<string, StoreError> {
    const id = newId();
    return this.#sql.query(
      `insert into ${this.#schema}.telegram_outbox (id, conversation_id, chat_id, kind, edits, text, options, traceparent)
       select $1, (select id from ${this.#schema}.conversations where transport = 'telegram' and chat_id = $2), $2, $3, $4, $5, $6, $7`,
      [id, String(chatId), kind, edits, text, JSON.stringify(options), traceparent],
    ).pipe(Effect.as(id));
  }

  /**
   * What is due for delivery: each chat's in the order it was queued, so an entry waiting
   * to be retried holds back those queued after it to the same chat; an edit with the
   * message id of the status message it edits, which was sent before it.
   */
  getDue(limit = 20): Effect.Effect<DueOutboxEntry[], StoreError> {
    return this.#sql.query<DueOutboxEntry>(
      `select reply.*, edited.telegram_message_id as edited_message_id
       from ${this.#schema}.telegram_outbox reply
       left join ${this.#schema}.telegram_outbox edited on edited.id = reply.edits
       where reply.state = 'pending' and reply.available_at <= now()
         and not exists (
           select 1 from ${this.#schema}.telegram_outbox earlier
           where earlier.chat_id = reply.chat_id and earlier.state = 'pending' and earlier.seq < reply.seq
         )
       order by reply.seq
       limit $1`,
      [limit],
    );
  }

  /** The media the reply shows, in the order they were queued. */
  getMedia(id: string): Effect.Effect<MediaAttachment[], StoreError> {
    return this.#sql.query<MediaAttachment>(
      `select id, kind, animation, file_name as "fileName", content from ${this.#schema}.outbox_media where outbox_id = $1 order by position`,
      [id],
    );
  }

  /**
   * Marks the entry delivered, its media deleted with it: of a status message, with its
   * message's id; of one given up on, as Telegram refused it for good, with why.
   */
  markSent(id: string, { telegramMessageId = null, refused = null }: { readonly telegramMessageId?: number | null; readonly refused?: string | null } = {}): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with media as (
         delete from ${this.#schema}.outbox_media where outbox_id = $1
       )
       update ${this.#schema}.telegram_outbox set state = 'sent', sent_at = now(), telegram_message_id = $2, last_error = $3 where id = $1`,
      [id, telegramMessageId, refused === null ? null : refused.slice(0, 2000)],
    ));
  }

  reschedule(id: string, error: unknown, delayMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.telegram_outbox
       set state = 'pending', attempts = attempts + 1, available_at = now() + make_interval(secs => $2), last_error = $3
       where id = $1`,
      [id, delayMs / 1000, String(error).slice(0, 2000)],
    ));
  }

  /** Deletes the replies Telegram accepted more than `ageMs` ago. */
  pruneSent(ageMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `delete from ${this.#schema}.telegram_outbox where state = 'sent' and sent_at < now() - make_interval(secs => $1)`,
      [ageMs / 1000],
    ));
  }

  getPendingCount(): Effect.Effect<number, StoreError> {
    // An aggregate without a group by always yields one row.
    return this.#sql.query<{ count: number }>(`select count(*)::integer as count from ${this.#schema}.telegram_outbox where state = 'pending'`).pipe(
      Effect.map(([row]) => row!.count),
    );
  }
}
