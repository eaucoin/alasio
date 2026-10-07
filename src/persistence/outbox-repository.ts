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

/** A row of `telegram_outbox`: a reply waiting for, or done with, delivery to Telegram. */
export interface OutboxEntry {
  readonly id: string;
  readonly conversation_id: string | null;
  readonly chat_id: string;
  readonly kind: "text";
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

  /**
   * The replies due for delivery: each chat's in the order they were queued, so a reply
   * waiting to be retried holds back those queued after it to the same chat.
   */
  getDue(limit = 20): Effect.Effect<OutboxEntry[], StoreError> {
    return this.#sql.query<OutboxEntry>(
      `select * from ${this.#schema}.telegram_outbox reply
       where state = 'pending' and available_at <= now()
         and not exists (
           select 1 from ${this.#schema}.telegram_outbox earlier
           where earlier.chat_id = reply.chat_id and earlier.state = 'pending' and earlier.seq < reply.seq
         )
       order by seq
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

  /** Marks the reply sent, its media deleted with it. */
  markSent(id: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with media as (
         delete from ${this.#schema}.outbox_media where outbox_id = $1
       )
       update ${this.#schema}.telegram_outbox set state = 'sent', sent_at = now(), last_error = null where id = $1`,
      [id],
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
