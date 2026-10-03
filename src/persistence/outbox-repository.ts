import type { Database } from "better-sqlite3";
import { newId } from "../shared/ids.ts";
import type { SqliteConversationRepository } from "./conversation-repository.ts";

/**
 * The options a reply is sent with, as the Telegram client takes them. They are kept
 * as JSON, so they hold only what survives it.
 */
export interface OutboxMessageOptions {
  /** A directory of media copied for the reply, removed once it is delivered. */
  readonly mediaDir?: string | undefined;
  readonly [option: string]: unknown;
}

export type OutboxState = "pending" | "sent";

/** A row of `telegram_outbox`: a reply waiting for, or done with, delivery to Telegram. */
export interface OutboxEntryRow {
  readonly id: string;
  readonly conversation_id: string | null;
  readonly chat_id: string;
  readonly kind: "text";
  readonly text: string;
  readonly options_json: string;
  readonly pending_response_id: string | null;
  readonly state: OutboxState;
  readonly attempts: number;
  /** Seconds since the epoch. */
  readonly available_at: number;
  readonly last_error: string | null;
  /** The W3C traceparent of the turn whose reply this is. */
  readonly traceparent: string | null;
  readonly created_at: string;
  readonly sent_at: string | null;
}

/** An outbox entry, with its options. */
export interface OutboxEntry extends OutboxEntryRow {
  readonly options: OutboxMessageOptions;
}

export interface NewOutboxText {
  readonly chatId: number | string;
  readonly text: string;
  readonly options?: OutboxMessageOptions | undefined;
  /** The pending response the reply delivers; one reply per pending response. */
  readonly pendingResponseId?: string | null | undefined;
  readonly traceparent?: string | null | undefined;
}

export class SqliteOutboxRepository {
  private readonly db: Database;
  private readonly conversations: SqliteConversationRepository;

  constructor(db: Database, conversationRepository: SqliteConversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  enqueueText({ chatId, text, options = {}, pendingResponseId = null, traceparent = null }: NewOutboxText): string {
    const conversationId = this.conversations.getConversationByChatId(chatId)?.id ?? null;
    const id = newId();
    const enqueue = this.db.transaction(() => {
      const inserted = this.db.prepare<[
        id: string,
        conversationId: string | null,
        chatId: string,
        text: string,
        optionsJson: string,
        pendingResponseId: string | null,
        traceparent: string | null,
        availableAt: number,
      ]>(`
        insert into telegram_outbox
          (id, conversation_id, chat_id, kind, text, options_json, pending_response_id, state, traceparent, available_at)
        values (?, ?, ?, 'text', ?, ?, ?, 'pending', ?, ?)
        on conflict (pending_response_id) where pending_response_id is not null do nothing
      `).run(id, conversationId, String(chatId), text, JSON.stringify(options), pendingResponseId, traceparent, Date.now() / 1000);
      if (pendingResponseId) {
        this.db.prepare<[string]>("update response_blocks set posted = 1 where pending_response_id = ?").run(pendingResponseId);
      }
      if (inserted.changes > 0) {
        return id;
      }
      // Only a reply already queued for this pending response conflicts with the insert.
      return this.db.prepare<[string | null], string>("select id from telegram_outbox where pending_response_id = ?").pluck().get(pendingResponseId)!;
    });
    return enqueue();
  }

  getDue(limit = 20): OutboxEntry[] {
    return this.db.prepare<[availableAt: number, limit: number], OutboxEntryRow>(`
      select * from telegram_outbox
      where state = 'pending' and available_at <= ?
      order by available_at asc, created_at asc
      limit ?
    `).all(Date.now() / 1000, limit).map((row) => ({
      ...row,
      // enqueueText is the only writer of options_json, and it writes an object.
      options: JSON.parse(row.options_json || "{}") as OutboxMessageOptions,
    }));
  }

  markSent(id: string): void {
    this.db.prepare<[string]>(`
      update telegram_outbox
      set state = 'sent', sent_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), last_error = null
      where id = ?
    `).run(id);
  }

  reschedule(id: string, error: unknown, delayMs: number): void {
    this.db.prepare<[availableAt: number, lastError: string, id: string]>(`
      update telegram_outbox
      set state = 'pending', attempts = attempts + 1, available_at = ?, last_error = ?
      where id = ?
    `).run((Date.now() + delayMs) / 1000, String(error).slice(0, 2000), id);
  }

  getPendingCount(): number {
    // An aggregate without a group by always yields one row.
    return this.db.prepare<[], { count: number }>("select count(*) count from telegram_outbox where state = 'pending'").get()!.count;
  }
}
