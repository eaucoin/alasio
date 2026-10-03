import type { Database } from "better-sqlite3";
import { newId } from "../shared/ids.ts";
import { createLogger } from "../shared/log.ts";
import type { SqliteConversationRepository } from "./conversation-repository.ts";

const log = createLogger("sqlite-response-repository");

/**
 * One block of a harness's response as it streams in. Its fields beyond `type` are
 * the projection's; `response_start` and `response_complete` frame the response.
 */
export interface ResponseBlock {
  readonly type: string;
  readonly [field: string]: unknown;
}

/** A row of `response_blocks`: one block of a pending response, in sequence. */
export interface ResponseBlockRow {
  readonly id: string;
  readonly pending_response_id: string;
  readonly conversation_id: string;
  readonly channel: string;
  readonly thread_ts: string;
  readonly session_id: string | null;
  readonly sequence: number;
  readonly block_json: string;
  readonly posted: 0 | 1;
  /** Seconds since the epoch. */
  readonly created_at: number;
}

/** A response its harness completed that has not been delivered yet. */
export interface CompletedResponse {
  /** The pending response id. */
  readonly id: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly session_id: string | null;
  readonly blocks: ResponseBlock[];
  readonly posted: false;
}

/**
 * What a new block of a pending response copies from its latest one. It is read with
 * an aggregate, which yields a row of nulls when the response has no blocks.
 */
interface LatestBlock {
  readonly conversation_id: string;
  readonly channel: string;
  readonly thread_ts: string;
  readonly session_id: string | null;
  readonly sequence: number;
}

export class SqliteResponseRepository {
  private readonly db: Database;
  private readonly conversations: SqliteConversationRepository;

  constructor(db: Database, conversationRepository: SqliteConversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  createPendingResponse(chatId: number | string, messageId: number | string, sessionId: string | null = null): string {
    const pendingResponseId = newId();
    const conversationId = this.conversations.getConversationByChatId(chatId)?.id ?? chatId;
    this.db.prepare<[string]>(`
      update response_blocks
      set posted = 1
      where thread_ts = ? and posted = 0
    `).run(String(messageId));
    this.db.prepare<[
      id: string,
      pendingResponseId: string,
      conversationId: number | string,
      channel: number | string,
      threadTs: string,
      sessionId: string | null,
      blockJson: string,
      createdAt: number,
    ]>(`
      insert into response_blocks (id, pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json, created_at)
      values (?, ?, ?, ?, ?, ?, 0, ?, ?)
    `).run(newId(), pendingResponseId, conversationId, chatId, String(messageId), sessionId, JSON.stringify({ type: "response_start" }), Date.now() / 1000);
    return pendingResponseId;
  }

  appendBlockToPending(pendingResponseId: string, block: ResponseBlock): void {
    const last = this.db.prepare<[string], LatestBlock>(`
      select conversation_id, channel, thread_ts, session_id, sequence from response_blocks
      where pending_response_id = ? order by sequence desc limit 1
    `).get(pendingResponseId);
    if (!last) {
      log.warn(`Pending response ${pendingResponseId} not found`);
      return;
    }
    this.db.prepare<[
      id: string,
      pendingResponseId: string,
      conversationId: string,
      channel: string,
      threadTs: string,
      sessionId: string | null,
      sequence: number,
      blockJson: string,
      createdAt: number,
    ]>(`
      insert into response_blocks (id, pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(newId(), pendingResponseId, last.conversation_id, last.channel, last.thread_ts, last.session_id, last.sequence + 1, JSON.stringify(block), Date.now() / 1000);
  }

  markPendingComplete(pendingResponseId: string): void {
    const complete = this.db.transaction(() => {
      const existing = this.db.prepare<[string], 1>(`
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

  updatePendingSessionId(pendingResponseId: string, sessionId: string | null): void {
    this.db.prepare<[sessionId: string | null, pendingResponseId: string]>("update response_blocks set session_id = ? where pending_response_id = ?").run(sessionId, pendingResponseId);
  }

  markPendingAsPosted(pendingResponseId: string): void {
    this.db.prepare<[string]>("update response_blocks set posted = 1 where pending_response_id = ?").run(pendingResponseId);
  }

  getCompletedResponsesPendingDelivery(): CompletedResponse[] {
    const rows = this.db.prepare<[], Omit<ResponseBlockRow, "id" | "posted" | "created_at">>(`
      select pending_response_id, conversation_id, channel, thread_ts, session_id, sequence, block_json
      from response_blocks
      where pending_response_id in (
        select pending_response_id
        from response_blocks
        where posted = 0 and json_extract(block_json, '$.type') = 'response_complete'
      )
      order by pending_response_id, sequence asc
    `).all();
    const grouped = new Map<string, CompletedResponse>();
    for (const row of rows) {
      const current = grouped.get(row.pending_response_id) ?? {
        id: row.pending_response_id,
        chatId: row.channel,
        messageId: row.thread_ts,
        session_id: row.session_id,
        blocks: [],
        posted: false,
      };
      // appendBlockToPending and createPendingResponse are the only writers of block_json.
      const block = JSON.parse(row.block_json) as ResponseBlock;
      if (block.type !== "response_start") {
        current.blocks.push(block);
      }
      grouped.set(row.pending_response_id, current);
    }
    return [...grouped.values()];
  }
}
