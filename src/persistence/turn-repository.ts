import type { Database } from "better-sqlite3";
import { type HarnessName, isHarnessName } from "../harness/names.ts";
import type { SqliteConversationRepository } from "./conversation-repository.ts";

export type TurnState = "active" | "completed";

/**
 * A row of `turns`: a conversation's turn with its harness, kept while it runs so
 * that a restart can resume it. A conversation has one row, its latest turn.
 */
export interface Turn {
  readonly id: string;
  readonly conversation_id: string;
  readonly thread_key: string;
  readonly channel: string;
  readonly thread_ts: string;
  readonly session_id: string | null;
  readonly harness: HarnessName;
  readonly pending_response_id: string | null;
  readonly prompt: string | null;
  readonly state: TurnState;
  readonly started_at: number;
  readonly completed_at: number | null;
}

/** A turn starting, as recorded by upsertActiveTurn. */
export interface ActiveTurn {
  readonly threadKey: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly sessionId?: string | null | undefined;
  /** The harness the turn runs on; the conversation's active one when not given. */
  readonly harness?: HarnessName | null | undefined;
  readonly pendingResponseId?: string | null | undefined;
  readonly prompt?: string | null | undefined;
  /** Seconds since the epoch. */
  readonly startedAt?: number | undefined;
}

export class SqliteTurnRepository {
  private readonly db: Database;
  private readonly conversations: SqliteConversationRepository;

  constructor(db: Database, conversationRepository: SqliteConversationRepository) {
    this.db = db;
    this.conversations = conversationRepository;
  }

  upsertActiveTurn(turn: ActiveTurn): void {
    const threadKey = turn.threadKey;
    const harness = isHarnessName(turn.harness) ? turn.harness : this.conversations.getActiveHarness(threadKey);
    if (!harness) {
      throw new Error(`Cannot record an active turn for ${threadKey}: no service is mounted`);
    }
    this.db.prepare<[
      id: string,
      conversationId: string,
      threadKey: string,
      channel: string,
      threadTs: string,
      sessionId: string | null,
      harness: HarnessName,
      pendingResponseId: string | null,
      prompt: string | null,
      startedAt: number,
    ]>(`
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

  updateActiveTurnSessionId(threadKey: string, sessionId: string | null): void {
    this.db.prepare<[sessionId: string | null, id: string]>("update turns set session_id = ? where id = ? and state = 'active'").run(sessionId, threadKey);
    const activeTurn = this.db.prepare<[string], Pick<Turn, "harness">>("select harness from turns where id = ? and state = 'active'").get(threadKey);
    const harness = isHarnessName(activeTurn?.harness) ? activeTurn.harness : this.conversations.requireActiveHarness(threadKey);
    this.conversations.setHarnessSessionId(threadKey, harness, sessionId);
  }

  updateActiveTurnPendingResponseId(threadKey: string, pendingResponseId: string | null): void {
    this.db.prepare<[pendingResponseId: string | null, id: string]>("update turns set pending_response_id = ? where id = ? and state = 'active'").run(pendingResponseId, threadKey);
  }

  clearActiveTurn(threadKey: string, pendingResponseId: string | null = null): void {
    if (pendingResponseId) {
      this.db.prepare<[completedAt: number, id: string, pendingResponseId: string]>(`
        update turns set state = 'completed', completed_at = ?
        where id = ? and state = 'active' and pending_response_id = ?
      `).run(Date.now() / 1000, threadKey, pendingResponseId);
      return;
    }
    this.db.prepare<[completedAt: number, id: string]>("update turns set state = 'completed', completed_at = ? where id = ? and state = 'active'").run(Date.now() / 1000, threadKey);
  }

  getActiveTurns(): Turn[] {
    return this.db.prepare<[], Turn>("select * from turns where state = 'active' order by started_at asc").all();
  }
}
