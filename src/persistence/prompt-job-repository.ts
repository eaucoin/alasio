import type { Database } from "better-sqlite3";
import { type HarnessName, isHarnessName } from "../harness/names.ts";
import { newId } from "../shared/ids.ts";

/**
 * Where a prompt job is: waiting its turn (or for the operator to say what to do with
 * it, while another runs), running, or done one way or another.
 */
export type PromptJobState = "pending" | "awaiting_choice" | "running" | "completed" | "cancelled" | "failed" | "interrupted";

/** A row of `prompt_jobs`: a prompt queued for its conversation's harness. */
export interface PromptJobRow {
  readonly id: string;
  readonly conversation_id: string;
  readonly chat_id: string;
  readonly message_id: string;
  readonly prompt: string;
  readonly file_paths_json: string;
  readonly harness: HarnessName;
  readonly state: PromptJobState;
  readonly priority: number;
  readonly attempts: number;
  readonly upstream_session_id: string | null;
  readonly upstream_turn_id: string | null;
  readonly upstream_started_at: number | null;
  readonly upstream_completed_at: number | null;
  readonly last_error: string | null;
  readonly traceparent: string | null;
  readonly created_at: number;
  readonly started_at: number | null;
  readonly completed_at: number | null;
}

/** A prompt job, with the paths of the files sent with its prompt. */
export interface PromptJob extends PromptJobRow {
  readonly filePaths: string[];
}

export interface NewPromptJob {
  readonly conversationId: string;
  readonly chatId: number | string;
  readonly messageId: number | string;
  readonly prompt: string;
  readonly filePaths?: readonly string[] | undefined;
  readonly state?: PromptJobState | undefined;
  readonly priority?: number | undefined;
  /** The harness to run the prompt on; null while none is mounted, which refuses the job. */
  readonly harness?: HarnessName | null | undefined;
  /** The W3C traceparent of the trace the prompt was queued in. */
  readonly traceparent?: string | null | undefined;
}

function mapRow(row: PromptJobRow): PromptJob;
function mapRow(row: PromptJobRow | null | undefined): PromptJob | null;
function mapRow(row: PromptJobRow | null | undefined): PromptJob | null {
  // enqueue is the only writer of file_paths_json, and it writes an array of paths.
  return row ? { ...row, filePaths: JSON.parse(row.file_paths_json || "[]") as string[] } : null;
}

export class SqlitePromptJobRepository {
  private readonly db: Database;

  constructor(db: Database) {
    this.db = db;
  }

  enqueue({ conversationId, chatId, messageId, prompt, filePaths = [], state = "pending", priority = 0, harness = null, traceparent = null }: NewPromptJob): PromptJob {
    if (!isHarnessName(harness)) {
      throw new Error(`Cannot queue a prompt for ${conversationId}: no service is mounted`);
    }
    const id = newId();
    this.db.prepare<[
      id: string,
      conversationId: string,
      chatId: string,
      messageId: string,
      prompt: string,
      filePathsJson: string,
      harness: HarnessName,
      state: PromptJobState,
      priority: number,
      traceparent: string | null,
      createdAt: number,
    ]>(`
      insert into prompt_jobs
        (id, conversation_id, chat_id, message_id, prompt, file_paths_json, harness, state, priority, traceparent, created_at)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      on conflict(conversation_id, message_id) do nothing
    `).run(id, conversationId, String(chatId), String(messageId), prompt, JSON.stringify(filePaths), harness, state, priority, traceparent, Date.now() / 1000);
    // The row is the one just inserted or the one already queued for this message.
    return mapRow(this.db.prepare<[conversationId: string, messageId: string], PromptJobRow>("select * from prompt_jobs where conversation_id = ? and message_id = ?").get(conversationId, String(messageId))!);
  }

  get(id: string): PromptJob | null {
    return mapRow(this.db.prepare<[string], PromptJobRow>("select * from prompt_jobs where id = ?").get(id));
  }

  claimNext(conversationId: string): PromptJob | null {
    const claim = this.db.transaction(() => {
      const row = this.db.prepare<[string], PromptJobRow>(`
        select * from prompt_jobs
        where conversation_id = ? and state = 'pending'
        order by priority desc, created_at asc
        limit 1
      `).get(conversationId);
      if (!row) {
        return null;
      }
      this.db.prepare<[startedAt: number, id: string]>(`
        update prompt_jobs
        set state = 'running', attempts = attempts + 1, started_at = ?, last_error = null
        where id = ? and state = 'pending'
      `).run(Date.now() / 1000, row.id);
      return this.db.prepare<[string], PromptJobRow>("select * from prompt_jobs where id = ?").get(row.id);
    });
    return mapRow(claim());
  }

  setDisposition(id: string, state: PromptJobState, priority = 0): void {
    this.db.prepare<[state: PromptJobState, priority: number, sameState: PromptJobState, completedAt: number, id: string]>(`
      update prompt_jobs set state = ?, priority = ?, completed_at = case when ? in ('completed', 'cancelled') then ? else null end
      where id = ?
    `).run(state, priority, state, Date.now() / 1000, id);
  }

  complete(id: string): void {
    this.setDisposition(id, "completed");
  }

  markUpstreamStarted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): void {
    this.db.prepare<[sessionId: string | null, turnId: string | null, startedAt: number, id: string]>(`
      update prompt_jobs
      set upstream_session_id = ?, upstream_turn_id = ?, upstream_started_at = ?
      where id = ? and state = 'running'
    `).run(sessionId ?? null, turnId ?? null, Date.now() / 1000, id);
  }

  markUpstreamCompleted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): void {
    this.db.prepare<[sessionId: string | null, turnId: string | null, completedAt: number, id: string]>(`
      update prompt_jobs
      set upstream_session_id = coalesce(?, upstream_session_id),
          upstream_turn_id = coalesce(?, upstream_turn_id),
          upstream_completed_at = ?
      where id = ? and state = 'running'
    `).run(sessionId ?? null, turnId ?? null, Date.now() / 1000, id);
  }

  fail(id: string, error: unknown): void {
    this.db.prepare<[lastError: string, completedAt: number, id: string]>(`
      update prompt_jobs set state = 'failed', last_error = ?, completed_at = ? where id = ?
    `).run(String(error).slice(0, 2000), Date.now() / 1000, id);
  }

  hasOpenJobs(conversationId: string): boolean {
    const row = this.db.prepare<[string], { count: number }>(`
      select count(*) as count from prompt_jobs
      where conversation_id = ? and state in ('pending', 'running', 'awaiting_choice')
    `).get(conversationId);
    return Number(row?.count ?? 0) > 0;
  }

  listPendingConversations(): string[] {
    return this.db.prepare<[], Pick<PromptJobRow, "conversation_id">>(`
      select distinct conversation_id from prompt_jobs where state = 'pending' order by conversation_id
    `).all().map((row) => row.conversation_id);
  }

  recoverAfterRestart(): string[] {
    const completed = this.db.prepare<[], Pick<PromptJobRow, "conversation_id">>(`
      select distinct conversation_id from prompt_jobs
      where state = 'running' and upstream_completed_at is not null
    `).all().map((row) => row.conversation_id);
    this.db.prepare<[completedAt: number]>(`
      update prompt_jobs set state = 'completed', completed_at = coalesce(completed_at, ?)
      where state = 'running' and upstream_completed_at is not null
    `).run(Date.now() / 1000);
    this.db.prepare<[]>(`
      update prompt_jobs set state = 'pending', started_at = null
      where state = 'running' and upstream_started_at is null and upstream_completed_at is null
    `).run();
    this.db.prepare<[completedAt: number]>(`
      update prompt_jobs set state = 'interrupted', completed_at = ?
      where state = 'running' and upstream_started_at is not null and upstream_completed_at is null
    `).run(Date.now() / 1000);
    return completed;
  }
}
