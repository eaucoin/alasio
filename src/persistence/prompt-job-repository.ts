import { Effect } from "effect";

import type { HarnessName } from "../harness/names.ts";
import { newId } from "../shared/ids.ts";
import type { Sql, StoreError } from "./sql.ts";

/**
 * Where a prompt job is: waiting its turn (or for the operator to say what to do with
 * it, while another runs), running, or done one way or another.
 */
export type PromptJobState = "pending" | "awaiting_choice" | "running" | "completed" | "cancelled" | "failed" | "interrupted";

/** A row of `prompt_jobs`: a prompt queued for its conversation's harness. */
export interface PromptJob {
  readonly id: string;
  readonly conversation_id: string;
  readonly chat_id: string;
  readonly message_id: string;
  readonly prompt: string;
  /** The files sent with the prompt, which its text names where they are written. */
  readonly file_ids: readonly string[];
  readonly harness: HarnessName;
  readonly state: PromptJobState;
  readonly priority: number;
  readonly attempts: number;
  readonly upstream_session_id: string | null;
  readonly upstream_turn_id: string | null;
  readonly upstream_dispatched_at: Date | null;
  readonly upstream_started_at: Date | null;
  readonly upstream_completed_at: Date | null;
  readonly last_error: string | null;
  readonly traceparent: string | null;
  readonly created_at: Date;
  readonly started_at: Date | null;
  readonly completed_at: Date | null;
}

export interface NewPromptJob {
  readonly conversationId: string;
  readonly chatId: number | string;
  readonly messageId: number | string;
  readonly prompt: string;
  readonly fileIds?: readonly string[] | undefined;
  readonly state?: PromptJobState | undefined;
  readonly priority?: number | undefined;
  /** The harness to run the prompt on; the conversation's mounted one when not given. */
  readonly harness?: HarnessName | undefined;
  /** The W3C traceparent of the trace the prompt was queued in. */
  readonly traceparent?: string | null | undefined;
}

export class NeonPromptJobRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Queues a prompt: the job queued, or the one already queued for its message. */
  enqueue({ conversationId, chatId, messageId, prompt, fileIds = [], state = "pending", priority = 0, harness, traceparent = null }: NewPromptJob): Effect.Effect<PromptJob, StoreError> {
    // The no-op update makes the job already queued the one returned.
    return this.#sql.query<PromptJob>(
      `insert into ${this.#schema}.prompt_jobs (id, conversation_id, chat_id, message_id, prompt, file_ids, harness, state, priority, traceparent)
       select $1, id, $3, $4, $5, $6, coalesce($7, active_harness), $8, $9, $10 from ${this.#schema}.conversations where id = $2
       on conflict (conversation_id, message_id) do update set conversation_id = excluded.conversation_id
       returning *`,
      [newId(), conversationId, String(chatId), String(messageId), prompt, fileIds, harness ?? null, state, priority, traceparent],
    ).pipe(Effect.map(([row]) => row!));
  }

  get(id: string): Effect.Effect<PromptJob | null, StoreError> {
    return this.#sql.query<PromptJob>(`select * from ${this.#schema}.prompt_jobs where id = $1`, [id]).pipe(Effect.map(([row]) => row ?? null));
  }

  /** Starts the conversation's next pending job, by priority and then in the order queued: the job. */
  claimNext(conversationId: string): Effect.Effect<PromptJob | null, StoreError> {
    return this.#sql.query<PromptJob>(
      `update ${this.#schema}.prompt_jobs set state = 'running', attempts = attempts + 1, started_at = now(), last_error = null
       where id = (
         select id from ${this.#schema}.prompt_jobs
         where conversation_id = $1 and state = 'pending'
         order by priority desc, seq
         limit 1
         for update skip locked
       )
       returning *`,
      [conversationId],
    ).pipe(Effect.map(([row]) => row ?? null));
  }

  /** Settles what becomes of a job: completed and cancelled jobs are done, any other state is not. */
  setDisposition(id: string, state: PromptJobState, priority = 0): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs
       set state = $2, priority = $3, completed_at = case when $2 in ('completed', 'cancelled') then now() end
       where id = $1`,
      [id, state, priority],
    ));
  }

  /** Records that the job's prompt was sent to the agent, which may act on it from then on. */
  markDispatched(id: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.prompt_jobs set upstream_dispatched_at = now() where id = $1`, [id]));
  }

  markUpstreamStarted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs set upstream_session_id = $2, upstream_turn_id = $3, upstream_started_at = now()
       where id = $1 and state = 'running'`,
      [id, sessionId ?? null, turnId ?? null],
    ));
  }

  markUpstreamCompleted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs
       set upstream_session_id = coalesce($2, upstream_session_id), upstream_turn_id = coalesce($3, upstream_turn_id), upstream_completed_at = now()
       where id = $1 and state = 'running'`,
      [id, sessionId ?? null, turnId ?? null],
    ));
  }

  fail(id: string, error: unknown): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs set state = 'failed', last_error = $2, completed_at = now() where id = $1`,
      [id, String(error).slice(0, 2000)],
    ));
  }

  hasOpenJobs(conversationId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query<{ open: boolean }>(
      `select exists (
         select 1 from ${this.#schema}.prompt_jobs where conversation_id = $1 and state in ('pending', 'running', 'awaiting_choice')
       ) as open`,
      [conversationId],
    ).pipe(Effect.map(([row]) => row!.open));
  }

  listPendingConversations(): Effect.Effect<string[], StoreError> {
    return this.#sql.query<{ conversation_id: string }>(
      `select distinct conversation_id from ${this.#schema}.prompt_jobs where state = 'pending' order by conversation_id`,
    ).pipe(Effect.map((rows) => rows.map((row) => row.conversation_id)));
  }

  /**
   * Settles the jobs a restart found running: one whose turn the agent finished is
   * completed; one never sent to the agent runs again; one sent to it is interrupted,
   * never sent twice, since the agent may have acted on it (the turn's restart recovery
   * continues it). Returns the conversations whose turn the agent finished.
   */
  recoverAfterRestart(): Effect.Effect<string[], StoreError> {
    return this.#sql.query<{ conversation_id: string }>(
      `with settled as (
         update ${this.#schema}.prompt_jobs set
           state = case
             when upstream_completed_at is not null then 'completed'
             when upstream_dispatched_at is null and upstream_started_at is null then 'pending'
             else 'interrupted'
           end,
           started_at = case when upstream_dispatched_at is null and upstream_started_at is null and upstream_completed_at is null then null else started_at end,
           completed_at = case
             when upstream_dispatched_at is null and upstream_started_at is null and upstream_completed_at is null then completed_at
             else coalesce(completed_at, now())
           end
         where state = 'running'
         returning conversation_id, state
       )
       select distinct conversation_id from settled where state = 'completed'`,
    ).pipe(Effect.map((rows) => rows.map((row) => row.conversation_id)));
  }
}
