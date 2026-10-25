import { Effect } from "effect";

import type { HarnessName } from "../harness/names.ts";
import { newId } from "../shared/ids.ts";
import type { Sql, StoreError } from "./sql.ts";

/**
 * Where a prompt job is: waiting its turn (or for the operator to say what to do with
 * it, while another runs), being handed to the running turn to steer it (steering, until
 * it is known whether the turn took it), running, or done one way or another.
 */
export type PromptJobState = "pending" | "awaiting_choice" | "steering" | "running" | "completed" | "cancelled" | "failed" | "interrupted";

/** A row of `prompt_jobs`: a prompt queued for its conversation's harness. */
export interface PromptJob {
  readonly id: string;
  readonly conversation_id: string;
  readonly chat_id: string;
  readonly message_id: string;
  readonly prompt: string;
  /** What the operator wrote, which the prompt is made from, as a question about it quotes it. */
  readonly visible_text: string;
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
  /** Of a goal's turn: the session its goal is of, which it runs on where the conversation mounts it still. */
  readonly goal_session_id: string | null;
  readonly created_at: Date;
  readonly started_at: Date | null;
  readonly completed_at: Date | null;
}

export interface NewPromptJob {
  readonly conversationId: string;
  readonly chatId: number | string;
  readonly messageId: number | string;
  readonly prompt: string;
  /** What the operator wrote, which the prompt is made from; the prompt itself unless given. */
  readonly visibleText?: string | undefined;
  readonly fileIds?: readonly string[] | undefined;
  readonly state?: PromptJobState | undefined;
  readonly priority?: number | undefined;
  /** The harness to run the prompt on; the conversation's mounted one when not given. */
  readonly harness?: HarnessName | undefined;
  /** The W3C traceparent of the trace the prompt was queued in. */
  readonly traceparent?: string | null | undefined;
  /** Of a goal's turn: the session its goal is of, which it runs on where the conversation mounts it still. */
  readonly goalSessionId?: string | null | undefined;
}

export class NeonPromptJobRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /**
   * Queues a prompt: the job queued, or the one already queued for its message, as an update
   * handled again queues it again.
   */
  enqueue({ conversationId, chatId, messageId, prompt, visibleText = prompt, fileIds = [], state = "pending", priority = 0, harness, traceparent = null, goalSessionId = null }: NewPromptJob): Effect.Effect<PromptJob, StoreError> {
    // The no-op update makes the job already queued the one returned.
    return this.#sql.query<PromptJob>(
      `insert into ${this.#schema}.prompt_jobs (id, conversation_id, chat_id, message_id, prompt, visible_text, file_ids, harness, state, priority, traceparent, goal_session_id)
       select $1, id, $3, $4, $5, $11, $6, coalesce($7, active_harness), $8, $9, $10, $12 from ${this.#schema}.conversations where id = $2
       on conflict (conversation_id, message_id) do update set conversation_id = excluded.conversation_id
       returning *`,
      [newId(), conversationId, String(chatId), String(messageId), prompt, fileIds, harness ?? null, state, priority, traceparent, visibleText, goalSessionId],
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

  /**
   * Settles what becomes of a job: completed and cancelled jobs are done, any other state is
   * not. Its priority is `priority`, where given, and stays as it was otherwise.
   */
  setDisposition(id: string, state: PromptJobState, priority: number | null = null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs
       set state = $2, priority = coalesce($3::int, priority), completed_at = case when $2 in ('completed', 'cancelled') then now() end
       where id = $1`,
      [id, state, priority],
    ));
  }

  /**
   * Settles the job its worker runs as `state` says, where it runs still, not run again since
   * (requeueUnsent), whose next run settles it: whether it did.
   */
  settleRunning(id: string, state: PromptJobState, priority: number | null = null): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.prompt_jobs
       set state = $2, priority = coalesce($3::int, priority), completed_at = case when $2 in ('completed', 'cancelled') then now() end
       where id = $1 and state = 'running'
       returning 1`,
      [id, state, priority],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Settles a job held for the operator's choice as `state` says: the job, or null where it
   * is held no longer (settled already, by another of its buttons), when nothing changes.
   */
  settleHeld(id: string, state: PromptJobState, priority = 0): Effect.Effect<PromptJob | null, StoreError> {
    return this.#sql.query<PromptJob>(
      `update ${this.#schema}.prompt_jobs
       set state = $2, priority = $3, completed_at = case when $2 in ('completed', 'cancelled') then now() end
       where id = $1 and state = 'awaiting_choice'
       returning *`,
      [id, state, priority],
    ).pipe(Effect.map(([row]) => row ?? null));
  }

  /**
   * Records that the job's prompt is sent to the agent, which may act on it from then on: only
   * while the job runs still, and its turn of `epoch` that sends it is still its conversation's.
   * A recovery running the job again (requeueUnsent) changes the job's row in the transaction that
   * ends the turn; this, waiting on that row, re-checks the job's state once it is through, and
   * finds it run again: so the two never both land. The turn is read as the statement began, the
   * job as it is once through, claimed before the turn started: so one claimed anew since, which
   * started after it, passes for that turn's no more than one run again, its start cleared.
   * Whether it was recorded: where not, its prompt is not to be sent.
   */
  markDispatched(id: string, epoch: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.prompt_jobs j set upstream_dispatched_at = coalesce(upstream_dispatched_at, now())
       where j.id = $1 and j.state = 'running'
         and exists (select 1 from ${this.#schema}.turns t where t.id = j.conversation_id and t.epoch = $2 and t.state = 'active' and t.thread_ts = j.message_id
           and j.started_at <= t.started_at)
       returning 1`,
      [id, epoch],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Puts the conversation's job of `messageId` back to run again where it runs and its prompt was
   * never sent to the agent (markDispatched): whether it did.
   */
  requeueUnsent(conversationId: string, messageId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.prompt_jobs set state = 'pending', started_at = null
       where conversation_id = $1 and message_id = $2 and state = 'running'
         and upstream_dispatched_at is null and upstream_started_at is null and upstream_completed_at is null
       returning 1`,
      [conversationId, messageId],
    ).pipe(Effect.map((rows) => rows.length > 0));
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

  /**
   * Fails the job its worker runs, where it runs still, not run again since (requeueUnsent), and
   * its turn is not left active: one whose harness was not reached once its turn began, which that
   * turn's recovery settles (endLeftOf, recoverLeftOf), is not failed, as it may yet run again as
   * itself, its prompt never sent; nor one whose turn could not be ended as it failed, which the
   * conversation's next claim settles (recoverLeft), its status message saying it failed. Whether
   * it did.
   */
  fail(id: string, error: unknown): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.prompt_jobs j set state = 'failed', last_error = $2, completed_at = now()
       where j.id = $1 and j.state = 'running'
         and not exists (select 1 from ${this.#schema}.turns t where t.id = j.conversation_id and t.state = 'active' and t.thread_ts = j.message_id)
       returning 1`,
      [id, String(error).slice(0, 2000)],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * Ends the job a worker runs of the conversation's turn of `messageId` that started at
   * `turnStarted`, as that turn is ended lost and not run again as itself
   * (Store.stageLostRecovery): completed, where the agent finished it or its turn's response was
   * complete (`completed`); interrupted otherwise, never run again. That job alone, claimed before
   * its turn started (as recoverLeftOf reads it), not one claimed anew since, nor another of the
   * conversation's claimed as a turn not its own ran.
   */
  endLeftOf(conversationId: string, messageId: string, turnStarted: Date, completed: boolean): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.prompt_jobs
       set state = case when upstream_completed_at is not null or $4 then 'completed' else 'interrupted' end, completed_at = coalesce(completed_at, now())
       where conversation_id = $1 and message_id = $2 and state = 'running' and date_trunc('milliseconds', started_at) <= $3`,
      [conversationId, messageId, turnStarted, completed],
    ));
  }

  hasOpenJobs(conversationId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query<{ open: boolean }>(
      `select exists (
         select 1 from ${this.#schema}.prompt_jobs where conversation_id = $1 and state in ('pending', 'running', 'awaiting_choice', 'steering')
       ) as open`,
      [conversationId],
    ).pipe(Effect.map(([row]) => row!.open));
  }

  /** Whether the conversation has jobs to run. */
  hasPendingJobs(conversationId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query<{ pending: boolean }>(
      `select exists (select 1 from ${this.#schema}.prompt_jobs where conversation_id = $1 and state = 'pending') as pending`,
      [conversationId],
    ).pipe(Effect.map(([row]) => row!.pending));
  }

  /** The conversation's jobs held for the operator's choice, in the order queued. */
  listHeld(conversationId: string): Effect.Effect<PromptJob[], StoreError> {
    return this.#sql.query<PromptJob>(
      `select * from ${this.#schema}.prompt_jobs where conversation_id = $1 and state = 'awaiting_choice' order by seq`,
      [conversationId],
    );
  }

  /** The conversations with jobs waiting: to run, or for the operator's choice. */
  listWaitingConversations(): Effect.Effect<string[], StoreError> {
    return this.#sql.query<{ conversation_id: string }>(
      `select distinct conversation_id from ${this.#schema}.prompt_jobs where state in ('pending', 'awaiting_choice') order by conversation_id`,
    ).pipe(Effect.map((rows) => rows.map((row) => row.conversation_id)));
  }

  /**
   * Holds again for the operator's choice the conversation's jobs its last worker left being
   * handed to a turn to steer it, which it may or may not have taken: the jobs, to be asked
   * about.
   */
  holdUnsteeredLeft(conversationId: string): Effect.Effect<PromptJob[], StoreError> {
    return this.#sql.query<PromptJob>(
      `update ${this.#schema}.prompt_jobs set state = 'awaiting_choice', completed_at = null where conversation_id = $1 and state = 'steering' returning *`,
      [conversationId],
    );
  }

  /**
   * Settles the conversation's jobs its last worker left running: one whose turn the agent
   * finished is completed; one never sent to the agent runs again, but where its turn is left
   * active, whose pod may hold it yet, as that turn's recovery settles it (requeueUnsent, as the
   * turn's pod knows nothing of it); one sent to it is interrupted, never sent twice, since the
   * agent may have acted on it (the turn's recovery continues it). Whether the agent finished
   * one's turn. Run as a worker's claim is taken, before anything runs under it: so every job
   * running is one the last worker left.
   */
  recoverLeft(conversationId: string): Effect.Effect<boolean, StoreError> {
    return this.#settleLeft(conversationId, "", []);
  }

  /**
   * Settles, as recoverLeft does, the job a worker left running of the conversation's turn that
   * started at `turnStarted`, attached to again under the claim since: the one claimed before
   * that turn started, its own, alone, as the conversation's worker may meanwhile run another, or
   * that one claimed anew, run again as its prompt was never sent (requeueUnsent). To the
   * millisecond, as `turnStarted` is read: one claimed anew is claimed once the turn is ended, as
   * its harness was reached or found lost, well past the millisecond it started in.
   */
  recoverLeftOf(conversationId: string, turnStarted: Date): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#settleLeft(conversationId, "and date_trunc('milliseconds', started_at) <= $2", [turnStarted]));
  }

  /** recoverLeft's settling of the conversation's jobs running, of those `where` says, its parameters from $2 on `params`. */
  #settleLeft(conversationId: string, where: string, params: readonly unknown[]): Effect.Effect<boolean, StoreError> {
    return this.#sql.query<{ finished: boolean }>(
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
         where conversation_id = $1 and state = 'running' ${where}
           and not (upstream_dispatched_at is null and upstream_started_at is null and upstream_completed_at is null
             and exists (select 1 from ${this.#schema}.turns t where t.id = $1 and t.state = 'active' and t.thread_ts = message_id))
         returning state
       )
       select exists (select 1 from settled where state = 'completed') as finished`,
      [conversationId, ...params],
    ).pipe(Effect.map(([row]) => row!.finished));
  }
}
