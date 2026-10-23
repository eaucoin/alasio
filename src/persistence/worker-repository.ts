import { Context, Effect } from "effect";

import { type Sql, StoreError } from "./sql.ts";

/** A conversation's worker's claim, as it was taken: the conversation, and the generation it took, which its writes hold under. */
export interface WorkerFence {
  readonly conversationId: string;
  readonly generation: number;
}

/**
 * The claim of the worker whose work runs now, where one does: the store's writes made under it
 * hold under it (Store's fenced writes), failing with WorkerTakenOver where another alasio has
 * taken the conversation since. None outside a worker's work, where they hold as they are.
 */
export const CurrentWorkerFence = Context.Reference<WorkerFence | null>("alasio/persistence/CurrentWorkerFence", { defaultValue: () => null });

/** A write of a worker's refused, as another alasio took its conversation over since its claim was taken. */
export class WorkerTakenOver extends Error {
  constructor({ conversationId, generation }: WorkerFence) {
    super(`the worker of ${conversationId} of generation ${generation} was taken over by another alasio, so it writes nothing more`);
  }
}

/** `fence`'s writes refused, as a store's failure saying why. */
export const takenOver = (fence: WorkerFence): StoreError => new StoreError({ cause: new WorkerTakenOver(fence) });

/** The workers' claims of their conversations (Store.claimWorker), on one connection or transaction. */
export class NeonWorkerRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /**
   * Takes the conversation's claim for `holder`, for `lifeMs`, where none holds it or its holder's
   * has lapsed, one generation past the last: the generation taken, or null where another holds it.
   */
  take(conversationId: string, holder: string, lifeMs: number): Effect.Effect<number | null, StoreError> {
    return this.#sql.query<{ generation: string }>(
      `insert into ${this.#schema}.conversation_workers as w (conversation_id, holder, generation, held_until)
       values ($1, $2, 1, now() + $3 * interval '1 millisecond')
       on conflict (conversation_id) do update
         set holder = excluded.holder, generation = w.generation + 1, held_until = excluded.held_until
         where w.held_until <= now()
       returning generation`,
      [conversationId, holder, lifeMs],
    ).pipe(Effect.map(([row]) => (row ? Number(row.generation) : null)));
  }

  /** Keeps `fence`'s claim for `lifeMs` more: whether it is still the conversation's, no other having taken it since. */
  renew({ conversationId, generation }: WorkerFence, lifeMs: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.conversation_workers set held_until = now() + $3 * interval '1 millisecond'
       where conversation_id = $1 and generation = $2 returning 1`,
      [conversationId, generation, lifeMs],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Lets go of `fence`'s claim, so another takes it at once: none, where another has taken it since. */
  release({ conversationId, generation }: WorkerFence): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversation_workers set held_until = now() where conversation_id = $1 and generation = $2`,
      [conversationId, generation],
    ));
  }

  /**
   * Lets go of the claims held by any process of the pod `pod`, its holder `pod/uuid`, as one
   * starting in the pod does, the processes before it gone: how many.
   */
  lapseOf(pod: string): Effect.Effect<number, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.conversation_workers set held_until = now() where split_part(holder, '/', 1) = $1 and held_until > now() returning 1`,
      [pod],
    ).pipe(Effect.map((rows) => rows.length));
  }

  /**
   * Whether `fence` is still its conversation's claim, its row kept from changing until the
   * transaction this is asked in ends: a takeover waits for it, and the writes it guards land
   * before the takeover or not at all.
   */
  holds({ conversationId, generation }: WorkerFence): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `select 1 from ${this.#schema}.conversation_workers where conversation_id = $1 and generation = $2 for share`,
      [conversationId, generation],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /**
   * The conversations with work left that no worker's claim holds: an active turn alasio drives,
   * as one its harness started on its own, which carries no prompt, is its harness host's; or a
   * prompt job to run or running.
   */
  unheld(): Effect.Effect<string[], StoreError> {
    return this.#sql.query<{ conversation_id: string }>(
      `select distinct work.conversation_id from (
         select conversation_id from ${this.#schema}.turns where state = 'active' and prompt is not null
         union select conversation_id from ${this.#schema}.prompt_jobs where state in ('pending', 'running', 'steering')
       ) work
       where not exists (
         select 1 from ${this.#schema}.conversation_workers w where w.conversation_id = work.conversation_id and w.held_until > now()
       )
       order by work.conversation_id`,
    ).pipe(Effect.map((rows) => rows.map(({ conversation_id }) => conversation_id)));
  }
}
