import type { CallbackQuery } from "@grammyjs/types";
import { Effect } from "effect";

import type { CallbackAction } from "./callback-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

/** A press to keep (src/telegram/presses.ts): the query, and the action it took. */
export interface PressToKeep {
  readonly callbackQuery: CallbackQuery;
  readonly action: CallbackAction;
}

/** A press kept while its control runs on past its update: what it is, and its answer, where its control was done but its answer kept unsent. */
export interface KeptPress extends PressToKeep {
  readonly answer: string | null;
}

/** A press to keep: what it is, and the hold of the alasio running it, by its token, for `holdMs`. */
export interface NewKeptPress extends PressToKeep {
  readonly holder: string;
  readonly holdMs: number;
}

interface PressRow {
  readonly callback_query: CallbackQuery;
  readonly action: CallbackAction;
  readonly answer: string | null;
}

/**
 * The presses whose controls run on past their updates (`press_jobs`), each held by the
 * alasio running it, by a token of its own, until its hold lapses, which that alasio renews
 * as it runs: a press whose hold lapsed, or was let go of, is another's to run again, or,
 * where its control was done and its answer kept unsent, to queue its answer.
 */
export class NeonPressRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Keeps `press`, held as it says, unless it is kept already. */
  keep({ callbackQuery, action, holder, holdMs }: NewKeptPress): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.press_jobs (id, conversation_id, callback_query, action, holder, held_until)
       values ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
       on conflict (id) do nothing`,
      [callbackQuery.id, action.conversationId, JSON.stringify(callbackQuery), JSON.stringify(action), holder, holdMs / 1000],
    ));
  }

  /** Holds the press `id` for `holdMs` more, where `holder` holds it still: whether it does. */
  renew(id: string, holder: string, holdMs: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.press_jobs set held_until = now() + make_interval(secs => $3) where id = $1 and holder = $2 returning id`,
      [id, holder, holdMs / 1000],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Lets go of the press `id`, where `holder` holds it, for another to run again at once. */
  release(id: string, holder: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.press_jobs set held_until = now() where id = $1 and holder = $2`, [id, holder]));
  }

  /** Keeps `answer` with the press `id`, its control done, where `holder` holds it, and lets go of it, for another to queue its answer at once. */
  pendAnswer(id: string, holder: string, answer: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.press_jobs set answer = $3, held_until = now() where id = $1 and holder = $2`, [id, holder, answer]));
  }

  /** Forgets the press `id`, its control done, where `holder` holds it. */
  forget(id: string, holder: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.press_jobs where id = $1 and holder = $2`, [id, holder]));
  }

  /** Takes the presses whose holds lapsed, or were let go of, held by `holder` for `holdMs`; none another takes at once. */
  takeLapsed(holder: string, holdMs: number): Effect.Effect<KeptPress[], StoreError> {
    return this.#sql.query<PressRow>(
      `update ${this.#schema}.press_jobs taken set holder = $1, held_until = now() + make_interval(secs => $2)
       where taken.id in (
         select lapsed.id from ${this.#schema}.press_jobs lapsed where lapsed.held_until <= now() for update skip locked
       )
       returning taken.callback_query, taken.action, taken.answer`,
      [holder, holdMs / 1000],
    ).pipe(Effect.map((rows) => rows.map(({ callback_query, action, answer }) => ({ callbackQuery: callback_query, action, answer }))));
  }
}
