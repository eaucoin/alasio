import type { CallbackQuery } from "@grammyjs/types";
import { Effect } from "effect";

import type { CallbackAction } from "./callback-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

/**
 * A control to keep (src/telegram/controls.ts): a press of one of alasio's buttons, its query
 * and the action it took; or a command the operator sent, its conversation's chat, the
 * message it came in and its text.
 */
export type Control =
  | { readonly kind: "press"; readonly callbackQuery: CallbackQuery; readonly action: CallbackAction }
  | { readonly kind: "command"; readonly conversationId: string; readonly chatId: number | string; readonly messageId: number; readonly text: string };

/** What a control is kept by, as no other control is: a press by its query's id, a command by its conversation and message. */
export function controlId(control: Control): string {
  return control.kind === "press" ? control.callbackQuery.id : `command:${control.conversationId}:${control.messageId}`;
}

/** The conversation a control is of. */
export function controlConversation(control: Control): string {
  return control.kind === "press" ? control.action.conversationId : control.conversationId;
}

/** A control kept while it runs on past its update: what it is, and its answer, where it was done but its answer kept unsent. */
export interface KeptControl {
  readonly control: Control;
  readonly answer: string | null;
}

/** A control to keep: what it is, and the hold of the alasio running it, by its token, for `holdMs`. */
export interface NewKeptControl {
  readonly control: Control;
  readonly holder: string;
  readonly holdMs: number;
}

interface ControlRow {
  readonly control: Control;
  readonly answer: string | null;
}

/**
 * The controls that run on past their updates (`control_jobs`), each held by the alasio
 * running it, by a token of its own, until its hold lapses, which that alasio renews as it
 * runs: a control whose hold lapsed, or was let go of, is another's to run again, or, where it
 * was done and its answer kept unsent, to queue its answer; and what controls made
 * (`control_results`), by keys of their own, so one run again finds what its run before made.
 */
export class NeonControlRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** Keeps `control`, held as it says, unless it is kept already. */
  keep({ control, holder, holdMs }: NewKeptControl): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.control_jobs (id, conversation_id, control, holder, held_until)
       values ($1, $2, $3, $4, now() + make_interval(secs => $5))
       on conflict (id) do nothing`,
      [controlId(control), controlConversation(control), JSON.stringify(control), holder, holdMs / 1000],
    ));
  }

  /** Holds the control `id` for `holdMs` more, where `holder` holds it still: whether it does. */
  renew(id: string, holder: string, holdMs: number): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.control_jobs set held_until = now() + make_interval(secs => $3) where id = $1 and holder = $2 returning id`,
      [id, holder, holdMs / 1000],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Lets go of the control `id`, where `holder` holds it, for another to run again at once. */
  release(id: string, holder: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.control_jobs set held_until = now() where id = $1 and holder = $2`, [id, holder]));
  }

  /** Keeps `answer` with the control `id`, done, where `holder` holds it, and lets go of it, for another to queue its answer at once. */
  pendAnswer(id: string, holder: string, answer: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.control_jobs set answer = $3, held_until = now() where id = $1 and holder = $2`, [id, holder, answer]));
  }

  /** Forgets the control `id`, done, where `holder` holds it. */
  forget(id: string, holder: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.control_jobs where id = $1 and holder = $2`, [id, holder]));
  }

  /** What a control made, kept under `key` (keepResult): null where nothing is. */
  resultOf(key: string): Effect.Effect<unknown, StoreError> {
    return this.#sql.query<{ result: unknown }>(`select result from ${this.#schema}.control_results where key = $1`, [key]).pipe(Effect.map(([row]) => row?.result ?? null));
  }

  /** Keeps `result` under `key`, unless a result is kept there already: the one kept there now. */
  keepResult(key: string, result: unknown): Effect.Effect<unknown, StoreError> {
    // The no-op update makes the result already kept the one returned.
    return this.#sql.query<{ result: unknown }>(
      `insert into ${this.#schema}.control_results (key, result) values ($1, $2)
       on conflict (key) do update set key = excluded.key
       returning result`,
      [key, JSON.stringify(result)],
    ).pipe(Effect.map(([row]) => row?.result ?? null));
  }

  /** Forgets the results kept longer ago than `ageMs`, past any control's running again. */
  pruneResults(ageMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.control_results where created_at < now() - make_interval(secs => $1)`, [ageMs / 1000]));
  }

  /** Takes the controls whose holds lapsed, or were let go of, held by `holder` for `holdMs`; none another takes at once. */
  takeLapsed(holder: string, holdMs: number): Effect.Effect<KeptControl[], StoreError> {
    return this.#sql.query<ControlRow>(
      `update ${this.#schema}.control_jobs taken set holder = $1, held_until = now() + make_interval(secs => $2)
       where taken.id in (
         select lapsed.id from ${this.#schema}.control_jobs lapsed where lapsed.held_until <= now() for update skip locked
       )
       returning taken.control, taken.answer`,
      [holder, holdMs / 1000],
    ).pipe(Effect.map((rows) => rows.map(({ control, answer }) => ({ control, answer }))));
  }
}
