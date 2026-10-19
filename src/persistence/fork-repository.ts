import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** What became of a request to fork a session for a branch: forked, not the parent's to give, a turn running in it, or refused. */
export type ForkOutcome = "forked" | "unknown" | "busy" | "refused";

/** A request's answer: its outcome, the internet a session forked was made with, and why one was refused. */
export interface ForkAnswer {
  readonly outcome: ForkOutcome;
  readonly netMode: "none" | "full" | null;
  readonly reason: string | null;
}

/** A request answered: the session it asked for, and its answer. */
export interface ForkRequestAnswered {
  readonly volumeId: string;
  readonly answer: ForkAnswer;
}

/** A row of fork_requests or fork_answers, as either is read. */
interface AnswerRow {
  readonly volume_id: string;
  readonly outcome: ForkOutcome;
  readonly net_mode: "none" | "full" | null;
  readonly reason: string | null;
}

const answeredOf = ([row]: readonly AnswerRow[]): ForkRequestAnswered | null =>
  (row ? { volumeId: row.volume_id, answer: { outcome: row.outcome, netMode: row.net_mode, reason: row.reason } } : null);

/**
 * What alasio keeps of the forks of sessions between an environment and its branches: as a
 * parent, each request it answered (`fork_requests`); as a branch, each answer to a request of
 * its own (`fork_answers`).
 */
export class NeonForkRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /** The session `branch`'s request `id` asked for and how it was answered, or null where it is yet to be. */
  requestAnswered(branch: string, id: string): Effect.Effect<ForkRequestAnswered | null, StoreError> {
    return this.#sql.query<AnswerRow>(`select volume_id, outcome, net_mode, reason from ${this.#schema}.fork_requests where branch = $1 and id = $2`, [branch, id]).pipe(Effect.map(answeredOf));
  }

  /** Records `branch`'s request `id` for `volumeId`, answered as `answer`, unless it was already: what it holds. */
  recordRequest(branch: string, id: string, volumeId: string, answer: ForkAnswer): Effect.Effect<ForkRequestAnswered, StoreError> {
    return this.#sql.query<AnswerRow>(
      `with kept as (
         insert into ${this.#schema}.fork_requests (branch, id, volume_id, outcome, net_mode, reason) values ($1, $2, $3, $4, $5, $6)
         on conflict (branch, id) do nothing
         returning volume_id, outcome, net_mode, reason
       )
       select volume_id, outcome, net_mode, reason from kept
       union all
       select volume_id, outcome, net_mode, reason from ${this.#schema}.fork_requests where branch = $1 and id = $2`,
      [branch, id, volumeId, answer.outcome, answer.netMode, answer.reason],
    ).pipe(Effect.map((rows) => answeredOf(rows) ?? { volumeId, answer }));
  }

  /** Records the answer to this alasio's request `request`, for `volumeId`, unless it was already. */
  recordAnswer(request: string, volumeId: string, answer: ForkAnswer): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.fork_answers (request, volume_id, outcome, net_mode, reason) values ($1, $2, $3, $4, $5) on conflict (request) do nothing`,
      [request, volumeId, answer.outcome, answer.netMode, answer.reason],
    ));
  }

  /** The answer to this alasio's request `request`, or null where none has come yet. */
  answerTo(request: string): Effect.Effect<ForkAnswer | null, StoreError> {
    return this.#sql.query<AnswerRow>(`select volume_id, outcome, net_mode, reason from ${this.#schema}.fork_answers where request = $1`, [request]).pipe(Effect.map((rows) => answeredOf(rows)?.answer ?? null));
  }

  /** Forgets the requests answered, and the answers, more than `ageMs` ago. */
  prune(ageMs: number): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `with requests as (
         delete from ${this.#schema}.fork_requests where answered_at < now() - make_interval(secs => $1)
       )
       delete from ${this.#schema}.fork_answers where answered_at < now() - make_interval(secs => $1)`,
      [ageMs / 1000],
    ));
  }
}
