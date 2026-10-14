import { Effect } from "effect";

import { newId } from "../shared/ids.ts";
import type { Sql, StoreError } from "./sql.ts";

/** One block of a harness's response as it streams in. Its fields beyond `type` are the projection's. */
export interface ResponseBlock {
  readonly type: string;
  readonly [field: string]: unknown;
}

/** A response its harness completed that has not been delivered yet. */
export interface CompletedResponse {
  /** The pending response id. */
  readonly id: string;
  readonly chatId: string;
  readonly messageId: string;
  readonly session_id: string | null;
  readonly blocks: ResponseBlock[];
}

export class NeonResponseRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  /**
   * Where a response's writer still holds its conversation: no turn has claimed it since
   * the one that wrote the response began it. The turn's row is locked for share, so a
   * claim committed while the write waits on it is seen, and refuses the write, and a
   * claim waits for the write.
   */
  #held(response: string): string {
    return `exists (select 1 from ${this.#schema}.turns where turns.id = ${response}.thread_key and turns.epoch = ${response}.epoch for share)`;
  }

  /**
   * A response to the message `messageId` of the chat, as the turn of `epoch` of the
   * conversation `threadKey` begins it: its id, or null where another turn has claimed the
   * conversation since. Any earlier response to the same message that was never
   * delivered is let go of.
   */
  createPendingResponse(threadKey: string, epoch: number, chatId: number | string, messageId: number | string, sessionId: string | null = null): Effect.Effect<string | null, StoreError> {
    const id = newId();
    return this.#sql.query(
      `with writer as (
         select 1 from ${this.#schema}.turns where id = $5 and epoch = $6 for share
       ), superseded as (
         update ${this.#schema}.responses set posted = true where chat_id = $2 and message_id = $3 and not posted and exists (select 1 from writer)
       )
       insert into ${this.#schema}.responses (id, chat_id, message_id, session_id, thread_key, epoch)
       select $1, $2, $3, $4, $5, $6 where exists (select 1 from writer)
       returning id`,
      [id, String(chatId), String(messageId), sessionId, threadKey, epoch],
    ).pipe(Effect.map((rows) => (rows.length > 0 ? id : null)));
  }

  /** Adds blocks to a response, after those it has, in order: whether its writer still holds its conversation. */
  appendBlocks(pendingResponseId: string, blocks: readonly ResponseBlock[]): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `insert into ${this.#schema}.response_blocks (response_id, block)
       select $1, block from unnest($2::json[]) with ordinality as blocks (block, n)
       where exists (select 1 from ${this.#schema}.responses where id = $1 and ${this.#held("responses")})
       order by n
       returning seq`,
      [pendingResponseId, blocks.map((block) => JSON.stringify(block))],
    ).pipe(Effect.map((rows) => blocks.length === 0 || rows.length > 0));
  }

  /** Completes a response: whether its writer still holds its conversation. */
  markPendingComplete(pendingResponseId: string): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.responses set completed = true where id = $1 and ${this.#held("responses")} returning id`,
      [pendingResponseId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Records the session a response is in: whether its writer still holds its conversation. */
  updatePendingSessionId(pendingResponseId: string, sessionId: string | null): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.responses set session_id = $2 where id = $1 and ${this.#held("responses")} returning id`,
      [pendingResponseId, sessionId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  /** Lets go of a response, as delivered or never to be: whether its writer still holds its conversation, where `fenced`. */
  markPendingAsPosted(pendingResponseId: string, fenced = false): Effect.Effect<boolean, StoreError> {
    return this.#sql.query(
      `update ${this.#schema}.responses set posted = true where id = $1${fenced ? ` and ${this.#held("responses")}` : ""} returning id`,
      [pendingResponseId],
    ).pipe(Effect.map((rows) => rows.length > 0));
  }

  getCompletedResponsesPendingDelivery(): Effect.Effect<CompletedResponse[], StoreError> {
    return this.#sql.query<CompletedResponse>(
      `select responses.id, chat_id as "chatId", message_id as "messageId", session_id,
         coalesce(json_agg(block order by seq) filter (where seq is not null), '[]') as blocks
       from ${this.#schema}.responses
       left join ${this.#schema}.response_blocks on response_id = responses.id
       where completed and not posted
       group by responses.id
       order by responses.created_at`,
    );
  }
}
