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
   * A response to the message `messageId` of the chat, as its harness begins it: its id.
   * Any earlier response to the same message that was never delivered is let go of.
   */
  createPendingResponse(chatId: number | string, messageId: number | string, sessionId: string | null = null): Effect.Effect<string, StoreError> {
    const id = newId();
    return this.#sql.query(
      `with superseded as (
         update ${this.#schema}.responses set posted = true where chat_id = $2 and message_id = $3 and not posted
       )
       insert into ${this.#schema}.responses (id, chat_id, message_id, session_id) values ($1, $2, $3, $4)`,
      [id, String(chatId), String(messageId), sessionId],
    ).pipe(Effect.as(id));
  }

  /** Adds blocks to a response, after those it has, in order. */
  appendBlocks(pendingResponseId: string, blocks: readonly ResponseBlock[]): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `insert into ${this.#schema}.response_blocks (response_id, block)
       select $1, block from unnest($2::json[]) with ordinality as blocks (block, n) order by n`,
      [pendingResponseId, blocks.map((block) => JSON.stringify(block))],
    ));
  }

  markPendingComplete(pendingResponseId: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.responses set completed = true where id = $1`, [pendingResponseId]));
  }

  updatePendingSessionId(pendingResponseId: string, sessionId: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.responses set session_id = $2 where id = $1`, [pendingResponseId, sessionId]));
  }

  markPendingAsPosted(pendingResponseId: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`update ${this.#schema}.responses set posted = true where id = $1`, [pendingResponseId]));
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
