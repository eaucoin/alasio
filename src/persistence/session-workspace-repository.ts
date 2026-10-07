import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** A session filesystem's internet: none, or the internet's public addresses. */
export type SessionWorkspaceNetMode = "none" | "full";

/** A session filesystem alasio made, or is making: its internet and when it was made, null until it is, as a fork is while it is cloned. */
export type SessionWorkspace = {
  readonly volumeId: string;
  /** The session filesystem it is a fork of; null for one made empty. */
  readonly forkedFrom: string | null;
} & ({ readonly netMode: SessionWorkspaceNetMode; readonly madeAt: Date } | { readonly netMode: null; readonly madeAt: null });

/** A session filesystem about to be made. */
export interface NewSessionWorkspace {
  readonly volumeId: string;
  readonly forkedFrom: string | null;
}

interface SessionWorkspaceRow {
  readonly volume_id: string;
  readonly forked_from: string | null;
  readonly net_mode: SessionWorkspaceNetMode | null;
  readonly made_at: Date | null;
}

function sessionWorkspaceOf({ volume_id: volumeId, forked_from: forkedFrom, net_mode: netMode, made_at: madeAt }: SessionWorkspaceRow): SessionWorkspace {
  return netMode !== null && madeAt !== null ? { volumeId, forkedFrom, netMode, madeAt } : { volumeId, forkedFrom, netMode: null, madeAt: null };
}

export class NeonSessionWorkspaceRepository {
  readonly #sql: Sql;
  readonly #schema: string;

  constructor(sql: Sql, schema: string) {
    this.#sql = sql;
    this.#schema = schema;
  }

  record({ volumeId, forkedFrom }: NewSessionWorkspace): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`insert into ${this.#schema}.session_workspaces (volume_id, forked_from) values ($1, $2)`, [volumeId, forkedFrom]));
  }

  markMade(volumeId: string, netMode: SessionWorkspaceNetMode): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.session_workspaces set net_mode = $2, made_at = now() where volume_id = $1`,
      [volumeId, netMode],
    ));
  }

  forget(volumeId: string): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(`delete from ${this.#schema}.session_workspaces where volume_id = $1`, [volumeId]));
  }

  list(): Effect.Effect<SessionWorkspace[], StoreError> {
    return this.#sql.query<SessionWorkspaceRow>(
      `select volume_id, net_mode, forked_from, made_at from ${this.#schema}.session_workspaces order by created_at desc, volume_id`,
    ).pipe(Effect.map((rows) => rows.map(sessionWorkspaceOf)));
  }
}
