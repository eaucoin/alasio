/**
 * What the harness host's role (src/neon/harness.ts) may do in alasio's state: the turn's
 * part of it and nothing more. It writes the turn it runs, and the turns its harness
 * starts on its own (claiming, ending, and what each writes as it runs: its response, its
 * session, its usage, what it waits on, a restart it causes, and its end, which lets go
 * of a restart recorded for it), and its job's progress; it reads the files a prompt
 * names, and the holds that keep a conversation busy as no turn. Telegram's updates, the
 * buttons, the outbox, making holds, the workspaces and the Codex login, which alasio alone
 * keeps and refreshes, as two refreshing it would end it, are the control plane's alone.
 * Every harness pool's pods, a session conversation's and a folder conversation's, write as
 * this one role.
 */
import { Effect } from "effect";

import { SESSION_COLUMNS } from "./conversation-repository.ts";
import type { Sql, StoreError } from "./sql.ts";

/** The progress a job's turn records of it (./prompt-job-repository.ts markDispatched, markUpstreamStarted, markUpstreamCompleted). */
const JOB_PROGRESS = ["upstream_dispatched_at", "upstream_session_id", "upstream_turn_id", "upstream_started_at", "upstream_completed_at"] as const;

/** Each table's privileges, as a grant names them. */
const GRANTS: Readonly<Record<string, string>> = {
  turns: "select, insert, update",
  responses: "select, insert, update",
  response_blocks: "select, insert",
  session_usage: "select, insert, update",
  // Deleted as a turn of its own ends (../persistence/store.ts endTurn).
  restart_events: "select, insert, update, delete",
  files: "select",
  conversation_holds: "select",
  // A turn's session is the conversation's, as its row says (./turn-repository.ts updateSessionIdAt).
  conversations: `select, update (${[...Object.values(SESSION_COLUMNS), "updated_at"].join(", ")})`,
  prompt_jobs: `select, update (${JOB_PROGRESS.join(", ")})`,
};

/** Grants `role` what the module says in `schema`, whose tables are made. Idempotent. */
export const grantHarness = (sql: Sql, schema: string, role: string): Effect.Effect<void, StoreError> =>
  Effect.asVoid(sql.query([
    `grant usage on schema ${schema} to ${role}`,
    ...Object.entries(GRANTS).map(([table, privileges]) => `grant ${privileges} on ${schema}.${table} to ${role}`),
  ].join(";\n")));
