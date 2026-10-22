/**
 * What the harness host's role (src/neon/harness.ts) may do in alasio's state: the turn's
 * part of it and nothing more. It writes the turn it runs, and the turns its harness
 * starts on its own (claiming, ending, and what each writes as it runs: its response, its
 * session, its usage, what it waits on, and its end), and its job's progress; it reads the files a prompt
 * names, and the holds that keep a conversation busy as no turn; and it reads and writes the
 * transcripts and rollouts of its sessions, in the stores of Claude Code's and Codex's.
 * Telegram's updates, the buttons, the outbox, making holds, the workspaces, the Codex login,
 * which alasio alone keeps and refreshes, as two refreshing it would end it, the transcripts'
 * search index, the grants and the sessions' conversations are the control plane's alone.
 *
 * Every harness pool's pod, a session conversation's and a folder conversation's, writes as
 * this one role, so what the role may do is only what each pod may: of its own conversation,
 * as its grant says (../harness/grant.ts). Row-level security holds each table it reaches to
 * the rows of the conversation whose grant its connection's setting is
 * (harness_conversation(), ./schema.ts), and the stores' tables and the sessions' usage to the
 * sessions of that conversation (harness_sessions()), a session it writes of first claimed for
 * it (harness_claim()), where no other conversation has. Its rows refused, a write of
 * another's fails, and what it reads, updates or deletes of another's it does not see. The
 * policies restrict the role alone: every other role reaches the rows as its grants say, the
 * lake's reading the stores among them, and alasio, the tables' owner, is held by none.
 */
import { createHash } from "node:crypto";

import { Effect } from "effect";

import { SESSION_COLUMNS } from "./conversation-repository.ts";
import { setupLock } from "./schema.ts";
import type { Sql, StoreError } from "./sql.ts";

/** The progress a job's turn records of it (./prompt-job-repository.ts markDispatched, markUpstreamStarted, markUpstreamCompleted). */
const JOB_PROGRESS = ["upstream_dispatched_at", "upstream_session_id", "upstream_turn_id", "upstream_started_at", "upstream_completed_at"] as const;

/** The schemas of the transcripts' and rollouts' stores (../harness/claude/session-store.ts, ../codex/rollouts/store.ts). */
const STORES = { claude_sessions: "claude", codex_sessions: "codex", codex_sessionfs_sessions: "codex" } as const;

/** A table's privileges, as a grant names them, and the rows of its the role reaches, read (`using`) and written (`check`), as its policy says them. */
interface Reach {
  readonly privileges: string;
  readonly using: string;
  readonly check: string;
}

/** Of `schema`'s state: the conversation of the connection's grant, and its sessions, each read once a statement. */
const grantOf = (schema: string) => ({ conversation: `(select ${schema}.harness_conversation())`, sessions: `(select ${schema}.harness_sessions())::text[]`, claim: (column: string) => `${schema}.harness_claim(${column})` });

/** Each table of the state's the role reaches, as the module says. */
const stateReach = (schema: string): Readonly<Record<string, Reach>> => {
  const { conversation, sessions, claim } = grantOf(schema);
  /** Rows of the grant's conversation, by `column`, written naming its sessions alone, by `sessionColumns`. */
  const of = (privileges: string, column: string, ...sessionColumns: string[]): Reach => ({
    privileges,
    using: `${column} = ${conversation}`,
    check: [`${column} = ${conversation}`, ...sessionColumns.map(claim)].join(" and "),
  });
  return {
    turns: of("select, insert, update", "conversation_id", "session_id"),
    responses: of("select, insert, update", "thread_key", "session_id"),
    response_blocks: {
      privileges: "select, insert",
      using: `response_id in (select id from ${schema}.responses where thread_key = ${conversation})`,
      check: `response_id in (select id from ${schema}.responses where thread_key = ${conversation})`,
    },
    session_usage: { privileges: "select, insert, update", using: `session_id = any (${sessions})`, check: claim("session_id") },
    files: of("select", "conversation_id"),
    conversation_holds: of("select", "conversation_id"),
    // A turn's session is the conversation's, as its row says (./turn-repository.ts updateSessionIdAt).
    conversations: of(`select, update (${[...Object.values(SESSION_COLUMNS), "updated_at"].join(", ")})`, "id", ...Object.values(SESSION_COLUMNS)),
    prompt_jobs: of(`select, update (${JOB_PROGRESS.join(", ")})`, "conversation_id", "upstream_session_id"),
  };
};

/** Each table of a store's the role reaches, in `store` of `kind`, as the module says, of the state in `schema`. */
const storeReach = (schema: string, store: string, kind: "claude" | "codex"): Readonly<Record<string, Reach>> => {
  const { sessions, claim } = grantOf(schema);
  const all = "select, insert, update, delete";
  const bySession = (column: string): Reach => ({ privileges: all, using: `${column} = any (${sessions})`, check: claim(column) });
  if (kind === "claude") return { entries: bySession("session_id"), summaries: bySession("session_id") };
  const rollouts = `name in (select name from ${store}.rollouts where thread_id = any (${sessions}))`;
  return { rollouts: bySession("thread_id"), rollout_chunks: { privileges: all, using: rollouts, check: rollouts } };
};

/**
 * `table`'s rows held to `reach` for `role`, as the module says: every role's policy, which
 * leaves each as its grants say, made where it is not; the role's, which restricts it, made anew
 * where it is not as `reach` and `role` say now, as a release that changes what the role reaches
 * changes it, known by the digest of what it was made of, which it is commented with; then
 * row-level security on. Nothing taken of a table whose policies are as they say already, as
 * alasio's start finds them, so no start locks the tables a pod writes but to change them.
 */
const heldTo = (table: string, role: string, { using, check }: Reach): string => {
  const made = createHash("sha256").update(JSON.stringify([role, using, check])).digest("hex");
  return `
do $$ begin
  if not exists (select from pg_policy where polrelid = '${table}'::regclass and polname = 'rows_granted') then
    create policy rows_granted on ${table} using (true) with check (true);
  end if;
  if (select obj_description(oid, 'pg_policy') from pg_policy where polrelid = '${table}'::regclass and polname = 'harness_rows') is distinct from '${made}' then
    drop policy if exists harness_rows on ${table};
    create policy harness_rows on ${table} as restrictive to ${role} using (${using}) with check (${check});
    comment on policy harness_rows on ${table} is '${made}';
  end if;
  if not (select relrowsecurity from pg_class where oid = '${table}'::regclass) then
    alter table ${table} enable row level security;
  end if;
end $$`;
};

/**
 * Grants `role` what the module says in `schema`, whose tables are made, and in the stores
 * made by then, as alasio makes them before its state (../main.ts), its rows held to its
 * grant's: what it was granted before, and is not now, revoked. Idempotent.
 */
export const grantHarness = (sql: Sql, schema: string, role: string): Effect.Effect<void, StoreError> =>
  Effect.gen(function*() {
    const stores = yield* sql.query<{ name: keyof typeof STORES }>("select nspname as name from pg_namespace where nspname = any($1)", [Object.keys(STORES)]);
    const reaches = [
      ...Object.entries(stateReach(schema)).map(([table, reach]) => [`${schema}.${table}`, reach] as const),
      ...stores.flatMap(({ name }) => Object.entries(storeReach(schema, name, STORES[name])).map(([table, reach]) => [`${name}.${table}`, reach] as const)),
    ];
    yield* sql.query([
      setupLock(schema),
      ...[schema, ...stores.map(({ name }) => name)].flatMap((granted) => [`grant usage on schema ${granted} to ${role}`, `revoke all on all tables in schema ${granted} from ${role}`]),
      `grant execute on function ${schema}.harness_conversation(), ${schema}.harness_sessions(), ${schema}.harness_claim(text) to ${role}`,
      ...reaches.flatMap(([table, reach]) => [`grant ${reach.privileges} on ${table} to ${role}`, heldTo(table, role, reach)]),
    ].join(";\n"));
  });
