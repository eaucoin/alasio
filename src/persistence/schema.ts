/**
 * alasio's state in Postgres: one schema, made (idempotently) as the store opens.
 *
 * JSON is kept as `json`, its exact text: what Telegram and the harnesses send may hold a
 * NUL, which `jsonb` refuses. Times are `timestamptz`, and the order things were queued
 * in is an identity column's.
 */
import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

/** The rows that name a conversation's sessions: their table, the column of their conversation, and those of the sessions. */
const SESSION_ROWS: ReadonlyArray<readonly [table: string, conversation: string, sessions: readonly string[]]> = [
  ["conversations", "id", ["codex_session_id", "claude_session_id"]],
  ["workspace_sessions", "conversation_id", ["session_id"]],
  ["messages", "conversation_id", ["session_id"]],
  ["turns", "conversation_id", ["session_id"]],
  ["responses", "thread_key", ["session_id"]],
  ["prompt_jobs", "conversation_id", ["upstream_session_id"]],
  ["callback_actions", "conversation_id", ["expected_session_id"]],
];

/** The trigger that claims the sessions `table`'s rows name, as they are written, made where it is not (claim_sessions): no lock taken of a table that has it. */
const claimTrigger = (SCHEMA: string, [table, conversation, sessions]: (typeof SESSION_ROWS)[number]): string => `
do $$ begin
  if not exists (select from pg_trigger where tgrelid = '${SCHEMA}.${table}'::regclass and tgname = '${table}_claim_sessions') then
    create trigger ${table}_claim_sessions after insert or update of ${[conversation, ...sessions].join(", ")} on ${SCHEMA}.${table}
      for each row execute function ${SCHEMA}.claim_sessions(${[conversation, ...sessions].map((column) => `'${column}'`).join(", ")});
  end if;
end $$;`;

/**
 * The first statement of each of the schema's setups (ensureSchema, ./harness-grants.ts
 * grantHarness), each one transaction: it waits for any other alasio's setup of the same schema
 * to end, as two alasios starting at once would refuse each other's replacing of the same
 * function or trigger, or granting on the same table ("tuple concurrently updated").
 */
export const setupLock = (SCHEMA: string): string => `select pg_advisory_xact_lock(hashtextextended('${SCHEMA} setup', 0))`;

const ddl = (SCHEMA: string): string => `
${setupLock(SCHEMA)};
create schema if not exists ${SCHEMA};

-- What alasio keeps by name: the branch environment its state is of (./branch-start.ts).
create table if not exists ${SCHEMA}.bot_state (
  key text primary key,
  value text not null,
  updated_at timestamptz not null default now()
);

create table if not exists ${SCHEMA}.conversations (
  id text primary key,
  transport text not null,
  chat_id text not null,
  user_id text,
  username text,
  first_name text,
  last_name text,
  codex_session_id text,
  claude_session_id text,
  -- null while no service is mounted
  active_harness text,
  -- null while no folder is mounted
  working_directory text,
  -- a model and effort chosen with /model, per harness; null for the harness's default
  claude_model text,
  claude_effort text,
  codex_model text,
  codex_effort text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (transport, chat_id)
);

-- The sessions a conversation had in the folders it mounted before, to restore.
create table if not exists ${SCHEMA}.workspace_sessions (
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  harness text not null,
  working_directory text not null,
  session_id text,
  updated_at timestamptz not null default now(),
  primary key (conversation_id, harness, working_directory)
);

-- The session filesystems alasio made, and those it is making: a fork is recorded before
-- its workspace is made, and made only once it is cloned, so one a crash cut short is known.
create table if not exists ${SCHEMA}.session_workspaces (
  volume_id text primary key,
  -- the session filesystem it is a fork of; null for one made empty
  forked_from text,
  -- both null while it is made
  net_mode text,
  made_at timestamptz,
  created_at timestamptz not null default now()
);

create table if not exists ${SCHEMA}.telegram_updates (
  update_id bigint primary key,
  payload json not null,
  received_at timestamptz not null default now(),
  processed_at timestamptz
);

create table if not exists ${SCHEMA}.messages (
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  direction text not null,
  kind text not null,
  transport_message_id text,
  text text,
  media_group_id text,
  raw json,
  session_id text,
  turn_id text,
  created_at timestamptz not null default now()
);

create index if not exists messages_media_group on ${SCHEMA}.messages (media_group_id);
-- A Telegram message kept once, by its id and the time it was sent
-- (./telegram-content-repository.ts insertMessage), whichever alasio keeps it.
create unique index if not exists messages_telegram_once on ${SCHEMA}.messages (conversation_id, direction, transport_message_id, (raw->>'date'))
  nulls not distinct where transport_message_id is not null;

-- Files messages carried, whole: the Bot API lets bots download 20 MB at most.
create table if not exists ${SCHEMA}.files (
  id text primary key,
  message_id text references ${SCHEMA}.messages (id) on delete set null,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  telegram_file_id text not null,
  telegram_file_unique_id text,
  file_name text,
  -- the name it is written under for the agent to read
  name text not null,
  mime_type text,
  file_size integer not null,
  sha256 text not null,
  content bytea not null,
  raw json not null,
  created_at timestamptz not null default now()
);

create index if not exists files_message on ${SCHEMA}.files (message_id);
-- A file a message carried kept once, whichever alasio keeps it.
create unique index if not exists files_telegram_once on ${SCHEMA}.files (message_id, telegram_file_unique_id) where telegram_file_unique_id is not null;

create table if not exists ${SCHEMA}.media_groups (
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  status text not null default 'pending',
  first_update_id bigint not null,
  created_at timestamptz not null default now(),
  flushed_at timestamptz
);

-- Each conversation's latest turn, its id the conversation's.
create table if not exists ${SCHEMA}.turns (
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  thread_key text not null,
  channel text not null,
  thread_ts text not null,
  session_id text,
  harness text not null,
  pending_response_id text,
  prompt text,
  state text not null,
  -- how many turns have claimed the conversation: each turn's writes, and its harness's,
  -- hold only while no turn has claimed it since
  epoch bigint not null default 0,
  -- what the turn's harness says it does: working, waiting (on waiting_on), interrupted, ended
  status text not null default 'working',
  waiting_on text,
  -- how it ended: completed, incomplete, interrupted, stopped, failed
  outcome text,
  -- its status message's outbox entry, which edits name, so a start after a crash can say how it ended
  status_outbox_id text,
  -- the binding of the conversation's harness it runs on, as alasio drove it there, or as the turn
  -- before ran on, of one its harness started on its own: the one turn the binding's loss takes
  -- with it (Store.stageLostRecovery)
  binding text,
  started_at timestamptz not null,
  completed_at timestamptz
);

-- What holds a conversation busy that is no turn: a fork of the workspace it mounts, which
-- suspends it; a row while it lasts, by its holder, who renews it while its fork runs, until
-- expires_at: a holder whose process stopped holds it no longer than that, and a fork stuck
-- in an await is stopped at its deadline (../harness/holds.ts).
create table if not exists ${SCHEMA}.conversation_holds (
  conversation_id text primary key references ${SCHEMA}.conversations (id) on delete cascade,
  reason text not null,
  holder text not null,
  held_at timestamptz not null default now(),
  expires_at timestamptz not null
);

-- The worker driving a conversation's prompts and turns, of one of the alasios on the store: its
-- holder, until held_until, which it renews while it runs, so another takes the conversation over
-- once it lapses; and its generation, one more at each take, which every write a worker makes
-- holds under (Store's fenced writes), so one that stalled past a takeover writes nothing after it.
create table if not exists ${SCHEMA}.conversation_workers (
  conversation_id text primary key references ${SCHEMA}.conversations (id) on delete cascade,
  holder text not null,
  generation bigint not null,
  held_until timestamptz not null
);

create table if not exists ${SCHEMA}.responses (
  id text primary key,
  chat_id text not null,
  message_id text not null,
  -- the conversation whose turn writes it, and that turn's epoch: written to only while
  -- no turn has claimed the conversation since
  thread_key text,
  epoch bigint,
  session_id text,
  completed boolean not null default false,
  posted boolean not null default false,
  created_at timestamptz not null default now()
);

create index if not exists responses_undelivered on ${SCHEMA}.responses (id) where completed and not posted;

create table if not exists ${SCHEMA}.response_blocks (
  seq bigint generated always as identity primary key,
  response_id text not null references ${SCHEMA}.responses (id) on delete cascade,
  block json not null
);

create index if not exists response_blocks_response on ${SCHEMA}.response_blocks (response_id, seq);

-- Codex's login (its auth.json), where its home does not outlast alasio's pod; one row.
create table if not exists ${SCHEMA}.codex_login (
  one boolean primary key default true check (one),
  auth text not null,
  updated_at timestamptz not null default now()
);

-- The refusal of the login kept at saved_at (codex_login's updated_at) as it was refreshed, its
-- refresh token refused for good, so no alasio refreshes it again (../codex/login-refresh.ts);
-- one of a login kept since says nothing of it.
create table if not exists ${SCHEMA}.codex_login_refusal (
  saved_at timestamptz primary key,
  refused_at timestamptz not null default now()
);

create table if not exists ${SCHEMA}.session_usage (
  session_id text primary key,
  cache_read_input_tokens integer not null,
  updated_at timestamptz not null default now()
);

create table if not exists ${SCHEMA}.callback_actions (
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  kind text not null,
  payload json not null,
  -- what the conversation had mounted when the button was made
  expected_session_id text,
  expected_harness text,
  created_at timestamptz not null default now()
);

-- Each control whose work runs on past its update (src/telegram/controls.ts), a press of
-- alasio's buttons or a command the operator sent, by its query's id or its message, kept as
-- its update is told handled until it is done: what it is (control), held by the alasio
-- running it, by its token, until held_until, which that alasio renews as it runs, so one
-- left by an alasio that stopped is run again by another; and its answer, where a press was
-- done but the answer could not be queued, which the next look queues alone.
create table if not exists ${SCHEMA}.control_jobs (
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  control jsonb not null,
  holder text not null,
  held_until timestamptz not null,
  answer text,
  created_at timestamptz not null default now()
);

-- What a control made, a session, a fork, a folder or a goal, or that it asked for a goal's
-- turn, by a key its message names
-- (src/operator/made-once.ts): so a control run again, as a kept one is after its alasio
-- stopped, finds what its run before made rather than make another.
create table if not exists ${SCHEMA}.control_results (
  key text primary key,
  result jsonb not null,
  created_at timestamptz not null default now()
);

-- What alasio sends to Telegram, in each chat's order: a reply ('text'), a turn's status
-- message ('status'), whose Telegram message id is recorded once it is sent, and an edit of
-- one ('edit', of the entry its edits column names).
create table if not exists ${SCHEMA}.telegram_outbox (
  seq bigint generated always as identity,
  id text primary key,
  conversation_id text references ${SCHEMA}.conversations (id) on delete cascade,
  chat_id text not null,
  kind text not null,
  -- of an edit, the entry it edits; no reference, as a sent entry is pruned in time
  edits text,
  -- of a status message, its message's id once Telegram accepted it
  telegram_message_id bigint,
  text text not null,
  options json not null,
  pending_response_id text,
  state text not null default 'pending',
  attempts integer not null default 0,
  available_at timestamptz not null default now(),
  last_error text,
  -- the W3C traceparent of the turn whose reply this is, so its delivery joins that trace
  traceparent text,
  created_at timestamptz not null default now(),
  sent_at timestamptz
);

create index if not exists telegram_outbox_due on ${SCHEMA}.telegram_outbox (state, chat_id, seq);
create unique index if not exists telegram_outbox_pending_response
  on ${SCHEMA}.telegram_outbox (pending_response_id) where pending_response_id is not null;

-- The claim of the delivery pass sending an entry, until it marks the entry or the claim lapses:
-- no other pass sends the entry while the claim holds, whatever becomes of the connection it was
-- made on. Its token is the pass's own, which its marks name.
create table if not exists ${SCHEMA}.outbox_claims (
  outbox_id text primary key references ${SCHEMA}.telegram_outbox (id) on delete cascade,
  token text not null,
  until timestamptz not null
);

-- The media a reply shows, kept with it until it is sent.
create table if not exists ${SCHEMA}.outbox_media (
  outbox_id text not null references ${SCHEMA}.telegram_outbox (id) on delete cascade,
  position integer not null,
  id text not null,
  kind text not null,
  animation boolean not null,
  file_name text not null,
  content bytea not null,
  primary key (outbox_id, position)
);

-- What a conversation was told of its turns besides their replies (TurnResult.notice), as
-- each was queued with it, in one transaction: told once, whichever alasio told it, and
-- whether or not it has restarted since.
create table if not exists ${SCHEMA}.conversation_notices (
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  notice text not null,
  told_at timestamptz not null default now(),
  primary key (conversation_id, notice)
);

-- Each event of the environment's events topic (src/events/) an alasio handled, by its id, recorded
-- in the transaction of what its handling wrote: handled once, whichever alasio it was delivered to
-- and however often (src/events/consume.ts). Kept as long as the topic keeps its records.
create table if not exists ${SCHEMA}.events_handled (
  id text primary key,
  kind text not null,
  handled_at timestamptz not null default now()
);

-- Each request a branch's alasio made of this one, its parent, to fork a session for it
-- (src/branch/fork.ts), by the branch and the id it gave it, with the session it asked for and
-- how it was answered: one delivered again is answered as it was, not forked twice.
create table if not exists ${SCHEMA}.fork_requests (
  branch text not null,
  id text not null,
  volume_id text not null,
  outcome text not null,
  net_mode text,
  reason text,
  answered_at timestamptz not null default now(),
  primary key (branch, id)
);

-- How this alasio's parent answered each request it made of it to fork a session, by the
-- request's id, as whichever of its replicas read the answer recorded it.
create table if not exists ${SCHEMA}.fork_answers (
  request text primary key,
  volume_id text not null,
  outcome text not null,
  net_mode text,
  reason text,
  answered_at timestamptz not null default now()
);

create table if not exists ${SCHEMA}.prompt_jobs (
  seq bigint generated always as identity,
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  chat_id text not null,
  message_id text not null,
  prompt text not null,
  -- what the operator wrote, which the prompt is made from (with notes of the files sent
  -- with it), as a question about the prompt quotes it
  visible_text text not null,
  -- the files the prompt was sent with, as the files table keeps them
  file_ids text[] not null default '{}',
  harness text not null,
  state text not null,
  priority integer not null default 0,
  attempts integer not null default 0,
  upstream_session_id text,
  upstream_turn_id text,
  -- when the prompt was sent to the agent, which may have acted on it since
  upstream_dispatched_at timestamptz,
  upstream_started_at timestamptz,
  upstream_completed_at timestamptz,
  last_error text,
  -- the W3C traceparent of the update that queued the prompt, so its turn joins that
  -- trace however long it waits, restarts included
  traceparent text,
  created_at timestamptz not null default now(),
  started_at timestamptz,
  completed_at timestamptz,
  unique (conversation_id, message_id)
);

create index if not exists prompt_jobs_ready on ${SCHEMA}.prompt_jobs (conversation_id, state, priority desc, seq);

-- The grants of each conversation's harness pool pods (src/harness/grant.ts), by their digests
-- alone: one of each binding of the conversation's harness, kept until that binding is let go of,
-- so a pod still bound as the conversation's harness is bound anew in another pool, as a switch
-- of its harness or its workspace binds it, reaches the conversation's rows as it drains. None of
-- the harness host's role's to read: it reaches its conversation through harness_conversation()
-- alone.
create table if not exists ${SCHEMA}.harness_grants (
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  binding text not null,
  secret_sha256 bytea not null unique,
  issued_at timestamptz not null default now(),
  primary key (conversation_id, binding)
);

-- The harness sessions each conversation has: as alasio's operator gives it one (give_session),
-- one another conversation has among them, as one resumed from a folder's panel, with the Codex
-- threads its history starts in; or as one of its rows first names one no conversation has, and
-- the stores keep nothing of (session_claimed), whoever wrote the row, alasio or the harness
-- host's role (harness_claim). What the role reaches of the transcripts' and rollouts' stores,
-- and of the sessions' usage, is its conversation's sessions' (src/persistence/harness-grants.ts).
-- None of the role's to read.
create table if not exists ${SCHEMA}.conversation_sessions (
  session_id text not null,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  claimed_at timestamptz not null default now(),
  primary key (session_id, conversation_id)
);

create index if not exists conversation_sessions_conversation on ${SCHEMA}.conversation_sessions (conversation_id);

-- The conversation whose grant the connection's setting is (alasio.harness_grant), or null where
-- it is none: a grant no binding holds, or no setting at all.
create or replace function ${SCHEMA}.harness_conversation() returns text
language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select conversation_id from ${SCHEMA}.harness_grants
  where secret_sha256 = sha256(convert_to(nullif(current_setting('alasio.harness_grant', true), ''), 'UTF8'))
$$;

-- The sessions of the connection's grant's conversation: read anew at each call, so a statement
-- that claimed one (harness_claim) sees it after.
create or replace function ${SCHEMA}.harness_sessions() returns text[]
language sql volatile security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce(array_agg(session_id), '{}') from ${SCHEMA}.conversation_sessions
  where conversation_id = ${SCHEMA}.harness_conversation()
$$;

-- Waits, until the transaction ends, for any other that claims the session given or names it in
-- a row, so of two at once one sees what the other did.
create or replace function ${SCHEMA}.session_lock(session text) returns void
language sql volatile security definer set search_path = pg_catalog, pg_temp as $$
  select pg_advisory_xact_lock(hashtextextended('${SCHEMA}.conversation_sessions:' || session, 0))
$$;

-- Whether the transcripts' or rollouts' stores keep anything of the session given: rows of it, as
-- of a session some harness wrote, which a pod writing of it first did not make.
create or replace function ${SCHEMA}.stored_session(session text) returns boolean
language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  found boolean;
begin
  if to_regclass('claude_sessions.entries') is not null then
    execute 'select exists (select from claude_sessions.entries where session_id = $1)' into found using session;
    if found then return true; end if;
  end if;
  if to_regclass('codex_sessions.rollouts') is not null then
    execute 'select exists (select from codex_sessions.rollouts where thread_id = $1)' into found using session;
    if found then return true; end if;
  end if;
  if to_regclass('codex_sessionfs_sessions.rollouts') is not null then
    execute 'select exists (select from codex_sessionfs_sessions.rollouts where thread_id = $1)' into found using session;
    if found then return true; end if;
  end if;
  return false;
end
$$;

-- Gives the conversation the Codex threads the session given's history starts in, transitively,
-- as its rollouts kept in either Codex store say them, as a fork's starts in its parent's: so a
-- thread alasio's operator gives it, as one resumed from a folder's panel, reaches its history,
-- which its pod writes back from the store as it resumes it. Of a thread alasio gives alone
-- (give_session), and the cut-over's: a rollout's history_base is its writer's word, which a pod
-- may write of its own thread, naming another's.
create or replace function ${SCHEMA}.claim_lineage(session text, conversation text) returns void
language plpgsql volatile security definer set search_path = pg_catalog, pg_temp as $$
declare
  store text;
begin
  foreach store in array array['codex_sessions', 'codex_sessionfs_sessions'] loop
    if to_regclass(store || '.rollouts') is not null then
      execute format($lineage$
        with recursive lineage (thread_id) as (
          select parent.thread_id from %1$I.rollouts child join %1$I.rollouts parent on parent.rollout_id = child.history_base
          where child.thread_id = $1
          union
          select parent.thread_id from lineage
          join %1$I.rollouts child on child.thread_id = lineage.thread_id
          join %1$I.rollouts parent on parent.rollout_id = child.history_base
        )
        insert into ${SCHEMA}.conversation_sessions (session_id, conversation_id) select thread_id, $2 from lineage on conflict do nothing
      $lineage$, store) using session, conversation;
    end if;
  end loop;
end
$$;

-- Whether the conversation given may name the session given: one it has, yes; one another
-- conversation has, or the stores keep something of already, as of a session no row of alasio's
-- names, never, but as alasio's operator gives it (give_session), so neither a pod that knows
-- another's session's id nor one alasio is told of by a pod makes it the conversation's; one no
-- conversation has, claimed for it as a row first names it. No Codex thread its history starts in
-- is claimed with it, as its rollout's history_base is its writer's word (claim_lineage).
create or replace function ${SCHEMA}.session_claimed(session text, conversation text) returns boolean
language plpgsql volatile security definer set search_path = pg_catalog, pg_temp as $$
begin
  -- One the conversation has stays its, so it is its without waiting on any claim of it: a write
  -- that waits for the lock as it holds its turn's row (a response's, fenced on it) would wait on
  -- one of the turn's row itself, whose check holds the lock as it waits for that row.
  if exists (select from ${SCHEMA}.conversation_sessions where session_id = session and conversation_id = conversation) then
    return true;
  end if;
  perform ${SCHEMA}.session_lock(session);
  if exists (select from ${SCHEMA}.conversation_sessions where session_id = session and conversation_id = conversation) then
    return true;
  end if;
  if exists (select from ${SCHEMA}.conversation_sessions where session_id = session) or ${SCHEMA}.stored_session(session) then
    return false;
  end if;
  insert into ${SCHEMA}.conversation_sessions (session_id, conversation_id) values (session, conversation);
  return true;
end
$$;

-- Whether the connection's grant's conversation may write of the session given, as
-- session_claimed says; no session at all is anyone's to write of.
create or replace function ${SCHEMA}.harness_claim(session text) returns boolean
language plpgsql volatile security definer set search_path = pg_catalog, pg_temp as $$
declare
  mine text := ${SCHEMA}.harness_conversation();
begin
  if session is null then
    return true;
  end if;
  if mine is null then
    return false;
  end if;
  return ${SCHEMA}.session_claimed(session, mine);
end
$$;

-- Gives the conversation given the session given, whoever has it, with the Codex threads its
-- history starts in (claim_lineage): as alasio's operator chooses it, a session of a folder's
-- panel resumed, the one way a conversation comes to have a session another has, or one the
-- stores kept before any row named it. alasio's alone to call.
create or replace function ${SCHEMA}.give_session(session text, conversation text) returns void
language plpgsql volatile security definer set search_path = pg_catalog, pg_temp as $$
begin
  perform ${SCHEMA}.session_lock(session);
  insert into ${SCHEMA}.conversation_sessions (session_id, conversation_id) values (session, conversation) on conflict do nothing;
  perform ${SCHEMA}.claim_lineage(session, conversation);
end
$$;

-- Holds each row naming a session (the columns the trigger's arguments after the first name) to
-- its conversation's sessions (the column its first names), whoever wrote it, as session_claimed
-- says: a session a row first names is the conversation's from then on; one it may not name, the
-- write is refused. A row of the harness host's role's reaches here once its policy let it name
-- the session (harness_claim); one of alasio's names a session alasio's operator gave the
-- conversation (give_session), or what alasio was told of by a pod, as of a turn's session, held
-- to it as the pod's own writes are.
create or replace function ${SCHEMA}.claim_sessions() returns trigger
language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  fields jsonb := to_jsonb(new);
  conversation text := fields ->> tg_argv[0];
  session text;
begin
  if conversation is null or not exists (select from ${SCHEMA}.conversations where id = conversation) then
    return null;
  end if;
  for i in 1 .. tg_nargs - 1 loop
    session := fields ->> tg_argv[i];
    if session is not null and not ${SCHEMA}.session_claimed(session, conversation) then
      raise exception using
        errcode = 'insufficient_privilege',
        message = format('the session %s is not the conversation %s''s: another conversation has it, or a harness kept it before any row named it, and only alasio''s operator gives such a session', session, conversation);
    end if;
  end loop;
  return null;
end
$$;

revoke all on function ${SCHEMA}.harness_conversation(), ${SCHEMA}.harness_sessions(), ${SCHEMA}.session_lock(text), ${SCHEMA}.stored_session(text), ${SCHEMA}.claim_lineage(text, text), ${SCHEMA}.session_claimed(text, text), ${SCHEMA}.harness_claim(text), ${SCHEMA}.give_session(text, text), ${SCHEMA}.claim_sessions() from public;
${SESSION_ROWS.map((rows) => claimTrigger(SCHEMA, rows)).join("\n")}
`;

/** Makes alasio's tables in `schema` where they are missing. */
export const ensureSchema = (sql: Sql, schema: string): Effect.Effect<void, StoreError> => Effect.asVoid(sql.query(ddl(schema)));
