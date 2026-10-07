/**
 * alasio's state in Postgres: one schema, made (idempotently) as the store opens.
 *
 * JSON is kept as `json`, its exact text: what Telegram and the harnesses send may hold a
 * NUL, which `jsonb` refuses. Times are `timestamptz`, and the order things were queued
 * in is an identity column's.
 */
import { Effect } from "effect";

import type { Sql, StoreError } from "./sql.ts";

const ddl = (SCHEMA: string): string => `
create schema if not exists ${SCHEMA};

-- What alasio keeps by name: Telegram's update offset, the operator it bootstrapped.
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
  started_at timestamptz not null,
  completed_at timestamptz
);

create table if not exists ${SCHEMA}.responses (
  id text primary key,
  chat_id text not null,
  message_id text not null,
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

create table if not exists ${SCHEMA}.restart_events (
  thread_key text primary key,
  cause text not null,
  channel text not null,
  thread_ts text not null,
  session_id text,
  -- the command that restarted alasio, for a restart the agent caused
  command text,
  recorded_at timestamptz not null default now()
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

create table if not exists ${SCHEMA}.telegram_outbox (
  seq bigint generated always as identity,
  id text primary key,
  conversation_id text references ${SCHEMA}.conversations (id) on delete cascade,
  chat_id text not null,
  kind text not null,
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

create table if not exists ${SCHEMA}.prompt_jobs (
  seq bigint generated always as identity,
  id text primary key,
  conversation_id text not null references ${SCHEMA}.conversations (id) on delete cascade,
  chat_id text not null,
  message_id text not null,
  prompt text not null,
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
`;

/** Makes alasio's tables in `schema` where they are missing. */
export const ensureSchema = (sql: Sql, schema: string): Effect.Effect<void, StoreError> => Effect.asVoid(sql.query(ddl(schema)));
