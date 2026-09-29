const SCHEMA_VERSION = "7";

const SQLITE_SCHEMA_SQL = `
  create table if not exists bot_state (
    key text primary key,
    value text not null,
    updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  );

  create table if not exists conversations (
    id text primary key,
    transport text not null,
    chat_id text not null,
    user_id text,
    username text,
    first_name text,
    last_name text,
    codex_session_id text,
    claude_session_id text,
    active_harness text,
    working_directory text,
    claude_model text,
    claude_effort text,
    codex_model text,
    codex_effort text,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    unique (transport, chat_id)
  );

  create table if not exists workspace_sessions (
    conversation_id text not null references conversations(id) on delete cascade,
    harness text not null,
    working_directory text not null,
    session_id text,
    updated_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    primary key (conversation_id, harness, working_directory)
  );

  -- Session filesystems: one JuiceFS volume per session, with its metadata namespace
  -- (a Valkey DB index, unique) and whether it has been formatted. The workspace a
  -- conversation points at stays in conversations.working_directory as the sentinel
  -- 'sessionfs:<id>' (src/workspace/kind.js); this table holds only mount/destroy state.
  create table if not exists session_volumes (
    id text primary key,
    db_index integer not null unique,
    formatted integer not null default 0,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  );

  create table if not exists telegram_updates (
    update_id integer primary key,
    payload_json text not null,
    received_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    processed_at text
  );

  create table if not exists messages (
    id text primary key,
    conversation_id text not null references conversations(id) on delete cascade,
    direction text not null,
    kind text not null,
    transport_message_id text,
    text text,
    media_group_id text,
    raw_json text,
    codex_session_id text,
    turn_id text,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  );

  create table if not exists files (
    id text primary key,
    message_id text references messages(id) on delete set null,
    conversation_id text not null references conversations(id) on delete cascade,
    telegram_file_id text not null,
    telegram_file_unique_id text,
    file_name text,
    mime_type text,
    file_size integer,
    local_path text,
    sha256 text,
    raw_json text,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    downloaded_at text
  );

  create table if not exists media_groups (
    id text primary key,
    conversation_id text not null references conversations(id) on delete cascade,
    status text not null default 'pending',
    first_update_id integer,
    flush_after_ms integer not null,
    created_at_ms integer not null,
    flushed_at text
  );

  create table if not exists turns (
    id text primary key,
    conversation_id text not null references conversations(id) on delete cascade,
    thread_key text not null,
    channel text not null,
    thread_ts text not null,
    session_id text,
    harness text not null default 'codex',
    pending_response_id text,
    prompt text,
    state text not null,
    started_at real not null,
    completed_at real
  );

  create table if not exists response_blocks (
    id text primary key,
    pending_response_id text not null,
    conversation_id text not null references conversations(id) on delete cascade,
    channel text not null,
    thread_ts text not null,
    session_id text,
    sequence integer not null,
    block_json text not null,
    posted integer not null default 0,
    created_at real not null
  );

  create table if not exists restart_events (
    thread_key text primary key,
    payload_json text not null,
    created_at real not null
  );

  create table if not exists session_usage (
    session_id text primary key,
    cache_read_input_tokens integer not null,
    updated_at text not null
  );

  create table if not exists callback_actions (
    id text primary key,
    conversation_id text not null references conversations(id) on delete cascade,
    kind text not null,
    payload_json text not null,
    consumed_at text,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  );

  create table if not exists telegram_outbox (
    id text primary key,
    conversation_id text references conversations(id) on delete cascade,
    chat_id text not null,
    kind text not null,
    text text not null,
    options_json text not null default '{}',
    pending_response_id text,
    state text not null default 'pending',
    attempts integer not null default 0,
    available_at real not null,
    last_error text,
    created_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    sent_at text
  );

  create table if not exists prompt_jobs (
    id text primary key,
    conversation_id text not null references conversations(id) on delete cascade,
    chat_id text not null,
    message_id text not null,
    prompt text not null,
    file_paths_json text not null default '[]',
    harness text not null default 'codex',
    state text not null,
    priority integer not null default 0,
    attempts integer not null default 0,
    upstream_session_id text,
    upstream_turn_id text,
    upstream_started_at real,
    upstream_completed_at real,
    last_error text,
    created_at real not null,
    started_at real,
    completed_at real,
    unique (conversation_id, message_id)
  );

  create index if not exists idx_prompt_jobs_ready
    on prompt_jobs (conversation_id, state, priority desc, created_at asc);

  create index if not exists idx_telegram_outbox_due
    on telegram_outbox (state, available_at, created_at);

  create index if not exists idx_messages_conversation_created_at
    on messages (conversation_id, created_at);

  create index if not exists idx_messages_media_group
    on messages (media_group_id);

  create index if not exists idx_files_message
    on files (message_id);

  create index if not exists idx_turns_state
    on turns (state, started_at);
`;

export function migrateSqliteSchema(db, { legacyWorkingDirectory = null } = {}) {
    db.exec(SQLITE_SCHEMA_SQL);
    const promptJobColumns = new Set(db.prepare("pragma table_info(prompt_jobs)").all().map((column) => column.name));
    if (!promptJobColumns.has("upstream_completed_at")) {
      db.exec("alter table prompt_jobs add column upstream_completed_at real");
    }
    if (!promptJobColumns.has("harness")) {
      db.exec("alter table prompt_jobs add column harness text not null default 'codex'");
    }
    const conversationColumnInfo = db.prepare("pragma table_info(conversations)").all();
    const conversationColumns = new Set(conversationColumnInfo.map((column) => column.name));
    if (!conversationColumns.has("claude_session_id")) {
      db.exec("alter table conversations add column claude_session_id text");
    }
    if (!conversationColumns.has("active_harness")) {
      // Pre-harness rows only ever ran Codex; keep them mounted there so an upgrade
      // does not strand existing conversations behind the service picker.
      db.exec("alter table conversations add column active_harness text");
      db.exec("update conversations set active_harness = 'codex' where active_harness is null");
    } else if (conversationColumnInfo.find((column) => column.name === "active_harness")?.notnull) {
      // Schema v5 declared active_harness not null default 'codex'. v6 makes "nothing
      // mounted" a real state, so the constraint has to go; SQLite can only do that by
      // swapping the column. Existing rows keep whichever harness they had.
      db.exec(`
        begin;
        alter table conversations rename column active_harness to active_harness_v5;
        alter table conversations add column active_harness text;
        update conversations set active_harness = active_harness_v5;
        alter table conversations drop column active_harness_v5;
        commit;
      `);
    }
    if (!conversationColumns.has("working_directory")) {
      db.exec("alter table conversations add column working_directory text");
    }
    // A model and effort chosen with /model, per harness: each service has its
    // own catalogue, so a choice made for one never applies to the other. Null
    // means the harness's pinned default.
    for (const column of ["claude_model", "claude_effort", "codex_model", "codex_effort"]) {
      if (!conversationColumns.has(column)) {
        db.exec(`alter table conversations add column ${column} text`);
      }
    }
    if (legacyWorkingDirectory) {
      // Conversations mounted before folders were per-conversation ran in the
      // deployment's WORKING_DIRECTORY; keep them there instead of stranding them
      // behind the folder picker.
      db.prepare(`
        update conversations
        set working_directory = ?
        where working_directory is null and active_harness is not null
      `).run(legacyWorkingDirectory);
    }
    const turnColumns = new Set(db.prepare("pragma table_info(turns)").all().map((column) => column.name));
    if (!turnColumns.has("harness")) {
      db.exec("alter table turns add column harness text not null default 'codex'");
    }
    db.exec(`
      delete from telegram_outbox
      where pending_response_id is not null
        and id not in (
          select id
          from (
            select
              id,
              row_number() over (
                partition by pending_response_id
                order by
                  case
                    when state = 'sent' then 0
                    when state = 'pending' then 1
                    else 2
                  end,
                  created_at desc,
                  id desc
              ) as delivery_rank
            from telegram_outbox
            where pending_response_id is not null
          )
          where delivery_rank = 1
        );

      create unique index if not exists idx_telegram_outbox_pending_response
        on telegram_outbox (pending_response_id)
        where pending_response_id is not null;
    `);
    db.prepare(`
      insert into bot_state (key, value, updated_at)
      values ('schema_version', ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at
    `).run(SCHEMA_VERSION);
}
