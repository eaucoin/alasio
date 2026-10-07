import { Effect } from "effect";

import { CLAUDE_HARNESS, CODEX_HARNESS, getDefaultHarness, type HarnessName, isHarnessName } from "../harness/names.ts";
import type { Sql, StoreError } from "./sql.ts";

type SessionColumn = "codex_session_id" | "claude_session_id";

interface ModelColumns {
  readonly model: "claude_model" | "codex_model";
  readonly effort: "claude_effort" | "codex_effort";
}

const SESSION_COLUMNS: Readonly<Record<HarnessName, SessionColumn>> = {
  [CODEX_HARNESS]: "codex_session_id",
  [CLAUDE_HARNESS]: "claude_session_id",
};

const MODEL_COLUMNS: Readonly<Record<HarnessName, ModelColumns>> = {
  [CLAUDE_HARNESS]: { model: "claude_model", effort: "claude_effort" },
  [CODEX_HARNESS]: { model: "codex_model", effort: "codex_effort" },
};

/**
 * A row of `conversations`: one Telegram chat, with the harness, folder, sessions,
 * and models it has mounted.
 */
export interface Conversation {
  readonly id: string;
  readonly transport: string;
  readonly chat_id: string;
  readonly user_id: string | null;
  readonly username: string | null;
  readonly first_name: string | null;
  readonly last_name: string | null;
  readonly codex_session_id: string | null;
  readonly claude_session_id: string | null;
  readonly active_harness: HarnessName | null;
  readonly working_directory: string | null;
  readonly claude_model: string | null;
  readonly claude_effort: string | null;
  readonly codex_model: string | null;
  readonly codex_effort: string | null;
  readonly created_at: Date;
  readonly updated_at: Date;
}

/** The Telegram user a conversation is with, as far as the conversation keeps them. */
export interface ConversationUser {
  readonly id: number | string;
  readonly username?: string | undefined;
  readonly first_name?: string | undefined;
  readonly last_name?: string | undefined;
}

export interface NewConversation {
  readonly chatId: number | string;
  readonly user?: ConversationUser | undefined;
}

/**
 * What a conversation has mounted: the service its turns run on and the folder they run
 * in, null until chosen, and the service's session there.
 */
export interface Mount {
  readonly harness: HarnessName | null;
  readonly workingDirectory: string | null;
  readonly sessionId: string | null;
}

/** A conversation whose mounted harness has a session, in the folder it runs in. */
export interface LinkedConversation {
  readonly id: string;
  readonly session_id: string;
  readonly active_harness: HarnessName;
  readonly working_directory: string;
}

/** A harness session alasio points at, and the folder it runs in. */
export interface HarnessSessionReference {
  readonly sessionId: string;
  readonly workingDirectory: string;
}

/** A conversation that knows a workspace: mounting it now, or keeping a session of it parked. */
export interface WorkspaceConversation {
  readonly conversationId: string;
  readonly mounted: boolean;
}

/** A model, and optionally a reasoning effort, chosen for a harness with /model. */
export interface ModelChoice {
  readonly model: string;
  readonly effort: string | null;
}

/** A model to choose with /model; without an effort, the model's default applies. */
export interface NewModelChoice {
  readonly model: string;
  readonly effort?: string | null | undefined;
}

/** The session a conversation has of `harness`, mounted or not; none for a conversation not yet made. */
export function harnessSessionOf(conversation: Conversation | null, harness: HarnessName): string | null {
  return conversation?.[SESSION_COLUMNS[harness]] ?? null;
}

/** The mount a conversation row holds; nothing mounted for a conversation not yet made. */
export function mountOf(conversation: Conversation | null): Mount {
  const harness = isHarnessName(conversation?.active_harness) ? conversation.active_harness : null;
  return {
    harness,
    workingDirectory: conversation?.working_directory || null,
    sessionId: harness ? harnessSessionOf(conversation, harness) : null,
  };
}

/** SQL, in a statement on a conversation's row: the session of the harness the expression `harness` names. */
export const sessionOf = (harness: string): string =>
  `case ${harness} ${Object.entries(SESSION_COLUMNS).map(([name, column]) => `when '${name}' then ${column}`).join(" ")} end`;

/**
 * SQL, in an update of a conversation's row: the assignments that set the session of the
 * harness the expression `harness` names to the expression `value`, and leave the others.
 */
export const assignSession = (harness: string, value: string): string =>
  Object.entries(SESSION_COLUMNS).map(([name, column]) => `${column} = case when ${harness} = '${name}' then ${value} else ${column} end`).join(", ");

export class NeonConversationRepository {
  readonly #sql: Sql;
  readonly #schema: string;
  readonly #defaultWorkingDirectory: string | null;

  /** `defaultWorkingDirectory`: the folder a new conversation is mounted on, if any. */
  constructor(sql: Sql, schema: string, defaultWorkingDirectory: string | null) {
    this.#sql = sql;
    this.#schema = schema;
    this.#defaultWorkingDirectory = defaultWorkingDirectory;
  }

  upsertConversation({ chatId, user }: NewConversation): Effect.Effect<string, StoreError> {
    const id = `telegram:${chatId}`;
    return this.#sql.query(
      `insert into ${this.#schema}.conversations (id, transport, chat_id, user_id, username, first_name, last_name, active_harness, working_directory)
       values ($1, 'telegram', $2, $3, $4, $5, $6, $7, $8)
       on conflict (id) do update set
         user_id = coalesce(excluded.user_id, conversations.user_id),
         username = coalesce(excluded.username, conversations.username),
         first_name = coalesce(excluded.first_name, conversations.first_name),
         last_name = coalesce(excluded.last_name, conversations.last_name),
         updated_at = now()`,
      [
        id,
        String(chatId),
        user?.id != null ? String(user.id) : null,
        user?.username ?? null,
        user?.first_name ?? null,
        user?.last_name ?? null,
        getDefaultHarness(),
        this.#defaultWorkingDirectory,
      ],
    ).pipe(Effect.as(id));
  }

  getConversationByChatId(chatId: number | string): Effect.Effect<Conversation | null, StoreError> {
    return this.#sql.query<Conversation>(`select * from ${this.#schema}.conversations where transport = 'telegram' and chat_id = $1`, [String(chatId)]).pipe(
      Effect.map(([row]) => row ?? null),
    );
  }

  getConversation(conversationId: string): Effect.Effect<Conversation | null, StoreError> {
    return this.#sql.query<Conversation>(`select * from ${this.#schema}.conversations where id = $1`, [conversationId]).pipe(
      Effect.map(([row]) => row ?? null),
    );
  }

  getMount(conversationId: string): Effect.Effect<Mount, StoreError> {
    return Effect.map(this.getConversation(conversationId), mountOf);
  }

  listConversationsWithSessions(harness: HarnessName): Effect.Effect<LinkedConversation[], StoreError> {
    const column = SESSION_COLUMNS[harness];
    return this.#sql.query<LinkedConversation>(
      `select id, ${column} as session_id, active_harness, working_directory
       from ${this.#schema}.conversations
       where ${column} <> '' and active_harness = $1 and working_directory is not null
       order by updated_at desc`,
      [harness],
    );
  }

  /**
   * Every session of a harness alasio points at, with the folder it runs in:
   * each conversation's mounted one, the ones parked per folder, and those of
   * active turns. Distinct by session.
   */
  /** The conversations that know the workspace `workingDirectory`, each once. */
  listWorkspaceConversations(workingDirectory: string): Effect.Effect<WorkspaceConversation[], StoreError> {
    return this.#sql.query<WorkspaceConversation>(
      `select id as "conversationId", bool_or(mounted) as mounted from (
         select id, true as mounted from ${this.#schema}.conversations where working_directory = $1
         union all
         select conversation_id, false from ${this.#schema}.workspace_sessions where working_directory = $1
       ) as knowing group by id`,
      [workingDirectory],
    );
  }

  listHarnessSessionReferences(harness: HarnessName): Effect.Effect<HarnessSessionReference[], StoreError> {
    const column = SESSION_COLUMNS[harness];
    return this.#sql.query<HarnessSessionReference>(
      `select session_id as "sessionId", min(working_directory) as "workingDirectory" from (
         select ${column} as session_id, working_directory from ${this.#schema}.conversations
         where ${column} <> '' and working_directory is not null
         union
         select session_id, working_directory from ${this.#schema}.workspace_sessions
         where harness = $1 and session_id <> ''
         union
         select turns.session_id, conversations.working_directory from ${this.#schema}.turns
         join ${this.#schema}.conversations on conversations.id = turns.conversation_id
         where turns.harness = $1 and turns.session_id <> '' and conversations.working_directory is not null
       ) as referenced group by session_id`,
      [harness],
    );
  }

  /**
   * The model and effort chosen for one harness in this conversation, or null
   * when none has been chosen and the harness's pinned default applies.
   */
  getModelChoice(conversationId: string, harness: HarnessName): Effect.Effect<ModelChoice | null, StoreError> {
    const { model, effort } = MODEL_COLUMNS[harness];
    return this.#sql.query<ModelChoice>(
      `select ${model} as model, ${effort} as effort from ${this.#schema}.conversations where id = $1 and ${model} is not null`,
      [conversationId],
    ).pipe(Effect.map(([row]) => row ?? null));
  }

  setModelChoice(conversationId: string, harness: HarnessName, { model, effort = null }: NewModelChoice): Effect.Effect<void, StoreError> {
    const columns = MODEL_COLUMNS[harness];
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversations set ${columns.model} = $2, ${columns.effort} = $3, updated_at = now() where id = $1`,
      [conversationId, model, effort],
    ));
  }

  clearModelChoice(conversationId: string, harness: HarnessName): Effect.Effect<void, StoreError> {
    const columns = MODEL_COLUMNS[harness];
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversations set ${columns.model} = null, ${columns.effort} = null, updated_at = now() where id = $1`,
      [conversationId],
    ));
  }

  /**
   * Mounts a folder. Sessions belong to one harness and one folder, so the
   * current session pointers are parked under the outgoing folder and whatever
   * was parked for the incoming folder is restored. The statement reads the
   * parked sessions as they were before it, which the folder it parks under,
   * being another, does not change.
   */
  setWorkingDirectory(conversationId: string, workingDirectory: string): Effect.Effect<void, StoreError> {
    const parked = (harness: HarnessName) =>
      `(select session_id from ${this.#schema}.workspace_sessions where conversation_id = $1 and harness = '${harness}' and working_directory = $2)`;
    return Effect.asVoid(this.#sql.query(
      `with outgoing as (
         select id, working_directory, codex_session_id, claude_session_id from ${this.#schema}.conversations
         where id = $1 and working_directory is distinct from $2
         for update
       ), park as (
         insert into ${this.#schema}.workspace_sessions (conversation_id, harness, working_directory, session_id)
         select outgoing.id, sessions.harness, outgoing.working_directory, sessions.session_id
         from outgoing, lateral (values ('${CODEX_HARNESS}', outgoing.codex_session_id), ('${CLAUDE_HARNESS}', outgoing.claude_session_id))
           as sessions (harness, session_id)
         where outgoing.working_directory is not null
         on conflict (conversation_id, harness, working_directory) do update set session_id = excluded.session_id, updated_at = now()
       )
       update ${this.#schema}.conversations set
         working_directory = $2,
         codex_session_id = ${parked(CODEX_HARNESS)},
         claude_session_id = ${parked(CLAUDE_HARNESS)},
         updated_at = now()
       where id in (select id from outgoing)`,
      [conversationId, workingDirectory],
    ));
  }

  setActiveHarness(conversationId: string, harness: HarnessName): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversations set active_harness = $2, updated_at = now() where id = $1`,
      [conversationId, harness],
    ));
  }

  setHarnessSessionId(conversationId: string, harness: HarnessName, sessionId: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversations set ${SESSION_COLUMNS[harness]} = $2, updated_at = now() where id = $1`,
      [conversationId, sessionId],
    ));
  }

  /** Sets the session of the conversation's mounted harness; with none mounted, nothing changes. */
  setSessionId(conversationId: string, sessionId: string | null): Effect.Effect<void, StoreError> {
    return Effect.asVoid(this.#sql.query(
      `update ${this.#schema}.conversations set ${assignSession("active_harness", "$2")}, updated_at = now() where id = $1`,
      [conversationId, sessionId],
    ));
  }
}
