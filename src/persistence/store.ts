/**
 * alasio's state, in its Neon: the conversations and what they have mounted, the prompts
 * queued and the turns running, the responses and replies on their way to Telegram,
 * what came from Telegram, and the restarts that cut turns short. The repositories keep
 * a table or a few each; the store is what alasio uses of them, and the few changes that
 * span them, each in one transaction.
 */
import type { Update } from "@grammyjs/types";
import { Context, Duration, Effect, Layer, Option, Schema } from "effect";
import type { Pool } from "pg";

import type { HarnessName } from "../harness/names.ts";
import type { MediaAttachment } from "../telegram/client.ts";
import { startBranch } from "./branch-start.ts";
import { type CallbackAction, NeonCallbackRepository, type NewCallbackAction } from "./callback-repository.ts";
import { NeonCodexLoginRepository } from "./codex-login-repository.ts";
import {
  type Conversation,
  type HarnessSessionReference,
  type LinkedConversation,
  type ModelChoice,
  type Mount,
  NeonConversationRepository,
  type NewConversation,
  type NewModelChoice,
  type WorkspaceConversation,
} from "./conversation-repository.ts";
import { type DueOutboxEntry, NeonOutboxRepository, type NewOutboxStatus, type NewOutboxText } from "./outbox-repository.ts";
import { NeonPromptJobRepository, type NewPromptJob, type PromptJob, type PromptJobState, type QueuedPromptJob } from "./prompt-job-repository.ts";
import { type CompletedResponse, NeonResponseRepository, type ResponseBlock } from "./response-repository.ts";
import { NeonRestartRepository, type NewRestartEvent, type RestartEvent } from "./restart-repository.ts";
import { ensureSchema } from "./schema.ts";
import { type NewSessionWorkspace, NeonSessionWorkspaceRepository, type SessionWorkspace, type SessionWorkspaceNetMode } from "./session-workspace-repository.ts";
import { poolDatabase, type Sql, StoreError } from "./sql.ts";
import { NeonStateRepository } from "./state-repository.ts";
import {
  type FileContent,
  type MediaGroup,
  type MediaGroupArrival,
  NeonTelegramContentRepository,
  type NewFile,
  type NewMessage,
  type StoredFile,
  type StoredMessage,
} from "./telegram-content-repository.ts";
import { type ActiveTurn, NeonTurnRepository, type Turn, type TurnOutcome, type TurnStatus } from "./turn-repository.ts";
import { NeonUsageRepository, type SessionUsage } from "./usage-repository.ts";

/**
 * The schema alasio's state is kept in. Not alasio's role's name: a role's search path
 * starts with the schema of its name, where whatever the role makes unqualified would
 * then go.
 */
export const DEFAULT_SCHEMA = "state";

/** A turn a restart cut short, and the prompt that resumes it. */
export interface RestartRecovery {
  readonly turn: Turn;
  readonly prompt: string;
}

/** What the store is made on: the pool it shares, and where in it the state is. */
export interface StoreOptions {
  readonly pool: Pool;
  /** `schema` is where the tables live: alasio's, or a test's own. */
  readonly schema?: string | undefined;
  /** The folder a new conversation is mounted on, if any. */
  readonly workingDirectory?: string | null | undefined;
  /** The branch environment alasio is, whose state is made its own as it first opens (./branch-start.ts); none for main. */
  readonly branch?: string | null | undefined;
  /** How long handling a chat's update waits for the chat's handling lock (withHandlingLock): HANDLING_LOCK_TIMEOUT unless a test says. */
  readonly handlingLockTimeout?: Duration.Input | undefined;
  /** How long a statement under a chat's lock may run (LOCKED_STATEMENT_TIMEOUT unless a test says). */
  readonly lockedStatementTimeout?: Duration.Input | undefined;
}

/**
 * How long handling a chat's update waits for the chat's handling lock before it fails,
 * and the update is taken again: far longer than an update's handling, so only a holder
 * that is stuck makes it fail.
 */
export const HANDLING_LOCK_TIMEOUT = Duration.seconds(30);

/**
 * How long a statement made under a chat's lock, in its transaction, may run before
 * Postgres ends it: far longer than any of them takes (a mark, a read of the chat's next
 * entry), and less than the grace alasio's pod stops with (60 s), so one that never
 * returns, as of a compute frozen under it, holds no delivery, which alasio does not cut
 * short as it stops, past its stopping. Set on the locks' transactions alone: alasio's
 * other statements, its schema's setup and its pruning among them, are not bounded by it.
 */
export const LOCKED_STATEMENT_TIMEOUT = Duration.seconds(45);

/** Each of alasio's repositories, on one connection or transaction. */
const repositories = (sql: Sql, schema: string, workingDirectory: string | null) => ({
  state: new NeonStateRepository(sql, schema),
  conversations: new NeonConversationRepository(sql, schema, workingDirectory),
  callbacks: new NeonCallbackRepository(sql, schema),
  telegramContent: new NeonTelegramContentRepository(sql, schema),
  turns: new NeonTurnRepository(sql, schema),
  responses: new NeonResponseRepository(sql, schema),
  outbox: new NeonOutboxRepository(sql, schema),
  promptJobs: new NeonPromptJobRepository(sql, schema),
  restarts: new NeonRestartRepository(sql, schema),
  usage: new NeonUsageRepository(sql, schema),
  codexLogin: new NeonCodexLoginRepository(sql, schema),
  sessionWorkspaces: new NeonSessionWorkspaceRepository(sql, schema),
});

/** What alasio does with its store; each of it fails as Neon does. */
type Stored<A> = Effect.Effect<A, StoreError>;

/**
 * A chat's locks, each held by one at a time of all the alasios on the store: its updates'
 * handling, and its replies' delivery, apart, so a reply Telegram is slow to take holds
 * back no update. Each is a transaction's advisory lock, the transaction open on a
 * connection of its own, let go of however what holds it ends. What its holder marks of
 * its work is written in that transaction (HandlingMarks, DeliveryMarks), so a lock lost,
 * as its connection is, rolls back the marks made under it: none outlives its lock.
 */
export type ChatLock = "handling" | "delivery";

/** What a chat's handling lock's holder marks of an update, in the lock's transaction. */
export interface HandlingMarks {
  /** Keeps an update, unless it is kept already: whether it is yet to be processed. */
  readonly recordTelegramUpdate: (update: Update) => Stored<boolean>;
  readonly markTelegramUpdateProcessed: (updateId: number) => Stored<void>;
}

/** What a chat's delivery lock's holder reads and marks of the chat's outbox, in the lock's transaction. */
export interface DeliveryMarks {
  /** The chat's next entry, if it is due. */
  readonly getDueOutboxOf: (chatId: string) => Stored<DueOutboxEntry | null>;
  /** Marks an entry delivered: a status message with its message's id, one given up on with why. */
  readonly markOutboxSent: (id: string, delivered?: { readonly telegramMessageId?: number | null; readonly refused?: string | null }) => Stored<void>;
  readonly rescheduleOutbox: (id: string, error: unknown, delayMs: number) => Stored<void>;
  /**
   * A round trip on the lock's connection: it fails where the connection, and so the lock,
   * is lost, for its holder to send nothing it may no longer send.
   */
  readonly fence: Stored<void>;
}

/**
 * What names a chat's lock to Postgres, whose advisory locks are numbers: its text, of the
 * store's schema, the lock and the chat, which Postgres hashes to one (hashtextextended).
 */
export const chatLockName = (schema: string, lock: ChatLock, chatId: string | number): string => `${schema}:${lock}:${chatId}`;

/**
 * A turn's write refused, as another turn has claimed its conversation since
 * (./turn-repository.ts): a write of a turn superseded, not a failure of the store's.
 */
export class TurnFenced extends Schema.TaggedError<TurnFenced>()("TurnFenced", {
  threadKey: Schema.String,
  epoch: Schema.Number,
}) {
  override get message(): string {
    return `the turn of epoch ${this.epoch} no longer holds conversation ${this.threadKey}: another turn has claimed it since`;
  }
}

/** A turn's write: failing as the store does, or with TurnFenced where the turn no longer holds its conversation. */
export type TurnWrite<A> = Effect.Effect<A, StoreError | TurnFenced>;

/**
 * What a turn, and its harness, write in alasio's store as it runs, all of the
 * conversation's turn it was made for: its response, its session and usage, what it
 * waits on, and any restart it causes. Each write holds only while no turn has claimed
 * the conversation since the turn did (Store.turnPersistence), and fails otherwise, with
 * TurnFenced; the turn's epoch is checked in the statement, or the transaction, that
 * writes.
 */
export interface TurnPersistence {
  /** The epoch of the turn it writes for, which TurnFenced names where it no longer holds. */
  readonly epoch: number;
  /** A response to the chat's message as the harness begins it: its id. */
  readonly createPendingResponse: (chatId: number | string, messageId: number | string, sessionId?: string | null) => TurnWrite<string>;
  /** Records the response the turn writes. */
  readonly updateActiveTurnPendingResponseId: (pendingResponseId: string | null) => TurnWrite<void>;
  readonly updatePendingSessionId: (pendingResponseId: string, sessionId: string | null) => TurnWrite<void>;
  /** Records the session the turn runs in, there and as its harness's session in the conversation. */
  readonly updateActiveTurnSessionId: (sessionId: string | null) => TurnWrite<void>;
  /** Adds blocks to a response, after those it has. */
  readonly appendBlocksToPending: (pendingResponseId: string, blocks: readonly ResponseBlock[]) => TurnWrite<void>;
  readonly markPendingResponseComplete: (pendingResponseId: string) => TurnWrite<void>;
  readonly markPendingAsPosted: (pendingResponseId: string) => TurnWrite<void>;
  readonly updateSessionUsage: (sessionId: string | null | undefined, usage: SessionUsage | null | undefined) => TurnWrite<void>;
  /** Says the turn waits on `waitingOn` (a workflow run), or works again, for null, while it works. */
  readonly setWaiting: (waitingOn: string | null) => TurnWrite<void>;
  /** Records a restart the turn causes, of its conversation. */
  readonly recordRestartEvent: (event: Omit<NewRestartEvent, "thread_key">) => TurnWrite<void>;
}

export class Store extends Context.Service<Store, {
  readonly getState: (key: string) => Stored<string | null>;

  /** Makes the chat's conversation, or updates who it is with: its id. */
  readonly upsertConversation: (conversation: NewConversation) => Stored<string>;
  readonly getConversation: (conversationId: string) => Stored<Conversation | null>;
  readonly getConversationByChatId: (chatId: number | string) => Stored<Conversation | null>;
  /** What the conversation has mounted; nothing, for one not yet made. */
  readonly getMount: (conversationId: string) => Stored<Mount>;
  /** The conversations whose mounted harness, `harness`, has a session, most recently changed first. */
  readonly listConversationsWithSessions: (harness: HarnessName) => Stored<LinkedConversation[]>;
  /** Every session of a harness alasio points at, with the folder it runs in. */
  readonly listHarnessSessionReferences: (harness: HarnessName) => Stored<HarnessSessionReference[]>;
  /** The conversations that know the workspace: mounting it, or keeping a session of it parked; none for one alasio never made. */
  readonly listWorkspaceConversations: (workingDirectory: string) => Stored<WorkspaceConversation[]>;
  readonly setActiveHarness: (conversationId: string, harness: HarnessName) => Stored<void>;
  /** Mounts a folder: the sessions of the folder left are kept for it, and those kept for this one restored. */
  readonly setWorkingDirectory: (conversationId: string, workingDirectory: string) => Stored<void>;
  readonly getModelChoice: (conversationId: string, harness: HarnessName) => Stored<ModelChoice | null>;
  readonly setModelChoice: (conversationId: string, harness: HarnessName, choice: NewModelChoice) => Stored<void>;
  readonly clearModelChoice: (conversationId: string, harness: HarnessName) => Stored<void>;
  /** Sets the session of the conversation's mounted harness. */
  readonly setSessionId: (conversationId: string, sessionId: string | null) => Stored<void>;

  /** Records a session filesystem before it is made, not yet made. */
  readonly recordSessionWorkspace: (workspace: NewSessionWorkspace) => Stored<void>;
  /** Marks a session filesystem made, with the internet it was made with. */
  readonly markSessionWorkspaceMade: (volumeId: string, netMode: SessionWorkspaceNetMode) => Stored<void>;
  /** Records, as made, a session filesystem a branch environment inherited, which its parent forked into it (../branch/fork.ts). */
  readonly recordInheritedSessionWorkspace: (volumeId: string, netMode: SessionWorkspaceNetMode) => Stored<void>;
  readonly forgetSessionWorkspace: (volumeId: string) => Stored<void>;
  /** Every session filesystem recorded, made or not, newest first. */
  readonly listSessionWorkspaces: Stored<SessionWorkspace[]>;

  /** Buttons' actions in the conversation, kept until pressed, which remember what it has mounted now: their ids, in order. */
  readonly createCallbackActions: (conversationId: string, actions: readonly NewCallbackAction[]) => Stored<string[]>;
  /** The action of a button pressed for the first time; null for one pressed before, or unknown. */
  readonly consumeCallbackAction: (id: string) => Stored<CallbackAction | null>;

  /** Keeps an update, unless it is kept already: whether it is yet to be processed. */
  readonly recordTelegramUpdate: (update: Update) => Stored<boolean>;
  /**
   * Runs `work` holding the chat's handling lock (ChatLock), given the marks it makes in
   * the lock's transaction, waiting while another holds it, as an update waits its turn,
   * but for the store's handling lock timeout at most: past it, it fails as the store does,
   * without running `work`.
   */
  readonly withHandlingLock: <A, E, R>(chatId: string | number, work: (marks: HandlingMarks) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | StoreError, R>;
  /**
   * Runs `work` holding the chat's delivery lock (ChatLock), given the reads and marks it
   * makes in the lock's transaction, where no other holds it: what it gave, or none,
   * without running `work`, where another does, which is delivering the chat's replies, so
   * nothing waits on it holding a connection.
   */
  readonly withDeliveryLock: <A, E, R>(chatId: string | number, work: (marks: DeliveryMarks) => Effect.Effect<A, E, R>) => Effect.Effect<Option.Option<A>, E | StoreError, R>;
  readonly markTelegramUpdateProcessed: (updateId: number) => Stored<void>;
  readonly insertMessage: (message: NewMessage) => Stored<string>;
  readonly insertFile: (file: NewFile) => Stored<string>;
  /** The files `ids` name, with their content. */
  readonly getFileContents: (ids: readonly string[]) => Stored<FileContent[]>;
  readonly upsertMediaGroup: (arrival: MediaGroupArrival) => Stored<void>;
  readonly markMediaGroupFlushed: (mediaGroupId: string) => Stored<void>;
  readonly getMediaGroupMessages: (mediaGroupId: string) => Stored<StoredMessage[]>;
  readonly getFilesForMessages: (messageIds: readonly string[]) => Stored<StoredFile[]>;
  readonly getPendingMediaGroupsDue: (ageMs: number) => Stored<MediaGroup[]>;

  /** Claims the conversation for a turn starting: the turn's epoch, which its writes hold under (turnPersistence). */
  readonly claimTurn: (turn: ActiveTurn) => Stored<number>;
  /** What the turn of `epoch`, and its harness, write as it runs, each write holding while no turn has claimed the conversation since. */
  readonly turnPersistence: (conversationId: string, epoch: number) => TurnPersistence;
  /** Says what the conversation's active turn of `epoch` is doing: whether it still holds the conversation. */
  readonly setTurnStatus: (conversationId: string, epoch: number, status: TurnStatus) => Stored<boolean>;
  /** Records the status message the conversation's turn of `epoch` queued: whether it still holds the conversation. */
  readonly setTurnStatusMessage: (conversationId: string, epoch: number, outboxId: string) => Stored<boolean>;
  /**
   * Ends the conversation's active turn of `epoch`, as `outcome` says it ended, and lets go
   * of any restart recorded under it: whether it was that turn, and active.
   */
  readonly endTurn: (conversationId: string, epoch: number, outcome: TurnOutcome) => Stored<boolean>;
  readonly getActiveTurns: Stored<Turn[]>;
  readonly getActiveTurn: (conversationId: string) => Stored<Turn | null>;
  /** The conversation's latest turn, active or ended: what its harness said it did, and how it ended. */
  readonly getTurn: (conversationId: string) => Stored<Turn | null>;
  /** Holds the conversations busy for `reason`, as no turn does (a fork). */
  readonly holdConversations: (conversationIds: readonly string[], reason: string) => Stored<void>;
  /** Lets go of the conversations' holds; of every one, for null. */
  readonly releaseConversations: (conversationIds: readonly string[] | null) => Stored<void>;

  /** Lets go of a response, as delivered or never to be. */
  readonly markPendingAsPosted: (pendingResponseId: string) => Stored<void>;
  readonly getCompletedResponsesPendingDelivery: Stored<CompletedResponse[]>;

  /** Queues a reply: its id, or that of the one already queued for its pending response. */
  readonly enqueueOutboxText: (text: NewOutboxText) => Stored<string>;
  /** Queues a turn's status message: its id, which an edit of it names. */
  readonly enqueueOutboxStatus: (status: NewOutboxStatus) => Stored<string>;
  /** Queues an edit of the status message `edits`. */
  readonly enqueueOutboxEdit: (edits: string, edit: NewOutboxStatus) => Stored<string>;
  readonly getDueOutbox: (limit?: number) => Stored<DueOutboxEntry[]>;
  /** The chat's next entry, if it is due. */
  readonly getDueOutboxOf: (chatId: string) => Stored<DueOutboxEntry | null>;
  /** The media a queued reply shows, kept with it until it is sent. */
  readonly getOutboxMedia: (id: string) => Stored<MediaAttachment[]>;
  /** Marks an entry delivered: a status message with its message's id, one given up on with why. */
  readonly markOutboxSent: (id: string, delivered?: { readonly telegramMessageId?: number | null; readonly refused?: string | null }) => Stored<void>;
  readonly rescheduleOutbox: (id: string, error: unknown, delayMs: number) => Stored<void>;
  readonly getPendingOutboxCount: Stored<number>;

  /** Queues a prompt: the job queued, or the one already queued for its message. */
  readonly enqueuePromptJob: (job: NewPromptJob) => Stored<QueuedPromptJob>;
  readonly hasOpenPromptJobs: (conversationId: string) => Stored<boolean>;
  readonly getPromptJob: (id: string) => Stored<PromptJob | null>;
  /** Starts the conversation's next pending job: the job, or null when none waits. */
  readonly claimNextPromptJob: (conversationId: string) => Stored<PromptJob | null>;
  /** Settles what becomes of a job, its priority kept unless given. */
  readonly setPromptJobDisposition: (id: string, state: PromptJobState, priority?: number) => Stored<void>;
  /**
   * Settles a job held for the operator's choice, the buttons that would settle it
   * forgotten: the job, or null where it is held no longer, when nothing changes.
   */
  readonly settleHeldPromptJob: (id: string, state: PromptJobState, priority?: number) => Stored<PromptJob | null>;
  /** The conversation's jobs held for the operator's choice, in the order queued. */
  readonly listHeldPromptJobs: (conversationId: string) => Stored<PromptJob[]>;
  readonly markPromptJobDispatched: (id: string) => Stored<void>;
  readonly markPromptJobUpstreamStarted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  readonly markPromptJobUpstreamCompleted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  readonly failPromptJob: (id: string, error: unknown) => Stored<void>;
  /** The conversations with prompt jobs waiting: to run, or for the operator's choice. */
  readonly listWaitingPromptConversations: Stored<string[]>;
  /** Settles the jobs a restart found running: the conversations whose turn the agent finished. */
  readonly recoverPromptJobsAfterRestart: Stored<string[]>;
  /** Holds again the jobs a restart found handed to a turn to steer it (prompt-job-repository.ts holdUnsteeredAfterRestart). */
  readonly holdUnsteeredPromptJobsAfterRestart: Stored<PromptJob[]>;

  /**
   * Continues a turn a restart cut short, if its restart was recorded: its session
   * mounted again, its prompt queued first, and the turn and the restart let go of. The
   * job queued, or null when no restart was recorded.
   */
  readonly stageRestartRecovery: (recovery: RestartRecovery) => Stored<PromptJob | null>;
  /** Records a restart alasio cannot attribute under the conversation's active turn of `epoch`, unless one is recorded. */
  readonly recordExternalRestartEvent: (conversationId: string, epoch: number) => Stored<void>;
  readonly getRestartEvent: (conversationId: string) => Stored<RestartEvent | null>;

  readonly getSessionTokens: (sessionId: string) => Stored<number>;

  /** The text of Codex's auth.json as last kept, or null when none is. */
  readonly getCodexLogin: Stored<string | null>;
  /** Keeps `auth` as Codex's login; null, Codex logged out, keeps none. */
  readonly setCodexLogin: (auth: string | null) => Stored<void>;

  /**
   * Deletes what is only kept while it is in flight, `age` after it landed: updates
   * processed, albums handled, and replies delivered.
   */
  readonly pruneTransient: (age: Duration.Input) => Stored<void>;
}>()("alasio/persistence/Store") {
  /** The store in `schema` of the pool's database, its tables made first where missing. */
  static readonly layer = (options: StoreOptions): Layer.Layer<Store, StoreError> => Layer.effect(Store, makeStore(options));
}

const makeStore = Effect.fnUntraced(function*({
  pool,
  schema = DEFAULT_SCHEMA,
  workingDirectory = null,
  branch = null,
  handlingLockTimeout = HANDLING_LOCK_TIMEOUT,
  lockedStatementTimeout = LOCKED_STATEMENT_TIMEOUT,
}: StoreOptions) {
  const database = poolDatabase(pool);
  /** Bounds the statements of a chat's lock's transaction `sql` (LOCKED_STATEMENT_TIMEOUT), for that transaction alone. */
  const bounded = (sql: Sql): Effect.Effect<void, StoreError> =>
    Effect.asVoid(sql.query("select set_config('statement_timeout', $1, true)", [`${Duration.toMillis(lockedStatementTimeout)}ms`]));
  yield* ensureSchema(database, schema);
  if (branch && (yield* startBranch(database, schema, branch))) {
    yield* Effect.logInfo(`made the state the branch ${branch}'s: what was in flight where it was branched from is gone`);
  }
  /** The repositories on `sql`: a transaction's. */
  const repositoriesOn = (sql: Sql) => repositories(sql, schema, workingDirectory);
  const { state, conversations, callbacks, telegramContent, turns, responses, outbox, promptJobs, restarts, usage, codexLogin, sessionWorkspaces } = repositoriesOn(database);

  return Store.of({
    getState: (key) => state.getState(key),

    upsertConversation: (conversation) => conversations.upsertConversation(conversation),
    getConversation: (conversationId) => conversations.getConversation(conversationId),
    getConversationByChatId: (chatId) => conversations.getConversationByChatId(chatId),
    getMount: (conversationId) => conversations.getMount(conversationId),
    listConversationsWithSessions: (harness) => conversations.listConversationsWithSessions(harness),
    listHarnessSessionReferences: (harness) => conversations.listHarnessSessionReferences(harness),
    listWorkspaceConversations: (workingDirectory) => conversations.listWorkspaceConversations(workingDirectory),
    setActiveHarness: (conversationId, harness) => conversations.setActiveHarness(conversationId, harness),
    setWorkingDirectory: (conversationId, workingDirectory) => conversations.setWorkingDirectory(conversationId, workingDirectory),
    getModelChoice: (conversationId, harness) => conversations.getModelChoice(conversationId, harness),
    setModelChoice: (conversationId, harness, choice) => conversations.setModelChoice(conversationId, harness, choice),
    clearModelChoice: (conversationId, harness) => conversations.clearModelChoice(conversationId, harness),
    setSessionId: (conversationId, sessionId) => conversations.setSessionId(conversationId, sessionId),

    recordSessionWorkspace: (workspace) => sessionWorkspaces.record(workspace),
    markSessionWorkspaceMade: (volumeId, netMode) => sessionWorkspaces.markMade(volumeId, netMode),
    recordInheritedSessionWorkspace: (volumeId, netMode) => sessionWorkspaces.recordInherited(volumeId, netMode),
    forgetSessionWorkspace: (volumeId) => sessionWorkspaces.forget(volumeId),
    listSessionWorkspaces: sessionWorkspaces.list(),

    createCallbackActions: (conversationId, actions) => callbacks.createCallbackActions(conversationId, actions),
    consumeCallbackAction: (id) => callbacks.consumeCallbackAction(id),

    recordTelegramUpdate: (update) => telegramContent.recordTelegramUpdate(update),
    withHandlingLock: (chatId, work) =>
      database.transaction((sql) =>
        // Past the timeout, Postgres fails the wait (lock_not_available), and the transaction with it.
        bounded(sql).pipe(
          Effect.andThen(sql.query("select set_config('lock_timeout', $1, true)", [`${Duration.toMillis(handlingLockTimeout)}ms`])),
          Effect.andThen(sql.query("select pg_advisory_xact_lock(hashtextextended($1, 0))", [chatLockName(schema, "handling", chatId)])),
          Effect.andThen(Effect.suspend(() => {
            const { telegramContent: locked } = repositoriesOn(sql);
            return work({
              recordTelegramUpdate: (update) => locked.recordTelegramUpdate(update),
              markTelegramUpdateProcessed: (updateId) => locked.markTelegramUpdateProcessed(updateId),
            });
          })),
        )
      ),
    // Waiting for a connection, and for the lock, may be cut short, as alasio stops; the
    // delivery under it, once the lock is taken, and its commit, are not.
    withDeliveryLock: (chatId, work) =>
      database.guardedTransaction(
        (sql) =>
          bounded(sql).pipe(
            Effect.andThen(sql.query<{ taken: boolean }>("select pg_try_advisory_xact_lock(hashtextextended($1, 0)) as taken", [chatLockName(schema, "delivery", chatId)])),
            Effect.map(([row]) => row?.taken ?? false),
          ),
        (sql) => {
          const { outbox: locked } = repositoriesOn(sql);
          return work({
            getDueOutboxOf: (chat) => locked.getDueOf(chat),
            markOutboxSent: (id, delivered) => locked.markSent(id, delivered),
            rescheduleOutbox: (id, error, delayMs) => locked.reschedule(id, error, delayMs),
            fence: Effect.asVoid(sql.query("select 1")),
          });
        },
      ),
    markTelegramUpdateProcessed: (updateId) => telegramContent.markTelegramUpdateProcessed(updateId),
    insertMessage: (message) => telegramContent.insertMessage(message),
    insertFile: (file) => telegramContent.insertFile(file),
    getFileContents: (ids) => telegramContent.getFileContents(ids),
    upsertMediaGroup: (arrival) => telegramContent.upsertMediaGroup(arrival),
    markMediaGroupFlushed: (mediaGroupId) => telegramContent.markMediaGroupFlushed(mediaGroupId),
    getMediaGroupMessages: (mediaGroupId) => telegramContent.getMediaGroupMessages(mediaGroupId),
    getFilesForMessages: (messageIds) => telegramContent.getFilesForMessages(messageIds),
    getPendingMediaGroupsDue: (ageMs) => telegramContent.getPendingMediaGroupsDue(ageMs),

    claimTurn: (turn) => turns.claim(turn),
    turnPersistence: (conversationId, epoch) => {
      const fenced = () => new TurnFenced({ threadKey: conversationId, epoch });
      /** `write`, failing with TurnFenced where it did not hold. */
      const held = (write: Stored<boolean>): TurnWrite<void> => Effect.flatMap(write, (holds) => (holds ? Effect.void : Effect.fail(fenced())));
      /**
       * `write`, of a table that names no turn, in one transaction with the turn's row
       * locked for share once it is seen to hold the conversation.
       */
      const holding = (write: (repositories: ReturnType<typeof repositoriesOn>) => Stored<void>): TurnWrite<void> =>
        database.transaction((sql) => {
          const on = repositoriesOn(sql);
          return Effect.flatMap(on.turns.lockAt(conversationId, epoch), (holds): TurnWrite<void> => (holds ? write(on) : Effect.fail(fenced())));
        });
      return {
        epoch,
        createPendingResponse: (chatId, messageId, sessionId) =>
          Effect.flatMap(responses.createPendingResponse(conversationId, epoch, chatId, messageId, sessionId), (id) => (id === null ? Effect.fail(fenced()) : Effect.succeed(id))),
        updateActiveTurnPendingResponseId: (pendingResponseId) => held(turns.updatePendingResponseIdAt(conversationId, epoch, pendingResponseId)),
        updatePendingSessionId: (pendingResponseId, sessionId) => held(responses.updatePendingSessionId(pendingResponseId, sessionId)),
        updateActiveTurnSessionId: (sessionId) => held(turns.updateSessionIdAt(conversationId, epoch, sessionId)),
        appendBlocksToPending: (pendingResponseId, blocks) => held(responses.appendBlocks(pendingResponseId, blocks)),
        markPendingResponseComplete: (pendingResponseId) => held(responses.markPendingComplete(pendingResponseId)),
        markPendingAsPosted: (pendingResponseId) => held(responses.markPendingAsPosted(pendingResponseId, true)),
        updateSessionUsage: (sessionId, sessionUsage) => holding((on) => on.usage.updateSessionUsage(sessionId, sessionUsage)),
        setWaiting: (waitingOn) => held(turns.setWaitingAt(conversationId, epoch, waitingOn)),
        recordRestartEvent: (event) => holding((on) => on.restarts.recordRestartEvent({ ...event, thread_key: conversationId })),
      };
    },
    setTurnStatus: (conversationId, epoch, status) => turns.setStatus(conversationId, epoch, status),
    setTurnStatusMessage: (conversationId, epoch, outboxId) => turns.setStatusMessageAt(conversationId, epoch, outboxId),
    endTurn: (conversationId, epoch, outcome) =>
      database.transaction((sql) => {
        const on = repositoriesOn(sql);
        return Effect.tap(on.turns.end(conversationId, epoch, outcome), (ended) => (ended ? on.restarts.clearRestartEvent(conversationId) : Effect.void));
      }),
    getActiveTurns: turns.getActiveTurns(),
    getActiveTurn: (conversationId) => turns.getActiveTurn(conversationId),
    getTurn: (conversationId) => turns.getTurn(conversationId),
    holdConversations: (conversationIds, reason) => turns.hold(conversationIds, reason),
    releaseConversations: (conversationIds) => turns.release(conversationIds),

    markPendingAsPosted: (pendingResponseId) => Effect.asVoid(responses.markPendingAsPosted(pendingResponseId)),
    getCompletedResponsesPendingDelivery: responses.getCompletedResponsesPendingDelivery(),

    enqueueOutboxText: (text) => outbox.enqueueText(text),
    enqueueOutboxStatus: (status) => outbox.enqueueStatus(status),
    enqueueOutboxEdit: (edits, edit) => outbox.enqueueEdit(edits, edit),
    getDueOutbox: (limit) => outbox.getDue(limit),
    getDueOutboxOf: (chatId) => outbox.getDueOf(chatId),
    getOutboxMedia: (id) => outbox.getMedia(id),
    markOutboxSent: (id, delivered) => outbox.markSent(id, delivered),
    rescheduleOutbox: (id, error, delayMs) => outbox.reschedule(id, error, delayMs),
    getPendingOutboxCount: outbox.getPendingCount(),

    enqueuePromptJob: (job) => promptJobs.enqueue(job),
    hasOpenPromptJobs: (conversationId) => promptJobs.hasOpenJobs(conversationId),
    getPromptJob: (id) => promptJobs.get(id),
    claimNextPromptJob: (conversationId) => promptJobs.claimNext(conversationId),
    setPromptJobDisposition: (id, state, priority) => promptJobs.setDisposition(id, state, priority),
    listHeldPromptJobs: (conversationId) => promptJobs.listHeld(conversationId),
    settleHeldPromptJob: (id, state, priority) =>
      database.transaction((sql) => {
        const { promptJobs, callbacks } = repositoriesOn(sql);
        return Effect.tap(promptJobs.settleHeld(id, state, priority), (job) => (job ? callbacks.forgetActionsOfJob(id) : Effect.void));
      }),
    markPromptJobDispatched: (id) => promptJobs.markDispatched(id),
    markPromptJobUpstreamStarted: (id, sessionId, turnId) => promptJobs.markUpstreamStarted(id, sessionId, turnId),
    markPromptJobUpstreamCompleted: (id, sessionId, turnId) => promptJobs.markUpstreamCompleted(id, sessionId, turnId),
    failPromptJob: (id, error) => promptJobs.fail(id, error),
    listWaitingPromptConversations: promptJobs.listWaitingConversations(),
    recoverPromptJobsAfterRestart: promptJobs.recoverAfterRestart(),
    holdUnsteeredPromptJobsAfterRestart: promptJobs.holdUnsteeredAfterRestart(),

    stageRestartRecovery: ({ turn, prompt }) =>
      database.transaction(Effect.fnUntraced(function*(sql) {
        const { conversations, promptJobs, responses, turns, restarts } = repositoriesOn(sql);
        const restartEvent = yield* restarts.getRestartEvent(turn.thread_key);
        if (!restartEvent) {
          return null;
        }
        if (restartEvent.session_id) {
          yield* conversations.setHarnessSessionId(turn.thread_key, turn.harness, restartEvent.session_id);
        }
        const job = yield* promptJobs.enqueue({
          conversationId: turn.thread_key,
          chatId: turn.channel,
          // One recovery per restart, however often it is staged.
          messageId: ["restart", turn.thread_ts, restartEvent.recorded_at.getTime() / 1000].join(":"),
          prompt,
          priority: 1,
          harness: turn.harness,
        });
        if (turn.pending_response_id) {
          yield* responses.markPendingAsPosted(turn.pending_response_id);
        }
        yield* turns.end(turn.thread_key, Number(turn.epoch), "stopped");
        yield* restarts.clearRestartEvent(turn.thread_key);
        return job;
      })),
    recordExternalRestartEvent: (conversationId, epoch) => restarts.recordExternalRestartEvent(conversationId, epoch),
    getRestartEvent: (conversationId) => restarts.getRestartEvent(conversationId),

    getSessionTokens: (sessionId) => usage.getSessionTokens(sessionId),

    getCodexLogin: codexLogin.getCodexLogin(),
    setCodexLogin: (auth) => codexLogin.setCodexLogin(auth),

    pruneTransient: (age) => {
      const ageMs = Duration.toMillis(age);
      return Effect.all([telegramContent.pruneHandled(ageMs), outbox.pruneSent(ageMs)], { discard: true });
    },
  });
});
