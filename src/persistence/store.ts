/**
 * alasio's state, in its Neon: the conversations and what they have mounted, the prompts
 * queued and the turns running, the responses and replies on their way to Telegram,
 * what came from Telegram, and the restarts that cut turns short. The repositories keep
 * a table or a few each; the store is what alasio uses of them, and the few changes that
 * span them, each in one transaction.
 */
import type { Update } from "@grammyjs/types";
import { Context, Duration, Effect, Layer } from "effect";
import type { Pool } from "pg";

import type { HarnessName } from "../harness/names.ts";
import type { MediaAttachment } from "../telegram/client.ts";
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
} from "./conversation-repository.ts";
import { NeonOutboxRepository, type NewOutboxText, type OutboxEntry } from "./outbox-repository.ts";
import { NeonPromptJobRepository, type NewPromptJob, type PromptJob, type PromptJobState } from "./prompt-job-repository.ts";
import { type CompletedResponse, NeonResponseRepository, type ResponseBlock } from "./response-repository.ts";
import { NeonRestartRepository, type NewRestartEvent, type RestartEvent } from "./restart-repository.ts";
import { ensureSchema } from "./schema.ts";
import { poolDatabase, type Sql, type StoreError } from "./sql.ts";
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
import { type ActiveTurn, NeonTurnRepository, type Turn } from "./turn-repository.ts";
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
}

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
});

/** What alasio does with its store; each of it fails as Neon does. */
type Stored<A> = Effect.Effect<A, StoreError>;

export class Store extends Context.Service<Store, {
  readonly getState: (key: string) => Stored<string | null>;
  /** The key's value: `value` if the key had none, which it then keeps. */
  readonly claimState: (key: string, value: string) => Stored<string>;
  readonly getTelegramOffset: Stored<number | undefined>;
  readonly setTelegramOffset: (offset: number) => Stored<void>;

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
  readonly setActiveHarness: (conversationId: string, harness: HarnessName) => Stored<void>;
  /** Mounts a folder: the sessions of the folder left are kept for it, and those kept for this one restored. */
  readonly setWorkingDirectory: (conversationId: string, workingDirectory: string) => Stored<void>;
  readonly getModelChoice: (conversationId: string, harness: HarnessName) => Stored<ModelChoice | null>;
  readonly setModelChoice: (conversationId: string, harness: HarnessName, choice: NewModelChoice) => Stored<void>;
  readonly clearModelChoice: (conversationId: string, harness: HarnessName) => Stored<void>;
  /** Sets the session of the conversation's mounted harness. */
  readonly setSessionId: (conversationId: string, sessionId: string | null) => Stored<void>;

  /** Buttons' actions in the conversation, kept until pressed, which remember what it has mounted now: their ids, in order. */
  readonly createCallbackActions: (conversationId: string, actions: readonly NewCallbackAction[]) => Stored<string[]>;
  /** The action of a button pressed for the first time; null for one pressed before, or unknown. */
  readonly consumeCallbackAction: (id: string) => Stored<CallbackAction | null>;

  readonly recordTelegramUpdate: (update: Update) => Stored<void>;
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

  readonly upsertActiveTurn: (turn: ActiveTurn) => Stored<void>;
  /** Records the session the conversation's active turn runs in, there and as its harness's session. */
  readonly updateActiveTurnSessionId: (conversationId: string, sessionId: string | null) => Stored<void>;
  readonly updateActiveTurnPendingResponseId: (conversationId: string, pendingResponseId: string | null) => Stored<void>;
  /** Completes the conversation's active turn; given a pending response, only the turn of that response. */
  readonly clearActiveTurn: (conversationId: string, pendingResponseId?: string | null) => Stored<void>;
  readonly getActiveTurns: Stored<Turn[]>;
  readonly getActiveTurn: (conversationId: string) => Stored<Turn | null>;

  /** A response to the chat's message as its harness begins it: its id. */
  readonly createPendingResponse: (chatId: number | string, messageId: number | string, sessionId?: string | null) => Stored<string>;
  /** Adds blocks to a response, after those it has. */
  readonly appendBlocksToPending: (pendingResponseId: string, blocks: readonly ResponseBlock[]) => Stored<void>;
  readonly markPendingResponseComplete: (pendingResponseId: string) => Stored<void>;
  readonly updatePendingSessionId: (pendingResponseId: string, sessionId: string | null) => Stored<void>;
  readonly markPendingAsPosted: (pendingResponseId: string) => Stored<void>;
  readonly getCompletedResponsesPendingDelivery: Stored<CompletedResponse[]>;

  /** Queues a reply: its id, or that of the one already queued for its pending response. */
  readonly enqueueOutboxText: (text: NewOutboxText) => Stored<string>;
  readonly getDueOutbox: (limit?: number) => Stored<OutboxEntry[]>;
  /** The media a queued reply shows, kept with it until it is sent. */
  readonly getOutboxMedia: (id: string) => Stored<MediaAttachment[]>;
  readonly markOutboxSent: (id: string) => Stored<void>;
  readonly rescheduleOutbox: (id: string, error: unknown, delayMs: number) => Stored<void>;
  readonly getPendingOutboxCount: Stored<number>;

  /** Queues a prompt: the job queued, or the one already queued for its message. */
  readonly enqueuePromptJob: (job: NewPromptJob) => Stored<PromptJob>;
  readonly hasOpenPromptJobs: (conversationId: string) => Stored<boolean>;
  readonly getPromptJob: (id: string) => Stored<PromptJob | null>;
  /** Starts the conversation's next pending job: the job, or null when none waits. */
  readonly claimNextPromptJob: (conversationId: string) => Stored<PromptJob | null>;
  readonly setPromptJobDisposition: (id: string, state: PromptJobState, priority?: number) => Stored<void>;
  readonly markPromptJobDispatched: (id: string) => Stored<void>;
  readonly markPromptJobUpstreamStarted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  readonly markPromptJobUpstreamCompleted: (id: string, sessionId: string | null | undefined, turnId: string | null | undefined) => Stored<void>;
  readonly failPromptJob: (id: string, error: unknown) => Stored<void>;
  readonly listPendingPromptConversations: Stored<string[]>;
  /** Settles the jobs a restart found running: the conversations whose turn the agent finished. */
  readonly recoverPromptJobsAfterRestart: Stored<string[]>;

  /**
   * Continues a turn a restart cut short, if its restart was recorded: its session
   * mounted again, its prompt queued first, and the turn and the restart let go of. The
   * job queued, or null when no restart was recorded.
   */
  readonly stageRestartRecovery: (recovery: RestartRecovery) => Stored<PromptJob | null>;
  readonly recordRestartEvent: (event: NewRestartEvent) => Stored<void>;
  /** Records a restart alasio cannot attribute under the conversation's active turn, unless one is recorded. */
  readonly recordExternalRestartEvent: (conversationId: string) => Stored<void>;
  readonly getRestartEvent: (conversationId: string) => Stored<RestartEvent | null>;
  readonly clearRestartEvent: (conversationId: string) => Stored<void>;

  readonly updateSessionUsage: (sessionId: string | null | undefined, usage: SessionUsage | null | undefined) => Stored<void>;
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

const makeStore = Effect.fnUntraced(function*({ pool, schema = DEFAULT_SCHEMA, workingDirectory = null }: StoreOptions) {
  const database = poolDatabase(pool);
  yield* ensureSchema(database, schema);
  const { state, conversations, callbacks, telegramContent, turns, responses, outbox, promptJobs, restarts, usage, codexLogin } = repositories(
    database,
    schema,
    workingDirectory,
  );

  return Store.of({
    getState: (key) => state.getState(key),
    claimState: (key, value) => state.claimState(key, value),
    getTelegramOffset: state.getTelegramOffset(),
    setTelegramOffset: (offset) => state.setTelegramOffset(offset),

    upsertConversation: (conversation) => conversations.upsertConversation(conversation),
    getConversation: (conversationId) => conversations.getConversation(conversationId),
    getConversationByChatId: (chatId) => conversations.getConversationByChatId(chatId),
    getMount: (conversationId) => conversations.getMount(conversationId),
    listConversationsWithSessions: (harness) => conversations.listConversationsWithSessions(harness),
    listHarnessSessionReferences: (harness) => conversations.listHarnessSessionReferences(harness),
    setActiveHarness: (conversationId, harness) => conversations.setActiveHarness(conversationId, harness),
    setWorkingDirectory: (conversationId, workingDirectory) => conversations.setWorkingDirectory(conversationId, workingDirectory),
    getModelChoice: (conversationId, harness) => conversations.getModelChoice(conversationId, harness),
    setModelChoice: (conversationId, harness, choice) => conversations.setModelChoice(conversationId, harness, choice),
    clearModelChoice: (conversationId, harness) => conversations.clearModelChoice(conversationId, harness),
    setSessionId: (conversationId, sessionId) => conversations.setSessionId(conversationId, sessionId),

    createCallbackActions: (conversationId, actions) => callbacks.createCallbackActions(conversationId, actions),
    consumeCallbackAction: (id) => callbacks.consumeCallbackAction(id),

    recordTelegramUpdate: (update) => telegramContent.recordTelegramUpdate(update),
    markTelegramUpdateProcessed: (updateId) => telegramContent.markTelegramUpdateProcessed(updateId),
    insertMessage: (message) => telegramContent.insertMessage(message),
    insertFile: (file) => telegramContent.insertFile(file),
    getFileContents: (ids) => telegramContent.getFileContents(ids),
    upsertMediaGroup: (arrival) => telegramContent.upsertMediaGroup(arrival),
    markMediaGroupFlushed: (mediaGroupId) => telegramContent.markMediaGroupFlushed(mediaGroupId),
    getMediaGroupMessages: (mediaGroupId) => telegramContent.getMediaGroupMessages(mediaGroupId),
    getFilesForMessages: (messageIds) => telegramContent.getFilesForMessages(messageIds),
    getPendingMediaGroupsDue: (ageMs) => telegramContent.getPendingMediaGroupsDue(ageMs),

    upsertActiveTurn: (turn) => turns.upsertActiveTurn(turn),
    updateActiveTurnSessionId: (conversationId, sessionId) => turns.updateActiveTurnSessionId(conversationId, sessionId),
    updateActiveTurnPendingResponseId: (conversationId, pendingResponseId) => turns.updateActiveTurnPendingResponseId(conversationId, pendingResponseId),
    clearActiveTurn: (conversationId, pendingResponseId) => turns.clearActiveTurn(conversationId, pendingResponseId),
    getActiveTurns: turns.getActiveTurns(),
    getActiveTurn: (conversationId) => turns.getActiveTurn(conversationId),

    createPendingResponse: (chatId, messageId, sessionId) => responses.createPendingResponse(chatId, messageId, sessionId),
    appendBlocksToPending: (pendingResponseId, blocks) => responses.appendBlocks(pendingResponseId, blocks),
    markPendingResponseComplete: (pendingResponseId) => responses.markPendingComplete(pendingResponseId),
    updatePendingSessionId: (pendingResponseId, sessionId) => responses.updatePendingSessionId(pendingResponseId, sessionId),
    markPendingAsPosted: (pendingResponseId) => responses.markPendingAsPosted(pendingResponseId),
    getCompletedResponsesPendingDelivery: responses.getCompletedResponsesPendingDelivery(),

    enqueueOutboxText: (text) => outbox.enqueueText(text),
    getDueOutbox: (limit) => outbox.getDue(limit),
    getOutboxMedia: (id) => outbox.getMedia(id),
    markOutboxSent: (id) => outbox.markSent(id),
    rescheduleOutbox: (id, error, delayMs) => outbox.reschedule(id, error, delayMs),
    getPendingOutboxCount: outbox.getPendingCount(),

    enqueuePromptJob: (job) => promptJobs.enqueue(job),
    hasOpenPromptJobs: (conversationId) => promptJobs.hasOpenJobs(conversationId),
    getPromptJob: (id) => promptJobs.get(id),
    claimNextPromptJob: (conversationId) => promptJobs.claimNext(conversationId),
    setPromptJobDisposition: (id, state, priority) => promptJobs.setDisposition(id, state, priority),
    markPromptJobDispatched: (id) => promptJobs.markDispatched(id),
    markPromptJobUpstreamStarted: (id, sessionId, turnId) => promptJobs.markUpstreamStarted(id, sessionId, turnId),
    markPromptJobUpstreamCompleted: (id, sessionId, turnId) => promptJobs.markUpstreamCompleted(id, sessionId, turnId),
    failPromptJob: (id, error) => promptJobs.fail(id, error),
    listPendingPromptConversations: promptJobs.listPendingConversations(),
    recoverPromptJobsAfterRestart: promptJobs.recoverAfterRestart(),

    stageRestartRecovery: ({ turn, prompt }) =>
      database.transaction(Effect.fnUntraced(function*(sql) {
        const { conversations, promptJobs, responses, turns, restarts } = repositories(sql, schema, workingDirectory);
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
        yield* turns.clearActiveTurn(turn.thread_key);
        yield* restarts.clearRestartEvent(turn.thread_key);
        return job;
      })),
    recordRestartEvent: (event) => restarts.recordRestartEvent(event),
    recordExternalRestartEvent: (conversationId) => restarts.recordExternalRestartEvent(conversationId),
    getRestartEvent: (conversationId) => restarts.getRestartEvent(conversationId),
    clearRestartEvent: (conversationId) => restarts.clearRestartEvent(conversationId),

    updateSessionUsage: (sessionId, sessionUsage) => usage.updateSessionUsage(sessionId, sessionUsage),
    getSessionTokens: (sessionId) => usage.getSessionTokens(sessionId),

    getCodexLogin: codexLogin.getCodexLogin(),
    setCodexLogin: (auth) => codexLogin.setCodexLogin(auth),

    pruneTransient: (age) => {
      const ageMs = Duration.toMillis(age);
      return Effect.all([telegramContent.pruneHandled(ageMs), outbox.pruneSent(ageMs)], { discard: true });
    },
  });
});
