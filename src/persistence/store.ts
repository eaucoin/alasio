import Database from "better-sqlite3";
import type { Update } from "@grammyjs/types";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import type { HarnessName } from "../harness/names.ts";
import { type CallbackAction, type NewCallbackAction, SqliteCallbackRepository } from "./callback-repository.ts";
import {
  type Conversation,
  type HarnessSessionReference,
  type LinkedConversation,
  type ModelChoice,
  type NewConversation,
  type NewModelChoice,
  SqliteConversationRepository,
} from "./conversation-repository.ts";
import { type CompletedResponse, type ResponseBlock, SqliteResponseRepository } from "./response-repository.ts";
import { type NewOutboxText, type OutboxEntry, SqliteOutboxRepository } from "./outbox-repository.ts";
import { type NewPromptJob, type PromptJob, type PromptJobState, SqlitePromptJobRepository } from "./prompt-job-repository.ts";
import { type RestartEvent, SqliteRestartRepository } from "./restart-repository.ts";
import { migrateSqliteSchema } from "./schema.ts";
import { SqliteStateRepository } from "./state-repository.ts";
import {
  type MediaGroup,
  type MediaGroupArrival,
  type NewFile,
  type NewMessage,
  type StoredFile,
  type StoredMessage,
  SqliteTelegramContentRepository,
} from "./telegram-content-repository.ts";
import { type ActiveTurn, type Turn, SqliteTurnRepository } from "./turn-repository.ts";
import { type SessionUsage, SqliteUsageRepository } from "./usage-repository.ts";

export interface SqliteStoreOptions {
  readonly defaultWorkingDirectory?: string | null | undefined;
}

/** A turn starting; its thread is its conversation's unless one is given. */
export interface ActiveTurnStart extends Omit<ActiveTurn, "threadKey"> {
  readonly threadKey?: string | undefined;
  readonly conversationId: string;
}

/** A turn a restart cut short, and the prompt that resumes it. */
export interface RestartRecovery {
  readonly turn: Turn;
  readonly prompt: string;
}

export class SqliteStore {
  readonly dbPath: string;
  private readonly defaultWorkingDirectory: string | null;
  /** The database itself, for what the repositories do not cover (tests inspect it). */
  readonly db: Database.Database;
  private readonly state: SqliteStateRepository;
  private readonly conversations: SqliteConversationRepository;
  private readonly callbacks: SqliteCallbackRepository;
  private readonly telegramContent: SqliteTelegramContentRepository;
  private readonly turns: SqliteTurnRepository;
  private readonly responses: SqliteResponseRepository;
  private readonly outbox: SqliteOutboxRepository;
  private readonly promptJobs: SqlitePromptJobRepository;
  private readonly restarts: SqliteRestartRepository;
  private readonly usage: SqliteUsageRepository;

  /**
   * @param stateRoot directory whose `.alasio/alasio.sqlite` holds state unless dbPath is given
   * @param options.defaultWorkingDirectory optional folder pre-mounted on new conversations and
   *   backfilled onto conversations that predate per-conversation folders
   */
  constructor(stateRoot: string, dbPath = join(stateRoot, ".alasio", "alasio.sqlite"), { defaultWorkingDirectory = null }: SqliteStoreOptions = {}) {
    this.dbPath = dbPath;
    this.defaultWorkingDirectory = defaultWorkingDirectory;
    if (!existsSync(dirname(dbPath))) {
      mkdirSync(dirname(dbPath), { recursive: true });
    }
    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("synchronous = NORMAL");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("busy_timeout = 5000");
    this.migrate();
    this.state = new SqliteStateRepository(this.db);
    this.conversations = new SqliteConversationRepository(this.db, { defaultWorkingDirectory });
    this.callbacks = new SqliteCallbackRepository(this.db);
    this.telegramContent = new SqliteTelegramContentRepository(this.db);
    this.turns = new SqliteTurnRepository(this.db, this.conversations);
    this.responses = new SqliteResponseRepository(this.db, this.conversations);
    this.outbox = new SqliteOutboxRepository(this.db, this.conversations);
    this.promptJobs = new SqlitePromptJobRepository(this.db);
    this.restarts = new SqliteRestartRepository(this.db);
    this.usage = new SqliteUsageRepository(this.db);
  }

  close(): void {
    this.db.close();
  }

  migrate(): void {
    migrateSqliteSchema(this.db, { legacyWorkingDirectory: this.defaultWorkingDirectory });
  }

  getState(key: string): string | null {
    return this.state.getState(key);
  }

  setState(key: string, value: string | number): void {
    this.state.setState(key, value);
  }

  getTelegramOffset(): number | undefined {
    return this.state.getTelegramOffset();
  }

  setTelegramOffset(offset: number): void {
    this.state.setTelegramOffset(offset);
  }

  upsertConversation({ chatId, user, sessionId }: NewConversation): string {
    return this.conversations.upsertConversation({ chatId, user, sessionId });
  }

  getConversationByChatId(chatId: number | string): Conversation | null {
    return this.conversations.getConversationByChatId(chatId);
  }

  getConversation(conversationId: string): Conversation | null {
    return this.conversations.getConversation(conversationId);
  }

  listConversationsWithSessions(harness?: HarnessName): LinkedConversation[] {
    return this.conversations.listConversationsWithSessions(harness);
  }

  listHarnessSessionReferences(harness: HarnessName): HarnessSessionReference[] {
    return this.conversations.listHarnessSessionReferences(harness);
  }

  getActiveHarness(threadKey: string): HarnessName | null {
    return this.conversations.getActiveHarness(threadKey);
  }

  setActiveHarness(threadKey: string, harness: HarnessName): void {
    this.conversations.setActiveHarness(threadKey, harness);
  }

  getWorkingDirectory(threadKey: string): string | null {
    return this.conversations.getWorkingDirectory(threadKey);
  }

  setWorkingDirectory(threadKey: string, workingDirectory: string): void {
    this.conversations.setWorkingDirectory(threadKey, workingDirectory);
  }

  getModelChoice(threadKey: string, harness: HarnessName): ModelChoice | null {
    return this.conversations.getModelChoice(threadKey, harness);
  }

  setModelChoice(threadKey: string, harness: HarnessName, choice: NewModelChoice): void {
    this.conversations.setModelChoice(threadKey, harness, choice);
  }

  clearModelChoice(threadKey: string, harness: HarnessName): void {
    this.conversations.clearModelChoice(threadKey, harness);
  }

  getHarnessSessionId(threadKey: string, harness: HarnessName): string | undefined {
    return this.conversations.getHarnessSessionId(threadKey, harness);
  }

  setHarnessSessionId(threadKey: string, harness: HarnessName, sessionId: string | null): void {
    this.conversations.setHarnessSessionId(threadKey, harness, sessionId);
  }

  getSessionId(threadKey: string): string | undefined {
    return this.conversations.getSessionId(threadKey);
  }

  setSessionId(threadKey: string, sessionId: string | null): void {
    this.conversations.setSessionId(threadKey, sessionId);
  }

  clearSessionId(threadKey: string): void {
    this.conversations.clearSessionId(threadKey);
  }

  createCallbackAction({ conversationId, kind, payload }: NewCallbackAction): string {
    return this.callbacks.createCallbackAction({
      conversationId,
      kind,
      payload: {
        ...payload,
        expectedSessionId: this.getSessionId(conversationId) ?? null,
        expectedHarness: this.getActiveHarness(conversationId),
      },
    });
  }

  consumeCallbackAction(id: string): CallbackAction | null {
    return this.callbacks.consumeCallbackAction(id);
  }

  recordTelegramUpdate(update: Update): void {
    this.telegramContent.recordTelegramUpdate(update);
  }

  markTelegramUpdateProcessed(updateId: number): void {
    this.telegramContent.markTelegramUpdateProcessed(updateId);
  }

  insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId }: NewMessage): string {
    return this.telegramContent.insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId });
  }

  insertFile({ conversationId, messageId, file, localPath, sha256 }: NewFile): string {
    return this.telegramContent.insertFile({ conversationId, messageId, file, localPath, sha256 });
  }

  upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs }: MediaGroupArrival): void {
    this.telegramContent.upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs });
  }

  markMediaGroupFlushed(mediaGroupId: string): void {
    this.telegramContent.markMediaGroupFlushed(mediaGroupId);
  }

  getMediaGroupMessages(mediaGroupId: string): StoredMessage[] {
    return this.telegramContent.getMediaGroupMessages(mediaGroupId);
  }

  getFilesForMessages(messageIds: readonly string[]): StoredFile[] {
    return this.telegramContent.getFilesForMessages(messageIds);
  }

  getPendingMediaGroupsDue(ageMs: number): MediaGroup[] {
    return this.telegramContent.getPendingMediaGroupsDue(ageMs);
  }

  upsertActiveTurn(turn: ActiveTurnStart): void {
    this.turns.upsertActiveTurn({
      threadKey: turn.threadKey ?? turn.conversationId,
      chatId: turn.chatId,
      messageId: turn.messageId,
      sessionId: turn.sessionId ?? null,
      harness: turn.harness ?? this.getActiveHarness(turn.threadKey ?? turn.conversationId),
      pendingResponseId: turn.pendingResponseId ?? null,
      prompt: turn.prompt ?? null,
      startedAt: turn.startedAt ?? Date.now() / 1000,
    });
  }

  updateActiveTurnSessionId(threadKey: string, sessionId: string | null): void {
    this.turns.updateActiveTurnSessionId(threadKey, sessionId);
  }

  updateActiveTurnPendingResponseId(threadKey: string, pendingResponseId: string | null): void {
    this.turns.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
  }

  clearActiveTurn(threadKey: string, pendingResponseId?: string | null): void {
    this.turns.clearActiveTurn(threadKey, pendingResponseId);
  }

  getActiveTurns(): Turn[] {
    return this.turns.getActiveTurns();
  }

  createPendingResponse(chatId: number | string, messageId: number | string, sessionId: string | null = null): string {
    return this.responses.createPendingResponse(chatId, messageId, sessionId);
  }

  appendBlockToPending(pendingResponseId: string, block: ResponseBlock): void {
    this.responses.appendBlockToPending(pendingResponseId, block);
  }

  markPendingResponseComplete(pendingResponseId: string): void {
    this.responses.markPendingComplete(pendingResponseId);
  }

  updatePendingSessionId(pendingResponseId: string, sessionId: string | null): void {
    this.responses.updatePendingSessionId(pendingResponseId, sessionId);
  }

  markPendingAsPosted(pendingResponseId: string): void {
    this.responses.markPendingAsPosted(pendingResponseId);
  }

  getCompletedResponsesPendingDelivery(): CompletedResponse[] {
    return this.responses.getCompletedResponsesPendingDelivery();
  }

  enqueueOutboxText(args: NewOutboxText): string {
    return this.outbox.enqueueText(args);
  }

  getDueOutbox(limit?: number): OutboxEntry[] {
    return this.outbox.getDue(limit);
  }

  markOutboxSent(id: string): void {
    this.outbox.markSent(id);
  }

  rescheduleOutbox(id: string, error: unknown, delayMs: number): void {
    this.outbox.reschedule(id, error, delayMs);
  }

  getPendingOutboxCount(): number {
    return this.outbox.getPendingCount();
  }

  enqueuePromptJob(args: NewPromptJob): PromptJob {
    return this.promptJobs.enqueue({
      ...args,
      harness: args.harness ?? this.getActiveHarness(args.conversationId),
    });
  }

  hasOpenPromptJobs(conversationId: string): boolean {
    return this.promptJobs.hasOpenJobs(conversationId);
  }

  getPromptJob(id: string): PromptJob | null {
    return this.promptJobs.get(id);
  }

  claimNextPromptJob(conversationId: string): PromptJob | null {
    return this.promptJobs.claimNext(conversationId);
  }

  setPromptJobDisposition(id: string, state: PromptJobState, priority?: number): void {
    this.promptJobs.setDisposition(id, state, priority);
  }

  completePromptJob(id: string): void {
    this.promptJobs.complete(id);
  }

  markPromptJobUpstreamStarted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): void {
    this.promptJobs.markUpstreamStarted(id, sessionId, turnId);
  }

  markPromptJobUpstreamCompleted(id: string, sessionId: string | null | undefined, turnId: string | null | undefined): void {
    this.promptJobs.markUpstreamCompleted(id, sessionId, turnId);
  }

  failPromptJob(id: string, error: unknown): void {
    this.promptJobs.fail(id, error);
  }

  listPendingPromptConversations(): string[] {
    return this.promptJobs.listPendingConversations();
  }

  recoverPromptJobsAfterRestart(): string[] {
    return this.promptJobs.recoverAfterRestart();
  }

  stageRestartRecovery({ turn, prompt }: RestartRecovery): PromptJob | null {
    const stage = this.db.transaction(() => {
      const restartEvent = this.restarts.getRestartEvent(turn.thread_key);
      if (!restartEvent) {
        return null;
      }
      const eventTimestamp = Number(restartEvent.timestamp ?? turn.started_at);
      const recoveryMessageId = ["restart", turn.thread_ts, eventTimestamp].join(":");
      const harness = turn.harness ?? this.getActiveHarness(turn.thread_key);
      if (restartEvent.session_id) {
        this.conversations.setHarnessSessionId(turn.thread_key, harness, restartEvent.session_id);
      }
      const job = this.promptJobs.enqueue({
        conversationId: turn.thread_key,
        chatId: turn.channel,
        messageId: recoveryMessageId,
        prompt,
        filePaths: [],
        state: "pending",
        priority: 1,
        harness,
      });
      if (turn.pending_response_id) {
        this.responses.markPendingAsPosted(turn.pending_response_id);
      }
      this.turns.clearActiveTurn(turn.thread_key);
      this.restarts.clearRestartEvent(turn.thread_key);
      return job;
    });
    return stage();
  }

  recordRestartEvent(event: RestartEvent): void {
    this.restarts.recordRestartEvent(event);
  }

  getRestartEvent(threadKey: string): RestartEvent | null {
    return this.restarts.getRestartEvent(threadKey);
  }

  clearRestartEvent(threadKey: string): void {
    this.restarts.clearRestartEvent(threadKey);
  }

  consumeRestartEvent(threadKey: string): RestartEvent | null {
    return this.restarts.consumeRestartEvent(threadKey);
  }

  updateSessionUsage(sessionId: string | null | undefined, usage: SessionUsage | null | undefined): void {
    this.usage.updateSessionUsage(sessionId, usage);
  }

  getSessionTokens(sessionId: string): number {
    return this.usage.getSessionTokens(sessionId);
  }
}
