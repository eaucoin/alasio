// @ts-nocheck
import Database from "better-sqlite3";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { SqliteCallbackRepository } from "./callback-repository.ts";
import { SqliteConversationRepository } from "./conversation-repository.ts";
import { SqliteResponseRepository } from "./response-repository.ts";
import { SqliteOutboxRepository } from "./outbox-repository.ts";
import { SqlitePromptJobRepository } from "./prompt-job-repository.ts";
import { SqliteRestartRepository } from "./restart-repository.ts";
import { migrateSqliteSchema } from "./schema.ts";
import { SqliteStateRepository } from "./state-repository.ts";
import { SqliteTelegramContentRepository } from "./telegram-content-repository.ts";
import { SqliteTurnRepository } from "./turn-repository.ts";
import { SqliteUsageRepository } from "./usage-repository.ts";

export class SqliteStore {
  /**
   * @param stateRoot directory whose `.alasio/alasio.sqlite` holds state unless dbPath is given
   * @param options.defaultWorkingDirectory optional folder pre-mounted on new conversations and
   *   backfilled onto conversations that predate per-conversation folders
   */
  constructor(stateRoot, dbPath = join(stateRoot, ".alasio", "alasio.sqlite"), { defaultWorkingDirectory = null } = {}) {
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

  close() {
    this.db.close();
  }

  migrate() {
    migrateSqliteSchema(this.db, { legacyWorkingDirectory: this.defaultWorkingDirectory });
  }

  getState(key) {
    return this.state.getState(key);
  }

  setState(key, value) {
    this.state.setState(key, value);
  }

  getTelegramOffset() {
    return this.state.getTelegramOffset();
  }

  setTelegramOffset(offset) {
    this.state.setTelegramOffset(offset);
  }

  upsertConversation({ chatId, user, sessionId }) {
    return this.conversations.upsertConversation({ chatId, user, sessionId });
  }

  getConversationByChatId(chatId) {
    return this.conversations.getConversationByChatId(chatId);
  }

  getConversation(conversationId) {
    return this.conversations.getConversation(conversationId);
  }

  listConversationsWithSessions(harness) {
    return this.conversations.listConversationsWithSessions(harness);
  }

  listHarnessSessionReferences(harness) {
    return this.conversations.listHarnessSessionReferences(harness);
  }

  getActiveHarness(threadKey) {
    return this.conversations.getActiveHarness(threadKey);
  }

  setActiveHarness(threadKey, harness) {
    this.conversations.setActiveHarness(threadKey, harness);
  }

  getWorkingDirectory(threadKey) {
    return this.conversations.getWorkingDirectory(threadKey);
  }

  setWorkingDirectory(threadKey, workingDirectory) {
    this.conversations.setWorkingDirectory(threadKey, workingDirectory);
  }

  getModelChoice(threadKey, harness) {
    return this.conversations.getModelChoice(threadKey, harness);
  }

  setModelChoice(threadKey, harness, choice) {
    this.conversations.setModelChoice(threadKey, harness, choice);
  }

  clearModelChoice(threadKey, harness) {
    this.conversations.clearModelChoice(threadKey, harness);
  }

  getHarnessSessionId(threadKey, harness) {
    return this.conversations.getHarnessSessionId(threadKey, harness);
  }

  setHarnessSessionId(threadKey, harness, sessionId) {
    this.conversations.setHarnessSessionId(threadKey, harness, sessionId);
  }

  getSessionId(threadKey) {
    return this.conversations.getSessionId(threadKey);
  }

  setSessionId(threadKey, sessionId) {
    this.conversations.setSessionId(threadKey, sessionId);
  }

  clearSessionId(threadKey) {
    this.conversations.clearSessionId(threadKey);
  }

  createCallbackAction({ conversationId, kind, payload }) {
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

  consumeCallbackAction(id) {
    return this.callbacks.consumeCallbackAction(id);
  }

  recordTelegramUpdate(update) {
    this.telegramContent.recordTelegramUpdate(update);
  }

  markTelegramUpdateProcessed(updateId) {
    this.telegramContent.markTelegramUpdateProcessed(updateId);
  }

  insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId }) {
    return this.telegramContent.insertMessage({ conversationId, direction, kind, transportMessageId, text, mediaGroupId, raw, sessionId, turnId });
  }

  insertFile({ conversationId, messageId, file, localPath, sha256 }) {
    return this.telegramContent.insertFile({ conversationId, messageId, file, localPath, sha256 });
  }

  upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs }) {
    this.telegramContent.upsertMediaGroup({ mediaGroupId, conversationId, updateId, flushAfterMs });
  }

  markMediaGroupFlushed(mediaGroupId) {
    this.telegramContent.markMediaGroupFlushed(mediaGroupId);
  }

  getMediaGroupMessages(mediaGroupId) {
    return this.telegramContent.getMediaGroupMessages(mediaGroupId);
  }

  getFilesForMessages(messageIds) {
    return this.telegramContent.getFilesForMessages(messageIds);
  }

  getPendingMediaGroupsDue(ageMs) {
    return this.telegramContent.getPendingMediaGroupsDue(ageMs);
  }

  upsertActiveTurn(turn) {
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

  updateActiveTurnSessionId(threadKey, sessionId) {
    this.turns.updateActiveTurnSessionId(threadKey, sessionId);
  }

  updateActiveTurnPendingResponseId(threadKey, pendingResponseId) {
    this.turns.updateActiveTurnPendingResponseId(threadKey, pendingResponseId);
  }

  clearActiveTurn(threadKey, pendingResponseId) {
    this.turns.clearActiveTurn(threadKey, pendingResponseId);
  }

  getActiveTurns() {
    return this.turns.getActiveTurns();
  }

  createPendingResponse(chatId, messageId, sessionId = null) {
    return this.responses.createPendingResponse(chatId, messageId, sessionId);
  }

  appendBlockToPending(pendingResponseId, block) {
    this.responses.appendBlockToPending(pendingResponseId, block);
  }

  markPendingResponseComplete(pendingResponseId) {
    this.responses.markPendingComplete(pendingResponseId);
  }

  updatePendingSessionId(pendingResponseId, sessionId) {
    this.responses.updatePendingSessionId(pendingResponseId, sessionId);
  }

  markPendingAsPosted(pendingResponseId) {
    this.responses.markPendingAsPosted(pendingResponseId);
  }

  getCompletedResponsesPendingDelivery() {
    return this.responses.getCompletedResponsesPendingDelivery();
  }

  enqueueOutboxText(args) {
    return this.outbox.enqueueText(args);
  }

  getDueOutbox(limit) {
    return this.outbox.getDue(limit);
  }

  markOutboxSent(id) {
    this.outbox.markSent(id);
  }

  rescheduleOutbox(id, error, delayMs) {
    this.outbox.reschedule(id, error, delayMs);
  }

  getPendingOutboxCount() {
    return this.outbox.getPendingCount();
  }

  enqueuePromptJob(args) {
    return this.promptJobs.enqueue({
      ...args,
      harness: args.harness ?? this.getActiveHarness(args.conversationId),
    });
  }

  hasOpenPromptJobs(conversationId) {
    return this.promptJobs.hasOpenJobs(conversationId);
  }

  getPromptJob(id) {
    return this.promptJobs.get(id);
  }

  claimNextPromptJob(conversationId) {
    return this.promptJobs.claimNext(conversationId);
  }

  setPromptJobDisposition(id, state, priority) {
    this.promptJobs.setDisposition(id, state, priority);
  }

  completePromptJob(id) {
    this.promptJobs.complete(id);
  }

  markPromptJobUpstreamStarted(id, sessionId, turnId) {
    this.promptJobs.markUpstreamStarted(id, sessionId, turnId);
  }

  markPromptJobUpstreamCompleted(id, sessionId, turnId) {
    this.promptJobs.markUpstreamCompleted(id, sessionId, turnId);
  }

  failPromptJob(id, error) {
    this.promptJobs.fail(id, error);
  }

  listPendingPromptConversations() {
    return this.promptJobs.listPendingConversations();
  }

  recoverPromptJobsAfterRestart() {
    return this.promptJobs.recoverAfterRestart();
  }

  stageRestartRecovery({ turn, prompt }) {
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

  recordRestartEvent(event) {
    this.restarts.recordRestartEvent(event);
  }

  getRestartEvent(threadKey) {
    return this.restarts.getRestartEvent(threadKey);
  }

  clearRestartEvent(threadKey) {
    this.restarts.clearRestartEvent(threadKey);
  }

  consumeRestartEvent(threadKey) {
    return this.restarts.consumeRestartEvent(threadKey);
  }

  updateSessionUsage(sessionId, usage) {
    this.usage.updateSessionUsage(sessionId, usage);
  }

  getSessionTokens(sessionId) {
    return this.usage.getSessionTokens(sessionId);
  }
}
