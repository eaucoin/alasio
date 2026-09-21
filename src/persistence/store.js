import Database from "better-sqlite3";
import { existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { SqliteCallbackRepository } from "./callback-repository.js";
import { SqliteConversationRepository } from "./conversation-repository.js";
import { SqliteResponseRepository } from "./response-repository.js";
import { SqliteOutboxRepository } from "./outbox-repository.js";
import { SqlitePromptJobRepository } from "./prompt-job-repository.js";
import { SqliteRestartRepository } from "./restart-repository.js";
import { migrateSqliteSchema } from "./schema.js";
import { SqliteStateRepository } from "./state-repository.js";
import { SqliteTelegramContentRepository } from "./telegram-content-repository.js";
import { SqliteTurnRepository } from "./turn-repository.js";
import { SqliteUsageRepository } from "./usage-repository.js";

export class SqliteStore {
  constructor(workingDirectory, dbPath = join(workingDirectory, ".alasio", "alasio.sqlite")) {
    this.workingDirectory = workingDirectory;
    this.dbPath = dbPath;
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
    this.conversations = new SqliteConversationRepository(this.db);
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
    migrateSqliteSchema(this.db);
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

  listConversationsWithSessions() {
    return this.conversations.listConversationsWithSessions();
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
    return this.promptJobs.enqueue(args);
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
      if (restartEvent.session_id) {
        this.conversations.setSessionId(turn.thread_key, restartEvent.session_id);
      }
      const job = this.promptJobs.enqueue({
        conversationId: turn.thread_key,
        chatId: turn.channel,
        messageId: recoveryMessageId,
        prompt,
        filePaths: [],
        state: "pending",
        priority: 1,
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
