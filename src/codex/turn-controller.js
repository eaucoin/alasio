import { executeCodexTurn, startFreshCodexSession } from "./runtime.js";
import { RestartRecovery } from "./restart-recovery.js";
import { finalResponseToMarkdown } from "./response-markdown.js";
import { buildFilePromptSuffix } from "../shared/file-prompt.js";
import { CommandHandler } from "../operator/command-handler.js";
import { truncateText } from "../operator/text.js";
import { StatusReporter } from "./status-reporter.js";
import { createLogger } from "../shared/log.js";

const log = createLogger("codex-turn-controller");

export class TurnController {
  constructor({ config, client, store, outbox, activeQueries, workflowWaits, workflowWakeEvents, isStopping }) {
    this.config = config;
    this.client = client;
    this.store = store;
    this.activeQueries = activeQueries;
    this.isStopping = isStopping;
    this.queuedMessages = new Map();
    this.promptWorkers = new Map();
    this.commands = new CommandHandler({
      client,
      config,
      store,
      activeQueries,
      runCodexTurn: (args) => this.runCodexTurn(args),
      runGoalTurn: (args) => this.runGoalTurn(args),
      startNewSession: (args) => this.startNewSession(args),
    });
    this.status = new StatusReporter({
      client,
      store,
      outbox,
      workflowWaits,
      workflowWakeEvents,
      log,
    });
    this.recovery = new RestartRecovery({ store });
  }

  enqueueMessage(conversationId, prompt, front = false) {
    const queue = this.queuedMessages.get(conversationId) ?? [];
    if (front) {
      queue.unshift(prompt);
    } else {
      queue.push(prompt);
    }
    this.queuedMessages.set(conversationId, queue);
  }

  async processPrompt({ conversationId, chatId, messageId, text, filePaths }) {
    const effectiveText = text || (filePaths.length > 0 ? "Please inspect the attached file(s)." : "");
    const handledCommand = await this.commands.handleTextCommand({
      text: effectiveText,
      filePaths,
      conversationId,
      chatId,
      messageId,
    });
    if (handledCommand) {
      return;
    }
    const prompt = effectiveText + buildFilePromptSuffix(filePaths);
    if (!prompt.trim()) {
      return;
    }
    const job = this.store.enqueuePromptJob({
      conversationId,
      chatId,
      messageId,
      prompt,
      filePaths,
      state: this.activeQueries.has(conversationId) ? "awaiting_choice" : "pending",
    });
    if (job.state === "awaiting_choice") {
      await this.askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText: effectiveText });
      return;
    }
    void this.scheduleConversation(conversationId);
  }

  scheduleConversation(conversationId) {
    const existing = this.promptWorkers.get(conversationId);
    if (existing) {
      return existing;
    }
    const worker = this.drainConversation(conversationId).finally(() => {
      this.promptWorkers.delete(conversationId);
    });
    this.promptWorkers.set(conversationId, worker);
    return worker;
  }

  async drainConversation(conversationId) {
    while (!this.activeQueries.has(conversationId) && !this.isStopping()) {
      const job = this.store.claimNextPromptJob(conversationId);
      if (!job) {
        return;
      }
      try {
        const completed = await this.runCodexTurn({
          conversationId,
          chatId: job.chat_id,
          messageId: job.message_id,
          prompt: job.prompt,
          jobId: job.id,
        });
        if (this.isStopping()) {
          return;
        }
        this.store.setPromptJobDisposition(job.id, completed ? "completed" : "cancelled");
      } catch (error) {
        this.store.failPromptJob(job.id, error);
        await this.client.sendMessage(job.chat_id, `Codex hit an error: ${error instanceof Error ? error.message : String(error)}`).catch(() => undefined);
      }
    }
  }

  resumePendingPrompts() {
    for (const conversationId of this.store.listPendingPromptConversations()) {
      void this.scheduleConversation(conversationId);
    }
  }

  reconcilePersistentState() {
    const completedConversations = this.store.recoverPromptJobsAfterRestart();
    for (const conversationId of completedConversations) {
      this.store.clearActiveTurn(conversationId);
      this.store.clearRestartEvent(conversationId);
    }
  }

  setPromptDisposition(jobId, state, priority = 0) {
    this.store.setPromptJobDisposition(jobId, state, priority);
    const job = this.store.getPromptJob(jobId);
    if (state === "pending" && job) {
      void this.scheduleConversation(job.conversation_id);
    }
    return job;
  }

  async startNewSession({ conversationId }) {
    if (this.activeQueries.has(conversationId)) {
      throw new Error("Codex is currently working. Stop the active turn before starting a new session.");
    }
    const sessionId = await startFreshCodexSession({
      threadKey: conversationId,
      workingDirectory: this.config.workingDirectory,
    });
    this.store.setSessionId(conversationId, sessionId);
    return sessionId;
  }

  async askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText }) {
    const payload = { jobId: job.id, prompt: job.prompt };
    const queueAction = this.store.createCallbackAction({ conversationId, kind: "queue", payload });
    const steerAction = this.store.createCallbackAction({ conversationId, kind: "steer", payload });
    const swerveAction = this.store.createCallbackAction({ conversationId, kind: "swerve", payload });
    const discardAction = this.store.createCallbackAction({ conversationId, kind: "discard", payload });
    await this.client.sendMessage(chatId, `Codex is currently working. What should I do with this message?\n\n${truncateText(visibleText, 220)}`, {
      reply_markup: {
        inline_keyboard: [[
          { text: "Steer", callback_data: steerAction },
          { text: "Queue", callback_data: queueAction },
        ], [
          { text: "Swerve", callback_data: swerveAction },
          { text: "Discard", callback_data: discardAction },
        ]],
      },
    });
  }

  async runCodexTurn({ conversationId, chatId, messageId, prompt, jobId = null }) {
    const existingSession = this.store.getSessionId(conversationId);
    return await this.runCodexTurnWithSession({
      conversationId,
      chatId,
      messageId,
      prompt,
      existingSession: existingSession ?? null,
      attachedTurn: null,
      jobId,
    });
  }

  async runGoalTurn({ conversationId, chatId, messageId, sessionId, turnId, prompt }) {
    if (this.activeQueries.has(conversationId)) {
      const job = this.store.enqueuePromptJob
        ? this.store.enqueuePromptJob({ conversationId, chatId, messageId, prompt, state: "awaiting_choice" })
        : { id: null, prompt };
      await this.askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText: prompt });
      return true;
    }
    this.store.setSessionId(conversationId, sessionId);
    await this.runCodexTurnWithSession({
      conversationId,
      chatId,
      messageId,
      prompt,
      existingSession: sessionId,
      attachedTurn: turnId ? { sessionId, turnId } : null,
    });
    return true;
  }

  async runCodexTurnWithSession({ conversationId, chatId, messageId, prompt, existingSession, attachedTurn, jobId = null }) {
    this.store.upsertActiveTurn({
      conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      sessionId: existingSession ?? null,
      pendingResponseId: null,
      prompt,
      startedAt: Date.now() / 1000,
    });
    const statusAbortController = new AbortController();
    let statusMessageId = null;
    let statusStartTime = null;
    const statusPromise = this.status.postStatusUpdates({
      chatId,
      signal: statusAbortController.signal,
      sessionId: existingSession ?? null,
      onStatusMessageCreated: (createdMessageId, startTime) => {
        statusMessageId = createdMessageId;
        statusStartTime = startTime;
      },
    });
    const queryResult = await executeCodexTurn({
      prompt,
      resumeSession: existingSession ?? null,
      threadKey: conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      workingDirectory: this.config.workingDirectory,
      persistence: this.store,
      activeQueries: this.activeQueries,
      attachedTurn,
      onTransportStarted: ({ sessionId, turnId }) => {
        if (jobId) {
          this.store.markPromptJobUpstreamStarted(jobId, sessionId, turnId);
        }
      },
      onTransportCompleted: ({ sessionId, turnId }) => {
        if (jobId) {
          this.store.markPromptJobUpstreamCompleted(jobId, sessionId, turnId);
        }
      },
    });
    statusAbortController.abort();
    await statusPromise.catch(() => undefined);
    const { blockSequence, sessionId: newSessionId, pendingResponseId, interrupted, responseCompleted } = queryResult;
    const sessionId = newSessionId ?? existingSession ?? null;
    if (this.isStopping()) {
      if (responseCompleted) {
        this.store.clearActiveTurn(conversationId, pendingResponseId);
        this.store.clearRestartEvent(conversationId);
        log.info(`Leaving completed response ${pendingResponseId} for post-restart delivery`);
      } else {
        log.info(`Leaving active turn ${conversationId} for post-restart recovery because the service is stopping`);
      }
      return;
    }
    if (newSessionId && !existingSession) {
      this.store.setSessionId(conversationId, newSessionId);
    }
    if (interrupted) {
      await this.status.finishWithoutResponse({ chatId, pendingResponseId, statusMessageId });
      this.store.clearActiveTurn(conversationId, pendingResponseId);
      this.store.clearRestartEvent(conversationId);
      const queued = this.queuedMessages.get(conversationId);
      if (queued && queued.length > 0) {
        this.queuedMessages.delete(conversationId);
        await this.runCodexTurn({
          conversationId,
          chatId,
          messageId,
          prompt: queued.join("\n\n---\n\n"),
        });
      }
      return false;
    }
    if (!responseCompleted) {
      await this.status.finishWithoutResponse({
        chatId,
        pendingResponseId,
        statusMessageId,
        statusText: "Codex did not complete.",
      });
      this.store.clearActiveTurn(conversationId, pendingResponseId);
      this.store.clearRestartEvent(conversationId);
    } else {
      try {
        const response = finalResponseToMarkdown(blockSequence);
        await this.status.postResponse({ chatId, response, pendingResponseId, statusMessageId, statusStartTime });
      } catch (error) {
        log.error(`Final response handoff deferred for ${conversationId}: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        this.store.clearActiveTurn(conversationId, pendingResponseId);
        this.store.clearRestartEvent(conversationId);
      }
    }
    const queued = this.queuedMessages.get(conversationId);
    if (queued && queued.length > 0) {
      this.queuedMessages.delete(conversationId);
      await this.runCodexTurn({
        conversationId,
        chatId,
        messageId,
        prompt: queued.join("\n\n---\n\n"),
      });
    }
    return responseCompleted;
  }

  async flushCompletedResponses() {
    await this.status.flushCompletedResponses();
  }

  async recoverInterruptedTurns() {
    await this.recovery.recoverInterruptedTurns();
  }

  recordExternalRestartEventsForActiveTurns() {
    this.recovery.recordExternalRestartEventsForActiveTurns();
  }
}
