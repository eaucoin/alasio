import { RestartRecovery } from "./restart-recovery.js";
import { NO_WORKSPACE_MOUNTED, createHarnessRegistry, harnessDisplayName, isHarnessName, resolveHarnessName, resolveWorkingDirectory } from "../harness/index.js";
import { finalResponseToMarkdown } from "./response-markdown.js";
import { buildFilePromptSuffix } from "../shared/file-prompt.js";
import { CommandHandler } from "../operator/command-handler.js";
import { sendChooseServicePanel } from "../operator/service-control.js";
import { sendChooseWorkspacePanel } from "../operator/workspace-control.js";
import { createWorkspace, resolveWorkspacePath } from "../workspace/policy.js";
import { sessionFsWorkspace } from "../workspace/kind.js";
import { newVolumeId } from "../sandbox/names.js";
import { truncateText } from "../operator/text.js";
import { StatusReporter } from "./status-reporter.js";
import { createLogger } from "../shared/log.js";

const log = createLogger("codex-turn-controller");

export class TurnController {
  constructor({ config, client, store, outbox, activeQueries, workflowWaits, workflowWakeEvents, isStopping, harnesses = null, sandbox = null }) {
    this.sandbox = sandbox;
    this.config = config;
    this.client = client;
    this.store = store;
    this.activeQueries = activeQueries;
    this.isStopping = isStopping;
    this.queuedMessages = new Map();
    this.promptWorkers = new Map();
    this.harnesses = harnesses ?? createHarnessRegistry({ config });
    this.commands = new CommandHandler({
      client,
      config,
      store,
      activeQueries,
      harnesses: this.harnesses,
      runCodexTurn: (args) => this.runCodexTurn(args),
      runGoalTurn: (args) => this.runGoalTurn(args),
      startNewSession: (args) => this.startNewSession(args),
      switchHarness: (args) => this.switchHarness(args),
      switchWorkspace: (args) => this.switchWorkspace(args),
      createWorkspace: (args) => this.createWorkspace(args),
      sandboxEnabled: this.sandboxEnabled,
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

  harnessFor(conversationId) {
    return this.harnesses.forConversation(this.store, conversationId);
  }

  requireHarness(conversationId) {
    return this.harnesses.requireForConversation(this.store, conversationId);
  }

  harnessLabel(conversationId) {
    const name = resolveHarnessName(this.store, conversationId);
    return name ? harnessDisplayName(name) : "No service";
  }

  workingDirectoryFor(conversationId) {
    return resolveWorkingDirectory(this.store, conversationId);
  }

  requireWorkingDirectory(conversationId) {
    const workingDirectory = this.workingDirectoryFor(conversationId);
    if (!workingDirectory) {
      throw new Error(NO_WORKSPACE_MOUNTED);
    }
    return workingDirectory;
  }

  /**
   * Per-conversation view of the deployment config for helpers that still read
   * config.workingDirectory (goal RPCs).
   */
  configFor(conversationId) {
    return { ...this.config, workingDirectory: this.workingDirectoryFor(conversationId) };
  }

  async sendChooseServicePanel({ conversationId, chatId }) {
    await sendChooseServicePanel({
      client: this.client,
      store: this.store,
      activeQueries: this.activeQueries,
      conversationId,
      chatId,
    });
  }

  async sendChooseWorkspacePanel({ conversationId, chatId }) {
    await sendChooseWorkspacePanel({
      client: this.client,
      store: this.store,
      activeQueries: this.activeQueries,
      conversationId,
      chatId,
      workspaceRoot: this.config.workspaceRoot,
      sandboxEnabled: this.sandboxEnabled,
    });
  }

  /**
   * Service first, then folder. Sends the picker for the first missing layer and
   * reports whether one was sent, so ingress can stop there.
   */
  async sendNextSetupStep({ conversationId, chatId }) {
    if (!resolveHarnessName(this.store, conversationId)) {
      await this.sendChooseServicePanel({ conversationId, chatId });
      return true;
    }
    if (!this.workingDirectoryFor(conversationId)) {
      await this.sendChooseWorkspacePanel({ conversationId, chatId });
      return true;
    }
    return false;
  }

  describeSwitchBlocker(conversationId) {
    if (this.activeQueries.has(conversationId)) {
      return `${this.harnessLabel(conversationId)} is currently working. Stop the active turn before switching services.`;
    }
    if (this.store.hasOpenPromptJobs?.(conversationId)) {
      return "Queued prompts are still waiting for the current service. Let them finish or discard them before switching.";
    }
    return null;
  }

  async switchHarness({ conversationId, harness }) {
    if (!isHarnessName(harness)) {
      throw new Error(`Unknown service: ${String(harness)}`);
    }
    const previous = this.store.getActiveHarness(conversationId);
    if (previous === harness) {
      return { switched: false, previous, next: harness, sessionId: this.store.getSessionId(conversationId) ?? null };
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    this.store.setActiveHarness(conversationId, harness);
    log.info(`service.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${harness}`);
    return {
      switched: true,
      previous,
      next: harness,
      sessionId: this.store.getSessionId(conversationId) ?? null,
      workingDirectory: this.workingDirectoryFor(conversationId),
    };
  }

  async switchWorkspace({ conversationId, target }) {
    const workingDirectory = await resolveWorkspacePath({ root: this.config.workspaceRoot, candidate: target });
    const previous = this.workingDirectoryFor(conversationId);
    if (previous === workingDirectory) {
      return { switched: false, previous, workingDirectory };
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${workingDirectory}`);
    return { switched: true, previous, workingDirectory };
  }

  async createWorkspace({ conversationId, name }) {
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    const workingDirectory = await createWorkspace({ root: this.config.workspaceRoot, name });
    const previous = this.workingDirectoryFor(conversationId);
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.created conversation=${JSON.stringify(conversationId)} path=${workingDirectory}`);
    return { switched: true, created: true, previous, workingDirectory };
  }

  /** Whether this deployment offers session filesystems (the sandbox is configured). */
  get sandboxEnabled() {
    return Boolean(this.sandbox);
  }

  /**
   * Create and mount a new, empty session filesystem with the chosen internet mode.
   * The workspace is the sentinel `sessionfs:<volumeId>` (src/workspace/kind.js), so it
   * parks and restores like any other workspace; its volume and sandbox come up when a
   * turn first needs them.
   */
  async createSessionWorkspace({ conversationId, netMode }) {
    if (!this.sandbox) {
      throw new Error("Session filesystems are not enabled on this deployment.");
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    const volumeId = newVolumeId();
    this.sandbox.volumes.create(volumeId, netMode === "full" ? "full" : "none");
    const workingDirectory = sessionFsWorkspace(volumeId);
    const previous = this.workingDirectoryFor(conversationId);
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.created.sessionfs conversation=${JSON.stringify(conversationId)} volume=${volumeId} net=${netMode}`);
    return { switched: true, created: true, previous, workingDirectory };
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
    if (await this.sendNextSetupStep({ conversationId, chatId })) {
      // Neutral by default: nothing is queued until a service and a folder are chosen.
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
      const activeHarness = this.requireHarness(conversationId).name;
      if (job.harness && job.harness !== activeHarness) {
        log.warn(`Prompt job ${job.id} was admitted under ${job.harness} but ${activeHarness} is active; running under ${activeHarness}`);
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
        await this.client.sendMessage(job.chat_id, `${this.harnessLabel(conversationId)} hit an error: ${error instanceof Error ? error.message : String(error)}`).catch(() => undefined);
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
    const harness = this.requireHarness(conversationId);
    if (this.activeQueries.has(conversationId)) {
      throw new Error(`${harness.displayName} is currently working. Stop the active turn before starting a new session.`);
    }
    const sessionId = await harness.startFreshSession({
      threadKey: conversationId,
      workingDirectory: this.requireWorkingDirectory(conversationId),
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
    await this.client.sendMessage(chatId, `${this.harnessLabel(conversationId)} is currently working. What should I do with this message?\n\n${truncateText(visibleText, 220)}`, {
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
    const harness = this.requireHarness(conversationId);
    if (attachedTurn && !harness.supportsGoals) {
      throw new Error(`${harness.displayName} does not support attached goal turns.`);
    }
    this.store.upsertActiveTurn({
      conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      sessionId: existingSession ?? null,
      harness: harness.name,
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
      harnessName: harness.displayName,
      onStatusMessageCreated: (createdMessageId, startTime) => {
        statusMessageId = createdMessageId;
        statusStartTime = startTime;
      },
    });
    const queryResult = await harness.executeTurn({
      prompt,
      resumeSession: existingSession ?? null,
      threadKey: conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      workingDirectory: this.requireWorkingDirectory(conversationId),
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
      // A harness that keeps running between prompts (Claude Code background work)
      // produces replies of its own and frees the conversation when they finish.
      onBackgroundResponse: () => {
        this.flushCompletedResponses().catch((error) => {
          log.warn(`Background response delivery deferred for ${conversationId}: ${error instanceof Error ? error.message : String(error)}`);
        });
      },
      onIdle: () => {
        void this.scheduleConversation(conversationId);
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
      await this.status.finishWithoutResponse({ chatId, pendingResponseId, statusMessageId, harnessName: harness.displayName });
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
        statusText: `${harness.displayName} did not complete.`,
      });
      this.store.clearActiveTurn(conversationId, pendingResponseId);
      this.store.clearRestartEvent(conversationId);
    } else {
      try {
        const response = finalResponseToMarkdown(blockSequence);
        await this.status.postResponse({ chatId, response, pendingResponseId, statusMessageId, statusStartTime, harnessName: harness.displayName });
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
