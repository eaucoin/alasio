import { NO_SERVICE_MOUNTED, NO_WORKSPACE_MOUNTED, interruptActiveTurn, isHarnessName, resolveHarnessName, resolveWorkingDirectory } from "../harness/index.js";
import { handleGoalControlCallback, isGoalControlAction } from "../operator/goal-control.js";
import { handleModelControlCallback, isModelControlAction } from "../operator/model-control.js";
import { handleServiceControlCallback, isServiceControlAction } from "../operator/service-control.js";
import { handleSessionControlCallback, isSessionControlAction } from "../operator/session-control.js";
import { handleWorkspaceControlCallback, isWorkspaceControlAction, sendChooseWorkspacePanel } from "../operator/workspace-control.js";

function clientAfterCallbackAck(client) {
  return new Proxy(client, {
    get(target, property) {
      if (property === "answerCallbackQuery") {
        return async () => null;
      }
      const value = target[property];
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export class CallbackHandler {
  constructor({ authorizer, client, config, store, turns, activeQueries }) {
    this.authorizer = authorizer;
    this.client = client;
    this.config = config;
    this.store = store;
    this.turns = turns;
    this.activeQueries = activeQueries;
  }

  async handle(callbackQuery) {
    if (!this.authorizer.isAuthorizedCallbackQuery(callbackQuery)) {
      await this.client.answerCallbackQuery(callbackQuery.id, "This action is not authorized for this Telegram user.");
      return;
    }
    const action = this.store.consumeCallbackAction(callbackQuery.data);
    if (!action) {
      await this.client.answerCallbackQuery(callbackQuery.id, "This action is no longer available.");
      return;
    }
    if (!action.kind.endsWith(":close")
      && Object.hasOwn(action.payload, "expectedSessionId")
      && (this.store.getSessionId(action.conversationId) ?? null) !== action.payload.expectedSessionId) {
      await this.client.answerCallbackQuery(callbackQuery.id, "This panel is stale. Open it again.");
      return;
    }
    if (!action.kind.endsWith(":close")
      && !isServiceControlAction(action.kind)
      && isHarnessName(action.payload?.expectedHarness)
      && this.store.getActiveHarness?.(action.conversationId) !== action.payload.expectedHarness) {
      await this.client.answerCallbackQuery(callbackQuery.id, "This panel belongs to another service. Open it again.");
      return;
    }
    const harness = this.turns.harnessFor(action.conversationId);
    const chatId = callbackQuery.message?.chat?.id;
    const messageId = callbackQuery.message?.message_id;
    if (!chatId || !messageId) {
      await this.client.answerCallbackQuery(callbackQuery.id, "Missing message context.");
      return;
    }
    if (isModelControlAction(action.kind)) {
      await handleModelControlCallback({
        client: this.client,
        store: this.store,
        action,
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
      });
      return;
    }
    if (isServiceControlAction(action.kind)) {
      await handleServiceControlCallback({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        action,
        switchHarness: (args) => this.turns.switchHarness(args),
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
        onMounted: async () => {
          // Service first, then folder: chain straight into the folder picker.
          if (!resolveWorkingDirectory(this.store, action.conversationId)) {
            await sendChooseWorkspacePanel({
              client: this.client,
              store: this.store,
              activeQueries: this.activeQueries,
              conversationId: action.conversationId,
              chatId,
              workspaceRoot: this.config.workspaceRoot,
            });
          }
        },
      });
      return;
    }
    if (isWorkspaceControlAction(action.kind)) {
      await handleWorkspaceControlCallback({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        action,
        workspaceRoot: this.config.workspaceRoot,
        switchWorkspace: (args) => this.turns.switchWorkspace(args),
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
      });
      return;
    }
    if (!harness) {
      const reason = resolveHarnessName(this.store, action.conversationId) ? NO_WORKSPACE_MOUNTED : NO_SERVICE_MOUNTED;
      await this.client.answerCallbackQuery(callbackQuery.id, reason);
      return;
    }
    if (isSessionControlAction(action.kind)) {
      const acknowledged = !action.kind.endsWith(":close");
      if (acknowledged) {
        await this.client.answerCallbackQuery(callbackQuery.id, "Working...");
      }
      await handleSessionControlCallback({
        client: acknowledged ? clientAfterCallbackAck(this.client) : this.client,
        store: this.store,
        harness,
        activeQueries: this.activeQueries,
        action,
        startNewSession: (args) => this.turns.startNewSession(args),
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
      });
      return;
    }
    if (action.kind === "steer") {
      const activeQuery = this.activeQueries.get(action.conversationId);
      const promptJob = action.payload.jobId ? this.store.getPromptJob(action.payload.jobId) : null;
      const prompt = promptJob?.prompt ?? action.payload.prompt;
      if (!activeQuery?.steer) {
        if (promptJob) {
          this.turns.setPromptDisposition(promptJob.id, "pending");
        } else {
          this.turns.enqueueMessage(action.conversationId, prompt);
        }
        await this.client.answerCallbackQuery(callbackQuery.id, "Queued.");
        await this.client.editMessageText(chatId, messageId, `${harness.displayName} is not ready to steer yet. Queued instead.`);
        return;
      }
      try {
        await activeQuery.steer(prompt);
        if (promptJob) {
          this.turns.setPromptDisposition(promptJob.id, "completed");
        }
        await this.client.answerCallbackQuery(callbackQuery.id, "Steered.");
        await this.client.editMessageText(chatId, messageId, `Sent as guidance to the active ${harness.displayName} turn.`);
      } catch (error) {
        if (promptJob) {
          this.turns.setPromptDisposition(promptJob.id, "pending");
        } else {
          this.turns.enqueueMessage(action.conversationId, prompt);
        }
        await this.client.answerCallbackQuery(callbackQuery.id, "Queued.");
        const message = error instanceof Error ? error.message : String(error);
        await this.client.editMessageText(chatId, messageId, `Steer failed; queued instead.\n\n${message}`, { format: "plain" });
      }
      return;
    }
    if (isGoalControlAction(action.kind)) {
      if (!harness.supportsGoals && !action.kind.endsWith(":close")) {
        await this.client.answerCallbackQuery(callbackQuery.id, `Goals are a Codex feature; ${harness.displayName} is active.`);
        return;
      }
      const acknowledged = !action.kind.endsWith(":close");
      if (acknowledged) {
        await this.client.answerCallbackQuery(callbackQuery.id, "Working...");
      }
      await handleGoalControlCallback({
        client: acknowledged ? clientAfterCallbackAck(this.client) : this.client,
        config: this.turns.configFor?.(action.conversationId) ?? this.config,
        store: this.store,
        action,
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
        runGoalTurn: (args) => this.turns.runGoalTurn(args),
        stopActiveTurn: async () => await interruptActiveTurn(this.activeQueries, action.conversationId),
        isTurnActive: this.activeQueries.has(action.conversationId),
      });
      return;
    }
    if (action.kind === "queue") {
      if (action.payload.jobId) {
        this.turns.setPromptDisposition(action.payload.jobId, "pending");
      } else {
        this.turns.enqueueMessage(action.conversationId, action.payload.prompt);
      }
      await this.client.answerCallbackQuery(callbackQuery.id, "Queued.");
      await this.client.editMessageText(chatId, messageId, `Queued. ${harness.displayName} will process this after the current task.`);
      return;
    }
    if (action.kind === "discard") {
      if (action.payload.jobId) {
        this.turns.setPromptDisposition(action.payload.jobId, "cancelled");
      }
      await this.client.answerCallbackQuery(callbackQuery.id, "Discarded.");
      await this.client.editMessageText(chatId, messageId, "Discarded.");
      return;
    }
    if (action.kind === "swerve") {
      if (action.payload.jobId) {
        this.store.setPromptJobDisposition(action.payload.jobId, "pending", 1);
      } else {
        this.turns.enqueueMessage(action.conversationId, action.payload.prompt, true);
      }
      const activeQuery = this.activeQueries.get(action.conversationId);
      if (activeQuery) {
        await activeQuery.abort("Telegram swerve");
      }
      if (action.payload.jobId) {
        void this.turns.scheduleConversation(action.conversationId);
      }
      await this.client.answerCallbackQuery(callbackQuery.id, "Swerving.");
      await this.client.editMessageText(chatId, messageId, `Swerving ${harness.displayName} to this message.`);
      return;
    }
    await this.client.answerCallbackQuery(callbackQuery.id, "Unknown action.");
  }
}
