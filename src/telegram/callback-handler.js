import { interruptCodexTurn } from "../codex/runtime.js";
import { handleGoalControlCallback, isGoalControlAction } from "../operator/goal-control.js";
import { handleSessionControlCallback, isSessionControlAction } from "../operator/session-control.js";

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
    const chatId = callbackQuery.message?.chat?.id;
    const messageId = callbackQuery.message?.message_id;
    if (!chatId || !messageId) {
      await this.client.answerCallbackQuery(callbackQuery.id, "Missing message context.");
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
        await this.client.editMessageText(chatId, messageId, "Codex is not ready to steer yet. Queued instead.");
        return;
      }
      try {
        await activeQuery.steer(prompt);
        if (promptJob) {
          this.turns.setPromptDisposition(promptJob.id, "completed");
        }
        await this.client.answerCallbackQuery(callbackQuery.id, "Steered.");
        await this.client.editMessageText(chatId, messageId, "Sent as guidance to the active Codex turn.");
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
      const acknowledged = !action.kind.endsWith(":close");
      if (acknowledged) {
        await this.client.answerCallbackQuery(callbackQuery.id, "Working...");
      }
      await handleGoalControlCallback({
        client: acknowledged ? clientAfterCallbackAck(this.client) : this.client,
        config: this.config,
        store: this.store,
        action,
        callbackQueryId: callbackQuery.id,
        chatId,
        messageId,
        runGoalTurn: (args) => this.turns.runGoalTurn(args),
        stopActiveTurn: async () => await interruptCodexTurn(this.activeQueries, action.conversationId),
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
      await this.client.editMessageText(chatId, messageId, "Queued. Codex will process this after the current task.");
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
      await this.client.editMessageText(chatId, messageId, "Swerving Codex to this message.");
      return;
    }
    await this.client.answerCallbackQuery(callbackQuery.id, "Unknown action.");
  }
}
