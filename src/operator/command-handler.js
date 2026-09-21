import { interruptCodexTurn } from "../codex/runtime.js";
import { createForkedSession } from "../sessions/forking.js";
import {
  getSessionByNumber,
  getSessionLastMessage,
  getTotalRewindPages,
  getTotalSessionPages,
  listSessionMessages,
  listSessions,
} from "../sessions/index.js";
import { parseCommand } from "./command-parser.js";
import { handleGoalTextCommand } from "./goal-control.js";
import { sendCurrentSessionPanel, sendSessionsPanel } from "./session-control.js";
import { truncateText } from "./text.js";
import { formatRewindForTelegram, formatSessionsForTelegram } from "./session-replies.js";

function shortSessionId(sessionId) {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

export class CommandHandler {
  constructor({ client, config, store, activeQueries, runCodexTurn, runGoalTurn, startNewSession }) {
    this.client = client;
    this.config = config;
    this.store = store;
    this.activeQueries = activeQueries;
    this.runCodexTurn = runCodexTurn;
    this.runGoalTurn = runGoalTurn;
    this.startNewSession = startNewSession;
  }

  async handleTextCommand({ text, filePaths, conversationId, chatId, messageId }) {
    const cmd = parseCommand(text);
    if (!cmd || filePaths.length > 0) {
      return false;
    }
    return await this.handleCommand({ cmd, conversationId, chatId, messageId });
  }

  async handleCommand({ cmd, conversationId, chatId, messageId }) {
    if (cmd.type === "stop") {
      if (!this.activeQueries.has(conversationId)) {
        await this.client.sendMessage(chatId, "No active query to stop.");
        return true;
      }
      const [status] = await this.client.sendMessage(chatId, "Stopping Codex...");
      const interrupted = await interruptCodexTurn(this.activeQueries, conversationId);
      const text = interrupted ? "Codex stopped." : "No active query to stop.";
      if (status?.message_id) {
        await this.client.editMessageText(chatId, status.message_id, text, { format: "plain" }).catch(() => undefined);
      } else {
        await this.client.sendMessage(chatId, text);
      }
      return true;
    }
    if (cmd.type === "sessions") {
      const sessions = listSessions(cmd.page);
      const totalPages = getTotalSessionPages();
      await this.client.sendMessage(chatId, formatSessionsForTelegram(sessions, cmd.page, totalPages));
      return true;
    }
    if (cmd.type === "sessions_panel") {
      await sendSessionsPanel({
        client: this.client,
        store: this.store,
        conversationId,
        chatId,
      });
      return true;
    }
    if (cmd.type === "session_panel") {
      await sendCurrentSessionPanel({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        conversationId,
        chatId,
      });
      return true;
    }
    if (cmd.type === "goal") {
      await handleGoalTextCommand({
        client: this.client,
        config: this.config,
        store: this.store,
        conversationId,
        chatId,
        messageId,
        args: cmd.args,
        runGoalTurn: this.runGoalTurn,
        stopActiveTurn: async () => await interruptCodexTurn(this.activeQueries, conversationId),
        startNewSession: this.startNewSession,
        isTurnActive: this.activeQueries.has(conversationId),
      });
      return true;
    }
    if (cmd.type === "sessions_new") {
      if (this.activeQueries.has(conversationId)) {
        await this.client.sendMessage(chatId, "Codex is currently working. Use /stop first, then /sessions new.");
        return true;
      }
      const sessionId = await this.startNewSession({ conversationId });
      await this.client.sendMessage(chatId, `New session mounted: ${shortSessionId(sessionId)}. Send your next message to start a turn.`);
      return true;
    }
    if (cmd.type === "rewind_list") {
      const sessionId = this.store.getSessionId(conversationId);
      if (!sessionId) {
        await this.client.sendMessage(chatId, "No session linked to this Telegram conversation. Use !resume <#> first.");
        return true;
      }
      const messages = listSessionMessages(sessionId);
      const totalPages = getTotalRewindPages(sessionId);
      await this.client.sendMessage(chatId, formatRewindForTelegram(messages, cmd.page, totalPages));
      return true;
    }
    if (cmd.type === "rewind_exec") {
      const sessionId = this.store.getSessionId(conversationId);
      if (!sessionId) {
        await this.client.sendMessage(chatId, "No session linked. Use !resume <#> first.");
        return true;
      }
      const messages = listSessionMessages(sessionId);
      const target = messages.find((message) => message.index === cmd.index);
      if (!target) {
        await this.client.sendMessage(chatId, `Message ${cmd.index} not found. Use !rewind to see available points.`);
        return true;
      }
      const forkedId = createForkedSession(sessionId, target.uuid);
      if (!forkedId) {
        await this.client.sendMessage(chatId, "Failed to create forked session.");
        return true;
      }
      this.store.setSessionId(conversationId, forkedId);
      await this.client.sendMessage(chatId, `Rewound to before message ${cmd.index}:\n\n${truncateText(target.text, 700)}\n\nReady to continue from earlier state.`);
      return true;
    }
    if (cmd.type === "resume") {
      let resolvedSessionId;
      if (/^\d+$/.test(cmd.ref)) {
        resolvedSessionId = getSessionByNumber(Number.parseInt(cmd.ref, 10));
        if (!resolvedSessionId) {
          await this.client.sendMessage(chatId, `Session #${cmd.ref} not found. Use !sessions to see available sessions.`);
          return true;
        }
      } else {
        resolvedSessionId = cmd.ref;
      }
      this.store.setSessionId(conversationId, resolvedSessionId);
      if (cmd.followUp) {
        await this.runCodexTurn({
          conversationId,
          chatId,
          messageId,
          prompt: cmd.followUp,
        });
        return true;
      }
      const lastMessage = getSessionLastMessage(resolvedSessionId);
      await this.client.sendMessage(chatId, lastMessage ? `Resuming; most recent Codex message:\n\n${lastMessage}` : "Resuming session.");
      return true;
    }
    return false;
  }
}
