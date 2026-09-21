import { createHarnessRegistry, interruptActiveTurn, resolveHarnessName, resolveWorkingDirectory } from "../harness/index.js";
import { parseCommand } from "./command-parser.js";
import { handleGoalTextCommand } from "./goal-control.js";
import { handleServiceTextCommand, sendChooseServicePanel } from "./service-control.js";
import { handleWorkspaceTextCommand, sendChooseWorkspacePanel } from "./workspace-control.js";
import { sendCurrentSessionPanel, sendSessionsPanel } from "./session-control.js";
import { truncateText } from "./text.js";
import { formatRewindForTelegram, formatSessionsForTelegram } from "./session-replies.js";

function shortSessionId(sessionId) {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

export class CommandHandler {
  constructor({
    client,
    config,
    store,
    activeQueries,
    harnesses = null,
    runCodexTurn,
    runGoalTurn,
    startNewSession,
    switchHarness = null,
    switchWorkspace = null,
    createWorkspace = null,
  }) {
    this.client = client;
    this.config = config;
    this.store = store;
    this.activeQueries = activeQueries;
    this.harnesses = harnesses ?? createHarnessRegistry({ config });
    this.runCodexTurn = runCodexTurn;
    this.runGoalTurn = runGoalTurn;
    this.startNewSession = startNewSession;
    this.switchHarness = switchHarness;
    this.switchWorkspace = switchWorkspace;
    this.createWorkspace = createWorkspace;
  }

  configFor(conversationId) {
    return { ...this.config, workingDirectory: resolveWorkingDirectory(this.store, conversationId) };
  }

  harnessFor(conversationId) {
    return this.harnesses.forConversation(this.store, conversationId);
  }

  async handleTextCommand({ text, filePaths, conversationId, chatId, messageId }) {
    const cmd = parseCommand(text);
    if (!cmd || filePaths.length > 0) {
      return false;
    }
    return await this.handleCommand({ cmd, conversationId, chatId, messageId });
  }

  async handleCommand({ cmd, conversationId, chatId, messageId }) {
    const harnessName = resolveHarnessName(this.store, conversationId);
    const harness = this.harnessFor(conversationId);
    const sessions = harness?.sessions;
    const label = harness?.displayName ?? "The agent";
    if (cmd.type === "stop") {
      if (!this.activeQueries.has(conversationId)) {
        await this.client.sendMessage(chatId, "No active query to stop.");
        return true;
      }
      const [status] = await this.client.sendMessage(chatId, `Stopping ${label}...`);
      const interrupted = await interruptActiveTurn(this.activeQueries, conversationId);
      const text = interrupted ? `${label} stopped.` : "No active query to stop.";
      if (status?.message_id) {
        await this.client.editMessageText(chatId, status.message_id, text, { format: "plain" }).catch(() => undefined);
      } else {
        await this.client.sendMessage(chatId, text);
      }
      return true;
    }
    if (cmd.type === "service") {
      if (!this.switchHarness) {
        await this.client.sendMessage(chatId, "Service switching is not available in this deployment.");
        return true;
      }
      await handleServiceTextCommand({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        conversationId,
        chatId,
        target: cmd.target,
        switchHarness: this.switchHarness,
        onMounted: async () => {
          // Service first, then folder: chain straight into the folder picker.
          if (!resolveWorkingDirectory(this.store, conversationId)) {
            await sendChooseWorkspacePanel({
              client: this.client,
              store: this.store,
              activeQueries: this.activeQueries,
              conversationId,
              chatId,
              workspaceRoot: this.config.workspaceRoot,
            });
          }
        },
      });
      return true;
    }
    if (cmd.type === "workspace") {
      if (!this.switchWorkspace || !this.createWorkspace) {
        await this.client.sendMessage(chatId, "Workspace selection is not available in this deployment.");
        return true;
      }
      await handleWorkspaceTextCommand({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        conversationId,
        chatId,
        args: cmd.args,
        workspaceRoot: this.config.workspaceRoot,
        switchWorkspace: this.switchWorkspace,
        createWorkspace: this.createWorkspace,
      });
      return true;
    }
    if (!harnessName) {
      // Every remaining control acts on the mounted service's own sessions or turns.
      await sendChooseServicePanel({ client: this.client, store: this.store, activeQueries: this.activeQueries, conversationId, chatId });
      return true;
    }
    if (!harness) {
      await sendChooseWorkspacePanel({
        client: this.client,
        store: this.store,
        activeQueries: this.activeQueries,
        conversationId,
        chatId,
        workspaceRoot: this.config.workspaceRoot,
      });
      return true;
    }
    if (cmd.type === "sessions") {
      const sessionList = await sessions.listSessions(cmd.page);
      const totalPages = await sessions.getTotalSessionPages();
      await this.client.sendMessage(chatId, formatSessionsForTelegram(sessionList, cmd.page, totalPages));
      return true;
    }
    if (cmd.type === "sessions_panel") {
      await sendSessionsPanel({
        client: this.client,
        store: this.store,
        harness,
        conversationId,
        chatId,
      });
      return true;
    }
    if (cmd.type === "session_panel") {
      await sendCurrentSessionPanel({
        client: this.client,
        store: this.store,
        harness,
        activeQueries: this.activeQueries,
        conversationId,
        chatId,
      });
      return true;
    }
    if (cmd.type === "goal") {
      if (!harness.supportsGoals) {
        await this.client.sendMessage(chatId, `Goals are a Codex feature. ${harness.displayName} is active; use /service codex to switch back.`);
        return true;
      }
      await handleGoalTextCommand({
        client: this.client,
        config: this.configFor(conversationId),
        store: this.store,
        conversationId,
        chatId,
        messageId,
        args: cmd.args,
        runGoalTurn: this.runGoalTurn,
        stopActiveTurn: async () => await interruptActiveTurn(this.activeQueries, conversationId),
        startNewSession: this.startNewSession,
        isTurnActive: this.activeQueries.has(conversationId),
      });
      return true;
    }
    if (cmd.type === "sessions_new") {
      if (this.activeQueries.has(conversationId)) {
        await this.client.sendMessage(chatId, `${harness.displayName} is currently working. Use /stop first, then /sessions new.`);
        return true;
      }
      const sessionId = await this.startNewSession({ conversationId });
      await this.client.sendMessage(chatId, `New ${harness.displayName} session mounted: ${shortSessionId(sessionId)}. Send your next message to start a turn.`);
      return true;
    }
    if (cmd.type === "rewind_list") {
      const sessionId = this.store.getSessionId(conversationId);
      if (!sessionId) {
        await this.client.sendMessage(chatId, "No session linked to this Telegram conversation. Use !resume <#> first.");
        return true;
      }
      const messages = await sessions.listSessionMessages(sessionId);
      const totalPages = await sessions.getTotalRewindPages(sessionId);
      await this.client.sendMessage(chatId, formatRewindForTelegram(messages, cmd.page, totalPages));
      return true;
    }
    if (cmd.type === "rewind_exec") {
      const sessionId = this.store.getSessionId(conversationId);
      if (!sessionId) {
        await this.client.sendMessage(chatId, "No session linked. Use !resume <#> first.");
        return true;
      }
      const messages = await sessions.listSessionMessages(sessionId);
      const target = messages.find((message) => message.index === cmd.index);
      if (!target) {
        await this.client.sendMessage(chatId, `Message ${cmd.index} not found. Use !rewind to see available points.`);
        return true;
      }
      const forkedId = await sessions.createForkedSession(sessionId, target.uuid);
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
        resolvedSessionId = await sessions.getSessionByNumber(Number.parseInt(cmd.ref, 10));
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
      const lastMessage = await sessions.getSessionLastMessage(resolvedSessionId);
      await this.client.sendMessage(chatId, lastMessage ? `Resuming; most recent ${harness.displayName} message:\n\n${lastMessage}` : "Resuming session.");
      return true;
    }
    return false;
  }
}
