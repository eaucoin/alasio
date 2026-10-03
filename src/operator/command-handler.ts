import { type ModelControlStore, sendModelPanel } from "./model-control.ts";
import type { AlasioConfig } from "../config.ts";
import {
  type ActiveQueries,
  type Harness,
  type HarnessRegistry,
  createHarnessRegistry,
  interruptActiveTurn,
  resolveHarnessName,
  resolveWorkingDirectory,
} from "../harness/index.ts";
import type { SqliteStore } from "../persistence/store.ts";
import type { ChatId, Client } from "../telegram/client.ts";
import { type OperatorCommand, parseCommand } from "./command-parser.ts";
import { type GoalControlStore, type RunGoalTurn, handleGoalTextCommand } from "./goal-control.ts";
import { type ServiceControlStore, type SwitchHarness, handleServiceTextCommand, sendChooseServicePanel } from "./service-control.ts";
import {
  type CreateWorkspace,
  type SwitchWorkspace,
  type WorkspaceControlStore,
  handleWorkspaceTextCommand,
  sendChooseWorkspacePanel,
} from "./workspace-control.ts";
import { type SessionControlStore, type StartNewSession, sendCurrentSessionPanel, sendSessionsPanel } from "./session-control.ts";
import { truncateText } from "./text.ts";
import { formatRewindForTelegram, formatSessionsForTelegram } from "./session-replies.ts";

/** A turn on the conversation's mounted session, started by a command (`!resume <#> <prompt>`). */
export interface CommandTurnRequest {
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
  readonly prompt: string;
}

/** Runs a turn on the conversation's mounted session; what it resolves to is not read. */
export type RunCommandTurn = (request: CommandTurnRequest) => Promise<unknown>;

/** The store, as the commands and the panels they open read and change it. */
export type CommandStore = ModelControlStore
  & ServiceControlStore
  & WorkspaceControlStore
  & SessionControlStore
  & GoalControlStore
  & Pick<SqliteStore, "getActiveHarness" | "getWorkingDirectory">;

/**
 * What the command handler is built with: the Telegram client, the store, and the
 * host's (the turn controller's) operations that commands start.
 */
export interface CommandHandlerOptions {
  readonly client: Pick<Client, "sendMessage" | "editMessageText">;
  readonly config: Pick<AlasioConfig, "workspaceRoot"> & Partial<Pick<AlasioConfig, "workingDirectory">>;
  readonly store: CommandStore;
  readonly activeQueries: ActiveQueries;
  readonly harnesses?: HarnessRegistry | null | undefined;
  readonly runCodexTurn: RunCommandTurn;
  readonly runGoalTurn: RunGoalTurn;
  readonly startNewSession: StartNewSession;
  /** Without it, /service says switching is not available. */
  readonly switchHarness?: SwitchHarness | null | undefined;
  /** Without it or createWorkspace, /workspace says selection is not available. */
  readonly switchWorkspace?: SwitchWorkspace | null | undefined;
  readonly createWorkspace?: CreateWorkspace | null | undefined;
  /** Whether the workspace panel offers new session filesystems. */
  readonly sandboxEnabled?: boolean | undefined;
}

/** A prompt that may be a command; one with files attached never is. */
export interface CommandText {
  readonly text: string;
  readonly filePaths: readonly string[];
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

/** A parsed command, and the conversation and message it came in. */
export interface CommandRequest {
  readonly cmd: OperatorCommand;
  readonly conversationId: string;
  readonly chatId: ChatId;
  readonly messageId: number;
}

function shortSessionId(sessionId: string): string {
  return sessionId ? sessionId.slice(0, 8) : "-";
}

export class CommandHandler {
  private readonly client: CommandHandlerOptions["client"];
  private readonly config: CommandHandlerOptions["config"];
  private readonly store: CommandStore;
  private readonly activeQueries: ActiveQueries;
  private readonly harnesses: HarnessRegistry;
  private readonly runCodexTurn: RunCommandTurn;
  private readonly runGoalTurn: RunGoalTurn;
  private readonly startNewSession: StartNewSession;
  private readonly switchHarness: SwitchHarness | null;
  private readonly switchWorkspace: SwitchWorkspace | null;
  private readonly createWorkspace: CreateWorkspace | null;
  private readonly sandboxEnabled: boolean;

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
    sandboxEnabled = false,
  }: CommandHandlerOptions) {
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
    this.sandboxEnabled = sandboxEnabled;
  }

  harnessFor(conversationId: string): Harness | null {
    return this.harnesses.forConversation(this.store, conversationId);
  }

  /** Handles `text` if it is a command; resolves to whether it was. */
  async handleTextCommand({ text, filePaths, conversationId, chatId, messageId }: CommandText): Promise<boolean> {
    const cmd = parseCommand(text);
    if (!cmd || filePaths.length > 0) {
      return false;
    }
    return await this.handleCommand({ cmd, conversationId, chatId, messageId });
  }

  /** Handles a parsed command; resolves to whether it was one this handler knows. */
  async handleCommand({ cmd, conversationId, chatId, messageId }: CommandRequest): Promise<boolean> {
    const harnessName = resolveHarnessName(this.store, conversationId);
    const harness = this.harnessFor(conversationId);
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
    if (cmd.type === "model") {
      await sendModelPanel({ client: this.client, store: this.store, harness, conversationId, chatId });
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
              sandboxEnabled: this.sandboxEnabled,
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
        sandboxEnabled: this.sandboxEnabled,
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
        sandboxEnabled: this.sandboxEnabled,
      });
      return true;
    }
    const sessions = harness.sessions;
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
      // A harness that supports goals has them; the check on goals only narrows.
      if (!harness.supportsGoals || !harness.goals) {
        await this.client.sendMessage(chatId, `Goals are a Codex feature. ${harness.displayName} is active; use /service codex to switch back.`);
        return true;
      }
      await handleGoalTextCommand({
        client: this.client,
        goalApi: harness.goals,
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
      const forkedId = await sessions.createForkedSession(sessionId, target.uuid, { threadKey: conversationId });
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
