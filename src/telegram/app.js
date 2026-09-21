import { TurnController } from "../codex/turn-controller.js";
import { CODEX_HARNESS, createHarnessRegistry } from "../harness/index.js";
import { HOOK_SERVER_PORT } from "../shared/runtime-constants.js";
import { SqliteStore } from "../persistence/store.js";
import { Authorizer } from "./authorizer.js";
import { CallbackHandler } from "./callback-handler.js";
import { Client } from "./client.js";
import { MediaGroupBuffer } from "./media-group-buffer.js";
import { MessageHandler } from "./message-handler.js";
import { UpdatePoller } from "./update-poller.js";
import { TelegramOutbox } from "./outbox.js";
import { startWorkflowHookServer } from "../workflow/hook-server.js";
import { createLogger } from "../shared/log.js";

const log = createLogger("telegram-app");

export class TelegramCodexApp {
  constructor(config) {
    this.config = config;
    this.client = new Client(config.telegramBotToken);
    this.store = new SqliteStore(config.workingDirectory);
    this.outbox = new TelegramOutbox({ client: this.client, store: this.store, log });
    this.activeQueries = new Map();
    this.workflowWaits = new Map();
    this.workflowWakeEvents = new Map();
    this.hookServer = null;
    this.isStopping = false;
    this.authorizer = new Authorizer({
      allowedUserIds: config.allowedUserIds,
      store: this.store,
      log,
    });
    this.harnesses = config.harnesses ?? createHarnessRegistry({ config });
    this.turns = new TurnController({
      config: this.config,
      client: this.client,
      store: this.store,
      outbox: this.outbox,
      activeQueries: this.activeQueries,
      workflowWaits: this.workflowWaits,
      workflowWakeEvents: this.workflowWakeEvents,
      isStopping: () => this.isStopping,
      harnesses: this.harnesses,
    });
    this.callbacks = new CallbackHandler({
      authorizer: this.authorizer,
      client: this.client,
      config: this.config,
      store: this.store,
      turns: this.turns,
      activeQueries: this.activeQueries,
    });
    this.mediaGroups = new MediaGroupBuffer({
      store: this.store,
      turns: this.turns,
      log,
    });
    this.completedResponseRecoveryTimer = null;
    this.messages = new MessageHandler({
      authorizer: this.authorizer,
      client: this.client,
      store: this.store,
      turns: this.turns,
      mediaGroups: this.mediaGroups,
      log,
    });
    this.poller = new UpdatePoller({
      client: this.client,
      store: this.store,
      processUpdate: (update) => this.processUpdate(update),
      log,
    });
  }

  async start() {
    const me = await this.client.getMe();
    log.info(`Starting Telegram alasio bot as @${me.username ?? me.id}`);
    log.info(`  Working directory: ${this.config.workingDirectory}`);
    log.info(`  Default service: ${this.config.defaultHarness ?? CODEX_HARNESS}`);
    await this.client.deleteWebhook(false);
    await this.configureNativeCommands();
    this.startHookServer();
    this.turns.reconcilePersistentState();
    await this.turns.flushCompletedResponses();
    this.outbox.start();
    await this.mediaGroups.flushDue();
    await this.turns.recoverInterruptedTurns();
    this.turns.resumePendingPrompts();
    this.poller.start();
    this.startCompletedResponseRecovery();
    this.warmLinkedSessions().catch((error) => {
      log.warn(`Linked Codex session warmup failed: ${error instanceof Error ? error.message : String(error)}`);
    });
  }

  async stop() {
    this.isStopping = true;
    this.mediaGroups.stop();
    if (this.completedResponseRecoveryTimer) {
      clearInterval(this.completedResponseRecoveryTimer);
      this.completedResponseRecoveryTimer = null;
    }
    this.outbox.stop();
    this.turns.recordExternalRestartEventsForActiveTurns();
    if (this.hookServer) {
      await new Promise((resolve) => this.hookServer.close(resolve));
      this.hookServer = null;
    }
    await this.poller.stop();
    this.harnesses.shutdownAll();
    this.store.close();
  }

  async configureNativeCommands() {
    try {
      await this.client.setMyCommands([
        { command: "service", description: "Switch between Codex and Claude Code" },
        { command: "session", description: "Manage the mounted agent session" },
        { command: "sessions", description: "Browse and mount agent sessions" },
        { command: "goal", description: "View or set the mounted session goal (Codex)" },
        { command: "stop", description: "Interrupt the active turn" },
      ]);
      await this.client.setChatMenuButton({ type: "commands" });
    } catch (error) {
      log.warn(`Failed to configure Telegram native commands: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  startCompletedResponseRecovery() {
    if (this.completedResponseRecoveryTimer) {
      return;
    }
    this.completedResponseRecoveryTimer = setInterval(() => {
      this.turns.flushCompletedResponses().catch((error) => {
        log.warn(`Completed response recovery failed: ${error instanceof Error ? error.message : String(error)}`);
      });
    }, 30_000);
    this.completedResponseRecoveryTimer.unref?.();
  }

  async warmLinkedSessions() {
    if (!this.config.warmLinkedSessions) {
      log.info("Skipping session warmup because warmLinkedSessions is false");
      return;
    }
    for (const harnessName of this.harnesses.names) {
      const harness = this.harnesses.get(harnessName);
      if (!harness.supportsWarmup) {
        continue;
      }
      const conversations = this.store.listConversationsWithSessions(harnessName);
      if (conversations.length === 0) {
        log.info(`No linked ${harness.displayName} sessions to warm`);
        continue;
      }
      for (const conversation of conversations) {
        const sessionId = conversation.session_id ?? conversation.codex_session_id;
        try {
          log.info(`Warming ${harness.displayName} session ${sessionId.slice(0, 8)} for ${conversation.id}`);
          await harness.warmSession({
            sessionId,
            threadKey: conversation.id,
            workingDirectory: this.config.workingDirectory,
          });
        } catch (error) {
          log.warn(`Failed to warm ${harness.displayName} session for ${conversation.id}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }
  }

  startHookServer() {
    this.hookServer = startWorkflowHookServer({
      port: HOOK_SERVER_PORT,
      store: this.store,
      workflowWaits: this.workflowWaits,
      workflowWakeEvents: this.workflowWakeEvents,
      log,
    });
  }

  async processUpdate(update) {
    this.store.recordTelegramUpdate(update);
    try {
      if (update.callback_query) {
        this.callbacks.handle(update.callback_query).catch((error) => {
          log.error(`Failed to process callback ${update.callback_query.id}: ${error instanceof Error ? error.message : String(error)}`);
        });
      } else if (update.message) {
        await this.messages.handle(update.message, update.update_id);
      }
      this.store.markTelegramUpdateProcessed(update.update_id);
    } catch (error) {
      log.error(`Failed to process update ${update.update_id}: ${error}`);
      throw error;
    }
  }
}
