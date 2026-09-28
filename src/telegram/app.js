import { codexHome } from "../codex/env.js";
import { restoreRollouts } from "../codex/rollouts/restore.js";
import { TurnController } from "../codex/turn-controller.js";
import { adoptTranscripts } from "../harness/claude/transcripts.js";
import { createHarnessRegistry } from "../harness/index.js";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../harness/names.js";
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
    this.store = new SqliteStore(config.stateDir, config.dbPath, { defaultWorkingDirectory: config.workingDirectory });
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
    this.harnesses = config.harnesses ?? createHarnessRegistry({
      config,
      sessionStore: config.sessionStore ?? null,
      rolloutStore: config.rolloutStore ?? null,
    });
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
    log.info(`  State database: ${this.config.dbPath}`);
    log.info(`  Workspace root: ${this.config.workspaceRoot}`);
    log.info(`  Default folder: ${this.config.workingDirectory ?? "none (operator chooses with /workspace)"}`);
    log.info(`  Default service: ${this.config.defaultHarness ?? "none (operator chooses with /service)"}`);
    log.info(`  Hook port: ${this.config.hookPort}`);
    await this.client.deleteWebhook(false);
    await this.configureNativeCommands();
    this.startHookServer();
    this.turns.reconcilePersistentState();
    await this.turns.flushCompletedResponses();
    this.outbox.start();
    await this.mediaGroups.flushDue();
    await this.adoptClaudeTranscripts();
    await this.restoreCodexRollouts();
    await this.turns.recoverInterruptedTurns();
    this.turns.resumePendingPrompts();
    this.poller.start();
    this.startCompletedResponseRecovery();
    this.warmLinkedSessions().catch((error) => {
      log.warn(`Linked Codex session warmup failed: ${error instanceof Error ? error.message : String(error)}`);
    });
  }

  /**
   * Brings every Claude session alasio points at into the session store before
   * any turn resumes one: imported whole the first time, then reconciled.
   */
  async adoptClaudeTranscripts() {
    const sessionStore = this.config.sessionStore;
    if (!sessionStore) {
      return;
    }
    const sessions = this.store.listHarnessSessionReferences(CLAUDE_HARNESS);
    log.info(`  Session store: adopting ${sessions.length} Claude session(s)`);
    await adoptTranscripts({ store: sessionStore, sessions });
  }

  /**
   * Writes back from the rollout store every Codex rollout file a thread alasio
   * points at needs and this machine lacks, before any turn resumes one.
   */
  async restoreCodexRollouts() {
    const rolloutStore = this.config.rolloutStore;
    if (!rolloutStore) {
      return;
    }
    const threadIds = this.store.listHarnessSessionReferences(CODEX_HARNESS).map(({ sessionId }) => sessionId);
    const written = await restoreRollouts({ store: rolloutStore, threadIds, home: codexHome() });
    log.info(`  Rollout store: ${threadIds.length} Codex thread(s), ${written.length} rollout file(s) written back`);
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
        { command: "service", description: "Switch between Codex and Claude" },
        { command: "model", description: "Choose the model and effort" },
        { command: "workspace", description: "Choose or create the folder to work in" },
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
      const conversations = this.store.listConversationsWithSessions(harnessName);
      if (conversations.length === 0) {
        log.info(`No linked ${harnessName} sessions to warm`);
        continue;
      }
      for (const conversation of conversations) {
        const sessionId = conversation.session_id ?? conversation.codex_session_id;
        const harness = this.harnesses.getFor(harnessName, conversation.working_directory);
        if (!harness.supportsWarmup) {
          break;
        }
        try {
          log.info(`Warming ${harness.displayName} session ${sessionId.slice(0, 8)} for ${conversation.id} in ${conversation.working_directory}`);
          await harness.warmSession({
            sessionId,
            threadKey: conversation.id,
            workingDirectory: conversation.working_directory,
          });
        } catch (error) {
          log.warn(`Failed to warm ${harness.displayName} session for ${conversation.id}: ${error instanceof Error ? error.message : String(error)}`);
        }
      }
    }
  }

  startHookServer() {
    this.hookServer = startWorkflowHookServer({
      port: this.config.hookPort,
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
