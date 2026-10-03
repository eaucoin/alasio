import type { Server } from "node:http";
import type { Update } from "@grammyjs/types";
import { TurnController } from "../codex/turn-controller.ts";
import type { CodexRollouts } from "../codex/rollouts/index.ts";
import { type SessionFsCodex, createSessionFsCodex, sessionFsCodexHome } from "../codex/sessionfs.ts";
import type { AlasioConfig } from "../config.ts";
import type { NeonSessionStore } from "../harness/claude/session-store.ts";
import { type AdoptedSession, adoptTranscripts } from "../harness/claude/transcripts.ts";
import type { KubeTemplates } from "../kube/config.ts";
import { type SessionFilesystems, createSandbox } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { type ActiveQueries, type HarnessRegistry, createHarnessRegistry } from "../harness/index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../harness/names.ts";
import { SqliteStore } from "../persistence/store.ts";
import { Authorizer } from "./authorizer.ts";
import { CallbackHandler } from "./callback-handler.ts";
import { Client } from "./client.ts";
import { MediaGroupBuffer } from "./media-group-buffer.ts";
import { MessageHandler } from "./message-handler.ts";
import { UpdatePoller } from "./update-poller.ts";
import { TelegramOutbox } from "./outbox.ts";
import { type WorkflowWait, type WorkflowWakeEvent, startWorkflowHookServer } from "../workflow/hook-server.ts";
import { createLogger } from "../shared/log.ts";
import { inSpan, SpanKind } from "../telemetry/index.ts";

const log = createLogger("telegram-app");

/** What the app is built from: alasio's configuration, and what main starts before it. */
export interface TelegramCodexAppConfig extends AlasioConfig {
  /** Claude Code's transcripts in Neon. */
  readonly sessionStore?: NeonSessionStore | null;
  /** Codex's rollouts in Neon, for the operator's Codex home and the session-filesystem one. */
  readonly codexRollouts?: CodexRollouts | null;
  readonly sessionFsCodexRollouts?: CodexRollouts | null;
  readonly kubeTemplates?: KubeTemplates | null;
  /** Stand-ins (test doubles) for what the app otherwise builds itself. */
  readonly sandbox?: SessionFilesystems | null;
  readonly sessionFsCodex?: SessionFsCodex | null;
  readonly harnesses?: HarnessRegistry | null;
}

export class TelegramCodexApp {
  readonly config: TelegramCodexAppConfig;
  readonly client: Client;
  readonly store: SqliteStore;
  readonly outbox: TelegramOutbox;
  readonly activeQueries: ActiveQueries;
  readonly workflowWaits: Map<string, WorkflowWait>;
  readonly workflowWakeEvents: Map<string, WorkflowWakeEvent>;
  private hookServer: Server | null;
  private isStopping: boolean;
  readonly authorizer: Authorizer;
  readonly sandbox: SessionFilesystems | null;
  readonly sessionFsCodex: SessionFsCodex | null;
  readonly harnesses: HarnessRegistry;
  readonly turns: TurnController;
  readonly callbacks: CallbackHandler;
  readonly mediaGroups: MediaGroupBuffer;
  private completedResponseRecoveryTimer: ReturnType<typeof setInterval> | null;
  readonly messages: MessageHandler;
  readonly poller: UpdatePoller;

  constructor(config: TelegramCodexAppConfig) {
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
    // Session filesystems, on when the deployment renders their template (createSandbox
    // returns null otherwise), with the Codex app-server that serves them.
    this.sandbox = config.sandbox ?? createSandbox({ templates: config.kubeTemplates ?? null, stateDir: config.stateDir });
    this.sessionFsCodex = config.sessionFsCodex
      ?? (this.sandbox ? createSessionFsCodex({ home: sessionFsCodexHome(config.stateDir) }) : null);
    this.harnesses = config.harnesses ?? createHarnessRegistry({
      config,
      sessionStore: config.sessionStore ?? null,
      codexRollouts: config.codexRollouts ?? null,
      sandbox: this.sandbox,
      sessionFsCodex: this.sessionFsCodex,
      sessionFsCodexRollouts: config.sessionFsCodexRollouts ?? null,
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
      sandbox: this.sandbox,
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

  async start(): Promise<void> {
    const me = await this.client.getMe();
    log.info(`Starting Telegram alasio bot as @${me.username ?? me.id}`);
    log.info(`  State database: ${this.config.dbPath}`);
    log.info(`  Workspace root: ${this.config.workspaceRoot}`);
    log.info(`  Default folder: ${this.config.workingDirectory ?? "none (operator chooses with /workspace)"}`);
    log.info(`  Default service: ${this.config.defaultHarness ?? "none (operator chooses with /service)"}`);
    log.info(`  Hook port: ${this.config.hookPort}`);
    log.info(`  Session filesystems: ${this.sandbox ? "enabled" : "off (the deployment renders no sessions template)"}`);
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
    this.warmLinkedSessions().catch((error: unknown) => {
      log.warn(`Linked Codex session warmup failed: ${error instanceof Error ? error.message : String(error)}`);
    });
  }

  /**
   * Brings every Claude session alasio points at into the session store before
   * any turn resumes one: imported whole the first time, then reconciled.
   */
  async adoptClaudeTranscripts(): Promise<void> {
    const sessionStore = this.config.sessionStore;
    if (!sessionStore) {
      return;
    }
    const sessions = this.store.listHarnessSessionReferences(CLAUDE_HARNESS)
      .map(({ sessionId, workingDirectory }) => ({ sessionId, workingDirectory: this.harnessDirectoryOf(workingDirectory) }))
      .filter((session): session is AdoptedSession => Boolean(session.workingDirectory));
    log.info(`  Session store: adopting ${sessions.length} Claude session(s)`);
    await adoptTranscripts({ store: sessionStore, sessions });
  }

  /**
   * The directory a workspace's harness runs in: a folder itself, or a session
   * filesystem's harness directory; null for a session filesystem this deployment does
   * not enable.
   */
  harnessDirectoryOf(workingDirectory: string): string | null {
    const workspace = parseWorkspace(workingDirectory);
    if (workspace?.kind !== "sessionfs") return workingDirectory;
    return this.sandbox?.harnessDirectory(workspace.volumeId) ?? null;
  }

  /**
   * Writes back from Neon every Codex rollout file a thread alasio points at
   * needs and this machine lacks, before any turn resumes one.
   */
  async restoreCodexRollouts(): Promise<void> {
    const references = this.store.listHarnessSessionReferences(CODEX_HARNESS);
    // Each thread goes back to the Codex home it runs from: the operator's for a folder,
    // the session-filesystem app-server's for a session filesystem.
    for (const [rollouts, sessionFs] of [[this.config.codexRollouts, false], [this.config.sessionFsCodexRollouts, true]] as const) {
      if (!rollouts) continue;
      const threadIds = references
        .filter(({ workingDirectory }) => (parseWorkspace(workingDirectory)?.kind === "sessionfs") === sessionFs)
        .map(({ sessionId }) => sessionId);
      const written = await rollouts.restore(threadIds);
      log.info(`  Rollout store${sessionFs ? " (session filesystems)" : ""}: ${threadIds.length} Codex thread(s), ${written.length} rollout file(s) written back`);
    }
  }

  async stop(): Promise<void> {
    this.isStopping = true;
    this.mediaGroups.stop();
    if (this.completedResponseRecoveryTimer) {
      clearInterval(this.completedResponseRecoveryTimer);
      this.completedResponseRecoveryTimer = null;
    }
    this.outbox.stop();
    this.turns.recordExternalRestartEventsForActiveTurns();
    const { hookServer } = this;
    if (hookServer) {
      await new Promise<Error | undefined>((resolve) => hookServer.close(resolve));
      this.hookServer = null;
    }
    await this.poller.stop();
    await this.harnesses.shutdownAll();
    await this.sessionFsCodex?.stop();
    await this.sandbox?.close();
    this.store.close();
  }

  async configureNativeCommands(): Promise<void> {
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

  startCompletedResponseRecovery(): void {
    if (this.completedResponseRecoveryTimer) {
      return;
    }
    this.completedResponseRecoveryTimer = setInterval(() => {
      this.turns.flushCompletedResponses().catch((error: unknown) => {
        log.warn(`Completed response recovery failed: ${error instanceof Error ? error.message : String(error)}`);
      });
    }, 30_000);
    this.completedResponseRecoveryTimer.unref?.();
  }

  async warmLinkedSessions(): Promise<void> {
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

  startHookServer(): void {
    this.hookServer = startWorkflowHookServer({
      port: this.config.hookPort,
      store: this.store,
      workflowWaits: this.workflowWaits,
      workflowWakeEvents: this.workflowWakeEvents,
      log,
    });
  }

  /**
   * Each update starts a trace of its own, which everything it leads to joins: the
   * turn its prompt queues, however much later it runs, and the reply's delivery.
   */
  async processUpdate(update: Update): Promise<void> {
    this.store.recordTelegramUpdate(update);
    const chatId = update.message?.chat?.id ?? update.callback_query?.message?.chat?.id;
    await inSpan("alasio.update", {
      kind: SpanKind.CONSUMER,
      parent: null,
      attributes: {
        "telegram.update.id": update.update_id,
        "telegram.update.type": update.callback_query ? "callback_query" : "message",
        ...(chatId === undefined ? {} : { "telegram.chat.id": String(chatId) }),
      },
    }, async () => {
      try {
        const callbackQuery = update.callback_query;
        if (callbackQuery) {
          this.callbacks.handle(callbackQuery).catch((error: unknown) => {
            log.error(`Failed to process callback ${callbackQuery.id}: ${error instanceof Error ? error.message : String(error)}`);
          });
        } else if (update.message) {
          await this.messages.handle(update.message, update.update_id);
        }
        this.store.markTelegramUpdateProcessed(update.update_id);
      } catch (error) {
        log.error(`Failed to process update ${update.update_id}: ${error}`);
        throw error;
      }
    });
  }
}
