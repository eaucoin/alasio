import type { Update } from "@grammyjs/types";
import { Fiber } from "effect";
import { TurnController } from "../codex/turn-controller.ts";
import type { KeptCodexRollouts } from "../codex/rollouts/index.ts";
import type { AlasioConfig } from "../config.ts";
import type { NeonSessionStore } from "../harness/claude/session-store.ts";
import { type AdoptedSession, adoptTranscripts } from "../harness/claude/transcripts.ts";
import type { KubeTemplates } from "../kube/config.ts";
import type { ClaudeQueryFactory } from "../harness/claude/runtime.ts";
import type { FolderBayma } from "../mcp/bayma.ts";
import { type SessionFilesystems, sessionFilesystemsFacade } from "../sandbox/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import type { ActiveTurnsFacade } from "../harness/active-turns.ts";
import type { HarnessesFacade } from "../harness/index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../harness/names.ts";
import { type SqliteStore, Store } from "../persistence/store.ts";
import { Authorizer } from "./authorizer.ts";
import { CallbackHandler } from "./callback-handler.ts";
import { type Client, telegramClientFacade } from "./client.ts";
import { MediaGroupBuffer } from "./media-group-buffer.ts";
import { MessageHandler } from "./message-handler.ts";
import { pollUpdates } from "./update-poller.ts";
import { type TelegramOutbox, outboxFacade } from "./outbox.ts";
import { type WorkflowWait, type WorkflowWakeEvent, WorkflowHooks } from "../workflow/hook-server.ts";
import type { AlasioEffects } from "../alasio.ts";
import { createLogger } from "../shared/log.ts";
import { inSpan, SpanKind } from "../telemetry/index.ts";

const log = createLogger("telegram-app");

/** What the app is built from: alasio's configuration, and what main starts before it. */
export interface TelegramCodexAppConfig extends AlasioConfig {
  /** Claude Code's transcripts in Neon. */
  readonly sessionStore?: NeonSessionStore | null;
  /** Codex's rollouts in Neon, for the operator's Codex home and the session-filesystem one. */
  readonly codexRollouts?: KeptCodexRollouts | null;
  readonly sessionFsCodexRollouts?: KeptCodexRollouts | null;
  readonly kubeTemplates?: KubeTemplates | null;
  /** Stand-ins for a folder workspace's bayma and for Claude Code, in the harnesses alasio makes (src/alasio.ts). */
  readonly folderBayma?: FolderBayma | undefined;
  readonly claudeQueryFactory?: ClaudeQueryFactory | undefined;
  /** What runs the effects of alasio's services for the app (src/alasio.ts). */
  readonly effects: AlasioEffects;
}

export class TelegramCodexApp {
  readonly config: TelegramCodexAppConfig;
  readonly client: Client;
  readonly store: SqliteStore;
  readonly outbox: TelegramOutbox;
  readonly activeTurns: ActiveTurnsFacade;
  readonly workflowWaits: ReadonlyMap<string, WorkflowWait>;
  readonly workflowWakeEvents: ReadonlyMap<string, WorkflowWakeEvent>;
  readonly authorizer: Authorizer;
  readonly sandbox: SessionFilesystems | null;
  readonly harnesses: HarnessesFacade;
  readonly turns: TurnController;
  readonly callbacks: CallbackHandler;
  readonly mediaGroups: MediaGroupBuffer;
  readonly messages: MessageHandler;
  /** Polling Telegram for updates, from start to stop. */
  private poller: Fiber.Fiber<never> | null;

  constructor(config: TelegramCodexAppConfig) {
    this.config = config;
    this.client = telegramClientFacade(config.effects);
    this.store = config.effects.runSync(Store);
    this.outbox = outboxFacade(config.effects);
    const workflowHooks = config.effects.runSync(WorkflowHooks);
    this.workflowWaits = workflowHooks.waits;
    this.workflowWakeEvents = workflowHooks.wakeEvents;
    this.authorizer = new Authorizer({
      allowedUserIds: config.allowedUserIds,
      store: this.store,
      log,
    });
    // Session filesystems, on when the deployment renders their template (alasio then
    // has SessionSandboxes, and SessionFsCodex for the Codex app-server that serves them).
    this.sandbox = sessionFilesystemsFacade(config.effects);
    // The turns, and the harnesses and running turns the operator's controls reach, are
    // alasio's services (src/alasio.ts); the app reaches them through the turns' façade.
    this.turns = new TurnController({
      config: this.config,
      client: this.client,
      store: this.store,
      effects: config.effects,
      sandbox: this.sandbox,
    });
    this.harnesses = this.turns.harnesses;
    this.activeTurns = this.turns.activeTurns;
    this.callbacks = new CallbackHandler({
      authorizer: this.authorizer,
      client: this.client,
      config: this.config,
      store: this.store,
      turns: this.turns,
      activeTurns: this.activeTurns,
    });
    this.mediaGroups = new MediaGroupBuffer({
      store: this.store,
      turns: this.turns,
      log,
    });
    this.messages = new MessageHandler({
      authorizer: this.authorizer,
      client: this.client,
      store: this.store,
      turns: this.turns,
      mediaGroups: this.mediaGroups,
      log,
    });
    this.poller = null;
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
    this.turns.reconcilePersistentState();
    await this.turns.flushCompletedResponses();
    await this.mediaGroups.flushDue();
    await this.adoptClaudeTranscripts();
    await this.restoreCodexRollouts();
    await this.turns.recoverInterruptedTurns();
    this.turns.resumePendingPrompts();
    this.poller = this.config.effects.runFork(pollUpdates((update) => this.processUpdate(update)));
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
      const written = await this.config.effects.runPromise(rollouts.restore(threadIds));
      log.info(`  Rollout store${sessionFs ? " (session filesystems)" : ""}: ${threadIds.length} Codex thread(s), ${written.length} rollout file(s) written back`);
    }
  }

  /**
   * Stops taking updates. The turns running then are interrupted after, as alasio's
   * services stop (src/alasio.ts), while the store and Telegram are still open.
   */
  async stop(): Promise<void> {
    this.mediaGroups.stop();
    const { poller } = this;
    if (poller) {
      await this.config.effects.runPromise(Fiber.interrupt(poller));
      this.poller = null;
    }
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
