/**
 * alasio serving Telegram: as it starts, what the last alasio left is settled (prompt jobs,
 * undelivered responses, albums, interrupted turns) and what turns resume is brought
 * back (Claude Code's transcripts, Codex's rollouts); then Telegram's updates are polled
 * and processed, undelivered responses are looked for every thirty seconds, what the
 * store keeps only in flight is pruned daily, and linked sessions are warmed. All of it
 * stops as its scope closes, polling first.
 */
import type { Update } from "@grammyjs/types";
import { Effect, FiberSet, Layer, Option, Schedule, type Scope } from "effect";

import { Turns } from "../codex/turns.ts";
import type { KeptCodexRollouts } from "../codex/rollouts/index.ts";
import type { RolloutRestoreError } from "../codex/rollouts/restore.ts";
import type { AlasioConfig } from "../config.ts";
import { type AdoptedSession, adoptTranscripts, type TranscriptAdoptionError } from "../harness/claude/transcripts.ts";
import type { NeonSessionStore } from "../harness/claude/session-store.ts";
import { Harnesses } from "../harness/index.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS } from "../harness/names.ts";
import type { CommandError, OperatorServices } from "../operator/command-handler.ts";
import { Mounts } from "../operator/mounts.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import { withLogScope } from "../shared/log.ts";
import { SpanKind, withAlasioSpan } from "../telemetry/index.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { Authorizer } from "./authorizer.ts";
import { handleCallbackQuery } from "./callback-handler.ts";
import { TelegramClient, type TelegramError } from "./client.ts";
import { MediaGroups } from "./media-group-buffer.ts";
import { handleMessage, processIncomingPrompt } from "./message-handler.ts";
import { pollUpdates } from "./update-poller.ts";

/** How often completed responses that were never delivered are looked for. */
const COMPLETED_RESPONSE_RECOVERY = "30 seconds";

/**
 * How long the store keeps what it holds only while in flight (updates processed, albums
 * handled, replies delivered) after it lands; conversations, their messages, files,
 * prompts, responses and the actions of buttons not yet pressed are kept.
 */
const TRANSIENT_RETENTION = "7 days";
/** How often what outlived TRANSIENT_RETENTION is pruned. */
const PRUNE_EVERY = "1 day";

/** The bot's commands, as Telegram's menu offers them. */
const NATIVE_COMMANDS = [
  { command: "service", description: "Switch between Codex and Claude" },
  { command: "model", description: "Choose the model and effort" },
  { command: "workspace", description: "Choose, create or fork the workspace to work in" },
  { command: "session", description: "Manage the mounted agent session" },
  { command: "sessions", description: "Browse and mount agent sessions" },
  { command: "goal", description: "View or set the mounted session goal (Codex)" },
  { command: "stop", description: "Interrupt the active turn" },
];

/** What the app is made with: alasio's configuration, and what main keeps in Neon. */
export interface TelegramAppConfig
  extends Pick<AlasioConfig, "workspaceRoot" | "workingDirectory" | "defaultHarness" | "hookPort" | "warmLinkedSessions">
{
  /** Claude Code's transcripts in Neon. */
  readonly sessionStore?: NeonSessionStore | null | undefined;
  /** Codex's rollouts in Neon, for the operator's Codex home and the session-filesystem one. */
  readonly codexRollouts?: KeptCodexRollouts | null | undefined;
  readonly sessionFsCodexRollouts?: KeptCodexRollouts | null | undefined;
}

/** What the app runs on. */
export type TelegramAppServices = Authorizer | MediaGroups | OperatorServices;

/** How the app fails to start. */
export type TelegramAppError = TelegramError | CommandError | TranscriptAdoptionError | RolloutRestoreError | StoreError;

/**
 * The app, started in its layer's scope: what it starts with runs before it serves, and
 * what it runs (polling, recovery, warmup) is interrupted as the scope closes.
 */
export const serveTelegram = (config: TelegramAppConfig): Layer.Layer<never, TelegramAppError, TelegramAppServices> =>
  Layer.effectDiscard(startTelegram(config));

const startTelegram = Effect.fnUntraced(function*(config: TelegramAppConfig): Effect.fn.Return<
  void,
  TelegramAppError,
  TelegramAppServices | Scope.Scope
> {
  const client = yield* TelegramClient;
  const store = yield* Store;
  const turns = yield* Turns;
  const harnesses = yield* Harnesses;
  const mediaGroups = yield* MediaGroups;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  // What an update is processed with, given to it as the poller hands it over.
  const services = yield* Effect.context<TelegramAppServices>();
  // What an update leads to that outlasts it (a prompt, a button's control), stopped as the app stops.
  const handling = yield* FiberSet.make();

  /**
   * The directory a workspace's harness runs in: a folder itself, or a session
   * filesystem's harness directory; null for a session filesystem this deployment does
   * not enable.
   */
  const harnessDirectoryOf = (workingDirectory: string): string | null => {
    const workspace = parseWorkspace(workingDirectory);
    if (workspace?.kind !== "sessionfs") return workingDirectory;
    return sandbox?.harnessDirectory(workspace.volumeId) ?? null;
  };

  /**
   * Brings every Claude session alasio points at into the session store before
   * any turn resumes one: imported whole the first time, then reconciled.
   */
  const adoptClaudeTranscripts = Effect.fnUntraced(function*(sessionStore: NeonSessionStore) {
    const sessions = (yield* store.listHarnessSessionReferences(CLAUDE_HARNESS))
      .map(({ sessionId, workingDirectory }) => ({ sessionId, workingDirectory: harnessDirectoryOf(workingDirectory) }))
      .filter((session): session is AdoptedSession => Boolean(session.workingDirectory));
    yield* Effect.logInfo(`  Session store: adopting ${sessions.length} Claude session(s)`);
    yield* adoptTranscripts({ store: sessionStore, sessions });
  });

  /**
   * Writes back from Neon every Codex rollout file a thread alasio points at
   * needs and this machine lacks, before any turn resumes one.
   */
  const restoreCodexRollouts = Effect.fnUntraced(function*() {
    const references = yield* store.listHarnessSessionReferences(CODEX_HARNESS);
    // Each thread goes back to the Codex home it runs from: the operator's for a folder,
    // the session-filesystem app-server's for a session filesystem.
    for (const [rollouts, sessionFs] of [[config.codexRollouts, false], [config.sessionFsCodexRollouts, true]] as const) {
      if (!rollouts) continue;
      const threadIds = references
        .filter(({ workingDirectory }) => (parseWorkspace(workingDirectory)?.kind === "sessionfs") === sessionFs)
        .map(({ sessionId }) => sessionId);
      const written = yield* rollouts.restore(threadIds);
      yield* Effect.logInfo(`  Rollout store${sessionFs ? " (session filesystems)" : ""}: ${threadIds.length} Codex thread(s), ${written.length} rollout file(s) written back`);
    }
  });

  /** Loads each linked session ahead of its next turn, for the harnesses that can. */
  const warmLinkedSessions = Effect.gen(function*() {
    if (!config.warmLinkedSessions) {
      yield* Effect.logInfo("Skipping session warmup because warmLinkedSessions is false");
      return;
    }
    for (const harnessName of harnesses.names) {
      const conversations = yield* store.listConversationsWithSessions(harnessName);
      if (conversations.length === 0) {
        yield* Effect.logInfo(`No linked ${harnessName} sessions to warm`);
        continue;
      }
      for (const conversation of conversations) {
        const sessionId = conversation.session_id;
        const harness = yield* harnesses.getFor(harnessName, conversation.working_directory);
        if (!harness.supportsWarmup) {
          break;
        }
        yield* Effect.logInfo(`Warming ${harness.displayName} session ${sessionId.slice(0, 8)} for ${conversation.id} in ${conversation.working_directory}`);
        yield* harness.warmSession({ sessionId, threadKey: conversation.id, workingDirectory: conversation.working_directory }).pipe(
          Effect.catch((error) => Effect.logWarning(`Failed to warm ${harness.displayName} session for ${conversation.id}: ${error.message}`)),
        );
      }
    }
  }).pipe(Effect.catch((error) => Effect.logWarning(`Linked session warmup failed: ${error.message}`)));

  /**
   * Each update starts a trace of its own, which everything it leads to joins: the
   * turn its prompt queues, however much later it runs, and the reply's delivery.
   * What it leads to runs on after it, so that polling goes on meanwhile.
   */
  const processUpdate = (update: Update): Effect.Effect<void, TelegramError | StoreError> =>
    Effect.suspend(() => {
      const chatId = update.message?.chat?.id ?? update.callback_query?.message?.chat?.id;
      return Effect.gen(function*() {
        yield* store.recordTelegramUpdate(update);
        const callbackQuery = update.callback_query;
        if (callbackQuery) {
          yield* FiberSet.run(handling, handleCallbackQuery(callbackQuery).pipe(
            Effect.catch((error) => Effect.logError(`Failed to process callback ${callbackQuery.id}: ${error.message}`)),
            Effect.provideContext(services),
            Effect.interruptible,
          ));
        } else if (update.message) {
          const prompt = yield* handleMessage(update.message, update.update_id).pipe(Effect.provideContext(services));
          if (prompt) {
            yield* FiberSet.run(handling, processIncomingPrompt(prompt).pipe(Effect.provideContext(services), Effect.interruptible));
          }
        }
        yield* store.markTelegramUpdateProcessed(update.update_id);
      }).pipe(
        Effect.tapError((error) => Effect.logError(`Failed to process update ${update.update_id}: ${error}`)),
        withAlasioSpan("alasio.update", {
          kind: SpanKind.CONSUMER,
          parent: null,
          attributes: {
            "telegram.update.id": update.update_id,
            "telegram.update.type": update.callback_query ? "callback_query" : "message",
            ...(chatId === undefined ? {} : { "telegram.chat.id": String(chatId) }),
          },
        }),
      );
    });

  const me = yield* client.getMe;
  yield* Effect.logInfo(`Starting Telegram alasio bot as @${me.username ?? me.id}`);
  yield* Effect.logInfo(`  Workspace root: ${config.workspaceRoot}`);
  yield* Effect.logInfo(`  Default folder: ${config.workingDirectory ?? "none (operator chooses with /workspace)"}`);
  yield* Effect.logInfo(`  Default service: ${config.defaultHarness ?? "none (operator chooses with /service)"}`);
  yield* Effect.logInfo(`  Hook port: ${config.hookPort}`);
  yield* Effect.logInfo(`  Session filesystems: ${sandbox ? "enabled" : "off (the deployment renders no sessions template)"}`);
  yield* client.deleteWebhook(false);
  yield* client.setMyCommands(NATIVE_COMMANDS).pipe(
    Effect.andThen(client.setChatMenuButton({ type: "commands" })),
    Effect.catch((error) => Effect.logWarning(`Failed to configure Telegram native commands: ${error.message}`)),
  );
  yield* turns.reconcilePersistentState;
  // Before anything can make another, the session filesystems a crash left unmade go.
  yield* Effect.flatMap(Mounts, (mounts) => mounts.reconcileSessionWorkspaces);
  yield* turns.flushCompletedResponses;
  yield* mediaGroups.flushDue;
  if (config.sessionStore) {
    yield* adoptClaudeTranscripts(config.sessionStore);
  }
  yield* restoreCodexRollouts();
  yield* turns.recoverInterruptedTurns;
  yield* turns.resumePendingPrompts;
  yield* pollUpdates(processUpdate).pipe(Effect.forkScoped);
  // Responses completed but never delivered (their delivery failed, or alasio stopped first) go out.
  yield* turns.flushCompletedResponses.pipe(
    Effect.catch((error) => Effect.logWarning(`Completed response recovery failed: ${error.message}`)),
    Effect.catchDefect((defect) => Effect.logWarning(`Completed response recovery failed: ${defect instanceof Error ? defect.message : String(defect)}`)),
    Effect.schedule(Schedule.spaced(COMPLETED_RESPONSE_RECOVERY)),
    Effect.forkScoped,
  );
  // Pruned as alasio starts, and every day it runs after.
  yield* store.pruneTransient(TRANSIENT_RETENTION).pipe(
    Effect.catch((error) => Effect.logWarning(`Pruning the store failed: ${error.message}`)),
    Effect.repeat(Schedule.spaced(PRUNE_EVERY)),
    Effect.forkScoped,
  );
  yield* Effect.forkScoped(warmLinkedSessions);
}, withLogScope("telegram-app"));
