import { Clock, Context, Deferred, Effect, Fiber, FiberMap, FiberSet, HashMap, Layer, Option, Ref, Schedule, Schema } from "effect";

import {
  type AttachedTurn,
  type Harness,
  type HarnessError,
  type HarnessFacade,
  Harnesses,
  type HarnessesFacade,
  type HarnessUnavailable,
  NO_WORKSPACE_MOUNTED,
  type NoServiceMounted,
  NoWorkspaceMounted,
  harnessDisplayName,
  harnessesFacade,
  isHarnessName,
  resolveHarnessName,
  resolveWorkingDirectory,
} from "../harness/index.ts";
import { ActiveTurns, type ActiveTurnsFacade, activeTurnsFacade } from "../harness/active-turns.ts";
import { errorsIn, type ResponseBlock } from "./event-projection.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";
import { buildFilePromptSuffix } from "../shared/file-prompt.ts";
import { CommandHandler } from "../operator/command-handler.ts";
import type { GoalTurnRequest } from "../operator/goal-control.ts";
import { type HarnessSwitch, sendChooseServicePanel } from "../operator/service-control.ts";
import { type WorkspaceChange, sendChooseWorkspacePanel } from "../operator/workspace-control.ts";
import type { AlasioConfig } from "../config.ts";
import type { PromptJob, PromptJobState } from "../persistence/prompt-job-repository.ts";
import { type SqliteStore, Store } from "../persistence/store.ts";
import { type NetMode, type SessionFilesystems, SessionSandboxes } from "../sandbox/index.ts";
import type { EffectRunner } from "../shared/effects.ts";
import { type ChatId, type Client, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { Outbox } from "../telegram/outbox.ts";
import { WorkflowHooks } from "../workflow/hook-server.ts";
import { createWorkspace, resolveWorkspacePath } from "../workspace/policy.ts";
import { sessionFsWorkspace } from "../workspace/kind.ts";
import { newVolumeId } from "../sandbox/names.ts";
import { truncateText } from "../operator/text.ts";
import { makeReplyMedia } from "./reply-media.ts";
import { recordExternalRestartEvent, recoverInterruptedTurns } from "./restart-recovery.ts";
import { makeStatusReporter } from "./status-reporter.ts";
import { createLogger, withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";

const LOG_SCOPE = "codex-turn-controller";
/** The scope's lines, for the operator controls not yet written in Effect. */
const log = createLogger(LOG_SCOPE);

const turnDuration = meter.createHistogram("alasio.turn.duration", {
  description: "Time from a turn starting to its reply being queued for delivery, by harness and outcome",
  unit: "s",
});
const runningTurns = meter.createUpDownCounter("alasio.turn.active", {
  description: "Turns running now, by harness",
  unit: "{turn}",
});
const promptWait = meter.createHistogram("alasio.prompt.wait", {
  description: "Time a queued prompt waited for its conversation to be free, restarts included",
  unit: "s",
});

/** How often completed responses that were never delivered are looked for. */
const COMPLETED_RESPONSE_RECOVERY = "30 seconds";

/**
 * The configuration the controller reads: the workspace root, the pre-mounted folder if
 * any, and the state directory a reply's media are copied under, without which replies
 * go as text only.
 */
export type TurnControllerConfig = Pick<AlasioConfig, "workspaceRoot"> & Partial<Pick<AlasioConfig, "workingDirectory" | "stateDir">>;

/** A conversation and the chat it is in, as the setup pickers are sent to. */
export interface ConversationChat {
  readonly conversationId: string;
  readonly chatId: ChatId;
}

/** A prompt as it arrives from Telegram: its text, and the files sent with it. */
export interface IncomingPrompt extends ConversationChat {
  readonly messageId: number;
  readonly text: string;
  readonly filePaths: readonly string[];
}

/** An operator's prompt to queue as a prompt job: its text as sent, and the prompt it makes. */
export interface QueuedPrompt extends ConversationChat {
  readonly messageId: number;
  readonly prompt: string;
  readonly filePaths: readonly string[];
  /** What the operator wrote, as the concurrent prompt's question quotes it. */
  readonly visibleText: string;
}

/** A turn to run on the conversation's mounted session. */
export interface TurnRequest extends ConversationChat {
  readonly messageId: number | string;
  readonly prompt: string;
  /** The prompt job the turn runs, if it runs one. */
  readonly jobId?: string | null | undefined;
  /** The trace to continue: a queued prompt's, or null for a trace of its own; the active span when absent. */
  readonly traceparent?: string | null | undefined;
}

/** A turn on a given session, attached to a turn already running upstream or not. */
interface SessionTurn extends TurnRequest {
  readonly existingSession: string | null;
  readonly attachedTurn: AttachedTurn | null;
}

/** How a turn ended: completed, without its answer, interrupted by the operator, or stopped by alasio stopping. */
export type TurnOutcome = "completed" | "incomplete" | "interrupted" | "stopped";

/**
 * What a concurrent prompt's buttons (steer, queue, swerve, discard) carry: its prompt
 * job, when one was queued, and its prompt. A type rather than an interface, so that it
 * is a CallbackPayload.
 */
export type ConcurrentPromptPayload = {
  readonly jobId: string | null;
  readonly prompt: string;
};

/** A goal turn asked of a harness that has no goals. */
export class GoalTurnsUnsupported extends Schema.TaggedError<GoalTurnsUnsupported>()("GoalTurnsUnsupported", {
  harness: Schema.String,
}) {
  override get message(): string {
    return `${this.harness} does not support attached goal turns.`;
  }
}

/** What needs the conversation free was asked while a turn runs in it. */
export class ConversationBusy extends Schema.TaggedError<ConversationBusy>()("ConversationBusy", {
  message: Schema.String,
}) {}

/** Why a turn could not run: no harness to run it on, or one that cannot run it. */
export type TurnError = NoServiceMounted | HarnessUnavailable | GoalTurnsUnsupported;

/** What a turn that ended without its answer shows: that it did not complete, and why, when the harness said. */
function notCompleted(harnessName: string, blocks: readonly ResponseBlock[]): string {
  const error = errorsIn(blocks).at(-1);
  return error ? `${harnessName} did not complete: ${error}` : `${harnessName} did not complete.`;
}

/** The mounted service's name as the operator reads it, or "No service". */
function harnessLabelOf(store: SqliteStore, conversationId: string): string {
  const name = resolveHarnessName(store, conversationId);
  return name ? harnessDisplayName(name) : "No service";
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * alasio's turns: each conversation's prompt jobs drained one at a time by a worker of its
 * own, each turn from its status message to its reply, a turn's queued messages as the
 * turn after it, and what a restart left (interrupted turns, undelivered responses).
 * Stopping alasio interrupts every running turn while the store and Telegram are still
 * open: the restart is recorded for the turn to continue after it, and its status
 * message says so.
 */
export class Turns extends Context.Service<Turns, {
  /**
   * Queues an operator's prompt as a prompt job: run as soon as the conversation is free,
   * or, while a turn runs in it, held for the operator to say what to do with it.
   */
  readonly submit: (prompt: QueuedPrompt) => Effect.Effect<void, TelegramError>;
  /** Keeps a message (a concurrent prompt's, steered or swerved without a job) for the turn after the current one. */
  readonly enqueueMessage: (conversationId: string, prompt: string, front?: boolean) => Effect.Effect<void>;
  /** Makes sure the conversation's worker is draining its prompt jobs. */
  readonly schedule: (conversationId: string) => Effect.Effect<void>;
  /** Settles what becomes of a prompt job; a job made pending again is scheduled. The job as it is now. */
  readonly setPromptDisposition: (jobId: string, state: PromptJobState, priority?: number) => Effect.Effect<PromptJob | null>;
  /** Runs a turn on the conversation's mounted session now, then its queued messages: whether its response completed. */
  readonly run: (request: TurnRequest) => Effect.Effect<boolean, TurnError>;
  /** Runs a goal's turn on its session, or asks what to do with it while a turn runs. */
  readonly runGoalTurn: (request: GoalTurnRequest) => Effect.Effect<boolean, TurnError | TelegramError>;
  /** A new, empty session of the mounted service, mounted on the conversation: its id. */
  readonly startNewSession: (conversationId: string) => Effect.Effect<string, NoServiceMounted | HarnessUnavailable | ConversationBusy | HarnessError>;
  /** Settles, as alasio starts, the prompt jobs the last alasio left running. */
  readonly reconcilePersistentState: Effect.Effect<void>;
  /** Queues every completed response that was never delivered. */
  readonly flushCompletedResponses: Effect.Effect<void>;
  /** Continues, or lets go of, the turns the last alasio was running when it stopped. */
  readonly recoverInterruptedTurns: Effect.Effect<void>;
  /** Schedules every conversation with prompt jobs waiting. */
  readonly resumePendingPrompts: Effect.Effect<void>;
}>()("alasio/codex/Turns") {
  static readonly layer = (config: Partial<Pick<AlasioConfig, "stateDir">> = {}): Layer.Layer<
    Turns,
    never,
    Store | TelegramClient | Outbox | WorkflowHooks | Harnesses | ActiveTurns
  > => Layer.effect(Turns, makeTurns(config));
}

const makeTurns = Effect.fnUntraced(function*({ stateDir }: Partial<Pick<AlasioConfig, "stateDir">>) {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const harnesses = yield* Harnesses;
  const activeTurns = yield* ActiveTurns;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  const status = yield* makeStatusReporter({
    // Media a response shows are copied under the state directory until delivered.
    replyMedia: stateDir
      ? makeReplyMedia({
        stateDir,
        workspaceForChat: (chatId) => {
          const conversation = store.getConversationByChatId(chatId);
          return conversation ? resolveWorkingDirectory(store, conversation.id) : null;
        },
        sandbox,
      })
      : null,
  });
  // Each conversation's prompt worker, and the turns run outside one (a command's, a
  // goal's): interrupted, each turn left for after the restart, as alasio stops.
  const workers = yield* FiberMap.make<string>();
  const directTurns = yield* FiberSet.make<boolean, TurnError>();
  const queuedMessages = yield* Ref.make(HashMap.empty<string, readonly string[]>());

  const flushCompletedResponses = status.flushCompletedResponses.pipe(withLogScope(LOG_SCOPE));

  /**
   * One turn through `harness`, from its status message to its reply: its outcome, and
   * whether its response completed. Interrupted, as alasio stops, the turn is left for
   * after the restart: the restart recorded for it to continue then, or its completed
   * response for delivery, and its status message saying so.
   */
  const runTurn = Effect.fnUntraced(function*(
    harness: Harness,
    { conversationId, chatId, messageId, prompt, existingSession, attachedTurn, jobId = null }: Omit<SessionTurn, "traceparent">,
  ): Effect.fn.Return<{ readonly outcome: TurnOutcome; readonly completed: boolean }, GoalTurnsUnsupported | NoWorkspaceMounted> {
    if (attachedTurn && !harness.supportsGoals) {
      return yield* new GoalTurnsUnsupported({ harness: harness.displayName });
    }
    const workingDirectory = resolveWorkingDirectory(store, conversationId);
    if (!workingDirectory) {
      return yield* new NoWorkspaceMounted();
    }
    store.upsertActiveTurn({
      conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      sessionId: existingSession ?? null,
      harness: harness.name,
      pendingResponseId: null,
      prompt,
      startedAt: (yield* Clock.currentTimeMillis) / 1000,
    });
    let responseCompleted: boolean | null = null;
    return yield* Effect.scoped(Effect.gen(function*() {
      const posted = yield* status.statusUpdates({ chatId, sessionId: existingSession ?? null, harnessName: harness.displayName });

      /** Leaves the turn, as alasio stops, for after the restart. */
      const leaveForRestart = Effect.gen(function*() {
        const turn = store.getActiveTurns().find((active) => active.thread_key === conversationId);
        if (!turn) {
          // The turn had already let go of the conversation.
          return;
        }
        const pendingResponseId = turn.pending_response_id;
        const completed = responseCompleted
          ?? (pendingResponseId !== null && store.getCompletedResponsesPendingDelivery().some((response) => response.id === pendingResponseId));
        if (completed) {
          store.clearActiveTurn(conversationId, pendingResponseId);
          store.clearRestartEvent(conversationId);
          yield* Effect.logInfo(`Leaving completed response ${pendingResponseId} for post-restart delivery`);
        } else {
          yield* recordExternalRestartEvent(store, conversationId);
          yield* Effect.logInfo(`Leaving active turn ${conversationId} for post-restart recovery because the service is stopping`);
        }
        yield* status.restarting(chatId, yield* Deferred.await(posted), harness.displayName);
      });

      return yield* Effect.gen(function*() {
        const queryResult = yield* harness.runTurn({
          prompt,
          resumeSession: existingSession ?? null,
          threadKey: conversationId,
          chatId: String(chatId),
          messageId: String(messageId),
          workingDirectory,
          persistence: store,
          attachedTurn,
          onPromptDispatched: () => {
            if (jobId) {
              store.markPromptJobDispatched(jobId);
            }
          },
          onTransportStarted: ({ sessionId, turnId }) => {
            if (jobId) {
              store.markPromptJobUpstreamStarted(jobId, sessionId, turnId);
            }
          },
          onTransportCompleted: ({ sessionId, turnId }) => {
            if (jobId) {
              store.markPromptJobUpstreamCompleted(jobId, sessionId, turnId);
            }
          },
          // A harness that keeps running between prompts (Claude Code background work)
          // produces replies of its own and frees the conversation when they finish.
          onBackgroundResponse: flushCompletedResponses.pipe(
            Effect.catchDefect((defect) => Effect.logWarning(`Background response delivery deferred for ${conversationId}: ${errorText(defect)}`)),
            withLogScope(LOG_SCOPE),
          ),
          onIdle: schedule(conversationId),
        });
        const { blockSequence, sessionId: newSessionId, pendingResponseId, interrupted } = queryResult;
        responseCompleted = queryResult.responseCompleted;
        const shown = yield* Deferred.await(posted);
        if (newSessionId && !existingSession) {
          store.setSessionId(conversationId, newSessionId);
          yield* Effect.annotateCurrentSpan("alasio.session.id", newSessionId);
        }
        if (interrupted) {
          yield* status.finishWithoutResponse({ chatId, pendingResponseId, status: shown, harnessName: harness.displayName });
          store.clearActiveTurn(conversationId, pendingResponseId);
          store.clearRestartEvent(conversationId);
          return { outcome: "interrupted", completed: false } as const;
        }
        if (!responseCompleted) {
          yield* status.finishWithoutResponse({
            chatId,
            pendingResponseId,
            status: shown,
            statusText: notCompleted(harness.displayName, blockSequence),
          });
          store.clearActiveTurn(conversationId, pendingResponseId);
          store.clearRestartEvent(conversationId);
          return { outcome: "incomplete", completed: false } as const;
        }
        yield* Effect.suspend(() =>
          status.postResponse({ chatId, response: finalResponseToMarkdown(blockSequence), pendingResponseId, status: shown, harnessName: harness.displayName })
        ).pipe(
          Effect.catchDefect((defect) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${errorText(defect)}`)),
          Effect.ensuring(Effect.sync(() => {
            store.clearActiveTurn(conversationId, pendingResponseId);
            store.clearRestartEvent(conversationId);
          })),
        );
        return { outcome: "completed", completed: true } as const;
      }).pipe(Effect.onInterrupt(() => leaveForRestart));
    }));
  });

  /**
   * Runs a turn, then the messages queued while it ran, as a turn of their own. The turn
   * is the span `alasio.turn`, continuing `traceparent` when given (a queued prompt's;
   * null for a trace of its own) and the active span otherwise; its outcome labels it
   * and its duration.
   */
  const runSessionTurn = (turn: SessionTurn): Effect.Effect<boolean, TurnError> =>
    Effect.gen(function*() {
      const harness = yield* harnesses.requireForConversation(turn.conversationId);
      const labels = { "alasio.harness": harness.name };
      const startedAt = yield* Clock.currentTimeMillis;
      let outcome: TurnOutcome | "failed" = "failed";
      runningTurns.add(1, labels);
      const { completed } = yield* runTurn(harness, turn).pipe(
        Effect.tap((settled) => Effect.sync(() => {
          outcome = settled.outcome;
        })),
        Effect.onInterrupt(() => Effect.sync(() => {
          outcome = "stopped";
        })),
        Effect.onExit(() => Effect.suspend(() => Effect.annotateCurrentSpan("alasio.turn.outcome", outcome))),
        withAlasioSpan("alasio.turn", {
          parent: turn.traceparent,
          attributes: {
            ...labels,
            "alasio.conversation.id": turn.conversationId,
            "telegram.chat.id": String(turn.chatId),
            ...(turn.jobId ? { "alasio.prompt_job.id": turn.jobId } : {}),
            ...(turn.existingSession ? { "alasio.session.id": turn.existingSession } : {}),
          },
        }),
        Effect.onExit(() => Effect.gen(function*() {
          runningTurns.add(-1, labels);
          turnDuration.record(((yield* Clock.currentTimeMillis) - startedAt) / 1000, { ...labels, "alasio.turn.outcome": outcome });
        })),
      );
      const queued = yield* Ref.modify(queuedMessages, (all) => [Option.getOrElse(HashMap.get(all, turn.conversationId), () => []), HashMap.remove(all, turn.conversationId)]);
      if (queued.length > 0) {
        yield* run({
          conversationId: turn.conversationId,
          chatId: turn.chatId,
          messageId: turn.messageId,
          prompt: queued.join("\n\n---\n\n"),
          traceparent: null,
        });
      }
      return completed;
    }).pipe(withLogScope(LOG_SCOPE));

  /** A turn on the conversation's mounted session, in the trace `traceparent` names (null: one of its own). */
  const run = (request: TurnRequest & { readonly traceparent: string | null }): Effect.Effect<boolean, TurnError> =>
    Effect.suspend(() =>
      runSessionTurn({
        ...request,
        existingSession: store.getSessionId(request.conversationId) ?? null,
        attachedTurn: null,
      })
    );

  /** A turn run outside a worker, as a fiber of the service's, for alasio's stopping to reach. */
  const runDirect = (turn: Effect.Effect<boolean, TurnError>): Effect.Effect<boolean, TurnError> =>
    Effect.flatMap(FiberSet.run(directTurns, turn), Fiber.join);

  /** The conversation's prompt jobs, one at a time, for as long as it is free and has any. */
  const drain = (conversationId: string): Effect.Effect<void, NoServiceMounted | HarnessUnavailable> =>
    Effect.gen(function*() {
      /** Whatever a job's turn failed with, the job fails with it and the operator is told why. */
      const failJob = (job: PromptJob, error: unknown) =>
        Effect.suspend(() => {
          store.failPromptJob(job.id, error);
          return client.sendMessage(job.chat_id, `${harnessLabelOf(store, conversationId)} hit an error: ${errorText(error)}`).pipe(Effect.ignore);
        });
      while (!(yield* activeTurns.isBusy(conversationId))) {
        const job = store.claimNextPromptJob(conversationId);
        if (!job) {
          return;
        }
        const activeHarness = (yield* harnesses.requireForConversation(conversationId)).name;
        if (job.harness && job.harness !== activeHarness) {
          yield* Effect.logWarning(`Prompt job ${job.id} was admitted under ${job.harness} but ${activeHarness} is active; running under ${activeHarness}`);
        }
        // Claiming a job stamps its start, so a claimed job's started_at is set.
        promptWait.record(job.started_at! - job.created_at, { "alasio.harness": activeHarness });
        yield* run({
          conversationId,
          chatId: job.chat_id,
          messageId: job.message_id,
          prompt: job.prompt,
          jobId: job.id,
          traceparent: job.traceparent,
        }).pipe(
          Effect.flatMap((completed) => Effect.sync(() => store.setPromptJobDisposition(job.id, completed ? "completed" : "cancelled"))),
          Effect.catch((error) => failJob(job, error)),
          Effect.catchDefect((defect) => failJob(job, defect)),
        );
      }
    });

  // A worker scheduled once alasio is stopping is not run: the map is closed.
  const schedule = (conversationId: string): Effect.Effect<void> =>
    FiberMap.run(workers, conversationId, drain(conversationId).pipe(
      Effect.catch((error) => Effect.logError(`Prompt worker for ${conversationId} stopped: ${error.message}`)),
      withLogScope(LOG_SCOPE),
    ), { onlyIfMissing: true }).pipe(Effect.exit, Effect.asVoid);

  const askHowToHandleConcurrentPrompt = Effect.fnUntraced(function*({ conversationId, chatId, job, visibleText }: ConversationChat & {
    readonly job: Pick<PromptJob, "id" | "prompt">;
    readonly visibleText: string;
  }): Effect.fn.Return<void, TelegramError> {
    const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
    const queueAction = store.createCallbackAction({ conversationId, kind: "queue", payload });
    const steerAction = store.createCallbackAction({ conversationId, kind: "steer", payload });
    const swerveAction = store.createCallbackAction({ conversationId, kind: "swerve", payload });
    const discardAction = store.createCallbackAction({ conversationId, kind: "discard", payload });
    yield* client.sendMessage(chatId, `${harnessLabelOf(store, conversationId)} is currently working. What should I do with this message?\n\n${truncateText(visibleText, 220)}`, {
      reply_markup: {
        inline_keyboard: [[
          { text: "Steer", callback_data: steerAction },
          { text: "Queue", callback_data: queueAction },
        ], [
          { text: "Swerve", callback_data: swerveAction },
          { text: "Discard", callback_data: discardAction },
        ]],
      },
    });
  });

  // Responses completed but never delivered (their delivery failed, or alasio stopped first) go out.
  yield* flushCompletedResponses.pipe(
    Effect.catchDefect((defect) => Effect.logWarning(`Completed response recovery failed: ${errorText(defect)}`).pipe(withLogScope("telegram-app"))),
    Effect.schedule(Schedule.spaced(COMPLETED_RESPONSE_RECOVERY)),
    Effect.forkScoped,
  );

  return Turns.of({
    submit: Effect.fnUntraced(function*({ conversationId, chatId, messageId, prompt, filePaths, visibleText }: QueuedPrompt) {
      const job = store.enqueuePromptJob({
        conversationId,
        chatId,
        messageId,
        prompt,
        filePaths,
        state: (yield* activeTurns.isBusy(conversationId)) ? "awaiting_choice" : "pending",
        traceparent: currentTraceparent(),
      });
      if (job.state === "awaiting_choice") {
        yield* askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText });
        return;
      }
      yield* schedule(conversationId);
    }),
    enqueueMessage: (conversationId, prompt, front = false) =>
      Ref.update(queuedMessages, (all) => {
        const queue = Option.getOrElse(HashMap.get(all, conversationId), () => []);
        return HashMap.set(all, conversationId, front ? [prompt, ...queue] : [...queue, prompt]);
      }),
    schedule,
    setPromptDisposition: (jobId, state, priority = 0) =>
      Effect.gen(function*() {
        store.setPromptJobDisposition(jobId, state, priority);
        const job = store.getPromptJob(jobId);
        if (state === "pending" && job) {
          yield* schedule(job.conversation_id);
        }
        return job;
      }),
    // The trace a turn asked for without one is the asker's, read before the turn is forked.
    run: (request) => Effect.suspend(() => runDirect(run({ ...request, traceparent: request.traceparent === undefined ? currentTraceparent() : request.traceparent }))),
    runGoalTurn: ({ conversationId, chatId, messageId, sessionId, turnId, prompt }) =>
      Effect.gen(function*() {
        if (yield* activeTurns.isBusy(conversationId)) {
          const job = store.enqueuePromptJob({ conversationId, chatId, messageId, prompt, state: "awaiting_choice" });
          yield* askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText: prompt });
          return true;
        }
        store.setSessionId(conversationId, sessionId);
        yield* runDirect(runSessionTurn({
          conversationId,
          chatId,
          messageId,
          prompt,
          existingSession: sessionId,
          attachedTurn: turnId ? { sessionId, turnId } : null,
          traceparent: currentTraceparent(),
        }));
        return true;
      }),
    startNewSession: (conversationId) =>
      Effect.gen(function*() {
        const harness = yield* harnesses.requireForConversation(conversationId);
        if (yield* activeTurns.isBusy(conversationId)) {
          return yield* new ConversationBusy({ message: `${harness.displayName} is currently working. Stop the active turn before starting a new session.` });
        }
        const workingDirectory = resolveWorkingDirectory(store, conversationId);
        if (!workingDirectory) {
          return yield* new NoWorkspaceMounted();
        }
        const sessionId = yield* harness.startFreshSession({ threadKey: conversationId, workingDirectory });
        store.setSessionId(conversationId, sessionId);
        return sessionId;
      }),
    reconcilePersistentState: Effect.sync(() => {
      const completedConversations = store.recoverPromptJobsAfterRestart();
      for (const conversationId of completedConversations) {
        store.clearActiveTurn(conversationId);
        store.clearRestartEvent(conversationId);
      }
    }),
    flushCompletedResponses,
    recoverInterruptedTurns: recoverInterruptedTurns(store),
    resumePendingPrompts: Effect.suspend(() => Effect.forEach(store.listPendingPromptConversations(), schedule, { discard: true })),
  });
}, withLogScope(LOG_SCOPE));

/** What the turn controller façade runs its effects in: the turns, and what the operator's controls ask of the harnesses and running turns. */
export type TurnControllerServices = Turns | ActiveTurns | Harnesses;

export interface TurnControllerOptions {
  readonly config: TurnControllerConfig;
  readonly client: Pick<Client, "sendMessage" | "editMessageText">;
  readonly store: SqliteStore;
  readonly effects: EffectRunner<TurnControllerServices>;
  readonly sandbox?: SessionFilesystems | null;
}

/**
 * Turns as the code not yet written in Effect drives them (the message and callback
 * handlers, the media-group buffer, the operator's commands and controls, the app's
 * start): its effects as promises run through alasio's EffectRunner, and the operator's
 * service, workspace and session switches, which move with that code. It goes when its
 * last caller moves.
 */
export class TurnController {
  private readonly sandbox: SessionFilesystems | null;
  private readonly config: TurnControllerConfig;
  private readonly client: Pick<Client, "sendMessage" | "editMessageText">;
  private readonly store: SqliteStore;
  private readonly effects: EffectRunner<TurnControllerServices>;
  private readonly turns: Turns["Service"];
  readonly activeTurns: ActiveTurnsFacade;
  readonly harnesses: HarnessesFacade;
  private readonly commands: CommandHandler;

  constructor({ config, client, store, effects, sandbox = null }: TurnControllerOptions) {
    this.sandbox = sandbox;
    this.config = config;
    this.client = client;
    this.store = store;
    this.effects = effects;
    this.turns = effects.runSync(Turns);
    this.activeTurns = activeTurnsFacade(effects);
    this.harnesses = harnessesFacade(effects);
    this.commands = new CommandHandler({
      client,
      config,
      store,
      activeTurns: this.activeTurns,
      harnessFor: (conversationId) => this.harnessFor(conversationId),
      runCodexTurn: (args) => this.runCodexTurn(args),
      runGoalTurn: (args) => this.runGoalTurn(args),
      startNewSession: (args) => this.startNewSession(args),
      switchHarness: (args) => this.switchHarness(args),
      switchWorkspace: (args) => this.switchWorkspace(args),
      createWorkspace: (args) => this.createWorkspace(args),
      sandboxEnabled: this.sandboxEnabled,
    });
  }

  harnessFor(conversationId: string): HarnessFacade | null {
    return this.harnesses.forConversation(conversationId);
  }

  requireHarness(conversationId: string): HarnessFacade {
    return this.harnesses.requireForConversation(conversationId);
  }

  harnessLabel(conversationId: string): string {
    return harnessLabelOf(this.store, conversationId);
  }

  workingDirectoryFor(conversationId: string): string | null {
    return resolveWorkingDirectory(this.store, conversationId);
  }

  requireWorkingDirectory(conversationId: string): string {
    const workingDirectory = this.workingDirectoryFor(conversationId);
    if (!workingDirectory) {
      throw new Error(NO_WORKSPACE_MOUNTED);
    }
    return workingDirectory;
  }

  async sendChooseServicePanel({ conversationId, chatId }: ConversationChat): Promise<void> {
    await sendChooseServicePanel({
      client: this.client,
      store: this.store,
      activeTurns: this.activeTurns,
      conversationId,
      chatId,
    });
  }

  async sendChooseWorkspacePanel({ conversationId, chatId }: ConversationChat): Promise<void> {
    await sendChooseWorkspacePanel({
      client: this.client,
      store: this.store,
      activeTurns: this.activeTurns,
      conversationId,
      chatId,
      workspaceRoot: this.config.workspaceRoot,
      sandboxEnabled: this.sandboxEnabled,
    });
  }

  /**
   * Service first, then folder. Sends the picker for the first missing layer and
   * reports whether one was sent, so ingress can stop there.
   */
  async sendNextSetupStep({ conversationId, chatId }: ConversationChat): Promise<boolean> {
    if (!resolveHarnessName(this.store, conversationId)) {
      await this.sendChooseServicePanel({ conversationId, chatId });
      return true;
    }
    if (!this.workingDirectoryFor(conversationId)) {
      await this.sendChooseWorkspacePanel({ conversationId, chatId });
      return true;
    }
    return false;
  }

  describeSwitchBlocker(conversationId: string): string | null {
    if (this.activeTurns.isBusy(conversationId)) {
      return `${this.harnessLabel(conversationId)} is currently working. Stop the active turn before switching services.`;
    }
    if (this.store.hasOpenPromptJobs(conversationId)) {
      return "Queued prompts are still waiting for the current service. Let them finish or discard them before switching.";
    }
    return null;
  }

  async switchHarness({ conversationId, harness }: { readonly conversationId: string; readonly harness: string }): Promise<HarnessSwitch> {
    if (!isHarnessName(harness)) {
      throw new Error(`Unknown service: ${String(harness)}`);
    }
    const previous = this.store.getActiveHarness(conversationId);
    if (previous === harness) {
      return { switched: false, previous, next: harness, sessionId: this.store.getSessionId(conversationId) ?? null };
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    this.store.setActiveHarness(conversationId, harness);
    log.info(`service.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${harness}`);
    return {
      switched: true,
      previous,
      next: harness,
      sessionId: this.store.getSessionId(conversationId) ?? null,
      workingDirectory: this.workingDirectoryFor(conversationId),
    };
  }

  async switchWorkspace({ conversationId, target }: { readonly conversationId: string; readonly target: string }): Promise<WorkspaceChange> {
    const workingDirectory = await resolveWorkspacePath({ root: this.config.workspaceRoot, candidate: target });
    const previous = this.workingDirectoryFor(conversationId);
    if (previous === workingDirectory) {
      return { switched: false, previous, workingDirectory };
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.switched conversation=${JSON.stringify(conversationId)} from=${previous} to=${workingDirectory}`);
    return { switched: true, previous, workingDirectory };
  }

  async createWorkspace({ conversationId, name }: { readonly conversationId: string; readonly name: string }): Promise<WorkspaceChange> {
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    const workingDirectory = await createWorkspace({ root: this.config.workspaceRoot, name });
    const previous = this.workingDirectoryFor(conversationId);
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.created conversation=${JSON.stringify(conversationId)} path=${workingDirectory}`);
    return { switched: true, created: true, previous, workingDirectory };
  }

  /** Whether this deployment offers session filesystems (the sandbox is configured). */
  get sandboxEnabled(): boolean {
    return Boolean(this.sandbox);
  }

  /**
   * Create and mount a new, empty session filesystem with the chosen internet mode.
   * The workspace is the sentinel `sessionfs:<volumeId>` (src/workspace/kind.ts), so it
   * parks and restores like any other workspace; its volume and sandbox come up when a
   * turn first needs them.
   */
  async createSessionWorkspace({ conversationId, netMode }: { readonly conversationId: string; readonly netMode: NetMode }): Promise<WorkspaceChange> {
    if (!this.sandbox) {
      throw new Error("Session filesystems are not enabled on this deployment.");
    }
    const blocker = this.describeSwitchBlocker(conversationId);
    if (blocker) {
      throw new Error(blocker);
    }
    const volumeId = newVolumeId();
    await this.sandbox.volumes.create(volumeId, netMode === "full" ? "full" : "none");
    const workingDirectory = sessionFsWorkspace(volumeId);
    const previous = this.workingDirectoryFor(conversationId);
    this.store.setWorkingDirectory(conversationId, workingDirectory);
    log.info(`workspace.created.sessionfs conversation=${JSON.stringify(conversationId)} volume=${volumeId} net=${netMode}`);
    return { switched: true, created: true, previous, workingDirectory };
  }

  enqueueMessage(conversationId: string, prompt: string, front = false): void {
    this.effects.runSync(this.turns.enqueueMessage(conversationId, prompt, front));
  }

  async processPrompt({ conversationId, chatId, messageId, text, filePaths }: IncomingPrompt): Promise<void> {
    const effectiveText = text || (filePaths.length > 0 ? "Please inspect the attached file(s)." : "");
    const handledCommand = await this.commands.handleTextCommand({
      text: effectiveText,
      filePaths,
      conversationId,
      chatId,
      messageId,
    });
    if (handledCommand) {
      return;
    }
    const prompt = effectiveText + buildFilePromptSuffix(filePaths);
    if (!prompt.trim()) {
      return;
    }
    if (await this.sendNextSetupStep({ conversationId, chatId })) {
      // Neutral by default: nothing is queued until a service and a folder are chosen.
      return;
    }
    await this.effects.runPromise(this.turns.submit({ conversationId, chatId, messageId, prompt, filePaths, visibleText: effectiveText }));
  }

  scheduleConversation(conversationId: string): void {
    this.effects.runSync(this.turns.schedule(conversationId));
  }

  resumePendingPrompts(): void {
    this.effects.runSync(this.turns.resumePendingPrompts);
  }

  reconcilePersistentState(): void {
    this.effects.runSync(this.turns.reconcilePersistentState);
  }

  setPromptDisposition(jobId: string, state: PromptJobState, priority = 0): PromptJob | null {
    return this.effects.runSync(this.turns.setPromptDisposition(jobId, state, priority));
  }

  async startNewSession({ conversationId }: { readonly conversationId: string }): Promise<string> {
    return await this.effects.runPromise(this.turns.startNewSession(conversationId));
  }

  async runCodexTurn(request: TurnRequest): Promise<boolean> {
    return await this.effects.runPromise(this.turns.run(request));
  }

  async runGoalTurn(request: GoalTurnRequest): Promise<boolean> {
    return await this.effects.runPromise(this.turns.runGoalTurn(request));
  }

  async flushCompletedResponses(): Promise<void> {
    await this.effects.runPromise(this.turns.flushCompletedResponses);
  }

  async recoverInterruptedTurns(): Promise<void> {
    await this.effects.runPromise(this.turns.recoverInterruptedTurns);
  }
}
