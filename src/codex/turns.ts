import { Clock, Context, Deferred, Effect, Fiber, FiberMap, FiberSet, HashMap, Layer, Option, Ref, Schema } from "effect";

import {
  type AttachedTurn,
  type Harness,
  type HarnessError,
  Harnesses,
  type HarnessUnavailable,
  type NoServiceMounted,
  NoWorkspaceMounted,
  harnessLabelOf,
} from "../harness/index.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { errorsIn, type ResponseBlock } from "./event-projection.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";
import type { GoalTurnRequest } from "../operator/goal-control.ts";
import { type Mount, mountOf } from "../persistence/conversation-repository.ts";
import type { PromptJob, PromptJobState } from "../persistence/prompt-job-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import { type ChatId, TelegramClient, type TelegramError } from "../telegram/client.ts";
import { ReceivedFiles } from "../telegram/files.ts";
import { Outbox } from "../telegram/outbox.ts";
import { WorkflowHooks } from "../workflow/hook-server.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { keepPanel } from "../operator/panel.ts";
import { truncateText } from "../operator/text.ts";
import { makeReplyMedia } from "./reply-media.ts";
import { recoverInterruptedTurns } from "./restart-recovery.ts";
import { makeStatusReporter } from "./status-reporter.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";

/** The scope these lines have always been logged in, kept for whatever reads alasio's logs. */
const LOG_SCOPE = "codex-turn-controller";

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

/** A conversation and the chat it is in. */
export interface ConversationChat {
  readonly conversationId: string;
  readonly chatId: ChatId;
}

/** An operator's prompt to queue as a prompt job: its text as sent, and the prompt it makes. */
export interface QueuedPrompt extends ConversationChat {
  readonly messageId: number;
  readonly prompt: string;
  /** The files sent with it, which the prompt names where ReceivedFiles writes them. */
  readonly fileIds: readonly string[];
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
type TurnOutcome = "completed" | "incomplete" | "interrupted" | "stopped";

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

/** Why a turn could not run: no harness to run it on, one that cannot run it, or alasio's store failing. */
export type TurnError = NoServiceMounted | HarnessUnavailable | GoalTurnsUnsupported | StoreError;

/** What a turn that ended without its answer shows: that it did not complete, and why, when the harness said. */
function notCompleted(harnessName: string, blocks: readonly ResponseBlock[]): string {
  const error = errorsIn(blocks).at(-1);
  return error ? `${harnessName} did not complete: ${error}` : `${harnessName} did not complete.`;
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
  readonly submit: (prompt: QueuedPrompt) => Effect.Effect<void, TelegramError | StoreError>;
  /** Keeps a message (a concurrent prompt's, steered or swerved without a job) for the turn after the current one. */
  readonly enqueueMessage: (conversationId: string, prompt: string, front?: boolean) => Effect.Effect<void>;
  /** Makes sure the conversation's worker is draining its prompt jobs. */
  readonly schedule: (conversationId: string) => Effect.Effect<void>;
  /** Settles what becomes of a prompt job; a job made pending again is scheduled. The job as it is now. */
  readonly setPromptDisposition: (jobId: string, state: PromptJobState, priority?: number) => Effect.Effect<PromptJob | null, StoreError>;
  /** Runs a turn on the conversation's mounted session now, then its queued messages: whether its response completed. */
  readonly run: (request: TurnRequest) => Effect.Effect<boolean, TurnError>;
  /** Runs a goal's turn on its session, or asks what to do with it while a turn runs. */
  readonly runGoalTurn: (request: GoalTurnRequest) => Effect.Effect<boolean, TurnError | TelegramError>;
  /** A new, empty session of the mounted service, mounted on the conversation: its id. */
  readonly startNewSession: (conversationId: string) => Effect.Effect<string, NoServiceMounted | HarnessUnavailable | ConversationBusy | HarnessError | StoreError>;
  /** Settles, as alasio starts, the prompt jobs the last alasio left running. */
  readonly reconcilePersistentState: Effect.Effect<void, StoreError>;
  /** Queues every completed response that was never delivered. */
  readonly flushCompletedResponses: Effect.Effect<void, StoreError>;
  /** Continues, or lets go of, the turns the last alasio was running when it stopped. */
  readonly recoverInterruptedTurns: Effect.Effect<void, StoreError>;
  /** Schedules every conversation with prompt jobs waiting. */
  readonly resumePendingPrompts: Effect.Effect<void, StoreError>;
}>()("alasio/codex/Turns") {
  static readonly layer = (): Layer.Layer<
    Turns,
    never,
    Store | TelegramClient | Outbox | WorkflowHooks | Harnesses | ActiveTurns | ReceivedFiles
  > => Layer.effect(Turns, makeTurns());
}

const makeTurns = Effect.fnUntraced(function*() {
  const store = yield* Store;
  const client = yield* TelegramClient;
  const harnesses = yield* Harnesses;
  const activeTurns = yield* ActiveTurns;
  const receivedFiles = yield* ReceivedFiles;
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  const status = yield* makeStatusReporter({
    replyMedia: makeReplyMedia({
      workspaceForChat: (chatId) => Effect.map(store.getConversationByChatId(chatId), (conversation) => mountOf(conversation).workingDirectory),
      sandbox,
    }),
  });
  // Each conversation's prompt worker, and the turns run outside one (a command's, a
  // goal's): interrupted, each turn left for after the restart, as alasio stops.
  const workers = yield* FiberMap.make<string>();
  const directTurns = yield* FiberSet.make<boolean, TurnError>();
  const queuedMessages = yield* Ref.make(HashMap.empty<string, readonly string[]>());

  const flushCompletedResponses = status.flushCompletedResponses.pipe(withLogScope(LOG_SCOPE));

  /**
   * One turn through `harness` in the mount's folder, from its status message to its
   * reply: its outcome, and whether its response completed. Interrupted, as alasio stops,
   * the turn is left for after the restart: the restart recorded for it to continue
   * then, or its completed response for delivery, and its status message saying so.
   */
  const runTurn = Effect.fnUntraced(function*(
    harness: Harness,
    mount: Mount,
    { conversationId, chatId, messageId, prompt, existingSession, attachedTurn, jobId = null }: Omit<SessionTurn, "traceparent">,
  ): Effect.fn.Return<{ readonly outcome: TurnOutcome; readonly completed: boolean }, GoalTurnsUnsupported | NoWorkspaceMounted | StoreError> {
    if (attachedTurn && !harness.supportsGoals) {
      return yield* new GoalTurnsUnsupported({ harness: harness.displayName });
    }
    const { workingDirectory } = mount;
    if (!workingDirectory) {
      return yield* new NoWorkspaceMounted();
    }
    const workspace = parseWorkspace(workingDirectory);
    if (workspace?.kind === "sessionfs") {
      yield* Effect.annotateCurrentSpan("alasio.volume.id", workspace.volumeId);
    }
    const modelChoice = yield* store.getModelChoice(conversationId, harness.name);
    yield* store.upsertActiveTurn({
      conversationId,
      chatId: String(chatId),
      messageId: String(messageId),
      sessionId: existingSession ?? null,
      harness: harness.name,
      prompt,
    });
    let responseCompleted: boolean | null = null;
    /**
     * What a job's turn records of its progress, for a restart to know what became of its
     * prompt. The turn dies on a record it cannot make: so a prompt whose dispatch is not
     * recorded is never sent, and a restart never misjudges the prompt of a turn that ran.
     */
    const recordJob = (record: (jobId: string) => Effect.Effect<void, StoreError>): Effect.Effect<void> =>
      jobId ? Effect.orDie(record(jobId)) : Effect.void;
    return yield* Effect.scoped(Effect.gen(function*() {
      const posted = yield* status.statusUpdates({ chatId, sessionId: existingSession ?? null, harnessName: harness.displayName });

      /** Leaves the turn, as alasio stops, for after the restart. */
      const leaveForRestart = Effect.gen(function*() {
        const turn = yield* store.getActiveTurn(conversationId);
        if (!turn) {
          // The turn had already let go of the conversation.
          return;
        }
        const pendingResponseId = turn.pending_response_id;
        const completed = responseCompleted
          ?? (pendingResponseId !== null && (yield* store.getCompletedResponsesPendingDelivery).some((response) => response.id === pendingResponseId));
        if (completed) {
          yield* store.clearActiveTurn(conversationId, pendingResponseId);
          yield* store.clearRestartEvent(conversationId);
          yield* Effect.logInfo(`Leaving completed response ${pendingResponseId} for post-restart delivery`);
        } else {
          yield* store.recordExternalRestartEvent(conversationId);
          yield* Effect.logInfo(`Leaving active turn ${conversationId} for post-restart recovery because the service is stopping`);
        }
        yield* status.restarting(chatId, yield* Deferred.await(posted), harness.displayName);
      }).pipe(
        Effect.catch((error) => Effect.logError(`Could not leave turn ${conversationId} for after the restart: ${error.message}`)),
      );

      /** Lets go of the conversation, with nothing of the turn left for a restart. */
      const letGo = (pendingResponseId: string) =>
        store.clearActiveTurn(conversationId, pendingResponseId).pipe(Effect.andThen(store.clearRestartEvent(conversationId)));

      return yield* Effect.gen(function*() {
        const queryResult = yield* harness.runTurn({
          prompt,
          resumeSession: existingSession ?? null,
          threadKey: conversationId,
          chatId: String(chatId),
          messageId: String(messageId),
          workingDirectory,
          modelChoice,
          persistence: store,
          attachedTurn,
          onPromptDispatched: recordJob(store.markPromptJobDispatched),
          onTransportStarted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamStarted(id, sessionId, turnId)),
          onTransportCompleted: ({ sessionId, turnId }) => recordJob((id) => store.markPromptJobUpstreamCompleted(id, sessionId, turnId)),
          // A harness that keeps running between prompts (Claude Code background work)
          // produces replies of its own and frees the conversation when they finish.
          onBackgroundResponse: flushCompletedResponses.pipe(
            Effect.catch((error) => Effect.logWarning(`Background response delivery deferred for ${conversationId}: ${error.message}`)),
            Effect.catchDefect((defect) => Effect.logWarning(`Background response delivery deferred for ${conversationId}: ${errorText(defect)}`)),
            withLogScope(LOG_SCOPE),
          ),
          onIdle: schedule(conversationId),
        });
        const { blockSequence, sessionId: newSessionId, pendingResponseId, interrupted } = queryResult;
        responseCompleted = queryResult.responseCompleted;
        const shown = yield* Deferred.await(posted);
        if (newSessionId && !existingSession) {
          yield* store.setSessionId(conversationId, newSessionId);
          yield* Effect.annotateCurrentSpan("alasio.session.id", newSessionId);
        }
        if (interrupted) {
          yield* status.finishWithoutResponse({ chatId, pendingResponseId, status: shown, harnessName: harness.displayName });
          yield* letGo(pendingResponseId);
          return { outcome: "interrupted", completed: false } as const;
        }
        if (!responseCompleted) {
          yield* status.finishWithoutResponse({
            chatId,
            pendingResponseId,
            status: shown,
            statusText: notCompleted(harness.displayName, blockSequence),
          });
          yield* letGo(pendingResponseId);
          return { outcome: "incomplete", completed: false } as const;
        }
        // The response is complete in the store, and delivered from there if its handoff fails.
        yield* Effect.suspend(() =>
          status.postResponse({ chatId, response: finalResponseToMarkdown(blockSequence), pendingResponseId, status: shown, harnessName: harness.displayName })
        ).pipe(
          Effect.catch((error) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${error.message}`)),
          Effect.catchDefect((defect) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${errorText(defect)}`)),
        );
        yield* letGo(pendingResponseId);
        return { outcome: "completed", completed: true } as const;
      }).pipe(Effect.onInterrupt(() => leaveForRestart));
    }));
  });

  /**
   * Runs a turn on the conversation's mount, then the messages queued while it ran, as a
   * turn of their own. The turn is the span `alasio.turn`, continuing `traceparent` when
   * given (a queued prompt's; null for a trace of its own) and the active span otherwise;
   * its outcome labels it and its duration.
   */
  const runSessionTurn = Effect.fnUntraced(function*(mount: Mount, turn: SessionTurn): Effect.fn.Return<boolean, TurnError> {
    const harness = yield* harnesses.requireForMount(mount);
    const labels = { "alasio.harness": harness.name };
    const startedAt = yield* Clock.currentTimeMillis;
    let outcome: TurnOutcome | "failed" = "failed";
    runningTurns.add(1, labels);
    const { completed } = yield* runTurn(harness, mount, turn).pipe(
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
  }, withLogScope(LOG_SCOPE));

  /** A turn on the conversation's mounted session, in the trace `traceparent` names (null: one of its own). */
  const run = Effect.fnUntraced(function*(request: TurnRequest & { readonly traceparent: string | null }): Effect.fn.Return<boolean, TurnError> {
    const mount = yield* store.getMount(request.conversationId);
    return yield* runSessionTurn(mount, { ...request, existingSession: mount.sessionId, attachedTurn: null });
  });

  /** A turn run outside a worker, as a fiber of the service's, for alasio's stopping to reach. */
  const runDirect = (turn: Effect.Effect<boolean, TurnError>): Effect.Effect<boolean, TurnError> =>
    Effect.flatMap(FiberSet.run(directTurns, turn), Fiber.join);

  /** The conversation's prompt jobs, one at a time, for as long as it is free and has any. */
  const drain = Effect.fnUntraced(function*(conversationId: string): Effect.fn.Return<void, NoServiceMounted | HarnessUnavailable | StoreError> {
    /** Whatever a job's turn failed with, the job fails with it and the operator is told why. */
    const failJob = Effect.fnUntraced(function*(job: PromptJob, error: unknown) {
      yield* store.failPromptJob(job.id, error);
      const mount = yield* store.getMount(conversationId);
      yield* client.sendMessage(job.chat_id, `${harnessLabelOf(mount)} hit an error: ${errorText(error)}`).pipe(Effect.ignore);
    });
    while (!(yield* activeTurns.isBusy(conversationId))) {
      const job = yield* store.claimNextPromptJob(conversationId);
      if (!job) {
        return;
      }
      const activeHarness = (yield* harnesses.requireForMount(yield* store.getMount(conversationId))).name;
      if (job.harness !== activeHarness) {
        yield* Effect.logWarning(`Prompt job ${job.id} was admitted under ${job.harness} but ${activeHarness} is active; running under ${activeHarness}`);
      }
      // Claiming a job stamps its start, so a claimed job's started_at is set.
      promptWait.record((job.started_at!.getTime() - job.created_at.getTime()) / 1000, { "alasio.harness": activeHarness });
      yield* receivedFiles.materialize(job.file_ids).pipe(
        Effect.andThen(run({
          conversationId,
          chatId: job.chat_id,
          messageId: job.message_id,
          prompt: job.prompt,
          jobId: job.id,
          traceparent: job.traceparent,
        })),
        Effect.flatMap((completed) => store.setPromptJobDisposition(job.id, completed ? "completed" : "cancelled")),
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
  }): Effect.fn.Return<void, TelegramError | StoreError> {
    const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
    const mount = yield* store.getMount(conversationId);
    const question = yield* keepPanel(conversationId, {
      text: `${harnessLabelOf(mount)} is currently working. What should I do with this message?\n\n${truncateText(visibleText, 220)}`,
      keyboard: [
        [{ text: "Steer", kind: "steer", payload }, { text: "Queue", kind: "queue", payload }],
        [{ text: "Swerve", kind: "swerve", payload }, { text: "Discard", kind: "discard", payload }],
      ],
    }).pipe(Effect.provideService(Store, store));
    yield* client.sendMessage(chatId, question.text, { reply_markup: question.options.reply_markup });
  });

  return Turns.of({
    submit: Effect.fnUntraced(function*({ conversationId, chatId, messageId, prompt, fileIds, visibleText }: QueuedPrompt) {
      const job = yield* store.enqueuePromptJob({
        conversationId,
        chatId,
        messageId,
        prompt,
        fileIds,
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
    setPromptDisposition: Effect.fnUntraced(function*(jobId, state, priority = 0) {
      yield* store.setPromptJobDisposition(jobId, state, priority);
      const job = yield* store.getPromptJob(jobId);
      if (state === "pending" && job) {
        yield* schedule(job.conversation_id);
      }
      return job;
    }),
    // The trace a turn asked for without one is the asker's, read before the turn is forked.
    run: (request) => Effect.suspend(() => runDirect(run({ ...request, traceparent: request.traceparent === undefined ? currentTraceparent() : request.traceparent }))),
    runGoalTurn: Effect.fnUntraced(function*({ conversationId, chatId, messageId, sessionId, turnId, prompt }) {
      if (yield* activeTurns.isBusy(conversationId)) {
        const job = yield* store.enqueuePromptJob({ conversationId, chatId, messageId, prompt, state: "awaiting_choice" });
        yield* askHowToHandleConcurrentPrompt({ conversationId, chatId, job, visibleText: prompt });
        return true;
      }
      yield* store.setSessionId(conversationId, sessionId);
      const mount = yield* store.getMount(conversationId);
      yield* runDirect(runSessionTurn(mount, {
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
    startNewSession: Effect.fnUntraced(function*(conversationId) {
      const mount = yield* store.getMount(conversationId);
      const harness = yield* harnesses.requireForMount(mount);
      if (yield* activeTurns.isBusy(conversationId)) {
        return yield* new ConversationBusy({ message: `${harness.displayName} is currently working. Stop the active turn before starting a new session.` });
      }
      if (!mount.workingDirectory) {
        return yield* new NoWorkspaceMounted();
      }
      const sessionId = yield* harness.startFreshSession({ threadKey: conversationId, workingDirectory: mount.workingDirectory });
      yield* store.setSessionId(conversationId, sessionId);
      return sessionId;
    }),
    reconcilePersistentState: Effect.gen(function*() {
      for (const conversationId of yield* store.recoverPromptJobsAfterRestart) {
        yield* store.clearActiveTurn(conversationId);
        yield* store.clearRestartEvent(conversationId);
      }
    }),
    flushCompletedResponses,
    recoverInterruptedTurns: recoverInterruptedTurns(store),
    resumePendingPrompts: Effect.flatMap(store.listPendingPromptConversations, (conversationIds) => Effect.forEach(conversationIds, schedule, { discard: true })),
  });
}, withLogScope(LOG_SCOPE));
