import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber, FiberSet, HashMap, Layer, Option, Ref, Schema } from "effect";

import {
  type AttachedTurn,
  type Harness,
  type HarnessError,
  Harnesses,
  type HarnessUnavailable,
  type NoServiceMounted,
  NoWorkspaceMounted,
  type OwnTurn,
  type OwnTurns,
  harnessDisplayName,
  harnessLabelOf,
} from "../harness/index.ts";
import { ActiveTurns } from "../harness/active-turns.ts";
import { errorsIn, type ResponseBlock } from "./event-projection.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";
import type { GoalTurnRequest } from "../operator/goal-control.ts";
import { type Mount, mountOf } from "../persistence/conversation-repository.ts";
import type { PromptJob, PromptJobState } from "../persistence/prompt-job-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { Turn, TurnOutcome } from "../persistence/turn-repository.ts";
import { Store, type TurnFenced } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import type { ChatId } from "../telegram/client.ts";
import { ReceivedFiles } from "../telegram/files.ts";
import { Outbox } from "../telegram/outbox.ts";
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

/**
 * What a concurrent prompt's buttons (steer, queue, swerve, discard) carry: its prompt
 * job, held for the operator's choice, and its prompt. A type rather than an interface,
 * so that it is a CallbackPayload.
 */
export type ConcurrentPromptPayload = {
  readonly jobId: string;
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

/**
 * Why a turn could not run: no harness to run it on, one that cannot run it, another turn
 * holding the conversation, alasio's store failing, or another turn claiming the
 * conversation as it ran.
 */
export type TurnError = NoServiceMounted | HarnessUnavailable | GoalTurnsUnsupported | ConversationBusy | StoreError | TurnFenced;

/** What a turn that ended without its answer shows: that it did not complete, and why, when the harness said. */
function notCompleted(harnessName: string, blocks: readonly ResponseBlock[]): string {
  const error = errorsIn(blocks).at(-1);
  return error ? `${harnessName} did not complete: ${error}` : `${harnessName} did not complete.`;
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * alasio's turns: each conversation's prompt jobs drained one at a time by a worker of its
 * own, each turn from its status message to its reply, and what a restart left
 * (interrupted turns, undelivered responses).
 * Stopping alasio interrupts every running turn while the store and Telegram are still
 * open: the restart is recorded for the turn to continue after it, and its status
 * message says so.
 */
export class Turns extends Context.Service<Turns, {
  /**
   * Queues an operator's prompt as a prompt job: run as soon as the conversation is free,
   * or, while a turn runs in it, held for the operator to say what to do with it.
   */
  readonly submit: (prompt: QueuedPrompt) => Effect.Effect<void, StoreError>;
  /** Makes sure the conversation's worker is draining its prompt jobs. */
  readonly schedule: (conversationId: string) => Effect.Effect<void>;
  /** Settles what becomes of a prompt job, its priority kept unless given; a job made pending again is scheduled. The job as it is now. */
  readonly setPromptDisposition: (jobId: string, state: PromptJobState, priority?: number) => Effect.Effect<PromptJob | null, StoreError>;
  /** Runs a turn on the conversation's mounted session now: whether its response completed. */
  readonly run: (request: TurnRequest) => Effect.Effect<boolean, TurnError>;
  /** Runs a goal's turn on its session, or asks what to do with it while a turn runs. */
  readonly runGoalTurn: (request: GoalTurnRequest) => Effect.Effect<boolean, TurnError>;
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
    Store | Outbox | Harnesses | ActiveTurns | ReceivedFiles
  > => Layer.effect(Turns, makeTurns());
}

const makeTurns = Effect.fnUntraced(function*() {
  const store = yield* Store;
  const outbox = yield* Outbox;
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
  const workers = yield* FiberSet.make<void>();
  /**
   * The conversations whose worker runs: `again` once it is scheduled while it runs, so it
   * drains once more before it ends. Whether a worker ends, and whether a schedule starts
   * one, are each decided in one change of it, so a prompt queued as its worker finds no
   * other is never left to a worker that is ending.
   */
  const draining = yield* Ref.make(HashMap.empty<string, "draining" | "again">());
  const directTurns = yield* FiberSet.make<boolean, TurnError>();

  const flushCompletedResponses = status.flushCompletedResponses.pipe(withLogScope(LOG_SCOPE));

  /**
   * One turn through `harness` in the mount's folder, from its status message to its
   * reply: its outcome, and whether its response completed, run while its conversation is
   * held busy for it (runSessionTurn). Every write of the turn's, and its end, holds only
   * under the epoch it claimed. Interrupted, as alasio stops, the turn is left for after the
   * restart: the restart recorded for it to continue then, or its completed response for
   * delivery, and its status message saying so.
   */
  const runTurn = Effect.fnUntraced(function*(
    harness: Harness,
    mount: Mount,
    { conversationId, chatId, messageId, prompt, existingSession, attachedTurn, jobId = null }: Omit<SessionTurn, "traceparent">,
  ): Effect.fn.Return<{ readonly outcome: TurnOutcome; readonly completed: boolean }, GoalTurnsUnsupported | NoWorkspaceMounted | StoreError | TurnFenced> {
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
    return yield* Effect.scoped(Effect.gen(function*() {
      // Claimed, the turn's writes and its harness's hold until another turn claims the conversation.
      const epoch = yield* store.claimTurn({
        conversationId,
        chatId: String(chatId),
        messageId: String(messageId),
        sessionId: existingSession ?? null,
        harness: harness.name,
        prompt,
      });
      const persistence = store.turnPersistence(conversationId, epoch);
      let responseCompleted: boolean | null = null;
      /**
       * What a job's turn records of its progress, for a restart to know what became of its
       * prompt. The turn dies on a record it cannot make: so a prompt whose dispatch is not
       * recorded is never sent, and a restart never misjudges the prompt of a turn that ran.
       */
      const recordJob = (record: (jobId: string) => Effect.Effect<void, StoreError>): Effect.Effect<void> =>
        jobId ? Effect.orDie(record(jobId)) : Effect.void;
      const posted = yield* status.statusUpdates({ chatId, conversationId, epoch, harnessName: harness.displayName });

      /** Leaves the turn, as alasio stops, for after the restart. */
      const leaveForRestart = Effect.gen(function*() {
        const turn = yield* store.getActiveTurn(conversationId);
        if (!turn || Number(turn.epoch) !== epoch) {
          // The turn had already let go of the conversation.
          return;
        }
        const pendingResponseId = turn.pending_response_id;
        const completed = responseCompleted
          ?? (pendingResponseId !== null && (yield* store.getCompletedResponsesPendingDelivery).some((response) => response.id === pendingResponseId));
        if (completed) {
          yield* store.endTurn(conversationId, epoch, "completed");
          yield* Effect.logInfo(`Leaving completed response ${pendingResponseId} for post-restart delivery`);
        } else {
          yield* store.recordExternalRestartEvent(conversationId, epoch);
          yield* store.setTurnStatus(conversationId, epoch, "interrupted");
          yield* Effect.logInfo(`Leaving active turn ${conversationId} for post-restart recovery because the service is stopping`);
        }
        yield* status.restarting(chatId, yield* Deferred.await(posted), harness.displayName);
      }).pipe(
        Effect.catch((error) => Effect.logError(`Could not leave turn ${conversationId} for after the restart: ${error.message}`)),
      );

      /** Lets go of the conversation, the turn ended as `outcome` says, with nothing of it left for a restart. */
      const letGo = (outcome: TurnOutcome) => store.endTurn(conversationId, epoch, outcome);

      return yield* Effect.gen(function*() {
        const queryResult = yield* harness.runTurn({
          prompt,
          resumeSession: existingSession ?? null,
          threadKey: conversationId,
          chatId: String(chatId),
          messageId: String(messageId),
          workingDirectory,
          modelChoice,
          persistence,
          ownTurns: ownTurnsOf(conversationId, chatId, harness),
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
          yield* persistence.updateActiveTurnSessionId(newSessionId);
          yield* Effect.annotateCurrentSpan("alasio.session.id", newSessionId);
        }
        if (interrupted) {
          yield* status.finishWithoutResponse({ chatId, pendingResponseId, status: shown, harnessName: harness.displayName });
          yield* letGo("interrupted");
          return { outcome: "interrupted", completed: false } as const;
        }
        if (!responseCompleted) {
          yield* status.finishWithoutResponse({
            chatId,
            pendingResponseId,
            status: shown,
            statusText: notCompleted(harness.displayName, blockSequence),
          });
          yield* letGo("incomplete");
          return { outcome: "incomplete", completed: false } as const;
        }
        // The response is complete in the store, and delivered from there if its handoff fails.
        yield* Effect.suspend(() =>
          status.postResponse({ chatId, response: finalResponseToMarkdown(blockSequence), pendingResponseId, status: shown, harnessName: harness.displayName })
        ).pipe(
          Effect.catch((error) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${error.message}`)),
          Effect.catchDefect((defect) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${errorText(defect)}`)),
        );
        yield* letGo("completed");
        return { outcome: "completed", completed: true } as const;
      }).pipe(
        Effect.onInterrupt(() => leaveForRestart),
        // A turn that failed lets go of its conversation, said so; one interrupted is left for the restart.
        Effect.onError((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : letGo("failed").pipe(Effect.catch((error) => Effect.logError(`Could not end failed turn ${conversationId}: ${error.message}`)))
        ),
      );
    }));
  });

  /**
   * How the conversation's harness claims it for a turn it starts on its own, in the chat:
   * as alasio's turns are claimed, the turn ending under the epoch it claimed.
   */
  const ownTurnsOf = (conversationId: string, chatId: ChatId, harness: Harness): OwnTurns => ({
    claim: (messageId, sessionId) =>
      Effect.map(
        store.claimTurn({ conversationId, chatId: String(chatId), messageId, sessionId, harness: harness.name, prompt: null }),
        (epoch): OwnTurn => ({
          persistence: store.turnPersistence(conversationId, epoch),
          end: (outcome) => Effect.asVoid(store.endTurn(conversationId, epoch, outcome)),
        }),
      ),
  });

  /**
   * Runs a turn on the conversation's mount. The turn is the span `alasio.turn`, continuing `traceparent` when
   * given (a queued prompt's; null for a trace of its own) and the active span otherwise;
   * its outcome labels it and its duration.
   */
  const runSessionTurn = Effect.fnUntraced(function*(mount: Mount, turn: SessionTurn): Effect.fn.Return<boolean, TurnError> {
    const harness = yield* harnesses.requireForMount(mount);
    const labels = { "alasio.harness": harness.name };
    // Held busy before it claims the conversation, as one decision, and until it has let go
    // of it; refused where another turn holds it, before it is a turn at all. Once it has
    // let go, the conversation's worker is scheduled, for what waits on it.
    return yield* Effect.scoped(Effect.gen(function*() {
      if (!(yield* activeTurns.reserve(turn.conversationId))) {
        return yield* new ConversationBusy({ message: `${harness.displayName} is already working in this conversation.` });
      }
      return yield* measuredTurn(harness, mount, turn, labels);
    })).pipe(Effect.ensuring(schedule(turn.conversationId)));
  }, withLogScope(LOG_SCOPE));

  /** A turn of `harness`'s, as the span `alasio.turn`, and counted and timed by its outcome. */
  const measuredTurn = Effect.fnUntraced(function*(harness: Harness, mount: Mount, turn: SessionTurn, labels: { readonly "alasio.harness": string }): Effect.fn.Return<boolean, TurnError> {
    const startedAt = yield* Clock.currentTimeMillis;
    let outcome: TurnOutcome = "failed";
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
  const drain = Effect.fnUntraced(function*(conversationId: string): Effect.fn.Return<void, StoreError> {
    /** Whatever a job's turn failed with, the job fails with it and the operator is told why. */
    const failJob = Effect.fnUntraced(function*(job: PromptJob, error: unknown) {
      yield* store.failPromptJob(job.id, error);
      const mount = yield* store.getMount(conversationId);
      yield* outbox.enqueueText({ chatId: job.chat_id, text: `${harnessLabelOf(mount)} hit an error: ${errorText(error)}` }).pipe(Effect.ignore);
    });
    while (!(yield* activeTurns.isBusy(conversationId))) {
      const job = yield* store.claimNextPromptJob(conversationId);
      if (!job) {
        return;
      }
      // A mount no harness serves (none, or one the deployment cannot serve) fails the job, saying why.
      yield* store.getMount(conversationId).pipe(
        Effect.flatMap(harnesses.requireForMount),
        Effect.tap(({ name: activeHarness }) =>
          Effect.gen(function*() {
            if (job.harness !== activeHarness) {
              yield* Effect.logWarning(`Prompt job ${job.id} was admitted under ${job.harness} but ${activeHarness} is active; running under ${activeHarness}`);
            }
            // Claiming a job stamps its start, so a claimed job's started_at is set.
            promptWait.record((job.started_at!.getTime() - job.created_at.getTime()) / 1000, { "alasio.harness": activeHarness });
          })
        ),
        Effect.andThen(receivedFiles.materialize(job.file_ids)),
        Effect.andThen(run({
          conversationId,
          chatId: job.chat_id,
          messageId: job.message_id,
          prompt: job.prompt,
          jobId: job.id,
          traceparent: job.traceparent,
        })),
        Effect.flatMap((completed) => store.setPromptJobDisposition(job.id, completed ? "completed" : "cancelled")),
        // Another turn took the conversation as this one was to start: the job waits for it to end, first in line as it was.
        Effect.catchTag("ConversationBusy", () => store.setPromptJobDisposition(job.id, "pending", job.priority)),
        Effect.catch((error) => failJob(job, error)),
        Effect.catchDefect((defect) => failJob(job, defect)),
      );
    }
  });

  /** Whether the conversation's worker drains again, having been scheduled while it ran; it is forgotten otherwise. */
  const drainsAgain = (conversationId: string): Effect.Effect<boolean> =>
    Ref.modify(draining, (all) =>
      Option.contains(HashMap.get(all, conversationId), "again")
        ? [true, HashMap.set(all, conversationId, "draining")] as const
        : [false, HashMap.remove(all, conversationId)] as const);

  /** The conversation's worker: it drains its prompt jobs until no schedule came while it did. */
  const worker = (conversationId: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      do {
        yield* drain(conversationId).pipe(
          Effect.catch((error) => Effect.logError(`Prompt worker for ${conversationId} stopped: ${error.message}`)),
        );
      } while (yield* drainsAgain(conversationId));
    }).pipe(
      // A worker that died or was interrupted ended without forgetting itself.
      Effect.onExit((exit) => Exit.isSuccess(exit) ? Effect.void : Ref.update(draining, HashMap.remove(conversationId))),
      withLogScope(LOG_SCOPE),
    );

  // A worker scheduled once alasio is stopping is not run: the set is closed.
  const schedule = (conversationId: string): Effect.Effect<void> =>
    Ref.modify(draining, (all) =>
      HashMap.has(all, conversationId)
        ? [false, HashMap.set(all, conversationId, "again")] as const
        : [true, HashMap.set(all, conversationId, "draining")] as const).pipe(
        Effect.flatMap((start) => start ? FiberSet.run(workers, worker(conversationId)).pipe(Effect.exit, Effect.asVoid) : Effect.void),
      );

  const askHowToHandleConcurrentPrompt = Effect.fnUntraced(function*({ conversationId, chatId, job, visibleText }: ConversationChat & {
    readonly job: Pick<PromptJob, "id" | "prompt">;
    readonly visibleText: string;
  }): Effect.fn.Return<void, StoreError> {
    const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
    const mount = yield* store.getMount(conversationId);
    const question = yield* keepPanel(conversationId, {
      text: `${harnessLabelOf(mount)} is currently working. What should I do with this message?\n\n${truncateText(visibleText, 220)}`,
      keyboard: [
        [{ text: "Steer", kind: "steer", payload }, { text: "Queue", kind: "queue", payload }],
        [{ text: "Swerve", kind: "swerve", payload }, { text: "Discard", kind: "discard", payload }],
      ],
    }).pipe(Effect.provideService(Store, store));
    yield* outbox.enqueueText({ chatId, text: question.text, options: { reply_markup: question.options.reply_markup } });
  });

  /** Says `text` on the status message of a turn the last alasio left running, where it has one. */
  const editLeftStatus = (turn: Turn, text: string): Effect.Effect<void> =>
    turn.status_outbox_id
      ? outbox.enqueueEdit(turn.status_outbox_id, { chatId: turn.channel, text }).pipe(
        Effect.asVoid,
        Effect.catch((error) => Effect.logWarning(`Could not queue an edit of turn ${turn.thread_key}'s status message: ${error.message}`)),
      )
      : Effect.void;

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
    schedule,
    setPromptDisposition: Effect.fnUntraced(function*(jobId, state, priority) {
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
      // What the last alasio held busy, it held while it ran.
      yield* store.releaseConversations(null);
      // A turn whose job the agent finished ended so, whatever stopped alasio as it did.
      for (const conversationId of yield* store.recoverPromptJobsAfterRestart) {
        const turn = yield* store.getActiveTurn(conversationId);
        if (turn) {
          yield* store.endTurn(conversationId, Number(turn.epoch), "completed");
          yield* editLeftStatus(turn, `${harnessDisplayName(turn.harness)} finished this turn as alasio stopped.`);
        }
      }
    }),
    flushCompletedResponses,
    recoverInterruptedTurns: recoverInterruptedTurns(store, editLeftStatus),
    resumePendingPrompts: Effect.flatMap(store.listPendingPromptConversations, (conversationIds) => Effect.forEach(conversationIds, schedule, { discard: true })),
  });
}, withLogScope(LOG_SCOPE));
