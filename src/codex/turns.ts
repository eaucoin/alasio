import { Cause, Clock, Context, Deferred, Duration, Effect, Exit, Fiber, FiberSet, HashMap, Layer, Option, PubSub, Ref, Schedule, Schema, Semaphore } from "effect";

import {
  type AttachedTurn,
  type HarnessError,
  type HarnessUnavailable,
  type NoServiceMounted,
  NoWorkspaceMounted,
  harnessDisplayName,
  harnessLabelOf,
  type TurnResult,
} from "../harness/index.ts";
import { HarnessHost } from "../harness/host.ts";
import { HarnessEvents } from "../harness/host-events.ts";
import { countRecovery } from "../harness/recoveries.ts";
import { errorsIn, type ResponseBlock } from "./event-projection.ts";
import { finalResponseToMarkdown } from "./response-markdown.ts";
import type { GoalTurnRequest } from "../operator/goal-control.ts";
import { type Mount, mountOf } from "../persistence/conversation-repository.ts";
import type { PromptJob, PromptJobState } from "../persistence/prompt-job-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import type { Turn, TurnOutcome } from "../persistence/turn-repository.ts";
import { CurrentWorkerFence, LOST_PREFIX, Store, type TurnFenced } from "../persistence/store.ts";
import { SessionSandboxes } from "../sandbox/index.ts";
import type { ChatId } from "../telegram/client.ts";
import { ReceivedFileError } from "../telegram/files.ts";
import { Outbox } from "../telegram/outbox.ts";
import type { HarnessPanels } from "../harness/host.ts";
import { Panels } from "../harness/panels.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { buildLostHarnessText, lostStatusText } from "../operator/lost-prompt.ts";
import { truncateText } from "../operator/text.ts";
import { makeReplyMedia } from "./reply-media.ts";
import { makeStatusReporter, type StatusAhead } from "./status-reporter.ts";
import { withLogScope } from "../shared/log.ts";
import { currentTraceparent, meter, withAlasioSpan } from "../telemetry/index.ts";
import { makeWorkerClaims, type WorkerClaimLost, type WorkerClaimsOptions } from "./worker-claims.ts";

/** The scope these lines have always been logged in, kept for whatever reads alasio's logs. */
const LOG_SCOPE = "codex-turn-controller";

/**
 * How long a turn's pod is asked for it again (reattach), each try REATTACH_EVERY after the
 * last, before the turn is taken as lost with its harness instance: longer than the sandbox
 * gateway takes to let go of a pod it reaches no more, so a pod that answers again in that
 * time is attached to, and one that does not is let go of, its loss told of, by then.
 */
const REATTACH_WITHIN = Duration.minutes(12);
const REATTACH_EVERY = Duration.seconds(5);

/**
 * How long a turn attached to again, its conversation bound to no pod, its binding gone, waits
 * for its binding's release or loss to be heard first (../harness/host-events.ts), which knows
 * why its pod went and lets go of the turn itself, ending it where its host ran out of its
 * memory, so as not to run it again, before it is taken as lost and run again (recoverLost):
 * longer than the sandbox gateway takes to tell of a release it made, its Lease gone; and how
 * often, meanwhile, the turn is looked at.
 */
const LOSS_HEARD_WITHIN = Duration.seconds(30);
const LOSS_LOOK_EVERY = Duration.seconds(1);

/** What alasio's turns are made with besides their claims: how long a turn found lost waits for its loss to be heard (LOSS_HEARD_WITHIN), unless a test says. */
export interface TurnsOptions {
  readonly lossHeardWithin?: Duration.Input | undefined;
}

/**
 * How long a turn run outside a worker waits for its conversation's claim, as another alasio's
 * worker holds it, before it is refused: past the pass of a worker that has nothing to run, so
 * one finding its conversation's prompts drained waits for that pass alone.
 */
const DIRECT_CLAIM_WAIT = Duration.seconds(10);
const DIRECT_CLAIM_EVERY = Duration.millis(250);

/** A turn's claim of its conversation, held by another alasio's worker, as it asked. */
class ClaimHeld extends Schema.TaggedError<ClaimHeld>()("ClaimHeld", {}) {}

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
  /** The files sent with it, which the prompt names where the harness host writes them. */
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
  /** The files the prompt names, which its host writes where its harness reads them; none unless given. */
  readonly fileIds?: readonly string[] | undefined;
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
 * holding the conversation, or another alasio driving it, alasio's store failing, or another
 * turn claiming the conversation as it ran, or another alasio taking it over.
 */
export type TurnError = NoServiceMounted | HarnessUnavailable | GoalTurnsUnsupported | ConversationBusy | StoreError | TurnFenced | WorkerClaimLost | ReceivedFileError;

/** What a turn that ended without its answer shows: that it did not complete, and why, when the harness said. */
function notCompleted(harnessName: string, blocks: readonly ResponseBlock[]): string {
  const error = errorsIn(blocks).at(-1);
  return error ? `${harnessName} did not complete: ${error}` : `${harnessName} did not complete.`;
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * alasio's turns: each conversation's prompt jobs drained one at a time by a worker of its
 * own, each turn from its status message to its reply, and what an alasio that stopped or
 * crashed left (interrupted turns, undelivered responses). Of the alasios on one store, the one
 * holding a conversation's worker's claim alone drives it (./worker-claims.ts), and recovers what
 * the last holder left as it takes the claim.
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
  /**
   * `work` under the conversation's worker's claim, as what drives it runs (./worker-claims.ts):
   * its result; none, without running it, where another alasio drives the conversation.
   */
  readonly holding: <A, E, R>(conversationId: string, work: Effect.Effect<A, E, R>) => Effect.Effect<Option.Option<A>, E | StoreError | WorkerClaimLost, R>;
  /** Settles what becomes of a prompt job, its priority kept unless given; a job made pending again is scheduled. The job as it is now. */
  readonly setPromptDisposition: (jobId: string, state: PromptJobState, priority?: number) => Effect.Effect<PromptJob | null, StoreError>;
  /** Runs a turn on the conversation's mounted session now: whether its response completed. */
  readonly run: (request: TurnRequest) => Effect.Effect<boolean, TurnError>;
  /** Runs a goal's turn on its session, or asks what to do with it while a turn runs. */
  readonly runGoalTurn: (request: GoalTurnRequest) => Effect.Effect<boolean, TurnError>;
  /** A new, empty session of the mounted service, mounted on the conversation: its id. */
  readonly startNewSession: (conversationId: string) => Effect.Effect<string, NoServiceMounted | HarnessUnavailable | ConversationBusy | HarnessError | StoreError>;
  /** Queues every completed response that was never delivered. */
  readonly flushCompletedResponses: Effect.Effect<void, StoreError>;
  /**
   * Takes up every conversation with work left, an active turn alasio drives or a prompt job to
   * run or running, that no alasio's worker's claim holds, as its alasio stopped or crashed: its
   * worker scheduled, which, taking the claim, recovers what the last holder left first
   * (recoverLeft).
   */
  readonly takeUnheld: Effect.Effect<void, StoreError>;
  /**
   * Lets go of the claims this alasio's pod's processes before it held, as a container restarted
   * in its pod leaves them, its process gone, for takeUnheld to take up at once (./worker-claims.ts).
   */
  readonly lapseOwnClaims: Effect.Effect<void, StoreError>;
  /**
   * Schedules every conversation with prompt jobs waiting: to run, or, held for the
   * operator's choice, to be asked about again.
   */
  readonly resumePendingPrompts: Effect.Effect<void, StoreError>;
}>()("alasio/codex/Turns") {
  /** alasio's turns, its conversations' claims made as `claims` says: of alasio's pod, with their own timings unless a test says. */
  static readonly layer = (claims: WorkerClaimsOptions, options: TurnsOptions = {}): Layer.Layer<
    Turns,
    never,
    Store | Outbox | Panels | HarnessHost | HarnessEvents
  > => Layer.effect(Turns, makeTurns(claims, options));
}

const makeTurns = Effect.fnUntraced(function*(claimsOptions: WorkerClaimsOptions, { lossHeardWithin = LOSS_HEARD_WITHIN }: TurnsOptions) {
  const store = yield* Store;
  const outbox = yield* Outbox;
  const panels = yield* Panels;
  const host = yield* HarnessHost;
  /**
   * Whether no turn may start in the conversation, as its host says; where it cannot say,
   * a host elsewhere not reached, not busy, said so: what would start then fails, saying why.
   */
  const isBusy = (conversationId: string): Effect.Effect<boolean> =>
    host.status(conversationId).pipe(
      Effect.map(({ busy }) => busy),
      Effect.catch((error) => Effect.as(Effect.logWarning(`could not tell whether ${conversationId} is busy: ${error.message}`), false)),
    );
  const sandbox = Option.getOrNull(yield* Effect.serviceOption(SessionSandboxes));
  const status = yield* makeStatusReporter({
    replyMedia: makeReplyMedia({
      workspaceForChat: (chatId) => Effect.map(store.getConversationByChatId(chatId), (conversation) => mountOf(conversation).workingDirectory),
      sandbox,
    }),
  });
  // Made first, so let go of last as alasio stops: once what holds the claims is interrupted, its writes under them made.
  const claims = yield* makeWorkerClaims(claimsOptions);
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

  // Of every conversation's, under none's claim.
  const flushCompletedResponses = status.flushCompletedResponses.pipe(Effect.provideService(CurrentWorkerFence, null), withLogScope(LOG_SCOPE));

  /**
   * One turn through `harness` in the mount's folder, from its status message to its
   * reply: its outcome, and whether its response completed, run while its conversation is
   * held busy for it (runSessionTurn). Every write of the turn's, and its end, holds only
   * under the epoch it claimed. Interrupted, as alasio stops, the turn is left running on its
   * pod, for the next alasio to attach to (reattach), or its completed response for delivery,
   * and its status message saying so; its pod not reached, it is attached to again.
   */
  const runTurn = Effect.fnUntraced(function*(
    harness: HarnessPanels,
    mount: Mount,
    { conversationId, chatId, messageId, prompt, existingSession, attachedTurn, jobId = null, fileIds = [] }: Omit<SessionTurn, "traceparent">,
    ahead: StatusAhead,
  ): Effect.fn.Return<{ readonly outcome: TurnOutcome; readonly completed: boolean }, GoalTurnsUnsupported | NoWorkspaceMounted | HarnessUnavailable | StoreError | TurnFenced | ReceivedFileError> {
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
      const posted = yield* status.statusUpdates({ chatId, conversationId, epoch, harnessName: harness.displayName, ahead });

      /**
       * Leaves the turn, as alasio stops, running on its conversation's pod, for alasio to
       * attach to once it starts again (reattach); its response, where it was complete, for
       * delivery then.
       */
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
          yield* store.setTurnStatus(conversationId, epoch, "interrupted");
          yield* Effect.logInfo(`Leaving turn ${conversationId} running on its harness's pod, for the next alasio to attach to`);
        }
        yield* status.restarting(chatId, yield* Deferred.await(posted), harness.displayName);
      }).pipe(
        Effect.catch((error) => Effect.logError(`Could not leave turn ${conversationId} for after the restart: ${error.message}`)),
      );

      /** Lets go of the conversation, the turn ended as `outcome` says, with nothing of it left for a restart. */
      const letGo = (outcome: TurnOutcome) => store.endTurn(conversationId, epoch, outcome);

      return yield* Effect.gen(function*() {
        const queryResult = yield* host.startTurn({
          conversationId,
          chatId: String(chatId),
          messageId: String(messageId),
          harness: harness.name,
          workingDirectory,
          prompt,
          fileIds,
          resumeSession: existingSession ?? null,
          modelChoice,
          epoch,
          jobId,
          attachedTurn,
        });
        const { blockSequence, sessionId: newSessionId, pendingResponseId, interrupted } = queryResult;
        responseCompleted = queryResult.responseCompleted;
        // What the operator is told of the turn besides its reply, after it, once a conversation.
        const tellNotice = queryResult.notice
          ? outbox.enqueueNotice({ conversationId, chatId, text: queryResult.notice }).pipe(
            Effect.asVoid,
            Effect.catch((error) => Effect.logWarning(`Could not tell ${conversationId} of its turn: ${error.message}`)),
          )
          : Effect.void;
        const shown = yield* Deferred.await(posted);
        if (newSessionId && !existingSession) {
          yield* persistence.updateActiveTurnSessionId(newSessionId);
          yield* Effect.annotateCurrentSpan("alasio.session.id", newSessionId);
        }
        if (interrupted) {
          yield* status.finishWithoutResponse({ conversationId, chatId, pendingResponseId, status: shown, harnessName: harness.displayName });
          yield* tellNotice;
          yield* letGo("interrupted");
          return { outcome: "interrupted", completed: false } as const;
        }
        if (!responseCompleted) {
          yield* status.finishWithoutResponse({
            conversationId,
            chatId,
            pendingResponseId,
            status: shown,
            statusText: notCompleted(harness.displayName, blockSequence),
          });
          yield* tellNotice;
          yield* letGo("incomplete");
          return { outcome: "incomplete", completed: false } as const;
        }
        // The response is complete in the store, and delivered from there if its handoff fails.
        yield* Effect.suspend(() =>
          status.postResponse({ conversationId, chatId, response: finalResponseToMarkdown(blockSequence), pendingResponseId, status: shown, harnessName: harness.displayName })
        ).pipe(
          Effect.catch((error) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${error.message}`)),
          Effect.catchDefect((defect) => Effect.logError(`Final response handoff deferred for ${conversationId}: ${errorText(defect)}`)),
        );
        yield* tellNotice;
        yield* letGo("completed");
        return { outcome: "completed", completed: true } as const;
      }).pipe(
        Effect.onInterrupt(() => leaveForRestart),
        // A turn that failed lets go of its conversation, its status message saying so; one
        // interrupted is left for the restart; one whose pod was not reached is left to
        // attach to again (reattach), as its pod may run it on, or have been lost under it.
        Effect.onError((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Option.exists(Cause.findErrorOption(cause), (error) => error._tag === "HarnessHostError")
            ? store.setTurnStatus(conversationId, epoch, "interrupted").pipe(
              Effect.andThen(Effect.logWarning(`${conversationId}'s turn lost its harness's pod (${Option.match(Cause.findErrorOption(cause), { onNone: () => "", onSome: (error) => error.message })}): attaching to it again`)),
              Effect.andThen(Effect.flatMap(Deferred.await(posted), (shown) => status.unreached(chatId, shown, harness.displayName))),
              Effect.andThen(Effect.flatMap(store.getActiveTurn(conversationId), (turn) => (turn && Number(turn.epoch) === epoch ? claims.forkHolding(conversationId, reattach(turn), workers) : Effect.void))),
              Effect.catch((error) => Effect.logError(`Could not leave turn ${conversationId} to attach to again: ${error.message}`)),
            )
            : Deferred.await(posted).pipe(
              Effect.flatMap((shown) => status.finishWithoutResponse({ conversationId, chatId, pendingResponseId: null, status: shown, statusText: `${harness.displayName} hit an error.` })),
              Effect.andThen(letGo("failed")),
              Effect.catch((error) => Effect.logError(`Could not end failed turn ${conversationId}: ${error.message}`)),
            )
        ),
      );
    }));
  });

  /**
   * Runs a turn on the conversation's mount. The turn is the span `alasio.turn`, continuing `traceparent` when
   * given (a queued prompt's; null for a trace of its own) and the active span otherwise;
   * its outcome labels it and its duration.
   */
  const runSessionTurn = Effect.fnUntraced(function*(mount: Mount, turn: SessionTurn): Effect.fn.Return<boolean, TurnError> {
    const harness = yield* panels.requireForMount(turn.conversationId, mount);
    const labels = { "alasio.harness": harness.name };
    // Held busy before it claims the conversation, as one decision, and until it has let go
    // of it; refused where another turn holds it, before it is a turn at all. What it waits on
    // before its host can hold it (a harness pool's pod readied for it) said from the first,
    // on its status message, posted ahead and taken on by the turn; said too, where it then
    // does not start, why. Once it has let go, the conversation's worker is scheduled, for what
    // waits on it.
    return yield* Effect.scoped(Effect.gen(function*() {
      const ahead = yield* status.ahead(turn.chatId, harness.displayName);
      const held = yield* host.reserve(turn.conversationId, ahead.waiting).pipe(
        Effect.onExit((exit) =>
          Exit.isSuccess(exit) ? Effect.void
          : Cause.hasInterruptsOnly(exit.cause) ? Effect.flatMap(ahead.posted, (shown) => status.restarting(turn.chatId, shown, harness.displayName))
          : ahead.end(`${harness.displayName} hit an error.`)
        ),
      );
      if (!held) {
        yield* ahead.end(`${harness.displayName} is already working in this conversation; this message waits for it.`);
        return yield* new ConversationBusy({ message: `${harness.displayName} is already working in this conversation.` });
      }
      return yield* measuredTurn(harness, mount, turn, labels, ahead);
    })).pipe(Effect.ensuring(schedule(turn.conversationId)));
  }, withLogScope(LOG_SCOPE));

  /** A turn of `harness`'s, as the span `alasio.turn`, and counted and timed by its outcome. */
  const measuredTurn = Effect.fnUntraced(function*(harness: HarnessPanels, mount: Mount, turn: SessionTurn, labels: { readonly "alasio.harness": string }, ahead: StatusAhead): Effect.fn.Return<boolean, TurnError> {
    const startedAt = yield* Clock.currentTimeMillis;
    let outcome: TurnOutcome = "failed";
    runningTurns.add(1, labels);
    const { completed } = yield* runTurn(harness, mount, turn, ahead).pipe(
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

  /**
   * A turn of the conversation's run outside a worker, as a fiber of the service's, for alasio's
   * stopping to reach, under the conversation's claim: asked for again while another alasio's
   * worker holds it, for DIRECT_CLAIM_WAIT, and refused past it.
   */
  const runDirect = (conversationId: string, turn: Effect.Effect<boolean, TurnError>): Effect.Effect<boolean, TurnError> =>
    Effect.flatMap(
      FiberSet.run(
        directTurns,
        claims.holding(conversationId, turn, recoverLeft(conversationId)).pipe(
          Effect.flatMap(Option.match({ onNone: () => Effect.fail(new ClaimHeld()), onSome: Effect.succeed })),
          Effect.retry({ while: (error) => error._tag === "ClaimHeld", schedule: Schedule.max([Schedule.spaced(DIRECT_CLAIM_EVERY), Schedule.during(DIRECT_CLAIM_WAIT)]) }),
          Effect.catchTag("ClaimHeld", () => Effect.fail(new ConversationBusy({ message: "Another alasio is working in this conversation; send this again once it is through." }))),
        ),
      ),
      Fiber.join,
    );

  /**
   * The conversation's prompt jobs, one at a time, for as long as it is free and has any;
   * once it is free with none, the prompts still held from while a turn ran are asked
   * about again. Whether it ended as none was left, or as the conversation was busy.
   */
  const drain = Effect.fnUntraced(function*(conversationId: string): Effect.fn.Return<"empty" | "busy", StoreError> {
    /** Whatever a job's turn failed with, the job fails with it and the operator is told why. */
    const failJob = Effect.fnUntraced(function*(job: PromptJob, error: unknown) {
      yield* store.failPromptJob(job.id, error);
      const label = harnessLabelOf(yield* store.getMount(conversationId));
      // Its files not written where its harness reads them, as its turn's host started it: said so, as no error of the harness's.
      const text = error instanceof ReceivedFileError
        ? `The files this prompt carried could not be written where ${label} reads them, so its prompt was not run: ${errorText(error)}. Send it again.`
        : `${label} hit an error: ${errorText(error)}`;
      yield* outbox.enqueueText({ chatId: job.chat_id, text }).pipe(Effect.ignore);
    });
    while (!(yield* isBusy(conversationId))) {
      const job = yield* store.claimNextPromptJob(conversationId);
      if (!job) {
        yield* askAgainOfHeld(conversationId);
        return "empty";
      }
      // A mount no harness serves (none, or one the deployment cannot serve) fails the job, saying why.
      yield* store.getMount(conversationId).pipe(
        Effect.flatMap((mount) => panels.requireForMount(conversationId, mount)),
        Effect.tap(({ name: activeHarness }) =>
          Effect.gen(function*() {
            if (job.harness !== activeHarness) {
              yield* Effect.logWarning(`Prompt job ${job.id} was admitted under ${job.harness} but ${activeHarness} is active; running under ${activeHarness}`);
            }
            // Claiming a job stamps its start, so a claimed job's started_at is set.
            promptWait.record((job.started_at!.getTime() - job.created_at.getTime()) / 1000, { "alasio.harness": activeHarness });
          })
        ),
        Effect.andThen(run({
          conversationId,
          chatId: job.chat_id,
          messageId: job.message_id,
          prompt: job.prompt,
          fileIds: job.file_ids,
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
    return "busy";
  });

  /** Whether the conversation's worker drains again, having been scheduled while it ran; it is forgotten otherwise. */
  const drainsAgain = (conversationId: string): Effect.Effect<boolean> =>
    Ref.modify(draining, (all) =>
      Option.contains(HashMap.get(all, conversationId), "again")
        ? [true, HashMap.set(all, conversationId, "draining")] as const
        : [false, HashMap.remove(all, conversationId)] as const);

  /**
   * The conversation's worker: it drains its prompt jobs under the conversation's claim until no
   * schedule came while it did; nothing, where another alasio's worker holds the claim, whose
   * drain is the job's. One that drained them all, its claim let go of, looks again for a job
   * queued meanwhile, of an alasio that found the claim held, which ran no worker for it.
   */
  const worker = (conversationId: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      do {
        const ended = yield* claims.holding(conversationId, drain(conversationId), recoverLeft(conversationId)).pipe(
          Effect.catch((error) => Effect.as(Effect.logError(`Prompt worker for ${conversationId} stopped: ${error.message}`), Option.none())),
        );
        if (Option.contains(ended, "empty") && (yield* store.hasPendingPromptJobs(conversationId).pipe(Effect.catch(() => Effect.succeed(false))))) {
          yield* Ref.update(draining, (all) => HashMap.set(all, conversationId, "again"));
        }
      } while (yield* drainsAgain(conversationId));
    }).pipe(
      // A worker that died or was interrupted ended without forgetting itself.
      Effect.onExit((exit) => Exit.isSuccess(exit) ? Effect.void : Ref.update(draining, HashMap.remove(conversationId))),
      withLogScope(LOG_SCOPE),
    );

  // A worker scheduled once alasio is stopping is not run: the set is closed. One scheduled under
  // a claim, as a turn ends, holds none of it: it takes its own.
  const schedule = (conversationId: string): Effect.Effect<void> =>
    Ref.modify(draining, (all) =>
      HashMap.has(all, conversationId)
        ? [false, HashMap.set(all, conversationId, "again")] as const
        : [true, HashMap.set(all, conversationId, "draining")] as const).pipe(
        Effect.flatMap((start) =>
          start ? FiberSet.run(workers, worker(conversationId).pipe(Effect.provideService(CurrentWorkerFence, null))).pipe(Effect.exit, Effect.asVoid) : Effect.void
        ),
      );

  /**
   * The prompts held, and the questions about them, decided one at a time: whether a prompt
   * is held, and asked about, as a turn runs, and which held ones are asked about again as
   * it ends, so neither question comes after the other out of turn.
   */
  const asking = yield* Semaphore.make(1);

  /** Asks the operator what to do with the prompt held as `job` while a turn runs, quoting what they wrote. */
  const askHowToHandleConcurrentPrompt = Effect.fnUntraced(function*({ conversationId, chatId, job }: ConversationChat & {
    readonly job: Pick<PromptJob, "id" | "prompt" | "visible_text">;
  }): Effect.fn.Return<void, StoreError> {
    const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
    const mount = yield* store.getMount(conversationId);
    yield* outbox.enqueuePanel({
      conversationId,
      chatId,
      text: `${harnessLabelOf(mount)} is currently working. What should I do with this message?\n\n${truncateText(job.visible_text, 220)}`,
      keyboard: [
        [{ text: "Steer", kind: "steer", payload }, { text: "Queue", kind: "queue", payload }],
        [{ text: "Swerve", kind: "swerve", payload }, { text: "Discard", kind: "discard", payload }],
      ],
    });
  });

  /** The held prompt jobs asked about again, by conversation, while each is held. */
  const askedAgain = new Map<string, ReadonlySet<string>>();

  /**
   * Asks again, once the conversation is free, what to do with each prompt still held
   * from while a turn ran, which the operator has not settled: run it now, or discard it.
   * The buttons first sent for it settle it as well. Each is asked about again once while
   * this alasio runs.
   */
  const askAgainOfHeld = (conversationId: string): Effect.Effect<void> =>
    asking.withPermit(Effect.gen(function*() {
      if (yield* isBusy(conversationId)) return;
      const held = yield* store.listHeldPromptJobs(conversationId);
      const asked = askedAgain.get(conversationId) ?? new Set<string>();
      askedAgain.set(conversationId, new Set(held.flatMap(({ id }) => (asked.has(id) ? [id] : []))));
      for (const job of held.filter(({ id }) => !asked.has(id))) {
        const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
        yield* outbox.enqueuePanel({
          conversationId,
          chatId: job.chat_id,
          text: `The turn has ended. What should I do with this message?\n\n${truncateText(job.visible_text, 220)}`,
          keyboard: [[{ text: "Run it", kind: "queue", payload }, { text: "Discard", kind: "discard", payload }]],
        });
        askedAgain.set(conversationId, new Set([...askedAgain.get(conversationId) ?? [], job.id]));
      }
      if (askedAgain.get(conversationId)?.size === 0) askedAgain.delete(conversationId);
    })).pipe(Effect.catch((error) => Effect.logWarning(`Could not ask again what to do with ${conversationId}'s held prompts: ${error.message}`)));

  /**
   * Asks what to do with a prompt the last alasio was handing to the running turn to steer
   * it as it stopped, which that turn may or may not have taken: run it now, or discard it,
   * so it is neither run twice nor lost unsaid. Its question stands for its asking again.
   */
  const askAboutUnsteered = (job: PromptJob): Effect.Effect<void, StoreError> =>
    Effect.gen(function*() {
      const payload: ConcurrentPromptPayload = { jobId: job.id, prompt: job.prompt };
      yield* outbox.enqueuePanel({
        conversationId: job.conversation_id,
        chatId: job.chat_id,
        text: `alasio restarted as it sent this message to steer the running turn, which may not have taken it. What should I do with it?\n\n${truncateText(job.visible_text, 220)}`,
        keyboard: [[{ text: "Run it", kind: "queue", payload }, { text: "Discard", kind: "discard", payload }]],
      });
      askedAgain.set(job.conversation_id, new Set([...askedAgain.get(job.conversation_id) ?? [], job.id]));
    });

  /** Says `text` on the status message of a turn the last alasio left running, where it has one. */
  const editLeftStatus = (turn: Turn, text: string): Effect.Effect<void> =>
    turn.status_outbox_id
      ? outbox.enqueueEdit(turn.status_outbox_id, { chatId: turn.channel, text }).pipe(
        Effect.asVoid,
        Effect.catch((error) => Effect.logWarning(`Could not queue an edit of turn ${turn.thread_key}'s status message: ${error.message}`)),
      )
      : Effect.void;

  /**
   * The turn `turn` attached to again on its conversation's pod, as an alasio that stopped
   * left it, or one whose pod its drive did not reach: its result taken as the turn would
   * have taken it (settleAttached); or, its pod knowing nothing of it, or not reached for
   * REATTACH_WITHIN, taken as lost with its harness instance, and run again (recoverLost).
   * Of that and a recovery staged by the binding's loss, told of on the events topic, the
   * first to end the turn alone acts, as each ends it under its epoch first. One that fails
   * otherwise, as its turn failed on its host, or its answer is refused (as of a session another
   * conversation has), ends failed, its status message saying so, rather than stay holding its
   * conversation. Its conversation's prompts then run.
   */
  const reattach = (turn: Turn): Effect.Effect<void> => {
    const conversationId = turn.thread_key;
    const epoch = Number(turn.epoch);
    return host.attach({ conversationId, epoch, messageId: turn.thread_ts }).pipe(
      Effect.retry({ while: (error) => error._tag === "HarnessHostError", schedule: Schedule.max([Schedule.spaced(REATTACH_EVERY), Schedule.during(REATTACH_WITHIN)]) }),
      Effect.flatMap((attached) =>
        attached._tag === "Answered"
          ? settleAttached(turn, attached.result)
          : attached._tag === "Unknown"
          ? recoverLost(turn, `${LOST_PREFIX}${epoch}`, "its harness instance knew nothing of it")
          // Its binding gone, as its pod's: its release, or its loss, which knows why, heard first.
          : Effect.andThen(lossHeard(turn), recoverLost(turn, `${LOST_PREFIX}${epoch}`, "its conversation is bound to no harness instance"))
      ),
      Effect.catchTag("HarnessHostError", (error) => recoverLost(turn, `${LOST_PREFIX}${epoch}`, `its harness was not reached within ${Duration.format(REATTACH_WITHIN)}: ${error.message}`)),
      Effect.catch((error) => failAttached(turn, error.message)),
      Effect.ensuring(schedule(conversationId)),
      withLogScope(LOG_SCOPE),
    );
  };

  /** The result `result` of the turn `turn`, taken as its alasio would have, once the turn is ended under its epoch: nothing, where something else let go of it first. */
  const settleAttached = Effect.fnUntraced(function*(turn: Turn, result: TurnResult) {
    const conversationId = turn.thread_key;
    const epoch = Number(turn.epoch);
    const harnessName = harnessDisplayName(turn.harness);
    if (result.sessionId && !turn.session_id) {
      yield* store.turnPersistence(conversationId, epoch).updateActiveTurnSessionId(result.sessionId).pipe(Effect.catchTag("TurnFenced", () => Effect.void));
    }
    const outcome: TurnOutcome = result.interrupted ? "interrupted" : result.responseCompleted ? "completed" : "incomplete";
    if (!(yield* store.endTurn(conversationId, epoch, outcome))) {
      return yield* Effect.logInfo(`${conversationId}'s turn attached to again had been let go of already`);
    }
    yield* Effect.logInfo(`${conversationId}'s turn attached to again ended ${outcome}`);
    if (outcome === "completed") {
      yield* editLeftStatus(turn, `${harnessName} finished this turn as alasio restarted.`);
      // Its response, which its pod completed, delivered as every completed one is.
      yield* flushCompletedResponses;
    } else {
      yield* store.markAnswerPosted(conversationId, result.pendingResponseId);
      yield* editLeftStatus(turn, outcome === "interrupted" ? `${harnessName} stopped.` : notCompleted(harnessName, result.blockSequence));
    }
    if (result.notice) {
      yield* outbox.enqueueNotice({ conversationId, chatId: turn.channel, text: result.notice }).pipe(
        Effect.asVoid,
        Effect.catch((error) => Effect.logWarning(`Could not tell ${conversationId} of its turn: ${error.message}`)),
      );
    }
  });

  /** The turn `turn`, attached to again, ended failed for `why` under its epoch, its status message saying so: nothing, where something else let go of it first. */
  const failAttached = (turn: Turn, why: string): Effect.Effect<void> =>
    Effect.gen(function*() {
      yield* Effect.logError(`${turn.thread_key}'s turn attached to again failed: ${why}`);
      if (yield* store.endTurn(turn.thread_key, Number(turn.epoch), "failed")) yield* editLeftStatus(turn, `${harnessDisplayName(turn.harness)} hit an error.`);
    }).pipe(Effect.catch((error) => Effect.logError(`Could not end ${turn.thread_key}'s turn attached to again: ${error.message}`)));

  /** Waits, for lossHeardWithin at most, for the turn `turn` to be let go of as its binding's release or loss is heard. */
  const lossHeard = (turn: Turn): Effect.Effect<void, StoreError> =>
    store.getActiveTurn(turn.thread_key).pipe(
      Effect.repeat({ until: (active) => active === null || active.epoch !== turn.epoch, schedule: Schedule.spaced(LOSS_LOOK_EVERY) }),
      Effect.timeoutOption(lossHeardWithin),
      Effect.asVoid,
    );

  /**
   * The turn `turn` taken as lost with its harness instance, for `why`: run again, as
   * `messageId`, once (Store.stageLostRecovery), its status message saying so; its reply
   * delivered, where it was complete; nothing, where something else, its binding's release or
   * loss among them, let go of it first.
   */
  const recoverLost = (turn: Turn, messageId: string, why: string): Effect.Effect<void, StoreError> =>
    store.stageLostRecovery({
      conversationId: turn.thread_key,
      epoch: Number(turn.epoch),
      binding: null,
      messageId,
      prompt: (lost) => buildLostHarnessText(lost.harness),
      said: (outcome, lost) => lostStatusText(outcome, lost.harness),
    }).pipe(
      Effect.tap((recovery) => countRecovery(recovery, "reattach")),
      Effect.flatMap((recovery) => (recovery.outcome === "none" ? Effect.void : Effect.andThen(
        Effect.logWarning(`${turn.thread_key}'s turn was lost with its harness instance (${why}): ${recovery.outcome}`),
        recovery.outcome === "completed" ? flushCompletedResponses : Effect.void,
      ))),
    );

  /**
   * What the conversation's last worker left, recovered under the claim taken from it, as it
   * stopped or crashed: a turn whose job the agent finished ended so, its status message saying
   * so; a steer it was handing over asked about, not run again; the jobs it ran settled; and the
   * turn it left running on its pod attached to again in the background (reattach), under the
   * claim. Run as what took the claim recovers, before anything that joins the claim may act: so it
   * never asks for its own conversation's claim (holding), which would wait on itself; what it
   * starts under the claim it forks (forkHolding), to run once it is through.
   */
  const recoverLeft = (conversationId: string): Effect.Effect<void, StoreError> =>
    Effect.gen(function*() {
      if (yield* store.recoverLeftPromptJobs(conversationId)) {
        const turn = yield* store.getActiveTurn(conversationId);
        if (turn && (yield* store.endTurn(conversationId, Number(turn.epoch), "completed"))) {
          yield* editLeftStatus(turn, `${harnessDisplayName(turn.harness)} finished this turn as alasio stopped.`);
        }
      }
      yield* Effect.forEach(yield* store.holdUnsteeredLeftPromptJobs(conversationId), askAboutUnsteered, { discard: true });
      // A turn its harness started on its own, carrying no prompt, is its harness host's, which alasio attaches to none of.
      const left = yield* store.getActiveTurn(conversationId);
      if (left && left.prompt !== null) yield* claims.forkHolding(conversationId, reattach(left), workers);
    }).pipe(withLogScope(LOG_SCOPE));

  // What a harness running on between prompts does on its own, as its host tells it: a
  // reply of its own, delivered; its letting go of the conversation, which may run what
  // waits on it.
  //
  // However often a pod tells of either, what it leads to runs once at a time, and once more
  // where it is told of meanwhile: a conversation's worker, as schedule runs it; and the replies'
  // delivery, as deliverBackground runs it; so no pod, telling of its conversation as often as it
  // likes, makes alasio do more than that.
  const backgroundDelivery = yield* Ref.make<"idle" | "delivering" | "again">("idle");
  const deliverOnce = flushCompletedResponses.pipe(
    Effect.catch((error) => Effect.logWarning(`Background response delivery deferred: ${error.message}`)),
    Effect.catchDefect((defect) => Effect.logWarning(`Background response delivery deferred: ${errorText(defect)}`)),
  );
  const deliverBackground: Effect.Effect<void> = Ref.modify(backgroundDelivery, (state) => (state === "idle" ? [true, "delivering"] as const : [false, "again"] as const)).pipe(
    Effect.flatMap((start) =>
      start
        ? Effect.asVoid(FiberSet.run(workers, Effect.gen(function*() {
          do yield* deliverOnce;
          while (yield* Ref.modify(backgroundDelivery, (state) => (state === "again" ? [true, "delivering"] as const : [false, "idle"] as const)));
        }).pipe(Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : Ref.set(backgroundDelivery, "idle"))))))
        : Effect.void
    ),
  );
  const hostEvents = yield* (yield* HarnessEvents).subscribe;
  yield* PubSub.take(hostEvents).pipe(
    Effect.flatMap((event) => (event.kind === "idle" ? schedule(event.conversationId) : deliverBackground)),
    Effect.forever,
    withLogScope(LOG_SCOPE),
    Effect.forkScoped,
  );

  return Turns.of({
    submit: Effect.fnUntraced(function*({ conversationId, chatId, messageId, prompt, fileIds, visibleText }: QueuedPrompt) {
      const traceparent = currentTraceparent();
      const job = yield* asking.withPermit(Effect.gen(function*() {
        const queued = yield* store.enqueuePromptJob({
          conversationId,
          chatId,
          messageId,
          prompt,
          visibleText,
          fileIds,
          state: (yield* isBusy(conversationId)) ? "awaiting_choice" : "pending",
          traceparent,
        });
        // Asked about once: a prompt queued already, as its update is handled again, was
        // asked about then, its buttons kept with the question, unless asking it failed.
        if (queued.state === "awaiting_choice" && !(yield* store.hasCallbackActionsOfJob(queued.id))) yield* askHowToHandleConcurrentPrompt({ conversationId, chatId, job: queued });
        return queued;
      }));
      if (job.state === "pending") yield* schedule(conversationId);
    }),
    schedule,
    holding: (conversationId, work) => claims.holding(conversationId, work, recoverLeft(conversationId)),
    setPromptDisposition: Effect.fnUntraced(function*(jobId, state, priority) {
      yield* store.setPromptJobDisposition(jobId, state, priority);
      const job = yield* store.getPromptJob(jobId);
      if (state === "pending" && job) {
        yield* schedule(job.conversation_id);
      }
      return job;
    }),
    // The trace a turn asked for without one is the asker's, read before the turn is forked.
    run: (request) => Effect.suspend(() => runDirect(request.conversationId, run({ ...request, traceparent: request.traceparent === undefined ? currentTraceparent() : request.traceparent }))),
    runGoalTurn: Effect.fnUntraced(function*({ conversationId, chatId, messageId, sessionId, turnId, prompt }) {
      const held = yield* asking.withPermit(Effect.gen(function*() {
        if (!(yield* isBusy(conversationId))) return false;
        const job = yield* store.enqueuePromptJob({ conversationId, chatId, messageId, prompt, state: "awaiting_choice" });
        yield* askHowToHandleConcurrentPrompt({ conversationId, chatId, job });
        return true;
      }));
      if (held) {
        return true;
      }
      const traceparent = currentTraceparent();
      yield* runDirect(
        conversationId,
        Effect.gen(function*() {
          // The thread of the operator's choice, of its goals' panel: given the conversation.
          yield* store.mountGivenSession(conversationId, sessionId);
          const mount = yield* store.getMount(conversationId);
          return yield* runSessionTurn(mount, { conversationId, chatId, messageId, prompt, existingSession: sessionId, attachedTurn: turnId ? { sessionId, turnId } : null, traceparent });
        }),
      );
      return true;
    }),
    startNewSession: Effect.fnUntraced(function*(conversationId) {
      const mount = yield* store.getMount(conversationId);
      const harness = yield* panels.requireForMount(conversationId, mount);
      if (yield* isBusy(conversationId)) {
        return yield* new ConversationBusy({ message: `${harness.displayName} is currently working. Stop the active turn before starting a new session.` });
      }
      if (!mount.workingDirectory) {
        return yield* new NoWorkspaceMounted();
      }
      const sessionId = yield* host.startFreshSession({ conversationId, harness: harness.name, workingDirectory: mount.workingDirectory });
      yield* store.setSessionId(conversationId, sessionId);
      return sessionId;
    }),
    flushCompletedResponses,
    takeUnheld: Effect.flatMap(store.unheldConversations, (conversationIds) => Effect.forEach(conversationIds, schedule, { discard: true })),
    lapseOwnClaims: Effect.asVoid(claims.lapseOwn),
    resumePendingPrompts: Effect.flatMap(store.listWaitingPromptConversations, (conversationIds) => Effect.forEach(conversationIds, schedule, { discard: true })),
  });
}, withLogScope(LOG_SCOPE));
