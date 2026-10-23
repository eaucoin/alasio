/**
 * The commands an operator sends a conversation's running turn, as its binding takes
 * them: stop it, steer it with a prompt held while it runs, queue that prompt for after
 * it, swerve to it, or discard it. alasio runs every turn in its own process for now, so
 * each command is a call of its harness host (HarnessHost) and of its prompt jobs (Turns);
 * where a harness runs elsewhere, these are what reach it there, and what calls them
 * stays as it is. Busy is a bound harness running a turn: the host's status.
 *
 * A held prompt is settled once: the first of its buttons pressed settles it, only while
 * it is held, and forgets the others; a press after that finds it gone.
 */
import { Effect, Option, Result } from "effect";

import { Turns } from "../codex/turns.ts";
import type { WorkerClaimLost } from "../codex/worker-claims.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import type { StopReason } from "./active-turns.ts";
import { HarnessHost } from "./host.ts";
import type { HarnessHostError } from "./index.ts";

/** What became of a prompt held while a turn ran, sent to steer it. */
export type Steered =
  /** The turn took it as guidance; the prompt is done. */
  | { readonly outcome: "steered" }
  /**
   * No turn took it, so it is queued: none runs (`idle`), and it runs next; the one running
   * cannot take guidance yet (`not ready`: a Codex turn not yet started upstream), or the
   * steer failed (`failed`, saying why); and it runs after the turn.
   */
  | { readonly outcome: "queued"; readonly reason: "idle" }
  | { readonly outcome: "queued"; readonly reason: "not ready" }
  | { readonly outcome: "queued"; readonly reason: "failed"; readonly failure: string }
  /** It is no longer held: settled by another of its buttons, or never was. */
  | { readonly outcome: "gone" };

/** Stops the conversation's running turn for `reason`, once it has let go: whether one ran. */
export const stopTurn = (conversationId: string, reason: StopReason): Effect.Effect<boolean, HarnessHostError, HarnessHost> =>
  Effect.flatMap(HarnessHost, (host) => host.interrupt(conversationId, reason));

/** Whether no turn may start in the conversation, as its harness host says (HarnessStatus.busy). */
export const isBusy = (conversationId: string): Effect.Effect<boolean, StoreError | HarnessHostError, HarnessHost> =>
  Effect.flatMap(HarnessHost, (host) => Effect.map(host.status(conversationId), ({ busy }) => busy));

/**
 * Whether the operator's panels show the conversation working, and refuse what it would
 * change: as isBusy says, or working where that cannot be told, said in alasio's log.
 */
export const shownBusy = (conversationId: string): Effect.Effect<boolean, never, HarnessHost> =>
  isBusy(conversationId).pipe(Effect.catch((error) => Effect.as(Effect.logWarning(`could not tell whether ${conversationId} is busy: ${error.message}`), true)));

/** Stops the conversation's running turn, as stopTurn does, saying what the operator is told where its host is not reached: whether one ran, or what to say. */
export const stopTurnSaying = (conversationId: string, reason: StopReason): Effect.Effect<boolean | string, never, HarnessHost> =>
  stopTurn(conversationId, reason).pipe(Effect.catchTag("HarnessHostError", (error) => Effect.succeed(error.message)));

/**
 * Steers the conversation's running turn with the prompt held as the job `jobId`, or
 * queues it, as Steered says. The job is taken from being held first, as steering, so no
 * other press of its buttons acts on it and no worker runs it; it is done once the turn
 * took it, and pending again otherwise, or where its files could not be written. Steered
 * under the conversation's worker's claim, so no alasio takes the conversation up meanwhile;
 * queued, for the alasio that drives it, where another does. One whose alasio stopped or
 * crashed as it steered, the turn may or may not have taken: the operator is asked what to do
 * with it as the conversation is taken up again (Turns recoverLeft), rather than it run again
 * or lost unsaid.
 */
export const steerWithHeld = (conversationId: string, jobId: string): Effect.Effect<Steered, StoreError | WorkerClaimLost, HarnessHost | Turns | Store> =>
  Effect.flatMap(Turns, (turns) => turns.holding(conversationId, steerHeld(conversationId, jobId))).pipe(
    Effect.flatMap(Option.match({
      onSome: Effect.succeed,
      onNone: () =>
        Effect.map(queueHeld(jobId), (held): Steered => (held ? { outcome: "queued", reason: "failed", failure: "Another alasio drives this conversation." } : { outcome: "gone" })),
    })),
  );

/** The steer of steerWithHeld, under the conversation's worker's claim. */
const steerHeld = (conversationId: string, jobId: string): Effect.Effect<Steered, StoreError, HarnessHost | Turns | Store> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const turns = yield* Turns;
    const job = yield* store.settleHeldPromptJob(jobId, "steering");
    if (!job) return { outcome: "gone" } as const;
    const queued = turns.setPromptDisposition(job.id, "pending");
    const steered = yield* (yield* HarnessHost).steer(conversationId, job.prompt, job.file_ids).pipe(Effect.result);
    if (Result.isSuccess(steered) && Option.getOrElse(steered.success, () => false)) {
      yield* store.setPromptJobDisposition(job.id, "completed");
      return { outcome: "steered" } as const;
    }
    yield* queued;
    if (Result.isFailure(steered)) return { outcome: "queued", reason: "failed", failure: steered.failure.message } as const;
    return Option.isNone(steered.success) ? { outcome: "queued", reason: "idle" } as const : { outcome: "queued", reason: "not ready" } as const;
  });

/** Queues the prompt held as the job `jobId` for after the running turn: whether it was held. */
export const queueHeld = (jobId: string): Effect.Effect<boolean, StoreError, Turns | Store> =>
  Effect.gen(function*() {
    const job = yield* (yield* Store).settleHeldPromptJob(jobId, "pending");
    if (job) yield* (yield* Turns).schedule(job.conversation_id);
    return job !== null;
  });

/** Discards the prompt held as the job `jobId`: whether it was held. */
export const discardHeld = (jobId: string): Effect.Effect<boolean, StoreError, Store> =>
  Effect.map(Effect.flatMap(Store, (store) => store.settleHeldPromptJob(jobId, "cancelled")), (job) => job !== null);

/** Stops the conversation's running turn for the prompt held as the job `jobId`, which runs next, first in line: whether it was held. */
export const swerveTo = (conversationId: string, jobId: string): Effect.Effect<boolean, StoreError, HarnessHost | Turns | Store> =>
  Effect.gen(function*() {
    if (!(yield* (yield* Store).settleHeldPromptJob(jobId, "pending", 1))) return false;
    // Where a host elsewhere stops no turn, the prompt runs once the turn ends, which is said.
    yield* stopTurn(conversationId, "swerve").pipe(Effect.catch((error) => Effect.logWarning(`could not swerve ${conversationId}'s turn: ${error.message}`)));
    yield* (yield* Turns).schedule(conversationId);
    return true;
  });
