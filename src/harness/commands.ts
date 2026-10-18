/**
 * The commands an operator sends a conversation's running turn, as its binding takes
 * them: stop it, steer it with a prompt held while it runs, queue that prompt for after
 * it, swerve to it, or discard it. alasio runs every turn in its own process for now, so
 * each command is a call of the turn's (ActiveTurns) and of its prompt jobs (Turns);
 * where a harness runs elsewhere, these are what reach it there, and what calls them
 * stays as it is. Busy is a bound harness running a turn: ActiveTurns.isBusy.
 *
 * A held prompt is settled once: the first of its buttons pressed settles it, only while
 * it is held, and forgets the others; a press after that finds it gone.
 */
import { Effect, Option, Result } from "effect";

import { Turns } from "../codex/turns.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type ReceivedFileError, ReceivedFiles } from "../telegram/files.ts";
import { parseWorkspace } from "../workspace/kind.ts";
import { ActiveTurns, type StopReason } from "./active-turns.ts";

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
export const stopTurn = (conversationId: string, reason: StopReason): Effect.Effect<boolean, never, ActiveTurns> =>
  Effect.flatMap(ActiveTurns, (activeTurns) => activeTurns.stop(conversationId, reason));

/** Whether a bound harness runs a turn in the conversation. */
export const isBusy = (conversationId: string): Effect.Effect<boolean, never, ActiveTurns> =>
  Effect.flatMap(ActiveTurns, (activeTurns) => activeTurns.isBusy(conversationId));

/**
 * Steers the conversation's running turn with the prompt held as the job `jobId`, or
 * queues it, as Steered says. The job is taken from being held first, as steering, so no
 * other press of its buttons acts on it and no worker runs it; it is done once the turn
 * took it, and pending again otherwise, or where its files could not be written. One a
 * restart finds steering, the turn may or may not have taken: the operator is asked what
 * to do with it (Turns.reconcilePersistentState), rather than it run again or lost unsaid.
 */
export const steerWithHeld = (conversationId: string, jobId: string): Effect.Effect<Steered, StoreError | ReceivedFileError, ActiveTurns | Turns | Store | ReceivedFiles> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const turns = yield* Turns;
    const job = yield* store.settleHeldPromptJob(jobId, "steering");
    if (!job) return { outcome: "gone" } as const;
    const queued = turns.setPromptDisposition(job.id, "pending");
    // The files the prompt names, written again if a restart came since.
    const workspace = parseWorkspace((yield* store.getMount(conversationId)).workingDirectory);
    yield* (yield* ReceivedFiles).materialize(job.file_ids, workspace).pipe(Effect.tapError(() => queued));
    const steered = yield* (yield* ActiveTurns).steer(conversationId, job.prompt).pipe(Effect.result);
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
export const swerveTo = (conversationId: string, jobId: string): Effect.Effect<boolean, StoreError, ActiveTurns | Turns | Store> =>
  Effect.gen(function*() {
    if (!(yield* (yield* Store).settleHeldPromptJob(jobId, "pending", 1))) return false;
    yield* stopTurn(conversationId, "swerve");
    yield* (yield* Turns).schedule(conversationId);
    return true;
  });
