/**
 * What an alasio told to stop owes before it ends, with what is left of its grace
 * (./stop.ts): its member of its environment's Telegram group gone first, so another alasio,
 * or one woken for them, takes the updates that come meanwhile (the group's member finishes
 * the update in hand and leaves the group as it ends, ../log/consume.ts); then the albums due
 * made prompts of, and every reply due delivered, as none is delivered by an alasio that is not
 * running, Telegram's waits among them, which a delivery waits out as it sends (./outbox.ts):
 * until none is due, or the drain's bound, whichever comes first. The bound counts from when
 * alasio was told to stop, as its grace does, so the whole stop, the update in hand among it,
 * fits the grace. A reply put off after it failed otherwise, its next attempt later, is the
 * next alasio's, as is what the bound leaves: a chat Telegram keeps refusing does not hold an
 * alasio's stop.
 */
import { Clock, Duration, Effect, type Scope } from "effect";

import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { MediaGroups } from "./media-group-buffer.ts";
import { Outbox } from "./outbox.ts";
import { STOP_GRACE_SECONDS } from "./stop.ts";

/** How long after alasio is told to stop the drain delivers at most: the grace, but for half a minute for what stops after it. */
export const DRAIN_BOUND = Duration.seconds(STOP_GRACE_SECONDS - 30);

/** What the drain says as it begins. */
export const DRAINING = "alasio delivers what it owes before it stops";

/**
 * When alasio was told to stop, as heard from now until the scope closes: the time, in
 * milliseconds since the epoch, of the first SIGTERM or SIGINT, the one its grace counts from;
 * null until one comes, as where it stops for another reason.
 */
export const stopToldAt: Effect.Effect<Effect.Effect<number | null>, never, Scope.Scope> = Effect.gen(function*() {
  let toldAt: number | null = null;
  const hear = () => void (toldAt ??= Date.now());
  yield* Effect.acquireRelease(
    Effect.sync(() => void process.on("SIGTERM", hear).on("SIGINT", hear)),
    () => Effect.sync(() => void process.off("SIGTERM", hear).off("SIGINT", hear)),
  );
  return Effect.sync(() => toldAt);
});

/** How long the drain waits before it looks again at what another alasio's pass holds. */
const DRAIN_LOOKS_EVERY = Duration.millis(200);

/** The drain, as the module says, for at most `bound` after `toldAt` says alasio was told to stop, or after it begins where that says none. */
export const drain = ({ bound = DRAIN_BOUND, toldAt = Effect.succeed(null) }: {
  readonly bound?: Duration.Input;
  readonly toldAt?: Effect.Effect<number | null>;
} = {}): Effect.Effect<void, never, Store | Outbox | MediaGroups> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const outbox = yield* Outbox;
    const mediaGroups = yield* MediaGroups;
    yield* Effect.logInfo(DRAINING);
    const until = ((yield* toldAt) ?? (yield* Clock.currentTimeMillis)) + Duration.toMillis(bound);
    yield* mediaGroups.flushDue;
    // Delivered until none is due: what an album's prompt or a turn queues meanwhile among it.
    for (;;) {
      const remaining = until - (yield* Clock.currentTimeMillis);
      const due = (yield* store.getDueOutbox(1).pipe(Effect.catch(() => Effect.succeed([])))).length;
      if (due === 0) return;
      if (remaining <= 0) return yield* Effect.logWarning("alasio stops with replies still due, which the next alasio delivers");
      // Cut short at the bound only where it waits out Telegram's wait, as a delivery is.
      yield* outbox.deliverDue.pipe(
        Effect.timeoutOption(Duration.millis(remaining)),
        Effect.catch((error) => Effect.logWarning(`delivering as alasio stops failed: ${error.message}`)),
      );
      // What is due still is another alasio's pass's, delivering it.
      yield* Effect.sleep(DRAIN_LOOKS_EVERY);
    }
  }).pipe(withLogScope("telegram-app"));
