/**
 * A conversation held busy as no turn holds it, for what no turn may start under: a fork,
 * which clones the workspace it mounts, which a turn would write into meanwhile. Held as a row of alasio's store, which its
 * harness host reads wherever it runs (./host.ts): it says the conversation is busy, and
 * reserves it for no turn, while the row is there. So a hold is made where no turn runs,
 * the row first and the host asked after, as the host reserves first and reads the row
 * after: of a hold and a turn begun at once, one sees the other.
 *
 * A hold is made for a piece of work (whileHeld), and lasts while that work runs: HOLD_TTL
 * at a time, renewed every HOLD_RENEWAL. A holder whose process stopped, or is stuck,
 * renews nothing, so it pins no conversation past HOLD_TTL. One whose work is stuck in an
 * await would renew it for as long as that work runs, so the work has a deadline,
 * HOLD_DEADLINE, from when it begins, past which it is stopped and the hold let go of. A
 * hold found expired, or taken, as it is renewed is the work's no longer: the work is
 * stopped, with HoldLost, before it touches the workspace again. The letting go is tried
 * until it is through, in the background, the hold expiring meanwhile if it never is.
 *
 * No alasio clears another's holds as it starts, as others on the store may run their forks
 * meanwhile: the holds of one that stopped, or crashed, expire within HOLD_TTL.
 */
import { Deferred, Duration, Effect, Option, Schedule, Schema } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import type { HarnessHost } from "./host.ts";
import type { HarnessHostError } from "./index.ts";

/** How long a hold lasts unless its holder renews it. */
export const HOLD_TTL = Duration.minutes(2);
/** How often a holder renews its hold, well within HOLD_TTL. */
export const HOLD_RENEWAL = Duration.seconds(30);
/**
 * How long the work a hold is made for may run once it begins: a fork, one call of the
 * sandbox gateway's (src/sandbox/gateway-client.ts fork), which clones the workspace and
 * answers within the minute alasio waits for any call of its but a bind's and a release's
 * (CALL_TIMEOUT_MS). So no fork that could still finish is stopped by it, and one stuck in
 * an await holds its conversations no longer than this: the minute, with room over it.
 */
export const HOLD_DEADLINE = Duration.minutes(5);
/** How a letting go that failed is tried again, until it is through. */
const LET_GO_AGAIN = Schedule.min([Schedule.exponential(Duration.seconds(1)), Schedule.spaced(Duration.minutes(1))]);

/** The work a hold was made for, stopped: the hold expired, or another took it, as it was renewed. */
export class HoldLost extends Schema.TaggedError<HoldLost>()("HoldLost", {
  conversationIds: Schema.Array(Schema.String),
  reason: Schema.String,
}) {
  override get message(): string {
    return `the hold of ${this.conversationIds.join(", ")} for ${this.reason} expired before it was renewed, so what it was held for was stopped`;
  }
}

/** The work a hold was made for, stopped as it ran past its deadline (HOLD_DEADLINE). */
export class HoldOverran extends Schema.TaggedError<HoldOverran>()("HoldOverran", {
  reason: Schema.String,
  deadline: Schema.String,
}) {
  override get message(): string {
    return `${this.reason} ran past its ${this.deadline}, so it was stopped and its hold let go of`;
  }
}

/** How a hold lasts: its ttl, how often it is renewed, its work's deadline, and how its letting go is tried again. */
export interface HoldTiming {
  readonly ttl?: Duration.Input;
  readonly renewal?: Duration.Input;
  readonly deadline?: Duration.Input;
  readonly letGoAgain?: Schedule.Schedule<unknown>;
}

/**
 * Runs `work` holding the conversations busy for `reason`, as the module says: what it
 * gave, or none where it could not hold them, as one is busy, and then holds none. The hold
 * is let go of once the work is done, however it ended. `work` is given what it runs as it
 * begins, which starts its deadline (HOLD_DEADLINE): a fork runs it as it asks the gateway.
 */
export const whileHeld = <A, E, R>(
  host: Pick<HarnessHost["Service"], "status">,
  store: Store["Service"],
  conversationIds: readonly string[],
  reason: string,
  work: (begun: Effect.Effect<void>) => Effect.Effect<A, E, R>,
  { ttl = HOLD_TTL, renewal = HOLD_RENEWAL, deadline = HOLD_DEADLINE, letGoAgain = LET_GO_AGAIN }: HoldTiming = {},
): Effect.Effect<Option.Option<A>, E | StoreError | HarnessHostError | HoldLost | HoldOverran, R> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function*() {
      const ttlMs = Duration.toMillis(Duration.fromInputUnsafe(ttl));
      const holder = yield* store.holdConversations(conversationIds, reason, ttlMs);
      if (holder === null) return Option.none();
      // Tried until it is through: where the first try fails, apart from what lets go, which goes on meanwhile.
      const letGo = store.releaseHolds(holder).pipe(
        Effect.catch((error) =>
          Effect.logWarning(`could not let go of the holds of ${conversationIds.join(", ")}, trying again: ${error.message}`).pipe(
            Effect.andThen(store.releaseHolds(holder).pipe(
              Effect.tapError((again) => Effect.logWarning(`could not let go of the holds of ${conversationIds.join(", ")}, trying again: ${again.message}`)),
              Effect.retry(letGoAgain),
              Effect.ignore,
              Effect.forkDetach,
            )),
          )
        ),
        Effect.asVoid,
      );
      // Held: let go of at once where a turn runs, or that cannot be told.
      const running = yield* restore(Effect.forEach(conversationIds, (conversationId) => Effect.map(host.status(conversationId), ({ running }) => running))).pipe(
        Effect.onError(() => letGo),
      );
      if (running.some(Boolean)) {
        yield* letGo;
        return Option.none();
      }
      const held = new Set(conversationIds).size;
      // Renewed while the work runs, only where it has not expired; one renewal that fails
      // is tried again at the next, the hold expiring if none is through in time.
      const renewing: Effect.Effect<never, HoldLost> = Effect.sleep(renewal).pipe(
        Effect.andThen(store.renewHolds(holder, ttlMs).pipe(
          Effect.catch((error) => Effect.as(Effect.logWarning(`could not renew the holds of ${conversationIds.join(", ")}: ${error.message}`), held)),
        )),
        Effect.flatMap((renewed) => (renewed < held ? Effect.fail(new HoldLost({ conversationIds: [...conversationIds], reason })) : Effect.void)),
        Effect.forever,
      );
      // Stopped once it has run past its deadline, counted from when it began.
      const begun = yield* Deferred.make<void>();
      const overrunning: Effect.Effect<never, HoldOverran> = Deferred.await(begun).pipe(
        Effect.andThen(Effect.sleep(deadline)),
        Effect.andThen(Effect.fail(new HoldOverran({ reason, deadline: Duration.format(Duration.fromInputUnsafe(deadline)) }))),
      );
      return yield* restore(work(Effect.asVoid(Deferred.succeed(begun, undefined))).pipe(
        Effect.raceFirst(renewing),
        Effect.raceFirst(overrunning),
        Effect.map(Option.some),
      )).pipe(Effect.ensuring(letGo));
    })
  );
