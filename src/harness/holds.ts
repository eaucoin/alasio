/**
 * A conversation held busy as no turn holds it, for what no turn may start under: a fork,
 * which suspends the workspace it mounts. Held as a row of alasio's store, which its
 * harness host reads wherever it runs (./host.ts): it says the conversation is busy, and
 * reserves it for no turn, while the row is there. So a hold is made where no turn runs,
 * the row first and the host asked after, as the host reserves first and reads the row
 * after: of a hold and a turn begun at once, one sees the other.
 *
 * A hold lasts HOLD_TTL unless its holder renews it, which it does every HOLD_RENEWAL while
 * it holds: a holder that stopped, or is stuck, holds the conversation no longer than that.
 * Its letting go is tried until it is through, in the background, the hold expiring
 * meanwhile if it never is.
 *
 * Clearing every row as alasio starts (Turns.reconcilePersistentState) is safe because
 * one alasio runs on its store at a time: its Deployment has one replica and replaces it
 * by Recreate (cli/src/manifests/alasio.ts), so the alasio starting is the only one, and
 * the one that held the rows has stopped, its forks with it. Two alasios on one store at
 * once (more replicas, or a rolling update) would make it unsafe: the one starting would
 * clear the holds of forks the other still runs.
 */
import { Duration, Effect, Schedule, type Scope } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import type { HarnessHost } from "./host.ts";
import type { HarnessHostError } from "./index.ts";

/** How long a hold lasts unless its holder renews it. */
export const HOLD_TTL = Duration.minutes(2);
/** How often a holder renews its hold, well within HOLD_TTL. */
export const HOLD_RENEWAL = Duration.seconds(30);
/** How a letting go that failed is tried again, until it is through. */
const LET_GO_AGAIN = Schedule.min([Schedule.exponential(Duration.seconds(1)), Schedule.spaced(Duration.minutes(1))]);

/**
 * Holds the conversations busy for `reason` until the scope closes: whether it holds them,
 * which it does only where none is busy, and then all of them; where it does not, it holds
 * none.
 */
export const holdConversations = (
  host: HarnessHost["Service"],
  store: Store["Service"],
  conversationIds: readonly string[],
  reason: string,
  { ttl = HOLD_TTL, renewal = HOLD_RENEWAL, letGoAgain = LET_GO_AGAIN }: { readonly ttl?: Duration.Input; readonly renewal?: Duration.Input; readonly letGoAgain?: Schedule.Schedule<unknown> } = {},
): Effect.Effect<boolean, StoreError | HarnessHostError, Scope.Scope> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function*() {
      const ttlMs = Duration.toMillis(Duration.fromInputUnsafe(ttl));
      const holder = yield* store.holdConversations(conversationIds, reason, ttlMs);
      if (holder === null) return false;
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
      // Held: let go of at once where a turn runs, or that cannot be told, and as the scope closes otherwise.
      const running = yield* restore(Effect.forEach(conversationIds, (conversationId) => Effect.map(host.status(conversationId), ({ running }) => running))).pipe(
        Effect.onError(() => letGo),
      );
      if (running.some(Boolean)) {
        yield* letGo;
        return false;
      }
      yield* Effect.addFinalizer(() => letGo);
      // Renewed while held; a renewal that fails is tried again at the next.
      yield* store.renewHolds(holder, ttlMs).pipe(
        Effect.catch((error) => Effect.logWarning(`could not renew the holds of ${conversationIds.join(", ")}: ${error.message}`)),
        Effect.delay(renewal),
        Effect.forever,
        // Stopped as the scope closes, though forked where nothing else is interrupted.
        Effect.interruptible,
        Effect.forkScoped,
      );
      return true;
    })
  );
