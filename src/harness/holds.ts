/**
 * A conversation held busy as no turn holds it, for what no turn may start under: a fork,
 * which suspends the workspace it mounts. Held as a row of alasio's store, which its
 * harness host reads wherever it runs (./host.ts): it says the conversation is busy, and
 * reserves it for no turn, while the row is there. So a hold is made where no turn runs,
 * the row first and the host asked after, as the host reserves first and reads the row
 * after: of a hold and a turn begun at once, one sees the other.
 *
 * Clearing every row as alasio starts (Turns.reconcilePersistentState) is safe because
 * one alasio runs on its store at a time: its Deployment has one replica and replaces it
 * by Recreate (cli/src/manifests/alasio.ts), so the alasio starting is the only one, and
 * the one that held the rows has stopped, its forks with it. Two alasios on one store at
 * once (more replicas, or a rolling update) would make it unsafe: the one starting would
 * clear the holds of forks the other still runs.
 */
import { Effect, type Scope } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import type { Store } from "../persistence/store.ts";
import type { HarnessHost } from "./host.ts";

/**
 * Holds the conversations busy for `reason` until the scope closes: whether it holds them,
 * which it does only where none is busy, and then all at once.
 */
export const holdConversations = (
  host: HarnessHost["Service"],
  store: Store["Service"],
  conversationIds: readonly string[],
  reason: string,
): Effect.Effect<boolean, StoreError, Scope.Scope> =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function*() {
      if (!(yield* store.holdConversations(conversationIds, reason))) return false;
      const letGo = store.releaseConversations(conversationIds).pipe(
        Effect.catch((error) => Effect.logWarning(`could not let go of the holds of ${conversationIds.join(", ")}: ${error.message}`)),
      );
      // Held: let go of at once where a turn runs, or that cannot be told.
      const running = yield* restore(Effect.forEach(conversationIds, (conversationId) => Effect.map(host.status(conversationId), ({ running }) => running))).pipe(
        Effect.onError(() => letGo),
      );
      if (running.some(Boolean)) {
        yield* letGo;
        return false;
      }
      yield* Effect.addFinalizer(() => letGo);
      return true;
    })
  );
