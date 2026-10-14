/**
 * A conversation held busy as no turn holds it, for what no turn may start under: a fork,
 * which suspends the workspace it mounts. Held in alasio (ActiveTurns.hold), which stops
 * every turn starting meanwhile, and as a row of alasio's store, which says so to what
 * reads it there; the rows of an alasio that stopped holding go as the next starts.
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
import type { ActiveTurns } from "./active-turns.ts";

/** Holds the conversations busy for `reason` until the scope closes: whether it holds them, which it does only where none is busy. */
export const holdConversations = (
  activeTurns: ActiveTurns["Service"],
  store: Store["Service"],
  conversationIds: readonly string[],
  reason: string,
): Effect.Effect<boolean, StoreError, Scope.Scope> =>
  Effect.gen(function*() {
    if (!(yield* activeTurns.hold(conversationIds))) return false;
    yield* Effect.acquireRelease(
      store.holdConversations(conversationIds, reason),
      () => store.releaseConversations(conversationIds).pipe(Effect.catch((error) => Effect.logWarning(`could not let go of the holds of ${conversationIds.join(", ")}: ${error.message}`))),
    );
    return true;
  });
