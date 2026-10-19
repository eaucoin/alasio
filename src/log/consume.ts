/**
 * A member of a consumer group, as alasio keeps one on each of its environment's topics
 * (Telegram's updates, its gateways' events): joined, each record handed to its handling in
 * turn and the group told of it only once it is handled, so a member started again, here or
 * on another alasio, reads on from the last record told, neither losing one nor skipping
 * one. A record's handling and the telling of it are not cut short as alasio stops: the one
 * in hand is finished first. A member that fails, or whose record's handling fails, is left,
 * and the group joined again after a backoff, from the last record told, so that record is
 * handed over again. Each stream is a group of its own, so one slow to handle holds back no
 * other.
 */
import { Effect } from "effect";

/** How long a member waits after it failed before it joins its group again. */
export const REJOIN_BACKOFF = "3 seconds";

/** A record as a member reads it: where it is, and what tells the group it is handled. */
export interface Received {
  readonly position: string;
  readonly commit: () => Promise<void>;
}

/** A member of a group: its records in turn, until it is closed. */
export interface Feed<R extends Received> {
  readonly next: () => Promise<R>;
  readonly close: () => Promise<void>;
}

/** What `error` says. */
export const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * Keeps a member of the group `join` joins, handing each record to `handle` in turn and
 * telling the group of it once handled, until it is interrupted; `name` says what it reads,
 * in what it logs. A failure of joining, reading, handling or telling is logged, and the
 * group joined again after REJOIN_BACKOFF.
 */
export const consumeGroup = <R extends Received, E1, E2, Q1, Q2>({ name, join, handle }: {
  readonly name: string;
  readonly join: Effect.Effect<Feed<R>, E1, Q1>;
  readonly handle: (record: R) => Effect.Effect<void, E2, Q2>;
}): Effect.Effect<never, never, Q1 | Q2> => {
  const handled = (record: R): Effect.Effect<void, string, Q2> =>
    handle(record).pipe(
      Effect.mapError((error) => `handling ${record.position}: ${errorText(error)}`),
      Effect.andThen(Effect.tryPromise({ try: () => record.commit(), catch: (error) => `telling the group of ${record.position}: ${errorText(error)}` })),
      Effect.uninterruptible,
    );

  const member = Effect.acquireRelease(
    // A join's failure says what it joined.
    Effect.mapError(join, errorText),
    (feed) => Effect.promise(() => feed.close().catch(() => {})),
  ).pipe(
    Effect.flatMap((feed) => Effect.forever(Effect.tryPromise({ try: () => feed.next(), catch: errorText }).pipe(Effect.flatMap(handled)))),
    Effect.scoped,
  );

  return member.pipe(
    Effect.catch((error) => Effect.logError(`Consuming ${name} failed: ${error}`)),
    Effect.andThen(Effect.sleep(REJOIN_BACKOFF)),
    Effect.forever,
  );
};
