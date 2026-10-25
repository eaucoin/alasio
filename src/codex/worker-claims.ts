/**
 * Which of the alasios on one store drives each conversation: the one holding its worker's
 * claim (Store.claimWorker), a row of the store's taken where none holds it or its holder's has
 * lapsed, each taking one generation past the last. Its prompt jobs, its turns and what is left
 * of them are run by that alasio alone, under the claim (CurrentWorkerFence): each write of
 * theirs holds only while the claim is still the conversation's, so an alasio stalled past its
 * claim's life, which another took over meanwhile, writes nothing once it resumes.
 *
 * Within an alasio, the claim is shared by whatever drives the conversation at once (its
 * worker, a turn run outside it, a turn attached to again): taken by the first, renewed every
 * WORKER_CLAIM_RENEWAL while any holds it, and let go of once the last ends, for another alasio
 * to take at once. One told to stop (SIGTERM) takes none from then on, and what holds its claims
 * is interrupted at once, not once its drain is through (../telegram/drain.ts): a turn so
 * interrupted is left running on its harness's pod for the next alasio to attach to, and its
 * claim let go of, so another alasio takes the conversation over at once, as it does every
 * claim it still holds as it ends. One that stops renewing, as
 * it crashed or stalled, has its claims lapse WORKER_CLAIM_LIFE after its last renewal; an
 * alasio's sweep of the conversations with work left that none holds (Turns.takeUnheld), every
 * WORKER_SWEEP_EVERY, takes them up, so a conversation is driven again within
 * WORKER_CLAIM_LIFE + WORKER_SWEEP_EVERY of its alasio's crash at the most.
 *
 * A claim found taken over as it is renewed is let go of: what holds it is interrupted, and
 * none of its writes lands meanwhile, being fenced. So does one whose renewals all failed, as
 * of a store that answered nothing for more than WORKER_CLAIM_LIFE - WORKER_CLAIM_RENEWAL
 * (45 s): another alasio may take the claim then, and what this one drove is interrupted once
 * a renewal finds that, its writes in between refused. Safe, disruptive to the turn it drove,
 * and unlikely, as the store answering nothing for that long stops every alasio on it alike.
 *
 * A claim's holder is the alasio's pod's name, as its Deployment gives it (ALASIO_POD), and a
 * uuid of its process's. An alasio starting lets go of the claims of its pod's processes before
 * it (lapseOwn), as a container restarted in its pod leaves them, its process gone: so its
 * conversations are taken up at once, not WORKER_CLAIM_LIFE after the crash. The pod's name, not
 * its hostname, which two alasios share under hostNetwork, so none lets go of another's claims.
 *
 * Each ask for a claim is timed (CLAIM_WAITS), from the ask to its answer, the conversation's
 * lock in the alasio and its claim's row in the store waited on, by how it was answered; and each
 * claim lost is counted (CLAIM_LOSSES): taken over, or let go of as the pod's next process starts.
 * By the pod alone, not the conversation, which would make a series of each; the ask's span
 * (alasio.worker_claim.join) and the log line of a claim taken over say which conversation.
 */
import { randomUUID } from "node:crypto";

import { Clock, Deferred, Duration, Effect, Fiber, FiberSet, Option, Schema, Semaphore } from "effect";

import type { StoreError } from "../persistence/sql.ts";
import { CurrentWorkerFence, Store, type WorkerFence } from "../persistence/store.ts";
import { meter, withAlasioSpan } from "../telemetry/index.ts";

export const CLAIM_WAITS = "alasio.worker_claim.wait";
const claimWaits = meter.createHistogram(CLAIM_WAITS, {
  description: "How long an ask for a conversation's worker's claim took to be answered, on the conversation's lock in alasio and its claim's row in the store, by the pod asking and outcome (taken: none held it; joined: this alasio held it; held: another alasio does)",
  unit: "s",
});
export const CLAIM_LOSSES = "alasio.worker_claim.losses";
const claimLosses = meter.createCounter(CLAIM_LOSSES, {
  description: "Claims of conversations' workers a pod's alasio lost, by the pod and outcome (lost: found taken over by another alasio as it was renewed; lapsed: let go of as the pod's next process started)",
  unit: "{claim}",
});

/** How long a worker's claim lasts unless renewed. */
export const WORKER_CLAIM_LIFE = Duration.seconds(60);
/** How often an alasio renews the claims it holds, well within WORKER_CLAIM_LIFE. */
export const WORKER_CLAIM_RENEWAL = Duration.seconds(15);
/** How often an alasio takes up the conversations with work left whose claims lapsed (Turns.takeUnheld). */
export const WORKER_SWEEP_EVERY = Duration.seconds(30);

/** What drove a conversation under its claim, interrupted as another alasio took it over. */
export class WorkerClaimLost extends Schema.TaggedError<WorkerClaimLost>()("WorkerClaimLost", {
  conversationId: Schema.String,
}) {
  override get message(): string {
    return `another alasio took ${this.conversationId} over, so this one drives it no more`;
  }
}

/** How long a claim lasts unless renewed, and how often it is renewed: WORKER_CLAIM_LIFE and WORKER_CLAIM_RENEWAL but for tests. */
export interface WorkerClaimTimings {
  readonly life: Duration.Input;
  readonly renewal: Duration.Input;
}

/** What WorkerClaims is made with: the name of the pod it is of, as the module says, its timings, and what tells it alasio is told to stop, SIGTERM or SIGINT but for tests. */
export interface WorkerClaimsOptions {
  readonly pod: string;
  readonly timings?: WorkerClaimTimings | undefined;
  readonly told?: Effect.Effect<void> | undefined;
}

/** Waits until the process is told to stop: its first SIGTERM or SIGINT. */
const toldToStop: Effect.Effect<void> = Effect.callback<void>((resume) => {
  const hear = () => resume(Effect.void);
  process.once("SIGTERM", hear).once("SIGINT", hear);
  return Effect.sync(() => void process.off("SIGTERM", hear).off("SIGINT", hear));
});

/**
 * A conversation's claim as this alasio holds it: by how many at once, whether what the last
 * holder left is recovered yet, and whether it was found taken over.
 */
interface Held {
  readonly fence: WorkerFence;
  users: number;
  /** Done once the claim's recovery of what the last holder left is through, as it ended: whatever holds the claim waits for it before its work. */
  readonly recovered: Deferred.Deferred<void, StoreError | WorkerClaimLost>;
  readonly lost: Deferred.Deferred<void>;
  readonly renewal: Fiber.Fiber<void>;
}

/** What drives a conversation, under its claim, as the module says. */
export interface WorkerClaims {
  /**
   * `work` under the conversation's claim: joining this alasio's where it holds it, taking it
   * where none does, `recover` run under a claim taken so, for what the last holder left, as a
   * fiber of the claim's own, whose end `work`, as whatever holds the claim, waits for.
   * Its result; none, without running it, where another alasio holds the claim.
   */
  readonly holding: <A, E, R>(conversationId: string, work: Effect.Effect<A, E, R>, recover: Effect.Effect<void, StoreError, R>) => Effect.Effect<Option.Option<A>, E | StoreError | WorkerClaimLost, R>;
  /**
   * `work` forked into `fibers` under the conversation's claim, which what runs now holds:
   * joined before it is forked, so it holds the claim on past it. Nothing, said so, where this
   * alasio holds no claim of the conversation's.
   */
  readonly forkHolding: <E>(conversationId: string, work: Effect.Effect<void, E>, fibers: FiberSet.FiberSet<void>) => Effect.Effect<void>;
  /** Lets go of the claims this alasio's pod's processes before it held, as the module says: how many. */
  readonly lapseOwn: Effect.Effect<number, StoreError>;
}

export const makeWorkerClaims = Effect.fnUntraced(function*({ pod, timings = { life: WORKER_CLAIM_LIFE, renewal: WORKER_CLAIM_RENEWAL }, told = toldToStop }: WorkerClaimsOptions) {
  const store = yield* Store;
  /** Done once alasio is told to stop, as the module says. */
  const stopping = yield* Deferred.make<void>();
  yield* Effect.forkScoped(Effect.andThen(told, Deferred.succeed(stopping, undefined)));
  const holder = `${pod}/${randomUUID()}`;
  const lifeMs = Duration.toMillis(timings.life);
  const held = new Map<string, Held>();
  /**
   * Each conversation's own lock of its claim's joins and leaves, taken and let go of one at a
   * time, which wait on the store's answers: so one conversation's slow statement holds up its
   * own, no other's. One a conversation, kept while the alasio runs.
   */
  const locks = new Map<string, Semaphore.Semaphore>();
  const lockOf = (conversationId: string): Semaphore.Semaphore => {
    const made = locks.get(conversationId) ?? Semaphore.makeUnsafe(1);
    locks.set(conversationId, made);
    return made;
  };
  const renewals = yield* FiberSet.make<void>();

  /** Forgets the claim `fence` where it is still the one held of its conversation. */
  const forget = (fence: WorkerFence) => {
    if (held.get(fence.conversationId)?.fence === fence) held.delete(fence.conversationId);
  };

  /**
   * Renews the claim every WORKER_CLAIM_RENEWAL; found taken over, it is forgotten and `lost`
   * told. A renewal that fails is tried again at the next: the claim holds while no other alasio
   * has taken it, lapsed or not.
   */
  const renewing = (fence: WorkerFence, lost: Deferred.Deferred<void>): Effect.Effect<void> =>
    Effect.gen(function*() {
      while (true) {
        yield* Effect.sleep(timings.renewal);
        const still = yield* store.renewWorker(fence, lifeMs).pipe(
          Effect.catch((error) => Effect.as(Effect.logWarning(`could not renew the claim of ${fence.conversationId}'s worker, trying again: ${error.message}`), true)),
        );
        if (!still) {
          claimLosses.add(1, { "k8s.pod.name": pod, "alasio.worker_claim.outcome": "lost" });
          yield* Effect.logWarning(new WorkerClaimLost({ conversationId: fence.conversationId }).message);
          forget(fence);
          yield* Deferred.succeed(lost, undefined);
          return;
        }
      }
    });

  /**
   * This alasio's claim of the conversation joined, or taken where it holds none, its recovery
   * of what the last holder left, `recover`, then started as a fiber of the claim's own
   * (recovering); none where another alasio holds it. Run where nothing interrupts it but
   * `restore`'s part, the take's statement, which may wait on the claim's row, locked by a
   * fenced write: an alasio stopping meanwhile interrupts it, the claim taken, if it was, lapsing
   * unrecorded. Timed (CLAIM_WAITS), from the ask to its answer.
   */
  const join = <R>(conversationId: string, restore: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>, recover: Effect.Effect<void, StoreError, R>): Effect.Effect<Option.Option<Held>, StoreError, R> =>
    Effect.flatMap(Clock.currentTimeMillis, (asked) =>
      Effect.flatMap(joining(conversationId, restore, recover), (joined) =>
        Effect.flatMap(Clock.currentTimeMillis, (answered) => {
          const outcome = Option.match(joined, { onNone: () => "held", onSome: ({ taken }) => (taken ? "taken" : "joined") });
          claimWaits.record((answered - asked) / 1_000, { "k8s.pod.name": pod, "alasio.worker_claim.outcome": outcome });
          return Effect.as(Effect.annotateCurrentSpan({ "alasio.worker_claim.outcome": outcome }), Option.map(joined, ({ entry }) => entry));
        }))).pipe(withAlasioSpan("alasio.worker_claim.join", { attributes: { "alasio.conversation.id": conversationId, "k8s.pod.name": pod } }));
  /** join's ask, untimed: the claim, and whether it was taken now. */
  const joining = <R>(conversationId: string, restore: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>, recover: Effect.Effect<void, StoreError, R>): Effect.Effect<Option.Option<{ readonly entry: Held; readonly taken: boolean }>, StoreError, R> =>
    lockOf(conversationId).withPermit(Effect.gen(function*() {
      // Told to stop, it takes no claim, nor joins one it holds, for another alasio to take.
      if (yield* Deferred.isDone(stopping)) return Option.none();
      const entry = held.get(conversationId);
      if (entry) {
        entry.users += 1;
        return Option.some({ entry, taken: false });
      }
      const fence = yield* restore(store.claimWorker(conversationId, holder, lifeMs));
      if (!fence) return Option.none();
      const lost = yield* Deferred.make<void>();
      // Held by what took it, and by its recovery.
      const made: Held = { fence, users: 2, recovered: yield* Deferred.make<void, StoreError | WorkerClaimLost>(), lost, renewal: yield* FiberSet.run(renewals, renewing(fence, lost)) };
      held.set(conversationId, made);
      yield* recovering(conversationId, made, recover);
      return Option.some({ entry: made, taken: true });
    }));

  /** One fewer holds `entry`: the last let go of the claim, for another alasio to take at once. */
  const leave = (conversationId: string, entry: Held): Effect.Effect<void> =>
    lockOf(conversationId).withPermit(Effect.gen(function*() {
      entry.users -= 1;
      if (entry.users > 0) return;
      forget(entry.fence);
      yield* Fiber.interrupt(entry.renewal);
      // Not let go of, it lapses within its life.
      yield* store.releaseWorker(entry.fence).pipe(
        Effect.catch((error) => Effect.logWarning(`could not let go of the claim of ${conversationId}'s worker, which lapses: ${error.message}`)),
      );
    }));

  /** `work` under `entry`'s claim, failing where it is found taken over, interrupted as alasio is told to stop. */
  const under = <A, E, R>(conversationId: string, entry: Held, work: Effect.Effect<A, E, R>): Effect.Effect<A, E | WorkerClaimLost, R> =>
    work.pipe(
      Effect.raceFirst(Effect.andThen(Deferred.await(entry.lost), Effect.fail(new WorkerClaimLost({ conversationId })))),
      Effect.raceFirst(Effect.andThen(Deferred.await(stopping), Effect.interrupt)),
      Effect.provideService(CurrentWorkerFence, entry.fence),
    );

  /**
   * `recover` run under `entry`'s claim, just taken, as a fiber of the claim's own: whatever holds
   * the claim, what took it among them, waits for its end (Held.recovered) before its work, so
   * nothing acts on what the last holder left before it is recovered, as a press's steer joining
   * the claim the conversation's worker took would; and what took it, interrupted, stops no
   * recovery under way. It holds the claim as it runs, let go of once it ends; it ends as alasio
   * stops (recoveries), or the claim is found taken over (under).
   */
  const recovering = <R>(conversationId: string, entry: Held, recover: Effect.Effect<void, StoreError, R>): Effect.Effect<void, never, R> =>
    Effect.asVoid(FiberSet.run(recoveries, under(conversationId, entry, recover).pipe(
      Effect.exit,
      Effect.flatMap((exit) => Deferred.done(entry.recovered, exit)),
      Effect.ensuring(leave(conversationId, entry)),
    )));

  // Whatever holds a claim as alasio stops is interrupted first; what is left is let go of.
  yield* Effect.addFinalizer(() =>
    Effect.forEach([...held.values()], (entry) => store.releaseWorker(entry.fence).pipe(Effect.ignore), { discard: true }).pipe(
      Effect.andThen(Effect.sync(() => held.clear())),
    )
  );
  /**
   * The claims' recoveries, each a fiber of the claim's own (recovering), ended as alasio stops:
   * made after the claims' letting go above, so they end before it, as a scope's finalizers run
   * last first, and no recovery runs on past its claim's release, beside another alasio's.
   */
  const recoveries = yield* FiberSet.make<void>();

  const claims: WorkerClaims = {
    holding: (conversationId, work, recover) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.flatMap(join(conversationId, restore, recover), Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (entry) =>
            restore(under(conversationId, entry, Effect.andThen(Deferred.await(entry.recovered), work))).pipe(
              Effect.map(Option.some),
              Effect.ensuring(leave(conversationId, entry)),
            ),
        }))
      ),
    forkHolding: (conversationId, work, fibers) =>
      lockOf(conversationId).withPermit(Effect.sync(() => {
        const entry = held.get(conversationId);
        if (entry) entry.users += 1;
        return entry;
      })).pipe(
        Effect.flatMap((entry) =>
          entry
            ? Effect.asVoid(FiberSet.run(fibers, under(conversationId, entry, Effect.andThen(Deferred.await(entry.recovered), work)).pipe(
              Effect.catch((error) => Effect.logWarning(`what drove ${conversationId} under its worker's claim failed: ${error instanceof Error ? error.message : String(error)}`)),
              Effect.ensuring(leave(conversationId, entry)),
            )))
            : Effect.logWarning(`nothing was forked under ${conversationId}'s worker's claim, as this alasio holds it no longer`)
        ),
      ),
    lapseOwn: Effect.tap(store.lapseWorkersOf(pod), (lapsed) =>
      lapsed > 0
        ? Effect.andThen(
          Effect.sync(() => claimLosses.add(lapsed, { "k8s.pod.name": pod, "alasio.worker_claim.outcome": "lapsed" })),
          Effect.logInfo(`let go of the claims of ${lapsed} conversation(s) an earlier process of pod ${pod} held`),
        )
        : Effect.void),
  };
  return claims;
});
