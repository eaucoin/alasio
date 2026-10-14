/**
 * The turns running now, one per conversation at most: a conversation with one is busy.
 * A turn holds its conversation for exactly as long as it runs, in a scope of its own:
 * reserved by alasio as it decides to start it, before it claims the conversation in the
 * store, so that deciding and holding are one change; then registered by its harness,
 * with how it is stopped and steered, for as long as the harness runs it. Whatever asks
 * whether a conversation is busy, or stops or steers its turn, asks here. Where alasio
 * runs in a pod (BusyPods), its pod and the one the turn works in are held busy while it
 * runs, so no drain stops either.
 */
import { Context, Deferred, Effect, HashMap, Layer, Option, Ref, Schedule, type Scope } from "effect";

import { BusyPods, type PodRef } from "../kube/busy.ts";
import type { HarnessError } from "./index.ts";

/** Why the operator stops a running turn: /stop (or a panel's Stop), or a swerve to the prompt that came in meanwhile. */
export type StopReason = "interrupt" | "swerve";

/** A stop reason as the harnesses' log lines and turn timings name it. */
export function stopReasonText(reason: StopReason): string {
  return reason === "swerve" ? "Telegram swerve" : "Interrupted from Telegram";
}

/** A running turn, as the operator's controls reach it. */
export interface RunningTurn {
  /** Stops the turn for `reason`; done once the turn has let go of its conversation. */
  readonly stop: (reason: StopReason) => Effect.Effect<void>;
  /** Sends the turn more of the operator's guidance; whether it took it. */
  readonly steer: (prompt: string) => Effect.Effect<boolean, HarnessError>;
  /** A turn Claude Code started on its own, rather than one an operator's prompt started. */
  readonly cliInitiated: boolean;
}

export interface RegisterOptions {
  /** Registers the turn only if its conversation is not busy; otherwise the registration is a no-op. */
  readonly onlyIfIdle?: boolean | undefined;
  /** The pod the turn works in, its workspace's: held busy while the turn runs, with alasio's own. */
  readonly workspace?: PodRef | null | undefined;
}

/** How often a stop of a turn reserved, whose harness has not registered it yet, looks again. */
const RESERVED_STOP_CHECK = "100 millis";

export class ActiveTurns extends Context.Service<ActiveTurns, {
  /**
   * Holds the conversation busy for a turn alasio is about to start, until the scope
   * closes, unless it is busy: whether it holds it. Until its harness registers the turn,
   * a steer is not taken, and a stop waits for that registration, which it then stops, or
   * for the turn to let go.
   */
  readonly reserve: (conversationId: string) => Effect.Effect<boolean, never, Scope.Scope>;
  /**
   * Registers `turn` as `conversationId`'s running turn until the scope closes, over any
   * registered or reserved before it, which is the conversation's turn again once it
   * closes: whether it registered it, which it does unless `onlyIfIdle` and the
   * conversation is busy. A turn registered holds its pods busy (BusyPods) for as long.
   */
  readonly register: (conversationId: string, turn: RunningTurn, options?: RegisterOptions) => Effect.Effect<boolean, never, Scope.Scope>;
  /**
   * Registers `turn` as `conversationId`'s running turn until the scope closes, once the
   * conversation is not busy, waiting for it as long as it is, its pods held busy as
   * register holds them.
   */
  readonly registerWhenIdle: (conversationId: string, turn: RunningTurn, options?: Omit<RegisterOptions, "onlyIfIdle">) => Effect.Effect<void, never, Scope.Scope>;
  /** The conversation's running turn, if it has one. */
  readonly get: (conversationId: string) => Effect.Effect<Option.Option<RunningTurn>>;
  /**
   * Holds the conversations busy until the scope closes, as a running turn would, for what
   * no turn may start under (a fork, which suspends the workspace they run in): whether it
   * holds them, which it does only where none is busy, and then all at once. A hold is not
   * stopped: stopping it is done once it ends.
   */
  readonly hold: (conversationIds: readonly string[]) => Effect.Effect<boolean, never, Scope.Scope>;
  /** Whether the conversation has a running turn. */
  readonly isBusy: (conversationId: string) => Effect.Effect<boolean>;
  /** Stops the conversation's running turn for `reason`, once it has let go; whether there was one. */
  readonly stop: (conversationId: string, reason: StopReason) => Effect.Effect<boolean>;
  /** Steers the conversation's running turn: whether it took the guidance, or none when no turn runs. */
  readonly steer: (conversationId: string, prompt: string) => Effect.Effect<Option.Option<boolean>, HarnessError>;
}>()("alasio/harness/ActiveTurns") {
  static readonly layer: Layer.Layer<ActiveTurns> = Layer.effect(ActiveTurns, Effect.gen(function*() {
    // Each busy conversation's turns, the one running last: a reservation, and the
    // registration of its harness over it.
    const running = yield* Ref.make(HashMap.empty<string, readonly RunningTurn[]>());
    const get = (conversationId: string) => Effect.map(Ref.get(running), (turns) => Option.flatMap(HashMap.get(turns, conversationId), (stack) => Option.fromNullishOr(stack.at(-1))));
    // Done, and made anew, each time a turn lets go of a conversation, for what waits for one to be free.
    const released = yield* Ref.make(yield* Deferred.make<void>());
    const letGo = Effect.flatMap(Deferred.make<void>(), (next) => Effect.flatMap(Ref.getAndSet(released, next), (done) => Deferred.succeed(done, undefined)));
    const busy = yield* Effect.serviceOption(BusyPods);
    const holdBusy = (workspace: PodRef | null): Effect.Effect<void, never, Scope.Scope> =>
      Option.match(busy, { onNone: () => Effect.void, onSome: (pods) => pods.hold(workspace) });

    /**
     * Puts `turn` over the turns of each of the conversations until the scope closes, all
     * at once, unless `onlyIfIdle` and one is busy: whether it did. Closing the scope takes
     * off that turn alone, wherever it is; a push that put nothing leaves the scope as it was.
     */
    const push = (conversationIds: readonly string[], turn: RunningTurn, onlyIfIdle: boolean): Effect.Effect<boolean, never, Scope.Scope> =>
      Effect.uninterruptible(Effect.flatMap(
        Ref.modify(running, (turns) =>
          onlyIfIdle && conversationIds.some((conversationId) => HashMap.has(turns, conversationId))
            ? [false, turns] as const
            : [true, conversationIds.reduce((all, conversationId) => HashMap.set(all, conversationId, [...Option.getOrElse(HashMap.get(all, conversationId), () => []), turn]), turns)] as const),
        (pushed) =>
          pushed
            ? Effect.as(
              Effect.addFinalizer(() =>
                Ref.update(running, (turns) =>
                  conversationIds.reduce((all, conversationId) => {
                    const left = Option.getOrElse(HashMap.get(all, conversationId), () => []).filter((each) => each !== turn);
                    return left.length > 0 ? HashMap.set(all, conversationId, left) : HashMap.remove(all, conversationId);
                  }, turns)).pipe(Effect.andThen(letGo))
              ),
              true,
            )
            : Effect.succeed(false),
      ));

    const register = (conversationId: string, turn: RunningTurn, options?: RegisterOptions): Effect.Effect<boolean, never, Scope.Scope> =>
      push([conversationId], turn, options?.onlyIfIdle ?? false).pipe(
        Effect.tap((registered) => (registered ? holdBusy(options?.workspace ?? null) : Effect.void)),
      );

    return ActiveTurns.of({
      reserve: (conversationId) =>
        Effect.gen(function*() {
          const ended = yield* Deferred.make<void>();
          const reserved: RunningTurn = {
            // Until the harness registers the turn over the reservation, which it then stops; or until the turn lets go.
            stop: (reason) =>
              Effect.all([Deferred.isDone(ended), get(conversationId)]).pipe(
                // Only the turn's harness registers over a reservation (a turn the harness
                // starts on its own registers only where the conversation is idle).
                Effect.flatMap(([done, current]) =>
                  done
                    ? Effect.succeed(true)
                    : Option.isSome(current) && current.value !== reserved
                    ? Effect.as(current.value.stop(reason), true)
                    : Effect.succeed(false)
                ),
                Effect.repeat({ until: (stopped) => stopped, schedule: Schedule.spaced(RESERVED_STOP_CHECK) }),
                Effect.asVoid,
              ),
            steer: () => Effect.succeed(false),
            cliInitiated: false,
          };
          const held = yield* push([conversationId], reserved, true);
          yield* Effect.addFinalizer(() => Deferred.succeed(ended, undefined));
          return held;
        }),
      register,
      registerWhenIdle: (conversationId, turn, options) =>
        Effect.gen(function*() {
          for (;;) {
            // Read before the try, so a turn letting go after it is not missed.
            const free = yield* Ref.get(released);
            if (yield* register(conversationId, turn, { ...options, onlyIfIdle: true })) return;
            yield* Deferred.await(free);
          }
        }),
      get,
      hold: (conversationIds) =>
        Effect.gen(function*() {
          const ended = yield* Deferred.make<void>();
          const hold: RunningTurn = { stop: () => Deferred.await(ended), steer: () => Effect.succeed(false), cliInitiated: false };
          yield* Effect.addFinalizer(() => Deferred.succeed(ended, undefined));
          return yield* push(conversationIds, hold, true);
        }),
      isBusy: (conversationId) => Effect.map(get(conversationId), Option.isSome),
      // The turn on top stops, and the stop is done once every turn under it, its reservation
      // by alasio among them, has let go too: once the conversation is free of the turn.
      stop: (conversationId, reason) =>
        Effect.gen(function*() {
          const stack = Option.getOrElse(HashMap.get(yield* Ref.get(running), conversationId), () => []);
          const [bottom] = stack;
          const top = stack.at(-1);
          if (!bottom || !top) return false;
          yield* top.stop(reason);
          for (;;) {
            const free = yield* Ref.get(released);
            if (!Option.exists(HashMap.get(yield* Ref.get(running), conversationId), (turns) => turns.includes(bottom))) return true;
            yield* Deferred.await(free);
          }
        }),
      steer: (conversationId, prompt) =>
        Effect.flatMap(get(conversationId), Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (turn) => Effect.asSome(turn.steer(prompt)),
        })),
    });
  }));
}
