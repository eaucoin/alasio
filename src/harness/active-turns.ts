/**
 * The turns running now, one per conversation at most: a conversation with one is busy.
 * A turn registers itself for exactly as long as it runs, in a scope of its own, with
 * how it is stopped and steered; whatever asks whether a conversation is busy, or stops
 * or steers its turn, asks here. Where alasio runs in a pod (BusyPods), its pod and the
 * one the turn works in are held busy while it runs, so no drain stops either.
 */
import { Context, Deferred, Effect, HashMap, Layer, Option, Ref, type Scope } from "effect";

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
  /** Registers the turn only if its conversation has none running; otherwise the registration is a no-op. */
  readonly onlyIfIdle?: boolean | undefined;
  /** The pod the turn works in, its workspace's: held busy while the turn runs, with alasio's own. */
  readonly workspace?: PodRef | null | undefined;
}

export class ActiveTurns extends Context.Service<ActiveTurns, {
  /**
   * Registers `turn` as `conversationId`'s running turn until the scope closes, in place
   * of any registered before it; closing the scope leaves a later registration be. A turn
   * registered holds its pods busy (BusyPods) for as long.
   */
  readonly register: (conversationId: string, turn: RunningTurn, options?: RegisterOptions) => Effect.Effect<void, never, Scope.Scope>;
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
    const running = yield* Ref.make(HashMap.empty<string, RunningTurn>());
    const get = (conversationId: string) => Effect.map(Ref.get(running), HashMap.get(conversationId));
    const busy = yield* Effect.serviceOption(BusyPods);
    const holdBusy = (workspace: PodRef | null): Effect.Effect<void, never, Scope.Scope> =>
      Option.match(busy, { onNone: () => Effect.void, onSome: (pods) => pods.hold(workspace) });

    return ActiveTurns.of({
      register: (conversationId, turn, options) =>
        Effect.acquireRelease(
          Ref.modify(running, (turns) =>
            options?.onlyIfIdle && HashMap.has(turns, conversationId)
              ? [false, turns] as const
              : [true, HashMap.set(turns, conversationId, turn)] as const),
          (registered) =>
            registered
              ? Ref.update(running, (turns) =>
                Option.exists(HashMap.get(turns, conversationId), (current) => current === turn) ? HashMap.remove(turns, conversationId) : turns)
              : Effect.void,
        ).pipe(Effect.flatMap((registered) => (registered ? holdBusy(options?.workspace ?? null) : Effect.void))),
      get,
      hold: (conversationIds) =>
        Effect.gen(function*() {
          const ended = yield* Deferred.make<void>();
          const hold: RunningTurn = { stop: () => Deferred.await(ended), steer: () => Effect.succeed(false), cliInitiated: false };
          return yield* Effect.acquireRelease(
            Ref.modify(running, (turns) =>
              conversationIds.some((conversationId) => HashMap.has(turns, conversationId))
                ? [false, turns] as const
                : [true, conversationIds.reduce((held, conversationId) => HashMap.set(held, conversationId, hold), turns)] as const),
            (held) =>
              Effect.andThen(
                held
                  ? Ref.update(running, (turns) =>
                    conversationIds.reduce(
                      (left, conversationId) => Option.exists(HashMap.get(left, conversationId), (current) => current === hold) ? HashMap.remove(left, conversationId) : left,
                      turns,
                    ))
                  : Effect.void,
                Deferred.succeed(ended, undefined),
              ),
          );
        }),
      isBusy: (conversationId) => Effect.map(get(conversationId), Option.isSome),
      stop: (conversationId, reason) =>
        Effect.flatMap(get(conversationId), Option.match({
          onNone: () => Effect.succeed(false),
          onSome: (turn) => Effect.as(turn.stop(reason), true),
        })),
      steer: (conversationId, prompt) =>
        Effect.flatMap(get(conversationId), Option.match({
          onNone: () => Effect.succeedNone,
          onSome: (turn) => Effect.asSome(turn.steer(prompt)),
        })),
    });
  }));
}
