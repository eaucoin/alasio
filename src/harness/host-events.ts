/**
 * What a harness running on between prompts does on its own (HostEvent): a reply of its own
 * to deliver, and its letting go of the conversation, which its host tells (HostEvents) and
 * the control plane hears (HarnessEvents), to act on.
 *
 * A harness pool's pod has no control plane beside it, nor any principal of the log: its host
 * tells them to the sandbox gateway's front (HostEvents.throughFront), with its binding's token, and the gateway
 * tells them on its environment's events topic as its own, of the pod's own conversation alone
 * (../gateway/harnesses.ts relayed); alasio hears them as it reads the topic in its group
 * (HarnessEvents.fromLog, ../events/consume.ts), a host's kinds as told of a host (source
 * "harness") alone, and the bindings' as told by the gateway ("sandbox-gateway") alone, each
 * handled once, whichever alasio it is delivered to. A binding let go of, released or lost, lets
 * go of its conversation as the hosts' idleness does, so a prompt that came as a host was drained
 * runs once its binding is let go of. What alasio does on hearing one is its turns' to do again: a
 * reply delivered once, as the store marks it so, and a conversation's waiting prompts run as it
 * is free, as alasio's start does both of what an alasio that stopped left, so one heard just
 * before an alasio stops is not lost.
 */
import { Context, Deferred, Duration, Effect, Layer, PubSub, Schedule, Schema, type Scope } from "effect";

import { EVENTS_PATH } from "../binder/names.ts";
import { EventsConsumer } from "../events/consume.ts";
import type { Event } from "../events/events.ts";

/** What a harness running on between prompts did on its own, for the control plane to act on. */
export type HostEvent =
  /** It has a reply of its own completed in the store, to deliver. */
  | { readonly kind: "background-response"; readonly conversationId: string }
  /** It let go of the conversation, which may run what waits on it. */
  | { readonly kind: "idle"; readonly conversationId: string };

/** How long a telling the front cannot take yet is told again, at most: as its watch of the bindings catches up, or the gateway starts again. */
const TELL_PATIENCE = Duration.minutes(1);
/** How often it is told again meanwhile. */
const TELL_RETRY = Duration.seconds(1);

/** A telling the front could not take now, as it answered 503 or was not reached, which is told again. */
class NotTaken extends Schema.TaggedError<NotTaken>()("NotTaken", { reason: Schema.String }) {}
/** A telling the front refused, which is not told again. */
class Refused extends Schema.TaggedError<Refused>()("Refused", { reason: Schema.String }) {}

/** Where a host tells its events. */
export class HostEvents extends Context.Service<HostEvents, {
  /** Tells of `event`; never fails, one not told being logged where it is told elsewhere. */
  readonly tell: (event: HostEvent) => Effect.Effect<void>;
}>()("alasio/harness/HostEvents") {
  /**
   * Told to the sandbox gateway's front at `url` (EVENTS_PATH), bearing the pod's binding's
   * `token`, as the module says: `harness.<kind>`, of the conversation, by its key; told again
   * while the front cannot take it yet, every TELL_RETRY for TELL_PATIENCE, and logged where it
   * is not told.
   */
  static readonly throughFront = ({ url, token }: { readonly url: string; readonly token: string }): Layer.Layer<HostEvents> =>
    Layer.succeed(HostEvents, HostEvents.of({
      tell: (event) => {
        const kind = `harness.${event.kind}`;
        const once = Effect.tryPromise({
          try: (signal) =>
            fetch(`${url}${EVENTS_PATH}`, {
              method: "POST",
              headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
              body: JSON.stringify({ kind, subject: { kind: "conversation", key: event.conversationId } }),
              signal,
            }).then(async (answer) => ({ status: answer.status, said: await answer.text() })),
          catch: (cause) => new NotTaken({ reason: cause instanceof Error ? cause.message : String(cause) }),
        }).pipe(Effect.flatMap(({ status, said }): Effect.Effect<void, NotTaken | Refused> => {
          if (status === 202) return Effect.void;
          const reason = `the sandbox gateway answered ${status}: ${said}`;
          return Effect.fail(status === 503 ? new NotTaken({ reason }) : new Refused({ reason }));
        }));
        return once.pipe(
          Effect.retry({ while: (error) => error._tag === "NotTaken", schedule: Schedule.max([Schedule.spaced(TELL_RETRY), Schedule.during(TELL_PATIENCE)]) }),
          Effect.catch((error) => Effect.logWarning(`could not tell of ${kind} of ${event.conversationId}: ${error.reason}`)),
        );
      },
    }));
}

/** What tells of events of `kind`: a host's kinds the gateway as of the host, the bindings' the gateway as its own. */
const toldBy = (kind: Event["kind"]): Event["source"] => (kind === "harness.idle" || kind === "harness.background-response" ? "harness" : "sandbox-gateway");

/** Where the control plane hears its hosts' events. */
export class HarnessEvents extends Context.Service<HarnessEvents, {
  /** Subscribes to the hosts' events, as they are heard from now on, for as long as the scope lasts. */
  readonly subscribe: Effect.Effect<PubSub.Subscription<HostEvent>, never, Scope.Scope>;
}>()("alasio/harness/HarnessEvents") {
  /**
   * Heard on the environment's events topic, as the module says, for as long as the layer
   * lasts; `written` told of each event a host told, and of each binding let go of, as what the
   * host committed since is the lake's to load, the pod telling the lake's loader nothing itself.
   */
  static readonly fromLog = ({ written = () => {} }: { readonly written?: (() => void) | undefined } = {}): Layer.Layer<HarnessEvents, never, EventsConsumer> => Layer.effect(
    HarnessEvents,
    Effect.gen(function*() {
      const heard = yield* PubSub.unbounded<HostEvent>();
      const told = (event: HostEvent): Effect.Effect<void> => Effect.andThen(Effect.sync(written), Effect.asVoid(PubSub.publish(heard, event)));
      /** `handling` of an event told by what tells of its kind; one told by another passed over, said so. */
      const fromItsTeller = <E extends Event, R>(handling: (event: E) => Effect.Effect<void, R>) => (event: E): Effect.Effect<void, R> =>
        event.source === toldBy(event.kind)
          ? handling(event)
          : Effect.logWarning(`passing over ${event.kind} ${event.id} of ${event.subject.key}, as ${event.source} tells of no ${event.kind}`);
      // Read once the turns listen, as an event handled is not handled again: none heard before
      // they subscribe, and so lost.
      const listened = yield* Deferred.make<void>();
      yield* (yield* EventsConsumer).handle({
        "harness.background-response": fromItsTeller((event) => told({ kind: "background-response", conversationId: event.subject.key })),
        "harness.idle": fromItsTeller((event) => told({ kind: "idle", conversationId: event.subject.key })),
        // A binding let go of, released or lost, lets go of its conversation too: what waited on it,
        // as its host was drained, runs, bound anew; and its grant reaches nothing from then on.
        "harness.lost": (event, writes) =>
          fromItsTeller((lost: typeof event) =>
            Effect.logWarning(`${lost.subject.key}'s harness's binding was lost, its pod ${lost.pod} gone while it was bound (${lost.reason}): its next turn binds it anew`).pipe(
              Effect.andThen(writes.revokeHarnessGrant(lost.subject.key, lost.binding)),
              Effect.andThen(told({ kind: "idle", conversationId: lost.subject.key })),
            )
          )(event),
        "harness.released": (event, writes) =>
          fromItsTeller((released: typeof event) =>
            Effect.logInfo(`${released.subject.key}'s harness was released (${released.reason})`).pipe(
              Effect.andThen(writes.revokeHarnessGrant(released.subject.key, released.binding)),
              Effect.andThen(told({ kind: "idle", conversationId: released.subject.key })),
            )
          )(event),
      }, Deferred.await(listened));
      return HarnessEvents.of({ subscribe: PubSub.subscribe(heard).pipe(Effect.tap(() => Deferred.succeed(listened, undefined))) });
    }),
  );
}
