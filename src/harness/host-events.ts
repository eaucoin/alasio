/**
 * What a harness running on between prompts does on its own (HostEvent): a reply of its own
 * to deliver, and its letting go of the conversation, which its host tells (HostEvents) and
 * the control plane hears (HarnessEvents), to act on.
 *
 * In alasio's process, both are one PubSub of its own (HostEvents.inProcess). A harness pool's
 * pod has no control plane beside it: its host tells them on its environment's events
 * topic of the log (HostEvents.onLog), as its environment's harness principal, which may write
 * that topic alone, and alasio hears them as it reads the topic in its group
 * (HarnessEvents.fromLog, ../events/consume.ts), each handled once, whichever alasio it is
 * delivered to, as of what the sandbox gateway tells of its conversations' harnesses'
 * bindings, which it says, and which let go of their conversations as the hosts' idleness does,
 * so a prompt that came as a host was drained runs once its binding is let go of. What alasio
 * does on hearing one is its turns' to do again: a reply
 * delivered once, as the store marks it so, and a conversation's waiting prompts run as it is
 * free, as alasio's start does both of what an alasio that stopped left, so one heard just
 * before an alasio stops is not lost.
 */
import { Context, Deferred, Effect, Layer, PubSub, type Scope } from "effect";

import { EventsConsumer } from "../events/consume.ts";
import { Events } from "../events/events.ts";

/** What a harness running on between prompts did on its own, for the control plane to act on. */
export type HostEvent =
  /** It has a reply of its own completed in the store, to deliver. */
  | { readonly kind: "background-response"; readonly conversationId: string }
  /** It let go of the conversation, which may run what waits on it. */
  | { readonly kind: "idle"; readonly conversationId: string };

/** Where a host tells its events. */
export class HostEvents extends Context.Service<HostEvents, {
  /** Tells of `event`; never fails, one not told being logged where it is told elsewhere. */
  readonly tell: (event: HostEvent) => Effect.Effect<void>;
}>()("alasio/harness/HostEvents") {
  /** Told, and heard, in alasio's process. */
  static readonly inProcess: Layer.Layer<HostEvents | HarnessEvents> = Layer.effectContext(
    Effect.map(PubSub.unbounded<HostEvent>(), (events) =>
      Context.make(HostEvents, HostEvents.of({ tell: (event) => Effect.asVoid(PubSub.publish(events, event)) })).pipe(
        Context.add(HarnessEvents, HarnessEvents.of({ subscribe: PubSub.subscribe(events) })),
      )),
  );

  /** Told on the events topic of the environment `environment`, as Events tells them: `harness.<kind>`, of the conversation, by its key. */
  static readonly onLog = (environment: string): Layer.Layer<HostEvents, never, Events> =>
    Layer.effect(
      HostEvents,
      Effect.map(Events, (events) =>
        HostEvents.of({
          tell: (event) => events.tell({ kind: `harness.${event.kind}`, environment, source: "harness", subject: { kind: "conversation", key: event.conversationId } }),
        })),
    );
}

/** Where the control plane hears its hosts' events. */
export class HarnessEvents extends Context.Service<HarnessEvents, {
  /** Subscribes to the hosts' events, as they are heard from now on, for as long as the scope lasts. */
  readonly subscribe: Effect.Effect<PubSub.Subscription<HostEvent>, never, Scope.Scope>;
}>()("alasio/harness/HarnessEvents") {
  /** Heard on the environment's events topic, as the module says, for as long as the layer lasts. */
  static readonly fromLog: Layer.Layer<HarnessEvents, never, EventsConsumer> = Layer.effect(
    HarnessEvents,
    Effect.gen(function*() {
      const heard = yield* PubSub.unbounded<HostEvent>();
      const told = (event: HostEvent): Effect.Effect<void> => Effect.asVoid(PubSub.publish(heard, event));
      // Read once the turns listen, as an event handled is not handled again: none heard before
      // they subscribe, and so lost.
      const listened = yield* Deferred.make<void>();
      yield* (yield* EventsConsumer).handle({
        "harness.background-response": (event) => told({ kind: "background-response", conversationId: event.subject.key }),
        "harness.idle": (event) => told({ kind: "idle", conversationId: event.subject.key }),
        // A binding let go of, released or lost, lets go of its conversation too: what waited on it,
        // as its host was drained, runs, bound anew; and its grant reaches nothing from then on.
        "harness.lost": (event, writes) =>
          Effect.logWarning(`${event.subject.key}'s harness's binding was lost, its pod ${event.pod} gone while it was bound (${event.reason}): its next turn binds it anew`).pipe(
            Effect.andThen(writes.revokeHarnessGrant(event.subject.key, event.binding)),
            Effect.andThen(told({ kind: "idle", conversationId: event.subject.key })),
          ),
        "harness.released": (event, writes) =>
          Effect.logInfo(`${event.subject.key}'s harness was released (${event.reason})`).pipe(
            Effect.andThen(writes.revokeHarnessGrant(event.subject.key, event.binding)),
            Effect.andThen(told({ kind: "idle", conversationId: event.subject.key })),
          ),
      }, Deferred.await(listened));
      return HarnessEvents.of({ subscribe: PubSub.subscribe(heard).pipe(Effect.tap(() => Deferred.succeed(listened, undefined))) });
    }),
  );
}
