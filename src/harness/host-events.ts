/**
 * What a harness running on between prompts does on its own (HostEvent): a reply of its own
 * to deliver, and its letting go of the conversation, which its host tells (HostEvents) and
 * the control plane hears (HarnessEvents), to act on.
 *
 * In alasio's process, both are one PubSub of its own (HostEvents.inProcess). A harness pool's
 * pod has no control plane beside it: its host tells them on its environment's events
 * topic of the log (HostEvents.onLog), as its environment's harness principal, which may write
 * that topic alone, and alasio hears them as it reads the topic in its group.
 */
import { Context, Effect, Layer, PubSub, type Scope } from "effect";

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
}>()("alasio/harness/HarnessEvents") {}
