/**
 * A harness host's events, as a test that runs the host in its own process hears them (../../src/harness/host-events.ts):
 * told, and heard, in one PubSub of the test's.
 */
import { Context, Effect, Layer, PubSub } from "effect";

import { HarnessEvents, type HostEvent, HostEvents } from "../../src/harness/host-events.ts";

/** What the host tells, heard by the test as alasio would hear it. */
export const inProcessEvents: Layer.Layer<HostEvents | HarnessEvents> = Layer.effectContext(
  Effect.map(PubSub.unbounded<HostEvent>(), (events) =>
    Context.make(HostEvents, HostEvents.of({ tell: (event) => Effect.asVoid(PubSub.publish(events, event)) })).pipe(
      Context.add(HarnessEvents, HarnessEvents.of({ subscribe: PubSub.subscribe(events) })),
    )),
);
