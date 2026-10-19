/**
 * An environment's events (./events.ts), as its alasio reads them: its events topic, as a
 * member of its group, `alasio-events.<environment>`, a group of its own beside Telegram's so
 * neither holds the other back (../log/consume.ts). A group told of none, as the first alasio
 * to read it, reads from the next written, as what was told before it began is no one's to
 * act on.
 *
 * Each event is handled once, whichever alasio it is delivered to and however often: its
 * handling runs in a transaction that records it handled (Store.withEventHandled), with what
 * the handling writes, so one delivered again, after a restart or to another alasio, finds
 * it handled and is passed over; one whose handling failed, its writes rolled back with its
 * record, is handled again as it is delivered again. A record it cannot read, of a higher
 * version, or of a kind it does not know, is skipped, saying why (decodeEvent); one of a kind
 * alasio has no handling of is passed over, unrecorded.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import { consumeGroup, errorText } from "../log/consume.ts";
import { joinTopic, type TopicFeed, type TopicLog, type TopicRecord } from "../log/topic.ts";
import type { StoreError } from "../persistence/sql.ts";
import { type EventWrites, Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { decodeEvent, type Event } from "./events.ts";

/** The group could not be joined. */
export class EventsFeedError extends Schema.TaggedError<EventsFeedError>()("EventsFeedError", { message: Schema.String }) {}

/** What joins the environment's events' group: the log's, or a stand-in's in tests. */
export class EnvironmentEvents extends Context.Service<EnvironmentEvents, {
  readonly join: Effect.Effect<TopicFeed, EventsFeedError>;
}>()("alasio/events/EnvironmentEvents") {
  /** The group on the log, as `log` says it. */
  static readonly layer = (log: TopicLog): Layer.Layer<EnvironmentEvents> =>
    Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({
      join: Effect.tryPromise({ try: () => joinTopic(log, "latest"), catch: (error) => new EventsFeedError({ message: `joining ${log.group} on ${log.topic}: ${errorText(error)}` }) }),
    }));
}

/** The handling of an event of the kind `K`, given what it writes in the transaction that records it handled. */
export type EventHandling<K extends Event["kind"]> = (event: Extract<Event, { readonly kind: K }>, writes: EventWrites) => Effect.Effect<void, StoreError>;

/** How alasio handles each kind of event it acts on. */
export type EventHandlers = { readonly [K in Event["kind"]]?: EventHandling<K> };

/** Hands each event of the environment's topic to its kind's handling, once, as the module says, until it is interrupted. */
export const consumeEvents = (handlers: EventHandlers): Effect.Effect<never, never, EnvironmentEvents | Store> =>
  Effect.gen(function*() {
    const { join } = yield* EnvironmentEvents;
    const store = yield* Store;

    const handle = (record: TopicRecord): Effect.Effect<void, StoreError> => {
      const decoded = decodeEvent(record.value ?? "");
      if (decoded._tag === "Skipped") return Effect.logWarning(`skipping ${record.position}: ${decoded.reason}`);
      const { event } = decoded;
      // The handling of the event's kind, as the kinds' handlers say it.
      const handling = handlers[event.kind] as EventHandling<typeof event.kind> | undefined;
      if (handling === undefined) return Effect.logDebug(`${event.kind} ${event.id} has no handling of alasio's`);
      return store.withEventHandled(event.id, event.kind, (writes) => handling(event, writes)).pipe(
        Effect.flatMap(Option.match({
          onNone: () => Effect.logDebug(`${event.kind} ${event.id} was handled already, and is passed over`),
          onSome: () => Effect.void,
        })),
      );
    };

    return yield* consumeGroup({ name: "the environment's events", join, handle });
  }).pipe(withLogScope("events"));
