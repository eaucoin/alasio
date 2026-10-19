/**
 * An environment's events (./events.ts), as its alasio reads them: its events topic, as a
 * member of its group, `alasio-events.<environment>`, a group of its own beside Telegram's so
 * neither holds the other back (../log/consume.ts). A group told of none, as the first alasio
 * to read it, reads main's from the next written, as what was told before it began is no
 * one's to act on, and a branch's from its first, as its topic is made with the branch, and
 * all on it was told for it: its parent's answers to its forks (../branch/fork.ts) among them,
 * which may be written before its first alasio has joined.
 *
 * Each event is handled once, whichever alasio it is delivered to and however often: its
 * handling runs in a transaction that records it handled (Store.withEventHandled), with what
 * the handling writes, so one delivered again, after a restart or to another alasio, finds
 * it handled and is passed over; one whose handling failed, its writes rolled back with its
 * record, is handled again as it is delivered again. A record it cannot read, of a higher
 * version, or of a kind it does not know, is skipped, saying why (decodeEvent); one of a kind
 * alasio has no handling of is passed over, unrecorded.
 */
import { Context, Effect, Layer, Option, Schema, type Scope } from "effect";

import { consumeGroup, errorText } from "../log/consume.ts";
import { type JoinTopic, joinTopic, type TopicFeed, type TopicLog, type TopicRecord } from "../log/topic.ts";
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
  /** The group on the log, as `log` says it, of main's events or a branch's (`branch`), read from where the module says; joined by `join`, the log's but in tests. */
  static readonly layer = (log: TopicLog, { branch }: { readonly branch: boolean }, join: JoinTopic = joinTopic): Layer.Layer<EnvironmentEvents> =>
    Layer.succeed(EnvironmentEvents, EnvironmentEvents.of({
      join: Effect.tryPromise({ try: () => join(log, branch ? "earliest" : "latest"), catch: (error) => new EventsFeedError({ message: `joining ${log.group} on ${log.topic}: ${errorText(error)}` }) }),
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

/**
 * The environment's one member of its events' group, which each part of alasio that acts on
 * events adds its handling to (handle) as alasio is assembled, so no environment runs two
 * members of the group, each holding back the other's events; started once all is assembled
 * (start), it reads once each handling added is ready, as one that would lose what it is
 * handed before it listens says, and hands each event to its kind's handling, as
 * consumeEvents does. A kind is handled by one part alone; one added twice, or a handling
 * added once it is started, is a defect of alasio's assembly.
 */
export class EventsConsumer extends Context.Service<EventsConsumer, {
  /** Adds `handlers` to the member, which reads once `ready` is done, where it is given. */
  readonly handle: (handlers: EventHandlers, ready?: Effect.Effect<void>) => Effect.Effect<void>;
  /** Starts the member, until the scope closes, where any handling was added; none is added after. */
  readonly start: Effect.Effect<void, never, Scope.Scope>;
}>()("alasio/events/EventsConsumer") {
  /** The member, of the group EnvironmentEvents joins, on the store. */
  static readonly layer: Layer.Layer<EventsConsumer, never, EnvironmentEvents | Store> = Layer.effect(
    EventsConsumer,
    Effect.gen(function*() {
      const events = yield* EnvironmentEvents;
      const store = yield* Store;
      const handlers: Record<string, EventHandling<Event["kind"]>> = {};
      const readies: Effect.Effect<void>[] = [];
      let started = false;
      return EventsConsumer.of({
        handle: (added, ready) =>
          Effect.suspend(() => {
            if (started) return Effect.die(new Error(`events of ${Object.keys(added).join(", ")} are handled only once the environment's events are read`));
            const twice = Object.keys(added).filter((kind) => kind in handlers);
            if (twice.length > 0) return Effect.die(new Error(`events of ${twice.join(", ")} are handled twice`));
            Object.assign(handlers, added);
            if (ready) readies.push(ready);
            return Effect.void;
          }),
        start: Effect.suspend(() => {
          started = true;
          if (Object.keys(handlers).length === 0) return Effect.void;
          return Effect.asVoid(Effect.forkScoped(Effect.andThen(Effect.all(readies, { concurrency: "unbounded", discard: true }), consumeEvents(handlers as EventHandlers)).pipe(
            Effect.provideService(EnvironmentEvents, events),
            Effect.provideService(Store, store),
          )));
        }),
      });
    }),
  );
}
