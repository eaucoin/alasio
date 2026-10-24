/**
 * An environment's events: what the sandbox gateway tells its alasio of what becomes of its
 * conversations' harnesses' bindings, and of what the harness hosts of its harness pools tell it
 * their harnesses do on their own, which hold no principal of the log (../gateway/harnesses.ts relayed), on its
 * events topic in the log, `events.<environment>` (eventsTopic), which its alasio reads in its
 * group (cli/src/manifests/log.ts).
 *
 * A record's key is its subject's key, the workspace's or the conversation's, so one
 * subject's events keep their order on a partition. Its value is JSON, an envelope of
 * EVENTS_VERSION: `{ v, id, kind, at, environment, source, subject: { kind, key } }`, a
 * UUID per record so a consumer can drop one delivered twice, the time it was made (ms),
 * what made it, and what its kind says beside. A consumer skips a record of a
 * higher version, or of a kind it does not know, saying why, and never fails on one
 * (decodeEvent): a writer can add kinds, or move to a new version, before every reader can
 * read them.
 *
 * Kinds, each one alasio acts on, as nothing else reads the topic (alasio's handlers of them,
 * HOST_EVENT_KINDS and FORK_EVENT_KINDS, are all of them):
 * - `harness.lost`: a conversation's harness's binding whose pod went while it was bound;
 * - `harness.released`: a conversation's harness's binding let go; one released as its host
 *   was killed for its memory (`oom`) saying the limit it ran out of;
 * - `harness.background-response`: a conversation's harness, running on between prompts, has
 *   a reply of its own completed in the store, to deliver;
 * - `harness.idle`: it let go of the conversation, which may run what waits on it;
 * - `workspace.fork-answered`: a branch's alasio's request to fork one of its parent's
 *   sessions into it (src/branch/fork.ts), as its parent's alasio answered it.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import { ProduceAcks, Producer } from "@platformatic/kafka";
import { Clock, Config, Context, Effect, Exit, Layer, Schema, type Scope } from "effect";

import { withLogScope } from "../shared/log.ts";
import { EVENTS_BROKERS_VARIABLE, EVENTS_PASSWORD_FILE_VARIABLE, EVENTS_USERNAME_VARIABLE, eventsTopic } from "./names.ts";

/** The envelope's version this release writes, and reads up to. */
export const EVENTS_VERSION = 1;

/** What an event is about: a session's workspace, by its key, or a conversation, by its. */
const Subject = Schema.Struct({ kind: Schema.Literals(["workspace", "conversation"]), key: Schema.String });

/** What tells of events: the sandbox gateway, of what it binds; a harness pool's host, of what its harness does on its own, through the gateway, which writes it so; a parent's alasio, of its branches' forks it answered. */
export const EVENT_SOURCES = ["sandbox-gateway", "harness", "alasio"] as const;

const envelope = {
  v: Schema.Literal(EVENTS_VERSION),
  id: Schema.String,
  at: Schema.Number,
  environment: Schema.String,
  source: Schema.Literals(EVENT_SOURCES),
  subject: Subject,
};

const HarnessLost = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.lost"), binding: Schema.String, pod: Schema.String, reason: Schema.String });
const HarnessReleased = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.released"), binding: Schema.String, reason: Schema.String, memoryLimit: Schema.optional(Schema.String) });
const HarnessBackgroundResponse = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.background-response") });
const HarnessIdle = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.idle") });

/**
 * What became of a branch's request to fork its parent's session `volumeId`, the request's id
 * `request`: forked, with the internet it was made with; not the parent's to give; a turn
 * running in it; or refused, saying why; signed by the parent with the branch's token, as no one
 * else who may write the branch's events holds it (../branch/fork.ts).
 */
const WorkspaceForkAnswered = Schema.Struct({
  ...envelope,
  kind: Schema.Literal("workspace.fork-answered"),
  request: Schema.String,
  volumeId: Schema.String,
  outcome: Schema.Literals(["forked", "unknown", "busy", "refused"]),
  netMode: Schema.NullOr(Schema.Literals(["none", "full"])),
  reason: Schema.NullOr(Schema.String),
  signature: Schema.String,
});

/** An event, of any kind this release knows. */
export const Event = Schema.Union([HarnessLost, HarnessReleased, HarnessBackgroundResponse, HarnessIdle, WorkspaceForkAnswered]);
export type Event = typeof Event.Type;

/** The kinds this release knows. */
export const EVENT_KINDS: ReadonlySet<string> = new Set(Event.members.map((member) => member.fields.kind.literal));

/** An event as its writer gives it: all but what each record has of its own, its version, id and time. */
export type EventInput = Event extends infer Each ? (Each extends Event ? Omit<Each, "v" | "id" | "at"> : never) : never;

/** A record of the topic as a consumer reads it: an event it knows, or one it skips, and why. */
export type Decoded = { readonly _tag: "Known"; readonly event: Event } | { readonly _tag: "Skipped"; readonly reason: string };

const decode = Schema.decodeUnknownExit(Event);

/** A record's value, as the module says a consumer reads it. */
export function decodeEvent(value: Uint8Array | string): Decoded {
  let parsed: unknown;
  try {
    parsed = JSON.parse(typeof value === "string" ? value : Buffer.from(value).toString("utf8"));
  } catch {
    return { _tag: "Skipped", reason: "not JSON" };
  }
  if (typeof parsed !== "object" || parsed === null) return { _tag: "Skipped", reason: "not an object" };
  const { v, kind } = parsed as { readonly v?: unknown; readonly kind?: unknown };
  if (typeof v !== "number" || v > EVENTS_VERSION) return { _tag: "Skipped", reason: `of version ${String(v)}, past ${EVENTS_VERSION}` };
  if (typeof kind !== "string" || !EVENT_KINDS.has(kind)) return { _tag: "Skipped", reason: `of a kind not known, ${String(kind)}` };
  const decoded = decode(parsed);
  return Exit.isSuccess(decoded) ? { _tag: "Known", event: decoded.value } : { _tag: "Skipped", reason: `a ${kind} not as its kind says: ${String(decoded.cause)}` };
}

/** The record of `event`, made `at`: its topic, its key, and its value. */
export function eventRecord(event: EventInput, at: number, id = randomUUID()): { readonly topic: string; readonly key: string; readonly value: string } {
  return { topic: eventsTopic(event.environment), key: event.subject.key, value: JSON.stringify({ v: EVENTS_VERSION, id, at, ...event }) };
}

/** Where the events topics are: the log's brokers, and the principal that writes them. */
export interface EventsLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
}

/** An event the log did not take. */
export class EventNotTold extends Schema.TaggedError<EventNotTold>()("EventNotTold", { message: Schema.String }) {}

/** What tells of events: on the log, or nothing, as in tests. */
export class Events extends Context.Service<Events, {
  /** Tells of `event`, on its environment's topic; never fails: one not written is logged. */
  readonly tell: (event: EventInput) => Effect.Effect<void>;
  /** Tells of `event`, on its environment's topic, failing where the log did not take it, for what tells to tell again. */
  readonly send: (event: EventInput) => Effect.Effect<void, EventNotTold>;
}>()("alasio/events/Events") {
  /** Nothing told. */
  static readonly none = Layer.succeed(Events, Events.of({ tell: () => Effect.void, send: () => Effect.void }));

  /** On the log, as `log` says, with @platformatic/kafka, alasio's one Kafka client, until the scope closes; each record written once every node has it. */
  static readonly make = (log: EventsLog): Effect.Effect<Events["Service"], never, Scope.Scope> =>
    Effect.gen(function*() {
      const producer = yield* Effect.acquireRelease(
        Effect.sync(() =>
          new Producer({
            clientId: log.username,
            bootstrapBrokers: [...log.brokers],
            sasl: { mechanism: "SCRAM-SHA-512", username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
            acks: ProduceAcks.ALL,
          })
        ),
        (made) => Effect.promise(() => made.close().catch(() => {})),
      );
      const send = (event: EventInput): Effect.Effect<void, EventNotTold> =>
        Effect.gen(function*() {
          const record = eventRecord(event, yield* Clock.currentTimeMillis);
          yield* Effect.tryPromise({
            try: () => producer.send({ messages: [{ topic: record.topic, key: Buffer.from(record.key), value: Buffer.from(record.value) }] }),
            catch: (error) => new EventNotTold({ message: `could not tell of ${event.kind} of ${event.subject.key}: ${error instanceof Error ? error.message : String(error)}` }),
          });
        });
      return Events.of({
        send,
        tell: (event) => send(event).pipe(Effect.catch((error) => Effect.logWarning(error.message)), withLogScope("events")),
      });
    });

  /** On the log its variables name (./names.ts), as `make` is. */
  static readonly fromEnvironment: Effect.Effect<Events["Service"], Config.ConfigError, Scope.Scope> = Effect.gen(function*() {
    const brokers = yield* Config.String(EVENTS_BROKERS_VARIABLE);
    const username = yield* Config.String(EVENTS_USERNAME_VARIABLE);
    const passwordFile = yield* Config.String(EVENTS_PASSWORD_FILE_VARIABLE);
    return yield* Events.make({ brokers: brokers.split(","), username, passwordFile });
  });
}
