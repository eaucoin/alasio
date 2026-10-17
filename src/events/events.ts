/**
 * An environment's events: what the gateways that bind for it tell its alasio of what
 * happens to what they bind, on its events topic in the log, `events.<environment>`
 * (eventsTopic), which its alasio reads in its group (cli/src/manifests/log.ts).
 *
 * A record's key is its subject's key, the workspace's or the conversation's, so one
 * subject's events keep their order on a partition. Its value is JSON, an envelope of
 * EVENTS_VERSION: `{ v, id, kind, at, environment, source, subject: { kind, key } }`, a
 * UUID per record so a consumer can drop one delivered twice, the time it was made (ms),
 * which gateway made it, and what its kind says beside. A consumer skips a record of a
 * higher version, or of a kind it does not know, saying why, and never fails on one
 * (decodeEvent): a writer can add kinds, or move to a new version, before every reader can
 * read them.
 *
 * Kinds:
 * - `workspace.exec-ended`: a background exec of a bound session's bayma ended;
 * - `workspace.lost`: a session's binding whose pod, or the mount it served from, went while
 *   it was bound;
 * - `workspace.released`: a session's binding let go: idle, its pod deleted, or broken;
 * - `harness.lost`, `harness.released`: likewise of a conversation's harness's binding.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import { ProduceAcks, Producer } from "@platformatic/kafka";
import { Clock, Context, Effect, Exit, Layer, Schema, type Scope } from "effect";

import { withLogScope } from "../shared/log.ts";
import { eventsTopic } from "./names.ts";

/** The envelope's version this release writes, and reads up to. */
export const EVENTS_VERSION = 1;

/** What an event is about: a session's workspace, by its key, or a conversation, by its. */
const Subject = Schema.Struct({ kind: Schema.Literals(["workspace", "conversation"]), key: Schema.String });

/** The gateways that tell of events. */
export const EVENT_SOURCES = ["sandbox-gateway", "harness-gateway"] as const;

const envelope = {
  v: Schema.Literal(EVENTS_VERSION),
  id: Schema.String,
  at: Schema.Number,
  environment: Schema.String,
  source: Schema.Literals(EVENT_SOURCES),
  subject: Subject,
};

const WorkspaceExecEnded = Schema.Struct({
  ...envelope,
  kind: Schema.Literal("workspace.exec-ended"),
  binding: Schema.String,
  exec: Schema.Struct({ id: Schema.String, exitCode: Schema.NullOr(Schema.Number), endedAt: Schema.Number }),
});
const WorkspaceLost = Schema.Struct({ ...envelope, kind: Schema.Literal("workspace.lost"), binding: Schema.String, pod: Schema.String, reason: Schema.String });
const WorkspaceReleased = Schema.Struct({ ...envelope, kind: Schema.Literal("workspace.released"), binding: Schema.String, reason: Schema.Literals(["idle", "deleted", "broken"]) });
const HarnessLost = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.lost"), binding: Schema.String, pod: Schema.String, reason: Schema.String });
const HarnessReleased = Schema.Struct({ ...envelope, kind: Schema.Literal("harness.released"), binding: Schema.String, reason: Schema.String });

/** An event, of any kind this release knows. */
export const Event = Schema.Union([WorkspaceExecEnded, WorkspaceLost, WorkspaceReleased, HarnessLost, HarnessReleased]);
export type Event = typeof Event.Type;

/** The kinds this release knows. */
export const EVENT_KINDS: ReadonlySet<string> = new Set(["workspace.exec-ended", "workspace.lost", "workspace.released", "harness.lost", "harness.released"]);

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

/** What tells of events: on the log, or nothing, as in tests. */
export class Events extends Context.Service<Events, {
  /** Tells of `event`, on its environment's topic; never fails: one not written is logged. */
  readonly tell: (event: EventInput) => Effect.Effect<void>;
}>()("alasio/events/Events") {
  /** Nothing told. */
  static readonly none = Layer.succeed(Events, Events.of({ tell: () => Effect.void }));

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
      return Events.of({
        tell: (event) =>
          Effect.gen(function*() {
            const record = eventRecord(event, yield* Clock.currentTimeMillis);
            yield* Effect.tryPromise(() => producer.send({ messages: [{ topic: record.topic, key: Buffer.from(record.key), value: Buffer.from(record.value) }] }));
          }).pipe(
            Effect.catch((error) => Effect.logWarning(`could not tell of ${event.kind} of ${event.subject.key}: ${error.message}`)),
            withLogScope("events"),
          ),
      });
    });
}
