/**
 * How a branch environment's alasio comes by the sessions its conversations inherited,
 * without holding anything of the alasio it was branched from (its parent): no right in
 * the parent's namespaces, no credential of its own to the parent's workspaces, and no way
 * to it but the log.
 *
 * The parent forks them for it. A branch asks on its parent's forks topic (forksTopic,
 * ./names.ts), as its alasio's principal may write it, by a request keyed by the branch:
 * `{ v, id, branch, volumeId, at, signature }`, its signature an HMAC of the rest with the
 * branch's token, forkToken(key, branch), and `at` when the branch made it, by its own clock.
 * The parent reads its forks topic in its group (forksGroup), from the topic's first request
 * kept where the group was told of none, and of each request:
 * - passes over one not signed with the token of the branch it names, as the key says, saying
 *   so, and answers it nothing, as its answer would go to that branch's events, which whoever
 *   wrote it chose; one that comes before the parent has its key is passed over so too, and its
 *   branch's wait for an answer runs out;
 * - answers one it answered before as it did then, as it records each it answers (Store
 *   recordForkRequest) by its branch and the id the branch gave it, so one delivered again is
 *   not forked twice; one whose id its branch gave a request for another session is refused;
 * - refuses one not made within FORK_WAIT, by its own clock, give or take CLOCK_SKEW, by which
 *   the two clocks may differ;
 * - forks its session `volumeId` into the branch's environment under the same id, through
 *   the sandbox gateway (../sandbox/index.ts fork): only a session it made, or one of its
 *   conversations knows, and none while a turn runs in it, so no turn writes into it as it is
 *   cloned: the conversations that mount it are held busy while it forks (../harness/holds.ts),
 *   so none starts a turn under it, and their prompts that came meanwhile run after; a session
 *   forked already is forked;
 * and answers each on the branch's events topic, `workspace.fork-answered`, signed with the
 * branch's token, which the gateways, writing the branch's events too, do not hold: forked, with
 * the internet it was made with; unknown; busy, a turn running in it; or refused, saying why. An
 * answer the log does not take leaves the request to be read again, and answered from its
 * record.
 *
 * The key is the stack's (neon/control/kube-setup.ts), which the parent reads as each
 * request comes, from a Secret that may be made after it started; each branch is given
 * its own token by `alasio branch create`, and can sign no other branch's.
 *
 * The branch asks as it first brings an inherited session up (parentForks). Whichever of its
 * replicas reads the answer, signed with its token, records it in the branch's own store
 * (forkAnswered, as its events are handled), which the one that asked reads until the answer
 * is there, for FORK_WAIT at most; one not so signed is passed over. A session forked is found
 * in the branch's own environment, under the id its conversations already name, so their
 * working directories and harness sessions stay as they are, and recorded as a session
 * workspace it made, with the internet its parent says.
 */
import { createHmac, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";

import { ProduceAcks, Producer } from "@platformatic/kafka";
import { Clock, Context, Duration, Effect, Layer, Option, Schedule, Schema, type Scope } from "effect";

import { nameProblem } from "../../neon/control/branches.ts";
import { Turns } from "../codex/turns.ts";
import type { EventHandling } from "../events/consume.ts";
import { Events } from "../events/events.ts";
import { whileHeld } from "../harness/holds.ts";
import { HarnessHost } from "../harness/host.ts";
import { consumeGroup, errorText } from "../log/consume.ts";
import { type JoinTopic, joinTopic, type TopicFeed, type TopicLog, type TopicRecord } from "../log/topic.ts";
import type { ForkAnswer } from "../persistence/fork-repository.ts";
import type { StoreError } from "../persistence/sql.ts";
import { Store } from "../persistence/store.ts";
import { type NetMode, SessionInheritError, SessionSandboxes } from "../sandbox/index.ts";
import { isValidVolumeId } from "../sandbox/names.ts";
import { withLogScope } from "../shared/log.ts";
import { sameToken } from "../shared/tokens.ts";
import { sessionFsWorkspace } from "../workspace/kind.ts";
import { forkToken } from "./names.ts";

/** How long a branch waits for its parent's answer to a request, and how old a request its parent still answers but by refusing. */
export const FORK_WAIT = Duration.minutes(10);
/** By how much a branch's clock and its parent's may differ, as the parent reads when the branch made a request. */
export const CLOCK_SKEW = Duration.minutes(2);
/** How often a branch reads its own store for its parent's answer. */
const ANSWER_POLL = Duration.seconds(1);

/** The version of a request this release writes, and reads. */
const REQUEST_VERSION = 1;
/** The longest id of a request's the parent reads: a UUID's is 36 characters. */
const MAX_REQUEST_ID = 64;

const ForkRequest = Schema.Struct({
  v: Schema.Literal(REQUEST_VERSION),
  id: Schema.String,
  branch: Schema.String,
  volumeId: Schema.String,
  at: Schema.Number,
  signature: Schema.String,
});
type ForkRequest = typeof ForkRequest.Type;

/** The signature of a request of `branch`'s, with its token: an HMAC of all it says. */
const signatureOf = (token: string, { id, branch, volumeId, at }: Omit<ForkRequest, "v" | "signature">): string =>
  createHmac("sha256", token).update(`alasio-branch-fork-request:${branch}:${volumeId}:${id}:${at}`).digest("base64url");

/** What the parent answers its branch `branch`'s request `request` for its session `volumeId`. */
interface AnswerTold extends ForkAnswer {
  readonly branch: string;
  readonly request: string;
  readonly volumeId: string;
}

/** The signature of an answer to a request of its branch's, with the branch's token: an HMAC of all it says, so the branch takes its parent's answers alone. */
export const answerSignatureOf = (token: string, { branch, request, volumeId, outcome, netMode, reason }: AnswerTold): string =>
  createHmac("sha256", token).update(`alasio-branch-fork-answer:${JSON.stringify([branch, request, volumeId, outcome, netMode, reason])}`).digest("base64url");

/** The record of the request `id` of `branch`'s, signed with its `token`, to fork `volumeId`, made `at`: its key, the branch, and its value. */
export const forkRequestRecord = (token: string, request: Omit<ForkRequest, "v" | "signature">): { readonly key: string; readonly value: string } => ({
  key: request.branch,
  value: JSON.stringify({ v: REQUEST_VERSION, ...request, signature: signatureOf(token, request) }),
});

/** What the parent reads its branches' requests on: its forks topic in its group, on the log, or a stand-in's in tests. */
export class ForkRequests extends Context.Service<ForkRequests, {
  readonly join: Effect.Effect<TopicFeed, SessionInheritError>;
}>()("alasio/branch/ForkRequests") {
  /** The group on the log, as `log` says it, from the topic's first request still kept where the group has been told of none, as a branch may ask before its parent first reads; joined by `join`, the log's but in tests. */
  static readonly layer = (log: TopicLog, join: JoinTopic = joinTopic): Layer.Layer<ForkRequests> =>
    Layer.succeed(ForkRequests, ForkRequests.of({
      join: Effect.tryPromise({ try: () => join(log, "earliest"), catch: (error) => new SessionInheritError({ message: `joining ${log.group} on ${log.topic}: ${errorText(error)}` }) }),
    }));
}

/** Where a branch asks: the log's brokers, its alasio's principal, and its parent's forks topic. */
export interface ForkAsksLog {
  readonly brokers: readonly string[];
  readonly username: string;
  readonly passwordFile: string;
  readonly topic: string;
}

/** What a branch asks its parent with: a request's record written on its parent's forks topic. */
export class ForkAsks extends Context.Service<ForkAsks, {
  readonly ask: (record: { readonly key: string; readonly value: string }) => Effect.Effect<void, SessionInheritError>;
}>()("alasio/branch/ForkAsks") {
  /** On the log, as `log` says, until the scope closes; each record written once every node has it. */
  static readonly make = (log: ForkAsksLog): Effect.Effect<ForkAsks["Service"], never, Scope.Scope> =>
    Effect.map(
      Effect.acquireRelease(
        Effect.sync(() =>
          new Producer({
            clientId: log.username,
            bootstrapBrokers: [...log.brokers],
            sasl: { mechanism: "SCRAM-SHA-512", username: log.username, password: readFileSync(log.passwordFile, "utf8").trim() },
            acks: ProduceAcks.ALL,
          })
        ),
        (made) => Effect.promise(() => made.close().catch(() => {})),
      ),
      (producer) =>
        ForkAsks.of({
          ask: ({ key, value }) =>
            Effect.asVoid(Effect.tryPromise({
              try: () => producer.send({ messages: [{ topic: log.topic, key: Buffer.from(key), value: Buffer.from(value) }] }),
              catch: (error) => new SessionInheritError({ message: `the alasio this branch was branched from could not be asked to fork a session: ${errorText(error)}` }),
            })),
        }),
    );
}

/** What the parent does with a request of the branch `branch` for its session `volumeId`, as the module says: its answer; failing where its store did. */
const forkFor = (branch: string, volumeId: string): Effect.Effect<ForkAnswer, StoreError, Store | HarnessHost | Turns | SessionSandboxes> =>
  Effect.gen(function*() {
    const store = yield* Store;
    const harnessHost = yield* HarnessHost;
    const turns = yield* Turns;
    const sandboxes = yield* SessionSandboxes;
    const unknown: ForkAnswer = { outcome: "unknown", netMode: null, reason: `alasio has no session ${volumeId}` };
    if (!isValidVolumeId(volumeId)) return unknown;
    const knowing = yield* store.listWorkspaceConversations(sessionFsWorkspace(volumeId));
    const made = (yield* store.listSessionWorkspaces).find((workspace) => workspace.volumeId === volumeId && workspace.madeAt !== null);
    if (knowing.length === 0 && !made) return unknown;
    const netMode: NetMode = made?.netMode ?? "none";
    const mounting = knowing.filter(({ mounted }) => mounted).map(({ conversationId }) => conversationId);
    const forked = yield* whileHeld(
      harnessHost,
      store,
      mounting,
      `forking session ${volumeId} for the branch ${branch}`,
      (begun) =>
        Effect.andThen(
          begun,
          sandboxes.volumes.fork(volumeId, volumeId, branch).pipe(
            // Forked already, by an earlier request of the branch's.
            Effect.catchIf((error) => error._tag === "GatewayError" && error.status === 409, () => Effect.void),
          ),
        ),
    ).pipe(
      Effect.ensuring(Effect.forEach(mounting, turns.schedule, { discard: true })),
      Effect.map(Option.match({
        onNone: (): ForkAnswer => ({ outcome: "busy", netMode: null, reason: `a turn runs in the session ${volumeId} now, which would write into it as it is cloned: send again once it ends` }),
        onSome: (): ForkAnswer => ({ outcome: "forked", netMode, reason: null }),
      })),
      // What was not the store's failing is the branch's to hear; the store's is the request's to be read again for.
      Effect.catch((error) => (error._tag === "StoreError" ? Effect.fail(error) : Effect.succeed<ForkAnswer>({ outcome: "refused", netMode: null, reason: error.message }))),
    );
    if (forked.outcome === "forked") yield* Effect.logInfo(`forked session ${volumeId} for the branch ${branch}`);
    return forked;
  });

/** Whether the request `request` was made within the time its branch waits for its answer, give or take the clocks' skew, as the parent's clock says it `now`. */
const madeInTime = (request: ForkRequest, now: number): boolean => {
  const skew = Duration.toMillis(CLOCK_SKEW);
  return request.at <= now + skew && request.at >= now - Duration.toMillis(FORK_WAIT) - skew;
};

/**
 * Answers the branches' requests to fork the parent's sessions, as the module says, read from
 * its forks topic until it is interrupted, its key read from `keyFile` as each request comes.
 */
export const serveBranchForks = ({ keyFile }: { readonly keyFile: string }): Effect.Effect<never, never, ForkRequests | Events | Store | HarnessHost | Turns | SessionSandboxes> =>
  Effect.gen(function*() {
    const { join } = yield* ForkRequests;
    const events = yield* Events;
    const store = yield* Store;
    const services = yield* Effect.context<Store | HarnessHost | Turns | SessionSandboxes>();

    const handle = (record: TopicRecord): Effect.Effect<void, StoreError | { readonly message: string }> =>
      Effect.gen(function*() {
        const decoded = Schema.decodeUnknownOption(ForkRequest)((() => {
          try {
            return JSON.parse(record.value?.toString("utf8") ?? "");
          } catch {
            return null;
          }
        })());
        if (Option.isNone(decoded) || nameProblem(decoded.value.branch) !== null || decoded.value.id.length > MAX_REQUEST_ID) {
          return yield* Effect.logWarning(`skipping ${record.position}, which is no branch's request to fork a session`);
        }
        const request = decoded.value;
        const key = yield* Effect.promise(() => readFile(keyFile, "utf8").then((text) => text.trim(), () => ""));
        if (!key) return yield* Effect.logWarning(`skipping ${record.position}, a request of the branch ${request.branch}'s to fork a session: there is no key yet to tell its token by`);
        const token = forkToken(key, request.branch);
        if (!sameToken(request.signature, signatureOf(token, request))) {
          return yield* Effect.logWarning(`skipping ${record.position}, a request to fork a session that bears no token of the branch ${request.branch}'s`);
        }
        // Looked up, then forked, then recorded, not in one transaction: of two handlings of one
        // request at once, a redelivery's or another alasio's, both may fork, the second answered
        // by the gateway as forked already (409), and the first record's answer is told by both.
        const answered = (yield* store.forkRequestAnswered(request.branch, request.id)) ?? (madeInTime(request, yield* Clock.currentTimeMillis)
          ? yield* store.recordForkRequest(request.branch, request.id, request.volumeId, yield* forkFor(request.branch, request.volumeId).pipe(Effect.provideContext(services)))
          : { volumeId: request.volumeId, answer: { outcome: "refused", netMode: null, reason: "the request was not made within the time a branch waits for its answer" } as const });
        const answer: ForkAnswer = answered.volumeId === request.volumeId
          ? answered.answer
          : { outcome: "refused", netMode: null, reason: `the request ${request.id} was made for another session, ${answered.volumeId}` };
        const told: AnswerTold = { branch: request.branch, request: request.id, volumeId: request.volumeId, ...answer };
        yield* events.send({
          kind: "workspace.fork-answered",
          environment: request.branch,
          source: "alasio",
          subject: { kind: "workspace", key: `sessionfs:${request.volumeId}` },
          request: request.id,
          volumeId: request.volumeId,
          ...answer,
          signature: answerSignatureOf(token, told),
        });
      });

    return yield* consumeGroup({ name: "the branches' requests to fork sessions", join, handle });
  }).pipe(withLogScope("branch-forks"));

/**
 * The branch `branch`'s events' handling of its parent's answers: each signed with its token,
 * read from `tokenFile`, recorded in its store, with the event's record as handled, for the
 * replica that asked to read; one not so signed, as none but its parent signs one, is passed
 * over, saying so, and the wait for its request's answer runs out.
 */
export const forkAnswered = ({ branch, tokenFile }: ParentForksOptions): EventHandling<"workspace.fork-answered"> => ({ id, request, volumeId, outcome, netMode, reason, signature }, writes) =>
  Effect.gen(function*() {
    const token = yield* Effect.promise(() => readFile(tokenFile, "utf8").then((text) => text.trim(), () => ""));
    const answer: ForkAnswer = { outcome, netMode, reason };
    if (!token || !sameToken(signature, answerSignatureOf(token, { branch, request, volumeId, ...answer }))) {
      return yield* Effect.logWarning(`passing over ${id}, an answer to the request ${request} to fork a session that bears no signature of this branch's parent`);
    }
    yield* writes.recordForkAnswer(request, volumeId, answer);
  });

/** What a branch asks its parent with: its name, and the file holding its token, from its Secret. */
export interface ParentForksOptions {
  readonly branch: string;
  readonly tokenFile: string;
}

/**
 * A branch environment's `inherit` (../sandbox/index.ts): its parent's session forked
 * into its own sessions as it is first brought up here, and the internet it was made with;
 * null for a session the parent does not know. Its request written on its parent's forks
 * topic, its answer read from its own store, where its events' handling records it.
 */
export const parentForks = ({ branch, tokenFile }: ParentForksOptions) => (volumeId: string): Effect.Effect<NetMode | null, SessionInheritError, ForkAsks | Store> =>
  Effect.gen(function*() {
    const asks = yield* ForkAsks;
    const store = yield* Store;
    const token = yield* Effect.tryPromise({
      try: () => readFile(tokenFile, "utf8").then((text) => text.trim()),
      catch: (error) => new SessionInheritError({ message: `this branch's token to ask its parent with could not be read: ${errorText(error)}` }),
    });
    const id = randomUUID();
    yield* asks.ask(forkRequestRecord(token, { id, branch, volumeId, at: yield* Clock.currentTimeMillis }));
    const unanswered = new SessionInheritError({ message: `the alasio this branch was branched from did not answer its request to fork the session ${volumeId} within ${Duration.format(FORK_WAIT)}` });
    const answered = Effect.flatMap(
      store.forkAnswerTo(id).pipe(Effect.mapError((error) => new SessionInheritError({ message: `the answer to this branch's request to fork the session ${volumeId} could not be read: ${error.message}` }))),
      (answer) => (answer === null ? Effect.fail(unanswered) : Effect.succeed(answer)),
    );
    const answer = yield* answered.pipe(
      Effect.retry({ while: (error) => error === unanswered, schedule: Schedule.max([Schedule.spaced(ANSWER_POLL), Schedule.during(FORK_WAIT)]) }),
    );
    switch (answer.outcome) {
      case "forked":
        return answer.netMode ?? "none";
      case "unknown":
        return null;
      default:
        return yield* new SessionInheritError({ message: `the alasio this branch was branched from did not fork the session ${volumeId}: ${answer.reason ?? answer.outcome}` });
    }
  });
