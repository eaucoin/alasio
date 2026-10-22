/**
 * The harness host of one conversation, served over HTTP as ./wire.ts says, to the caller
 * that bears its token: `alasio harness`'s interface (./main.ts), which the control plane
 * reaches through the sandbox gateway's front where its harnesses run in pools.
 *
 * A turn the host starts runs in the host's scope, not the request's (HarnessHost.startTurn):
 * its caller may let go of the call, as alasio does when it stops, and the turn goes on, its
 * writes in the store, under the epoch it was claimed under, as they are wherever it runs;
 * asked for again under its epoch, it is the turn running that answers, not another; and a
 * caller that lost its answer attaches to it (HarnessHost.attach), its answer the turn's as it
 * comes to it, or what the host kept of it. A
 * reservation lasts until its caller releases it, or lets go of the connection, as a
 * caller that is gone does. `GET /healthz` answers 200 to anyone once it serves, as a harness
 * pool's pod is ready only once bound. A call that carries a trace context is served in a span
 * of it, its turn and its harness's calls within it, so they join its caller's trace; one that
 * carries none, as the sandbox gateway's asking whether the host is idle, is in no trace. A call
 * that gives a grant (GRANT_HEADER, ./grant.ts), as alasio's of a binding it bound do, has it
 * taken first (`granted`), and is answered once it is: a pool's pod's host reaches its
 * conversation's rows by it from then on.
 */
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Deferred, Effect, Option, Result, Schema, type Scope, Stream } from "effect";
import { HttpMiddleware, type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import { withLogScope } from "../shared/log.ts";
import { BearerToken, sameToken } from "../shared/tokens.ts";
import { GRANT_HEADER, isGrant } from "./grant.ts";
import { HarnessHost, type TurnStart } from "./host.ts";
import {
  AttachRequest,
  AttachUnknown,
  BackgroundWorkAnswer,
  BackgroundWorkRequest,
  CodexLoginAnswer,
  CodexLoginRequest,
  DrainAnswer,
  DrainRequest,
  encodeTurnAnswer,
  failureOf,
  ForkSessionAnswer,
  ForkSessionRequest,
  FreshSessionAnswer,
  FreshSessionRequest,
  GoalAnswer,
  GoalClearAnswer,
  GoalClearRequest,
  GoalReadRequest,
  GoalSetRequest,
  GoalTurnAnswer,
  GoalTurnRequest,
  HOST_PATHS,
  InterruptAnswer,
  InterruptRequest,
  KEEP_ALIVE_MS,
  ReleaseAnswer,
  ReleaseRequest,
  ReserveRequest,
  StatusAnswer,
  StatusRequest,
  SteerAnswer,
  SteerRequest,
  turnStartOf,
  TurnStartRequest,
} from "./wire.ts";

/**
 * What serveHost is given: where it listens, the conversation it serves, the token its caller
 * bears, what takes the grant a call gives (none where nothing does, as a host that reaches its
 * state as no pool's pod), and how often a call that lasts says so (KEEP_ALIVE_MS unless a test
 * says).
 */
export interface HostServerOptions {
  readonly port: number;
  readonly host?: string | undefined;
  readonly conversationId: string;
  readonly token: string;
  readonly granted?: ((grant: string) => Effect.Effect<void, { readonly message: string }>) | undefined;
  /** What the host is given of each turn before it runs it: what alasio brought up for it, its workspace's bayma and its Codex login (./program.ts); nothing where none is given. */
  readonly given?: ((start: TurnStart) => Effect.Effect<void>) | undefined;
  readonly keepAliveMs?: number | undefined;
}

/** `body`, as JSON, with `status`. */
const answer = (status: number, body: object): HttpServerResponse.HttpServerResponse => HttpServerResponse.jsonUnsafe(body, { status });

/** A failure of `error`'s, as the wire says it, with `status`. */
const failed = (status: number, error: { readonly _tag: string; readonly message: string }): HttpServerResponse.HttpServerResponse => answer(status, failureOf(error));

/**
 * Values as the lines of an answer that lasts, a line of JSON each, begun at once and kept
 * alive every `keepAliveMs` (Alive) for as long as `values` last.
 */
const lines = <A, E>(values: Stream.Stream<A, E>, keepAliveMs: number): HttpServerResponse.HttpServerResponse =>
  HttpServerResponse.stream(
    Stream.encodeText(Stream.map(Stream.merge(values, Stream.as(Stream.tick(keepAliveMs), { alive: true }), { haltStrategy: "left" }), (value) => `${JSON.stringify(value)}\n`)),
    { contentType: "application/x-ndjson" },
  );

/**
 * Serves `HarnessHost` for the conversation `conversationId`, until the scope closes: the
 * port it listens on, once it does.
 */
export const serveHost = Effect.fnUntraced(function*(
  { port, host: hostname = "0.0.0.0", conversationId, token, granted, given = () => Effect.void, keepAliveMs = KEEP_ALIVE_MS }: HostServerOptions,
): Effect.fn.Return<{ readonly port: number }, HttpServerError.ServeError, Scope.Scope | HarnessHost> {
  const host = yield* HarnessHost;

  /** The conversation's holds, by id: each let go of once it is released, and closed once the host has let go. */
  const holds = new Map<string, { readonly release: Deferred.Deferred<void>; readonly closed: Deferred.Deferred<void> }>();
  /**
   * A reservation's answer: whether the conversation is held, and the hold, which lasts
   * until it is released or the answer ends. Its closing is said once the host's hold is
   * let go of, which is after it is made, and so let go of first.
   */
  const reservation = Effect.gen(function*() {
    const id = randomUUID();
    const release = yield* Deferred.make<void>();
    const closed = yield* Deferred.make<void>();
    yield* Effect.addFinalizer(() => Effect.andThen(Effect.sync(() => holds.delete(id)), Deferred.succeed(closed, undefined)));
    const held = yield* host.reserve(conversationId);
    if (!held) return Stream.make<[object]>({ held, id: null });
    holds.set(id, { release, closed });
    return Stream.concat(Stream.make<[object]>({ held, id }), Stream.drain(Stream.fromEffect(Deferred.await(release))));
  });

  /** The request's body, as `schema` reads it, of the conversation served; a refusal otherwise. */
  const body = <A extends { readonly conversationId: string }>(schema: Schema.ConstraintDecoder<A, never>) =>
    HttpServerRequest.schemaBodyJson(schema).pipe(
      Effect.mapError((error) => failed(400, { _tag: "BadRequest", message: error.message })),
      Effect.filterOrFail(
        (decoded) => decoded.conversationId === conversationId,
        (decoded) => failed(404, { _tag: "OtherConversation", message: `this host serves ${conversationId}, not ${decoded.conversationId}` }),
      ),
    );

  const route = (method: string, path: string): Effect.Effect<HttpServerResponse.HttpServerResponse, HttpServerResponse.HttpServerResponse, HttpServerRequest.HttpServerRequest> => {
    if (method !== "POST") return Effect.fail(failed(405, { _tag: "BadRequest", message: `${method} ${path} is no call of the host's` }));
    switch (path) {
      case HOST_PATHS.reserve:
        return Effect.map(body(ReserveRequest), () => lines(Stream.unwrap(reservation), keepAliveMs));
      case HOST_PATHS.release:
        return Effect.flatMap(body(ReleaseRequest), ({ id }) => {
          const held = holds.get(id);
          const released = held ? Deferred.succeed(held.release, undefined).pipe(Effect.andThen(Deferred.await(held.closed)), Effect.as(true)) : Effect.succeed(false);
          return Effect.map(released, (released) => answer(200, Schema.encodeSync(ReleaseAnswer)({ released })));
        });
      case HOST_PATHS.turns:
        return Effect.map(body(TurnStartRequest), (start) =>
          lines(
            Stream.fromEffect(Effect.suspend(() => {
              const turn = turnStartOf(start);
              return Effect.andThen(given(turn), host.startTurn(turn));
            }).pipe(
              Effect.map((result): object => ({ answer: encodeTurnAnswer(result) })),
              Effect.catch((error) => Effect.succeed(failureOf(error))),
            )),
            keepAliveMs,
          ));
      case HOST_PATHS.attach:
        return Effect.map(body(AttachRequest), (turn) =>
          lines(
            Stream.fromEffect(host.attach(turn).pipe(
              Effect.map(Option.match({
                onNone: (): object => Schema.encodeSync(AttachUnknown)({ known: false }),
                onSome: (result): object => ({ answer: encodeTurnAnswer(result) }),
              })),
              Effect.catch((error) => Effect.succeed(failureOf(error))),
            )),
            keepAliveMs,
          ));
      case HOST_PATHS.steer:
        return Effect.flatMap(body(SteerRequest), ({ prompt, fileIds }) =>
          host.steer(conversationId, prompt, fileIds).pipe(
            Effect.map((taken) => answer(200, Schema.encodeSync(SteerAnswer)({ taken: Option.getOrNull(taken) }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.interrupt:
        return Effect.flatMap(body(InterruptRequest), ({ reason }) =>
          host.interrupt(conversationId, reason).pipe(
            Effect.map((interrupted) => answer(200, Schema.encodeSync(InterruptAnswer)({ interrupted }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.status:
        return Effect.flatMap(body(StatusRequest), () =>
          host.status(conversationId).pipe(
            Effect.map((status) => answer(200, Schema.encodeSync(StatusAnswer)(status))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.backgroundWork:
        return Effect.flatMap(body(BackgroundWorkRequest), () =>
          host.backgroundWork(conversationId).pipe(
            Effect.map((working) => answer(200, Schema.encodeSync(BackgroundWorkAnswer)({ working }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.drain:
        return Effect.flatMap(body(DrainRequest), () =>
          host.drain(conversationId).pipe(
            Effect.map((drained) => answer(200, Schema.encodeSync(DrainAnswer)({ drained }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.codexLogin:
        return Effect.flatMap(body(CodexLoginRequest), ({ login }) =>
          host.renewCodexLogin(conversationId, login).pipe(
            Effect.map(() => answer(200, Schema.encodeSync(CodexLoginAnswer)({ renewed: true }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.freshSession:
        return Effect.flatMap(body(FreshSessionRequest), ({ harness, workingDirectory }) =>
          host.startFreshSession({ conversationId, harness, workingDirectory }).pipe(
            Effect.map((sessionId) => answer(200, Schema.encodeSync(FreshSessionAnswer)({ sessionId }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.forkSession:
        return Effect.flatMap(body(ForkSessionRequest), ({ harness, workingDirectory, sessionId, beforeUuid }) =>
          host.forkSession({ conversationId, harness, workingDirectory, sessionId, beforeUuid }).pipe(
            Effect.map((forked) => answer(200, Schema.encodeSync(ForkSessionAnswer)({ sessionId: forked }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.goalRead:
        return Effect.flatMap(body(GoalReadRequest), ({ harness, workingDirectory, threadId }) =>
          host.goals({ conversationId, harness, workingDirectory }).read({ threadId }).pipe(
            Effect.map((goal) => answer(200, Schema.encodeSync(GoalAnswer)({ goal }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.goalSet:
        return Effect.flatMap(body(GoalSetRequest), ({ harness, workingDirectory, threadId, objective, status }) =>
          host.goals({ conversationId, harness, workingDirectory }).set({ threadId, objective, status }).pipe(
            Effect.map((goal) => answer(200, Schema.encodeSync(GoalAnswer)({ goal }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.goalClear:
        return Effect.flatMap(body(GoalClearRequest), ({ harness, workingDirectory, threadId }) =>
          host.goals({ conversationId, harness, workingDirectory }).clear({ threadId }).pipe(
            Effect.map((cleared) => answer(200, Schema.encodeSync(GoalClearAnswer)(cleared))),
            Effect.mapError((error) => failed(422, error)),
          ));
      case HOST_PATHS.goalTurn:
        return Effect.flatMap(body(GoalTurnRequest), ({ harness, workingDirectory, threadId, timeoutMs }) =>
          host.goals({ conversationId, harness, workingDirectory }).waitForTurnId(threadId, timeoutMs ?? undefined).pipe(
            Effect.map((turnId) => answer(200, Schema.encodeSync(GoalTurnAnswer)({ turnId }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      default:
        return Effect.fail(failed(404, { _tag: "BadRequest", message: `${path} is no call of the host's` }));
    }
  };

  const handle = Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.method === "GET" && request.url === "/healthz") return answer(200, { ok: true });
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    if (Option.isNone(bearer) || !sameToken(bearer.value[1], token)) return failed(401, { _tag: "Unauthorized", message: "the request bears no token of this host's" });
    const grant = request.headers[GRANT_HEADER];
    if (grant !== undefined) {
      if (!isGrant(grant)) return failed(400, { _tag: "BadRequest", message: "its grant is none alasio makes" });
      if (granted) {
        const taken = yield* Effect.result(granted(grant));
        if (Result.isFailure(taken)) return failed(503, { _tag: "GrantNotTaken", message: `its grant could not be taken: ${taken.failure.message}` });
      }
    }
    return yield* route(request.method, new URL(request.url, "http://host").pathname).pipe(Effect.catch(Effect.succeed));
  }).pipe(withLogScope("harness-host"));

  const listening = createServer();
  const server = yield* NodeHttpServer.make(() => listening, { port, host: hostname });
  // Its server's span of each call continues the trace context it carries; one that carries
  // none is in no trace.
  yield* server.serve(handle).pipe(
    Effect.provideService(HttpMiddleware.TracerDisabledWhen, (request) => request.headers["traceparent"] === undefined),
    Effect.provideService(HttpMiddleware.SpanNameGenerator, (request) => `harness-host ${new URL(request.url, "http://host").pathname}`),
  );
  // What it serves ends as the host stops, rather than holding its stop until each caller
  // lets go: a reservation's, which its caller holds for as long as its turn runs.
  yield* Effect.addFinalizer(() => Effect.sync(() => {
    listening.close();
    listening.closeAllConnections();
  }));
  // Listening on a TCP port, its address is an internet one.
  return { port: server.address._tag === "UnixPathAddress" ? port : server.address.port };
});
