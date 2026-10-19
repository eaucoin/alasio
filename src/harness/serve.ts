/**
 * The harness host of one conversation, served over HTTP as ./wire.ts says, to the caller
 * that bears its token: `alasio harness`'s interface (./main.ts), which the control plane
 * reaches through 14.3's harness gateway.
 *
 * A turn the host starts runs in the server's scope, not the request's: its caller may let
 * go of the call, as alasio does when it stops, and the turn goes on, its writes in the
 * store, under the epoch it was claimed under, as they are wherever it runs; it ends as
 * the turn does, or as the host stops; asked for again under its epoch, as a caller that
 * lost its answer asks again, it is the turn running that answers, not another. A
 * reservation lasts until its caller releases it, or lets go of the connection, as a
 * caller that is gone does. A call that carries a trace context is served in a span of it, its
 * turn and its harness's calls within it, so they join its caller's trace; one that carries
 * none, as the sandbox gateway's asking whether the host is idle, is in no trace.
 */
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";

import { NodeHttpServer } from "@effect/platform-node";
import { Deferred, Effect, Fiber, Option, Schema, type Scope, Stream } from "effect";
import { HttpMiddleware, type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

import type { StoreError } from "../persistence/sql.ts";
import type { TurnFenced } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { BearerToken, sameToken } from "../shared/tokens.ts";
import type { ReceivedFileError } from "../telegram/files.ts";
import { HarnessHost, type TurnStart } from "./host.ts";
import type { HarnessUnavailable, TurnResult } from "./index.ts";
import {
  BackgroundWorkAnswer,
  BackgroundWorkRequest,
  DrainAnswer,
  DrainRequest,
  encodeTurnAnswer,
  failureOf,
  FreshSessionAnswer,
  FreshSessionRequest,
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

/** What serveHost is given: where it listens, the conversation it serves, the token its caller bears, and how often a call that lasts says so (KEEP_ALIVE_MS unless a test says). */
export interface HostServerOptions {
  readonly port: number;
  readonly host?: string | undefined;
  readonly conversationId: string;
  readonly token: string;
  readonly keepAliveMs?: number | undefined;
}

/** How many of the latest turns' answers are kept, for a caller that asks again (serveHost). */
const TURNS_KEPT = 8;

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
  { port, host: hostname = "0.0.0.0", conversationId, token, keepAliveMs = KEEP_ALIVE_MS }: HostServerOptions,
): Effect.fn.Return<{ readonly port: number }, HttpServerError.ServeError, Scope.Scope | HarnessHost> {
  const host = yield* HarnessHost;
  const scope = yield* Effect.scope;

  /**
   * The latest turns, by the epoch each was claimed under and the message it answers,
   * running or ended: a turn asked for again so, as a caller that lost its answer asks
   * again, is the one started then, answered as it is, not another. The latest TURNS_KEPT
   * alone are kept, as a conversation's epochs only grow and an answer is asked for again
   * soon or never.
   */
  const turns = new Map<string, { readonly epoch: number; readonly fiber: Fiber.Fiber<TurnResult, StoreError | TurnFenced | HarnessUnavailable | ReceivedFileError> }>();
  const turnOf = (start: TurnStart) =>
    Effect.suspend(() => {
      const key = `${start.epoch}:${start.messageId}`;
      const started = turns.get(key);
      if (started) return Effect.succeed(started.fiber);
      return Effect.forkIn(host.startTurn(start), scope).pipe(
        Effect.tap((fiber) =>
          Effect.sync(() => {
            turns.set(key, { epoch: start.epoch, fiber });
            for (const [kept] of [...turns].sort(([, a], [, b]) => b.epoch - a.epoch).slice(TURNS_KEPT)) turns.delete(kept);
          })
        ),
      );
    });

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
            Stream.fromEffect(turnOf(turnStartOf(start)).pipe(
              Effect.flatMap(Fiber.join),
              Effect.map((result): object => ({ answer: encodeTurnAnswer(result) })),
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
      case HOST_PATHS.freshSession:
        return Effect.flatMap(body(FreshSessionRequest), ({ harness, workingDirectory }) =>
          host.startFreshSession({ conversationId, harness, workingDirectory }).pipe(
            Effect.map((sessionId) => answer(200, Schema.encodeSync(FreshSessionAnswer)({ sessionId }))),
            Effect.mapError((error) => failed(422, error)),
          ));
      default:
        return Effect.fail(failed(404, { _tag: "BadRequest", message: `${path} is no call of the host's` }));
    }
  };

  const handle = Effect.gen(function*() {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const bearer = Schema.decodeUnknownOption(BearerToken)(request.headers["authorization"]);
    if (Option.isNone(bearer) || !sameToken(bearer.value[1], token)) return failed(401, { _tag: "Unauthorized", message: "the request bears no token of this host's" });
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
