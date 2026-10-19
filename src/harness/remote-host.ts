/**
 * The harness host elsewhere: an `alasio harness` (./program.ts), reached over HTTP as
 * ./wire.ts says, which the control plane reaches as HarnessHost, as it reaches the host in
 * its process (./host.ts). What fails there fails here as the same error; a host not
 * reached, or answering as none of alasio's does, fails a call with HarnessHostError,
 * what it says of its conversation and its stopping a turn among them, so that what
 * decides by them refuses, saying why. A call that lasts (./wire.ts) skips what says only
 * that it does.
 *
 * Its events are not heard here: it tells them as its HostEvents says (./host-events.ts), on
 * the log, where the control plane hears them. The operator's panels are reached where the
 * control plane says, as the host elsewhere does not serve them yet (14.6).
 */
import { Effect, Layer, Option, Schema } from "effect";

import { HarnessHost } from "./host.ts";
import { HarnessHostError } from "./index.ts";
import {
  Alive,
  BackgroundWorkAnswer,
  DrainAnswer,
  type CrossingError,
  crossingErrorOf,
  decodeTurnAnswer,
  Failure,
  FreshSessionAnswer,
  HOST_PATHS,
  InterruptAnswer,
  ReleaseAnswer,
  Reserved,
  StatusAnswer,
  SteerAnswer,
  TurnAnswered,
} from "./wire.ts";

/** Where the host elsewhere is, the token it takes, and what the panels reach instead. */
export interface RemoteHostOptions {
  /** The host's address, `http://<host>:<port>`. */
  readonly url: string;
  readonly token: string;
  readonly panels: HarnessHost["Service"]["panels"];
}

const errorText = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/** `body`, as `schema` reads it, or a HarnessHostError where the host answered as none of alasio's does. */
const read = <A>(schema: Schema.ConstraintDecoder<A, never>, body: unknown): Effect.Effect<A, HarnessHostError> =>
  Schema.decodeUnknownEffect(schema)(body).pipe(Effect.mapError((error) => new HarnessHostError({ reason: `it answered as no harness host does: ${error.message}` })));

/**
 * The lines of an answer that lasts, read as they come, but those that say only that it
 * does (Alive): the next of them, or a HarnessHostError where the answer ends first.
 */
function linesOf(body: ReadableStream<Uint8Array>, what: string): { readonly next: Effect.Effect<unknown, HarnessHostError> } {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  return {
    next: Effect.tryPromise({
      try: async () => {
        for (;;) {
          const end = buffered.indexOf("\n");
          if (end >= 0) {
            const line: unknown = JSON.parse(buffered.slice(0, end));
            buffered = buffered.slice(end + 1);
            if (!Schema.is(Alive)(line)) return line;
            continue;
          }
          const { value, done } = await reader.read();
          if (done) throw new Error(`it ended its answer to ${what} before it said`);
          buffered += value;
        }
      },
      catch: (cause) => new HarnessHostError({ reason: errorText(cause) }),
    }),
  };
}

function makeRemoteHost({ url, token, panels }: RemoteHostOptions): HarnessHost["Service"] {
  const headers = { authorization: `Bearer ${token}`, "content-type": "application/json" };

  /** The host's answer to `path` with `body`: its status and what it said, or a HarnessHostError where it was not reached. */
  const post = (path: string, body: object, signal?: AbortSignal) =>
    Effect.tryPromise({
      try: (interrupted) => fetch(`${url}${path}`, { method: "POST", headers, body: JSON.stringify(body), signal: signal ?? interrupted }),
      catch: (cause) => new HarnessHostError({ reason: errorText(cause) }),
    });

  /** What a call answered, as `schema` reads it; what it failed with, as the host failed with it. */
  const call = <A>(path: string, body: object, schema: Schema.ConstraintDecoder<A, never>): Effect.Effect<A, CrossingError | HarnessHostError> =>
    Effect.gen(function*() {
      const response = yield* post(path, body);
      const said = yield* Effect.tryPromise({ try: () => response.json() as Promise<unknown>, catch: (cause) => new HarnessHostError({ reason: errorText(cause) }) });
      if (response.status === 200) return yield* read(schema, said);
      const failure = yield* read(Failure, said);
      return yield* Effect.fail(crossingErrorOf(failure) ?? new HarnessHostError({ reason: failure.error.message }));
    });

  /** An answer that lasts: its lines, where the host answered with them; what it failed with otherwise, as the host failed with it. */
  const lasting = (path: string, body: object, what: string, signal?: AbortSignal) =>
    Effect.gen(function*() {
      const response = yield* post(path, body, signal);
      const answer = response.body;
      if (response.status === 200 && answer !== null) return linesOf(answer, what);
      const said = yield* Effect.tryPromise({ try: () => response.json() as Promise<unknown>, catch: (cause) => new HarnessHostError({ reason: errorText(cause) }) });
      const failure = yield* read(Failure, said);
      return yield* Effect.fail(crossingErrorOf(failure) ?? new HarnessHostError({ reason: failure.error.message }));
    });

  /** `error`, if the host's status may fail with it, as its reservation may: the store's, or else as the host's not reaching it. */
  const asStatusError = (error: CrossingError | HarnessHostError) => (error._tag === "StoreError" || error._tag === "HarnessHostError" ? error : new HarnessHostError({ reason: error.message }));

  /** `error`, if a steer may fail with it, or else as the host's not reaching it. */
  const asSteerError = (error: CrossingError | HarnessHostError) =>
    error._tag === "StoreError" || error._tag === "ReceivedFileError" || error._tag === "HarnessHostError" ? error : new HarnessHostError({ reason: error.message });

  return HarnessHost.of({
    reserve: (conversationId) =>
      Effect.gen(function*() {
        // The connection the conversation is held on, until the scope closes.
        const holding = new AbortController();
        yield* Effect.addFinalizer(() => Effect.sync(() => holding.abort()));
        const answer = yield* lasting(HOST_PATHS.reserve, { conversationId }, "a reservation", holding.signal).pipe(Effect.mapError(asStatusError));
        const { held, id } = yield* read(Reserved, yield* answer.next);
        // Let go of as the scope closes, and so let go of by then, as the host in alasio's process lets go.
        if (id !== null) {
          yield* Effect.addFinalizer(() =>
            call(HOST_PATHS.release, { conversationId, id }, ReleaseAnswer).pipe(
              Effect.catch((error) => Effect.logWarning(`the harness host's hold of ${conversationId} was left to its connection: ${error.message}`)),
            )
          );
        }
        return held;
      }),
    startTurn: (start) =>
      Effect.gen(function*() {
        const said = yield* (yield* lasting(HOST_PATHS.turns, start, "a turn")).next;
        if (Schema.is(Failure)(said)) return yield* Effect.fail(crossingErrorOf(said) ?? new HarnessHostError({ reason: said.error.message }));
        const { answer } = yield* read(TurnAnswered, said);
        return yield* Effect.mapError(decodeTurnAnswer(answer), (error) => new HarnessHostError({ reason: `it answered as no harness host does: ${error.message}` }));
      }),
    steer: (conversationId, prompt, fileIds) =>
      call(HOST_PATHS.steer, { conversationId, prompt, fileIds }, SteerAnswer).pipe(
        Effect.map(({ taken }) => (taken === null ? Option.none() : Option.some(taken))),
        Effect.mapError(asSteerError),
      ),
    interrupt: (conversationId, reason) =>
      call(HOST_PATHS.interrupt, { conversationId, reason }, InterruptAnswer).pipe(
        Effect.map(({ interrupted }) => interrupted),
        Effect.mapError((error) => (error._tag === "HarnessHostError" ? error : new HarnessHostError({ reason: error.message }))),
      ),
    status: (conversationId) => call(HOST_PATHS.status, { conversationId }, StatusAnswer).pipe(Effect.mapError(asStatusError)),
    drain: (conversationId) => call(HOST_PATHS.drain, { conversationId }, DrainAnswer).pipe(Effect.map(({ drained }) => drained), Effect.mapError(asStatusError)),
    backgroundWork: (conversationId) =>
      call(HOST_PATHS.backgroundWork, { conversationId }, BackgroundWorkAnswer).pipe(Effect.map(({ working }) => working), Effect.mapError(asStatusError)),
    startFreshSession: ({ conversationId, harness, workingDirectory }) =>
      call(HOST_PATHS.freshSession, { conversationId, harness, workingDirectory }, FreshSessionAnswer).pipe(
        Effect.map(({ sessionId }) => sessionId),
        Effect.mapError((error) => (error._tag === "HarnessHostError" || error._tag === "UnknownHarness" || error._tag === "NoWorkspaceMounted" || error._tag === "SessionFilesystemsDisabled" || error._tag === "FolderOnBranch" ? error : new HarnessHostError({ reason: error.message }))),
      ),
    panels,
  });
}

/** The harness host at `url`, as the module says. */
export const remoteHarnessHost = (options: RemoteHostOptions): Layer.Layer<HarnessHost> => Layer.succeed(HarnessHost, makeRemoteHost(options));
