import type { ClientNotification, RequestId, ServerRequest } from "../../../.types/codex/index.js";
import { Clock, Deferred, type Duration, Effect, Exit, Option, Schema, type Scope, ScopedRef, Semaphore, Stream } from "effect";

import { withLogScope } from "../../shared/log.ts";
import { traceCarrier, type TraceCarrier, withRpcCall } from "../../telemetry/index.ts";
import type { CodexEnv } from "../env.ts";
import type { AppServerEnded, AppServerProcess, AppServerSpawnFailed, CodexBinaryMissing, SpawnAppServer } from "./process.ts";
import {
  type AppServerMethod,
  type AppServerNotification,
  type AppServerParams,
  type AppServerResult,
  serverRequestResponse,
} from "./protocol.ts";

const REQUEST_TIMEOUT = "5 minutes";
const START_TIMEOUT = "20 seconds";

/** Where the app-server process runs: its environment and working directory. */
export interface AppServerScope {
  readonly env: CodexEnv;
  readonly cwd: string;
}

/** The app-server a request or a wait was for was stopped, or replaced. */
export class AppServerStopped extends Schema.TaggedError<AppServerStopped>()("AppServerStopped", {}) {
  override get message(): string {
    return "Codex app-server stopped";
  }
}

/** A request was made with no app-server running. */
export class AppServerNotRunning extends Schema.TaggedError<AppServerNotRunning>()("AppServerNotRunning", {}) {
  override get message(): string {
    return "Codex app-server is not running";
  }
}

/** The app-server did not answer a request in time. */
export class AppServerRequestTimeout extends Schema.TaggedError<AppServerRequestTimeout>()("AppServerRequestTimeout", {
  method: Schema.String,
}) {
  override get message(): string {
    return `Codex app-server request timed out: ${this.method}`;
  }
}

/** The app-server answered a request with a JSON-RPC error. */
export class AppServerRequestFailed extends Schema.TaggedError<AppServerRequestFailed>()("AppServerRequestFailed", {
  error: Schema.Record(Schema.String, Schema.Unknown),
}) {
  override get message(): string {
    return typeof this.error["message"] === "string" ? this.error["message"] : JSON.stringify(this.error);
  }
}

/** Why the app-server a request or a wait was for is gone. */
export type AppServerGone = AppServerEnded | AppServerStopped;

/** How a request fails. */
export type AppServerRequestError = AppServerGone | AppServerNotRunning | AppServerRequestTimeout | AppServerRequestFailed;

/** How starting the app-server fails: it would not start, or not answer `initialize`. */
export type AppServerStartError = CodexBinaryMissing | AppServerSpawnFailed | AppServerRequestError;

/** A line the app-server writes: a JSON-RPC message, of whichever kind. */
const AppServerMessage = Schema.Struct({
  id: Schema.optional(Schema.NullOr(Schema.Union([Schema.String, Schema.Number]))),
  method: Schema.optional(Schema.String),
  params: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
type AppServerMessage = typeof AppServerMessage.Type;

const decodeJson = Schema.decodeUnknownExit(Schema.fromJsonString(Schema.Unknown));
const decodeMessage = Schema.decodeUnknownExit(AppServerMessage);

/** A JSON-RPC message alasio writes: a request, a notification, or an answer to the app-server's request. */
interface OutgoingMessage {
  readonly id?: RequestId;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly trace?: TraceCarrier;
}

/** A started app-server: its process, the requests it has yet to answer, and its end. */
interface Connection {
  readonly process: AppServerProcess;
  readonly pending: Map<RequestId, Deferred.Deferred<unknown, AppServerRequestFailed>>;
  /** Fails with why the app-server is gone, once it is. */
  readonly gone: Deferred.Deferred<never, AppServerGone>;
}

/** JSON-RPC with an app-server process, started when asked for and again once it is gone. */
export interface AppServerRpc {
  /** Starts the app-server in `scope`, unless one is running; one that is gone is replaced. */
  readonly start: (scope: AppServerScope) => Effect.Effect<void, AppServerStartError>;
  /**
   * A request, as a client span named by its method. The request carries the span's
   * trace context, so what the app-server does for it joins alasio's trace.
   */
  readonly request: <M extends AppServerMethod>(method: M, params: AppServerParams<M>, timeout?: Duration.Input) => Effect.Effect<AppServerResult<M>, AppServerRequestError>;
  /** Stops the app-server running, if one is. */
  readonly stop: Effect.Effect<void>;
  /**
   * What fails once the app-server running now is gone, with why; it waits forever
   * when none is running.
   */
  readonly whenGone: Effect.Effect<Effect.Effect<never, AppServerGone>>;
}

export interface AppServerRpcOptions {
  /** How the app-server process is started. */
  readonly spawn: SpawnAppServer;
  /** What is done with each notification the app-server sends, in order. */
  readonly onNotification: (message: AppServerNotification) => Effect.Effect<void>;
}

/**
 * JSON-RPC with an app-server process, until the scope closes. The process is held in
 * a ScopedRef: each process lives in a scope of its own, which replacing it closes
 * before the next is started, and the ScopedRef's own scope closes the last. A ScopedRef
 * rather than an RcRef, because the process is started in the directory and environment
 * of the call that needs it, where an RcRef acquires with one fixed effect, and because
 * alasio stops and replaces it on its own terms, not when no one holds it.
 */
export const makeAppServerRpc = Effect.fnUntraced(function*({ spawn, onNotification }: AppServerRpcOptions): Effect.fn.Return<AppServerRpc, never, Scope.Scope> {
  const current = yield* ScopedRef.make(Option.none<Connection>);
  // One start at a time: whoever comes while one runs finds its app-server.
  const starting = yield* Semaphore.make(1);
  let nextId = 1;

  const running = (connection: Option.Option<Connection>) => Option.filter(connection, ({ gone }) => !Deferred.isDoneUnsafe(gone));

  const write = (connection: Connection, message: OutgoingMessage) => connection.process.write(JSON.stringify(message));

  const call = <M extends AppServerMethod>(
    connect: Effect.Effect<Connection, AppServerNotRunning>,
    method: M,
    params: AppServerParams<M>,
    timeout: Duration.Input,
  ): Effect.Effect<AppServerResult<M>, AppServerRequestError> =>
    Effect.gen(function*() {
      const connection = yield* connect;
      const id = nextId;
      nextId += 1;
      yield* Effect.annotateCurrentSpan("rpc.jsonrpc.request_id", String(id));
      const reply = yield* Deferred.make<unknown, AppServerRequestFailed>();
      connection.pending.set(id, reply);
      const trace = traceCarrier();
      yield* write(connection, { id, method, params, ...(trace ? { trace } : {}) });
      const result = yield* Deferred.await(reply).pipe(
        Effect.raceFirst(Deferred.await(connection.gone)),
        Effect.timeoutOrElse({ duration: timeout, orElse: () => Effect.fail(new AppServerRequestTimeout({ method })) }),
        Effect.ensuring(Effect.sync(() => connection.pending.delete(id))),
      );
      // The app-server answers a request with its method's result.
      return result as AppServerResult<M>;
    }).pipe(withRpcCall({ system: "jsonrpc", service: "codex", method }));

  const handleMessage = (connection: Connection, message: AppServerMessage): Effect.Effect<void> => {
    const { id, method } = message;
    const reply = id == null ? undefined : connection.pending.get(id);
    if (reply) {
      return Deferred.done(reply, message.error ? Exit.fail(new AppServerRequestFailed({ error: message.error })) : Exit.succeed(message.result)).pipe(Effect.asVoid);
    }
    if (id != null && method) {
      // A message with an id and a method is a request of the app-server's, in its protocol.
      return write(connection, { id, result: serverRequestResponse(method as ServerRequest["method"]) });
    }
    if (method) {
      // A message with a method and no id is a notification, in the app-server's protocol.
      return onNotification(message as AppServerNotification);
    }
    return Effect.void;
  };

  const handleLine = (connection: Connection, line: string): Effect.Effect<void> => {
    if (!line.trim()) {
      return Effect.void;
    }
    const json = decodeJson(line);
    if (Exit.isFailure(json)) {
      return Effect.logWarning(`Ignoring non-JSON app-server line: ${line.slice(0, 200)}`);
    }
    const message = decodeMessage(json.value);
    if (Exit.isFailure(message)) {
      return Effect.logWarning(`Ignoring app-server line that is no JSON-RPC message: ${line.slice(0, 200)}`);
    }
    return handleMessage(connection, message.value);
  };

  /** Starts an app-server and initializes it, in the scope the ScopedRef gives it. */
  const connect = Effect.fnUntraced(function*({ env, cwd }: AppServerScope) {
    const startedAt = yield* Clock.currentTimeNanos;
    const connection: Connection = {
      process: yield* spawn({ cwd, env }),
      pending: new Map(),
      gone: yield* Deferred.make<never, AppServerGone>(),
    };
    yield* Effect.addFinalizer(() => Deferred.fail(connection.gone, new AppServerStopped()));
    yield* connection.process.lines.pipe(Stream.runForEach((line) => handleLine(connection, line)), Effect.forkScoped);
    yield* connection.process.ended.pipe(
      Effect.catch((ended) => (ended._tag === "AppServerExited"
        ? Effect.logWarning(`app-server exited code=${ended.code} signal=${ended.signal}`)
        : Effect.logError(`app-server spawn failed: ${ended.message}`)
      ).pipe(Effect.andThen(Deferred.fail(connection.gone, ended)))),
      Effect.forkScoped,
    );
    const init = yield* call(Effect.succeed(connection), "initialize", {
      clientInfo: {
        name: "alasio_telegram",
        title: "Alasio Telegram",
        version: "1.0.0",
      },
      capabilities: {
        experimentalApi: true,
      },
    }, START_TIMEOUT);
    const notification: ClientNotification["method"] = "initialized";
    yield* write(connection, { method: notification, params: null });
    const elapsedMs = Number((yield* Clock.currentTimeNanos) - startedAt) / 1_000_000;
    yield* Effect.logInfo(`initialized ms=${elapsedMs.toFixed(1)} user_agent=${JSON.stringify(init.userAgent)}`);
    return Option.some(connection);
  });

  const stop = ScopedRef.set(current, Effect.succeedNone);

  return {
    start: (scope) => starting.withPermit(Effect.gen(function*() {
      if (Option.isSome(running(yield* ScopedRef.get(current)))) {
        return;
      }
      // The app-server that is gone is let go of before the next starts.
      yield* stop;
      yield* ScopedRef.set(current, connect(scope));
    })).pipe(withLogScope("codex-app-server")),

    request: (method, params, timeout = REQUEST_TIMEOUT) =>
      call(
        Effect.flatMap(ScopedRef.get(current), (connection) => Effect.fromOption(running(connection)).pipe(Effect.mapError(() => new AppServerNotRunning()))),
        method,
        params,
        timeout,
      ),

    stop,

    whenGone: Effect.map(ScopedRef.get(current), (connection) => Option.match(running(connection), {
      onNone: () => Effect.never,
      onSome: ({ gone }) => Deferred.await(gone),
    })),
  };
});
