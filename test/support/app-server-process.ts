/**
 * An app-server process in memory, for tests of the app-server client below the app:
 * alasio's spawn of it is recorded, what alasio writes to it is kept, its requests are
 * answered as the test says (initialize always is), and the test sends it lines as the
 * app-server writes them.
 */
import { type Cause, Deferred, Effect, Queue, Stream } from "effect";

import type { ClientRequest, RequestId } from "../../.types/codex/index.js";
import type { AppServerEnded, AppServerProcessOptions, SpawnAppServer } from "../../src/codex/app-server/process.ts";
import { initializeResponse } from "./codex-protocol.ts";

/** A message alasio wrote to the app-server: a request, a notification, or an answer to one of its requests. */
export interface WrittenMessage {
  readonly id?: RequestId;
  readonly method?: string;
  readonly params?: unknown;
  readonly result?: unknown;
  readonly trace?: { readonly traceparent?: string };
}

/** How the test answers a request: with its result, or, for none, it is left unanswered. */
export type Answer = (params: unknown) => unknown;

export interface AppServerProcessStandIn {
  readonly spawn: SpawnAppServer;
  /** How each process was started, in order. */
  readonly spawned: AppServerProcessOptions[];
  /** What alasio wrote, to any process, in order. */
  readonly written: WrittenMessage[];
  /** Sends `message` to alasio from the process started last, as the app-server writes it. */
  send(message: object): void;
  /** Answers every request of `method` from now on with what `answer` returns. */
  answer(method: ClientRequest["method"], answer: Answer): void;
}

export function appServerProcess(): AppServerProcessStandIn {
  const spawned: AppServerProcessOptions[] = [];
  const written: WrittenMessage[] = [];
  const answers = new Map<string, Answer>([["initialize", () => initializeResponse()]]);
  let output: Queue.Queue<string, Cause.Done> | null = null;

  const spawn: SpawnAppServer = (options) =>
    Effect.gen(function*() {
      spawned.push(options);
      const lines = yield* Effect.acquireRelease(Queue.unbounded<string, Cause.Done>(), Queue.end);
      const ended = yield* Deferred.make<never, AppServerEnded>();
      output = lines;
      return {
        write: (line) =>
          Effect.sync(() => {
            // alasio writes JSON-RPC, one message a line.
            const message = JSON.parse(line) as WrittenMessage;
            written.push(message);
            const answer = message.id != null && message.method ? answers.get(message.method) : undefined;
            if (answer) Queue.offerUnsafe(lines, JSON.stringify({ id: message.id, result: answer(message.params) }));
          }),
        lines: Stream.fromQueue(lines),
        ended: Deferred.await(ended),
      };
    });

  return {
    spawn,
    spawned,
    written,
    send(message) {
      if (!output) throw new Error("no app-server process was started");
      Queue.offerUnsafe(output, JSON.stringify(message));
    },
    answer(method, answer) {
      answers.set(method, answer);
    },
  };
}
