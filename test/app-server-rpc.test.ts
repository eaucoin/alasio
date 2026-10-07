/**
 * JSON-RPC with the Codex app-server (src/codex/app-server/rpc-client.ts), on an
 * app-server process the test stands in for: what alasio writes it, it answers as the
 * test says, and it exits when the test has it exit.
 */
import assert from "node:assert/strict";
import { test } from "node:test";

import { Deferred, Effect, Queue, Stream } from "effect";

import { AppServerExited, type AppServerEnded, type SpawnAppServer } from "../src/codex/app-server/process.ts";
import { makeAppServerRpc } from "../src/codex/app-server/rpc-client.ts";

/** An app-server process that answers `initialize`, and exits with code 1 once `exit` is done. */
function standIn(exit: Deferred.Deferred<void>): SpawnAppServer {
  return () =>
    Effect.gen(function*() {
      const output = yield* Queue.unbounded<string>();
      const ended = yield* Deferred.make<never, AppServerEnded>();
      yield* Effect.forkScoped(Effect.andThen(Deferred.await(exit), Deferred.fail(ended, new AppServerExited({ code: 1, signal: null }))));
      return {
        write: (line: string) => {
          const message = JSON.parse(line) as { readonly id?: number; readonly method?: string };
          return message.method === "initialize" ? Queue.offer(output, JSON.stringify({ id: message.id, result: { userAgent: "stand-in" } })).pipe(Effect.asVoid) : Effect.void;
        },
        lines: Stream.fromQueue(output),
        ended: Deferred.await(ended),
      };
    });
}

test("an app-server that is gone already is answered as gone to whoever asks after, with why, not waited for forever", async () => {
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const exit = yield* Deferred.make<void>();
    const rpc = yield* makeAppServerRpc({ spawn: standIn(exit), onNotification: () => Effect.void });
    yield* rpc.start({ env: {}, cwd: "/" });
    const before = yield* rpc.whenGone;
    yield* Deferred.succeed(exit, undefined);
    assert.equal((yield* Effect.flip(before))._tag, "AppServerExited");
    // Asked only once it has exited, as a turn whose app-server crashed as it started does.
    const after = yield* Effect.flip(Effect.flatten(rpc.whenGone)).pipe(Effect.timeoutOption("2 seconds"));
    assert.equal(after._tag === "Some" ? after.value._tag : "waited", "AppServerExited");
  })));
});
