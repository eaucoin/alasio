/**
 * The harness host's interface as alasio harness serves it (src/harness/serve.ts): what
 * lasts says so often enough for a client, or a proxy between, that gives up on a quiet
 * connection, and a turn asked for again under its epoch is the one running.
 */
import assert from "node:assert/strict";
import { request } from "node:http";
import { test } from "node:test";

import { Effect, Exit, Option, Scope } from "effect";

import { HarnessHost } from "../src/harness/host.ts";
import type { TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS } from "../src/harness/names.ts";
import { serveHost } from "../src/harness/serve.ts";
import { HOST_PATHS } from "../src/harness/wire.ts";

const CONVERSATION = "telegram:41";

/** A host whose turns take `turnMs`, counted in `started`, and whose reservations say nothing. */
function slowHost(turnMs: number, started: { count: number }): HarnessHost["Service"] {
  const result: TurnResult = { blockSequence: [], sessionId: "thread-1", pendingResponseId: "pending-1", interrupted: false, responseCompleted: true };
  return HarnessHost.of({
    reserve: () => Effect.succeed(true),
    startTurn: () => Effect.sync(() => void (started.count += 1)).pipe(Effect.andThen(Effect.sleep(turnMs)), Effect.as(result)),
    steer: () => Effect.succeedNone,
    interrupt: () => Effect.succeed(false),
    status: () => Effect.succeed({ busy: false, running: false, cliInitiated: false, held: false, idleSince: 0 }),
    backgroundWork: () => Effect.succeed(false),
    startFreshSession: () => Effect.succeed("thread-2"),
    panels: () => Effect.succeed(Option.none()),
  });
}

/**
 * What a call to `path` with `body` answered, as a client that gives up on a connection
 * quiet for `quietMs` reads it: its lines, until the answer ended or `untilMs` passed; or
 * that it gave up.
 */
function called(port: number, path: string, body: object | null, quietMs: number, untilMs: number): Promise<{ readonly lines: unknown[]; readonly gaveUp: boolean }> {
  return new Promise((resolve, reject) => {
    const lines: unknown[] = [];
    let text = "";
    const sent = request({ host: "127.0.0.1", port, path, method: body === null ? "GET" : "POST", headers: { authorization: "Bearer host-token", "content-type": "application/json" }, timeout: quietMs }, (answer) => {
      answer.setEncoding("utf8");
      answer.on("data", (chunk: string) => {
        text += chunk;
        for (let end = text.indexOf("\n"); end >= 0; end = text.indexOf("\n")) {
          lines.push(JSON.parse(text.slice(0, end)));
          text = text.slice(end + 1);
        }
      });
      answer.on("end", () => resolve({ lines, gaveUp: false }));
    });
    sent.on("timeout", () => {
      sent.destroy();
      resolve({ lines, gaveUp: true });
    });
    sent.on("error", (error) => (sent.destroyed ? undefined : reject(error)));
    setTimeout(() => {
      sent.destroy();
      resolve({ lines, gaveUp: false });
    }, untilMs);
    sent.end(body === null ? undefined : JSON.stringify(body));
  });
}

/** The pending response of a turn's answer among `lines`, whatever keeps it alive about it. */
const answerOf = (lines: readonly unknown[]): string | undefined =>
  lines.map((line) => (line as { readonly answer?: { readonly pendingResponseId?: string } }).answer?.pendingResponseId).find((id) => id !== undefined);

const START = {
  conversationId: CONVERSATION,
  chatId: "41",
  messageId: "10",
  harness: CODEX_HARNESS,
  workingDirectory: "/work",
  prompt: "go",
  fileIds: [],
  resumeSession: null,
  modelChoice: null,
  epoch: 3,
  jobId: null,
  attachedTurn: null,
};

/** Runs `use` with `host` served, its calls that last kept alive every `keepAliveMs`, on the port given it. */
async function serving<A>(host: HarnessHost["Service"], keepAliveMs: number, use: (port: number) => Promise<A>): Promise<A> {
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Scope.provide(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId: CONVERSATION, token: "host-token", keepAliveMs }), HarnessHost, host), scope));
    return await use(port);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
}

test("a turn longer than a client waits on a quiet connection is answered, kept alive meanwhile, as one not kept alive is not", { timeout: 30_000 }, async () => {
  const started = { count: 0 };
  // Kept alive every 100 ms, a turn of 1.5 s reaches a client that gives up after 500 ms quiet.
  const kept = await serving(slowHost(1_500, started), 100, (port) => called(port, HOST_PATHS.turns, START, 500, 5_000));
  assert.equal(kept.gaveUp, false);
  assert.ok(kept.lines.filter((line) => JSON.stringify(line) === JSON.stringify({ alive: true })).length >= 5);
  assert.deepEqual(answerOf(kept.lines), "pending-1");
  // Kept alive less often than the client waits, it is given up on.
  const quiet = await serving(slowHost(1_500, started), 5_000, (port) => called(port, HOST_PATHS.turns, START, 500, 5_000));
  assert.equal(quiet.gaveUp, true);
});

test("a reservation, quiet for longer than a client waits, is kept alive", { timeout: 30_000 }, async () => {
  await serving(slowHost(0, { count: 0 }), 100, async (port) => {
    const held = await called(port, HOST_PATHS.reserve, { conversationId: CONVERSATION }, 500, 1_500);
    assert.equal(held.gaveUp, false);
    assert.ok(held.lines.some((line) => (line as { readonly held?: boolean }).held === true));
    assert.ok(held.lines.filter((line) => (line as { readonly alive?: boolean }).alive === true).length >= 5);
  });
});

test("a turn asked for again under its epoch, as a caller that lost its answer asks again, is the one started then, answered as it is", { timeout: 30_000 }, async () => {
  const started = { count: 0 };
  await serving(slowHost(500, started), 100, async (port) => {
    const [first, again] = await Promise.all([called(port, HOST_PATHS.turns, START, 5_000, 5_000), called(port, HOST_PATHS.turns, START, 5_000, 5_000)]);
    assert.equal(started.count, 1);
    for (const { lines } of [first, again]) assert.equal(answerOf(lines), "pending-1");
    // Asked for again once it has ended, it is answered as it ended, run no more.
    const late = await called(port, HOST_PATHS.turns, START, 5_000, 5_000);
    assert.equal(answerOf(late.lines), "pending-1");
    assert.equal(started.count, 1);
    // A turn of another epoch is another turn.
    await called(port, HOST_PATHS.turns, { ...START, epoch: 4 }, 5_000, 5_000);
    assert.equal(started.count, 2);
  });
});
