/** A harness pool's pod's bayma, as its turns give it (src/harness/given-bayma.ts). */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";

import { Effect, Exit, Schema, Scope } from "effect";

import { makeGivenBayma } from "../src/harness/given-bayma.ts";
import type { TurnStart } from "../src/harness/host.ts";
import { CLAUDE_HARNESS } from "../src/harness/names.ts";
import { TurnStartRequest } from "../src/harness/wire.ts";

/** A stand-in sandbox gateway on the loopback: each request's method, path, bearer and body, answered 200 with `ok`, a file's bytes. */
async function standInGateway(): Promise<{ readonly url: string; readonly asked: { readonly method: string; readonly path: string; readonly bearer: string; readonly body: string }[]; close: () => Promise<void> }> {
  const asked: { method: string; path: string; bearer: string; body: string }[] = [];
  const server = createServer(async (incoming, outgoing) => {
    const chunks: Buffer[] = [];
    for await (const chunk of incoming as AsyncIterable<Buffer>) chunks.push(chunk);
    asked.push({ method: incoming.method ?? "", path: incoming.url ?? "", bearer: String(incoming.headers.authorization), body: Buffer.concat(chunks).toString() });
    if (incoming.method === "PUT") return void outgoing.writeHead(204).end();
    outgoing.writeHead(200, { "content-type": "application/octet-stream" }).end("ok");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, asked, close: () => new Promise((resolve) => server.close(() => resolve())) };
}

/** A turn's start of the conversation `telegram:41`, in `workingDirectory`, giving `bayma`. */
const start = (workingDirectory: string, bayma: TurnStart["bayma"]): TurnStart => ({
  conversationId: "telegram:41",
  chatId: "41",
  messageId: "10",
  harness: CLAUDE_HARNESS,
  workingDirectory,
  prompt: "go",
  fileIds: [],
  resumeSession: null,
  modelChoice: null,
  epoch: 1,
  jobId: null,
  attachedTurn: null,
  bayma,
  codex: null,
});

test("a turn's start carries its workspace's bayma, or why it has none, across to a host elsewhere, as it was", () => {
  for (const bayma of [{ endpoint: { url: "http://gateway/mcp", headers: { Authorization: "Bearer t" } }, notice: null }, { endpoint: null, notice: "its bayma did not start" }, null] as const) {
    const encoded = JSON.parse(JSON.stringify(start("/work", bayma))) as unknown;
    assert.deepEqual(Schema.decodeUnknownSync(TurnStartRequest)(encoded), start("/work", bayma));
  }
});

test("a pod's harness reaches each turn's bayma at its loopback, with the bearer the latest turn gave, and its session's files through the gateway by it; none before a turn gives it", async () => {
  const gateway = await standInGateway();
  const scope = Effect.runSync(Scope.make());
  try {
    const given = await Effect.runPromise(Scope.provide(makeGivenBayma({ gatewayUrl: gateway.url, environment: "main", stateDir: "/state" }), scope));
    // No turn has given it yet: a folder's turn runs without it, saying why; a session's files are reached by no token.
    const before = await Effect.runPromise(given.folderBayma({ harness: CLAUDE_HARNESS, threadKey: "telegram:41", workingDirectory: "/work" }));
    assert.equal(before.server, null);
    assert.match(before.notice ?? "", /^No turn has said where this conversation's bayma is yet/u);
    const unreached = await Effect.runPromise(Effect.flip(given.sessions.writeFile("fs-abc123", "/home/agent/note", Buffer.from("x"))));
    assert.match(unreached.message, /no turn has given its token/u);
    // A folder's turn: its harness reaches the bayma it gave at the loopback.
    await Effect.runPromise(given.give(start("/work", { endpoint: { url: `${gateway.url}/mcp`, headers: { Authorization: "Bearer folder-token" } }, notice: null })));
    const folder = await Effect.runPromise(given.folderBayma({ harness: CLAUDE_HARNESS, threadKey: "telegram:41", workingDirectory: "/work" }));
    assert.deepEqual(folder.notice, null);
    assert.match(folder.server?.url ?? "", /^http:\/\/127\.0\.0\.1:\d+\/mcp\/[\w-]+$/u);
    const reach = (endpoint: { readonly url: string; readonly headers: Readonly<Record<string, string>> } | null | undefined) => fetch(endpoint?.url ?? "", { method: "POST", headers: endpoint?.headers ?? {}, body: "{}" });
    await reach(folder.server);
    assert.deepEqual(gateway.asked.at(-1), { method: "POST", path: "/mcp", bearer: "Bearer folder-token", body: "{}" });
    // A session's turn: the loopback passes on with its bearer, and its files are written and read through the gateway by it.
    await Effect.runPromise(given.give(start("sessionfs:fs-abc123", { endpoint: { url: `${gateway.url}/mcp`, headers: { Authorization: "Bearer session-token" } }, notice: null })));
    const { bayma } = await Effect.runPromise(given.sessions.ensureSession("fs-abc123"));
    assert.notEqual(bayma.url, folder.server?.url, "each workspace at an endpoint of its own");
    await reach(bayma);
    assert.equal(gateway.asked.at(-1)?.bearer, "Bearer session-token");
    await Effect.runPromise(given.sessions.writeFile("fs-abc123", "/home/agent/.alasio/received/1/note.txt", Buffer.from("noted")));
    assert.deepEqual(gateway.asked.at(-1), { method: "PUT", path: "/workspace/files?path=home%2F.alasio%2Freceived%2F1%2Fnote.txt", bearer: "Bearer session-token", body: "noted" });
    assert.deepEqual(await Effect.runPromise(given.sessions.readFile("fs-abc123", "/workspace/out.txt", 100)), { bytes: Buffer.from("ok") });
    assert.equal(gateway.asked.at(-1)?.bearer, "Bearer session-token");
    // The next turn's bearer, fresh, in place of the last.
    await Effect.runPromise(given.give(start("sessionfs:fs-abc123", { endpoint: { url: `${gateway.url}/mcp`, headers: { Authorization: "Bearer fresher-token" } }, notice: null })));
    await reach(bayma);
    assert.equal(gateway.asked.at(-1)?.bearer, "Bearer fresher-token");
    // A turn whose bayma did not start: its folder's harness told why.
    await Effect.runPromise(given.give(start("/work", { endpoint: null, notice: "its bayma did not start" })));
    assert.deepEqual(await Effect.runPromise(given.folderBayma({ harness: CLAUDE_HARNESS, threadKey: "telegram:41", workingDirectory: "/work" })), { server: null, notice: "its bayma did not start" });
    // What only alasio does with sessions, no pod does.
    assert.equal(Exit.isFailure(await Effect.runPromiseExit(given.sessions.volumes.create("fs-new123"))), true);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    await gateway.close();
  }
});
