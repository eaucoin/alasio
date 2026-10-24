/**
 * The harness host as alasio reaches it where its harnesses run in pools (src/harness/
 * pooled-host.ts), through a stand-in for the sandbox gateway's front: it checks each call's
 * caller's token, answers one that binds nothing of a conversation it has not bound as the
 * front does, and passes the rest on to a host served as alasio harness serves it.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, request } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";

import { Context, Effect, Layer, Option, Scope, Exit } from "effect";

import { NO_BIND_HEADER, PINNED_HEADER } from "../src/binder/names.ts";
import { CodexAppServer } from "../src/codex/app-server/client.ts";
import { CodexLoginFreshness, CodexLoginRefused } from "../src/codex/login-refresh.ts";
import { answeredByFront } from "../src/binder/server.ts";
import { type TokenClaims, verifyToken } from "../src/binder/tokens.ts";
import { poolLoginKey } from "../src/harness/names.ts";
import { GRANT_HEADER, grantDigest, grantKey, harnessGrant } from "../src/harness/grant.ts";
import { type Attached, HarnessHost, type TurnCodex, type TurnRequest, type TurnStart } from "../src/harness/host.ts";
import { BusyPods } from "../src/kube/busy.ts";
import { Harnesses, type TurnResult } from "../src/harness/index.ts";
import { CODEX_HARNESS, HARNESS_NAMES } from "../src/harness/names.ts";
import { CODEX_LOGIN_RELAY, HARNESS_BINDING, pooledHarnessHost } from "../src/harness/pooled-host.ts";
import { serveHost } from "../src/harness/serve.ts";
import { HOST_PATHS } from "../src/harness/wire.ts";
import { type FolderBaymaScope, HostBayma } from "../src/mcp/bayma.ts";
import { Store } from "../src/persistence/store.ts";
import { SessionSandboxes } from "../src/sandbox/index.ts";
import { sessionFsWorkspace } from "../src/workspace/kind.ts";
import { newSchema, run, testPool, testStore } from "./support/store.ts";
import { eventually } from "./support/wait.ts";

/** The result an attach answered, which it must have. */
function answered(attached: Attached): TurnResult {
  assert.ok(attached._tag === "Answered", JSON.stringify(attached));
  return attached.result;
}

const CALLER_KEY = Buffer.from("the environment's key");

/** A relay of the Codex login on `port`, a free one unless given, refusing every request, as one asked without a token does: its URL, as a turn's Codex is given it. */
async function relayAt(port = 0): Promise<{ readonly url: string; readonly close: () => void }> {
  const server = createServer((_incoming, outgoing) => void outgoing.writeHead(401).end("refused"));
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/v1`, close: () => server.close() };
}
/** The environment's relay of its Codex login, as the tests' turns reach it. */
const RELAY = await relayAt();
after(() => RELAY.close());

/** The operator's Codex home, as alasio has it: its configuration, and its login, which no pod is given; removed as the file's tests end. */
const CODEX_HOME = mkdtempSync(join(tmpdir(), "alasio-pooled-codex-"));
after(() => rmSync(CODEX_HOME, { recursive: true, force: true }));
mkdirSync(join(CODEX_HOME, "skills", "review"), { recursive: true });
writeFileSync(join(CODEX_HOME, "config.toml"), 'model = "gpt-5"\n');
writeFileSync(join(CODEX_HOME, "AGENTS.md"), "Be brief.\n");
writeFileSync(join(CODEX_HOME, "skills", "review", "SKILL.md"), "Review it.\n");
writeFileSync(join(CODEX_HOME, "auth.json"), '{"tokens":{"access_token":"the operator\'s"}}');
mkdirSync(join(CODEX_HOME, "sessions"), { recursive: true });
writeFileSync(join(CODEX_HOME, "sessions", "rollout.jsonl"), "{}\n");

/** alasio's own pod, as BusyPods holds it: how many hold it now, and how many did as each turn ran. */
const pod = { holds: 0, heldDuring: [] as number[] };
const busyPods = BusyPods.of({ hold: Effect.acquireRelease(Effect.sync(() => void (pod.holds += 1)), () => Effect.sync(() => void (pod.holds -= 1))) });

/** A host of one conversation, as a pool's pod serves it, recording the turns it is started with, and the Codex logins it is renewed with; each turn taking `turnMs`. */
/** What a pod's host is given of each turn, as it starts it (serveHost's given), recorded in `started`. */
const givenTo = (started: TurnStart[]) => (start: TurnStart) => Effect.sync(() => void started.push(start));

function podHost(started: TurnStart[], { renewed = [], turnMs = 0 }: { readonly renewed?: TurnCodex["login"][]; readonly turnMs?: number } = {}): HarnessHost["Service"] {
  return HarnessHost.of({
    reserve: () => Effect.succeed(true),
    startTurn: () =>
      Effect.sync(() => void pod.heldDuring.push(pod.holds)).pipe(
        Effect.andThen(Effect.sleep(turnMs)),
        Effect.as({ blockSequence: [], sessionId: "thread-1", pendingResponseId: "pending-1", interrupted: false, responseCompleted: true }),
      ),
    attach: ({ epoch, messageId }) =>
      Effect.succeed<Attached>(started.some((start) => start.epoch === epoch && start.messageId === messageId)
        ? { _tag: "Answered", result: { blockSequence: [], sessionId: "thread-1", pendingResponseId: "pending-1", interrupted: false, responseCompleted: true } }
        : { _tag: "Unknown" }),
    steer: () => Effect.succeed(Option.some(true)),
    interrupt: () => Effect.succeed(true),
    status: () => Effect.succeed({ busy: true, running: true, cliInitiated: false, held: false, idleSince: null }),
    backgroundWork: () => Effect.succeed(true),
    drain: () => Effect.succeed(true),
    renewCodexLogin: (_conversationId, login) => Effect.sync(() => void renewed.push(login)),
    startFreshSession: () => Effect.succeed("thread-2"),
    forkSession: () => Effect.succeed(null),
    goals: () => ({ read: () => Effect.succeed(null), set: () => Effect.succeed(null), clear: () => Effect.succeed({ cleared: false }), waitForTurnId: () => Effect.succeed(null) }),
  });
}

/**
 * A stand-in for the front: the claims of each call's token, whether it bound nothing, and the
 * grant it gave the pod, as it was asked; a bind asked ahead (POST /bind) answered each of `refusals` in turn, as one
 * the front cannot make yet or holds is, then its binding, binding the conversation; a call that binds
 * nothing answered 409 `{ bound: false }` until the conversation is bound, as a call that binds
 * does; the rest passed on to the host at `podPort`; `onBind` run as each bind is asked.
 */
async function front(podPort: number, { refusals = [], onBind = () => {}, movesOn }: { readonly refusals?: readonly object[]; readonly onBind?: () => void; readonly movesOn?: string } = {}): Promise<{ url: string; asked: { claims: TokenClaims | null; bindsNothing: boolean; path: string; grant: string | undefined; pinned: string | undefined }[]; close: () => void }> {
  const asked: { claims: TokenClaims | null; bindsNothing: boolean; path: string; grant: string | undefined; pinned: string | undefined }[] = [];
  const refusing = [...refusals];
  let bound = false;
  /** The binding the key is bound to, a new one once the pod is gone (movesOn). */
  let binding = "binding-1";
  const server = createServer((incoming, outgoing) => {
    const claims = verifyToken(CALLER_KEY, String(incoming.headers.authorization).slice("Bearer ".length), Date.now());
    const bindsNothing = incoming.headers[NO_BIND_HEADER] === "never";
    const grant = incoming.headers[GRANT_HEADER];
    const pinned = incoming.headers[PINNED_HEADER];
    asked.push({ claims, bindsNothing, path: incoming.url ?? "", grant: typeof grant === "string" ? grant : undefined, pinned: typeof pinned === "string" ? pinned : undefined });
    if (!claims) return void outgoing.writeHead(401, { "content-type": "application/json" }).end(JSON.stringify({ error: "no caller's token" }));
    if (incoming.method === "POST" && incoming.url === "/bind") {
      incoming.resume();
      onBind();
      const refusal = refusing.shift();
      if (refusal) return void outgoing.writeHead(503, { "content-type": "application/json" }).end(JSON.stringify(refusal));
      bound = true;
      return void outgoing.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ binding, namespace: "alasio" }));
    }
    if (bindsNothing && !bound) {
      incoming.resume();
      return void outgoing.writeHead(409, { "content-type": "application/json" }).end(JSON.stringify({ bound: false }));
    }
    bound = true;
    // Its pod gone as the first call of `movesOn` comes, the key bound anew: the call, for the binding before, refused, saying the one now.
    if (movesOn !== undefined && incoming.url === movesOn && binding === "binding-1") {
      binding = "binding-2";
      incoming.resume();
      return void outgoing.writeHead(409, { "content-type": "application/json" }).end(JSON.stringify({ error: "bound to another binding now", moved: { binding, namespace: "alasio" } }));
    }
    const { [NO_BIND_HEADER]: _bindsNothing, [PINNED_HEADER]: _pinned, ...headers } = { ...incoming.headers, authorization: "Bearer host-token" } as Record<string, string | string[] | undefined>;
    const upstream = request({ host: "127.0.0.1", port: podPort, path: incoming.url, method: incoming.method, headers }, (answered) => {
      outgoing.writeHead(answered.statusCode ?? 502, answered.headers);
      answered.pipe(outgoing);
    });
    incoming.pipe(upstream);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, asked, close: () => {
    server.closeAllConnections();
    server.close();
  } };
}

/** A turn of `start`'s on `host`, its conversation reserved first, as Turns runs one. */
const reservedTurn = (host: HarnessHost["Service"], start: TurnRequest) => Effect.scoped(Effect.andThen(host.reserve(start.conversationId), host.startTurn(start)));

/**
 * alasio's own app-server, as it refreshes its Codex login: how often it was asked to, each
 * refresh writing the login `refreshed` gives of its count, where given, as Codex writes one anew.
 */
const appServer = { refreshes: 0, refreshed: null as ((count: number) => string) | null };
const codexAppServer = Layer.succeed(CodexAppServer, {
  refreshLogin: () => Effect.sync(() => {
    appServer.refreshes += 1;
    if (appServer.refreshed !== null) writeFileSync(join(CODEX_HOME, "auth.json"), appServer.refreshed(appServer.refreshes));
    return "asked" as const;
  }),
  // Its login, the home's own, loaded anew as it next refreshes.
  stop: Effect.void,
} as unknown as CodexAppServer["Service"]);
/** alasio's Codex login kept fresh by `codexAppServer`, as its store keeps it. */
const loginFreshness = (store: Store["Service"]) => CodexLoginFreshness.layer(CODEX_HOME).pipe(Layer.provide(Layer.mergeAll(codexAppServer, Layer.succeed(Store, store))));

/** alasio's harnesses, as the panels reach them: none of a test's. */
const harnesses = Harnesses.of({ names: HARNESS_NAMES, getFor: () => Effect.die("no harness of alasio's runs a turn"), forMount: () => Effect.succeedNone, requireForMount: () => Effect.die("no harness of alasio's runs a turn") });

test("alasio reaches each conversation's pooled host through the front, a token minted per call as its mount places it now, binding nothing of what only reads a binding, and gives each turn its workspace's bayma", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: "41", user: { id: 41 } }));
  const started: TurnStart[] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started)).pipe(Scope.provide(scope)));
    const stand = await front(port);
    try {
      // Each bayma's bearer held to the binding its bind answered with.
      const heldTo: unknown[] = [];
      const sessions = { ensureSession: (volumeId: string, held: unknown) => Effect.sync(() => (heldTo.push(held), { bayma: { url: "http://gateway/mcp", headers: { Authorization: `Bearer session-${volumeId}` } } })) } as unknown as SessionSandboxes["Service"];
      // A folder's bind that waits a while, saying on what, then is bound: what the turn's status read meanwhile.
      const waitedOn: (string | null)[] = [];
      const folders = {
        ensure: (scope: FolderBaymaScope) =>
          Effect.gen(function*() {
            yield* scope.waiting?.("whose image is being pulled") ?? Effect.void;
            waitedOn.push((yield* store.getActiveTurn(conversationId))?.waiting_on ?? null);
            yield* scope.waiting?.(null) ?? Effect.void;
            waitedOn.push((yield* store.getActiveTurn(conversationId))?.waiting_on ?? null);
            heldTo.push(scope.held);
            return { server: { type: "http", url: "http://gateway/mcp", headers: { Authorization: "Bearer folder" } }, notice: null } as const;
          }),
      } as unknown as HostBayma["Service"];
      // What a pod commits told the lake's loader, as each turn of its ends.
      let wakes = 0;
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME }, written: () => void (wakes += 1) }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions), Layer.succeed(HostBayma, folders), Layer.succeed(BusyPods, busyPods))),
      )).pipe(Scope.provide(scope))), HarnessHost);

      // No harness in a workspace: nothing bound, nothing asked of the front.
      assert.deepEqual(await Effect.runPromise(host.status(conversationId)), { busy: false, running: false, cliInitiated: false, held: false, idleSince: 0 });
      assert.equal(stand.asked.length, 0);

      // A folder conversation's, unbound: what reads a binding binds nothing, and answers as of a conversation nothing runs in.
      await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
      await run(store.setWorkingDirectory(conversationId, "/work"));
      assert.deepEqual(await Effect.runPromise(host.status(conversationId)), { busy: false, running: false, cliInitiated: false, held: false, idleSince: 0 });
      assert.deepEqual(await Effect.runPromise(host.backgroundWork(conversationId)), false);
      assert.deepEqual(await Effect.runPromise(host.steer(conversationId, "more", [])), Option.none());
      assert.equal(await Effect.runPromise(host.interrupt(conversationId, "interrupt")), false);
      // Nor does an attach to a turn of it, its conversation bound to no pod.
      assert.deepEqual(await Effect.runPromise(host.attach({ conversationId, epoch: 1, messageId: "10" })), { _tag: "Unbound" });
      assert.deepEqual(stand.asked.map(({ bindsNothing }) => bindsNothing), [true, true, true, true, true]);
      assert.deepEqual(stand.asked[0]?.claims?.key, `harness:${conversationId}`);
      assert.deepEqual(stand.asked[0]?.claims?.options, { environment: "main", harness: "codex", workspace: "folder" });
      const lifetime = (stand.asked[0]?.claims?.expires ?? 0) - Date.now();
      assert.ok(lifetime > 4 * 60_000 && lifetime <= 5 * 60_000, String(lifetime));

      // A turn binds it, given its folder's bayma as alasio brought it up, what its bind waited on said in its status meanwhile.
      const epoch = await run(store.claimTurn({ conversationId, chatId: "41", messageId: "10", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      const turn: TurnRequest = { conversationId, chatId: "41", messageId: "10", harness: CODEX_HARNESS, workingDirectory: "/work", prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null };
      assert.equal((await Effect.runPromise(reservedTurn(host, turn))).pendingResponseId, "pending-1");
      assert.deepEqual(started.at(-1)?.bayma, { endpoint: { url: "http://gateway/mcp", headers: { Authorization: "Bearer folder" } }, notice: null });
      assert.deepEqual(waitedOn, ["whose image is being pulled", null]);
      assert.equal(wakes, 1);
      const held = { key: `harness:${conversationId}`, binding: "binding-1", namespace: "alasio" };
      assert.deepEqual(heldTo, [held]);
      // alasio's own pod held busy while it drove the turn, and let go of once it ended.
      assert.deepEqual([pod.heldDuring, pod.holds], [[1], 0]);
      // Its Codex given a token of its turn's for alasio's relay of the login, and the operator's configuration, and not their login.
      const codex = started.at(-1)?.codex;
      assert.equal(codex?.login.url, RELAY.url);
      const claims = verifyToken(poolLoginKey(CALLER_KEY), codex?.login.token ?? "", Date.now());
      assert.equal(claims?.key, `harness:${conversationId}`);
      // Good for minutes, as one taken from a pod is of little use.
      const relayLifetime = (claims?.expires ?? 0) - Date.now();
      assert.ok(relayLifetime > 9 * 60_000 && relayLifetime <= 10 * 60_000, String(relayLifetime));
      assert.equal(verifyToken(CALLER_KEY, codex?.login.token ?? "", Date.now()), null, "a relay token is no caller's");
      assert.deepEqual(codex?.home, { "config.toml": 'model = "gpt-5"\n', "AGENTS.md": "Be brief.\n", "skills/review/SKILL.md": "Review it.\n" });
      assert.equal(stand.asked.findLast(({ path }) => path === HOST_PATHS.turns)?.bindsNothing, false);
      // The pod given the grant of the binding its bind answered with, by its reservation and its turn, and by no call that binds nothing; alasio keeping its digest as the conversation's.
      const grant = harnessGrant(grantKey(CALLER_KEY), conversationId, "binding-1");
      assert.deepEqual(stand.asked.map(({ path, grant, pinned }) => [path, grant, pinned]), [
        ...[HOST_PATHS.status, HOST_PATHS.backgroundWork, HOST_PATHS.steer, HOST_PATHS.interrupt, HOST_PATHS.attach].map((path) => [path, undefined, undefined]),
        ["/bind", undefined, undefined],
        [HOST_PATHS.reserve, grant, "binding-1"],
        [HOST_PATHS.turns, grant, "binding-1"],
        [HOST_PATHS.release, grant, "binding-1"],
      ]);
      assert.deepEqual((await (await testPool()).query(`select binding, secret_sha256 as digest from ${schema}.harness_grants where conversation_id = $1`, [conversationId])).rows, [{ binding: "binding-1", digest: grantDigest(grant) }]);
      // A fresh session is bound ahead too, so its pod has the grant as its harness makes it.
      assert.equal(await Effect.runPromise(host.startFreshSession({ conversationId, harness: CODEX_HARNESS, workingDirectory: "/work" })), "thread-2");
      assert.deepEqual(stand.asked.slice(-2).map(({ path, grant }) => [path, grant]), [["/bind", undefined], [HOST_PATHS.freshSession, grant]]);
      // Bound, what reads it is passed on to its host, an attach to its turn among it, binding nothing still, nor giving its grant.
      assert.deepEqual(await Effect.runPromise(host.status(conversationId)), { busy: true, running: true, cliInitiated: false, held: false, idleSince: null });
      assert.equal(await Effect.runPromise(host.backgroundWork(conversationId)), true);
      assert.equal(answered(await Effect.runPromise(host.attach({ conversationId, epoch, messageId: "10" }))).pendingResponseId, "pending-1");
      assert.deepEqual([stand.asked.at(-1)?.path, stand.asked.at(-1)?.bindsNothing, stand.asked.at(-1)?.grant], [HOST_PATHS.attach, true, undefined]);

      // Its workspace switched to a session: the next call's token places it so, and its turn is given the session's bayma.
      await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
      await Effect.runPromise(host.status(conversationId));
      assert.deepEqual(stand.asked.at(-1)?.claims?.options, { environment: "main", harness: "codex", workspace: "session" });
      await Effect.runPromise(reservedTurn(host, { ...turn, workingDirectory: sessionFsWorkspace("fs-abc123"), messageId: "11", epoch: epoch + 1 }));
      assert.deepEqual(started.at(-1)?.bayma, { endpoint: { url: "http://gateway/mcp", headers: { Authorization: "Bearer session-fs-abc123" } }, notice: null });
      assert.deepEqual(heldTo, [held, held]);
      // A session's Codex has a home of its own, of no operator's configuration: its login alone.
      assert.equal(started.at(-1)?.codex?.home, null);
      assert.ok(started.at(-1)?.codex?.login.token);
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a turn's reservation binds its conversation's harness ahead, asked again while the front cannot bind it yet, telling what it waits on meanwhile, then holds it, and the turn starts on the pod bound; one the front holds fails at once", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "43", user: { id: 43 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
  const started: TurnStart[] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started)).pipe(Scope.provide(scope)));
    const sessions = { ensureSession: () => Effect.succeed({ bayma: { url: "http://gateway/mcp", headers: {} } }) } as unknown as SessionSandboxes["Service"];
    const hostOn = async (url: string) =>
      Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
    const notYet = { error: `harness:${conversationId} could not be bound now`, reason: "it waited for its pod to serve" };
    // What the reservation's caller was told it waits on, as each bind was asked, and in all.
    const told: (string | null)[] = [];
    const said: (string | null)[][] = [];
    const slow = await front(port, { refusals: [notYet, notYet], onBind: () => void said.push([...told]) });
    try {
      // As Turns runs a turn: reserved, then claimed, then started, the reservation held throughout.
      const host = await hostOn(slow.url);
      const answer = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* host.reserve(conversationId, (what) => Effect.sync(() => void told.push(what))), true);
        const epoch = yield* store.claimTurn({ conversationId, chatId: "43", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" });
        return yield* host.startTurn({ conversationId, chatId: "43", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null });
      })));
      assert.equal(answer.pendingResponseId, "pending-1");
      // A turn not reserved first has no binding its bayma could be held to: refused.
      const unreserved = await Effect.runPromise(Effect.flip(Effect.scoped(host.startTurn({ conversationId, chatId: "43", messageId: "2", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch: 0, jobId: null, attachedTurn: null }))));
      assert.match(unreserved.message, /reserved for none/u);
      assert.deepEqual(said, [[], [HARNESS_BINDING], [HARNESS_BINDING]]);
      assert.deepEqual(told, [HARNESS_BINDING, null]);
      assert.deepEqual(slow.asked.map(({ path }) => path), ["/bind", "/bind", "/bind", HOST_PATHS.reserve, HOST_PATHS.turns, HOST_PATHS.release]);
    } finally {
      slow.close();
    }
    // Held, its binds having failed one after another: not asked again, the reservation failing with what the front said.
    const holding = await front(port, { refusals: [{ error: `harness:${conversationId} is bound again only as its caller asks`, reason: "it waited for its pod to serve", held: true }] });
    try {
      const failed = await Effect.runPromise(Effect.flip(Effect.scoped((await hostOn(holding.url)).reserve(conversationId))));
      assert.match(failed.message, /the sandbox gateway answered 503: harness:\S+ is bound again only as its caller asks/u);
      assert.deepEqual(holding.asked.map(({ path }) => path), ["/bind"]);
    } finally {
      holding.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a turn whose harness is bound anew after its reservation, its pod gone, runs on the new binding's pod with that binding's grant and bayma, never the old one's", async () => {
  const schema = newSchema();
  const store = await testStore({ schema });
  const conversationId = await run(store.upsertConversation({ chatId: "47", user: { id: 47 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
  const started: TurnStart[] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started)).pipe(Scope.provide(scope)));
    // Its pod gone as its turn is started: the front binds it anew, and refuses the turn, for the binding before.
    const stand = await front(port, { movesOn: HOST_PATHS.turns });
    try {
      const heldTo: unknown[] = [];
      const sessions = { ensureSession: (_volumeId: string, held: unknown) => Effect.sync(() => (heldTo.push(held), { bayma: { url: "http://gateway/mcp", headers: {} } })) } as unknown as SessionSandboxes["Service"];
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      const epoch = await run(store.claimTurn({ conversationId, chatId: "47", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      const answer = await Effect.runPromise(Effect.scoped(Effect.andThen(host.reserve(conversationId), host.startTurn({ conversationId, chatId: "47", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null }))));
      assert.equal(answer.pendingResponseId, "pending-1");
      const grantOf = (binding: string) => harnessGrant(grantKey(CALLER_KEY), conversationId, binding);
      // Each call for one binding alone, the turn made again of the new one with its own grant.
      assert.deepEqual(stand.asked.filter(({ path }) => path !== "/bind").map(({ path, grant, pinned }) => [path, grant, pinned]), [
        [HOST_PATHS.reserve, grantOf("binding-1"), "binding-1"],
        [HOST_PATHS.turns, grantOf("binding-1"), "binding-1"],
        [HOST_PATHS.turns, grantOf("binding-2"), "binding-2"],
        [HOST_PATHS.release, grantOf("binding-1"), "binding-1"],
      ]);
      assert.equal(started.length, 1);
      // Its bayma held to the binding its turn ran on; and each binding's grant kept, until its own binding is let go of, as the gateway tells.
      assert.deepEqual(heldTo.map((held) => (held as { readonly binding: string }).binding), ["binding-1", "binding-2"]);
      assert.deepEqual((await (await testPool()).query(`select binding, secret_sha256 as digest from ${schema}.harness_grants where conversation_id = $1 order by binding`, [conversationId])).rows, [
        { binding: "binding-1", digest: grantDigest(grantOf("binding-1")) },
        { binding: "binding-2", digest: grantDigest(grantOf("binding-2")) },
      ]);
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a reservation the host refuses, as another holds its conversation, leaves the other's as it was, the turn of that one's bayma held to its binding", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "46", user: { id: 46 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
  const started: TurnStart[] = [];
  // The host reserves its conversation once, and refuses another while that holds it.
  let holding = false;
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, { ...podHost(started), reserve: () => Effect.sync(() => !holding && (holding = true)) }).pipe(Scope.provide(scope)));
    const stand = await front(port);
    try {
      const heldTo: unknown[] = [];
      const sessions = { ensureSession: (_volumeId: string, held: unknown) => Effect.sync(() => (heldTo.push(held), { bayma: { url: "http://gateway/mcp", headers: {} } })) } as unknown as SessionSandboxes["Service"];
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      const epoch = await run(store.claimTurn({ conversationId, chatId: "46", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      const answer = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        assert.equal(yield* host.reserve(conversationId), true);
        // Another reservation of it, as a second message's turn would ask, refused and let go of.
        assert.equal(yield* Effect.scoped(host.reserve(conversationId)), false);
        return yield* host.startTurn({ conversationId, chatId: "46", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null });
      })));
      assert.equal(answer.pendingResponseId, "pending-1");
      assert.deepEqual(heldTo, [{ key: `harness:${conversationId}`, binding: "binding-1", namespace: "alasio" }]);
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a hold let go of binds nothing: its pod gone meanwhile, as one released or lost is, the front binds it no other", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "44", user: { id: 44 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token" }), HarnessHost, podHost([])).pipe(Scope.provide(scope)));
    const stand = await front(port);
    try {
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      await Effect.runPromise(Effect.scoped(host.reserve(conversationId)));
      const released = stand.asked.find(({ path }) => path === HOST_PATHS.release);
      assert.equal(released?.bindsNothing, true);
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a Codex turn's token for alasio's relay is renewed as it runs; alasio, the one refresher of the login, refreshes it before the turn and as it renews where its access token is about to stop", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "45", user: { id: 45 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
  const jwt = (exp: number) => `h.${Buffer.from(JSON.stringify({ exp: Math.floor(exp / 1000) })).toString("base64url")}.s`;
  const login = (exp: number, refresh = "rt-0") => JSON.stringify({ tokens: { access_token: jwt(exp), refresh_token: refresh } });
  // About to stop: refreshed before the turn, each refresh again about to stop, as only a test's would be.
  const kept = readFileSync(join(CODEX_HOME, "auth.json"), "utf8");
  writeFileSync(join(CODEX_HOME, "auth.json"), login(Date.now() + 60_000));
  appServer.refreshes = 0;
  appServer.refreshed = (count) => login(Date.now() + 120_000, `rt-${count}`);
  const started: TurnStart[] = [];
  const renewed: TurnCodex["login"][] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started, { renewed, turnMs: 1_000 })).pipe(Scope.provide(scope)));
    const stand = await front(port);
    try {
      const sessions = { ensureSession: () => Effect.succeed({ bayma: { url: "http://gateway/mcp", headers: {} } }) } as unknown as SessionSandboxes["Service"];
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME, renewMs: 300 } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      const epoch = await run(store.claimTurn({ conversationId, chatId: "45", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      await Effect.runPromise(reservedTurn(host, { conversationId, chatId: "45", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null }));
      // Renewed as it ran, each a fresh token of the conversation's, through what binds nothing; no more once it ended.
      assert.ok(renewed.length >= 2, String(renewed.length));
      const tokens = [started[0]?.codex?.login.token, ...renewed.map(({ token }) => token)];
      assert.equal(new Set(tokens).size, tokens.length);
      for (const token of tokens) assert.equal(verifyToken(poolLoginKey(CALLER_KEY), token ?? "", Date.now())?.key, `harness:${conversationId}`);
      assert.ok(stand.asked.filter(({ path }) => path === HOST_PATHS.codexLogin).every(({ bindsNothing }) => bindsNothing));
      const after = renewed.length;
      await new Promise((resolve) => setTimeout(resolve, 700));
      assert.equal(renewed.length, after);
      // Refreshed before the turn, and as each renewal found it about to stop again.
      assert.equal(appServer.refreshes, 1 + renewed.length);
      // A login far from stopping is refreshed by none.
      appServer.refreshes = 0;
      writeFileSync(join(CODEX_HOME, "auth.json"), login(Date.now() + 24 * 60 * 60_000));
      await Effect.runPromise(reservedTurn(host, { conversationId, chatId: "45", messageId: "2", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "again", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null }));
      assert.equal(appServer.refreshes, 0);
    } finally {
      stand.close();
    }
  } finally {
    appServer.refreshed = null;
    writeFileSync(join(CODEX_HOME, "auth.json"), kept);
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a Codex turn whose login is refused fails saying to log in again, started on no pod", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "46", user: { id: 46 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc123")));
  const started: TurnStart[] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started)).pipe(Scope.provide(scope)));
    const stand = await front(port);
    try {
      const sessions = { ensureSession: () => Effect.succeed({ bayma: { url: "http://gateway/mcp", headers: {} } }) } as unknown as SessionSandboxes["Service"];
      const refused = Layer.succeed(CodexLoginFreshness, CodexLoginFreshness.of({ fresh: Effect.fail(new CodexLoginRefused()) }));
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: "http://alasio-codex-login:4320/v1", home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), refused, Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      const epoch = await run(store.claimTurn({ conversationId, chatId: "46", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      const failed = await Effect.runPromise(Effect.flip(reservedTurn(host, { conversationId, chatId: "46", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc123"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null })));
      assert.equal(failed._tag, "CodexLoginRefused");
      assert.match(failed.message, /run `alasio login codex`/u);
      assert.deepEqual(started, []);
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("a Codex turn starts once its environment's relay of its Codex login answers, as KEDA scales it up from none, its status saying what it waits on meanwhile", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "46", user: { id: 46 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, sessionFsWorkspace("fs-abc124")));
  // A port nothing listens on yet, as the relay's Service has no pod while it is at none.
  const free = await relayAt();
  free.close();
  const port = Number(new URL(free.url).port);
  const started: TurnStart[] = [];
  const scope = Effect.runSync(Scope.make());
  try {
    const { port: podPort } = await Effect.runPromise(Effect.provideService(serveHost({ port: 0, host: "127.0.0.1", conversationId, token: "host-token", given: givenTo(started) }), HarnessHost, podHost(started)).pipe(Scope.provide(scope)));
    const stand = await front(podPort);
    try {
      const sessions = { ensureSession: () => Effect.succeed({ bayma: { url: "http://gateway/mcp", headers: {} } }) } as unknown as SessionSandboxes["Service"];
      const host = Context.get(await Effect.runPromise(Layer.build(pooledHarnessHost({ url: stand.url, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: `http://127.0.0.1:${port}/v1`, home: CODEX_HOME } }).pipe(
        Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store), Layer.succeed(SessionSandboxes, sessions))),
      )).pipe(Scope.provide(scope))), HarnessHost);
      const epoch = await run(store.claimTurn({ conversationId, chatId: "46", messageId: "1", sessionId: null, harness: CODEX_HARNESS, prompt: "go" }));
      const turn = Effect.runPromise(reservedTurn(host, { conversationId, chatId: "46", messageId: "1", harness: CODEX_HARNESS, workingDirectory: sessionFsWorkspace("fs-abc124"), prompt: "go", fileIds: [], resumeSession: null, modelChoice: null, epoch, jobId: null, attachedTurn: null }));
      await eventually("the turn to say it waits on the relay", async () => ((await run(store.getActiveTurn(conversationId)))?.waiting_on === CODEX_LOGIN_RELAY ? true : undefined));
      assert.deepEqual(started, [], "the turn started before its relay answered");
      const relay = await relayAt(port);
      try {
        await turn;
        assert.equal(started.length, 1);
      } finally {
        relay.close();
      }
    } finally {
      stand.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});

test("no call of a pooled host's is one the front answers itself, so each reaches the host: a hold let go of releases no binding", () => {
  for (const [call, path] of Object.entries(HOST_PATHS)) assert.equal(answeredByFront(path), false, `${call} at ${path}`);
});

test("a pooled host the front refuses fails as not reached, saying what the front said", async () => {
  const store = await testStore();
  const conversationId = await run(store.upsertConversation({ chatId: "42", user: { id: 42 } }));
  await run(store.setActiveHarness(conversationId, CODEX_HARNESS));
  await run(store.setWorkingDirectory(conversationId, "/work"));
  const refusing = createServer((incoming, outgoing) => {
    incoming.resume();
    outgoing.writeHead(503, { "content-type": "application/json" }).end(JSON.stringify({ error: `harness:${conversationId} could not be bound now`, reason: "no free pod" }));
  });
  await new Promise<void>((resolve) => refusing.listen(0, "127.0.0.1", resolve));
  try {
    const host = Context.get(await Effect.runPromise(Effect.scoped(Layer.build(pooledHarnessHost({ url: `http://127.0.0.1:${(refusing.address() as AddressInfo).port}`, environment: "main", callerKey: CALLER_KEY, codexLogin: { url: RELAY.url, home: CODEX_HOME } }).pipe(
      Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), Layer.succeed(Harnesses, harnesses), loginFreshness(store))),
    )))), HarnessHost);
    const failed = await Effect.runPromise(Effect.flip(host.status(conversationId)));
    assert.equal(failed.message, `The conversation's harness could not be reached: the sandbox gateway answered 503: harness:${conversationId} could not be bound now (no free pod)`);
  } finally {
    refusing.close();
  }
});
