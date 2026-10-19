import assert from "node:assert/strict";
import { test } from "node:test";

import type { InlineKeyboardMarkup } from "@grammyjs/types";
import { Effect, Layer } from "effect";

import { buildCodexThreadConfig, codexMcpServer } from "../src/codex/thread-config.ts";
import { claudeMcpServers } from "../src/harness/claude/mcp.ts";
import { type BaymaMcpServer, folderBayma, HostBayma, noFolderBayma } from "../src/mcp/bayma.ts";
import { BaymaProxy } from "../src/mcp/bayma-proxy.ts";
import { Store } from "../src/persistence/store.ts";
import type { HeldTo } from "../src/binder/tokens.ts";
import { type BaymaEndpoint, FolderBindFailed, type FolderScope, GatewayError, type GatewayWorkspaces } from "../src/sandbox/gateway-client.ts";
import { run, testStore } from "./support/store.ts";
import { type RecordingTelegram, recordingTelegram } from "./support/telegram-calls.ts";

const BAYMA: BaymaMcpServer = { type: "http", url: "http://alasio-sandbox-gateway.alasio.svc:8080/mcp", headers: { Authorization: "Bearer folder.t" } };
/** A folder instance's bayma as a harness is given it, at alasio's loopback. */
const LOOPBACK: BaymaMcpServer = { type: "http", url: "http://127.0.0.1:1/mcp/folder:claude:telegram:42", headers: { Authorization: "Bearer loopback" } };

/** alasio's loopback, as a stand-in: each workspace's endpoint at its key, its upstream kept. */
const upstreams = new Map<string, Effect.Effect<BaymaEndpoint, { readonly message: string }>>();
const loopback = Layer.succeed(BaymaProxy, BaymaProxy.of({
  reach: (key, upstream) => Effect.sync(() => (upstreams.set(key, upstream), { url: `http://127.0.0.1:1/mcp/${key}`, headers: { Authorization: "Bearer loopback" } })),
}));

test("a deployment whose gateway serves no folder instances offers no folder bayma", async () => {
  const none = Effect.runSync(folderBayma);
  assert.equal(none, noFolderBayma);
  const refused = await Effect.runPromise(Effect.flip(none({ harness: "claude", threadKey: "telegram:42", workingDirectory: "/home/op/repo" })));
  assert.equal(refused.message, "this deployment offers no folder workspaces: it has no host profile");
});

/** Folder instances as the gateway's client gives them, binding as `binds` says, each call recorded in `calls`. */
function fakeFolders(binds: () => Effect.Effect<void, FolderBindFailed | GatewayError>) {
  const calls: string[] = [];
  const folders = {
    bind: (scope: FolderScope, { retry = false }: { readonly retry?: boolean } = {}) => Effect.andThen(Effect.sync(() => void calls.push(`bind${retry ? " retry" : ""} ${scope.harness} ${scope.threadKey}`)), binds()),
    release: (scope: FolderScope) => Effect.sync(() => void calls.push(`release ${scope.harness} ${scope.threadKey}`)),
    endpoint: (scope: FolderScope, held?: HeldTo) => Effect.sync(() => (calls.push(`endpoint ${scope.harness} ${scope.threadKey}${held ? ` held to ${held.binding}` : ""}`), { url: BAYMA.url, headers: BAYMA.headers })),
    starting: (_scope: FolderScope) => Effect.succeed<string | null>(null),
  };
  return { calls, folders };
}

/** Runs `use` of HostBayma on `folders`, on a store holding the conversation telegram:42 and Telegram recording into `telegram`. */
async function withHostBayma<A>(folders: GatewayWorkspaces["folders"], telegram: RecordingTelegram, use: (host: HostBayma["Service"]) => Effect.Effect<A>, timing: Parameters<typeof HostBayma.layer>[1] = {}): Promise<A> {
  const store = await testStore();
  await run(store.upsertConversation({ chatId: 42, user: { id: 1 } }));
  return Effect.runPromise(Effect.flatMap(HostBayma, use).pipe(Effect.provide(HostBayma.layer(folders, timing).pipe(Layer.provide(Layer.mergeAll(Layer.succeed(Store, store), telegram.layer, loopback))))));
}

const SCOPE = { harness: "claude", threadKey: "telegram:42", workingDirectory: "/home/op/repo" } as const;

test("a folder conversation's bayma is its folder instance, bound through the gateway before its endpoint at alasio's loopback is given, in Claude Code's shape, the loopback passing it on with the gateway's endpoint as each request goes", async () => {
  const { calls, folders } = fakeFolders(() => Effect.void);
  const telegram = recordingTelegram();
  const outcome = await withHostBayma(folders, telegram, (host) => host.ensure({ ...SCOPE, chatId: "42" }));
  assert.deepEqual(outcome, { server: LOOPBACK, notice: null });
  assert.deepEqual(calls, ["bind claude telegram:42"]);
  const { type: _, ...endpoint } = BAYMA;
  assert.deepEqual(await Effect.runPromise(upstreams.get("folder:claude:telegram:42") ?? Effect.die("not reached")), endpoint);
  assert.deepEqual(calls, ["bind claude telegram:42", "endpoint claude telegram:42"]);
  // For a harness bound in a pool, which reaches it from there: the gateway's own, its bearer held to that harness's binding.
  const held = await withHostBayma(folders, telegram, (host) => host.ensure({ ...SCOPE, held: { key: "harness:telegram:42", binding: "binding-1", namespace: "alasio" } }));
  assert.deepEqual(held, { server: BAYMA, notice: null });
  assert.equal(calls.at(-1), "endpoint claude telegram:42 held to binding-1");
  assert.deepEqual(telegram.calls.sendMessage, []);
});

test("a folder instance that did not start leaves the turn without bayma, told why, the chat told first with buttons to retry and to restart it; held, the chat is told so once, until a bind goes through", async () => {
  let failing: FolderBindFailed | null = new FolderBindFailed({ reason: "its pod is pending: no node", held: false });
  const { calls, folders } = fakeFolders(() => (failing ? Effect.fail(failing) : Effect.void));
  const telegram = recordingTelegram();
  const told = () => telegram.calls.sendMessage.map(([, text, options]) => [text, (options as { reply_markup?: InlineKeyboardMarkup } | undefined)?.reply_markup?.inline_keyboard.flat().map(({ text: label }) => label)]);
  await withHostBayma(folders, telegram, (host) =>
    Effect.gen(function*() {
      const notice = "Folder workspace /home/op/repo: its bayma did not start (its pod is pending: no node). This turn runs without bayma's REPL tools; your REPL sessions are kept and come back when it starts.";
      assert.deepEqual(yield* host.ensure({ ...SCOPE, chatId: "42" }), { server: null, notice });
      assert.deepEqual(told(), [[notice, ["Retry now", "Restart folder instance"]]]);
      // No chat given, none told: a call no operator waits on.
      assert.deepEqual(yield* host.ensure(SCOPE), { server: null, notice });
      assert.equal(told().length, 1);
      failing = new FolderBindFailed({ reason: "its last 3 binds failed, the last as its pod is pending: no node", held: true });
      const held = "Folder workspace /home/op/repo: its bayma failed to start three times in a row (its last 3 binds failed, the last as its pod is pending: no node), so alasio tries no more until you ask. This turn runs without bayma's REPL tools; your REPL sessions are kept and come back when it starts.";
      assert.deepEqual(yield* host.ensure({ ...SCOPE, chatId: "42" }), { server: null, notice: held });
      assert.deepEqual(yield* host.ensure({ ...SCOPE, chatId: "42" }), { server: null, notice: held });
      assert.deepEqual(told().map(([text]) => text), [notice, held], "held, told once");
      // As the operator asks, bound again though held.
      failing = null;
      assert.equal(yield* host.retry(SCOPE), "Its bayma started: the next message works with its REPL tools.");
      assert.equal(yield* host.restart(SCOPE), "Its folder instance is stopped, its REPL sessions saved, and its failed binds forgotten: the next message starts it anew.");
    }));
  assert.deepEqual(calls.filter((call) => !call.startsWith("bind claude")), ["bind retry claude telegram:42", "release claude telegram:42"]);
});

test("a restart of a folder instance being bound or released says its failed binds are forgotten but it was not restarted, and its chat is told again of it held", async () => {
  const telegram = recordingTelegram();
  let held = true;
  const { folders } = fakeFolders(() => (held ? Effect.fail(new FolderBindFailed({ reason: "its last 3 binds failed", held: true })) : Effect.void));
  const busy = { ...folders, release: () => Effect.fail(new GatewayError({ call: "release the folder instance of telegram:42 under claude", status: 409, reason: "being bound now" })) };
  await withHostBayma(busy, telegram, (host) =>
    Effect.gen(function*() {
      yield* host.ensure({ ...SCOPE, chatId: "42" });
      assert.equal(yield* host.restart(SCOPE), "Its failed binds are forgotten, but it is being started or stopped now, so it was not restarted: ask again in a moment.");
      yield* host.ensure({ ...SCOPE, chatId: "42" });
      held = false;
    }));
  assert.equal(telegram.calls.sendMessage.length, 2, "held, told again after the restart");
});

test("a bind that waits long says in its turn's status what it waits on, as the gateway answers it, until it goes through, and degrades nothing; a quick one says nothing", async () => {
  const telegram = recordingTelegram();
  let bindFor = 300;
  const answers = ["whose instance before is saving its REPL sessions", "whose instance before is saving its REPL sessions", "whose image is being pulled"];
  const folders = {
    ...fakeFolders(() => Effect.void).folders,
    bind: () => Effect.as(Effect.sleep(bindFor), "binding-1"),
    starting: () => Effect.sync(() => answers.shift() ?? "whose bayma is starting"),
  };
  const said: (string | null)[] = [];
  const waiting = (what: string | null) => Effect.sync(() => void said.push(what));
  const [slow, quick] = await withHostBayma(folders, telegram, (host) =>
    Effect.gen(function*() {
      const slow = yield* host.ensure({ ...SCOPE, chatId: "42", waiting });
      bindFor = 10;
      const quick = yield* host.ensure({ ...SCOPE, chatId: "42", waiting });
      return [slow, quick];
    }), { toldAfterMs: 100, askedEveryMs: 50 });
  assert.deepEqual([slow?.server, quick?.server], [LOOPBACK, LOOPBACK]);
  // Each change said once, then that it waits no more.
  assert.deepEqual(said.slice(0, 2), ["its folder's bayma, whose instance before is saving its REPL sessions", "its folder's bayma, whose image is being pulled"]);
  assert.equal(said.at(-1), null);
  assert.equal(said.filter((what) => what === null).length, 1, "the quick bind said nothing");
  assert.deepEqual(telegram.calls.sendMessage, [], "nothing degraded, so the chat told nothing");
});

test("a retry that fails says why, and a restart the gateway refuses says it was not", async () => {
  const telegram = recordingTelegram();
  const { folders } = fakeFolders(() => Effect.fail(new FolderBindFailed({ reason: "its bayma ended (Error)", held: false })));
  const refusing = { ...folders, release: () => Effect.fail(new GatewayError({ call: "release the folder instance of telegram:42 under claude", status: 503, reason: "not now" })) };
  const said = await withHostBayma(refusing, telegram, (host) => Effect.all([host.retry(SCOPE), host.restart(SCOPE)]));
  assert.deepEqual(said, [
    "Its bayma did not start: its bayma ended (Error)",
    "Its folder instance could not be restarted: the sandbox gateway did not release the folder instance of telegram:42 under claude (503): not now",
  ]);
});

test("alasio adds bayma to Claude Code's servers, in its shape", () => {
  assert.deepEqual(claudeMcpServers(BAYMA), { bayma: BAYMA });
});

test("Codex threads add bayma over HTTP and leave the operator's servers and apps connector on", () => {
  const config = buildCodexThreadConfig({ codexEnv: {}, bayma: BAYMA });
  assert.deepEqual(config.mcp_servers, { bayma: codexMcpServer(BAYMA) });
  assert.deepEqual(codexMcpServer(BAYMA), { url: BAYMA.url, http_headers: BAYMA.headers, startup_timeout_sec: 60 });
  // Of a folder whose bayma did not start, none of alasio's.
  assert.deepEqual(buildCodexThreadConfig({ codexEnv: {}, bayma: null }).mcp_servers, {});
  // The config's type has no `features`; this pins that none is sent at all.
  assert.equal(Reflect.get(config, "features"), undefined);
});
