/**
 * alasio's Codex login kept fresh (src/codex/login-refresh.ts) by the alasios on one store, each
 * with a home of its own: a login due refreshed by one of them alone, holding the store's lock,
 * the others taking what it kept; one due as its last refresh is old, whatever its access token
 * says; a refresh refused for good never tried again with its token, nor any refresh of a login
 * refused until another is kept or the home's file changes; a refresh that failed otherwise
 * refusing nothing, tried again once its wait is past; and a home that takes whatever
 * login the store kept since it last took one, of an API key or of ChatGPT, by the store's own
 * stamp of each save.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Cause, Data, Duration, Effect, Exit } from "effect";

import type { LoginRefresh } from "../src/codex/app-server/thread-client.ts";
import { makeCodexLoginFreshness, REFRESHED_WITHIN_MS } from "../src/codex/login-refresh.ts";
import { Store } from "../src/persistence/store.ts";
import { newSchema, run, testStore } from "./support/store.ts";

/** An access token stopping at `exp`, in milliseconds since the epoch, as a JWT carries it. */
const jwt = (exp: number) => ["e30", Buffer.from(JSON.stringify({ exp: Math.floor(exp / 1000) })).toString("base64url"), "c2ln"].join(".");

/** A ChatGPT login's auth.json, its access token stopping at `stops`, last refreshed at `refreshed`, with the refresh token `token`. */
const login = (stops: number, refreshed: number, token: string) =>
  JSON.stringify({ tokens: { access_token: jwt(stops), refresh_token: token }, last_refresh: new Date(refreshed).toISOString() });

/** The app-server's request failing, as one that ended before it answered fails it. */
class Gone extends Data.TaggedError("Gone")<{ readonly message: string }> {}

/**
 * What issues the login's tokens, as OpenAI's does, as Codex's refresh reaches it: each refresh
 * token works once, a refresh writing the home's auth.json anew, as Codex writes it, with a token
 * stopping in `issuedForMs`, two hours unless given; one whose token is spent refused for good. While `unreachable` counts
 * down, a refresh reaches no issuer, as one reset or of no DNS answer does not, which Codex
 * answers as it does any, its login left as it was.
 */
function issuer(first: string, issuedForMs = 2 * 3_600_000) {
  const state = { current: first, refreshes: 0, refused: 0, unreachable: 0, unreached: 0 };
  const refreshOf = (home: string): Effect.Effect<LoginRefresh> =>
    Effect.gen(function*() {
      const auth: { readonly tokens: { readonly refresh_token: string } } = JSON.parse(readFileSync(join(home, "auth.json"), "utf8"));
      // Slow enough that two alasios not holding the lock would both ask.
      yield* Effect.sleep("100 millis");
      if (state.unreachable > 0) {
        state.unreachable -= 1;
        state.unreached += 1;
        return "asked";
      }
      if (auth.tokens.refresh_token !== state.current) {
        state.refused += 1;
        return "refused";
      }
      state.refreshes += 1;
      state.current = `rt-${state.refreshes + 1}`;
      writeFileSync(join(home, "auth.json"), login(Date.now() + issuedForMs, Date.now(), state.current));
      return "asked";
    });
  return { state, refreshOf };
}

/** `store`, counting each time its Codex login lock is taken. */
function countingLocks(store: Store["Service"]) {
  const taken = { locks: 0 };
  const counted: Store["Service"] = {
    ...store,
    withCodexLoginLock: (work) => Effect.suspend(() => {
      taken.locks += 1;
      return store.withCodexLoginLock(work);
    }),
  };
  return { counted, taken };
}

/** A Codex home of its own, holding `auth`, removed as the test ends. */
function homeWith(t: TestContext, auth: string): string {
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-home-"));
  t.after(() => rmSync(home, { recursive: true, force: true }));
  writeFileSync(join(home, "auth.json"), auth);
  return home;
}

/**
 * The freshness of the login of `home`, of an alasio on the store `store`, a refresh failing but
 * not for good tried again `retryAfterMs` later, its app-server's reloads counted in `reloads`.
 */
const freshnessOf = (store: Store["Service"], home: string, refresh: Effect.Effect<LoginRefresh, Gone>, retryAfterMs?: number, reloads = { count: 0 }) =>
  Effect.runPromise(makeCodexLoginFreshness({ home, refresh, retryAfterMs, reload: Effect.sync(() => void (reloads.count += 1)) }).pipe(Effect.provideService(Store, store)));

const authOf = (home: string) => readFileSync(join(home, "auth.json"), "utf8");

test("two alasios on one store, their logins due at once, refresh it once, holding its lock, and both hold the login refreshed, which the store keeps", async (t) => {
  const schema = newSchema();
  const [one, two] = [await testStore({ schema }), await testStore({ schema })];
  const due = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(one.setCodexLogin(due));
  const tokens = issuer("rt-1");
  const [first, second] = [homeWith(t, due), homeWith(t, due)];
  const fresh = [await freshnessOf(one, first, tokens.refreshOf(first)), await freshnessOf(two, second, tokens.refreshOf(second))];
  await Effect.runPromise(Effect.all(fresh.map(({ fresh: each }) => each), { concurrency: "unbounded" }));
  assert.deepEqual([tokens.state.refreshes, tokens.state.refused], [1, 0]);
  assert.equal(authOf(first), authOf(second));
  assert.equal(await run(one.getCodexLogin), authOf(first));
  assert.match(authOf(first), /"refresh_token":"rt-2"/u);
});

test("a login last refreshed seven days ago is refreshed, its access token hours from stopping, before Codex would refresh it on its own; one refreshed less long ago is not", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const old = login(Date.now() + 5 * 3_600_000, Date.now() - REFRESHED_WITHIN_MS - 60_000, "rt-1");
  await run(store.setCodexLogin(old));
  const tokens = issuer("rt-1");
  const home = homeWith(t, old);
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home));
  await Effect.runPromise(fresh);
  assert.equal(tokens.state.refreshes, 1);
  // Refreshed now, its token two hours from stopping: not due again.
  await Effect.runPromise(fresh);
  const recent = homeWith(t, login(Date.now() + 5 * 3_600_000, Date.now() - Duration.toMillis(Duration.days(6)), "rt-other"));
  await Effect.runPromise((await freshnessOf(store, recent, tokens.refreshOf(recent))).fresh);
  assert.equal(tokens.state.refreshes, 1);
});

test("a refresh refused, its token spent, is not tried again with it: the store's login, refreshed by another, is taken instead", async (t) => {
  const schema = newSchema();
  const store = await testStore({ schema });
  // Both hold a login due whose token another refresh spent.
  const spent = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(spent));
  const tokens = issuer("rt-2");
  const home = homeWith(t, spent);
  // As the refresh is refused, the other alasio keeps what it refreshed.
  const refresh = tokens.refreshOf(home).pipe(Effect.tap((outcome) => (outcome === "refused" ? Effect.orDie(store.setCodexLogin(login(Date.now() + 2 * 3_600_000, Date.now(), "rt-2"))) : Effect.void)));
  const { fresh } = await freshnessOf(store, home, refresh);
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, tokens.state.refused], [0, 1]);
  assert.match(authOf(home), /"refresh_token":"rt-2"/u);
  // Taken, it is not due: nothing asked again.
  await Effect.runPromise(fresh);
  assert.equal(tokens.state.refused, 1);
});

test("a home's login older than the store's, as another alasio refreshed it, is replaced by the store's before it is refreshed, and not refreshed again", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const refreshed = login(Date.now() + 2 * 3_600_000, Date.now(), "rt-2");
  await run(store.setCodexLogin(refreshed));
  const tokens = issuer("rt-2");
  const home = homeWith(t, login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1"));
  await Effect.runPromise((await freshnessOf(store, home, tokens.refreshOf(home))).fresh);
  assert.deepEqual([tokens.state.refreshes, tokens.state.refused], [0, 0]);
  assert.equal(authOf(home), refreshed);
});

test("a lone alasio's refresh refused, as no other kept a login since, refreshes no more: each fresh after fails saying to log in again, with no lock taken nor token offered, until the store keeps another", async (t) => {
  const store = await testStore({ schema: newSchema() });
  // Revoked, or spent by an alasio that died before it kept what it refreshed.
  const spent = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(spent));
  const tokens = issuer("rt-revoked");
  const home = homeWith(t, spent);
  const { counted, taken } = countingLocks(store);
  const { fresh } = await freshnessOf(counted, home, tokens.refreshOf(home));
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const exit = await Effect.runPromiseExit(fresh);
    assert.ok(Exit.isFailure(exit), `attempt ${attempt} was not refused`);
    assert.match(String(Cause.squash(exit.cause)), /run `alasio login codex`/u);
  }
  assert.deepEqual([tokens.state.refreshes, tokens.state.refused, taken.locks], [0, 1, 1]);
  assert.ok((await run(store.getCodexLoginRecord))?.refusedAt);
  // Another alasio on the store, its home holding the same, offers the token neither, nor locks for it.
  const other = homeWith(t, spent);
  const others = countingLocks(store);
  assert.ok(Exit.isFailure(await Effect.runPromiseExit((await freshnessOf(others.counted, other, tokens.refreshOf(other))).fresh)));
  assert.deepEqual([tokens.state.refused, others.taken.locks], [1, 0]);
  // `alasio login codex`: a login kept anew, taken, and kept fresh again.
  const relogged = login(Date.now() + 5 * 3_600_000, Date.now(), "rt-new");
  await run(store.setCodexLogin(relogged));
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), relogged);
  assert.equal((await run(store.getCodexLoginRecord))?.refusedAt, null);
});

/** A while past `ms`. */
const past = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms + 50));

test("a refresh that reaches no issuer refuses nothing: the login is used as it is, and refreshed again once its wait is past, not before", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const due = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(due));
  const tokens = issuer("rt-1");
  tokens.state.unreachable = 1;
  const home = homeWith(t, due);
  const { counted, taken } = countingLocks(store);
  const { fresh } = await freshnessOf(counted, home, tokens.refreshOf(home), 300);
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), due);
  assert.equal((await run(store.getCodexLoginRecord))?.refusedAt, null);
  // Within the wait: asked of no issuer, nor locked for.
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.unreached, tokens.state.refreshes, taken.locks], [1, 0, 1]);
  await past(300);
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.unreached, tokens.state.refreshes, taken.locks], [1, 1, 2]);
  assert.match(authOf(home), /"refresh_token":"rt-2"/u);
});

test("a refresh the app-server fails, as one ending fails it, refuses nothing either, and is tried again once its wait is past", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const due = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(due));
  const tokens = issuer("rt-1");
  const home = homeWith(t, due);
  let failing = true;
  const refresh = Effect.suspend(() => (failing ? Effect.fail(new Gone({ message: "Codex app-server stopped" })) : tokens.refreshOf(home)));
  const { fresh } = await freshnessOf(store, home, refresh, 300);
  await Effect.runPromise(fresh);
  assert.equal((await run(store.getCodexLoginRecord))?.refusedAt, null);
  failing = false;
  await Effect.runPromise(fresh);
  assert.equal(tokens.state.refreshes, 0);
  await past(300);
  await Effect.runPromise(fresh);
  assert.equal(tokens.state.refreshes, 1);
});

test("a home's own login, not yet kept by the store, refused as it is refreshed, is not refreshed again, though the store records no refusal of its own", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const kept = login(Date.now() + 5 * 3_600_000, Date.now(), "rt-kept");
  await run(store.setCodexLogin(kept));
  const tokens = issuer("rt-kept");
  const home = homeWith(t, kept);
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home));
  await Effect.runPromise(fresh);
  // Codex wrote one of its own since, due, whose token is spent, before alasio kept it.
  writeFileSync(join(home, "auth.json"), login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-own-spent"));
  for (let attempt = 0; attempt < 2; attempt += 1) assert.ok(Exit.isFailure(await Effect.runPromiseExit(fresh)));
  assert.equal(tokens.state.refused, 1);
  assert.equal((await run(store.getCodexLoginRecord))?.refusedAt, null);
});

test("a login refused is refreshed again once its home's file changes, as Codex or the operator writes it", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const spent = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(spent));
  const tokens = issuer("rt-2");
  const home = homeWith(t, spent);
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home));
  assert.ok(Exit.isFailure(await Effect.runPromiseExit(fresh)));
  // A login of the home's own, due, whose token works.
  writeFileSync(join(home, "auth.json"), login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-2"));
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, tokens.state.refused], [1, 1]);
});

test("alasio's app-server is reloaded as the home takes the store's login, due or not, and before a refresh of a home it did not leave as it is, never before one it did", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const due = login(Date.now() + 60_000, Date.now() - 3_600_000, "rt-1");
  await run(store.setCodexLogin(due));
  // Each refresh's login due again at once, as only a test's is.
  const tokens = issuer("rt-1", 60_000);
  const home = homeWith(t, due);
  const reloads = { count: 0 };
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home), undefined, reloads);
  // Its home as it was, which alasio knows nothing of yet: loaded anew before the refresh.
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, reloads.count], [1, 1]);
  // Due again as the app-server left it: refreshed without loading it anew.
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, reloads.count], [2, 1]);
  // Another login kept, as `alasio login codex` keeps one: loaded anew as it is taken, and refreshed so, once.
  await run(store.setCodexLogin(login(Date.now() + 60_000, Date.now() - 3_600_000, tokens.state.current)));
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, reloads.count], [3, 2]);
  // One not due, taken: loaded anew all the same, for what else the app-server does with it, and refreshed by none.
  await run(store.setCodexLogin(login(Date.now() + 5 * 3_600_000, Date.now(), "rt-later")));
  await Effect.runPromise(fresh);
  assert.deepEqual([tokens.state.refreshes, reloads.count], [3, 3]);
});

/** A login of an API key's, as `codex login --api-key` writes it: no tokens, no times. */
const apiKey = (key: string) => JSON.stringify({ OPENAI_API_KEY: key, tokens: null });

test("a home takes an API key's login the store kept since, in place of a ChatGPT one, and a ChatGPT one in place of an API key's, whatever their tokens say", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const chatgpt = login(Date.now() + 5 * 3_600_000, Date.now(), "rt-1");
  await run(store.setCodexLogin(chatgpt));
  const tokens = issuer("rt-1");
  const home = homeWith(t, chatgpt);
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home));
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), chatgpt);
  // `alasio login codex` with an API key, in another alasio's pod: its login kept, which no token's time says is newer.
  await run(store.setCodexLogin(apiKey("sk-1")));
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), apiKey("sk-1"));
  // And back to ChatGPT, a login refreshed before the API key's was made.
  const older = login(Date.now() + 5 * 3_600_000, Date.now() - 86_400_000, "rt-1");
  await run(store.setCodexLogin(older));
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), older);
  assert.equal(tokens.state.refreshes, 0);
});

test("a home's own change of its login, as Codex makes one, is kept until the store keeps another", async (t) => {
  const store = await testStore({ schema: newSchema() });
  const kept = login(Date.now() + 5 * 3_600_000, Date.now(), "rt-1");
  await run(store.setCodexLogin(kept));
  const tokens = issuer("rt-1");
  const home = homeWith(t, kept);
  const { fresh } = await freshnessOf(store, home, tokens.refreshOf(home));
  await Effect.runPromise(fresh);
  const own = login(Date.now() + 5 * 3_600_000, Date.now(), "rt-own");
  writeFileSync(join(home, "auth.json"), own);
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), own);
  await run(store.setCodexLogin(apiKey("sk-2")));
  await Effect.runPromise(fresh);
  assert.equal(authOf(home), apiKey("sk-2"));
});
