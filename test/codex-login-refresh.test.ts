/**
 * alasio's Codex login kept fresh (src/codex/login-refresh.ts) by the alasios on one store, each
 * with a home of its own: a login due refreshed by one of them alone, holding the store's lock,
 * the others taking what it kept; one due as its last refresh is old, whatever its access token
 * says; a refresh refused never tried again with its token; and a home that takes whatever
 * login the store kept since it last took one, of an API key or of ChatGPT, by the store's own
 * stamp of each save.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";

import { Data, Duration, Effect } from "effect";

import { makeCodexLoginFreshness, REFRESHED_WITHIN_MS } from "../src/codex/login-refresh.ts";
import { Store } from "../src/persistence/store.ts";
import { newSchema, run, testStore } from "./support/store.ts";

/** An access token stopping at `exp`, in milliseconds since the epoch, as a JWT carries it. */
const jwt = (exp: number) => ["e30", Buffer.from(JSON.stringify({ exp: Math.floor(exp / 1000) })).toString("base64url"), "c2ln"].join(".");

/** A ChatGPT login's auth.json, its access token stopping at `stops`, last refreshed at `refreshed`, with the refresh token `token`. */
const login = (stops: number, refreshed: number, token: string) =>
  JSON.stringify({ tokens: { access_token: jwt(stops), refresh_token: token }, last_refresh: new Date(refreshed).toISOString() });

class Refused extends Data.TaggedError("Refused")<{ readonly message: string }> {}

/**
 * What issues the login's tokens, as OpenAI's does: each refresh token works once, a refresh
 * writing the home's auth.json anew, as Codex writes it, with a token stopping in two hours.
 */
function issuer(first: string) {
  const state = { current: first, refreshes: 0, refused: 0 };
  const refreshOf = (home: string) =>
    Effect.gen(function*() {
      const auth: { readonly tokens: { readonly refresh_token: string } } = JSON.parse(readFileSync(join(home, "auth.json"), "utf8"));
      // Slow enough that two alasios not holding the lock would both ask.
      yield* Effect.sleep("100 millis");
      if (auth.tokens.refresh_token !== state.current) {
        state.refused += 1;
        return yield* new Refused({ message: `the refresh token ${auth.tokens.refresh_token} was used already` });
      }
      state.refreshes += 1;
      state.current = `rt-${state.refreshes + 1}`;
      writeFileSync(join(home, "auth.json"), login(Date.now() + 2 * 3_600_000, Date.now(), state.current));
    });
  return { state, refreshOf };
}

/** A Codex home of its own, holding `auth`, removed as the test ends. */
function homeWith(t: TestContext, auth: string): string {
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-home-"));
  t.after(() => rmSync(home, { recursive: true, force: true }));
  writeFileSync(join(home, "auth.json"), auth);
  return home;
}

/** The freshness of the login of `home`, of an alasio on the store `store`. */
const freshnessOf = (store: Store["Service"], home: string, refresh: Effect.Effect<void, Refused>) =>
  Effect.runPromise(makeCodexLoginFreshness({ home, refresh }).pipe(Effect.provideService(Store, store)));

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
  const refresh = tokens.refreshOf(home).pipe(Effect.tapError(() => Effect.orDie(store.setCodexLogin(login(Date.now() + 2 * 3_600_000, Date.now(), "rt-2")))));
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
