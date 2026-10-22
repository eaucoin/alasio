/**
 * What the pinned Codex's app-server says of a refresh of its login (src/codex/app-server/
 * thread-client.ts LoginRefresh), its issuer a stand-in of OpenAI's (Codex's
 * CODEX_REFRESH_TOKEN_URL_OVERRIDE): a refresh token refused for good, spent, expired or revoked,
 * is "refused"; one whose issuer resets the connection, or answers 500, leaves the login as it
 * was and is "asked", as is one refreshed, its home's auth.json rewritten, and one of a home
 * holding no login. And alasio's login kept fresh on it (src/codex/login-refresh.ts): its
 * long-lived app-server reaching the issuer for each login the home takes from the store, of the
 * account before or another, and for one kept after its login was refused.
 */
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Cause, Context, Effect, Exit, Layer, Logger, Scope } from "effect";

import { CodexAppServer, makeAppServer } from "../src/codex/app-server/client.ts";
import { buildCodexEnv } from "../src/codex/env.ts";
import { CodexLoginFreshness } from "../src/codex/login-refresh.ts";
import { Store } from "../src/persistence/store.ts";
import { newSchema, run, testStore } from "./support/store.ts";

const CODEX_BIN = new URL("../node_modules/.bin/codex", import.meta.url).pathname;
const skip = !existsSync(CODEX_BIN) && "needs the Codex binary alasio installs";

/** A JWT of `claims`, unsigned, as Codex reads one without checking its signature. */
const jwt = (claims: object) => ["e30", Buffer.from(JSON.stringify(claims)).toString("base64url"), "c2ln"].join(".");

/** The claims of the ChatGPT account `account`, as Codex reads them of its tokens. */
const accountOf = (account: string) => ({ "https://api.openai.com/auth": { chatgpt_account_id: account, chatgpt_plan_type: "plus" } });

/** Tokens as OpenAI's issuer gives them, of `account`: an access token stopping in `stopsInMs`, and `refresh`. */
const tokensOf = (stopsInMs: number, refresh: string, account = "account-1") => ({
  id_token: jwt({ email: "codex@example.test", ...accountOf(account) }),
  access_token: jwt({ exp: Math.floor((Date.now() + stopsInMs) / 1000), ...accountOf(account) }),
  refresh_token: refresh,
});

/** The auth.json of a login of `account` whose refresh token is `refresh`, its access token stopping in 20 minutes: due for alasio, not yet for Codex. */
const loginOf = (refresh: string, account = "account-1") =>
  JSON.stringify({ OPENAI_API_KEY: null, tokens: { ...tokensOf(20 * 60_000, refresh, account), account_id: account }, last_refresh: new Date().toISOString() });

/** How the stand-in issuer answers a refresh. */
type Answer = { readonly status: number; readonly body: object } | "reset";

/**
 * What `answer` comes to, as alasio's app-server refreshes a login due, on a home of its own:
 * what it says, the refresh token the home holds after, and how often the issuer was asked. Its
 * access token stops in 20 minutes: due for alasio (FRESH_FOR_MS), not yet for Codex on its own.
 * `loggedIn` false, the home holds no login.
 */
async function refreshAnswered(answer: Answer, { loggedIn = true } = {}) {
  const asked: string[] = [];
  const issuer = createServer((request, response: ServerResponse) => {
    asked.push(request.url ?? "");
    request.resume();
    request.on("end", () => {
      if (answer === "reset") return void request.socket.destroy();
      response.writeHead(answer.status, { "content-type": "application/json" }).end(JSON.stringify(answer.body));
    });
  });
  await new Promise<void>((resolve) => issuer.listen(0, "127.0.0.1", resolve));
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-refresh-"));
  const scope = Effect.runSync(Scope.make());
  try {
    if (loggedIn) writeFileSync(join(home, "auth.json"), loginOf("rt-1"));
    const env = { ...buildCodexEnv(), CODEX_HOME: home, CODEX_REFRESH_TOKEN_URL_OVERRIDE: `http://127.0.0.1:${(issuer.address() as AddressInfo).port}/oauth/token` };
    const said = await Effect.runPromise(
      Effect.flatMap(makeAppServer(), (appServer) => appServer.refreshLogin({ env, cwd: home })).pipe(
        Scope.provide(scope),
        Effect.provideService(Logger.CurrentLoggers, new Set()),
      ),
    );
    const held: { readonly tokens: { readonly refresh_token: string } } | null = loggedIn ? JSON.parse(readFileSync(join(home, "auth.json"), "utf8")) : null;
    return { said, held: held?.tokens.refresh_token ?? null, asked: asked.length };
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    issuer.close();
    rmSync(home, { recursive: true, force: true });
  }
}

for (const code of ["refresh_token_reused", "refresh_token_expired", "refresh_token_invalidated"]) {
  test(`a refresh token the issuer refuses as ${code} is refused`, { skip }, async () => {
    assert.deepEqual(await refreshAnswered({ status: 401, body: { error: { code, message: "refused" } } }), { said: "refused", held: "rt-1", asked: 1 });
  });
}

test("a refresh whose issuer resets the connection, or answers 500, is asked, its login left as it was", { skip }, async () => {
  assert.deepEqual(await refreshAnswered("reset"), { said: "asked", held: "rt-1", asked: 1 });
  assert.deepEqual(await refreshAnswered({ status: 500, body: { error: "unavailable" } }), { said: "asked", held: "rt-1", asked: 1 });
});

test("a refresh the issuer answers is asked, its home's login rewritten", { skip }, async () => {
  assert.deepEqual(await refreshAnswered({ status: 200, body: tokensOf(2 * 3_600_000, "rt-2") }), { said: "asked", held: "rt-2", asked: 1 });
});

test("a home holding no login is asked, refused nothing, its issuer never reached", { skip }, async () => {
  assert.deepEqual(await refreshAnswered({ status: 401, body: { error: { code: "refresh_token_reused" } } }, { loggedIn: false }), { said: "asked", held: null, asked: 0 });
});

test("alasio's app-server, refreshing on, reaches the issuer for each login the home takes from the store, of the account before or another, and for one kept after its login was refused", { skip }, async (t) => {
  // A spent refresh token refused for good; any other refreshed, its login due again at once.
  const asked: string[] = [];
  const issuer = createServer((request, response: ServerResponse) => {
    let body = "";
    request.on("data", (chunk: Buffer) => (body += chunk.toString()));
    request.on("end", () => {
      const { refresh_token: refresh }: { readonly refresh_token: string } = JSON.parse(body);
      asked.push(refresh);
      const [status, answer] = refresh.startsWith("spent") ? [401, { error: { code: "refresh_token_reused" } }] : [200, tokensOf(20 * 60_000, `issued-${asked.length}`)];
      response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(answer));
    });
  });
  await new Promise<void>((resolve) => issuer.listen(0, "127.0.0.1", resolve));
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-refresh-"));
  // alasio's app-server runs with alasio's environment (buildCodexEnv): this home, and the issuer standing in.
  const saved = { home: process.env["CODEX_HOME"], issuer: process.env["CODEX_REFRESH_TOKEN_URL_OVERRIDE"] };
  process.env["CODEX_HOME"] = home;
  process.env["CODEX_REFRESH_TOKEN_URL_OVERRIDE"] = `http://127.0.0.1:${(issuer.address() as AddressInfo).port}/oauth/token`;
  const scope = Effect.runSync(Scope.make());
  t.after(async () => {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    issuer.close();
    rmSync(home, { recursive: true, force: true });
    for (const [name, value] of [["CODEX_HOME", saved.home], ["CODEX_REFRESH_TOKEN_URL_OVERRIDE", saved.issuer]] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });
  const store = await testStore({ schema: newSchema() });
  await run(store.setCodexLogin(loginOf("rt-1")));
  writeFileSync(join(home, "auth.json"), loginOf("rt-1"));
  const quiet = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromiseExit(effect.pipe(Effect.provideService(Logger.CurrentLoggers, new Set())));
  const built = await Effect.runPromise(Layer.build(CodexLoginFreshness.layer(home).pipe(Layer.provide(Layer.mergeAll(CodexAppServer.layer, Layer.succeed(Store, store))))).pipe(Scope.provide(scope)));
  const { fresh } = Context.get(built, CodexLoginFreshness);
  assert.ok(Exit.isSuccess(await quiet(fresh)));
  // Kept by another alasio, as one it refreshed, of the account before; then of another, as `alasio login codex` keeps one.
  for (const [refresh, account] of [["rt-2", "account-1"], ["rt-3", "account-2"]] as const) {
    await run(store.setCodexLogin(loginOf(refresh, account)));
    assert.ok(Exit.isSuccess(await quiet(fresh)));
  }
  // A login whose token is spent, refused; then one kept anew, refreshed.
  await run(store.setCodexLogin(loginOf("spent-4", "account-2")));
  const refused = await quiet(fresh);
  assert.ok(Exit.isFailure(refused) && String(Cause.squash(refused.cause)).includes("alasio login codex"), String(refused));
  await run(store.setCodexLogin(loginOf("rt-5", "account-2")));
  assert.ok(Exit.isSuccess(await quiet(fresh)));
  assert.deepEqual(asked, ["rt-1", "rt-2", "rt-3", "spent-4", "rt-5"]);
  const held: { readonly tokens: { readonly refresh_token: string } } = JSON.parse(readFileSync(join(home, "auth.json"), "utf8"));
  assert.equal(held.tokens.refresh_token, "issued-5");
});
