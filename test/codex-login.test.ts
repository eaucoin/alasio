/**
 * Codex's login kept in alasio's store, where alasio's Codex home does not outlast its
 * pod: written back as alasio starts, and kept as Codex rewrites it.
 */
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Exit, Layer, Scope } from "effect";

import { keepCodexLogin } from "../src/codex/login.ts";
import { Store } from "../src/persistence/store.ts";
import { run, testStore } from "./support/store.ts";
import { eventually } from "./support/wait.ts";

const LOGIN = JSON.stringify({ tokens: { access_token: "a1", refresh_token: "r1" } });
const REFRESHED = JSON.stringify({ tokens: { access_token: "a2", refresh_token: "r2" } });

/** Keeps the login of a Codex home on `store` until the returned stop, as alasio does while it runs. */
async function keeping(store: Store["Service"], home: string): Promise<() => Promise<void>> {
  const scope = await Effect.runPromise(Scope.make());
  await Effect.runPromise(keepCodexLogin(home).pipe(Scope.provide(scope), Effect.provide(Layer.succeed(Store, store))));
  return () => Effect.runPromise(Scope.close(scope, Exit.void));
}

test("the login kept is written into a new home, for Codex alone to read, and what Codex rewrites is kept", async () => {
  const store = await testStore();
  await run(store.setCodexLogin(LOGIN));
  const home = join(mkdtempSync(join(tmpdir(), "alasio-codex-login-")), ".codex");
  try {
    const stop = await keeping(store, home);
    const file = join(home, "auth.json");
    assert.equal(readFileSync(file, "utf8"), LOGIN);
    assert.equal(statSync(file).mode & 0o777, 0o600);

    // Codex refreshes its login, spending the refresh token it had.
    writeFileSync(file, REFRESHED);
    await eventually("the refreshed login to be kept", async () => (await run(store.getCodexLogin)) === REFRESHED || undefined);
    await stop();
  } finally {
    rmSync(join(home, ".."), { recursive: true, force: true });
  }
});

test("a login already in the home is newer than the one kept, and is kept; a logout keeps none", async () => {
  const store = await testStore();
  await run(store.setCodexLogin(LOGIN));
  const home = mkdtempSync(join(tmpdir(), "alasio-codex-login-"));
  try {
    writeFileSync(join(home, "auth.json"), REFRESHED);
    const stop = await keeping(store, home);
    assert.equal(await run(store.getCodexLogin), REFRESHED);
    assert.equal(readFileSync(join(home, "auth.json"), "utf8"), REFRESHED);

    rmSync(join(home, "auth.json"));
    await eventually("the logout to be kept", async () => (await run(store.getCodexLogin)) === null || undefined);
    await stop();
  } finally {
    rmSync(home, { recursive: true, force: true });
  }
});

