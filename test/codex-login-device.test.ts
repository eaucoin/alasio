/**
 * Codex's device login as `alasio login codex` runs it in a pod of the Codex login relay's
 * (src/codex/login-device.ts): Codex's login, at the terminal, in the relay's home, the login it
 * writes there kept in the store as the relay's role, whatever alasio runs; and its exit, Codex's.
 */
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { ConfigProvider, Context, Effect, Layer } from "effect";
import pg from "pg";

import { CODEX_LOGIN_ROLE, HARNESS_ROLE } from "../neon/control/secrets.ts";
import { scramVerifier } from "../neon/control/scram.ts";
import { Neon } from "../src/neon/connect.ts";
import { LakeWake } from "../src/neon/lake-wake.ts";
import { Store } from "../src/persistence/store.ts";
import { startPostgres } from "./support/postgres.ts";

const PROGRAM = fileURLToPath(new URL("../src/codex/login-device.ts", import.meta.url));

test("Codex's device login run as the relay's role keeps the login Codex writes in its home in the store, and exits as Codex does", { timeout: 120_000 }, async () => {
  const database = await startPostgres();
  const directory = mkdtempSync(join(tmpdir(), "alasio-login-device-"));
  const as = (user: string, password: string) => {
    const url = new URL(database.url);
    Object.assign(url, { username: user, password, pathname: "/alasio" });
    return url.toString();
  };
  try {
    const admin = new pg.Client({ connectionString: database.url });
    await admin.connect();
    await admin.query("create role alasio login password 'alasio-password' createrole createdb");
    await admin.query("create database alasio owner alasio");
    await admin.end();
    const env = {
      ALASIO_DATABASE_URL: as("alasio", "alasio-password"),
      ALASIO_LAKE_VERIFIER: scramVerifier("lake-password"),
      ALASIO_HARNESS_VERIFIER: scramVerifier("harness-password"),
      ALASIO_CODEX_LOGIN_VERIFIER: scramVerifier("codex-login-password"),
    };
    /** alasio's store, which makes the state and grants the relay's role the login, while `use` runs on it. */
    const withAlasio = <A>(use: (store: Store["Service"]) => Effect.Effect<A, unknown>) =>
      Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        const neon = yield* Neon.make;
        return yield* use(Context.get(yield* Layer.build(Store.layer({ pool: neon.pool, harnessRole: HARNESS_ROLE, codexLoginRole: CODEX_LOGIN_ROLE })), Store));
      }).pipe(Effect.provideService(LakeWake, LakeWake.none), Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env }))))));
    await withAlasio(() => Effect.void);
    // A Codex that writes the login the operator's code gave it, what it was asked, and exits as told.
    const codex = join(directory, "codex");
    writeFileSync(codex, `#!/bin/sh\necho "$*" >"$CODEX_HOME/asked"\nprintf '%s' '{"tokens":"of the device"}' >"$CODEX_HOME/auth.json"\nexit "\${CODEX_EXIT:-0}"\n`, { mode: 0o755 });
    /** The program, as the relay's pod runs it: its exit code. */
    const login = (home: string, exit: number) =>
      new Promise<number | null>((resolve) => {
        const child = spawn(process.execPath, [PROGRAM], {
          stdio: ["ignore", "inherit", "inherit"],
          env: { PATH: process.env["PATH"] ?? "", ALASIO_DATABASE_URL: as(CODEX_LOGIN_ROLE, "codex-login-password"), ALASIO_KEEP_CODEX_LOGIN: "1", CODEX_HOME: home, ALASIO_CODEX_BIN: codex, CODEX_EXIT: String(exit) },
        });
        child.on("exit", (code) => resolve(code));
      });
    const home = join(directory, "home");
    assert.equal(await login(home, 0), 0);
    assert.equal(readFileSync(join(home, "asked"), "utf8").trim(), "login --device-auth");
    assert.equal(await withAlasio((store) => store.getCodexLogin), '{"tokens":"of the device"}');
    // And a login Codex fails: its code, whatever it wrote kept still.
    assert.equal(await login(join(directory, "again"), 3), 3);
  } finally {
    rmSync(directory, { recursive: true, force: true });
    await database.stop();
  }
});
