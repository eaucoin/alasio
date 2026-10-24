/**
 * Codex's device login at the operator's terminal, as `alasio login codex` runs it in a pod of
 * the environment's Codex login relay (cli/src/commands/login.ts), so it runs whether or not the
 * environment's alasio runs: Codex's login by a code entered on another device, in the relay's
 * Codex home, reaching the store as the relay's role does, the login's tables alone. Where that
 * home is the relay's own (ALASIO_KEEP_CODEX_LOGIN), the login Codex writes there is kept in the
 * store (./login.ts), whence alasio and the relays take it; the operator's own home, under the
 * host profile, keeps it itself. It exits as Codex does.
 */
import { spawn } from "node:child_process";

import { Effect, Layer } from "effect";

import { connectRelayNeon } from "../neon/connect.ts";
import { Store } from "../persistence/store.ts";
import { codexBinPath } from "./app-server/process.ts";
import { codexHome } from "./env.ts";
import { keepCodexLogin } from "./login.ts";

/** Codex's arguments of its login by a code entered on another device. */
export const DEVICE_LOGIN = ["login", "--device-auth"] as const;

/** Codex's device login, at this process's terminal, in `home`: the code it exited with. */
const deviceLogin = (home: string): Effect.Effect<number> =>
  Effect.promise(() =>
    new Promise<number>((resolve) => {
      const child = spawn(codexBinPath(), [...DEVICE_LOGIN], { stdio: "inherit", env: { ...process.env, CODEX_HOME: home } });
      child.on("error", (error) => {
        console.error(`codex could not be run: ${error.message}`);
        resolve(127);
      });
      child.on("exit", (code, signal) => resolve(code ?? (signal === null ? 1 : 128)));
    })
  );

const home = codexHome();
const login = process.env["ALASIO_KEEP_CODEX_LOGIN"] === "1"
  // Kept as Codex writes it, and once more as the scope closes, after Codex has ended.
  ? Effect.scoped(Effect.gen(function*() {
    const pool = yield* connectRelayNeon;
    const store = yield* Layer.build(Store.layer({ pool, makesSchema: false }));
    return yield* Effect.andThen(keepCodexLogin(home), deviceLogin(home)).pipe(Effect.provideContext(store));
  }))
  : deviceLogin(home);
process.exitCode = await Effect.runPromise(login);
