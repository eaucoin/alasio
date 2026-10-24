/**
 * `alasio codex-login`, a program of alasio's image: the relay of an environment's Codex login
 * to its harness pools' pods (./login-relay.ts), a component of its own
 * (cli/src/manifests/codex-login.ts), which KEDA keeps up while any of the environment's Codex
 * harnesses is bound, and at none otherwise. Its Service is the one the pods' Codex reaches
 * (ALASIO_CODEX_LOGIN_URL), so a pod bound to a conversation goes on calling the model while the
 * environment's alasio sleeps.
 *
 * It keeps the login fresh before each request it passes on (./login-refresh.ts), refreshing a
 * login that is due through an app-server of its own, under the store's Codex login lock, as
 * alasio's own app-server does for the panels: whichever finds the login due first refreshes it,
 * and the other takes it from the store. The store is the login's record: before each request,
 * the relay's home takes the login the store kept since it last took one, as one `alasio login
 * codex` made in a pod of the relay's is (./login-device.ts). A home of the relay's own (ALASIO_KEEP_CODEX_LOGIN) is written from the
 * store as it starts, and what Codex itself writes there kept again in the store (./login.ts), as
 * alasio does with a home of its own; the operator's home, where the host profile gives alasio
 * theirs, is the relay's too.
 *
 * A request bears a token alasio gave the turn of a conversation's harness, checked by the key
 * alasio signs them with (poolLoginKey), the relay's alone of the environment's keys: it holds
 * none of the caller key that key is made of, which signs the environment's sessions' tokens. Any
 * other is refused. It reaches alasio's Neon as a role of its own, which reaches the login of the
 * state alone (../neon/codex-login.ts), and makes nothing there.
 *
 * Environment: ALASIO_DATABASE_URL (or its _FILE form), the key of the turns' tokens
 * (CODEX_LOGIN_KEY_VARIABLE, in hexadecimal), ALASIO_KEEP_CODEX_LOGIN, and CODEX_HOME or HOME.
 */
import { join } from "node:path";

import { Effect, Layer, type Scope } from "effect";

import { CODEX_LOGIN_KEY_VARIABLE } from "../harness/names.ts";
import { connectRelayNeon } from "../neon/connect.ts";
import { Store } from "../persistence/store.ts";
import { withLogScope } from "../shared/log.ts";
import { CodexAppServer } from "./app-server/client.ts";
import { codexHome } from "./env.ts";
import { keepCodexLogin } from "./login.ts";
import { CodexLoginFreshness } from "./login-refresh.ts";
import { CODEX_LOGIN_PORT, type LoginRelayOptions, poolLoginRefusal, startLoginRelay } from "./login-relay.ts";

/** What the relay is run with, as its environment says. */
export interface RelayConfig {
  /** The key alasio signs its turns' tokens for the relay with (poolLoginKey). */
  readonly loginKey: Buffer;
  /** Whether its Codex home is its own, written from the store and kept there, rather than the operator's. */
  readonly keepCodexLogin: boolean;
  /** Its Codex home. */
  readonly home: string;
  /** The port it listens on: CODEX_LOGIN_PORT, which its Service reaches it by, unless a test says. */
  readonly port: number;
}

/** The relay's configuration from `env`, its process's. */
export function loadRelayConfig(env: NodeJS.ProcessEnv = process.env): RelayConfig {
  const loginKey = env[CODEX_LOGIN_KEY_VARIABLE]?.trim() ?? "";
  if (!/^(?:[0-9a-f]{2})+$/u.test(loginKey)) throw new Error(`${CODEX_LOGIN_KEY_VARIABLE} must be set, in hexadecimal`);
  return { loginKey: Buffer.from(loginKey, "hex"), keepCodexLogin: env["ALASIO_KEEP_CODEX_LOGIN"] === "1", home: codexHome(env), port: CODEX_LOGIN_PORT };
}

/** The relay of `config`, on the store, until the scope closes, as the module says, sending each request on as `upstreamFor` says, a test's: the port it listens on. */
export const startRelay = Effect.fnUntraced(function*({ loginKey, keepCodexLogin: keeps, home, port }: RelayConfig, { upstreamFor }: Pick<LoginRelayOptions, "upstreamFor"> = {}): Effect.fn.Return<number, unknown, Store | CodexLoginFreshness | Scope.Scope> {
  if (keeps) yield* keepCodexLogin(home);
  const { fresh } = yield* CodexLoginFreshness;
  const authFile = join(home, "auth.json");
  const relay = yield* startLoginRelay({
    authFile,
    // A login refused passes on as it is, its access token good until it stops: a Codex turn refused it says why.
    fresh: Effect.catchTag(fresh, "CodexLoginRefused", () => Effect.void),
    listen: { host: "0.0.0.0", port },
    authorize: poolLoginRefusal(loginKey, Date.now),
    ...(upstreamFor ? { upstreamFor } : {}),
  });
  return Number(new URL(relay.url).port);
}, withLogScope("codex-login-relay"));

/** `alasio codex-login`, as ./relay-main.ts runs it: the relay, from its environment, on alasio's Neon, until it is stopped. */
export const relay: Effect.Effect<never, unknown, Scope.Scope> = Effect.gen(function*() {
  const config = loadRelayConfig();
  const pool = yield* connectRelayNeon;
  // Built in the program's scope, as the relay uses them for as long as it serves.
  const services = yield* Layer.build(CodexLoginFreshness.layer(config.home).pipe(
    Layer.provideMerge(CodexAppServer.layer),
    Layer.provideMerge(Store.layer({ pool, makesSchema: false })),
  ));
  yield* startRelay(config).pipe(Effect.provideContext(services));
  return yield* Effect.never;
});
