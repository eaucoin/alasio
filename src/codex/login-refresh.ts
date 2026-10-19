/**
 * alasio's Codex login, kept fresh by alasio alone where its harnesses run in pools of their
 * own (../harness/pooled-host.ts). No Codex of alasio's process calls the model there, so none
 * refreshes the login as Codex does as it calls, on a refusal or once the login is old; and the
 * pods' Codex calls the model through alasio's relay (./login-relay.ts), holding no login to
 * refresh. So before each Codex turn alasio drives in a pod, and while it runs, a ChatGPT login
 * whose access token stops within FRESH_FOR_MS is refreshed through Codex's own refresh, by
 * alasio's app-server (account/read, its refresh token asked for), one refresh at a time: one
 * process alone ever refreshes the login, whose refresh token works once. A login of an API key
 * has nothing to refresh, nor one whose access token says no time.
 */
import { readFileSync } from "node:fs";

import { Clock, Effect, Semaphore } from "effect";

/** How long before its access token stops a login is refreshed: past the longest a turn's relay token is good for. */
const FRESH_FOR_MS = 30 * 60_000;

/**
 * When the access token of the ChatGPT login in `authFile` stops, in milliseconds since the
 * epoch, as its JWT's `exp` says; null where there is no such login, or it says no time.
 */
function accessTokenExpiry(authFile: string): number | null {
  try {
    // Codex writes auth.json; what is read of it is checked below.
    const auth: { readonly tokens?: { readonly access_token?: unknown } | null } = JSON.parse(readFileSync(authFile, "utf8"));
    const token = auth.tokens?.access_token;
    if (typeof token !== "string") return null;
    const claims: { readonly exp?: unknown } = JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString("utf8"));
    return typeof claims.exp === "number" ? claims.exp * 1000 : null;
  } catch {
    return null;
  }
}

/** The login of `authFile` kept fresh, as the module says, by `refresh`, Codex's refresh of it: run before what calls the model with it. */
export const makeLoginFreshness = <E>(authFile: string, refresh: Effect.Effect<void, E>): Effect.Effect<Effect.Effect<void, E>> =>
  Effect.map(Semaphore.make(1), (oneAtATime) =>
    Effect.gen(function*() {
      const stops = accessTokenExpiry(authFile);
      if (stops === null || stops - (yield* Clock.currentTimeMillis) > FRESH_FOR_MS) return;
      yield* refresh;
      yield* Effect.logInfo(`refreshed the Codex login, whose access token stopped at ${new Date(stops).toISOString()}`);
    }).pipe(Semaphore.withPermit(oneAtATime)));
