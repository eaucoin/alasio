/**
 * alasio's Codex login, kept fresh by alasio alone where its harnesses run in pools of their
 * own (../harness/pooled-host.ts), whichever of the alasios on one store does. No Codex of
 * alasio's process calls the model for a turn there, and the pods' Codex calls it through
 * alasio's relay (./login-relay.ts), holding no login to refresh. So before each Codex turn alasio
 * drives in a pod, and while it runs, before each request the relay passes on, and before
 * alasio's own app-server lists the login's models, a ChatGPT login that is due is refreshed
 * through Codex's own refresh, by alasio's app-server (account/read, its refresh token asked
 * for).
 *
 * A login is due where its access token stops within FRESH_FOR_MS, or it was last refreshed
 * REFRESHED_WITHIN_MS ago: before Codex would refresh it on its own as it calls (codex-rs
 * refreshes one whose access token stops within 5 minutes, or last refreshed 8 days ago,
 * login/src/auth/manager.rs), so no Codex of alasio's ever does, outside the lock below. A login
 * of an API key has nothing to refresh, nor one whose access token says no time.
 *
 * Its refresh token works once, and every alasio on the store holds a copy of the login in its
 * own Codex home, the store's being the one that counts. So the home takes the store's login
 * whenever the store kept one after the one the home last took, as the store's own stamp of each
 * save says (Store.getCodexLoginRecord), whatever the login is: one another alasio refreshed, or
 * one `alasio login codex` made, of an API key or of ChatGPT, in place of the other. And a due
 * login is refreshed holding the store's Codex login lock (Store.withCodexLoginLock): the home
 * made the store's first, as above, and refreshed only where it is due still, the store kept of
 * what the refresh wrote, in the same transaction. A refresh refused reads the store again,
 * never refreshing again with a token spent. One that is not due is left as it is, without the
 * lock.
 */
import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Clock, Context, Duration, Effect, Layer, Semaphore } from "effect";

import { type CodexLoginRecord, Store } from "../persistence/store.ts";
import { CodexAppServer } from "./app-server/client.ts";
import { buildCodexEnv } from "./env.ts";

/** How long before its access token stops a login is refreshed: past the longest a turn's relay token is good for. */
export const FRESH_FOR_MS = 30 * 60_000;

/**
 * How long after it was last refreshed a login is refreshed, whatever its access token says: a
 * day short of codex-rs's TOKEN_REFRESH_INTERVAL, 8 days, past which Codex refreshes a login on
 * its own as it next calls (login/src/auth/manager.rs).
 */
export const REFRESHED_WITHIN_MS = Duration.toMillis(Duration.days(7));

/** When a ChatGPT login stops and was last refreshed, in milliseconds since the epoch, each null where it says none. */
interface LoginTimes {
  readonly stops: number | null;
  readonly refreshed: number | null;
}

/** The times of the ChatGPT login in `auth`, the text of an auth.json; null where it is no such login. */
function timesOf(auth: string | null): LoginTimes | null {
  if (auth === null) return null;
  try {
    // Codex writes auth.json; what is read of it is checked below.
    const parsed: { readonly tokens?: { readonly access_token?: unknown } | null; readonly last_refresh?: unknown } = JSON.parse(auth);
    const token = parsed.tokens?.access_token;
    if (typeof token !== "string") return null;
    const claims: { readonly exp?: unknown } = JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString("utf8"));
    const refreshed = typeof parsed.last_refresh === "string" ? Date.parse(parsed.last_refresh) : Number.NaN;
    return { stops: typeof claims.exp === "number" ? claims.exp * 1000 : null, refreshed: Number.isNaN(refreshed) ? null : refreshed };
  } catch {
    return null;
  }
}

/** Whether the login in `auth` is due at `now`, as the module says. */
const due = (auth: string | null, now: number): boolean => {
  const times = timesOf(auth);
  if (times === null) return false;
  return (times.stops !== null && times.stops - now <= FRESH_FOR_MS) || (times.refreshed !== null && now - times.refreshed >= REFRESHED_WITHIN_MS);
};

/** What keeps alasio's Codex login fresh, as the module says. */
export class CodexLoginFreshness extends Context.Service<CodexLoginFreshness, {
  /** The home made the store's login where the store kept one since, then refreshed where it is due, as the module says; what fails is said, and what there is used. */
  readonly fresh: Effect.Effect<void>;
}>()("alasio/codex/CodexLoginFreshness") {
  /** The login of the Codex home `home`, refreshed by alasio's app-server. */
  static readonly layer = (home: string): Layer.Layer<CodexLoginFreshness, never, Store | CodexAppServer> =>
    Layer.effect(CodexLoginFreshness, Effect.flatMap(CodexAppServer, (appServer) => makeCodexLoginFreshness({ home, refresh: appServer.refreshLogin({ env: buildCodexEnv(), cwd: home }) })));
}

/** The freshness of the login of the Codex home `home`, `refresh` Codex's refresh of it, which rewrites its auth.json. */
export const makeCodexLoginFreshness = <E extends { readonly message: string }>({ home, refresh }: { readonly home: string; readonly refresh: Effect.Effect<void, E> }) =>
  Effect.gen(function*() {
    const store = yield* Store;
    const path = join(home, "auth.json");
    /** The home's auth.json, or null where Codex has none there. */
    const read = Effect.sync(() => {
      try {
        return readFileSync(path, "utf8");
      } catch {
        return null;
      }
    });
    /** `auth` written as the home's auth.json, beside it and moved into place, as Codex reads it whole or not at all. */
    const write = (auth: string) =>
      Effect.sync(() => {
        writeFileSync(`${path}.alasio`, auth, { mode: 0o600 });
        renameSync(`${path}.alasio`, path);
      });
    /** When the store kept the login the home last took or kept, by the store's own clock; null before it took any. */
    let tookSavedAt: number | null = null;
    /** The store's login, `kept`, written to the home where the store kept it since the home last took one, and it differs. */
    const take = (kept: CodexLoginRecord | null) =>
      Effect.gen(function*() {
        if (kept === null || kept.savedAt === tookSavedAt) return;
        if (kept.auth !== (yield* read)) {
          yield* write(kept.auth);
          yield* Effect.logInfo("took the Codex login the store kept since its home last took one");
        }
        tookSavedAt = kept.savedAt;
      });
    const oneAtATime = yield* Semaphore.make(1);
    const fresh = Effect.gen(function*() {
      yield* take(yield* store.getCodexLoginRecord);
      if (!due(yield* read, yield* Clock.currentTimeMillis)) return;
      yield* store.withCodexLoginLock((login) =>
        Effect.gen(function*() {
          yield* take(yield* login.get);
          const current = yield* read;
          if (!due(current, yield* Clock.currentTimeMillis)) return;
          const refreshed = yield* Effect.result(refresh);
          if (refreshed._tag === "Failure") {
            // Refused, as its token was spent: what the store keeps is read again, and the token never offered again.
            yield* take(yield* login.get);
            return yield* Effect.logWarning(`the Codex login could not be refreshed: ${refreshed.failure.message}`);
          }
          const after = yield* read;
          if (after !== null) tookSavedAt = yield* login.save(after);
          const stopped = timesOf(current)?.stops;
          yield* Effect.logInfo(`refreshed the Codex login${stopped ? `, whose access token stopped at ${new Date(stopped).toISOString()}` : ""}`);
        })
      );
    }).pipe(
      Semaphore.withPermit(oneAtATime),
      Effect.catch((error) => Effect.logWarning(`the Codex login could not be kept fresh: ${error.message}`)),
    );
    return CodexLoginFreshness.of({ fresh });
  });
