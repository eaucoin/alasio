/**
 * alasio's Codex login, kept fresh by alasio and by its environment's relay of it (./relay.ts)
 * where its harnesses run in pools of their own (../harness/pooled-host.ts), whichever of them
 * on one store does. No Codex of alasio's process calls the model for a turn there, and the pods'
 * Codex calls it through the relay (./login-relay.ts), holding no login to refresh. So before
 * each Codex turn alasio drives in a pod, and while it runs, and before alasio's own app-server
 * lists the login's models, by alasio's app-server, and before each request the relay passes
 * on, by the relay's, a ChatGPT login that is due is refreshed through Codex's own refresh
 * (account/read, its refresh token asked for).
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
 * what the refresh wrote, in the same transaction. One that is not due is left as it is,
 * without the lock.
 *
 * A refresh refused for good, as Codex's app-server says one is (LoginRefresh), its refresh token
 * spent, expired or revoked, reads the store again, and takes the login another alasio kept
 * since, if one did. Where none did, as of a login revoked, or one whose refresh another alasio
 * spent and died before it kept, or of a lone alasio, the login is refused: the store records it
 * so (Store.withCodexLoginLock's refuse), and this alasio remembers its content, so neither it
 * nor another refreshes it again, nor locks for it, until the store keeps another, as `alasio
 * login codex` makes one, or the home's file changes. Meanwhile each fresh fails with
 * CodexLoginRefused, which a Codex turn fails with, saying to log in again; `alasio status` says
 * it too (./login-state.ts).
 *
 * A refresh that failed otherwise, as the issuer was not reached, or answered amiss, or the
 * app-server did not answer, says nothing of the login: it is used as it is, the failure said,
 * and this alasio refreshes it again only REFRESH_RETRY_AFTER_MS later, its access token being
 * good long past that (FRESH_FOR_MS).
 *
 * alasio's app-server refreshes the login it holds in memory, as it loaded it, not the home's
 * file: where the file changed since, the pinned Codex reloads it and asks the issuer nothing
 * that time, and where the file is of another ChatGPT account, it never reloads it, nor
 * refreshes (codex-rs login/src/auth/manager.rs refresh_token, reload_if_account_id_matches),
 * and its protocol has no reload but its own logins. So the app-server is stopped (`reload`), for
 * the next call to start one that loads the home as it is, wherever the home is not the login
 * the app-server last held, as far as alasio knows: as the home takes the store's, and before a
 * refresh where the home changed otherwise, or alasio knows nothing of it yet.
 *
 * Each fresh reads the store's login, a statement of its own: before each Codex turn, each request
 * the relay passes on, and each listing of models, so that a login another alasio kept is taken at
 * once.
 */
import { readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Clock, Context, Duration, Effect, Layer, Schema, Semaphore } from "effect";

import { type CodexLoginRecord, Store } from "../persistence/store.ts";
import { CodexAppServer } from "./app-server/client.ts";
import type { LoginRefresh } from "./app-server/thread-client.ts";
import { buildCodexEnv } from "./env.ts";

/** How long before its access token stops a login is refreshed: past the longest a turn's relay token is good for. */
export const FRESH_FOR_MS = 30 * 60_000;

/**
 * How long after it was last refreshed a login is refreshed, whatever its access token says: a
 * day short of codex-rs's TOKEN_REFRESH_INTERVAL, 8 days, past which Codex refreshes a login on
 * its own as it next calls (login/src/auth/manager.rs).
 */
export const REFRESHED_WITHIN_MS = Duration.toMillis(Duration.days(7));

/** How long after a refresh that failed, but not for good, the login is refreshed again, as the module says: a small part of FRESH_FOR_MS. */
export const REFRESH_RETRY_AFTER_MS = 60_000;

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

/** alasio's Codex login, refused as alasio refreshed it, its refresh token spent or revoked, which Codex runs no turn on until the operator logs in again, as the module says. */
export class CodexLoginRefused extends Schema.TaggedError<CodexLoginRefused>()("CodexLoginRefused", {}) {
  override get message(): string {
    return "Codex's login was refused as alasio refreshed it, so Codex runs no turn until it is logged in again: run `alasio login codex`.";
  }
}

/** What keeps alasio's Codex login fresh, as the module says. */
export class CodexLoginFreshness extends Context.Service<CodexLoginFreshness, {
  /**
   * The home made the store's login where the store kept one since, then refreshed where it is
   * due, as the module says; what fails is said, and what there is used. Failing with
   * CodexLoginRefused where the login is refused.
   */
  readonly fresh: Effect.Effect<void, CodexLoginRefused>;
}>()("alasio/codex/CodexLoginFreshness") {
  /** The login of the Codex home `home`, refreshed by alasio's app-server, stopped to load the home anew. */
  static readonly layer = (home: string): Layer.Layer<CodexLoginFreshness, never, Store | CodexAppServer> =>
    Layer.effect(
      CodexLoginFreshness,
      Effect.flatMap(CodexAppServer, (appServer) => makeCodexLoginFreshness({ home, refresh: appServer.refreshLogin({ env: buildCodexEnv(), cwd: home }), reload: appServer.stop })),
    );
}

/**
 * The freshness of the login of the Codex home `home`, `refresh` Codex's refresh of it, which
 * rewrites its auth.json, and `reload` what has the app-server that refreshes it load the home
 * anew as it next refreshes, as the module says; a refresh that failed but not for good tried
 * again `retryAfterMs` later.
 */
export const makeCodexLoginFreshness = <E extends { readonly message: string }>({ home, refresh, reload, retryAfterMs = REFRESH_RETRY_AFTER_MS }: {
  readonly home: string;
  readonly refresh: Effect.Effect<LoginRefresh, E>;
  readonly reload: Effect.Effect<void>;
  readonly retryAfterMs?: number | undefined;
}) =>
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
    /** The home's login the app-server holds, as far as this alasio knows, as the module says: null where it knows nothing of it. */
    let held: string | null = null;
    /** The app-server stopped, the next to start loading the home as it is now. */
    const reloaded = Effect.andThen(reload, Effect.map(read, (auth) => void (held = auth)));
    /** When the store kept the login the home last took or kept, by the store's own clock; null before it took any. */
    let tookSavedAt: number | null = null;
    /** The home's login whose refresh was refused, as the module says; null where none was. */
    let refusedAuth: string | null = null;
    /**
     * The store's login, `kept`, written to the home where the store kept it since the home last
     * took one, and it differs: the stamp to take it as of, which the caller adopts once what it
     * reads it in is through (tookSavedAt); none where there is nothing to take.
     */
    const take = (kept: CodexLoginRecord | null) =>
      Effect.gen(function*() {
        if (kept === null || kept.savedAt === tookSavedAt) return null;
        if (kept.auth !== (yield* read)) {
          yield* write(kept.auth);
          yield* reloaded;
          yield* Effect.logInfo("took the Codex login the store kept since its home last took one");
        }
        return kept.savedAt;
      });
    /** Whether the home's login, `current`, is refused: as this alasio found it, or another, as the store, holding the same, says. */
    const refusedNow = (kept: CodexLoginRecord | null, current: string | null): boolean =>
      current !== null && (current === refusedAuth || (kept !== null && kept.refusedAt !== null && kept.auth === current));
    /** When this alasio may refresh again after a refresh that failed but not for good, as the module says; 0 where none did. */
    let retryAt = 0;
    /** The refresh failed but not for good, as said: tried again retryAfterMs from now. */
    const unrefreshed = (why: string) =>
      Effect.gen(function*() {
        yield* Effect.logWarning(`the Codex login could not be refreshed, so it is used as it is, and refreshed again in ${retryAfterMs} ms: ${why}`);
        retryAt = (yield* Clock.currentTimeMillis) + retryAfterMs;
      });
    const oneAtATime = yield* Semaphore.make(1);
    const fresh = Effect.gen(function*() {
      const kept = yield* store.getCodexLoginRecord;
      const took = yield* take(kept);
      if (took !== null) tookSavedAt = took;
      const home = yield* read;
      if (refusedNow(kept, home)) return yield* new CodexLoginRefused();
      const now = yield* Clock.currentTimeMillis;
      if (!due(home, now) || now < retryAt) return;
      /** What the lock's holder came to, and the stamp the home took the store's login as of, adopted once its transaction commits. */
      const { outcome, stamp } = yield* store.withCodexLoginLock((login) =>
        Effect.gen(function*() {
          const locked = yield* login.get;
          let stamp = yield* take(locked);
          const current = yield* read;
          if (refusedNow(locked, current)) return { outcome: "refused", stamp } as const;
          if (!due(current, yield* Clock.currentTimeMillis)) return { outcome: "fresh", stamp } as const;
          if (current !== held) yield* reloaded;
          const refreshed = yield* Effect.result(refresh);
          // What it holds now: the home, as it left it, or as it was where the refresh failed.
          held = refreshed._tag === "Failure" ? null : yield* read;
          if (refreshed._tag === "Failure") {
            yield* unrefreshed(refreshed.failure.message);
            return { outcome: "unrefreshed", stamp } as const;
          }
          if (refreshed.success === "refused") {
            yield* Effect.logWarning("the Codex login's refresh token was refused for good as it was refreshed");
            // The login another alasio kept since taken, if one did, and the refused token never offered again.
            const since = yield* login.get;
            const taken = yield* take(since);
            if (taken !== null) return { outcome: "taken", stamp: taken } as const;
            if (since !== null && since.auth === current) yield* login.refuse(since.savedAt);
            refusedAuth = current;
            return { outcome: "refused", stamp } as const;
          }
          const after = yield* read;
          if (after === current) {
            yield* unrefreshed("Codex left its login as it was, its log saying why");
            return { outcome: "unrefreshed", stamp } as const;
          }
          if (after !== null) stamp = yield* login.save(after);
          const stopped = timesOf(current)?.stops;
          yield* Effect.logInfo(`refreshed the Codex login${stopped ? `, whose access token stopped at ${new Date(stopped).toISOString()}` : ""}`);
          return { outcome: "refreshed", stamp } as const;
        })
      );
      if (stamp !== null) tookSavedAt = stamp;
      if (outcome === "refused") return yield* new CodexLoginRefused();
    }).pipe(
      Semaphore.withPermit(oneAtATime),
      Effect.catchTag("StoreError", (error) => Effect.logWarning(`the Codex login could not be kept fresh: ${error.message}`)),
    );
    return CodexLoginFreshness.of({ fresh });
  });
