/**
 * A harness pool's pod's Codex login, as its turns give it: none of its own. alasio alone
 * holds the installation's Codex login, and refreshes it, as a ChatGPT refresh token is
 * single-use (../codex/login-refresh.ts); it relays it to its pools' pods
 * (../codex/login-relay.ts), and gives each Codex turn it starts in one a token for it, good for
 * minutes, which it renews as the turn runs (TurnStart.codex, HarnessHost.renewCodexLogin). The
 * pod relays on its own loopback to alasio's, with the latest token given, as it reaches its
 * bayma by the latest bearer its turns gave (./given-bayma.ts), and its Codex calls the model
 * through that alone: its session conversations' Codex as its home's one model provider, its
 * folder conversations' by what is given over the operator's configuration
 * (loginRelayOverrides), which each turn writes anew into the pod's own Codex home, never
 * their login.
 *
 * Pooled Codex turns so need alasio awake, which they always have, as alasio drives each turn
 * it starts: a request made before any turn gave a token, or of one that has stopped, as one a
 * few minutes past the turn that gave it, is refused, as the model would refuse it.
 */
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join, normalize, sep } from "node:path";

import { Effect, type Scope } from "effect";

import { type LoginRelay, LoginRelayError, startLoginRelay } from "../codex/login-relay.ts";
import type { TurnCodex, TurnStart } from "./host.ts";

/** A pod's Codex login, as its turns give it; see the module. */
export interface GivenCodexLogin {
  /** The pod's loopback relay, which its Codex is given as its model provider. */
  readonly relay: LoginRelay;
  /** Takes what the turn `start` gives of Codex: the loopback pointed at alasio's relay with its token, and, of a folder conversation's, the operator's configuration written into `home` anew. */
  readonly give: (start: TurnStart) => Effect.Effect<void, LoginRelayError>;
  /** Takes a fresh token for alasio's relay, as a turn alasio drives runs on past the last's time (HarnessHost.renewCodexLogin). */
  readonly renew: (login: TurnCodex["login"]) => Effect.Effect<void>;
}

/** Where a file of the operator's Codex configuration goes under `home`, its path one under it alone; null for one that would leave it. */
const under = (home: string, path: string): string | null => {
  const at = normalize(join(home, path));
  return at.startsWith(`${normalize(home)}${sep}`) && !path.split("/").includes("auth.json") ? at : null;
};

/** A pod's Codex login, as the module says, its folder conversations' Codex home `home`, until the scope closes. */
export const makeGivenCodexLogin = Effect.fnUntraced(function*({ home }: { readonly home: string }): Effect.fn.Return<GivenCodexLogin, LoginRelayError, Scope.Scope> {
  /** Where the latest turn said alasio's relay is, and its token for it. */
  let given: { readonly url: URL; readonly token: string } | null = null;
  /** What of the operator's configuration the latest turn wrote, by its path under `home`. */
  let written: readonly string[] = [];
  const relay = yield* startLoginRelay({
    authFile: "alasio's, through its relay",
    upstreamFor: (_authFile, path) => (given === null ? null : { origin: given.url.origin, path, headers: { authorization: `Bearer ${given.token}` } }),
  });
  const renew = (login: TurnCodex["login"]) => Effect.sync(() => void (given = { url: new URL(login.url), token: login.token }));
  return {
    relay,
    renew,
    give: (start) =>
      Effect.gen(function*() {
        if (start.codex === null) return;
        yield* renew(start.codex.login);
        const files = start.codex.home;
        if (files === null) return;
        const paths = Object.keys(files).flatMap((path) => (under(home, path) === null ? [] : [path]));
        yield* Effect.tryPromise({
          try: async () => {
            // What the operator removed since the last turn removed here too.
            for (const path of written.filter((each) => !paths.includes(each))) await rm(under(home, path) ?? "", { force: true });
            for (const path of paths) {
              const at = under(home, path) ?? "";
              await mkdir(dirname(at), { recursive: true });
              await writeFile(at, files[path] ?? "");
            }
          },
          catch: (cause) => new LoginRelayError({ cause }),
        });
        written = paths;
      }),
  };
});
