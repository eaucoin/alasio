/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): makes a session as
 * alasio does, through its own session filesystems and its ServiceAccount's permissions,
 * `create <volumeId> <none|full>`. Prints one JSON line of the session made.
 *
 * Its Sandbox is made from what this script, without alasio's telemetry settings, makes
 * of the template, as ./session-bring-up.ts brings sessions up, so either finds the
 * other's sessions on their own template.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { Effect } from "effect";

import { KubeClient } from "../../src/kube/client.ts";
import { loadKubeTemplates } from "../../src/kube/config.ts";
import { type NetMode, SessionSandboxes } from "../../src/sandbox/index.ts";

/** The session made. */
export interface Made {
  readonly volumeId: string;
  readonly netMode: NetMode;
}

const [verb, first, second] = process.argv.slice(2);
if (verb !== "create" || !first || (second !== "none" && second !== "full")) throw new Error("usage: session-volumes.ts create <volumeId> <none|full>");

const made = await Effect.runPromise(Effect.gen(function*() {
  const { sessions } = yield* loadKubeTemplates;
  if (!sessions) return yield* Effect.die(new Error("the installation gives no sessions template"));
  return yield* Effect.gen(function*() {
    const { volumes } = yield* SessionSandboxes;
    const done: Made = yield* volumes.create(first, second);
    return done;
  }).pipe(Effect.provide(SessionSandboxes.layer({ profile: sessions, stateDir: "/tmp/volumes", env: {} })));
}).pipe(Effect.scoped, Effect.provide(KubeClient.layer)));
console.log(JSON.stringify(made));
