/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): makes a session as
 * alasio does, through its own session filesystems, through the sandbox gateway,
 * `create <volumeId> <none|full>`, forks one, `fork <sourceVolumeId> <volumeId>`, or deletes
 * one, its binding of the internet it is told let go first, `destroy <volumeId> <none|full>`.
 * Prints one JSON line of the session made, or deleted.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in
 * harness.ts rewrites to the image's as it pipes this in.
 */
import { Effect } from "effect";

import { loadAlasioConfig } from "../../src/config.ts";
import { makeSessionSandboxes, type NetMode } from "../../src/sandbox/index.ts";

/** The session made, or deleted. */
export interface Made {
  readonly volumeId: string;
  readonly netMode: NetMode;
}

const [verb, first, second] = process.argv.slice(2);
const mode = (named: string | undefined): NetMode | null => (named === "none" || named === "full" ? named : null);
if (!first || !second || !(((verb === "create" || verb === "destroy") && mode(second)) || verb === "fork")) {
  throw new Error("usage: session-volumes.ts create <volumeId> <none|full> | fork <sourceVolumeId> <volumeId> | destroy <volumeId> <none|full>");
}

const { sessionsGateway: gateway } = loadAlasioConfig();
if (!gateway) throw new Error("the installation gives alasio no sandbox gateway");
// The internet alasio records of each session: the one it is told of here, none of a fork's source unless told.
const sessions = makeSessionSandboxes({ gateway, stateDir: "/tmp/volumes", netModeOf: () => Effect.succeed(mode(second)) });

const made = await Effect.runPromise(Effect.gen(function*() {
  if (verb === "create") return yield* sessions.volumes.create(first, mode(second) ?? "none");
  if (verb === "fork") return yield* sessions.volumes.fork(first, second);
  yield* sessions.volumes.destroy(first);
  const done: Made = { volumeId: first, netMode: mode(second) ?? "none" };
  return done;
}));
console.log(JSON.stringify(made));
