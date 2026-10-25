/**
 * Run inside alasio's pod, by node there (inAlasio in harness.ts): asks the sandbox gateway as
 * alasio's own client does (src/sandbox/gateway-client.ts), `bind <volumeId> <none|full>` or
 * `release <volumeId> <none|full>`. Prints one JSON line of what it came to: whether it did, what
 * failed where it did not, and in how long.
 *
 * alasio's modules are imported by their paths in the repository, which inAlasio in harness.ts
 * rewrites to the image's as it pipes this in.
 */
import { Effect } from "effect";

import { loadAlasioConfig } from "../../src/config.ts";
import { makeGatewayWorkspaces } from "../../src/sandbox/gateway-client.ts";

/** What a call of alasio's client came to. */
export interface Controlled {
  readonly ok: boolean;
  readonly error: string | null;
  readonly ms: number;
}

const [verb, volumeId, mode] = process.argv.slice(2);
if ((verb !== "bind" && verb !== "release") || !volumeId || (mode !== "none" && mode !== "full")) throw new Error("usage: gateway-control.ts <bind|release> <volumeId> <none|full>");
const { sessionsGateway: gateway } = loadAlasioConfig();
if (!gateway) throw new Error("the installation gives alasio no sandbox gateway");
const workspaces = makeGatewayWorkspaces(gateway);
const started = Date.now();
const done = await Effect.runPromise(Effect.result(verb === "bind" ? workspaces.bind(volumeId, mode) : workspaces.release(volumeId, mode)));
const controlled: Controlled = { ok: done._tag === "Success", error: done._tag === "Failure" ? done.failure.message : null, ms: Date.now() - started };
console.log(JSON.stringify(controlled));
