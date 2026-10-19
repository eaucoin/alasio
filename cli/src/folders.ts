/**
 * The folder conversations' instances, as the command line sees and restarts them, through
 * the sandbox gateway (src/gateway/folders.ts): what `alasio status` says of each, and
 * `alasio folder restart`, which lets a conversation's instances go, saved whole, so its next
 * message makes them anew. Each asked by main's token, signed by main's own key.
 */
import type { V1Secret } from "@kubernetes/client-node";
import { Effect, Schema } from "effect";

import { CLAUDE_HARNESS, CODEX_HARNESS, harnessDisplayName } from "../../src/harness/names.ts";
import { folderKey } from "../../src/gateway/names.ts";
import { MAIN_ENVIRONMENT_NAME } from "../../src/branch/names.ts";
import { gatewayCalls, type GatewayCall, GatewayUnreached } from "./gateway-calls.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import { NAMESPACE, SESSIONS_CALLER_SECRET } from "./manifests/common.ts";

/** A folder instance, as the gateway lists it. */
const FolderInstance = Schema.Struct({
  key: Schema.String,
  harness: Schema.String,
  conversation: Schema.String,
  pod: Schema.NullOr(Schema.String),
  boundAt: Schema.NullOr(Schema.String),
  lastExecAt: Schema.NullOr(Schema.String),
  failing: Schema.NullOr(Schema.Struct({ reason: Schema.String, at: Schema.String, times: Schema.Number, held: Schema.Boolean })),
});
export type FolderInstance = typeof FolderInstance.Type;
const Listed = Schema.Struct({ folders: Schema.Array(FolderInstance) });

/** Calls the gateway as main, for `key`: what it answered. */
const asMain = (key: string, method: GatewayCall["method"], path: string) =>
  Effect.gen(function*() {
    const kube = yield* KubeApi;
    const secret = yield* kube.get<V1Secret>({ ...kind("Secret"), namespace: NAMESPACE, name: SESSIONS_CALLER_SECRET });
    const callerKey = secret?.data?.["caller-key"];
    if (!callerKey) return yield* new GatewayUnreached({ reason: `${SESSIONS_CALLER_SECRET} holds no caller key of main's` });
    const call = yield* gatewayCalls;
    return yield* call({ callerKey: Buffer.from(callerKey, "base64"), environment: MAIN_ENVIRONMENT_NAME, key, method, path });
  }).pipe(Effect.scoped);

/** The folder instances, bound or whose last bind failed, as the gateway lists them; or why they could not be read. */
export type FolderListing = { readonly listed: readonly FolderInstance[] } | { readonly unread: string };
export const folderInstances: Effect.Effect<FolderListing, never, KubeApi> = asMain("folders", "GET", "/folders").pipe(
  Effect.flatMap(({ status, text }) => (status === 200 ? Effect.succeed(text) : Effect.fail(new GatewayUnreached({ reason: `it answered ${status}: ${text}` })))),
  Effect.flatMap((text) => Schema.decodeUnknownEffect(Schema.fromJsonString(Listed))(text)),
  Effect.map(({ folders }): FolderListing => ({ listed: folders })),
  Effect.catch((error) => Effect.succeed({ unread: error.message })),
);

/** How long ago `at` was, of `now`, as people say it: in seconds, minutes, hours, or days. */
function ago(at: string, now: number): string {
  const seconds = Math.max(0, Math.round((now - Date.parse(at)) / 1000));
  if (seconds < 120) return `${seconds}s`;
  const minutes = Math.round(seconds / 60);
  return minutes < 120 ? `${minutes}m` : minutes < 48 * 60 ? `${Math.round(minutes / 60)}h` : `${Math.round(minutes / (24 * 60))}d`;
}

/**
 * What `alasio status` says of the folder instances `listed`, at `now`, a line each: its
 * conversation and harness; bound, to which pod, for how long, and when its bayma last ran an
 * exec, or released; and its binds' last failure, how many in a row, and whether it is held.
 * Pure, for tests.
 */
export function describeFolders(listing: FolderListing, now: number): string[] {
  if ("unread" in listing) return [`folder instances: not seen, as the sandbox gateway did not list them: ${listing.unread}`];
  if (listing.listed.length === 0) return ["folder instances: none bound"];
  return ["folder instances:", ...listing.listed.map(({ conversation, harness, pod, boundAt, lastExecAt, failing }) => {
    const of = `  ${conversation} under ${harnessDisplayName(harness === CLAUDE_HARNESS || harness === CODEX_HARNESS ? harness : null)}`;
    const bound = pod === null ? "released" : `bound to ${pod}${boundAt === null ? "" : ` for ${ago(boundAt, now)}`}, ${lastExecAt === null ? "running an exec, or none yet" : `its last exec ${ago(lastExecAt, now)} ago`}`;
    const failed = failing === null
      ? ""
      : `; its last bind failed ${ago(failing.at, now)} ago, ${failing.times} in a row${failing.held ? ", so it is bound again only as the operator asks (Retry now, or alasio folder restart)" : ""}: ${failing.reason}`;
    return `${of}: ${bound}${failed}`;
  })];
}

/**
 * Lets the folder instances of `conversation` go, each harness's, saved whole as each stops,
 * so the next message makes them anew: what to say of each.
 */
export const restartFolder = (conversation: string): Effect.Effect<string[], GatewayUnreached | KubeApiError, KubeApi> =>
  Effect.forEach([CLAUDE_HARNESS, CODEX_HARNESS] as const, (harness) =>
    Effect.map(asMain(folderKey(harness, conversation), "POST", "/release"), ({ status, text }) =>
      status === 204
        ? `${harnessDisplayName(harness)}: its folder instance of ${conversation} is let go, its REPL sessions saved, its failed binds forgotten; the next message makes it anew`
        : status === 409
        ? `${harnessDisplayName(harness)}: its failed binds of ${conversation} are forgotten, but it is being started or stopped now, so it was not let go: ask again in a moment`
        : `${harnessDisplayName(harness)}: the sandbox gateway did not let it go (${status}): ${text}`));
