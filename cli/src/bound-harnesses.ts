/**
 * The conversations' harnesses bound on a cluster (../../src/binder/bindings.ts): each a pod
 * of a harness pool (./manifests/harness-pools.ts), in any environment's namespace, that a
 * binding's Lease names its holder, whose conversation's turn runs in it, and what it holds of
 * the conversation lives in it, until its release. A pod stopped while it is bound is lost
 * with what it held, and its turn is run again on another (src/harness/host-events.ts), so an
 * upgrade that would stop the cluster's pods, restarting k3s or making its nodes anew, waits
 * for them to be released first, unless told to go on now.
 *
 * `alasio status` says each environment's bound harnesses as the sandbox gateway lists them to
 * that environment's caller (src/gateway/harnesses.ts), each host asked as it lists them.
 */
import { Clock, Duration, Effect, Schema } from "effect";

import type { BindingLease } from "../../src/binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, POOL_LABEL } from "../../src/binder/names.ts";
import { CLAUDE_HARNESS, CODEX_HARNESS, harnessDisplayName } from "../../src/harness/names.ts";
import { ago } from "./folders.ts";
import { callAs, GatewayUnreached } from "./gateway-calls.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import type { WaitOptions } from "./kube/rollout.ts";
import { FOLDER_HARNESS_POOLS, HARNESS_POOLS } from "./manifests/harness-pools.ts";

/** A bound harness, as the gateway lists it (ListedHarness). */
const ListedHarness = Schema.Struct({
  conversation: Schema.String,
  harness: Schema.String,
  pool: Schema.String,
  pod: Schema.String,
  boundAt: Schema.NullOr(Schema.String),
  host: Schema.Union([
    Schema.Struct({ running: Schema.Boolean, background: Schema.Boolean, idleSince: Schema.NullOr(Schema.String) }),
    Schema.Struct({ unreached: Schema.String }),
  ]),
});
export type ListedHarness = typeof ListedHarness.Type;
const Listed = Schema.fromJsonString(Schema.Struct({ harnesses: Schema.Array(ListedHarness) }));

/** An environment's bound harnesses, as the gateway lists them; or why they could not be read. */
export type HarnessListing = { readonly listed: readonly ListedHarness[] } | { readonly unread: string };

/** The bound harnesses of `environment`, main or a branch, as the gateway lists them to its caller. */
export const harnessesOf = (environment: string): Effect.Effect<HarnessListing, never, KubeApi> =>
  callAs(environment, "harnesses", "GET", "/harnesses").pipe(
    Effect.flatMap(({ status, text }) => (status === 200 ? Effect.succeed(text) : Effect.fail(new GatewayUnreached({ reason: `it answered ${status}: ${text}` })))),
    Effect.flatMap((text) => Schema.decodeUnknownEffect(Listed)(text)),
    Effect.map(({ harnesses }): HarnessListing => ({ listed: harnesses })),
    Effect.catch((error) => Effect.succeed({ unread: error.message })),
  );

/**
 * What `alasio status` says of `environment`'s bound harnesses `listing`, at `now`, a line
 * each: its conversation and harness, the pod and pool it is bound in, for how long, and, as
 * its host said, a turn running, work in the background, or idle since when; or why its host
 * was not reached. Pure, for tests.
 */
export function describeHarnesses(environment: string, listing: HarnessListing, now: number): string[] {
  if ("unread" in listing) return [`${environment}'s bound harnesses: not seen, as the sandbox gateway did not list them: ${listing.unread}`];
  if (listing.listed.length === 0) return [`${environment}'s bound harnesses: none`];
  return [`${environment}'s bound harnesses:`, ...listing.listed.map(({ conversation, harness, pool, pod, boundAt, host }) => {
    const of = `  ${conversation} under ${harnessDisplayName(harness === CLAUDE_HARNESS || harness === CODEX_HARNESS ? harness : null)}`;
    const bound = `bound to ${pod} of ${pool}${boundAt === null ? "" : ` for ${ago(boundAt, now)}`}`;
    const doing = "unreached" in host
      ? `its host not reached: ${host.unreached}`
      : host.running
      ? "running a turn"
      : host.background
      ? "working in the background"
      : host.idleSince === null
      ? "idle"
      : `idle for ${ago(host.idleSince, now)}`;
    return `${of}: ${bound}, ${doing}`;
  })];
}

/** Every harness pool's name, of session conversations and of folder ones. */
const HARNESS_POOL_NAMES: ReadonlySet<string> = new Set([...Object.values(HARNESS_POOLS), ...Object.values(FOLDER_HARNESS_POOLS)]);

/** The harnesses bound on the cluster, each as its pod, `namespace/pod`, and the conversation it is bound to. */
export const boundHarnesses: Effect.Effect<readonly string[], KubeApiError, KubeApi> = Effect.flatMap(KubeApi, (kube) => kube.list<BindingLease>(kind("Lease"), { labelSelector: BINDING_LABEL })).pipe(
  Effect.map((leases) =>
    leases
      .filter(({ metadata, spec }) => HARNESS_POOL_NAMES.has(metadata?.labels?.[POOL_LABEL] ?? "") && spec?.holderIdentity)
      .map(({ metadata, spec }) => `${metadata?.namespace ?? ""}/${spec?.holderIdentity ?? ""} (${metadata?.annotations?.[KEY_ANNOTATION] ?? "a conversation"})`)
      .sort()
  ),
);

/** Harnesses stayed bound for as long as an upgrade waited for their release, before it would stop their pods. */
export class HarnessesBound extends Schema.TaggedError<HarnessesBound>()("HarnessesBound", {
  harnesses: Schema.Array(Schema.String),
  waited: Schema.String,
}) {
  override get message(): string {
    const count = this.harnesses.length;
    return `${count} conversation harness${count === 1 ? " stayed" : "es stayed"} bound for ${this.waited}, which restarting the cluster would stop, ` +
      `ending the turn in each: ${this.harnesses.join(", ")}. Upgrade again once they are released, as each is once its conversation ` +
      "has been quiet for harness.pools.releaseAfter, or with --now to go on at once, each turn ended then run again on a harness of its own.";
  }
}

/** Waits for no harness to be bound on the cluster, looking every `poll`, and saying once what it waits for: for `timeout` at most. */
export const untilHarnessesReleased = ({ timeout, poll }: WaitOptions): Effect.Effect<void, HarnessesBound | KubeApiError, KubeApi> =>
  Effect.gen(function*() {
    const limit = Duration.fromInputUnsafe(timeout);
    const startedAt = yield* Clock.currentTimeMillis;
    let said = false;
    for (;;) {
      const bound = yield* boundHarnesses;
      if (bound.length === 0) return;
      if ((yield* Clock.currentTimeMillis) - startedAt >= Duration.toMillis(limit)) return yield* new HarnessesBound({ harnesses: bound, waited: Duration.format(limit) });
      if (!said) {
        yield* Effect.logInfo(`waiting for ${bound.length} conversation harness${bound.length === 1 ? "" : "es"} to be released before the cluster restarts: ${bound.join(", ")}`);
        said = true;
      }
      yield* Effect.sleep(poll);
    }
  });
