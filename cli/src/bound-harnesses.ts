/**
 * The conversations' harnesses bound on a cluster (../../src/binder/bindings.ts): each a pod
 * of a harness pool (./manifests/harness-pools.ts), in any environment's namespace, that a
 * binding's Lease names its holder, whose conversation's turn runs in it, and what it holds of
 * the conversation lives in it, until its release. A pod stopped while it is bound is lost
 * with what it held, and its turn is run again on another (src/harness/host-events.ts), so an
 * upgrade that would stop the cluster's pods, restarting k3s or making its nodes anew, waits
 * for them to be released first, unless told to go on now.
 */
import { Clock, Duration, Effect, Schema } from "effect";

import type { BindingLease } from "../../src/binder/bindings.ts";
import { BINDING_LABEL, KEY_ANNOTATION, POOL_LABEL } from "../../src/binder/names.ts";
import { kind, KubeApi, type KubeApiError } from "./kube/api.ts";
import type { WaitOptions } from "./kube/rollout.ts";
import { FOLDER_HARNESS_POOLS, HARNESS_POOLS } from "./manifests/harness-pools.ts";

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
