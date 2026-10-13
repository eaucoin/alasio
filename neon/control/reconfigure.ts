/**
 * A running compute given the shards' new layout (./service.ts's /notify-attach), through
 * compute_ctl and Kubernetes, which `Reconfiguring` reaches, so what is done with each is
 * decided here alone, and tested.
 *
 * compute_ctl takes a spec while its compute runs, and answers 412 otherwise: while it
 * starts on the spec it fetched (`init`), applies another (`configuration_pending`,
 * `configuration`), has failed, or stops. Of these, one starting or failed is restarted,
 * on the spec as it is now: a compute that reads a shard where it no longer is never comes
 * to run (in CI, after the whole stack restarted, one stayed in `init` until its waiting
 * backends filled max_connections). Any other is left alone, as it may hold its clients'
 * sessions, and asked again with the next notification. One whose compute_ctl cannot be
 * reached, as no container of it runs (pulling its image, or crashing and started again),
 * fetches its spec, with the layout as recorded then, when its container next starts.
 */
import { sameLayout, type ShardLayout } from "./branches.ts";

/** compute_ctl's statuses of a compute that never takes a spec given now (libs/compute_api/src/responses.rs at Neon's release-compute-9073). */
const RESTARTED_IN = ["init", "failed"];

/** What a compute is reconfigured through: its Deployment, compute_ctl, and the record. */
export interface Reconfiguring {
  /** Whether its Deployment is awake, scaled to one. */
  awake(): Promise<boolean>;
  /** Whether a container of it runs compute_ctl (./computes.ts's computeRuns). */
  running(): Promise<boolean>;
  /** compute_ctl's answer to its spec with `layout` (./computes.ts's configureCompute): 200, 412, or 0 where not reached. */
  configure(layout: ShardLayout): Promise<number>;
  /** How compute_ctl says the compute is (./computes.ts's computeCtlStatus), or null. */
  status(): Promise<string | null>;
  /** Restarts it, on the spec as it is now. */
  restart(): Promise<void>;
  /** Records it given `layout`. */
  gave(layout: ShardLayout): Promise<void>;
}

/**
 * What came of it: `given` the layout; `asleep`, as it fetches it when woken; `starting`,
 * as no container of it runs, which fetches it when one starts; `restarted`; `waiting`,
 * as compute_ctl answered 412 while it configures or stops (its status, null where it did
 * not say); `unreached`, where compute_ctl answered otherwise, its container running (0
 * where not at all); or `failed`, with what failed.
 */
export type Reconfigured =
  | { readonly outcome: "given" | "asleep" | "starting" | "restarted" }
  | { readonly outcome: "waiting"; readonly status: string | null }
  | { readonly outcome: "unreached"; readonly answered: number }
  | { readonly outcome: "failed"; readonly error: string };

/** Whether the compute has the layout it was given, or fetches it at its next start. */
export const tookLayout = ({ outcome }: Reconfigured): boolean => outcome === "given" || outcome === "asleep" || outcome === "starting";

/** Gives the compute `layout`, through `through`, as the module says; never fails. */
export async function reconfigure(layout: ShardLayout, through: Reconfiguring): Promise<Reconfigured> {
  try {
    if (!await through.awake()) return { outcome: "asleep" };
    const answered = await through.configure(layout);
    if (answered === 200) {
      await through.gave(layout);
      return { outcome: "given" };
    }
    if (answered !== 412) return await through.running() ? { outcome: "unreached", answered } : { outcome: "starting" };
    const status = await through.status();
    if (status === null || !RESTARTED_IN.includes(status)) return { outcome: "waiting", status };
    await through.restart();
    return { outcome: "restarted" };
  } catch (error) {
    return { outcome: "failed", error: error instanceof Error ? error.message : String(error) };
  }
}

/**
 * The layout a compute's spec gives it, `read` as recorded, and recorded given it by
 * `gave`: read again once recorded, and given again until it reads the same, so a new
 * layout recorded meanwhile, which a notification's reconfiguration may have read this
 * compute's row before, is never left unrecorded as what it runs on.
 */
export async function settledLayout(read: () => Promise<ShardLayout>, gave: (layout: ShardLayout) => Promise<void>): Promise<ShardLayout> {
  let layout = await read();
  for (;;) {
    await gave(layout);
    const now = await read();
    if (sameLayout(now, layout)) return layout;
    layout = now;
  }
}
