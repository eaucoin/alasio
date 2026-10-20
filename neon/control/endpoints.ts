/**
 * neon-control as the control plane of Neon's proxy (its `cplane-v1` backend:
 * proxy/src/control_plane/client/cplane_proxy_v1.rs and messages.rs at Neon's
 * release-9129), what is decided without I/O. Every client of alasio's Neon connects to
 * the proxy, naming the compute it wants by `options=endpoint=<id>` (the compute's id,
 * ./branches.ts's computeId), and the proxy asks:
 *
 * - `GET /get_endpoint_access_control?endpointish&role`: the role's SCRAM verifier, which
 *   it authenticates the client against and then the compute with, so the compute must
 *   store that verifier to the byte (./scram.ts). Main's roles' are the stack's, from its
 *   secrets; a branch's, the verifiers its request gave. Anything else is no endpoint, or
 *   no role, and refused.
 * - `GET /wake_compute?endpointish`: the compute's address, once it accepts connections,
 *   woken first if it sleeps (./service.ts).
 *
 * A compute sleeps when no client has had a session on it for its `suspendAfter`, which
 * neon-control counts on the compute itself: so a session holding an advisory lock, idle,
 * keeps it awake, as compute_ctl's own activity monitor would not, and so does one opened
 * and closed between two counts, and a wake asked for since, which a connection, or
 * alasio's command line, is about to use.
 */
import { COMPUTE_PORT, type ComputeCredentials } from "./branches.ts";

/** The annotation of a compute's Deployment saying how many seconds it may be idle before it sleeps (./computes.ts). */
export const SUSPEND_AFTER = "alasio.dev/suspend-after-seconds";

/** What the proxy asked for: the endpoint, and the role whose secret it wants, if any. */
export interface ProxyQuery {
  endpoint: string;
  role: string | null;
}

/** The query of a proxy's call, at `url`, or null when it names no endpoint. */
export function proxyQuery(url: string): ProxyQuery | null {
  const { searchParams } = new URL(url, "http://neon-control");
  const endpoint = searchParams.get("endpointish");
  return endpoint ? { endpoint, role: searchParams.get("role") } : null;
}

/**
 * The verifier `role` authenticates with on a branch's compute, given its `credentials`:
 * of main's, which has none of its own, `mainVerifiers`' (the stack's); of any other, its
 * own; null for a role neither has, as neon-control's own role, which no client connects
 * as.
 */
export function roleSecret(credentials: ComputeCredentials | null, role: string, mainVerifiers: Readonly<Record<string, string>>): string | null {
  const verifiers: Readonly<Record<string, string>> = credentials?.verifiers ?? mainVerifiers;
  return Object.hasOwn(verifiers, role) ? verifiers[role] ?? null : null;
}

/**
 * Neon's error, as the proxy reads one (ControlPlaneErrorMessage): `reason` decides
 * whether it asks again, ENDPOINT_IDLE among those it does, after the given delay.
 */
export function controlPlaneError(reason: string, message: string, retryDelayMs: number | null = null): object {
  return {
    error: message,
    status: {
      code: reason,
      message,
      details: {
        error_info: { reason },
        retry_info: retryDelayMs === null ? null : { retry_delay_ms: retryDelayMs },
        user_facing_message: { message },
      },
    },
  };
}

/** Where a compute is, as the proxy connects to it: its Service in `namespace`, which takes connections before the pod is ready. */
export const computeAddress = (service: string, namespace: string): string => `${service}.${namespace}.svc.cluster.local:${COMPUTE_PORT}`;

/** What `wake_compute` answers: the compute of the branch `branch`'s address, and labels for the proxy's metrics. */
export function woken(address: string, branch: string, endpoint: string, cold: boolean): object {
  return {
    address,
    aux: { endpoint_id: endpoint, project_id: "alasio", branch_id: branch, compute_id: endpoint, cold_start_info: cold ? "pool_miss" : "warm" },
  };
}

/** What a compute's clients are doing, as counted on it (./computes.ts). */
export interface Activity {
  /** Their sessions open now. */
  sessions: number;
  /** The sessions ever established to its databases, since it started: one that opened and closed between two counts moves it. */
  established: number;
}

/** A compute's activity, and when it was last asked to wake (./record.ts), in milliseconds since the epoch, or null for never. */
export interface Use extends Activity {
  wokenAt: number | null;
}

/** A compute's idleness, as last counted: since when it has had no client (null while it has), and its sessions established, and its last wake, then. */
export interface Idleness {
  since: number | null;
  established: number;
  wokenAt: number | null;
}

/**
 * A compute's idleness, given what it was (none before the first count), its `use`
 * counted now, and the time: used while a session is open, or one was established, or a
 * wake asked for, since the last count; idle from the first count that finds none of
 * them.
 */
export function idleness(previous: Idleness | undefined, use: Use, now: number): Idleness {
  const used = use.sessions > 0 || (previous !== undefined && (use.established !== previous.established || use.wokenAt !== previous.wokenAt));
  return { since: used ? null : previous?.since ?? now, established: use.established, wokenAt: use.wokenAt };
}

/** Whether a compute idle as `idle` says has been idle for `suspendAfterMs` by `now`. */
export const suspendDue = ({ since }: Idleness, now: number, suspendAfterMs: number): boolean => since !== null && now - since >= suspendAfterMs;

/**
 * The label of a compute's pod that its Service selects it by, which its Deployment's
 * selector does not name: a sleep takes it off the pod before it stops it, so connections
 * stop reaching the pod before it stops, and the proxy, finding the address it keeps of
 * the compute refusing, asks for a wake (suspendCompute).
 */
export const SERVING_LABEL = "alasio.dev/serving";

/** What a wake reads of and does to one compute, and to its record (wakeCompute). */
export interface Waking<D extends { readonly replicas: number }> {
  /** The compute's Deployment, or a failure where it is not deployed. */
  readonly deployment: () => Promise<D>;
  /** Records that it was asked to wake, which keeps it awake a while longer. */
  readonly asked: () => Promise<void>;
  /** Whether a sleep has begun to stop it (suspendCompute's mark), and not ended; not where that cannot be read, as no sleep can then begin. */
  readonly stopping: () => Promise<boolean>;
  /** Runs `scaling` under the compute's lock, which a sleep holds while it stops the compute. */
  readonly locked: <T>(scaling: () => Promise<T>) => Promise<T>;
  /** Waits until no pod of the compute, `deployment` as read, is left. */
  readonly untilGone: (deployment: D) => Promise<void>;
  /** Ends a stop a sleep began and never finished, neon-control stopped in it: the compute's pods, if any, its Service's again, and the mark cleared. */
  readonly resume: () => Promise<void>;
  /** Scales the compute to one replica. */
  readonly scaleUp: () => Promise<void>;
  /** Whether the compute takes connections now; a failure once the wake is out of time. */
  readonly accepts: () => Promise<boolean>;
  /** Waits before the compute is asked again. */
  readonly pause: () => Promise<void>;
}

/**
 * Wakes a compute, as `steps` read it and act: records the wake asked for, and wakes a
 * compute that sleeps, or that a sleep is stopping, under its lock, which that sleep holds
 * until its pod is gone: read again under it, as a sleep may have ended meanwhile, or been
 * given up, it is scaled to one once the pod put to sleep is gone; then waits until it
 * takes connections. A stop found begun but no longer under way, its sleep's neon-control
 * stopped in it, is ended (Waking's resume). One found running, and not stopping, after
 * the wake was recorded needs no lock: a sleep marks the compute stopping before it counts
 * it again (suspendCompute), so one that has not, by then, counts this wake, and lets it
 * be. While it takes no connection, the compute is read again, and one found asleep or
 * stopping woken likewise. Whether it slept.
 */
export async function wakeCompute<D extends { readonly replicas: number }>(steps: Waking<D>): Promise<boolean> {
  const first = await steps.deployment();
  await steps.asked();
  const sleeps = async (deployment: D) => deployment.replicas === 0 || await steps.stopping();
  const scaled = () =>
    steps.locked(async () => {
      const now = await steps.deployment();
      if (now.replicas > 0) {
        if (await steps.stopping()) await steps.resume();
        return false;
      }
      await steps.untilGone(now);
      if (await steps.stopping()) await steps.resume();
      await steps.scaleUp();
      return true;
    });
  let slept = (await sleeps(first)) && await scaled();
  while (!(await steps.accepts())) {
    if (await sleeps(await steps.deployment())) slept = (await scaled()) || slept;
    await steps.pause();
  }
  return slept;
}

/** What taking a compute's pods off its Service reads of it and does (withdrawCompute). */
export interface Withdrawing<Pod> {
  /** Its pods that run, or may, none being deleted already. */
  readonly pods: () => Promise<readonly Pod[]>;
  /** Takes `pod` off its Service (SERVING_LABEL), where its Service selects it. */
  readonly takeOff: (pod: Pod) => Promise<void>;
  /** Whether its Service's EndpointSlices list any of `pods` still. */
  readonly listed: (pods: readonly Pod[]) => Promise<boolean>;
  /** Whether a connection to its Service's address, as Neon's proxy connects to it (computeAddress), is let through still, not refused. */
  readonly reached: () => Promise<boolean>;
  /** Waits before it is asked again; a failure, saying `still` is so, once out of time. */
  readonly pause: (still: string) => Promise<void>;
}

/**
 * Takes a compute's pods off its Service, as `steps` act, and waits until its Service
 * reaches none of them: until its EndpointSlices list none, and then until a connection to
 * its address is refused, as kube-proxy writes the rules that route one from those slices a
 * moment later (its sync, a second at least). Those pods. Only the rules of the node that
 * connection is made from are seen so: on a cluster of several nodes, kube-proxy on Neon's
 * proxy's may write them later, and a connection the proxy makes there in that moment, after
 * the sleep has counted the compute again, is let through to its pod, and cut with it. Nor
 * is a client's connection still authenticating as the sleep counts the compute again seen
 * by the count, as Postgres lists a session only once it has authenticated, milliseconds
 * after its connection. Where the cluster's network drops a connection to a Service that
 * routes to no pod rather than refuse it, none is ever refused, and the sleep, out of time,
 * gives up: the compute never sleeps, rather than cut a client.
 */
export async function withdrawCompute<Pod>(steps: Withdrawing<Pod>): Promise<readonly Pod[]> {
  const pods = await steps.pods();
  for (const pod of pods) await steps.takeOff(pod);
  while (await steps.listed(pods)) await steps.pause("its EndpointSlices list its pods taken off it");
  while (await steps.reached()) await steps.pause("a connection to it is let through to its pods taken off it");
  return pods;
}

/** What a sleep does to one compute, under its lock, once it has counted it idle long enough (suspendCompute). */
export interface Suspending<Pods> {
  /** Records the compute stopping, which a wake reads (Waking's stopping). */
  readonly mark: () => Promise<void>;
  /** Takes the compute's pods off its Service, and waits until it reaches none of them (withdrawCompute): those pods. */
  readonly withdraw: () => Promise<Pods>;
  /** Whether a client has used the compute since it was counted idle, counted again on `pods`, as its Service no longer reaches them. */
  readonly used: (pods: Pods) => Promise<boolean>;
  /** Gives the compute's pods back to its Service, and clears the mark: the sleep given up. */
  readonly restore: () => Promise<void>;
  /** Scales the compute to none. */
  readonly scaleDown: () => Promise<void>;
  /** Waits until no pod of the compute is left. */
  readonly untilGone: () => Promise<void>;
  /** Clears the mark. */
  readonly unmark: () => Promise<void>;
}

/**
 * Puts a compute to sleep, as `steps` act: marks it stopping, takes its pods off its
 * Service, counts it again on them, and, still idle, scales it to none and waits until its
 * pod is gone. Marked first, so a wake recorded before the count is counted, and the
 * sleep given up, and one recorded after finds the mark, and waits for the sleep
 * (wakeCompute). Taken off its Service before the count, so a connection made after it is
 * refused, not let through to a pod about to stop, wherever kube-proxy routes as on
 * neon-control's node (withdrawCompute): Neon's proxy, refused at the address it keeps of
 * the compute, forgets it and asks for a wake, which waits for the sleep and wakes the
 * compute. A sleep given up, or failing before it scales, gives the pods back,
 * as one used gives them back; one failing after leaves the mark, which the next wake
 * ends. Whether it slept.
 */
export async function suspendCompute<Pods>(steps: Suspending<Pods>): Promise<boolean> {
  await steps.mark();
  try {
    if (await steps.used(await steps.withdraw())) {
      await steps.restore();
      return false;
    }
  } catch (error) {
    // Its own failure is the one to tell; one restoring is the next wake's to end.
    await steps.restore().catch(() => {});
    throw error;
  }
  await steps.scaleDown();
  await steps.untilGone();
  await steps.unmark();
  return true;
}

/** The bytes from LSN `from` to LSN `to` (each `<high>/<low>` in hexadecimal, as Postgres writes one), 0 where `to` is not past `from`. */
export function lsnDistance(from: string, to: string): number {
  const bytes = (lsn: string) => {
    const [high = "0", low = "0"] = lsn.split("/");
    return (BigInt(`0x${high}`) << 32n) + BigInt(`0x${low}`);
  };
  const distance = bytes(to) - bytes(from);
  return distance > 0n ? Number(distance) : 0;
}
