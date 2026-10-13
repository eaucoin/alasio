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

/** What a wake reads of and does to one compute, and to its record (wakeCompute). */
export interface Waking<D extends { readonly replicas: number }> {
  /** The compute's Deployment, or a failure where it is not deployed. */
  readonly deployment: () => Promise<D>;
  /** Records that it was asked to wake, which keeps it awake a while longer. */
  readonly asked: () => Promise<void>;
  /** Runs `scaling` under the compute's lock, which a sleep holds while it stops the compute. */
  readonly locked: <T>(scaling: () => Promise<T>) => Promise<T>;
  /** Waits until no pod of the compute, `deployment` as read, is left. */
  readonly untilGone: (deployment: D) => Promise<void>;
  /** Scales the compute to one replica. */
  readonly scaleUp: () => Promise<void>;
  /** Whether the compute takes connections now; a failure once the wake is out of time. */
  readonly accepts: () => Promise<boolean>;
  /** Waits before the compute is asked again. */
  readonly pause: () => Promise<void>;
}

/**
 * Wakes a compute, as `steps` read it and act: records the wake asked for, and scales a
 * compute that sleeps to one under its lock, once the pod put to sleep is gone, read
 * again under it, as a sleep may have ended meanwhile; then waits until it takes
 * connections. One found running needs no lock, but a sleep may then have scaled it
 * down: one that counted it again before the wake was recorded, its scale to none
 * landing after the compute was read. So while it takes none, the compute is read again,
 * and one found asleep woken likewise. Whether it slept.
 */
export async function wakeCompute<D extends { readonly replicas: number }>(steps: Waking<D>): Promise<boolean> {
  const first = await steps.deployment();
  await steps.asked();
  const scaled = () =>
    steps.locked(async () => {
      const now = await steps.deployment();
      if (now.replicas > 0) return false;
      await steps.untilGone(now);
      await steps.scaleUp();
      return true;
    });
  let slept = first.replicas === 0 && await scaled();
  while (!(await steps.accepts())) {
    if ((await steps.deployment()).replicas === 0) slept = (await scaled()) || slept;
    await steps.pause();
  }
  return slept;
}
