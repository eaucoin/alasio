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
 */
import { COMPUTE_PORT, type ComputeCredentials } from "./branches.ts";

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
