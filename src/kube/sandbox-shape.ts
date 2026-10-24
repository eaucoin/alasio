/**
 * What alasio's pods of bayma carry, beyond their templates, that more than one of its
 * parts knows: the gateway that binds them (../gateway/), the node agents that snapshot and
 * restore them (../node-agent/), and the admission policies that hold them to what alasio
 * up renders (cli/src/manifests/admission.ts). In one place, with no dependency, so the
 * command line takes it as it is.
 */

/** The container of a session's or a folder instance's pod that runs bayma. */
export const BAYMA_CONTAINER = "bayma";

/** A session's internet, `none` or `full`, which its NetworkPolicies follow. */
export const NET_MODE_LABEL = "alasio.dev/net-mode";

/**
 * The argument a session's bayma is given how long it holds a stop by, for alasio to
 * snapshot it first (bayma's mcp-http help): a bayma run without it holds none.
 */
export const STOP_WAIT_ARG = "--stop-wait-ms";

/** How long a session's bayma takes to stop once it holds its stop no more: it persists its sessions. */
export const BAYMA_STOP_SECONDS = 30;

/**
 * The variable bayma's hook, run as its sandbox is restored, takes why its host stopped the
 * sandbox from, as the end of a sentence, to tell each live session's next exec (bayma's
 * sandbox-hook help).
 */
export const SANDBOX_STOP_REASON_VARIABLE = "BAYMA_SANDBOX_STOP_REASON";

/** A path segment from `value`: never empty, nor starting with a dot. */
export function sanitizePathToken(value: string): string {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/**
 * What a session's pod template carries of its whole-sandbox snapshots
 * (../gateway/snapshots.ts): the hash of the template it was made from, which its
 * snapshots record; the CPU features its sandbox is held to; and, of a pod restored from
 * a snapshot, the image it is restored from, on its node under the snapshot cache, left
 * paused until its node's agent resumes it, which its readiness gate waits for.
 */
export const SESSION_TEMPLATE_ANNOTATION = "alasio.dev/session-template";
/** gVisor's annotation that holds a sandbox to the CPU features it names, of those its node has. */
export const CPU_FEATURES_ANNOTATION = "dev.gvisor.internal.cpufeatures";
/** The annotation alasio's fork of gVisor restores a pod's sandbox from, naming the image on the node. */
export const RESTORE_ANNOTATION = "dev.gvisor.internal.restore.host-image-path";
/** The fork's annotation that leaves a restored sandbox paused, running nothing, until it is resumed. */
export const RESTORE_PAUSED_ANNOTATION = "dev.gvisor.internal.restore.paused";
/** The condition of a restored pod's readiness gate, which alasio sets once its sandbox is resumed. */
export const RESUMED_CONDITION = "alasio.dev/resumed";
/** Where a node agent keeps the snapshots it prepares, as `<environment>/<workspace>/<snapshot>` (../node-agent/cache.ts). */
export const SNAPSHOT_CACHE_DIR = "/var/lib/alasio/snapshots";
/**
 * A snapshot's id, as a node agent names it: when it was taken, to the millisecond, so a
 * workspace's ids sort as its snapshots were taken, then 32 random bits.
 */
export const SNAPSHOT_ID = "[0-9]{8}T[0-9]{9}Z-[0-9a-f]{8}";
/** A DNS label: an environment's name (its sessions' namespace) and a workspace's, as the snapshot cache's directories are named. */
export const NAME = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;
