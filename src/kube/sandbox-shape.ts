/**
 * What alasio adds to a workspace's Sandbox over the template its installation gives it
 * (../kube/config.ts): bayma's token, mounted from a Secret of the Sandbox's, and passed
 * as a file; the labels that name the Sandbox and say what it is; and, of a session's,
 * its internet's gate and DNS (../sandbox/index.ts), and of a folder's bayma, its state
 * directory (../mcp/bayma.ts). In one place, with no dependency, so the admission policy
 * that holds alasio to its templates (cli/src/manifests/admission.ts) knows it as they do.
 */

/** The container in every Sandbox's pod that runs bayma. */
export const BAYMA_CONTAINER = "bayma";

/** Where a Sandbox's token Secret is mounted in its bayma container, its key, and its volume. */
export const TOKEN_DIR = "/run/alasio/bayma";
export const TOKEN_KEY = "token";
export const TOKEN_VOLUME = "alasio-bayma-token";
/** The token volume's files' mode: readable by the pod's group, which is how a non-root bayma reads it. */
export const TOKEN_MODE = 0o440;
/** The file bayma reads its token from, which a restored Sandbox's new token replaces. */
export const TOKEN_FILE = `${TOKEN_DIR}/${TOKEN_KEY}`;

/** The arguments bayma is given after its template's: where its token is. */
export const TOKEN_FILE_ARGS: readonly string[] = ["--token-file", TOKEN_FILE];
/** The arguments a session's bayma is given after those when it exports telemetry: the token it exports with, read from the same file for each export. */
export const TELEMETRY_TOKEN_FILE_ARGS: readonly string[] = ["--telemetry-token-file", TOKEN_FILE];

/** The name of a Sandbox's token Secret. */
export function tokenSecretName(name: string): string {
  return `${name}-bayma-token`;
}

/** The label every object alasio makes carries, and the one that names its Sandbox. */
export const MANAGED_BY = { "app.kubernetes.io/managed-by": "alasio" } as const;
export const SANDBOX_LABEL = "alasio.dev/sandbox";

/** What a Sandbox's pod is to alasio: a session's, or a folder's bayma's. */
export const WORKLOAD_LABEL = "alasio.dev/workload";
/** A session's internet, `none` or `full`, which its NetworkPolicies follow. */
export const NET_MODE_LABEL = "alasio.dev/net-mode";
/** The harness a folder's bayma serves, as a path segment. */
export const HARNESS_LABEL = "alasio.dev/harness";

/** The resolvers a session with internet uses unless its template names others. */
export const DEFAULT_FULL_MODE_NAMESERVERS: readonly string[] = ["1.1.1.1", "8.8.8.8"];

/** The argument a folder's bayma is given its state directory by, under its template's state root. */
export const STATE_DIR_ARG = "--state-dir";

/** A path segment from `value`: never empty, nor starting with a dot. */
export function sanitizePathToken(value: string): string {
  return String(value).replace(/[^A-Za-z0-9_.-]+/g, "-").replace(/^[-.]+|-+$/g, "") || "default";
}

/**
 * What a session's pod template carries of its whole-sandbox snapshots
 * (../sandbox/snapshots.ts): the hash of the template it was made from, which its
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
/**
 * The annotation of a suspended Sandbox naming the snapshot its pod was saved into, which
 * alone it may be restored from: set as it is suspended, removed as it is next resumed,
 * from that snapshot or not, so a snapshot outlives no resume of its Sandbox's.
 */
export const SNAPSHOT_ANNOTATION = "alasio.dev/snapshot";
/**
 * The annotation naming the gVisor release that took the snapshot SNAPSHOT_ANNOTATION
 * names, beside it, the only one that restores it: `alasio up` keeps every release one
 * names (cli/src/kept-releases.ts).
 */
export const SNAPSHOT_RELEASE_ANNOTATION = "alasio.dev/snapshot-release";
/** Where a node agent keeps the snapshots it prepares, as `<environment>/<workspace>/<snapshot>` (../node-agent/cache.ts). */
export const SNAPSHOT_CACHE_DIR = "/var/lib/alasio/snapshots";
/**
 * A snapshot's id, as a node agent names it: when it was taken, to the millisecond, so a
 * workspace's ids sort as its snapshots were taken, then 32 random bits.
 */
export const SNAPSHOT_ID = "[0-9]{8}T[0-9]{9}Z-[0-9a-f]{8}";
/** A DNS label: an environment's name (its sessions' namespace) and a workspace's (its Sandbox's), as the snapshot cache's directories are named. */
export const NAME = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;

/** The init container that holds a session's pod until its egress is confined: its name, and its resources. */
export const EGRESS_GATE = { name: "egress-gate", resources: { requests: { cpu: "10m", memory: "32Mi" }, limits: { memory: "64Mi" } } } as const;

/**
 * The egress gate's program: exits once three connections in a row to the API server
 * fail, and fails the pod if they still succeed after two minutes. Runs on the agent
 * image's Node.
 */
export const EGRESS_GATE_SCRIPT = [
  "const net = require('node:net');",
  "const host = process.env.KUBERNETES_SERVICE_HOST, port = Number(process.env.KUBERNETES_SERVICE_PORT);",
  "const deadline = Date.now() + 120000; let refused = 0;",
  "(function probe() {",
  "  const socket = net.connect({ host, port, timeout: 1000 });",
  "  const next = (blocked) => { socket.destroy(); refused = blocked ? refused + 1 : 0;",
  "    if (refused >= 3) process.exit(0);",
  "    if (Date.now() > deadline) { console.error('egress is not confined: is NetworkPolicy enforced in this cluster?'); process.exit(1); }",
  "    setTimeout(probe, 250); };",
  "  socket.once('connect', () => next(false)); socket.once('timeout', () => next(true)); socket.once('error', () => next(true));",
  "})();",
].join("\n");
