/**
 * Where a node keeps its gVisor releases, containerd's runtimes of them and its
 * snapshots, and where its node agent serves: as the agent reads them, the clusters alasio
 * makes lay them out (cli/src/cluster/), and the installation's manifests give them to
 * the agent (cli/src/manifests/node-agent.ts). Free of any import but sandbox-shape's,
 * itself free of any, so the command line takes it without the agent.
 */

/** The port a node agent serves its API on, on its pod's address. */
export const NODE_AGENT_PORT = 8484;

/**
 * Where a node keeps the gVisor releases alasio keeps, each in a directory of its own
 * named after it, as its release archive has them: runsc, its shim, and gvisor-bin/, the
 * binaries runsc runs beside it.
 */
export const GVISOR_RELEASES = "/usr/local/lib/alasio/gvisor";

/** The directory of gVisor's `release` on a node. */
export const gvisorDirectory = (release: string): string => `${GVISOR_RELEASES}/${release}`;

/** Where k3s's containerd reads its template, and runsc its options. */
export const CONTAINERD = "/var/lib/rancher/k3s/agent/etc/containerd";

/** gVisor's `release` as a DNS label, which containerd's runtimes and Kubernetes' RuntimeClasses are named with: its dots made dashes. */
export const gvisorLabel = (release: string): string => release.replaceAll(".", "-");

/**
 * The containerd runtime of gVisor's `release`, which a RuntimeClass names as its
 * handler, and whose runsc options are `<CONTAINERD>/<runtime>.toml`.
 */
export const gvisorHandler = (release: string): string => `runsc-${gvisorLabel(release)}`;

/** The RuntimeClass of gVisor's `release` alone, which a sandbox snapshotted under it is restored with. */
export const gvisorRuntimeClass = (release: string): string => `gvisor-${gvisorLabel(release)}`;

/** The version runsc of gVisor's `release` reports, and saves its images with: its release's tag. */
export const runscVersion = (release: string): string => `release-${release}`;

/** The root the gVisor shims run runsc with for Kubernetes' containers, under the node's /run. */
export const RUNSC_ROOT = "/run/containerd/runsc/k8s.io";

/** The node's snapshot cache, where its sandboxes are restored from, as the admission policy holds a restore to. */
export { SNAPSHOT_CACHE_DIR } from "../kube/sandbox-shape.ts";

/**
 * The node agent's container that says, as its pod starts, the CPU features its node has
 * under the current gVisor release (./cpu-features.ts), as its termination message, which
 * Kubernetes keeps up to 4 KiB of: a CpuFeaturesReport, as JSON.
 */
export const CPU_FEATURES_CONTAINER = "cpu-features";

/** What a node agent's CPU_FEATURES_CONTAINER says: the release, and the features runsc of it says the node has. */
export interface CpuFeaturesReport {
  readonly release: string;
  readonly cpuFeatures: readonly string[];
}

/** The variable holding the token alasio presents to a node agent. */
export const NODE_AGENT_TOKEN_VARIABLE = "ALASIO_NODE_AGENT_TOKEN";

/** The variable holding the most bytes the node's snapshot cache keeps. */
export const SNAPSHOT_CACHE_BYTES_VARIABLE = "ALASIO_SNAPSHOT_CACHE_BYTES";
