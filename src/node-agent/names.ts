/**
 * Where a node keeps its gVisor releases, containerd's runtimes of them and its
 * snapshots: as the clusters alasio makes lay them out (cli/src/cluster/), and as what
 * runs on a node finds them. Free of any import, so the command line takes it as the
 * server does.
 */

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

/** The version runsc of gVisor's `release` reports, and saves its images with: its release's tag. */
export const runscVersion = (release: string): string => `release-${release}`;

/** The node's snapshot cache, where its sandboxes are restored from. */
export const SNAPSHOT_CACHE_DIR = "/var/lib/alasio/snapshots";
