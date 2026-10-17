/**
 * Where a node keeps its gVisor releases, containerd's runtimes of them and its
 * snapshots, where its node agent serves, and the memory the agent needs: as the agent
 * reads them, the clusters alasio makes lay them out (cli/src/cluster/), and the
 * installation's manifests give them to the agent (cli/src/manifests/node-agent.ts). Free
 * of any import but sandbox-shape's, itself free of any, so the command line takes it
 * without the agent.
 */

/** The port a node agent serves its API on, on its pod's address. */
export const NODE_AGENT_PORT = 8484;

/**
 * The port a node agent serves the snapshots bucket on, to its node's restores, on its
 * pod's address (../node-agent/store-proxy.ts): no network policy admits it, so the node
 * alone reaches it, as a node reaches the pods it runs.
 */
export const STORE_PROXY_PORT = 8485;

/** The variable that gives a node agent its pod's address, which it serves the bucket to its node on. */
export const POD_IP_VARIABLE = "POD_IP";

/**
 * Where a node keeps the gVisor releases alasio keeps, each in a directory of its own
 * named after it, as its release archive has them: runsc, its shim, and gvisor-bin/, the
 * binaries runsc runs beside it.
 */
export const GVISOR_RELEASES = "/usr/local/lib/alasio/gvisor";

/** The directory of gVisor's `release` on a node. */
export const gvisorDirectory = (release: string): string => `${GVISOR_RELEASES}/${release}`;

/**
 * Whether gVisor's `release` restores a sandbox straight from the bucket, its images read
 * through the checkpoint gofer as the restore goes: one of the fork's at or past `since`,
 * the first release that does, as cluster/node/pins.json says it (restoresFromBucketSince)
 * and the installation gives the node agent it (RESTORES_FROM_BUCKET_SINCE_VARIABLE), by
 * its base's date and patch, then its own number. One before it, as the fork's
 * 20260921.0-eaucoin.6, which ends the sandbox as it tries (`urpc method
 * "containerManager.Restore" failed: EOF`), is fetched into the node's cache first and
 * restored from there; so is one that names no release of the fork's, and every release
 * where `since` names none.
 */
export function restoresFromBucket(release: string, since: string): boolean {
  const parts = (named: string) => {
    const matched = /^(\d{8})\.(\d+)-eaucoin\.(\d+)$/u.exec(named);
    return matched ? matched.slice(1).map(Number) : null;
  };
  const [of, first] = [parts(release), parts(since)];
  if (of === null || first === null) return false;
  const differs = of.findIndex((part, index) => part !== first[index]);
  return differs === -1 || (of[differs] ?? 0) > (first[differs] ?? 0);
}

/** The variable that gives the node agent the first gVisor release that restores straight from the bucket (restoresFromBucket). */
export const RESTORES_FROM_BUCKET_SINCE_VARIABLE = "ALASIO_GVISOR_RESTORES_FROM_BUCKET_SINCE";

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

/** A part of a snapshot file the node agent puts or gets (./s3.ts): 32 MiB, as large as S3's 10000 parts make an object of 312 GiB. */
export const SNAPSHOT_PART_BYTES = 32 * 1024 ** 2;

/** How many parts of a snapshot file the node agent puts or gets at a time, and how many gets, of every snapshot it gets. */
export const SNAPSHOT_PARTS_AT_ONCE = 4;

/** What a get of the node agent's writes to its file between syncs to the disk: 8 MiB. */
export const SNAPSHOT_SYNC_BYTES = 8 * 1024 ** 2;

/**
 * The node agent's memory budget for what it gets: what it holds written and not yet on
 * the disk, at most, of every get at a time together (./s3.ts). The kernel counts it in the
 * agent's memory and cannot reclaim it before it is written, so the agent's memory limit
 * leaves room for it beside the agent's own; what it reads through the page cache, the
 * kernel reclaims.
 */
export const UNSYNCED_DOWNLOAD_BYTES = SNAPSHOT_PARTS_AT_ONCE * SNAPSHOT_SYNC_BYTES;

/**
 * The node agent's own memory: what it was measured to hold restoring a 1 GiB session's
 * snapshot from the bucket on a node of its own (test/e2e, 125 to 145 MiB anonymous),
 * rounded up.
 */
export const NODE_AGENT_OWN_BYTES = 160 * 1024 ** 2;

/** The least memory limit a node agent runs under: its own, and a download's budget beside it. */
export const NODE_AGENT_MEMORY_FLOOR = NODE_AGENT_OWN_BYTES + UNSYNCED_DOWNLOAD_BYTES;

/** The variable holding the token alasio presents to a node agent. */
export const NODE_AGENT_TOKEN_VARIABLE = "ALASIO_NODE_AGENT_TOKEN";

/** The variable holding the most bytes the node's snapshot cache keeps. */
export const SNAPSHOT_CACHE_BYTES_VARIABLE = "ALASIO_SNAPSHOT_CACHE_BYTES";

/**
 * How long a checkpoint, a prepare or a copy may take: a sandbox's memory, through the store,
 * both ways, or within it. alasio waits for one so long, and an agent that stops lets the
 * ones it runs end within it (./operations.ts), after its restores' reads (nodeAgentStopSeconds).
 */
export const OPERATION_LIMIT_SECONDS = 15 * 60;

/**
 * How long a restore from the bucket may take from its prepare to its resume, which its token
 * lasts unless resumed before (./store-proxy.ts): the longest it takes, alasio waiting 3
 * minutes for its pod to run (POD_RUNNING_TIMEOUT in src/sandbox/snapshots.ts), then for the
 * agent's resume, and a margin.
 */
export const RESTORE_PREPARED_FOR_SECONDS = 5 * 60;

/** The variable holding how long, in seconds, a resumed restore from the bucket may read it through the node agent (backgroundReadSeconds). */
export const BACKGROUND_READ_SECONDS_VARIABLE = "ALASIO_BACKGROUND_READ_SECONDS";

/**
 * The slowest a resumed restore is taken to read the rest of its sandbox through the node
 * agent, in the background: half of what gate 4 measured on a runner's bundled SeaweedFS,
 * 8.7 MB/s through the agent's proxy from the restore's start to its last read
 * (program/progress/live-lazy.md).
 */
export const BACKGROUND_READ_BYTES_PER_SECOND = 4 * 1024 ** 2;

/**
 * How long a resumed restore of a sandbox of at most `memoryBytes` may read the rest of it
 * through the node agent, at the slowest (BACKGROUND_READ_BYTES_PER_SECOND), in seconds: a
 * minute at least.
 */
export function backgroundReadSeconds(memoryBytes: number): number {
  return Math.max(60, Math.ceil(memoryBytes / BACKGROUND_READ_BYTES_PER_SECOND));
}

/**
 * How long a node agent that stops may take, its sandboxes of at most `memoryBytes`: the
 * restores that read the bucket through it done first, the last within its token's time (one
 * prepared just before the stop taking as long as a restore takes to its resume, then as long
 * as it takes to read its sandbox whole), then the checkpoints, prepares and copies it runs
 * (OPERATION_LIMIT_SECONDS).
 */
export function nodeAgentStopSeconds(memoryBytes: number): number {
  return RESTORE_PREPARED_FOR_SECONDS + backgroundReadSeconds(memoryBytes) + OPERATION_LIMIT_SECONDS;
}
