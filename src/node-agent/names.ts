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

/**
 * Where a node agent keeps, on its node's /run, a tmpfs, what outlives it until the node
 * restarts: the Unix domain socket it serves the snapshots bucket on to its node's restores
 * too, which a restore's checkpoint gofer dials by its path, whatever pod the agent runs in
 * (STORE_SOCKET), and those restores' tokens, read back as it starts again
 * (STORE_TOKENS_FILE), each only root's. Of the pods alasio makes, the agent alone mounts the
 * node's /run (cli/src/manifests/node-agent.ts), and the sessions' pods are held to their
 * templates, which mount none of it (cli/src/manifests/admission.ts sandboxPodPolicy). The
 * host profile's mounts, which the operator gives alasio and the folder's bayma, could reach
 * it, but both run as the operator's user (1000 unless the profile says), whom the socket's
 * and the file's mode, 0600 and root's, keep out.
 */
export const NODE_AGENT_RUN_DIR = "/run/alasio/node-agent";
/** The variable that moves NODE_AGENT_RUN_DIR, as a test of the agent's own process does. */
export const NODE_AGENT_RUN_DIR_VARIABLE = "ALASIO_NODE_AGENT_RUN_DIR";
/** The socket a node agent serves the snapshots bucket on, on its node, in `runDir` (NODE_AGENT_RUN_DIR). */
export const storeSocket = (runDir: string): string => `${runDir}/store.sock`;
/** The file a node agent keeps its restores' tokens in, on its node, in `runDir` (NODE_AGENT_RUN_DIR). */
export const storeTokensFile = (runDir: string): string => `${runDir}/tokens.json`;
/** The URL a restore's gofer names the store by through STORE_SOCKET, which it signs: no host's, as the socket alone is dialled. */
export const STORE_SOCKET_ENDPOINT = "http://alasio-node-agent";

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
  return releaseAtOrPast(release, since);
}

/**
 * Whether gVisor's `release` is the fork's `since` or one past it, by its base's date and
 * patch, then its own number; never where either names no release of the fork's.
 */
function releaseAtOrPast(release: string, since: string): boolean {
  const parts = (named: string) => {
    const matched = /^(\d{8})\.(\d+)-eaucoin\.(\d+)$/u.exec(named);
    return matched ? matched.slice(1).map(Number) : null;
  };
  const [of, first] = [parts(release), parts(since)];
  if (of === null || first === null) return false;
  const differs = of.findIndex((part, index) => part !== first[index]);
  return differs === -1 || (of[differs] ?? 0) > (first[differs] ?? 0);
}

/**
 * How a restore's checkpoint gofer reads its images: through a socket or over TCP, and
 * whether it reads again a range of pages a read of failed; as the release that took the
 * snapshot restores best, which the gateway says of each kept release (gofersOf).
 */
export interface Gofer {
  readonly transport: "socket" | "tcp";
  readonly pagesRetry: boolean;
}

/**
 * The gofer a restore of a snapshot taken under gVisor's `release` asks for: through a socket,
 * reading again pages a read of failed, of one at or past `since`, the first release that
 * restores so, as cluster/node/pins.json says it (gvisor.restoresThroughSocketSince); none,
 * the agent's own way, over TCP, of one before it, and of every release where `since` names none.
 */
export const goferOf = (release: string, since: string): Gofer | null => (releaseAtOrPast(release, since) ? { transport: "socket", pagesRetry: true } : null);

/**
 * Whether the shim of gVisor's `release` restores into a standby, a pod booted ahead of its
 * restore (../kube/sandbox-shape.ts STANDBY_ANNOTATION), soundly: one at or past `since`, the
 * first release that does, as cluster/node/pins.json says it (gvisor.restoresIntoStandbySince).
 * One before it runs a standby's containers as it would any pod's, and takes no restore into it;
 * or, as the fork's .3, takes one, but leaves a standby's container it stops with its I/O open,
 * which CRI waits on the end of, to stop it and as containerd starts again; or, as .4, takes one
 * on a socket wherever its annotation names it, as root, without the rules of the directory
 * alasio names it in (../kube/sandbox-shape.ts STANDBY_SOCKETS), which only a shim of .5 on holds to.
 */
export const restoresIntoStandby = (release: string, since: string): boolean => releaseAtOrPast(release, since);

/**
 * The name of a restore's pod, as the gateway makes it (../gateway/names.ts restorePodName): its
 * pool's, `-restore-`, then seven of base 36's digits; the one name whose directory the node
 * agent makes for it, before the pod is made.
 */
export const RESTORE_POD = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?-restore-[0-9a-z]{7}$/u;

/**
 * The variable that gives the node agent the gVisor release the node's runtime starts sandboxes with (its RuntimeClass's,
 * the first pinned), whose runsc it reads the state every shim keeps with: the one its sandboxes keep in memory.
 */
export const GVISOR_RELEASE_VARIABLE = "ALASIO_GVISOR_RELEASE";

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

/**
 * The directory alasio's pods keep on a node, which nothing of k3s's removes: the workspaces'
 * JuiceFS mount and its state and cache, its juicefs, and the pool pods' directories, which
 * workspaces are attached into, mounts all (cli/src/manifests/sandbox-gateway.ts).
 */
export const NODE_DIRECTORY = "/var/lib/alasio";

/**
 * Where a node has workspace storage mounted, held by a pod of its own, each workspace's
 * files at `<environment>/<workspace>/files`.
 */
export const WORKSPACES_ROOT = `${NODE_DIRECTORY}/workspaces`;

/** Where a node keeps the directory of each pod of a sandbox pool, or of a harness pool, `<namespace>/<pod>`, which the pod mounts by `subPathExpr` of its namespace and its name. */
export const ATTACH_ROOT = `${NODE_DIRECTORY}/attach`;

/** The file of a harness pool's pod's binding in its directory, which its node's agent writes as the gateway binds it, and the pod serves once it is there (src/harness/program.ts). */
export const BINDING_FILE = "binding.json";

/**
 * What of a node's memory kubelet keeps from its pods (kube-reserved and system-reserved), and
 * how little it lets be available before it evicts them softly (eviction-soft) and at once
 * (eviction-hard), as alasio's nodes run it (cli/src/cluster/k3s.ts KUBELET_ARGS): what the
 * agent's memory budget leaves aside (./memory.ts), and what its guard keeps sandboxes from
 * (./guard.ts).
 */
export const NODE_MEMORY_RESERVED_BYTES = (1024 + 512) * 1024 ** 2;
export const NODE_MEMORY_SOFT_EVICTION_BYTES = 1536 * 1024 ** 2;
export const NODE_MEMORY_HARD_EVICTION_BYTES = 1024 ** 3;

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

/** The variable holding the token the sandbox gateway presents to a node agent. */
export const NODE_AGENT_TOKEN_VARIABLE = "ALASIO_NODE_AGENT_TOKEN";

/** The variable holding workspace storage's metadata engine's address, its password in it, which the agent sets workspaces' quotas on alone. */
export const META_URL_VARIABLE = "META_URL";

/** The variable holding the most bytes the node's snapshot cache keeps. */
export const SNAPSHOT_CACHE_BYTES_VARIABLE = "ALASIO_SNAPSHOT_CACHE_BYTES";

/**
 * How long a checkpoint, a prepare or a copy may take: a sandbox's memory, through the store,
 * both ways, or within it. alasio waits for one so long, and an agent that stops lets the
 * ones it runs end within it (./operations.ts), after its restores' reads (nodeAgentStopSeconds).
 */
export const OPERATION_LIMIT_SECONDS = 15 * 60;

/**
 * How long bayma's hook may take to ready the sandbox for its save, and to go on after one
 * taken live, as it bounds itself in those (sandbox/agent/start.sh).
 */
export const SAVE_HOOK_SECONDS = 30;

/**
 * What a release's save of a sandbox whose restore from the bucket still reads it may wait on
 * those reads before the agent revokes the restore's token (./agent.ts boundWhileLoading): its
 * reads refused from then, not retried, its loading fails, and so does the save, which the
 * session comes back from bayma's checkpoints after, rather than stay paused for as long as the
 * restore asks of a store that is away, past the OPERATION_LIMIT_SECONDS alasio waits for it.
 * The agent revokes it only once the save has run LOADING_SAVE_FLOOR_SECONDS, from as runsc's
 * save starts, and either no byte of the restore's pages has been served for its stall window
 * (LOADING_STALL_SECONDS), or, at the rate they have been served since the save began, those
 * left would be served only past the latest a revocation can come (LOADING_SAVE_LATEST_SECONDS):
 * a store that answers, however slowly, is never cut while it can serve them in time.
 *
 * LOADING_SAVE_FLOOR_SECONDS is how long the save runs at least before a stall or a rate too
 * slow revokes the token: as long as bayma's hook may take (SAVE_HOOK_SECONDS), which waits on
 * the pages it touches too, twice, so that what the save itself has served is measured over a
 * minute at least.
 */
export const LOADING_SAVE_FLOOR_SECONDS = 60;

/**
 * How long no byte of a restore's pages may be served, as its release's save waits, before the
 * agent takes its reads as stalled, unless told (LOADING_STALL_SECONDS_VARIABLE): 90 s. A restore
 * whose store answers is served bytes as each read streams, its loader keeping reads in flight;
 * one whose store is away tries each read again within 20 s (10 s for its pages), so 90 s is four
 * attempts or more that served nothing, past the minute a store's brief failover takes.
 */
export const LOADING_STALL_SECONDS = 90;

/** The least a stall window may be set to: bayma's hook's own bound (SAVE_HOOK_SECONDS), which a save may wait on first. */
export const LOADING_STALL_LEAST_SECONDS = SAVE_HOOK_SECONDS;

/**
 * How long from as a release's checkpoint is asked its save may wait on its restore's reads at
 * the most: OPERATION_LIMIT_SECONDS less 5 minutes for what follows a revocation (the gofer's
 * next attempt of a read, 20 s at most; the save's end; forgetting the workspace's snapshot, a
 * request to a store away sent again for 2 minutes at most, the last of 60 s at most (./s3.ts
 * RESEND_WITHIN_MS); the answer to alasio), which a stall window may take no more of either.
 */
export const LOADING_SAVE_LATEST_SECONDS = OPERATION_LIMIT_SECONDS - 5 * 60;

/** The most a stall window may be set to, so that one counted from the floor still ends by LOADING_SAVE_LATEST_SECONDS. */
export const LOADING_STALL_MOST_SECONDS = LOADING_SAVE_LATEST_SECONDS - LOADING_SAVE_FLOOR_SECONDS;

/** The variable holding a node agent's stall window, in seconds (LOADING_STALL_SECONDS). */
export const LOADING_STALL_SECONDS_VARIABLE = "ALASIO_LOADING_STALL_SECONDS";

/** The variable saying whether a node agent's checkpoints upload their images in one pass (./agent.ts NodeAgentOptions onePassSaves): `true` or `false`. */
export const ONE_PASS_SAVES_VARIABLE = "ALASIO_ONE_PASS_SAVES";

/**
 * The variable saying whether a node agent's restores from the bucket keep their image's pages metadata in the cache, for the
 * restored sandbox's next checkpoint's delta (./agent.ts NodeAgentOptions deltaAfterColdRestore): `true` or `false`.
 */
export const DELTA_AFTER_COLD_RESTORE_VARIABLE = "ALASIO_DELTA_AFTER_COLD_RESTORE";

/**
 * The variable that gives a node agent, for a test alone, the timings LOADING_SAVE_FLOOR_SECONDS,
 * LOADING_STALL_SECONDS and how often it looks at a restore's reads, as JSON
 * ({"floorMs","stallMs","pollMs"}): shorter than an installation may set them, as the end-to-end
 * run's proof of a revocation does. No installation's manifests set it.
 */
export const LOADING_SAVE_TEST_TIMINGS_VARIABLE = "ALASIO_LOADING_SAVE_TEST_TIMINGS";

/**
 * Whether gVisor's `release` writes a checkpoint's summary as JSON (runsc checkpoint
 * --summary-file), which describes its image as `runsc image inspect` and `runsc image layers`
 * do, the CPU features it was saved with among it, so that the agent runs neither after a
 * checkpoint. One at or past `since`, the first release that does, as cluster/node/pins.json says
 * it (gvisor.savesSummarySince) and the installation gives the node agent it
 * (SAVES_SUMMARY_SINCE_VARIABLE); none where `since` names none.
 */
export const savesSummary = (release: string, since: string): boolean => releaseAtOrPast(release, since);

/** The variable that gives the node agent the first gVisor release that writes a checkpoint's summary (savesSummary). */
export const SAVES_SUMMARY_SINCE_VARIABLE = "ALASIO_GVISOR_SAVES_SUMMARY_SINCE";

/**
 * Whether gVisor's `release` bounds a save's waits for the pages that its sandbox's restore has
 * not loaded yet (runsc checkpoint --load-wait): for those it saves, and bayma's hook's for those
 * it touches, past which runsc kills the hook. Past it, the save fails, what it wrote
 * incomplete, and the sandbox runs on as it was. One at or past `since`, the first release
 * that does, as cluster/node/pins.json says it (gvisor.savesWithLoadWaitSince) and the
 * installation gives the node agent it (SAVES_WITH_LOAD_WAIT_SINCE_VARIABLE); none where `since`
 * names none.
 */
export const savesWithLoadWait = (release: string, since: string): boolean => releaseAtOrPast(release, since);

/** The variable that gives the node agent the first gVisor release that bounds a save's wait for pages still loading (savesWithLoadWait). */
export const SAVES_WITH_LOAD_WAIT_SINCE_VARIABLE = "ALASIO_GVISOR_SAVES_WITH_LOAD_WAIT_SINCE";

/**
 * Whether gVisor's `release` checks a restore's files in the bucket itself: its checkpoint gofer,
 * told each file's ETag (s3 options' `etags`), asks the store for that version of its object on
 * every read, and runsc reads the end of every image's pages file before it loads any, so that a
 * restore whose images were lost or written over fails before its sandbox runs, and the prepare
 * need not look at each first (./agent.ts prepare). One at or past `since`, the first release that
 * does, as cluster/node/pins.json says it (gvisor.restoresWithETagsSince) and the installation
 * gives the node agent it (RESTORES_WITH_ETAGS_SINCE_VARIABLE); none where `since` names none.
 */
export const restoresWithETags = (release: string, since: string): boolean => releaseAtOrPast(release, since);

/** The variable that gives the node agent the first gVisor release that checks a restore's files in the bucket itself (restoresWithETags). */
export const RESTORES_WITH_ETAGS_SINCE_VARIABLE = "ALASIO_GVISOR_RESTORES_WITH_ETAGS_SINCE";

/**
 * Whether gVisor's `release` records a sandbox's working set as a live checkpoint resumes it,
 * where the sandbox holds none (runsc's working-set-after-save "if-none"): the pages it touches
 * first after the save, which a restore of that image then reads first, as it does those touched
 * first after a restore. So a session never restored yet is restored from its images as quickly
 * as one restored before. One at or past `since`, the first release that does, as
 * cluster/node/pins.json says it (gvisor.recordsWorkingSetAfterSaveSince); none where `since`
 * names none.
 */
export const recordsWorkingSetAfterSave = (release: string, since: string): boolean => releaseAtOrPast(release, since);

/**
 * How long a save, of a release that bounds it (savesWithLoadWait), waits for the pages that its
 * sandbox's restore from the bucket has not loaded yet (./agent.ts SaveDeferred): those it saves,
 * and those bayma's hook touches, the hook's own time aside. 5 minutes, well within the
 * OPERATION_LIMIT_SECONDS alasio waits for it, bayma's hook's time and the upload after included.
 * A store that comes back within it is waited for; one away for longer defers the save, its
 * sandbox running on, for the next to take. runsc does not bound writing the save, nor the
 * hook's time, which its timeout bounds (./agent.ts restoreHookSeconds). A release's save is given
 * less where that, the hook's own bound (SAVE_HOOK_SECONDS) and LOAD_WAIT_WATCHDOG_MARGIN_SECONDS
 * would end past the latest it may be bounded as its sandbox loads (LOADING_SAVE_LATEST_SECONDS
 * from its ask, its wait for the workspace's lock counted), and none where nothing is left
 * (./agent.ts loadWaitOf).
 */
export const SAVE_LOAD_WAIT_SECONDS = 5 * 60;

/**
 * How long past its load wait (SAVE_LOAD_WAIT_SECONDS) and bayma's hook's own bound
 * (SAVE_HOOK_SECONDS) a save of a release that bounds its waits may run before the
 * agent bounds it while its sandbox loads all the same (./agent.ts boundWhileLoading), as a
 * watchdog should runsc not defer it: a minute, for its pause and the writing of the pages
 * dirtied since its wait, which runsc does not bound. The load wait is cut so that all of it ends
 * by LOADING_SAVE_LATEST_SECONDS from the save's ask, which bounds it whatever its floor.
 */
export const LOAD_WAIT_WATCHDOG_MARGIN_SECONDS = 60;

/**
 * How long a workspace's release saves may have been deferred, as their waits for its pages
 * still loading expired (./agent.ts SaveDeferred), before its next is bounded as one of a release
 * that does not bound it (boundWhileLoading): an hour. Its restore's token is then revoked once
 * its reads have stalled, its loading fails and so does the save, and the session comes back
 * from bayma's checkpoints, as a session whose store stays away must, its node keeping its pod
 * and memory meanwhile.
 */
export const SAVES_DEFERRED_FOR_SECONDS = 60 * 60;

/**
 * How long an agent holds a request for what its node holds (`GET /holdings`), asked as of
 * what alasio has of it, before it answers that nothing changed: alasio asks again at once, so
 * what it keeps of every node changes as the node's does, and an agent gone is found within it.
 */
export const HOLDINGS_WAIT_MS = 25_000;

/**
 * How long a restore's pod is waited for to run its containers, its images pulled: by the
 * gateway, as it may not start, and by its node's agent, which resumes its sandbox once it runs.
 */
export const RESTORE_RUNNING_WAIT_MS = 180_000;

/**
 * How long a restore from the bucket may take from its prepare to its resume, which its token
 * lasts unless resumed before (./store-proxy.ts): the longest it takes, its pod waited for to
 * run for 3 minutes (RESTORE_RUNNING_WAIT_MS), then the agent's resume waiting for the pod's
 * network policies before it resumes its containers, from which its token lasts as its
 * resumed restore reads, and a margin.
 */
export const RESTORE_PREPARED_FOR_SECONDS = 5 * 60;

/** The variable holding how long, in seconds, a resumed restore from the bucket may read it through the node agent (backgroundReadSeconds). */
export const BACKGROUND_READ_SECONDS_VARIABLE = "ALASIO_BACKGROUND_READ_SECONDS";

/**
 * The slowest a resumed restore is taken to read the rest of its sandbox through the node
 * agent, in the background: half of what was measured on a runner's bundled SeaweedFS,
 * 8.7 MB/s through the agent's proxy from the restore's start to its last read.
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
