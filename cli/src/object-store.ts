/**
 * What `alasio status` says of the bundled object store: how many copies of each object it
 * keeps, and of each volume server, how near its disk is to its taking no more writes, as
 * the volume server's own metrics say. A volume server takes none once its disk has less
 * free than its minFreeSpace; once none takes any, nor does the store, as each of its
 * filers' writes is logged to a volume. And the snapshot of its volumes a cut-over took before
 * an upgrade, while it is kept. Pure, for tests; ./commands/status.ts reads the cluster.
 */
import { memoryText } from "./capacity.ts";

/** A volume server's disk and volumes, as its metrics say them, in bytes. */
export interface VolumeServerDisk {
  /** The disk's size, what is free of it, and how much of that it may still fill before it takes no more writes. */
  readonly size: number;
  readonly free: number;
  readonly room: number;
  /** Its volumes, and the most it keeps. */
  readonly volumes: number;
  readonly maxVolumes: number;
  /** What its volumes take on the disk, and of that, what is deleted and not yet compacted away. */
  readonly kept: number;
  readonly deleted: number;
}

/** A sample of Prometheus's text format: its metric's name, labels and value. */
interface Sample {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

/** The samples of a scrape's text, `text`. */
function samples(text: string): Sample[] {
  return text.split("\n").flatMap((line) => {
    const match = /^([A-Za-z_:][A-Za-z0-9_:]*)(?:\{(.*)\})? (\S+)/u.exec(line);
    if (!match) return [];
    const labels = Object.fromEntries([...(match[2] ?? "").matchAll(/([A-Za-z_][A-Za-z0-9_]*)="((?:[^"\\]|\\.)*)"/gu)].map(([, key = "", value = ""]) => [key, value]));
    return [{ name: match[1] ?? "", labels, value: Number(match[3]) }];
  });
}

/**
 * A volume server's disk, from its metrics' text, `text`: SeaweedFS's
 * `SeaweedFS_volumeServer_resource` of its one directory (`all`, `free`, and `avail`, the
 * free less its minFreeSpace), its volumes' counts, and their sizes on the disk of every
 * collection, deleted bytes apart. Null where they are not all there.
 */
export function volumeServerDisk(text: string): VolumeServerDisk | null {
  const scraped = samples(text);
  const sum = (name: string, labels: Readonly<Record<string, string>> = {}) => {
    const matching = scraped.filter((sample) => sample.name === name && Object.entries(labels).every(([key, value]) => sample.labels[key] === value));
    return matching.length > 0 ? matching.reduce((total, { value }) => total + value, 0) : null;
  };
  const resource = "SeaweedFS_volumeServer_resource";
  const disk = {
    size: sum(resource, { type: "all" }),
    free: sum(resource, { type: "free" }),
    room: sum(resource, { type: "avail" }),
    volumes: sum("SeaweedFS_volumeServer_volumes", { type: "volume" }) ?? 0,
    maxVolumes: sum("SeaweedFS_volumeServer_max_volumes"),
    kept: sum("SeaweedFS_volumeServer_total_disk_size", { type: "normal" }) ?? 0,
    deleted: sum("SeaweedFS_volumeServer_total_disk_size", { type: "deleted_bytes" }) ?? 0,
  };
  const { size, free, room, maxVolumes } = disk;
  return size === null || free === null || room === null || maxVolumes === null ? null : { ...disk, size, free, room, maxVolumes };
}

/**
 * Whether a master's metrics' text, `text`, says it holds volume growth frozen
 * (`SeaweedFS_master_volume_growth_frozen`, from SeaweedFS 4.49): as a master does that finds
 * the volume servers holding a topology other than its own, its state lost or made anew, so
 * that it would give new volumes ids those servers hold already. The store writes on to the
 * volumes it has until they fill, and makes no others. Null where the metric is not there.
 */
export function growthFrozen(text: string): boolean | null {
  const frozen = samples(text).find(({ name }) => name === "SeaweedFS_master_volume_growth_frozen");
  return frozen === undefined ? null : frozen.value > 0;
}

/**
 * What `status` says of the store whose masters `frozen` names, each by its pod, holding volume
 * growth frozen: that it is, and how to resume it once the store is seen whole, in `weed
 * shell` as `shell` runs it; nothing where none does.
 */
export function describeGrowthFrozen(frozen: readonly string[], shell: string): string[] {
  if (frozen.length === 0) return [];
  return [
    `its volume growth: frozen by ${frozen.join(", ")}, which found the volume servers holding another master state than its own, lost or made anew: ` +
    `the store writes on to the volumes it has until they fill, and makes none; once its volume servers are all seen, and its masters' state is the one they hold, ` +
    `resume it in weed shell (${shell}) with volume.growth.resume`,
  ];
}

/**
 * The ConfigMap of alasio's namespace by which a cut-over (upgrades/from-<release>.sh) names
 * the snapshot it took of the store's volumes before an upgrade, on this machine; deleted with
 * the snapshot, by the deletion the snapshot carries.
 */
export const STORE_SNAPSHOT_CONFIGMAP = "alasio-object-store-snapshot";

/**
 * A snapshot of the store's volumes, as its ConfigMap says it: where it is, when it was taken
 * and from when it may be deleted, the floor of its disk's room its deletion may be sooner
 * under, what it copied, the data files of the volumes it links, each shared with the store
 * until the store compacts or deletes its volume, its own from then, and since when the store's
 * compaction is paused, null where it is said paused on none.
 */
export interface StoreSnapshot {
  readonly path: string;
  readonly taken: string;
  readonly keepUntil: string;
  readonly floorBytes: number;
  readonly copiedBytes: number;
  readonly volumes: readonly string[];
  readonly vacuumPausedAt: string | null;
}

/** The snapshot the ConfigMap's `data` says, or null where it says none whole. */
export function storeSnapshotOf(data: Readonly<Record<string, string>> | undefined): StoreSnapshot | null {
  const { path, taken, keepUntil, floorBytes, copiedBytes, volumes = "", vacuumPausedAt } = data ?? {};
  const count = (text: string | undefined) => (text !== undefined && /^\d+$/u.test(text) ? Number(text) : null);
  const floor = count(floorBytes);
  const copied = count(copiedBytes);
  if (!path || !taken || !keepUntil || floor === null || copied === null) return null;
  return { path, taken, keepUntil, floorBytes: floor, copiedBytes: copied, volumes: volumes.split("\n").filter(Boolean), vacuumPausedAt: vacuumPausedAt || null };
}

/**
 * How much of the disk `snapshot` holds of its own: what it copied, and each data file it links
 * that the store no longer does, as `linked` says each one's links and blocks, the store's
 * compaction having renamed a new file over it, or its deletion unlinked it.
 */
export function storeSnapshotHolds(snapshot: StoreSnapshot, linked: readonly { readonly nlink: number; readonly blocks: number }[]): number {
  return linked.reduce((total, { nlink, blocks }) => total + (nlink === 1 ? blocks * 512 : 0), snapshot.copiedBytes);
}

/** A master of the store, by its pod, and when its master started, as its pod says, null where it does not run. */
export interface StoreMaster {
  readonly name: string;
  readonly startedAt: string | null;
}

/**
 * What `status` says of `snapshot`, as of `now`: where it is and when it was taken, what of the
 * disk it holds of its own as `holds` says, or why that is not known; whether the store's
 * compaction is paused, which a master of `masters` started since its pause forgets, its pause
 * held in its memory alone, and how to pause it again, telling its ConfigMap so; from
 * when it may be deleted, by a deletion run by hand, and sooner by its override; and what its
 * restore puts back, the store alone, Neon and workspace storage left to their own recovery.
 */
export function describeStoreSnapshot(snapshot: StoreSnapshot, holds: number | { readonly unread: string }, masters: readonly StoreMaster[], now: Date): string[] {
  const { path, taken, keepUntil, floorBytes, vacuumPausedAt } = snapshot;
  const held = typeof holds === "number"
    ? `holding ${memoryText(holds)} of the disk of its own`
    : `what it holds of the disk not read: ${holds.unread}`;
  const restarted = vacuumPausedAt === null ? [] : masters.filter(({ startedAt }) => startedAt === null || Date.parse(startedAt) > Date.parse(vacuumPausedAt));
  const pauseAgain = `pause it again with sudo ${path}/pause.sh, which tells this status so`;
  const vacuum = vacuumPausedAt === null
    ? `its compaction not said paused on SeaweedFS 4.54, so the snapshot may grow as the store compacts the volumes it shares: ${pauseAgain}`
    : restarted.length > 0
    ? `its compaction running again, paused at ${vacuumPausedAt} and forgotten by ${restarted.map(({ name, startedAt }) => (startedAt === null ? `${name}, which does not run` : `${name}, started at ${startedAt}`)).join(", ")}: the snapshot grows as the store compacts the volumes it shares; ${pauseAgain}`
    : `its compaction paused since ${vacuumPausedAt}, so the snapshot grows only as the store deletes what it shares`;
  const deletion = now.getTime() < Date.parse(keepUntil)
    ? `it may be deleted from ${keepUntil}, by sudo ${path}/delete.sh, run by hand; before, by sudo ${path}/delete.sh --under-floor, only while its disk has less than ${memoryText(floorBytes)} free and every part of the store is ready`
    : `it may be deleted, since ${keepUntil}: sudo ${path}/delete.sh, run by hand, deletes it`;
  return [
    `its snapshot, taken ${taken} before its upgrade to SeaweedFS 4.54, at ${path}: ${held}`,
    `  ${vacuum}`,
    `  ${deletion}, which resumes the compaction`,
    `  sudo ${path}/restore.sh --disaster-recovery puts back the object store alone, as it was before its upgrade, its parts stopped: Neon's safekeepers and pageservers, and workspace storage's metadata, are ahead of it then, so it stops Neon's, whose databases are recovered from their newest dump, and workspace storage's metadata from its newest JuiceFS dump, before either runs again`,
  ];
}

/** The copies of each object a replication, `000` or `00N`, keeps, as said. */
export function describeReplication(replication: string): string {
  const copies = Number(replication.slice(2)) + 1;
  const times = copies === 1 ? "once" : copies === 2 ? "twice" : `${copies} times`;
  return `the object store: each object kept ${times} (replication ${replication})`;
}

/** The volume server `name`'s disk, as `status` says it under the object store. */
export function describeVolumeServer(name: string, disk: VolumeServerDisk): string {
  const { size, free, room, volumes, maxVolumes, kept, deleted } = disk;
  return `${name}: ${memoryText(room)} to fill before it takes no more writes, of ${memoryText(free)} free on its disk of ${memoryText(size)}, ` +
    `${memoryText(free - room)} kept free (minFreeSpace); ${volumes} of ${maxVolumes} volumes, taking ${memoryText(kept)}, ${memoryText(deleted)} of it deleted`;
}
