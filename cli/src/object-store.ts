/**
 * What `alasio status` says of the bundled object store: how many copies of each object it
 * keeps, and of each volume server, how near its disk is to its taking no more writes, as
 * the volume server's own metrics say. A volume server takes none once its disk has less
 * free than its minFreeSpace; once none takes any, nor does the store, as each of its
 * filers' writes is logged to a volume. Pure, for tests; ./commands/status.ts reads the
 * cluster.
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
