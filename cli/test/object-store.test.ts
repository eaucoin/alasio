import assert from "node:assert/strict";
import { test } from "node:test";

import { describeGrowthFrozen, describeReplication, describeStoreSnapshot, growthFrozen, storeSnapshotHolds, storeSnapshotOf, volumeServerDisk } from "../src/object-store.ts";

test("says the copies a replication keeps of each object", () => {
  assert.equal(describeReplication("000"), "the object store: each object kept once (replication 000)");
  assert.equal(describeReplication("001"), "the object store: each object kept twice (replication 001)");
  assert.equal(describeReplication("002"), "the object store: each object kept 3 times (replication 002)");
});

test("reads a volume server's disk from its metrics, and nothing from metrics without it", () => {
  const disk = volumeServerDisk([
    'SeaweedFS_volumeServer_resource{name="/data",type="all"} 1000',
    'SeaweedFS_volumeServer_resource{name="/data",type="free"} 300',
    'SeaweedFS_volumeServer_resource{name="/data",type="avail"} 0',
    "SeaweedFS_volumeServer_max_volumes 30",
  ].join("\n"));
  // A disk with less free than its minFreeSpace: nothing to fill; no volumes yet.
  assert.deepEqual(disk, { size: 1000, free: 300, room: 0, volumes: 0, maxVolumes: 30, kept: 0, deleted: 0 });
  assert.equal(volumeServerDisk('SeaweedFS_volumeServer_resource{name="/data",type="all"} 1000\n'), null);
});

test("reads from a master's metrics whether it holds volume growth frozen, and says how to resume it where one does", () => {
  assert.equal(growthFrozen("# HELP SeaweedFS_master_volume_growth_frozen 1 while volume growth is frozen\nSeaweedFS_master_volume_growth_frozen 1\n"), true);
  assert.equal(growthFrozen("SeaweedFS_master_volume_growth_frozen 0\n"), false);
  // A master of a SeaweedFS before 4.49 says nothing of it.
  assert.equal(growthFrozen("SeaweedFS_master_leader_changes 1\n"), null);
  assert.deepEqual(describeGrowthFrozen([], "weed shell"), []);
  const [said] = describeGrowthFrozen(["alasio-seaweedfs-master-0"], "kubectl exec ... weed shell");
  assert.match(said ?? "", /^its volume growth: frozen by alasio-seaweedfs-master-0, [^]* resume it in weed shell \(kubectl exec \.\.\. weed shell\) with volume\.growth\.resume$/u);
});

test("reads the snapshot a cut-over took of the store's volumes from its ConfigMap, what of the disk it holds of its own, and whether its compaction is paused, and says how to delete it, when, and what its restore puts back", () => {
  const data = { path: "/srv/cut-over/object-store", taken: "2026-11-26T19:28:22Z", keepUntil: "2026-11-28T19:28:22Z", floorBytes: String(60 * 2 ** 30), copiedBytes: "1048576", volumes: "/srv/a.dat\n/srv/b.dat\n" };
  const unpaused = storeSnapshotOf(data);
  assert.deepEqual(unpaused, { path: data.path, taken: data.taken, keepUntil: data.keepUntil, floorBytes: 60 * 2 ** 30, copiedBytes: 1_048_576, volumes: ["/srv/a.dat", "/srv/b.dat"], vacuumPausedAt: null });
  // A ConfigMap missing what it names, or counting no bytes, names none.
  assert.equal(storeSnapshotOf(undefined), null);
  assert.equal(storeSnapshotOf({ ...data, keepUntil: "" }), null);
  assert.equal(storeSnapshotOf({ ...data, copiedBytes: "many" }), null);
  assert.equal(storeSnapshotOf({ ...data, floorBytes: "" }), null);
  const snapshot = storeSnapshotOf({ ...data, vacuumPausedAt: "2026-11-26T19:40:00Z" });
  if (unpaused === null || snapshot === null) return assert.fail("no snapshot read");
  // What it copied, and each data file the store no longer links: one compacted away, of 2 GiB on the disk; one shared still.
  assert.equal(storeSnapshotHolds(snapshot, [{ nlink: 1, blocks: 4 * 1024 ** 2 }, { nlink: 2, blocks: 8 }]), 1_048_576 + 2 * 1024 ** 3);
  const held = 1_048_576 + 2 * 1024 ** 3;
  const masters = [{ name: "alasio-seaweedfs-master-0", startedAt: "2026-11-26T19:35:00Z" }];
  const [first, vacuum, deletion, restore, ...more] = describeStoreSnapshot(snapshot, held, masters, new Date("2026-11-27T00:00:00Z"));
  assert.deepEqual(more, []);
  assert.equal(first, "its snapshot, taken 2026-11-26T19:28:22Z before its upgrade to SeaweedFS 4.54, at /srv/cut-over/object-store: holding 2.0 GiB of the disk of its own");
  assert.equal(vacuum, "  its compaction paused since 2026-11-26T19:40:00Z, so the snapshot grows only as the store deletes what it shares");
  // Deleted by hand alone, and before its time only by the override, and when.
  assert.equal(deletion, "  it may be deleted from 2026-11-28T19:28:22Z, by sudo /srv/cut-over/object-store/delete.sh, run by hand; before, by sudo /srv/cut-over/object-store/delete.sh --under-floor, only while its disk has less than 60.0 GiB free and every part of the store is ready, which resumes the compaction");
  assert.match(restore ?? "", /^ {2}sudo \/srv\/cut-over\/object-store\/restore\.sh --disaster-recovery puts back the object store alone, as it was before its upgrade, its parts stopped: Neon's safekeepers and pageservers, and workspace storage's metadata, are ahead of it then, so it stops Neon's, whose databases are recovered from their newest dump, and workspace storage's metadata from its newest JuiceFS dump, before either runs again$/u);
  // A master started since the pause, or not running, has forgotten it: said, and how to pause it again.
  const [, forgotten] = describeStoreSnapshot(snapshot, held, [{ name: "alasio-seaweedfs-master-0", startedAt: "2026-11-26T22:00:00Z" }, { name: "alasio-seaweedfs-master-1", startedAt: null }], new Date("2026-11-27T00:00:00Z"));
  assert.equal(forgotten, `  its compaction running again, paused at 2026-11-26T19:40:00Z and forgotten by alasio-seaweedfs-master-0, started at 2026-11-26T22:00:00Z, alasio-seaweedfs-master-1, which does not run: the snapshot grows as the store compacts the volumes it shares; pause it again with sudo /srv/cut-over/object-store/pause.sh, which tells this status so`);
  const [, never] = describeStoreSnapshot(unpaused, held, masters, new Date("2026-11-27T00:00:00Z"));
  assert.match(never ?? "", /^ {2}its compaction not said paused on SeaweedFS 4\.54, so the snapshot may grow as the store compacts the volumes it shares: pause it again with sudo \/srv\/cut-over\/object-store\/pause\.sh, which tells this status so$/u);
  // Past its time, and its size unread.
  const [late, , due] = describeStoreSnapshot(snapshot, { unread: "EACCES: permission denied, stat '/srv/a.dat'" }, masters, new Date("2026-11-29T00:00:00Z"));
  assert.match(late ?? "", /: what it holds of the disk not read: EACCES: permission denied, stat '\/srv\/a\.dat'$/u);
  assert.equal(due, "  it may be deleted, since 2026-11-28T19:28:22Z: sudo /srv/cut-over/object-store/delete.sh, run by hand, deletes it, which resumes the compaction");
});
