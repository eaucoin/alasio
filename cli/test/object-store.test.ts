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

test("reads the snapshot a cut-over took of the store's volumes from its ConfigMap, what of the disk it holds of its own, and says how to restore and delete it, and when", () => {
  const data = { path: "/srv/cut-over/object-store", taken: "2026-11-26T19:28:22Z", keepUntil: "2026-11-28T19:28:22Z", copiedBytes: "1048576", volumes: "/srv/a.dat\n/srv/b.dat\n" };
  const snapshot = storeSnapshotOf(data);
  assert.deepEqual(snapshot, { path: data.path, taken: data.taken, keepUntil: data.keepUntil, copiedBytes: 1_048_576, volumes: ["/srv/a.dat", "/srv/b.dat"] });
  // A ConfigMap missing what it names, or counting no bytes, names none.
  assert.equal(storeSnapshotOf(undefined), null);
  assert.equal(storeSnapshotOf({ ...data, keepUntil: "" }), null);
  assert.equal(storeSnapshotOf({ ...data, copiedBytes: "many" }), null);
  if (snapshot === null) return;
  // What it copied, and each data file the store no longer links: one compacted away, of 2 GiB on the disk; one shared still.
  assert.equal(storeSnapshotHolds(snapshot, [{ nlink: 1, blocks: 4 * 1024 ** 2 }, { nlink: 2, blocks: 8 }]), 1_048_576 + 2 * 1024 ** 3);
  const held = 1_048_576 + 2 * 1024 ** 3;
  assert.equal(
    describeStoreSnapshot(snapshot, held, new Date("2026-11-27T00:00:00Z")),
    "its snapshot, taken 2026-11-26T19:28:22Z before its upgrade, at /srv/cut-over/object-store: holding 2.0 GiB of the disk of its own, more as the store compacts or deletes the volumes it shares with it; " +
      "sudo /srv/cut-over/object-store/restore.sh puts the store back as it was, its parts stopped; kept until 2026-11-28T19:28:22Z, then deleted by sudo /srv/cut-over/object-store/delete.sh",
  );
  assert.match(describeStoreSnapshot(snapshot, { unread: "EACCES: permission denied, stat '/srv/a.dat'" }, new Date("2026-11-29T00:00:00Z")), /: what it holds of the disk not read: EACCES: permission denied, stat '\/srv\/a\.dat'; .*; kept until 2026-11-28T19:28:22Z, which has passed: delete it with sudo \/srv\/cut-over\/object-store\/delete\.sh$/u);
});
