import assert from "node:assert/strict";
import { test } from "node:test";

import { describeReplication, volumeServerDisk } from "../src/object-store.ts";

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
