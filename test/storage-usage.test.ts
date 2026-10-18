import assert from "node:assert/strict";
import type { StatsFs } from "node:fs";
import { test } from "node:test";

import { Effect } from "effect";

import { collected } from "./support/metrics.ts";
import { observeStorageUsage, storageUsage } from "../src/node-agent/storage-usage.ts";

/** What statfs says of a FUSE mount of 4 KiB blocks, 1000 of them, 250 free; and of 100 inodes, 40 free. */
const FUSE: StatsFs = { type: 0x65735546, bsize: 4096, blocks: 1000, bfree: 250, bavail: 250, frsize: 4096, files: 100, ffree: 40 };

test("what workspace storage holds is counted from a statfs of its mount, and is nothing of a root that is no FUSE mount, or cannot be read", async () => {
  assert.deepEqual(await storageUsage("/w", async () => FUSE), { usedBytes: 750 * 4096, totalBytes: 1000 * 4096, usedInodes: 60, totalInodes: 100 });
  assert.equal(await storageUsage("/w", async () => ({ ...FUSE, type: 0xef53 })), null);
  assert.equal(await storageUsage("/w", () => Promise.reject(new Error("ENOTCONN"))), null);
});

test("the node agent's gauges of workspace storage say what its mount counts as they are read", async () => {
  const value = async (name: string) => (await collected(name)).map(({ value }) => value);
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    yield* observeStorageUsage("/w", async () => FUSE);
    const read = yield* Effect.promise(async () => [await value("alasio.workspace_storage.used"), await value("alasio.workspace_storage.capacity"), await value("alasio.workspace_storage.inodes.used"), await value("alasio.workspace_storage.inodes")]);
    assert.deepEqual(read, [[750 * 4096], [1000 * 4096], [60], [100]]);
  })));
  // Read again as its mount counts more.
  await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    yield* observeStorageUsage("/w", async () => ({ ...FUSE, bfree: 100 }));
    assert.deepEqual(yield* Effect.promise(() => value("alasio.workspace_storage.used")), [900 * 4096]);
  })));
});
