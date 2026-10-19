/**
 * What workspace storage's file system holds, as the node agent's node mounts it: the bytes
 * and inodes it uses and has, by a statfs of the mount at the workspaces' root, which asks
 * the mount's JuiceFS client what it last counted and touches no metadata. The nodes' mount
 * runs no background job, so JuiceFS's own gauges of it (juicefs_used_space and the like) are
 * none; these are its, every node agent's of the one file system, so a reader takes the
 * greatest of them, never their sum. Where the root is no FUSE mount, as on a node whose
 * mount is not there yet, nothing.
 */
import type { StatsFs } from "node:fs";
import { statfs } from "node:fs/promises";

import type { BatchObservableResult } from "@opentelemetry/api";
import { Effect, type Scope } from "effect";

import { meter } from "../telemetry/index.ts";

/** Linux's magic number of FUSE's file systems (statfs(2)), JuiceFS's mount's. */
const FUSE_SUPER_MAGIC = 0x65735546;

/** What the file system holds, in bytes and inodes. */
export interface StorageUsage {
  readonly usedBytes: number;
  readonly totalBytes: number;
  readonly usedInodes: number;
  readonly totalInodes: number;
}

/** What the file system mounted at `root` holds, as `read` (statfs) says of it; null where it is no FUSE mount, or cannot be read. */
export async function storageUsage(root: string, read: (path: string) => Promise<StatsFs> = statfs): Promise<StorageUsage | null> {
  const stats = await read(root).catch(() => null);
  if (!stats || stats.type !== FUSE_SUPER_MAGIC) return null;
  return {
    usedBytes: (stats.blocks - stats.bfree) * stats.bsize,
    totalBytes: stats.blocks * stats.bsize,
    usedInodes: stats.files - stats.ffree,
    totalInodes: stats.files,
  };
}

const used = meter.createObservableGauge("alasio.workspace_storage.used", {
  description: "The bytes workspace storage's file system uses, as a node's mount of it counts them; every node's of the one file system, so read the greatest",
  unit: "By",
});
const capacity = meter.createObservableGauge("alasio.workspace_storage.capacity", {
  description: "The bytes workspace storage's file system has, as a node's mount of it counts them; every node's of the one file system, so read the greatest",
  unit: "By",
});
const usedInodes = meter.createObservableGauge("alasio.workspace_storage.inodes.used", {
  description: "The inodes workspace storage's file system uses, as a node's mount of it counts them; every node's of the one file system, so read the greatest",
  unit: "{inode}",
});
const inodes = meter.createObservableGauge("alasio.workspace_storage.inodes", {
  description: "The inodes workspace storage's file system has, as a node's mount of it counts them; every node's of the one file system, so read the greatest",
  unit: "{inode}",
});

/** Observes what the file system mounted at `root` holds, as its gauges are read, for as long as the scope is open. */
export const observeStorageUsage = (root: string, read?: (path: string) => Promise<StatsFs>): Effect.Effect<void, never, Scope.Scope> =>
  Effect.asVoid(Effect.acquireRelease(
    Effect.sync(() => {
      const gauges = [used, capacity, usedInodes, inodes];
      const observe = async (result: BatchObservableResult) => {
        const usage = await storageUsage(root, read);
        if (!usage) return;
        result.observe(used, usage.usedBytes);
        result.observe(capacity, usage.totalBytes);
        result.observe(usedInodes, usage.usedInodes);
        result.observe(inodes, usage.totalInodes);
      };
      meter.addBatchObservableCallback(observe, gauges);
      return { observe, gauges };
    }),
    ({ observe, gauges }) => Effect.sync(() => meter.removeBatchObservableCallback(observe, gauges)),
  ));
