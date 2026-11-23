/**
 * What the node agent reads of the node's memory (src/node-agent/memory.ts): its sandboxes', by
 * their pods' state, from their pods' cgroups, on a stand-in of runsc and of the node's cgroups and
 * /proc/meminfo; the agent's own; and the budget of what is held ahead.
 */
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect } from "effect";

import { STANDBY_ANNOTATION } from "../src/kube/sandbox-shape.ts";
import { meminfoOf, nodeMemory, ownMemory, podCgroup, statOf } from "../src/node-agent/memory.ts";
import { NODE_MEMORY_RESERVED_BYTES, NODE_MEMORY_SOFT_EVICTION_BYTES, RUNSC_ROOT } from "../src/node-agent/names.ts";
import { makeRunsc } from "../src/node-agent/runsc.ts";
import { fakeReleases } from "./support/fake-runsc.ts";

const RELEASE = "20261102.0-eaucoin.10";
const MIB = 1024 ** 2;
const GIB = 1024 ** 3;

/** A node of the test's own: its runsc's stand-in, its cgroups and /proc/meminfo, the agent's pods' directories and its own cgroup. */
function memoryNode() {
  const root = mkdtempSync(join(tmpdir(), "alasio-memory-"));
  const releases = fakeReleases(root, [RELEASE]);
  const nodeRoot = join(root, "node");
  const attachRoot = join(root, "attach");
  // The agent's own cgroup, as it sees the node's, sharing its cgroup namespace.
  const ownCgroups = join(nodeRoot, "sys", "fs", "cgroup");
  const ownProc = join(root, "self-cgroup");
  mkdirSync(join(ownCgroups, "kubepods.slice", "agent.scope"), { recursive: true });
  writeFileSync(join(ownCgroups, "kubepods.slice", "agent.scope", "memory.current"), `${150 * MIB}\n`);
  writeFileSync(ownProc, "0::/kubepods.slice/agent.scope\n");
  mkdirSync(join(nodeRoot, "proc"), { recursive: true });
  writeFileSync(join(nodeRoot, "proc", "meminfo"), `MemTotal:       ${16 * 1024 * 1024} kB\nMemFree:         1000 kB\nMemAvailable:    ${9 * 1024 * 1024} kB\n`);
  /** A pod's cgroup, as kubelet makes it under systemd's driver, of `bytes`, `anon` of them anonymous, `shmem` of the rest its MemoryFile's, and zswap's where given. */
  const cgroupOf = (uid: string, bytes: number, anon: number, shmem: number, zswap?: number) => {
    const dir = join(ownCgroups, "kubepods.slice", "kubepods-burstable.slice", `kubepods-burstable-pod${uid.replaceAll("-", "_")}.slice`);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "memory.current"), `${bytes}\n`);
    writeFileSync(join(dir, "memory.stat"), `anon ${anon}\nfile ${bytes - anon}\nkernel 1000\nshmem ${shmem}\n`);
    if (zswap !== undefined) writeFileSync(join(dir, "memory.zswap.current"), `${zswap}\n`);
  };
  /** A pod of the sessions' namespace, its pause container and bayma's, which run, of the uid `uid`. */
  const pod = (name: string, uid: string) => {
    releases.runs(`${name}-pause`, RELEASE, "alasio-sessions", name, true, [], uid);
    releases.rooted(`${name}-pause`);
    releases.runs(`${name}-bayma`, RELEASE, "alasio-sessions", name, true, [], uid);
  };
  /** Binds the pod `name`: its binding's token written in its directory. */
  const bound = (name: string) => {
    mkdirSync(join(attachRoot, "alasio-sessions", name), { recursive: true });
    writeFileSync(join(attachRoot, "alasio-sessions", name, "token"), "a binding's token");
  };
  const runsc = makeRunsc({ releasesDir: releases.releasesDir, containerdDir: releases.containerdDir, root: RUNSC_ROOT, procRoot: releases.procRoot, current: RELEASE });
  const read = () => Effect.runPromise(nodeMemory({ runsc, nodeRoot, attachRoot, ownProc, ownCgroups }));
  return { root, releases, cgroupOf, pod, bound, read, done: () => rmSync(root, { recursive: true, force: true }) };
}

test("memory.stat's and /proc/meminfo's values are read by their keys, none of a key they lack", () => {
  assert.equal(statOf("anon 4096\nfile 8192\n", "file"), 8192);
  assert.equal(statOf("anon 4096\nanon_thp 0\n", "anon"), 4096);
  assert.equal(statOf("anon 4096\n", "file"), null);
  assert.equal(statOf(null, "anon"), null);
  assert.equal(meminfoOf("MemTotal:       16384 kB\nMemAvailable:    8192 kB\n", "MemAvailable"), 8192 * 1024);
  assert.equal(meminfoOf("MemTotal:       16384 kB\n", "MemAvailable"), null);
});

test("a pod's cgroup is found by its uid, as kubelet names it under systemd's driver and under cgroupfs's, and none where it has none", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-cgroups-"));
  try {
    const systemd = join(root, "kubepods.slice", "kubepods-pod1a2b_3c4d.slice");
    const cgroupfs = join(root, "kubepods", "besteffort", "pod5e6f-7a8b");
    for (const dir of [systemd, cgroupfs]) {
      mkdirSync(dir, { recursive: true });
      writeFileSync(join(dir, "memory.current"), "4096\n");
    }
    assert.equal(await podCgroup(root, "1a2b-3c4d"), systemd);
    assert.equal(await podCgroup(root, "5e6f-7a8b"), cgroupfs);
    assert.equal(await podCgroup(root, "9c0d-1e2f"), null);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the agent's own memory is its cgroup's, by the path its process names where it shares the node's cgroup namespace, and its namespace's root's where it has its own", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-own-cgroup-"));
  try {
    const proc = join(root, "cgroup");
    mkdirSync(join(root, "fs", "pod.slice"), { recursive: true });
    writeFileSync(join(root, "fs", "pod.slice", "memory.current"), "4096\n");
    writeFileSync(proc, "0::/pod.slice\n");
    assert.equal(await ownMemory(proc, join(root, "fs")), 4096);
    // Its own namespace: its cgroup is the root it sees, its path said as /.
    writeFileSync(join(root, "fs", "memory.current"), "8192\n");
    writeFileSync(proc, "0::/\n");
    assert.equal(await ownMemory(proc, join(root, "fs")), 8192);
    rmSync(join(root, "fs", "memory.current"));
    assert.equal(await ownMemory(proc, join(root, "fs")), null);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("each sandbox's memory is read from its pod's cgroup, its MemoryFile's of it, by its pod's state; and what may be held ahead is half of what kubelet's reservations, its soft eviction and the bound sandboxes leave", async () => {
  const node = memoryNode();
  try {
    // A session's, bound, its token written beside its workspace.
    node.pod("sandboxes-none-0aaa", "aaaa-0001");
    node.cgroupOf("aaaa-0001", 3 * GIB, 64 * MIB, 2900 * MIB, 64 * MIB);
    node.bound("sandboxes-none-0aaa");
    // A standby, its bayma's container created and not yet run, as one waits for a restore.
    node.pod("sandboxes-none-restore-1bbb", "bbbb-0002");
    node.releases.held("sandboxes-none-restore-1bbb-bayma", "created");
    for (const container of ["sandboxes-none-restore-1bbb-pause", "sandboxes-none-restore-1bbb-bayma"]) node.releases.annotate(container, { [STANDBY_ANNOTATION]: "/run/standby.sock" });
    node.cgroupOf("bbbb-0002", 32 * MIB, 16 * MIB, 8 * MIB);
    // A pool's free pod, whose cgroup is not to be found.
    node.pod("sandboxes-none-2ccc", "cccc-0003");
    // One that has stopped, which is no sandbox that runs.
    node.pod("sandboxes-none-3ddd", "dddd-0004");
    node.releases.died("sandboxes-none-3ddd-pause");
    node.releases.died("sandboxes-none-3ddd-bayma");
    const memory = await node.read();
    assert.deepEqual(memory.sandboxes, [
      { namespace: "alasio-sessions", pod: "sandboxes-none-0aaa", state: "bound", bytes: 3 * GIB, anonBytes: 64 * MIB, fileBytes: 3 * GIB - 64 * MIB, shmemBytes: 2900 * MIB, zswapBytes: 64 * MIB },
      { namespace: "alasio-sessions", pod: "sandboxes-none-2ccc", state: "free", bytes: null, anonBytes: null, fileBytes: null, shmemBytes: null, zswapBytes: null },
      { namespace: "alasio-sessions", pod: "sandboxes-none-restore-1bbb", state: "standby", bytes: 32 * MIB, anonBytes: 16 * MIB, fileBytes: 16 * MIB, shmemBytes: 8 * MIB, zswapBytes: 0 },
    ]);
    assert.deepEqual([memory.totalBytes, memory.availableBytes, memory.agentBytes], [16 * GIB, 9 * GIB, 150 * MIB]);
    assert.equal(memory.budgetBytes, (16 * GIB - NODE_MEMORY_RESERVED_BYTES - NODE_MEMORY_SOFT_EVICTION_BYTES - 3 * GIB) / 2);
    assert.equal(memory.budgetedBytes, 32 * MIB);
  } finally {
    node.done();
  }
});

test("no budget is left where the bound sandboxes and what kubelet keeps take the node's memory, and runsc that cannot list its sandboxes counts none", async () => {
  const node = memoryNode();
  try {
    node.pod("sandboxes-none-0aaa", "aaaa-0001");
    node.cgroupOf("aaaa-0001", 15 * GIB, 15 * GIB, 0);
    node.bound("sandboxes-none-0aaa");
    assert.equal((await node.read()).budgetBytes, 0);
    writeFileSync(join(node.releases.dirOf(RELEASE), "runsc"), "#!/bin/sh\necho 'runsc could not list' >&2\nexit 1\n");
    const memory = await node.read();
    assert.deepEqual(memory.sandboxes, []);
    assert.equal(memory.budgetBytes, (16 * GIB - NODE_MEMORY_RESERVED_BYTES - NODE_MEMORY_SOFT_EVICTION_BYTES) / 2);
  } finally {
    node.done();
  }
});
