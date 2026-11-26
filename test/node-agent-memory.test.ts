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

import { STANDBY_ANNOTATION, STANDBY_SOCKETS } from "../src/kube/sandbox-shape.ts";
import { meminfoOf, nodeMemory, ownMemory, podCgroup, sandboxesOf, statOf } from "../src/node-agent/memory.ts";
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
  writeFileSync(join(ownCgroups, "kubepods.slice", "agent.scope", "memory.stat"), `anon ${40 * MIB}\nfile ${100 * MIB}\nactive_file ${30 * MIB}\ninactive_file ${70 * MIB}\nfile_dirty 0\n`);
  writeFileSync(join(ownCgroups, "kubepods.slice", "agent.scope", "memory.events"), "low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n");
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
  /** Processes in the pod sandbox's scope of the pod `uid`, as the node's /proc says them: each pid's argv and smaps_rollup's Pss, in kB. */
  const processes = (uid: string, each: Readonly<Record<number, readonly [readonly string[], number]>>) => {
    const scope = join(ownCgroups, "kubepods.slice", "kubepods-burstable.slice", `kubepods-burstable-pod${uid.replaceAll("-", "_")}.slice`, "cri-containerd-0abc.scope");
    mkdirSync(scope, { recursive: true });
    writeFileSync(join(scope, "cgroup.procs"), Object.keys(each).map((pid) => `${pid}\n`).join(""));
    for (const [pid, [argv, pss]] of Object.entries(each)) {
      mkdirSync(join(nodeRoot, "proc", pid), { recursive: true });
      writeFileSync(join(nodeRoot, "proc", pid, "cmdline"), `${argv.join("\0")}\0`);
      writeFileSync(join(nodeRoot, "proc", pid, "smaps_rollup"), `55d0c0000000-7ffd00000000 ---p 00000000 00:00 0                          [rollup]\nRss:              ${pss * 2} kB\nPss:              ${pss} kB\nPss_Anon:         ${pss} kB\n`);
    }
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
  return { root, releases, runsc, nodeRoot, attachRoot, cgroupOf, processes, pod, bound, read, done: () => rmSync(root, { recursive: true, force: true }) };
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
    assert.equal((await ownMemory(proc, join(root, "fs"))).agentBytes, 4096);
    // Its own namespace: its cgroup is the root it sees, its path said as /.
    writeFileSync(join(root, "fs", "memory.current"), "8192\n");
    writeFileSync(proc, "0::/\n");
    assert.equal((await ownMemory(proc, join(root, "fs"))).agentBytes, 8192);
    rmSync(join(root, "fs", "memory.current"));
    assert.deepEqual(await ownMemory(proc, join(root, "fs")), {
      agentBytes: null, agentAnonBytes: null, agentFileBytes: null, agentActiveFileBytes: null, agentInactiveFileBytes: null, agentDirtyBytes: null, agentEvents: null,
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the agent's own memory is split as its memory.stat says, its anonymous memory from its page cache, the page cache's active, inactive and dirty, with its cgroup's events", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-own-split-"));
  try {
    const proc = join(root, "cgroup");
    const own = join(root, "fs", "pod.slice");
    mkdirSync(own, { recursive: true });
    writeFileSync(proc, "0::/pod.slice\n");
    writeFileSync(join(own, "memory.current"), `${500 * MIB}\n`);
    writeFileSync(join(own, "memory.stat"), `anon ${90 * MIB}\nfile ${400 * MIB}\nkernel 1000\nactive_file ${100 * MIB}\ninactive_file ${300 * MIB}\nfile_dirty ${2 * MIB}\n`);
    writeFileSync(join(own, "memory.events"), "low 0\nhigh 0\nmax 1234\noom 0\noom_kill 0\noom_group_kill 0\n");
    assert.deepEqual(await ownMemory(proc, join(root, "fs")), {
      agentBytes: 500 * MIB, agentAnonBytes: 90 * MIB, agentFileBytes: 400 * MIB, agentActiveFileBytes: 100 * MIB, agentInactiveFileBytes: 300 * MIB, agentDirtyBytes: 2 * MIB,
      agentEvents: { high: 0, max: 1234, oom: 0, oomKill: 0 },
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("each sandbox's memory is read from its pod's cgroup, its MemoryFile's of it and its gofers' from their processes, by its pod's state; and what may be held ahead is half of what kubelet's reservations, its soft eviction and the bound sandboxes leave", async () => {
  const node = memoryNode();
  try {
    // A session's, bound, its token written beside its workspace.
    node.pod("sandboxes-none-0aaa", "aaaa-0001");
    node.cgroupOf("aaaa-0001", 3 * GIB, 64 * MIB, 2900 * MIB, 64 * MIB);
    node.bound("sandboxes-none-0aaa");
    // Its sandbox, and a gofer to each of its two containers, all in its pod sandbox's scope.
    node.processes("aaaa-0001", {
      101: [["runsc-sandbox", "--root=/run/containerd/runsc/k8s.io", "boot"], 90_000],
      102: [["runsc-gofer", "--root=/run/containerd/runsc/k8s.io", "gofer", "--bundle", "/run/a"], 3_000],
      103: [["runsc-gofer", "--root=/run/containerd/runsc/k8s.io", "gofer", "--bundle", "/run/b"], 1_000],
    });
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
      { namespace: "alasio-sessions", pod: "sandboxes-none-0aaa", state: "bound", bytes: 3 * GIB, anonBytes: 64 * MIB, fileBytes: 3 * GIB - 64 * MIB, shmemBytes: 2900 * MIB, zswapBytes: 64 * MIB, goferBytes: 4_000 * 1024 },
      { namespace: "alasio-sessions", pod: "sandboxes-none-2ccc", state: "free", bytes: null, anonBytes: null, fileBytes: null, shmemBytes: null, zswapBytes: null, goferBytes: null },
      { namespace: "alasio-sessions", pod: "sandboxes-none-restore-1bbb", state: "standby", bytes: 32 * MIB, anonBytes: 16 * MIB, fileBytes: 16 * MIB, shmemBytes: 8 * MIB, zswapBytes: 0, goferBytes: null },
    ]);
    assert.deepEqual([memory.totalBytes, memory.availableBytes, memory.agentBytes, memory.agentAnonBytes, memory.agentFileBytes], [16 * GIB, 9 * GIB, 150 * MIB, 40 * MIB, 100 * MIB]);
    assert.deepEqual(memory.agentEvents, { high: 0, max: 3, oom: 1, oomKill: 1 });
    assert.equal(memory.budgetBytes, (16 * GIB - NODE_MEMORY_RESERVED_BYTES - NODE_MEMORY_SOFT_EVICTION_BYTES - 3 * GIB) / 2);
    assert.equal(memory.budgetedBytes, 32 * MIB);
  } finally {
    node.done();
  }
});

test("a standby, which runsc lists none of, is counted by its shim's socket, its pod's uid as kubelet's logs name it, held ahead; a socket whose pod is gone is not", async () => {
  const node = memoryNode();
  try {
    const sockets = join(node.nodeRoot, STANDBY_SOCKETS);
    mkdirSync(sockets, { recursive: true });
    // Its pod's shim's socket and its container's beside it; and one a standby left as it went.
    for (const name of ["alasio-sessions.sandboxes-none-restore-1bbb.sock", "alasio-sessions.sandboxes-none-restore-1bbb.bayma.sock", "alasio-sessions.sandboxes-none-restore-9zzz.sock"]) writeFileSync(join(sockets, name), "");
    for (const dir of ["alasio-sessions_sandboxes-none-restore-1bbb_bbbb-0002", "alasio-sessions_sandboxes-none-restore-9zzz_zzzz-0009"]) mkdirSync(join(node.nodeRoot, "var", "log", "pods", dir), { recursive: true });
    node.cgroupOf("bbbb-0002", 17 * MIB, 12 * MIB, 4 * MIB);
    const memory = await node.read();
    assert.deepEqual(memory.sandboxes.map(({ pod, state, bytes, anonBytes }) => [pod, state, bytes, anonBytes]), [["sandboxes-none-restore-1bbb", "standby", 17 * MIB, 12 * MIB]]);
    assert.equal(memory.budgetedBytes, 17 * MIB);
    // Claimed, its binding's token written, before runsc lists it: bound, not held ahead.
    node.bound("sandboxes-none-restore-1bbb");
    const claimed = await node.read();
    assert.deepEqual(claimed.sandboxes.map(({ pod, state }) => [pod, state]), [["sandboxes-none-restore-1bbb", "bound"]]);
    assert.equal(claimed.budgetedBytes, 0);
  } finally {
    node.done();
  }
});

test("the sandboxes are read without their split where asked, of what their cgroups hold alone, their memory.stat not read", async () => {
  const node = memoryNode();
  try {
    node.pod("sandboxes-none-0aaa", "aaaa-0001");
    node.cgroupOf("aaaa-0001", 3 * GIB, 64 * MIB, 2900 * MIB, 64 * MIB);
    node.bound("sandboxes-none-0aaa");
    const [sandbox] = await Effect.runPromise(sandboxesOf({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot }, false));
    assert.deepEqual(
      { ...sandbox, cgroup: sandbox?.cgroup?.endsWith("kubepods-burstable-podaaaa_0001.slice") },
      { namespace: "alasio-sessions", pod: "sandboxes-none-0aaa", state: "bound", bytes: 3 * GIB, anonBytes: null, fileBytes: null, shmemBytes: null, zswapBytes: null, goferBytes: null, cgroup: true },
    );
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
