/**
 * The node agent's guard of its node's memory (src/node-agent/guard.ts): each bound sandbox's
 * cgroup limited to what it holds and its headroom of what the node has left, killing as a group;
 * the sandbox that grew the most ended as the node comes near its end, and none where none grew,
 * the want another pod's; never its pod's own cgroup written to; on a stand-in of runsc and of the
 * node's cgroups and /proc/meminfo.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Exit, Scope } from "effect";

import {
  aboveRoom,
  END_WINDOW_MS,
  GROWN_BYTES,
  type Guarded,
  guardNodeMemory,
  guardOnce,
  guardPlan,
  GuardRecords,
  newGuardState,
  SANDBOX_MEMORY_HEADROOM_BYTES,
  SANDBOX_MEMORY_LIMITED_BELOW_BYTES,
  SANDBOX_MEMORY_SLACK_BYTES,
  sandboxCgroup,
} from "../src/node-agent/guard.ts";
import { NODE_MEMORY_SOFT_EVICTION_BYTES, RUNSC_ROOT } from "../src/node-agent/names.ts";
import { makeRunsc } from "../src/node-agent/runsc.ts";
import { fakeReleases } from "./support/fake-runsc.ts";

const RELEASE = "20261102.0-eaucoin.10";
const MIB = 1024 ** 2;
const GIB = 1024 ** 3;

/** A sandbox as the guard plans for it, its pod's and its own cgroup's holding the same, grown by `grown` over the guard's window. */
const sandbox = (key: string, state: Guarded["state"], bytes: number, grown = 0): Guarded => ({ key, state, bytes, scopeBytes: bytes, grown });

test("each bound sandbox may grow by what the node has left, up to 1 GiB, the slack at the least, unlimited where its share is ample, and those not bound are left as they are", () => {
  const sandboxes = [sandbox("a", "bound", 3 * GIB), sandbox("b", "bound", GIB), sandbox("free", "free", 30 * MIB), sandbox("standby", "standby", 20 * MIB)];
  // Each one's share no less than SANDBOX_MEMORY_LIMITED_BELOW_BYTES: none limited.
  const roomy = guardPlan(2 * SANDBOX_MEMORY_LIMITED_BELOW_BYTES, 6 * GIB, sandboxes, false);
  assert.deepEqual([...roomy.limits], [["a", null], ["b", null]]);
  assert.equal(roomy.end, null);
  // Each one's share less, 768 MiB: limited, but each may still take a sudden 1 GiB the node has room for.
  const less = guardPlan(1.5 * GIB, 6 * GIB, sandboxes, false);
  assert.deepEqual([...less.limits], [["a", 3 * GIB + SANDBOX_MEMORY_HEADROOM_BYTES], ["b", GIB + SANDBOX_MEMORY_HEADROOM_BYTES]]);
  assert.equal(SANDBOX_MEMORY_HEADROOM_BYTES, GIB);
  // Less left than that: what is left.
  assert.equal(guardPlan(0.5 * GIB, 6 * GIB, sandboxes, false).limits.get("a"), 3.5 * GIB);
  // Little left: the slack, so that a sandbox that frees as it writes is not ended by its own cgroup.
  const tight = guardPlan(100 * MIB, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, false);
  assert.deepEqual([...tight.limits], [["a", 3 * GIB + SANDBOX_MEMORY_SLACK_BYTES], ["b", GIB + SANDBOX_MEMORY_SLACK_BYTES]]);
  assert.equal(tight.end, null);
  // None left, as kubelet's soft eviction threshold is passed: the slack still.
  assert.equal(guardPlan(-GIB, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, false).limits.get("a"), 3 * GIB + SANDBOX_MEMORY_SLACK_BYTES);
});

test("as the node comes near its end, the sandbox that grew the most is ended, not the largest, and none more while one ended is there or was just ended", () => {
  const sandboxes = [sandbox("a", "bound", 3 * GIB, 100 * MIB), sandbox("b", "bound", GIB, 512 * MIB), sandbox("c", "bound", 2 * GIB, GROWN_BYTES), sandbox("free", "free", 30 * MIB)];
  const plan = guardPlan(0, SANDBOX_MEMORY_SLACK_BYTES - 1, sandboxes, false);
  assert.deepEqual([plan.end, plan.foreign], ["b", false]);
  assert.deepEqual([guardPlan(0, -GIB, sandboxes, true).end, guardPlan(0, -GIB, sandboxes, true).foreign], [null, false]);
  // Room enough before kubelet evicts at once: none, however much one grew.
  assert.deepEqual([guardPlan(0, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, false).end, guardPlan(0, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, false).foreign], [null, false]);
});

test("as the node comes near its end with no sandbox grown into it, the want another pod's, none is ended, however large", () => {
  const sandboxes = [sandbox("a", "bound", 6 * GIB, GROWN_BYTES - 1), sandbox("b", "bound", GIB), sandbox("free", "free", 30 * MIB)];
  const plan = guardPlan(-GIB, -GIB, sandboxes, false);
  assert.deepEqual([plan.end, plan.foreign], [null, true]);
  // Its limits still set, as for any want.
  assert.equal(plan.limits.get("a"), 6 * GIB + SANDBOX_MEMORY_SLACK_BYTES);
  assert.deepEqual([guardPlan(0, -GIB, [], false).end, guardPlan(0, -GIB, [], false).foreign], [null, true]);
});

test("a sandbox's processes are found in the one cgroup of its pod's that holds them, its pod sandbox's scope; none where its pod's own holds any, or none of its cgroups does, or more than one", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-guard-scope-"));
  try {
    const pod = join(root, "kubepods-burstable-pod1a2b.slice");
    for (const scope of ["cri-containerd-0a.scope", "cri-containerd-1b.scope"]) mkdirSync(join(pod, scope), { recursive: true });
    writeFileSync(join(pod, "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-0a.scope", "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-1b.scope", "cgroup.procs"), "");
    assert.equal(await sandboxCgroup(pod), null);
    writeFileSync(join(pod, "cri-containerd-1b.scope", "cgroup.procs"), "101\n102\n");
    assert.equal(await sandboxCgroup(pod), join(pod, "cri-containerd-1b.scope"));
    writeFileSync(join(pod, "cgroup.procs"), "100\n");
    assert.equal(await sandboxCgroup(pod), null);
    writeFileSync(join(pod, "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-0a.scope", "cgroup.procs"), "103\n");
    assert.equal(await sandboxCgroup(pod), null);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("what the cgroups above the pods have left is the least of each's limit less what it holds but its clean page cache, one without a limit counting none", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-guard-above-"));
  try {
    const qos = join(root, "kubepods.slice", "kubepods-burstable.slice");
    const pod = join(qos, "kubepods-burstable-pod1a2b.slice");
    mkdirSync(pod, { recursive: true });
    writeFileSync(join(root, "kubepods.slice", "memory.max"), `${12 * GIB}\n`);
    writeFileSync(join(root, "kubepods.slice", "memory.current"), `${8 * GIB}\n`);
    writeFileSync(join(qos, "memory.max"), "max\n");
    writeFileSync(join(qos, "memory.current"), `${7 * GIB}\n`);
    assert.equal(await aboveRoom(root, [pod]), 4 * GIB);
    writeFileSync(join(qos, "memory.max"), `${9 * GIB}\n`);
    assert.equal(await aboveRoom(root, [pod]), 2 * GIB);
    // Its clean page cache is room, as the kernel reclaims it first; not what is dirty or being written back.
    writeFileSync(join(root, "kubepods.slice", "memory.max"), `${20 * GIB}\n`);
    writeFileSync(join(qos, "memory.stat"), `anon ${4 * GIB}\nfile ${3 * GIB}\nactive_file ${GIB}\ninactive_file ${2 * GIB}\nfile_dirty ${512 * MIB}\nfile_writeback ${256 * MIB}\nshmem 0\n`);
    assert.equal(await aboveRoom(root, [pod]), 2 * GIB + 3 * GIB - 768 * MIB);
    assert.equal(await aboveRoom(root, []), Number.POSITIVE_INFINITY);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

/** A node of the test's own: its runsc's stand-in, its cgroups and /proc/meminfo, and its pods' directories. */
function guardedNode() {
  const root = mkdtempSync(join(tmpdir(), "alasio-guard-"));
  const releases = fakeReleases(root, [RELEASE]);
  const nodeRoot = join(root, "node");
  const attachRoot = join(root, "attach");
  const cgroups = join(nodeRoot, "sys", "fs", "cgroup");
  const qos = join(cgroups, "kubepods.slice", "kubepods-burstable.slice");
  mkdirSync(join(nodeRoot, "proc"), { recursive: true });
  /** The node's memory available, and what the pods' QoS class's cgroup holds of its limit. */
  const memory = (available: number, held: number, max: number) => {
    writeFileSync(join(nodeRoot, "proc", "meminfo"), `MemTotal:       ${16 * 1024 * 1024} kB\nMemAvailable:    ${available / 1024} kB\n`);
    mkdirSync(qos, { recursive: true });
    writeFileSync(join(qos, "memory.current"), `${held}\n`);
    writeFileSync(join(qos, "memory.max"), `${max}\n`);
  };
  /** A pod of the sessions' namespace, of the uid `uid`, holding `bytes`, its sandbox's processes in its pod sandbox's scope; bound where said. */
  const pod = (name: string, uid: string, bytes: number, bound: boolean) => {
    releases.runs(`${name}-pause`, RELEASE, "alasio-sessions", name, true, [], uid);
    releases.rooted(`${name}-pause`);
    releases.runs(`${name}-bayma`, RELEASE, "alasio-sessions", name, true, [], uid);
    const dir = join(qos, `kubepods-burstable-pod${uid.replaceAll("-", "_")}.slice`);
    mkdirSync(join(dir, "cri-containerd-pause.scope"), { recursive: true });
    writeFileSync(join(dir, "memory.current"), `${bytes}\n`);
    writeFileSync(join(dir, "memory.stat"), `anon ${bytes}\nfile 0\nshmem 0\n`);
    writeFileSync(join(dir, "cgroup.procs"), "");
    writeFileSync(join(dir, "cri-containerd-pause.scope", "cgroup.procs"), "4242\n");
    writeFileSync(join(dir, "cri-containerd-pause.scope", "memory.current"), `${bytes - MIB}\n`);
    writeFileSync(join(dir, "cri-containerd-pause.scope", "memory.max"), "max\n");
    writeFileSync(join(dir, "cri-containerd-pause.scope", "memory.oom.group"), "0\n");
    if (bound) {
      mkdirSync(join(attachRoot, "alasio-sessions", name), { recursive: true });
      writeFileSync(join(attachRoot, "alasio-sessions", name, "token"), "a binding's token");
    }
    return dir;
  };
  const runsc = makeRunsc({ releasesDir: releases.releasesDir, containerdDir: releases.containerdDir, root: RUNSC_ROOT, procRoot: releases.procRoot, current: RELEASE });
  const read = (path: string) => (existsSync(path) ? readFileSync(path, "utf8").trim() : null);
  return { root, releases, nodeRoot, attachRoot, runsc, memory, pod, read, done: () => rmSync(root, { recursive: true, force: true }) };
}

test("a look limits each bound sandbox's scope to what it holds and its share, killing as a group, lifts the limit as the share grows ample, and leaves a free pod's as kubelet made it", async () => {
  const node = guardedNode();
  try {
    // 9 GiB available, 1.5 GiB of it kubelet's soft eviction threshold's; the pods' class 1.5 GiB under its limit: 1.5 GiB left.
    node.memory(9 * GIB, 5 * GIB, 6.5 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 2 * GIB, true);
    const b = node.pod("sandboxes-none-1bbb", "bbbb-0002", GIB, true);
    const free = node.pod("sandboxes-none-2ccc", "cccc-0003", 30 * MIB, false);
    const records = new GuardRecords();
    const look = () => Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot, records }, newGuardState()));
    await look();
    const scope = (pod: string, file: string) => node.read(join(pod, "cri-containerd-pause.scope", file));
    // Each one's share 768 MiB: each may grow by 1 GiB, the room holding as much.
    assert.deepEqual([scope(a, "memory.max"), scope(a, "memory.oom.group")], [String(2 * GIB - MIB + GIB), "1"]);
    assert.deepEqual([scope(b, "memory.max"), scope(b, "memory.oom.group")], [String(GIB - MIB + GIB), "1"]);
    assert.deepEqual([scope(free, "memory.max"), scope(free, "memory.oom.group")], ["max", "0"]);
    // What it set kept for the gateway to say.
    assert.deepEqual(records.all().map(({ pod, limitBytes, endedBytes }) => [pod, limitBytes, endedBytes]), [["sandboxes-none-0aaa", 3 * GIB - MIB, null], ["sandboxes-none-1bbb", 2 * GIB - MIB, null]]);
    // Where the node's memory available is the least: 512 MiB over the soft eviction threshold.
    node.memory(0.5 * GIB + NODE_MEMORY_SOFT_EVICTION_BYTES, 5 * GIB, 16 * GIB);
    await look();
    assert.equal(scope(a, "memory.max"), String(2 * GIB - MIB + 0.5 * GIB));
    // Room enough that each one's share is ample: no limit, killing as a group still.
    node.memory(9 * GIB, 5 * GIB, 16 * GIB);
    await look();
    assert.deepEqual([scope(a, "memory.max"), scope(a, "memory.oom.group"), scope(b, "memory.max")], ["max", "1", "max"]);
    assert.equal(records.all().find(({ pod }) => pod === "sandboxes-none-0aaa")?.limitBytes, null);
    // None ended while there is room, and nothing written to a pod's own cgroup.
    assert.deepEqual([a, b, free].flatMap((pod) => [node.read(join(pod, "cgroup.kill")), node.read(join(pod, "cri-containerd-pause.scope", "cgroup.kill")), node.read(join(pod, "memory.max"))]), Array(9).fill(null));
  } finally {
    node.done();
  }
});

test("as the node's pods come near their limit with no sandbox grown, the want another pod's, the looks end none; once one grows, they end its scope's processes whole, it alone, and none more for a while", async () => {
  const node = guardedNode();
  try {
    node.memory(9 * GIB, 8 * GIB - 100 * MIB, 8 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 5 * GIB, true);
    const b = node.pod("sandboxes-none-1bbb", "bbbb-0002", GIB, true);
    const records = new GuardRecords();
    const state = newGuardState();
    const look = () => Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot, records }, state));
    const kills = () => [a, b].flatMap((pod) => [node.read(join(pod, "cgroup.kill")), node.read(join(pod, "cri-containerd-pause.scope", "cgroup.kill"))]);
    for (let each = 0; each < 3; each += 1) await look();
    assert.deepEqual(kills(), [null, null, null, null]);
    // The smaller grows by 512 MiB: it alone ended, by its scope, its pod's own cgroup not written to.
    writeFileSync(join(b, "cri-containerd-pause.scope", "memory.current"), `${1.5 * GIB}\n`);
    await look();
    assert.deepEqual(kills(), [null, null, null, "1"]);
    assert.equal(records.all().find(({ pod }) => pod === "sandboxes-none-1bbb")?.endedBytes, GIB);
    // Its memory not yet freed at the next look, it is not ended again, nor another for it.
    rmSync(join(b, "cri-containerd-pause.scope", "cgroup.kill"));
    await look();
    assert.deepEqual(kills(), [null, null, null, null]);
    // Gone, the larger grown since, with the node still near its end: none while the room is measured anew.
    node.releases.died("sandboxes-none-1bbb-pause");
    node.releases.died("sandboxes-none-1bbb-bayma");
    writeFileSync(join(a, "cri-containerd-pause.scope", "memory.current"), `${6 * GIB}\n`);
    await look();
    assert.deepEqual(kills(), [null, null, null, null]);
    // Once that while is over, it is ended.
    state.lastEndAt -= END_WINDOW_MS;
    await look();
    assert.deepEqual(kills(), [null, "1", null, null]);
  } finally {
    node.done();
  }
});

test("a sandbox whose pod's own cgroup holds processes is left unguarded: nothing written to its pod's cgroup nor its scope, however it grows", async () => {
  const node = guardedNode();
  try {
    node.memory(9 * GIB, 8 * GIB - 100 * MIB, 8 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 5 * GIB, true);
    writeFileSync(join(a, "cgroup.procs"), "4243\n");
    const state = newGuardState();
    const look = () => Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot }, state));
    await look();
    writeFileSync(join(a, "cri-containerd-pause.scope", "memory.current"), `${6 * GIB}\n`);
    await look();
    assert.deepEqual(
      ["cgroup.kill", "memory.max", "memory.oom.group", "cri-containerd-pause.scope/cgroup.kill", "cri-containerd-pause.scope/memory.oom.group"].map((file) => node.read(join(a, file))),
      [null, null, null, null, "0"],
    );
    assert.equal(node.read(join(a, "cri-containerd-pause.scope", "memory.max")), "max");
  } finally {
    node.done();
  }
});

test("the node's memory is guarded look after look for as long as the agent runs, a sandbox bound since limited", async () => {
  const node = guardedNode();
  const scope = Effect.runSync(Scope.make());
  try {
    node.memory(9 * GIB, 5 * GIB, 5.5 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 2 * GIB, false);
    await Effect.runPromise(guardNodeMemory({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot, everyMs: 20 }).pipe(Scope.provide(scope)));
    mkdirSync(join(node.attachRoot, "alasio-sessions", "sandboxes-none-0aaa"), { recursive: true });
    writeFileSync(join(node.attachRoot, "alasio-sessions", "sandboxes-none-0aaa", "token"), "a binding's token");
    const limited = () => node.read(join(a, "cri-containerd-pause.scope", "memory.max")) !== "max";
    for (const deadline = Date.now() + 5_000; !limited() && Date.now() < deadline;) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(node.read(join(a, "cri-containerd-pause.scope", "memory.max")), String(2 * GIB - MIB + 0.5 * GIB));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    node.done();
  }
});

test("what the cgroups above the pods hold that the kernel reclaims is read at most every 2 s, as its read is the kernel's work, and at the next look once one has reached its limit since", async () => {
  const node = guardedNode();
  const scope = Effect.runSync(Scope.make());
  try {
    node.memory(9 * GIB, 5 * GIB, 8 * GIB);
    const events = join(node.nodeRoot, "sys", "fs", "cgroup", "kubepods.slice", "kubepods-burstable.slice", "memory.events");
    writeFileSync(events, "low 0\nhigh 0\nmax 0\noom 0\n");
    node.pod("sandboxes-none-0aaa", "aaaa-0001", 2 * GIB, true);
    let reads = 0;
    const reclaimable = async () => ((reads += 1), 0);
    await Effect.runPromise(guardNodeMemory({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot, everyMs: 20, reclaimable }).pipe(Scope.provide(scope)));
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(reads, 1);
    // It reached its limit: read again at the next look, then not again while it reaches it no more.
    writeFileSync(events, "low 0\nhigh 0\nmax 3\noom 0\n");
    await new Promise((resolve) => setTimeout(resolve, 600));
    assert.equal(reads, 2);
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    node.done();
  }
});
