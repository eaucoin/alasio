/**
 * The node agent's guard of its node's memory (src/node-agent/guard.ts): each bound sandbox's
 * cgroup limited to what it holds and its share of what the node has left, killing as a group;
 * and the largest sandbox ended as the node comes near its end; on a stand-in of runsc and of the
 * node's cgroups and /proc/meminfo.
 */
import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Effect, Exit, Scope } from "effect";

import { aboveRoom, type Guarded, guardNodeMemory, guardOnce, guardPlan, SANDBOX_MEMORY_SLACK_BYTES, sandboxCgroup } from "../src/node-agent/guard.ts";
import { NODE_MEMORY_SOFT_EVICTION_BYTES, RUNSC_ROOT } from "../src/node-agent/names.ts";
import { makeRunsc } from "../src/node-agent/runsc.ts";
import { fakeReleases } from "./support/fake-runsc.ts";

const RELEASE = "20261102.0-eaucoin.10";
const MIB = 1024 ** 2;
const GIB = 1024 ** 3;

/** A sandbox as the guard plans for it, its pod's and its own cgroup's holding the same. */
const sandbox = (key: string, state: Guarded["state"], bytes: number): Guarded => ({ key, state, bytes, scopeBytes: bytes });

test("each bound sandbox may grow by an equal share of what the node has left, the slack at the least, and those not bound are left as they are", () => {
  const sandboxes = [sandbox("a", "bound", 3 * GIB), sandbox("b", "bound", GIB), sandbox("free", "free", 30 * MIB), sandbox("standby", "standby", 20 * MIB)];
  const roomy = guardPlan(4 * GIB, 6 * GIB, sandboxes, new Set());
  assert.deepEqual([...roomy.limits], [["a", 5 * GIB], ["b", 3 * GIB]]);
  assert.equal(roomy.end, null);
  // Little left: the slack, so that a sandbox that frees as it writes is not ended by its own cgroup.
  const tight = guardPlan(100 * MIB, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, new Set());
  assert.deepEqual([...tight.limits], [["a", 3 * GIB + SANDBOX_MEMORY_SLACK_BYTES], ["b", GIB + SANDBOX_MEMORY_SLACK_BYTES]]);
  assert.equal(tight.end, null);
  // None left, as kubelet's soft eviction threshold is passed: the slack still.
  assert.equal(guardPlan(-GIB, SANDBOX_MEMORY_SLACK_BYTES, sandboxes, new Set()).limits.get("a"), 3 * GIB + SANDBOX_MEMORY_SLACK_BYTES);
});

test("the largest sandbox, bound or not, is ended as the node comes near its end, and none more while one ended is there, its memory being freed", () => {
  const sandboxes = [sandbox("b", "bound", GIB), sandbox("a", "bound", 3 * GIB), sandbox("free", "free", 30 * MIB)];
  assert.equal(guardPlan(0, SANDBOX_MEMORY_SLACK_BYTES - 1, sandboxes, new Set()).end, "a");
  assert.equal(guardPlan(0, -GIB, sandboxes, new Set(["a"])).end, null);
  // Gone, another is ended where the node is still near its end.
  assert.equal(guardPlan(0, -GIB, sandboxes.filter(({ key }) => key !== "a"), new Set(["a"])).end, "b");
  assert.equal(guardPlan(0, -GIB, [], new Set()).end, null);
});

test("a sandbox's processes are found in the one cgroup of its pod's that holds them, its pod sandbox's scope, or in its pod's own", async () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-guard-scope-"));
  try {
    const pod = join(root, "kubepods-burstable-pod1a2b.slice");
    for (const scope of ["cri-containerd-0a.scope", "cri-containerd-1b.scope"]) mkdirSync(join(pod, scope), { recursive: true });
    writeFileSync(join(pod, "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-0a.scope", "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-1b.scope", "cgroup.procs"), "101\n102\n");
    assert.equal(await sandboxCgroup(pod), join(pod, "cri-containerd-1b.scope"));
    writeFileSync(join(pod, "cgroup.procs"), "100\n");
    assert.equal(await sandboxCgroup(pod), pod);
    writeFileSync(join(pod, "cgroup.procs"), "");
    writeFileSync(join(pod, "cri-containerd-0a.scope", "cgroup.procs"), "103\n");
    // Two that hold processes: none is the sandbox's alone, and its pod is limited whole.
    assert.equal(await sandboxCgroup(pod), pod);
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

test("a look limits each bound sandbox's scope to what it holds and its share, killing as a group, and leaves a free pod's as kubelet made it", async () => {
  const node = guardedNode();
  try {
    // 9 GiB available, 1.5 GiB of it kubelet's soft eviction threshold's; the pods' class 3 GiB under its limit: 3 GiB left.
    node.memory(9 * GIB, 5 * GIB, 8 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 2 * GIB, true);
    const b = node.pod("sandboxes-none-1bbb", "bbbb-0002", GIB, true);
    const free = node.pod("sandboxes-none-2ccc", "cccc-0003", 30 * MIB, false);
    await Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot }, new Set()));
    const scope = (pod: string, file: string) => node.read(join(pod, "cri-containerd-pause.scope", file));
    assert.deepEqual([scope(a, "memory.max"), scope(a, "memory.oom.group")], [String(2 * GIB - MIB + 1.5 * GIB), "1"]);
    assert.deepEqual([scope(b, "memory.max"), scope(b, "memory.oom.group")], [String(GIB - MIB + 1.5 * GIB), "1"]);
    assert.deepEqual([scope(free, "memory.max"), scope(free, "memory.oom.group")], ["max", "0"]);
    // Where the node's memory available is the least: 2 GiB over the soft eviction threshold.
    node.memory(2 * GIB + NODE_MEMORY_SOFT_EVICTION_BYTES, 5 * GIB, 16 * GIB);
    await Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot }, new Set()));
    assert.equal(scope(a, "memory.max"), String(2 * GIB - MIB + GIB));
    // None ended while there is room.
    assert.deepEqual([a, b, free].map((pod) => node.read(join(pod, "cgroup.kill"))), [null, null, null]);
  } finally {
    node.done();
  }
});

test("as the node's pods come near their limit, the look ends the largest sandbox's pod whole, and the next only once it is gone", async () => {
  const node = guardedNode();
  try {
    node.memory(9 * GIB, 8 * GIB - 100 * MIB, 8 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 5 * GIB, true);
    const b = node.pod("sandboxes-none-1bbb", "bbbb-0002", GIB, true);
    const ended = new Set<string>();
    const look = () => Effect.runPromise(guardOnce({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot }, ended));
    await look();
    assert.deepEqual([node.read(join(a, "cgroup.kill")), node.read(join(b, "cgroup.kill"))], ["1", null]);
    // Its memory not yet freed at the next look, it is not ended again, nor another for it.
    rmSync(join(a, "cgroup.kill"));
    await look();
    assert.deepEqual([node.read(join(a, "cgroup.kill")), node.read(join(b, "cgroup.kill"))], [null, null]);
    // Gone, with the node still near its end: the next largest.
    node.releases.died("sandboxes-none-0aaa-pause");
    node.releases.died("sandboxes-none-0aaa-bayma");
    await look();
    assert.equal(node.read(join(b, "cgroup.kill")), "1");
  } finally {
    node.done();
  }
});

test("the node's memory is guarded look after look for as long as the agent runs, a sandbox bound since limited", async () => {
  const node = guardedNode();
  const scope = Effect.runSync(Scope.make());
  try {
    node.memory(9 * GIB, 5 * GIB, 8 * GIB);
    const a = node.pod("sandboxes-none-0aaa", "aaaa-0001", 2 * GIB, false);
    await Effect.runPromise(guardNodeMemory({ runsc: node.runsc, nodeRoot: node.nodeRoot, attachRoot: node.attachRoot, everyMs: 20 }).pipe(Scope.provide(scope)));
    mkdirSync(join(node.attachRoot, "alasio-sessions", "sandboxes-none-0aaa"), { recursive: true });
    writeFileSync(join(node.attachRoot, "alasio-sessions", "sandboxes-none-0aaa", "token"), "a binding's token");
    const limited = () => node.read(join(a, "cri-containerd-pause.scope", "memory.max")) !== "max";
    for (const deadline = Date.now() + 5_000; !limited() && Date.now() < deadline;) await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(node.read(join(a, "cri-containerd-pause.scope", "memory.max")), String(2 * GIB - MIB + 3 * GIB));
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    node.done();
  }
});
