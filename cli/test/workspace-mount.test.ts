/**
 * The script of the node's mount of workspace storage (cli/src/manifests/sandbox-gateway.ts),
 * run by sh with a stand-in for juicefs: idle once its mount is handed over, a failure once
 * it ends any other way, and ready only while its own mount serves.
 */
import assert from "node:assert/strict";
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { HANDED_OVER, workspaceMountReady, workspaceMountScript } from "../src/manifests/sandbox-gateway.ts";

/** What juicefs's stand-in does: serve until it is ended, hand its mount over, or fail. */
type Juicefs = "serves" | "hands over" | "fails";

const JUICEFS: Record<Juicefs, string> = {
  serves: "echo mounted; exec sleep 60",
  "hands over": `echo 'juicefs[7] <INFO>: ${HANDED_OVER} [launchMount@mount_unix.go:1057]'; exit 1`,
  fails: "echo 'juicefs[7] <FATAL>: the metadata engine refused us'; exit 3",
};

interface Mount {
  readonly run: string;
  readonly root: string;
  /** Has the node lose its mount at the root, as the pod that held it going does. */
  readonly lose: () => void;
  /** What mount was asked, a call a line. */
  readonly mounted: () => string[];
  readonly child: ChildProcess;
  readonly out: () => string;
  readonly ended: Promise<number | null>;
  readonly ready: () => boolean;
}

/**
 * The mount's script, its juicefs doing as `juicefs` says; stat saying the root is FUSE's
 * while the file `fs` says so, as it does unless a test says otherwise; and mount binding as it
 * is asked, each call logged in `mounts`, a bind at the root making it FUSE's again.
 */
function mount(t: TestContext, juicefs: Juicefs): Mount {
  const directory = mkdtempSync(join(tmpdir(), "alasio-workspace-mount-"));
  const bin = join(directory, "bin");
  const root = join(directory, "workspaces");
  const run = join(directory, "run");
  mkdirSync(bin);
  writeFileSync(join(bin, "juicefs"), `#!/bin/sh\n${JUICEFS[juicefs]}\n`);
  const fs = join(directory, "fs");
  const mounts = join(directory, "mounts");
  writeFileSync(fs, "fuseblk\n");
  writeFileSync(join(bin, "stat"), `#!/bin/sh\ncat ${fs}\n`);
  writeFileSync(join(bin, "mount"), `#!/bin/sh\necho "$*" >> ${mounts}\nif [ "$3" = ${root} ]; then echo fuseblk > ${fs}; fi\n`);
  for (const name of ["juicefs", "stat", "mount"]) chmodSync(join(bin, name), 0o755);
  const env = { ...process.env, PATH: `${bin}:${process.env["PATH"] ?? ""}`, META_URL: "redis://valkey/1" };
  // A group of its own, so whatever it started is ended with it.
  const child = spawn("sh", ["-c", workspaceMountScript(root, 1024, run)], { env, stdio: ["ignore", "pipe", "pipe"], detached: true });
  let out = "";
  child.stdout?.on("data", (chunk: Buffer) => (out += chunk.toString()));
  child.stderr?.on("data", (chunk: Buffer) => (out += chunk.toString()));
  const ended = new Promise<number | null>((resolve) => child.on("exit", (code) => resolve(code)));
  t.after(() => {
    try {
      process.kill(-(child.pid ?? 0), "SIGKILL");
    } catch {
      // Ended already, all of it.
    }
    rmSync(directory, { recursive: true, force: true });
  });
  return {
    run,
    root,
    child,
    out: () => out,
    ended,
    ready: () => spawnSync("sh", ["-c", workspaceMountReady(root, run)], { env }).status === 0,
    lose: () => writeFileSync(fs, "overlayfs\n"),
    mounted: () => (existsSync(mounts) ? readFileSync(mounts, "utf8").trim().split("\n") : []),
  };
}

/** Waits until `condition` holds, for a few seconds at most. */
async function until(condition: () => boolean): Promise<void> {
  for (let n = 0; n < 100 && !condition(); n += 1) await sleep(50);
  assert.ok(condition());
}

test("a mount that serves is ready, and, asked to end, ends its mount with it, a failure", async (t) => {
  const serving = mount(t, "serves");
  await until(() => serving.ready());
  await until(() => serving.out().includes("mounted"));
  serving.child.kill("SIGTERM");
  assert.notEqual(await serving.ended, 0);
  assert.equal(existsSync(join(serving.run, "handed-over")), false);
});

test("a mount that hands its session over idles, not ready, until its pod is deleted", async (t) => {
  const handed = mount(t, "hands over");
  await until(() => existsSync(join(handed.run, "handed-over")));
  assert.equal(handed.ready(), false);
  await sleep(500);
  assert.equal(handed.child.exitCode, null);
  assert.match(handed.out(), /handed over to the pod that replaces this one/u);
  handed.child.kill("SIGTERM");
  assert.equal(await handed.ended, 0);
});

test("a mount that ends without handing its session over fails, so kubelet starts it again, and is never idle", async (t) => {
  const failed = mount(t, "fails");
  assert.equal(await failed.ended, 3);
  assert.equal(existsSync(join(failed.run, "handed-over")), false);
  assert.match(failed.out(), /the metadata engine refused us/u);
  assert.match(failed.out(), /juicefs mount ended \(3\) without handing its mount over/u);
});

test("a mount that serves keeps a bind of the node's mount of its own, and binds it back at the root once the node has lost it", async (t) => {
  const serving = mount(t, "serves");
  await until(() => serving.mounted().length === 1);
  assert.deepEqual(serving.mounted(), [`--bind ${serving.root} ${join(serving.run, "kept")}`]);
  // The pod that held it before a handover gone, its mount with it.
  serving.lose();
  await until(() => serving.mounted().length === 2);
  assert.equal(serving.mounted()[1], `--bind ${join(serving.run, "kept")} ${serving.root}`);
  await until(() => serving.out().includes("it is bound again from this pod's own"));
  // The node's again: bound back once, not again and again.
  await sleep(1500);
  assert.equal(serving.mounted().length, 2);
});

test("a mount that has handed its session over makes its mounts of the node's its own, so its going leaves the node's whole, and keeps the node's mount no more", async (t) => {
  const handed = mount(t, "hands over");
  await until(() => existsSync(join(handed.run, "handed-over")));
  await until(() => handed.mounted().includes(`--make-rprivate ${handed.root}`));
  const bound = handed.mounted().length;
  handed.lose();
  await sleep(1500);
  assert.equal(handed.mounted().length, bound);
});
