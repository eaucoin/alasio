/**
 * The script of the node's mount of workspace storage (cli/src/manifests/sandbox-gateway.ts),
 * run by sh with a stand-in for juicefs: idle once its mount is handed over, a failure once
 * it ends any other way, and ready only while its own mount serves.
 */
import assert from "node:assert/strict";
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
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
  readonly child: ChildProcess;
  readonly out: () => string;
  readonly ended: Promise<number | null>;
  readonly ready: () => boolean;
}

/** The mount's script, its juicefs doing as `juicefs` says, and stat saying the root is FUSE's. */
function mount(t: TestContext, juicefs: Juicefs): Mount {
  const directory = mkdtempSync(join(tmpdir(), "alasio-workspace-mount-"));
  const bin = join(directory, "bin");
  const root = join(directory, "workspaces");
  const run = join(directory, "run");
  mkdirSync(bin);
  writeFileSync(join(bin, "juicefs"), `#!/bin/sh\n${JUICEFS[juicefs]}\n`);
  writeFileSync(join(bin, "stat"), "#!/bin/sh\necho fuseblk\n");
  for (const name of ["juicefs", "stat"]) chmodSync(join(bin, name), 0o755);
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
  return { run, child, out: () => out, ended, ready: () => spawnSync("sh", ["-c", workspaceMountReady(root, run)], { env }).status === 0 };
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
