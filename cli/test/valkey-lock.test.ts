/**
 * The script a Valkey's server is started by (cli/src/manifests/valkey.ts oneAtATime), run
 * as its container runs it, on a lock of a directory of the test's: the server it is given
 * started once no other holds the lock, and none started where the lock cannot be taken.
 */
import assert from "node:assert/strict";
import { type ChildProcess, spawn, spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { oneAtATime } from "../src/manifests/valkey.ts";

const directory = mkdtempSync(join(tmpdir(), "alasio-valkey-lock-"));
after(() => rmSync(directory, { recursive: true, force: true }));

/** The script on the lock `lock`, starting `echo SERVED` for the server, with `path` for its PATH. */
function start(lock: string, path = process.env["PATH"] ?? ""): { readonly child: ChildProcess; readonly said: () => string; readonly ended: Promise<number | null> } {
  const child = spawn("/bin/sh", ["-c", oneAtATime(lock), "echo", "SERVED"], { env: { PATH: path }, stdio: ["ignore", "pipe", "pipe"] });
  let said = "";
  child.stdout.on("data", (chunk: Buffer) => (said += chunk.toString()));
  child.stderr.on("data", (chunk: Buffer) => (said += chunk.toString()));
  return { child, said: () => said, ended: new Promise((resolve) => child.on("exit", (code) => resolve(code))) };
}

/** Holds `lock` from other processes, a group of their own, until the returned function ends them. */
async function holding(lock: string): Promise<() => Promise<void>> {
  const holder = spawn("flock", [lock, "sleep", "60"], { stdio: "ignore", detached: true });
  for (let deadline = Date.now() + 5_000; spawnSync("flock", ["-n", lock, "true"]).status === 0; await sleep(20)) {
    assert.ok(Date.now() < deadline, "the lock was never held");
  }
  return async () => {
    const gone = new Promise((resolve) => holder.on("exit", resolve));
    // The group, as the sleep holds the lock's descriptor too.
    process.kill(-(holder.pid ?? 0), "SIGKILL");
    await gone;
  };
}

test("starts the server at once where no other holds the lock", async () => {
  const run = start(join(directory, "free.lock"));
  assert.equal(await run.ended, 0);
  assert.equal(run.said(), "SERVED\n");
});

test("starts the server only once the one holding the lock has ended, saying it waits", async () => {
  const lock = join(directory, "held.lock");
  const letGo = await holding(lock);
  const run = start(lock);
  await sleep(500);
  assert.equal(run.child.exitCode, null);
  assert.equal(run.said(), `waiting for the Valkey that holds ${lock} to end\n`);
  await letGo();
  assert.equal(await run.ended, 0);
  assert.match(run.said(), /SERVED\n$/u);
});

test("starts no server where the lock cannot be taken, or its wait is killed", async () => {
  // A flock that fails, as one would on a file system without locks.
  const failing = join(directory, "failing-bin");
  mkdirSync(failing);
  writeFileSync(join(failing, "flock"), "#!/bin/sh\nexit 1\n");
  chmodSync(join(failing, "flock"), 0o755);
  const refused = start(join(directory, "failing.lock"), `${failing}:${process.env["PATH"] ?? ""}`);
  assert.notEqual(await refused.ended, 0);
  assert.doesNotMatch(refused.said(), /SERVED/u);

  // A wait killed: the flock the script waits in.
  const lock = join(directory, "killed.lock");
  const letGo = await holding(lock);
  try {
    const killed = start(lock);
    await sleep(500);
    spawnSync("pkill", ["-KILL", "-P", String(killed.child.pid), "flock"]);
    assert.notEqual(await killed.ended, 0);
    assert.doesNotMatch(killed.said(), /SERVED/u);
  } finally {
    await letGo();
  }
});

test("starts no server where the lock's file cannot be opened", async () => {
  const run = start(join(directory, "missing", "valkey.lock"));
  assert.notEqual(await run.ended, 0);
  assert.doesNotMatch(run.said(), /SERVED/u);
});
