/**
 * What a sandbox pool's pod runs before bayma (sandbox/agent/start.sh), run by sh on
 * directories of the test's own: bayma's server once in a pod, its hook as often as it is asked.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { SAVE_HOOK_SECONDS } from "../src/node-agent/agent.ts";

const START = new URL("../sandbox/agent/start.sh", import.meta.url).pathname;

test("bayma's server is started once in a pod, its container started again running nothing of the workspace, and its hook as often as it is asked", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-agent-start-"));
  try {
    const attach = join(root, "attach");
    const workspace = join(root, "workspace");
    for (const directory of [attach, workspace, join(root, "home")]) mkdirSync(directory);
    writeFileSync(join(attach, "env"), "BAYMA_EXAMPLE='bound'\n");
    writeFileSync(join(attach, "token"), "binding");
    const start = (...args: string[]) =>
      spawnSync("sh", [START, "sh", "-c", 'echo "$BAYMA_EXAMPLE in $(pwd)"', ...args], { env: { PATH: process.env["PATH"], HOME: join(root, "home"), ALASIO_ATTACH: attach, ALASIO_WORKSPACE: workspace }, encoding: "utf8" });
    const served = start("mcp-http");
    assert.deepEqual([served.status, served.stdout.trim()], [0, `bound in ${workspace}`]);
    const again = start("mcp-http");
    assert.equal(again.status, 1);
    assert.match(again.stderr, /has served this pod's binding already/u);
    for (const _ of [1, 2]) assert.equal(start("sandbox-hook", "stop").status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("bayma's hook is killed past its bound as it readies the sandbox for a save, or goes on after one taken live, and has runsc's longer one as the sandbox is restored", () => {
  const root = mkdtempSync(join(tmpdir(), "alasio-agent-start-"));
  try {
    const attach = join(root, "attach");
    const workspace = join(root, "workspace");
    for (const directory of [attach, workspace, join(root, "home")]) mkdirSync(directory);
    writeFileSync(join(attach, "env"), "");
    writeFileSync(join(attach, "token"), "binding");
    // A hook that takes 2 s, its bound for a save 1 s.
    const hook = (mode: string) =>
      spawnSync("sh", [START, "sh", "-c", "sleep 2", "sandbox-hook"], {
        env: { PATH: process.env["PATH"], HOME: join(root, "home"), ALASIO_ATTACH: attach, ALASIO_WORKSPACE: workspace, ALASIO_SAVE_HOOK_SECONDS: "1", GVISOR_SAVE_RESTORE_AUTO_EXEC_MODE: mode },
        encoding: "utf8",
      });
    // Ended as timeout(1) ends what outlasts it: 124.
    assert.deepEqual(["save", "resume", "restore"].map((mode) => hook(mode).status), [124, 124, 0]);
    // Its bound for a save, but for tests, the agent's.
    assert.equal(/ALASIO_SAVE_HOOK_SECONDS:-(\d+)/u.exec(readFileSync(START, "utf8"))?.[1], String(SAVE_HOOK_SECONDS));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
