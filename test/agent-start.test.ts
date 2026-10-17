/**
 * What a sandbox pool's pod runs before bayma (sandbox/agent/start.sh), run by sh on
 * directories of the test's own: bayma's server once in a pod, its hook as often as it is asked.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

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
