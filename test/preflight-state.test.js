import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { isolatePreflightState } from "../src/mcp/preflight-state.js";

test("MCP preflights use unique temporary state without changing runtime state", async () => {
  const root = await mkdtemp(join(tmpdir(), "alasio-preflight-state-test-"));
  const runtimeStateDir = join(root, "bayma_repl");
  const config = {
    command: "bayma-repl",
    args: ["mcp-stdio", "--state-dir", runtimeStateDir],
  };

  const first = await isolatePreflightState(config);
  const second = await isolatePreflightState(config);
  const firstStateDir = first.serverConfig.args.at(-1);
  const secondStateDir = second.serverConfig.args.at(-1);

  try {
    assert.deepEqual(config.args, ["mcp-stdio", "--state-dir", runtimeStateDir]);
    assert.match(firstStateDir, new RegExp(`^${runtimeStateDir}-preflight-`));
    assert.match(secondStateDir, new RegExp(`^${runtimeStateDir}-preflight-`));
    assert.notEqual(firstStateDir, secondStateDir);
    assert.equal(existsSync(runtimeStateDir), false);
    assert.equal(existsSync(firstStateDir), true);
    assert.equal(existsSync(secondStateDir), true);

    await first.cleanup();
    await second.cleanup();
    assert.equal(existsSync(firstStateDir), false);
    assert.equal(existsSync(secondStateDir), false);
  } finally {
    await first.cleanup();
    await second.cleanup();
    await rm(root, { recursive: true, force: true });
  }
});

test("MCP configs without state directories pass through unchanged", async () => {
  const config = { command: "generic-mcp", args: ["serve"] };
  const isolated = await isolatePreflightState(config);

  assert.equal(isolated.serverConfig, config);
  await isolated.cleanup();
});
