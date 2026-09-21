import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";

import {
  invokesBaymaReplServer,
  materializeMcpServerConfig,
} from "../src/mcp/bayma-state.js";

function stateDirFor(serverName, threadKey = "telegram:5325143339") {
  const config = materializeMcpServerConfig(serverName, {
    command: "/usr/bin/node",
    args: [`/opt/${serverName}.cjs`, "mcp-stdio"],
  }, { TMPDIR: "/tmp/alasio-test" }, threadKey);
  const stateDirIndex = config.args.indexOf("--state-dir");
  return config.args[stateDirIndex + 1];
}

test("the unified Bayma MCP receives one deterministic per-thread state directory", () => {
  const replState = stateDirFor("bayma_repl");

  assert.equal(replState, join("/tmp/alasio-test", "alasio-bayma", `${process.pid}-telegram-5325143339`, "bayma_repl"));
  assert.equal(stateDirFor("bayma_repl"), replState);
});

test("Bayma state paths sanitize server and thread names", () => {
  assert.equal(
    stateDirFor("bayma.repl unsafe", "telegram:unsafe/thread"),
    join("/tmp/alasio-test", "alasio-bayma", `${process.pid}-telegram-unsafe-thread`, "bayma.repl-unsafe"),
  );
});

test("an explicit Bayma state directory is replaced with the isolated path", () => {
  const config = materializeMcpServerConfig("bayma_repl", {
    command: "bayma-repl",
    args: ["mcp-stdio", "--state-dir", "/shared/state"],
  }, { TMPDIR: "/tmp/alasio-test" }, "telegram:1");

  assert.deepEqual(config.args, [
    "mcp-stdio",
    "--state-dir",
    join("/tmp/alasio-test", "alasio-bayma", `${process.pid}-telegram-1`, "bayma_repl"),
  ]);
});

test("unified Bayma REPL classification follows server names, commands, and launchers", () => {
  assert.equal(
    invokesBaymaReplServer("bayma_repl", {
      command: "bayma-repl",
      args: ["mcp-stdio"],
    }),
    true,
  );
  assert.equal(
    invokesBaymaReplServer("language-workbench", {
      command: "/usr/bin/node",
      args: ["/opt/bayma-repl.cjs", "mcp-stdio"],
    }),
    true,
  );
  assert.equal(
    invokesBaymaReplServer("other-mcp", {
      command: "other-mcp",
      args: ["mcp-stdio"],
    }),
    false,
  );
});
